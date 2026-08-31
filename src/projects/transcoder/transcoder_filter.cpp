#include "transcoder_filter.h"

#include "filter/filter_resampler.h"
#include "filter/filter_rescaler.h"
#include "transcoder_gpu.h"
#include "transcoder_private.h"

using namespace cmn;

#define PTS_INCREMENT_LIMIT 15

// Minimum delay between two attempts to (re)build a filter graph that failed.
#define FILTER_RETRY_INTERVAL_MS 1000

TranscodeFilter::TranscodeFilter()
	: _internal(nullptr)
{
}

TranscodeFilter::~TranscodeFilter()
{
}

bool TranscodeFilter::Configure(int32_t id,
								const std::shared_ptr<info::Stream>& input_stream_info, std::shared_ptr<MediaTrack> input_track,
								const std::shared_ptr<info::Stream>& output_stream_info, std::shared_ptr<MediaTrack> output_track,
								CompleteHandler complete_handler)
{
	_id = id;
	_input_stream_info = input_stream_info;
	_input_track = input_track;
	_output_stream_info = output_stream_info;
	_output_track = output_track;
	_complete_handler = complete_handler;

	_timestamp_jump_threshold = (int64_t)_input_track->GetTimeBase().GetTimescale() * PTS_INCREMENT_LIMIT;

	return Create();
}

bool TranscodeFilter::Create()
{
	std::lock_guard<std::shared_mutex> lock(_mutex);

	_last_create_attempt_at = std::chrono::steady_clock::now();

	// If there is a previously created filter, remove it.
	if (_internal != nullptr)
	{
		_internal->Stop();
		_internal.reset();
		_internal = nullptr;
	}

	switch (_input_track->GetMediaType())
	{
		case MediaType::Audio:
			_internal = std::make_shared<FilterResampler>();
			break;
		case MediaType::Video:
			_internal = std::make_shared<FilterRescaler>();
			break;
		default:
			logte("Unsupported media type in filter");
			return false;
	}

	auto name = ov::String::FormatString("filter_%s", cmn::GetMediaTypeString(_input_track->GetMediaType()).CStr());
	auto urn = std::make_shared<info::ManagedQueue::URN>(
		_input_stream_info->GetApplicationName(),
		_input_stream_info->GetName(),
		"trs",
		name.LowerCaseString());
	_internal->SetQueueUrn(urn);
	_internal->SetCompleteHandler(bind(&TranscodeFilter::OnComplete, this, std::placeholders::_1));

	bool success = _internal->Configure(_input_track, _output_track);
	if (success == false)
	{
		logte("Could not create filter");

		// Drop the half-initialized filter instead of keeping it around. Its worker thread was
		// never started, so it would silently swallow every frame from now on, and IsNeedUpdate()
		// would never rebuild it because the input resolution already matches the one that failed.
		// Releasing it is what makes the next frame retry the whole setup.
		_internal.reset();
		_internal = nullptr;

		return false;
	}

	return _internal->Start();
}

void TranscodeFilter::Stop()
{
	std::lock_guard<std::shared_mutex> lock(_mutex);

	if (_internal != nullptr)
	{
		_internal->Stop();
		_internal.reset();
		_internal = nullptr;
	}
}

bool TranscodeFilter::SendBuffer(std::shared_ptr<MediaFrame> buffer)
{
	if (IsNeedUpdate(buffer) == true)
	{
		if (Create() == false)
		{
			logte("Failed to regenerate filter");
			return false;
		}

		return true;
	}

	std::shared_lock<std::shared_mutex> lock(_mutex);
	if (_internal == nullptr)
	{
		return false;
	}

	return _internal->SendBuffer(std::move(buffer));
}

bool TranscodeFilter::IsNeedUpdate(std::shared_ptr<MediaFrame> buffer)
{
	// In case of pts/dts jumps
	int64_t last_timestamp = _last_timestamp;
	int64_t curr_timestamp = buffer->GetPts();
	_last_timestamp = curr_timestamp;

	// Check #1 - Abnormal timestamp
	int64_t increment = abs(curr_timestamp - last_timestamp);
	bool is_abnormal_timestamp = (last_timestamp != -1LL && increment > _timestamp_jump_threshold) ? true : false;
	if (is_abnormal_timestamp)
	{
		logtw("Timestamp has changed abnormally.  %lld -> %lld", last_timestamp, buffer->GetPts());

		return true;
	}

	// Check #2 - Resolution change
	std::shared_lock<std::shared_mutex> lock(_mutex);

	if (_internal == nullptr)
	{
		// A previous Create() failed. Retry, but not on every single frame: a filter graph that
		// cannot be built for this input/output pair would otherwise flood the log and burn CPU
		// at the input framerate.
		return HasRetryDelayElapsed();
	}

	if (_input_track->GetMediaType() == MediaType::Video)
	{
		if (buffer->GetWidth() != (int32_t)_internal->GetInputWidth() ||
			buffer->GetHeight() != (int32_t)_internal->GetInputHeight())
		{
			logti("Changed input resolution of %u track. (%dx%d -> %dx%d)", _input_track->GetId(), _internal->GetInputWidth(), _internal->GetInputHeight(), buffer->GetWidth(), buffer->GetHeight());
			_input_track->SetWidth(buffer->GetWidth());
			_input_track->SetHeight(buffer->GetHeight());
			return true;
		}
	}

	// A filter that went into ERROR never recovers on its own, and nothing else in the pipeline
	// rebuilds it: the frames keep being queued into a graph that no longer produces anything, so
	// the output track stays dead for the rest of the stream. This was originally limited to the
	// XMA scaler (intermittent resource allocation failures), but the same dead end is reachable
	// with the software scaler - typically when a filter graph rebuild triggered by an input
	// resolution change fails - so recreate on ERROR whatever the codec module is.
	if (_internal->GetState() == FilterBase::State::ERROR)
	{
		if (HasRetryDelayElapsed() == false)
		{
			return false;
		}

		logtw("The filter is in an error state. So, recreate the filter.");
		return true;
	}

	return false;
}

bool TranscodeFilter::HasRetryDelayElapsed() const
{
	auto elapsed = std::chrono::steady_clock::now() - _last_create_attempt_at;

	return std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() >= FILTER_RETRY_INTERVAL_MS;
}

void TranscodeFilter::SetCompleteHandler(CompleteHandler complete_handler)
{
	_complete_handler = move(complete_handler);
}

void TranscodeFilter::OnComplete(std::shared_ptr<MediaFrame> frame)
{
	if (_complete_handler)
	{
		_complete_handler(_id, frame);
	}
}

cmn::Timebase TranscodeFilter::GetInputTimebase() const
{
	return _internal->GetInputTimebase();
}

cmn::Timebase TranscodeFilter::GetOutputTimebase() const
{
	return _internal->GetOutputTimebase();
}

std::shared_ptr<MediaTrack>& TranscodeFilter::GetInputTrack()
{
	return _input_track;
}

std::shared_ptr<MediaTrack>& TranscodeFilter::GetOutputTrack()
{
	return _output_track;
}