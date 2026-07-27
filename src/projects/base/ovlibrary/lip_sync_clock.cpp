#include "lip_sync_clock.h"

#include "base/ovlibrary/clock.h"
#define OV_LOG_TAG "LipSyncClock"

bool LipSyncClock::RegisterRtpClock(uint32_t id, double timebase)
{
	auto clock = std::make_shared<Clock>();
	clock->_timebase = timebase;

	std::lock_guard<std::shared_mutex> lock(_lock);
	_clock_map.emplace(id, clock);

	return true;
}

std::shared_ptr<LipSyncClock::Clock> LipSyncClock::GetClock(uint32_t id)
{
	std::shared_lock<std::shared_mutex> lock(_lock);

	auto item = _clock_map.find(id);
	if (item == _clock_map.end())
	{
		return nullptr;
	}

	return item->second;
}

std::optional<int64_t> LipSyncClock::CalcPTS(uint32_t id, uint32_t rtp_timestamp)
{
	auto clock = GetClock(id);
	if (clock == nullptr)
	{
		return {};
	}

	// Exclusive : this function mutates the clock state (a shared lock would let two threads
	// corrupt it), and it must be serialized against UpdateSenderReportTime() for the same clock
	std::lock_guard<std::shared_mutex> lock(clock->_clock_lock);

	int64_t delta = 0;
	if (clock->_first_pts)
	{
		clock->_first_pts = false;
		clock->_first_packet_time = std::chrono::system_clock::now();
		clock->_extended_rtp_timestamp = rtp_timestamp;
		clock->_first_extended_rtp_timestamp = clock->_extended_rtp_timestamp;

		std::lock_guard<std::shared_mutex> first_clock_lock(_lock);
		if (_first_clock == nullptr)
		{
			_first_clock = clock;
		}
	}
	else
	{
		if (rtp_timestamp > clock->_last_rtp_timestamp)
		{
			delta = rtp_timestamp - clock->_last_rtp_timestamp;
		}
		else
		{
			delta = clock->_last_rtp_timestamp - rtp_timestamp;
			if (delta > 0x80000000)
			{
				// wrap around
				delta = 0xFFFFFFFF - clock->_last_rtp_timestamp + rtp_timestamp + 1;
			}
			else
			{
				// reordering or duplicate or error
				// delta = 0; /!\ Setting the delta to 0 generates an offset on the next timestamps. This can cause drift and loss of synchronization
				// delta cannot be greater clock->_extended_rtp_timestamp
				delta *= -1;
				logtw("RTP timestamp is not monotonic: %u -> %u delta: %" PRId64, clock->_last_rtp_timestamp, rtp_timestamp, delta);
			}
		}

		clock->_extended_rtp_timestamp += delta;
	}

	clock->_last_rtp_timestamp = rtp_timestamp;

	std::shared_ptr<Clock> first_clock;
	{
		std::shared_lock<std::shared_mutex> first_clock_lock(_lock);
		first_clock = _first_clock;
	}

	int64_t final_pts = 0;
	if (first_clock == nullptr)
	{
		// can not happen
		return {};
	}

	if (clock->_updated)
	{
		if (!clock->_ready)
		{
			// calculate constant with first RTCP SR
			clock->_adjust_pts = clock->_pts - static_cast<int64_t>(clock->_extended_rtcp_timestamp - clock->_first_extended_rtp_timestamp);
			clock->_ready = true;
		}
		auto pts = clock->_pts + (static_cast<int64_t>(clock->_extended_rtp_timestamp) - static_cast<int64_t>(clock->_extended_rtcp_timestamp));
		final_pts = pts - clock->_adjust_pts;
	}
	else
	{
		final_pts = clock->_extended_rtp_timestamp - clock->_first_extended_rtp_timestamp;
	}

	if (first_clock != clock)
	{
		// delta with the first clock (time between first packet of first clock and first packet of this clock)
		int64_t delta_ms = 0;
		if (clock->_offset_state != Clock::OffsetState::FINAL_VALUE)
		{
			// The first clock's fields are written by the thread handling that other track, so
			// they must be read under its own lock. Ordering is always "this clock then first
			// clock": the first clock never takes another clock's lock, so there is no cycle.
			bool first_clock_ready = false;
			int64_t first_clock_adjust_pts = 0;
			double first_clock_timebase = 0;
			{
				std::shared_lock<std::shared_mutex> first_clock_lock(first_clock->_clock_lock);
				first_clock_ready = first_clock->_ready;
				first_clock_adjust_pts = first_clock->_adjust_pts;
				first_clock_timebase = first_clock->_timebase;
			}

			if (first_clock_ready && clock->_ready)
			{
				// use RTCP SR to calculate delta
				auto start_time = static_cast<double>(clock->_adjust_pts) * clock->_timebase;
				auto start_time_first_clock = static_cast<double>(first_clock_adjust_pts) * first_clock_timebase;
				delta_ms = (start_time - start_time_first_clock) * 1000.0;	// to ms

				// _adjust_pts is anchored on the first RTP packet's timestamp and on the first
				// SR's timestamp. If the 32 bits RTP timestamp wrapped between those two anchors
				// it is off by 2^32 ticks: that cancels out in final_pts, which is modular, but
				// not here, where the value is used in plain floating point - the offset would be
				// wrong by about 13 hours at 90kHz and desynchronise the track for good.
				if (std::abs(delta_ms) > LIP_SYNC_MAX_OFFSET_MS)
				{
					logtw("Implausible inter-track offset for id(%u) : %" PRId64 " ms, ignored", id, delta_ms);
				}
				else
				{
					clock->_offset_pts_target = delta_ms / (clock->_timebase * 1000.0);
					clock->_offset_state = Clock::OffsetState::FINAL_VALUE;
				}
			}
			else if (clock->_offset_state != Clock::OffsetState::TEMPORARY_VALUE)
			{
				// No SR yet on both tracks: no offset can be computed, start aligned
				clock->_offset_pts_target = 0;
				clock->_offset_state = Clock::OffsetState::TEMPORARY_VALUE;
			}
		}

		// Converge toward the target rather than stepping to it. The offset only becomes known
		// once both tracks have received an SR, seconds into the stream; applying it at once
		// would shift every following PTS of this track in one go, and for a negative offset
		// (this track started before the first one) the PTS would go backwards - which OME does
		// not allow downstream. Bounding the step to a fraction of the media progress keeps the
		// PTS strictly increasing while the correction is spread over a few seconds.
		if (clock->_offset_pts != clock->_offset_pts_target && delta > 0)
		{
			int64_t remaining = clock->_offset_pts_target - clock->_offset_pts;
			int64_t max_step = std::max<int64_t>(1, delta / LIP_SYNC_OFFSET_SLEW_DIVIDER);

			if (std::abs(remaining) <= max_step)
			{
				clock->_offset_pts = clock->_offset_pts_target;
			}
			else
			{
				clock->_offset_pts += (remaining > 0) ? max_step : -max_step;
			}
		}

		final_pts += clock->_offset_pts;
	}

	logtd("Calc PTS : id(%u) final_pts(%" PRId64 ") last_rtp_timestamp(%u) rtp_timestamp(%u) delta(%" PRId64 ") extended_rtp_timestamp(%" PRIu64 ")",
		  id, final_pts, clock->_last_rtp_timestamp, rtp_timestamp, delta, clock->_extended_rtp_timestamp);
	return final_pts;
}

bool LipSyncClock::UpdateSenderReportTime(uint32_t id, uint32_t ntp_msw, uint32_t ntp_lsw, uint32_t rtcp_timestamp)
{
	auto clock = GetClock(id);
	if (clock == nullptr)
	{
		return false;
	}

	// OBS WHIP incorrectly sends RTP Timestamp with 0xFFFFFFFF in the first SR. Below is the code to avoid this.
	if (rtcp_timestamp == 0xFFFFFFFF)
	{
		return false;
	}

	_enabled = true;

	std::lock_guard<std::shared_mutex> lock(clock->_clock_lock);
	clock->_updated = true;

	if (clock->_first_sr == true)
	{
		clock->_extended_rtcp_timestamp = rtcp_timestamp;
		clock->_first_sr = false;
	}
	else
	{
		int64_t delta = 0;
		if (rtcp_timestamp > clock->_last_rtcp_timestamp)
		{
			delta = rtcp_timestamp - clock->_last_rtcp_timestamp;
		}
		else
		{
			delta = clock->_last_rtcp_timestamp - rtcp_timestamp;

			if (delta > 0x80000000)
			{
				// wrap around
				delta = 0xFFFFFFFF - clock->_last_rtcp_timestamp + rtcp_timestamp + 1;
			}
			else
			{
				// reordering or duplicate or error
				// delta = 0; /!\ Setting the delta to 0 generates an offset on the next timestamps. This can cause drift and loss of synchronization
				// delta cannot be greater clock->_extended_rtcp_timestamp
				delta *= -1;
				logtw("RTCP timestamp is not monotonic: %u -> %u delta: %" PRId64, clock->_last_rtcp_timestamp, rtcp_timestamp, delta);
			}
		}

		clock->_extended_rtcp_timestamp += delta;
	}

	auto ntp = ov::Converter::NtpTsToSeconds(ntp_msw, ntp_lsw);
	// printf("RTCP SR : %u %u %ld\n", id, rtcp_timestamp, (int64_t)(ntp * 1000));
	clock->_last_rtcp_timestamp = rtcp_timestamp;
	clock->_pts = ntp / clock->_timebase;

	logtd("Update SR : id(%u) NTP(%u/%u) pts(%" PRId64 ") rtp timestamp(%u) extended timestamp (%" PRIu64 ")",
		  id, ntp_msw, ntp_lsw, clock->_pts, clock->_last_rtcp_timestamp, clock->_extended_rtcp_timestamp);

	return true;
}