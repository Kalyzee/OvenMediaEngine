#include "rtp_frame_jitter_buffer.h"

#define OV_LOG_TAG "RtpVideoJitterBuffer"

/************************************************************************
 * 								RTPFrame
 ***********************************************************************/

RtpFrame::RtpFrame(uint32_t timestamp)
{
	_timestamp = timestamp;
	_stop_watch.Start();
}

std::shared_ptr<RtpPacket> RtpFrame::GetFirstRtpPacket()
{
	if (IsCompleted() == false)
	{
		return nullptr;
	}

	_curr_order_number = _min_order_number;

	auto it = _packets.find(_curr_order_number);
	if (it == _packets.end())
	{
		return nullptr;
	}

	return it->second;
}

std::shared_ptr<RtpPacket> RtpFrame::GetNextRtpPacket()
{
	if (IsCompleted() == false)
	{
		return nullptr;
	}

	_curr_order_number++;

	auto it = _packets.find(_curr_order_number);
	if (it == _packets.end())
	{
		return nullptr;
	}

	return it->second;
}

uint16_t RtpFrame::GetOrderNumber(uint16_t sequence_number)
{
	if (_first_packet == true)
	{
		_first_packet = false;
		_first_sequence_number = sequence_number;
		_base_order_number = std::numeric_limits<uint16_t>::max() / 2;

		return _base_order_number;
	}

	if (sequence_number > _first_sequence_number)
	{
		if (sequence_number - _first_sequence_number > _base_order_number + 1)
		{
			// out of order : 0 -> 65535 ==> gap : -1
			uint16_t gap = std::numeric_limits<uint16_t>::max() - sequence_number + _first_sequence_number + 1;
			return _base_order_number - gap;
		}
		else
		{
			// In order : 1 -> 2 ==> gap : +1
			return _base_order_number + (sequence_number - _first_sequence_number);
		}
	}
	else
	{
		if (_first_sequence_number - sequence_number > _base_order_number)
		{
			// Roll over : 65535 -> 0 ==> gpa : +1
			uint16_t gap = std::numeric_limits<uint16_t>::max() - _first_sequence_number + sequence_number + 1;
			return _base_order_number + gap;
		}
		else
		{
			// Out of order : 2 -> 1
			return _base_order_number - (_first_sequence_number - sequence_number);
		}
	}

	// Should not reach here
	return 0;
}

bool RtpFrame::InsertPacket(const std::shared_ptr<RtpPacket>& packet)
{
	if (packet == nullptr || packet->Timestamp() != _timestamp)
	{
		logte("Invalid packet : %s (expected ts : %u)", packet->Dump().CStr(), _timestamp);
		return false;
	}

	logtd("Insert packet : %s", packet->Dump().CStr());

	auto order_number = GetOrderNumber(packet->SequenceNumber());

	_packets.emplace(order_number, packet);

	// First packet
	_min_order_number = std::min(_min_order_number, order_number);
	_max_order_number = std::max(_max_order_number, order_number);

	if (packet->Marker())
	{
		_marked = true;
		_marker_sequence_number = packet->SequenceNumber();
		_marked_time = std::chrono::system_clock::now();
	}

	// Check if frame is completed
	if (_marked == true)
	{
		CheckCompleted();
	}

	return true;
}

bool RtpFrame::IsMarked()
{
	return _marked;
}

bool RtpFrame::IsCompleted()
{
	if (_completed == true)
	{
		return true;
	}

	if (_marked == true)
	{
		return CheckCompleted();
	}

	return false;
}

bool RtpFrame::CheckCompleted()
{
	// Already completed
	if (_completed == true)
	{
		return true;
	}

	if (_marked == false)
	{
		return false;
	}

	auto elapsed_ms = ov::Clock::GetElapsedMiliSecondsFromNow(_marked_time);
	if (elapsed_ms < _marker_completion_delay_ms)
	{
		// Wait for a delay to ensure that the "marked" packet was not received before the other packets.
		return false;
	}

	// Check number of packets
	uint16_t need_number_of_packets = _max_order_number - _min_order_number + 1;

	// Check if frame is valid
	if (need_number_of_packets == _packets.size())
	{
		_completed = true;
		logtd("Frame completed: timestamp(%u) packets(%u) need packets(%u)", _timestamp, _packets.size(), need_number_of_packets);
	}
	else
	{
		// Not an error, and not even unusual: this runs on every IsCompleted() call - so on
		// every received packet - for as long as a marked frame is still missing packets, which
		// is exactly the state the buffer exists to wait through. At error level a single lost
		// packet produced dozens of lines per packet received.
		logtd("Frame not complete yet: timestamp(%u) %u/%u", _timestamp, _packets.size(), need_number_of_packets);
	}

	return _completed;
}

uint64_t RtpFrame::GetElapsed()
{
	return _stop_watch.Elapsed();
}

/************************************************************************
 * 							Jitter Buffer
 ***********************************************************************/

uint64_t RtpFrameJitterBuffer::GetExtentedTimestamp(uint32_t timestamp)
{
	// If the timestamp is less than the previous timestamp, it is assumed that the timestamp has been rolled over.
	if (timestamp < _last_timestamp && _last_timestamp - timestamp > 0x80000000)
	{
		_timestamp_cycle++;
	}

	_last_timestamp = timestamp;

	return (static_cast<uint64_t>(_timestamp_cycle) << 32) | timestamp;
}

bool RtpFrameJitterBuffer::InsertPacket(const std::shared_ptr<RtpPacket>& packet)
{
	auto extended_timestamp = GetExtentedTimestamp(packet->Timestamp());
	
	// Already it determined this packet was lost
	if (extended_timestamp <= _last_extended_timestamp)
	{
		return false;  // packet of previous frame
	}

	auto it = _rtp_frames.find(extended_timestamp);
	std::shared_ptr<RtpFrame> frame;

	if (it == _rtp_frames.end())
	{
		logtd("Create frame buffer for timestamp %llu", extended_timestamp);
		// First packet received of frame (not sure is really the first packet) 
		frame = std::make_shared<RtpFrame>(packet->Timestamp());
		frame->SetMaxBufferingTime(_default_max_buffering_time_ms);
		_rtp_frames[extended_timestamp] = frame;
	}
	else
	{
		frame = it->second;
	}

	return frame->InsertPacket(packet);;
}

void RtpFrameJitterBuffer::BurnOutExpiredFrames()
{
	// printf("Check - BurnOutExpiredFrames : %d\n", _rtp_frames.size());
	while (true)
	{
		auto it = _rtp_frames.begin();
		if (it == _rtp_frames.end())
		{
			break;
		}
		auto extended_timestamp = it->first;
		auto frame = it->second;
		const uint64_t age = frame->GetElapsed();

		if (frame->IsCompleted())
		{
			break;
		}

		auto max_buffering_time_ms = frame->GetMaxBufferingTime();
		if (max_buffering_time_ms > 0 && age > max_buffering_time_ms)
		{
			logtw("Dropping expired frame - timestamp(%u) age(%llu ms)", frame->Timestamp(), age);
			_last_extended_timestamp = extended_timestamp;
			_rtp_frames.erase(it);
		}
		else 
		{
			// waiting next packet
			break;
		}
	}
}

bool RtpFrameJitterBuffer::HasAvailableFrame()
{
	BurnOutExpiredFrames();

	auto it = _rtp_frames.begin();
	if (it == _rtp_frames.end())
	{
		return false;
	}

	auto first_frame = it->second;
	return first_frame->IsCompleted();
}

std::shared_ptr<RtpFrame> RtpFrameJitterBuffer::PopAvailableFrame()
{
	if (HasAvailableFrame() == false)
	{
		return nullptr;
	}

	auto it = _rtp_frames.begin();
	auto extended_timestamp = it->first;
	auto frame = it->second;

	_last_extended_timestamp = extended_timestamp;

	logtd("Pop frame - extended(%llu) timestamp(%u) packets(%d) frames(%u)", it->first, frame->Timestamp(), frame->PacketCount(), _rtp_frames.size());

	// remove front frame
	_rtp_frames.erase(it);

	return frame;
}