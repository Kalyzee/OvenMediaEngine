#include "rtp_history.h"

RtpHistory::RtpHistory(uint8_t origin_payload_type, uint8_t rtx_payload_type, uint32_t rtx_ssrc, uint32_t max_history_size)
{
	_origin_paylod_type = origin_payload_type;
	_rtx_paylod_type = rtx_payload_type;
	_rtx_ssrc = rtx_ssrc;
	_max_history_size = max_history_size;

	_history.reserve(_max_history_size);
}

// Converting to RtxRtpPacket
bool RtpHistory::StoreRtpPacket(const std::shared_ptr<RtpPacket> &packet)
{
	std::lock_guard<std::shared_mutex> guard(_history_lock);
	auto sequence_number = packet->SequenceNumber();
	auto timestamp = packet->Timestamp();
	if (packet->IsKeyframe())
	{
		if (!_key_frame_stored || _last_key_frame_timestamp != timestamp)
		{
			_key_frame_stored = true;
			_last_key_frame_first_sequence_number = sequence_number;
			_last_key_frame_timestamp = timestamp;
		}
	}
	else if (_key_frame_stored && _last_key_frame_timestamp == timestamp)
	{
		_key_frame_stored = false;
	}

	// The history is a ring buffer: once the distance from the keyframe exceeds its
	// capacity, the keyframe packets start being overwritten and cannot be resent
	if (_key_frame_stored && static_cast<uint16_t>(sequence_number - _last_key_frame_first_sequence_number) >= _max_history_size)
	{
		_key_frame_stored = false;
	}

	_last_sequence_number = sequence_number;
	_history[GetIndex(sequence_number)] = packet;

	return true;
}

std::shared_ptr<RtxRtpPacket> RtpHistory::GetRtxRtpPacket(uint16_t seq_no)
{
	auto index = GetIndex(seq_no);
	// First, look in the cache
	std::shared_lock<std::shared_mutex> cache_read_guard(_history_cache_lock);
	auto cache_item = _history_cache.find(index);
	if(cache_item != _history_cache.end())
	{
		// Found!
		auto rtx_packet = cache_item->second;
		
		if(rtx_packet->GetOriginalSequenceNumber() == seq_no)// && ov::Clock::GetElapsedMiliSecondsFromNow(rtx_packet->GetCreatedTime()) < VALID_TIME_MS_STORED_RTP_PACKET)
		{
			return rtx_packet;
		}
	}
	cache_read_guard.unlock();

	auto rtp_packet = GetRtpPacket(seq_no);
	if (rtp_packet != nullptr)
	{
		// Create Rtx Packet and store it
		auto rtx_packet = std::make_shared<RtxRtpPacket>(GetRtxSsrc(), GetRtxPayloadType(), *rtp_packet);

		std::lock_guard<std::shared_mutex> cache_write_guard(_history_cache_lock);
		_history_cache[index] = rtx_packet;
		
		return rtx_packet;
	}
	return nullptr;
}

std::shared_ptr<RtpPacket> RtpHistory::GetRtpPacket(uint16_t seq_no)
{
	auto index = GetIndex(seq_no);

	// find in rtp history
	std::shared_lock<std::shared_mutex> history_guard(_history_lock);
	auto rtp_item = _history.find(index);
	if(rtp_item != _history.end())
	{
		auto rtp_packet = rtp_item->second;
		history_guard.unlock();

		// now, I consider all requests are valid because webrtc player doesn't ask for too old packet anyway 
		//auto elapsed_ms = ov::Clock::GetElapsedMiliSecondsFromNow(rtp_packet->GetCreatedTime());
		//if(elapsed_ms < VALID_TIME_MS_STORED_RTP_PACKET)
		if(rtp_packet->SequenceNumber() == seq_no)
		{			
			return rtp_packet;
		}
	}

	return nullptr;
}

uint8_t	RtpHistory::GetOriginPayloadType()
{
	return _origin_paylod_type;
}

uint32_t RtpHistory::GetRtxSsrc()
{
	return _rtx_ssrc;
}

uint8_t RtpHistory::GetRtxPayloadType()
{
	return _rtx_paylod_type;
}

uint16_t RtpHistory::GetIndex(uint16_t seq_no)
{
	return seq_no % _max_history_size;
}

bool RtpHistory::GetCatchUpRange(uint16_t &start_seq_no, uint16_t &end_seq_no) const
{
	std::shared_lock<std::shared_mutex> guard(_history_lock);
	if (_key_frame_stored == false)
	{
		return false;
	}

	start_seq_no = _last_key_frame_first_sequence_number;
	end_seq_no = _last_sequence_number;
	return true;
}
