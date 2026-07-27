#include "rtp_minimal_jitter_buffer.h"

#define OV_LOG_TAG "RtpVideoJitterBuffer"

bool RtpMinimalJitterBuffer::InsertPacket(const std::shared_ptr<RtpPacket> &packet)
{
	// Already it determined this packet was lost.
	// The comparison must use modular (16 bits) arithmetic: a naive "<" would reject every
	// packet after a wraparound (e.g. next == 65535 lost, then 0, 1, 2... all look "older"),
	// which would leave the buffer empty forever - so the lost-packet skip in
	// PopAvailablePacket() could never trigger and the track would stall until the sequence
	// number came back around.
	// _first_packet must be checked first: _next_sequence_number is still 0 at that point,
	// so any initial sequence number in the upper half would look negative.
	if (_first_packet == false && static_cast<int16_t>(packet->SequenceNumber() - _next_sequence_number) < 0)
	{
		return false;
	}

	_rtp_packets.emplace(packet->SequenceNumber(), std::make_shared<RtpPacketBox>(ov::Clock::NowMSec(), packet));
	return true;
}

bool RtpMinimalJitterBuffer::HasAvailablePacket()
{
	if(_rtp_packets.size() <= 0)
	{
		return false;
	}

	return true;
}

std::shared_ptr<RtpPacket> RtpMinimalJitterBuffer::PopAvailablePacket()
{
	std::shared_ptr<RtpPacketBox> packet_box = nullptr;

	// First packet
	if(_first_packet)
	{
		_first_packet = false;
		auto it = _rtp_packets.begin();
		if(it == _rtp_packets.end())
		{
			return nullptr;
		}
		packet_box = it->second;
		_rtp_packets.erase(it);
	}
	else
	{
		auto it = _rtp_packets.find(_next_sequence_number);
		// There is no next packet
		if(it == _rtp_packets.end())
		{
			// Check the first packet in the buffer
			auto front_it = _rtp_packets.begin();
			if(front_it == _rtp_packets.end())
			{
				return nullptr;
			}

			// Check the time spent in the buffer
			packet_box = front_it->second;
			
			// If next of next packet is Available and wait for 1/2 buffering time in buffer
			if(ov::Clock::NowMSec() - packet_box->_packaging_time_ms > _max_buffering_time_ms / 2)
			{
				// It is determined that the next packet is lost.
				_rtp_packets.erase(front_it);
			}
			// Wait a little more 
			else
			{
				return nullptr;
			}
		}
		else
		{
			// There is a next packet
			packet_box = it->second;
			_rtp_packets.erase(it);
		}
	}

	auto packet = packet_box->_packet;
	_next_sequence_number = packet->SequenceNumber() + 1;
	return packet;
}