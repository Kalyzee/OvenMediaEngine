#pragma once

#include "base/ovlibrary/ovlibrary.h"
#include "rtp_packet.h"
#include <unordered_map>

#define DEFAULT_VIDEO_SLIDES_MAX_BUFFERING_TIME_MS	1500
#define DEFAULT_VIDEO_MAX_BUFFERING_TIME_MS	500	
#define DEFAULT_MARKER_COMPLETION_DELAY_MS	30

// RTP Packet Group by Frame
class RtpFrame
{
public:
	RtpFrame(uint32_t timestamp);
	bool InsertPacket(const std::shared_ptr<RtpPacket> &packet);
	bool IsCompleted();
	bool IsMarked();
	uint64_t GetElapsed();

	std::shared_ptr<RtpPacket> GetFirstRtpPacket();
	std::shared_ptr<RtpPacket> GetNextRtpPacket();

	uint32_t Timestamp(){return _timestamp;}
	size_t PacketCount(){return _packets.size();}
	uint32_t MarkerSequenceNumber(){return _marker_sequence_number;}

	uint32_t GetMaxBufferingTime(){return _max_buffering_time_ms;}
	void SetMaxBufferingTime(uint32_t max_buffering_time_ms)
	{
		_max_buffering_time_ms = max_buffering_time_ms;
	}

private:
	bool CheckCompleted();
	uint16_t GetOrderNumber(uint16_t sequence_number);

	uint32_t _marker_completion_delay_ms = DEFAULT_MARKER_COMPLETION_DELAY_MS;


	ov::StopWatch _stop_watch;

	uint32_t	_timestamp = 0;
	
	uint32_t	_marker_sequence_number = 0;
	
	// marked time
	std::chrono::system_clock::time_point _marked_time;
	bool		_marked = false;
	bool		_completed = false;

	uint16_t 	_first_sequence_number = 0;
	bool		_first_packet = true;
	uint16_t 	_base_order_number = 0;

	uint16_t 	_curr_order_number = 0;
	uint16_t 	_min_order_number = 65535;
	uint16_t 	_max_order_number = 0;

	uint32_t _max_buffering_time_ms = 0;


	// Order Number : RtpPacket
	std::unordered_map<uint16_t, std::shared_ptr<RtpPacket>> _packets;
};

// A jitter buffer for a media stream in the form that the frame is fragmented
// and the rtp marker bit indicates that it is the last fragment.
class RtpFrameJitterBuffer
{
public:
	bool InsertPacket(const std::shared_ptr<RtpPacket> &packet);
	bool HasAvailableFrame();
	std::shared_ptr<RtpFrame> PopAvailableFrame();

	void SetMaxBufferingTime(uint32_t default_max_buffering_time_ms)
	{
		_default_max_buffering_time_ms = default_max_buffering_time_ms;
	}

private:	
	void BurnOutExpiredFrames();

	uint64_t GetExtentedTimestamp(uint32_t timestamp);

	uint32_t _last_timestamp = 0;
	uint32_t _timestamp_cycle = 0;
	uint64_t _last_extended_timestamp = 0;
	uint32_t _default_max_buffering_time_ms = DEFAULT_VIDEO_MAX_BUFFERING_TIME_MS;


	// timestamp : RtpFrameInfo
	// it should be ordered, so use std::map
	std::map<uint64_t, std::shared_ptr<RtpFrame>> _rtp_frames;
};