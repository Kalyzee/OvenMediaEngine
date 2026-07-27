#pragma once

#include "base/ovlibrary/ovlibrary.h"

// Two tracks of the same stream never start more than a few seconds apart. An inter-track offset
// beyond this is a computation artefact rather than a real desynchronisation, and is discarded.
#define LIP_SYNC_MAX_OFFSET_MS 10000

// The inter-track offset is applied progressively, by at most 1/N of the media progress per
// packet, so that correcting it never makes the PTS go backwards or stall.
#define LIP_SYNC_OFFSET_SLEW_DIVIDER 8

class LipSyncClock
{
public:
	LipSyncClock() = default;

	bool RegisterRtpClock(uint32_t id, double timebase);

	// Returns no value when the id is unknown; the caller must then drop the frame rather than
	// publish it with a default timestamp.
	std::optional<int64_t> CalcPTS(uint32_t id, uint32_t rtp_timestamp);
	bool UpdateSenderReportTime(uint32_t id, uint32_t ntp_msw, uint32_t ntp_lsw, uint32_t rtcp_timestamp);

	bool IsEnabled() {return _enabled;}

private:
	struct Clock
	{
		enum class OffsetState
    {
        NOT_CALCULATED,
        TEMPORARY_VALUE,
        FINAL_VALUE,
        // Both SRs arrived but the resulting offset was not credible. Terminal, like
        // FINAL_VALUE: the inputs never change afterwards, so recomputing it on every packet
        // would only produce the same value and flood the log.
        ABANDONED,
    };

		std::shared_mutex _clock_lock;
		bool		_updated = false;
		double		_timebase = 0;
		uint32_t	_last_rtcp_timestamp = 0;
		uint64_t	_extended_rtcp_timestamp = 0;
		uint32_t 	_last_rtp_timestamp = 0;
		uint64_t	_extended_rtp_timestamp = 0;
		uint64_t	_first_extended_rtp_timestamp = 0;
		int64_t		_pts = 0;	// converted NTP timestamp to timebase timestamp
		bool _first_pts = true;
		bool _first_sr = true;
		std::chrono::system_clock::time_point _first_packet_time;
		bool _ready = false;
		int64_t _adjust_pts = 0;
		int64_t _offset_pts = 0;		 // offset with the first clock, as currently applied
		int64_t _offset_pts_target = 0;	 // offset with the first clock, to converge to
		OffsetState _offset_state = OffsetState::NOT_CALCULATED;

	};

	// Guards _clock_map and _first_clock.
	// Lock ordering: Clock::_clock_lock then _lock, never the opposite (GetClock() releases
	// _lock before its caller takes a clock lock).
	std::shared_mutex _lock;

	// Id, Clock
	std::map<uint32_t, std::shared_ptr<Clock>> _clock_map;

	bool _enabled = false;
	// The track whose first packet was processed first; every other track is aligned on it
	std::shared_ptr<Clock> _first_clock;

	std::shared_ptr<Clock> GetClock(uint32_t id);
};