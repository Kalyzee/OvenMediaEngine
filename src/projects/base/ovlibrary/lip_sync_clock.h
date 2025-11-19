#pragma once

#include "base/ovlibrary/ovlibrary.h"

class LipSyncClock
{
public:
	LipSyncClock() = default;

	bool RegisterRtpClock(uint32_t id, double timebase);

	std::optional<uint64_t> CalcPTS(uint32_t id, uint32_t rtp_timestamp);
	bool UpdateSenderReportTime(uint32_t id, uint32_t ntp_msw, uint32_t ntp_lsw, uint32_t rtcp_timestamp);

	bool IsEnabled() {return _enabled;}
	bool RtpClockIsReady(uint32_t id);

private:
	struct Clock
	{
		std::shared_mutex _clock_lock;
		bool		_updated = false;
		double		_timebase = 0;
		uint32_t	_last_rtcp_timestamp = 0;
		uint64_t	_extended_rtcp_timestamp = 0;
		uint32_t 	_last_rtp_timestamp = 0;
		uint64_t	_extended_rtp_timestamp = 0;
		uint64_t	_pts = 0;	// converted NTP timestamp to timebase timestamp
		bool _first_pts = true;
		bool _first_sr = true;
		bool _ready = false;
		uint64_t _adjust_pts = 0;
		uint64_t	_offset_pts = 0;
	};

	// Id, Clock
	std::map<uint32_t, std::shared_ptr<Clock>> _clock_map;

	bool _enabled = false;

	std::shared_ptr<Clock> GetClock(uint32_t id);
};