#include "lip_sync_clock.h"
#include "base/ovlibrary/clock.h"
#define OV_LOG_TAG "LipSyncClock"

bool LipSyncClock::RegisterRtpClock(uint32_t id, double timebase)
{
	auto clock = std::make_shared<Clock>();
	clock->_timebase = timebase;
	_clock_map.emplace(id, clock);

	return true;
}

std::shared_ptr<LipSyncClock::Clock> LipSyncClock::GetClock(uint32_t id)
{
	if(_clock_map.find(id) == _clock_map.end())
	{
		return nullptr;
	}
	return _clock_map[id];
}

bool LipSyncClock::RtpClockIsReady(uint32_t id)
{
	auto clock = GetClock(id);
	return clock != nullptr && clock->_updated;
}

std::optional<uint64_t> LipSyncClock::CalcPTS(uint32_t id, uint32_t rtp_timestamp)
{
	auto clock = GetClock(id);
	if(clock == nullptr)
	{
		return {};
	}

	int64_t delta = 0;
	if (clock->_first_pts)
	{
		clock->_first_pts = false;
		clock->_first_packet_time = std::chrono::system_clock::now();
		clock->_extended_rtp_timestamp = rtp_timestamp;
		clock->_first_extended_rtp_timestamp = clock->_extended_rtp_timestamp;
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
				logtw("RTP timestamp is not monotonic: %u -> %u delta: %ld", clock->_last_rtp_timestamp, rtp_timestamp, delta);
			}
		}

		clock->_extended_rtp_timestamp += delta;
	}

	clock->_last_rtp_timestamp = rtp_timestamp;


	std::shared_lock<std::shared_mutex> lock(clock->_clock_lock);

		
	uint64_t final_pts = 0;
	if (_first_clock == nullptr)
	{
		// can not happen
		return {};
	}

	if (clock->_updated)
	{
		if (!clock->_ready)
		{
			// calculate constant with first RTCP SR
			clock->_offset_pts = clock->_extended_rtcp_timestamp - clock->_first_extended_rtp_timestamp;
			clock->_adjust_pts = clock->_pts;
			clock->_ready= true;
		}
		auto pts = clock->_pts + ((int64_t)clock->_extended_rtp_timestamp - (int64_t)clock->_extended_rtcp_timestamp);
		final_pts = clock->_offset_pts + pts - clock->_adjust_pts;
	}
	else
	{
		final_pts = clock->_extended_rtp_timestamp - clock->_first_extended_rtp_timestamp;
	}

	if (_first_clock != clock)
	{
		// delta with the first clock (time between first packet of first clock and first packet of this clock)
		int64_t delta_ms = 0;
		if (_first_clock->_ready && clock->_ready)
		{
			// use RTCP SR to calculate delta
			auto start_time = (clock->_adjust_pts - clock->_offset_pts) * clock->_timebase;
			auto start_time_first_clock = (_first_clock->_adjust_pts - _first_clock->_offset_pts) * _first_clock->_timebase;
			delta_ms = (start_time - start_time_first_clock) * 1000.0; // to ms
		}
		else
		{
			// use local time to calculate delta
			delta_ms = std::chrono::duration_cast<std::chrono::milliseconds>(clock->_first_packet_time - _first_clock->_first_packet_time).count();
		}
		final_pts += delta_ms / (clock->_timebase * 1000.0);
	}

	// printf("Final PTS : %u %ld %d %d\n", id, final_pts, clock->_ready, clock->_updated);

	logtd("Calc PTS : id(%u) final_pts(%lld) last_rtp_timestamp(%u) rtp_timestamp(%u) delta(%u) extended_rtp_timestamp(%llu)", id, final_pts, clock->_last_rtp_timestamp, rtp_timestamp, delta, clock->_extended_rtp_timestamp);

	return final_pts; 
}

bool LipSyncClock::UpdateSenderReportTime(uint32_t id, uint32_t ntp_msw, uint32_t ntp_lsw, uint32_t rtcp_timestamp)
{
	auto clock = GetClock(id);
	if(clock == nullptr)
	{
		return false;
	}

	// OBS WHIP incorrectly sends RTP Timestamp with 0xFFFFFFFF in the first SR. Below is the code to avoid this.
	if (rtcp_timestamp == 0xFFFFFFFF)
	{
		return false;
	}

	printf("RTCP SR : %u %u\n", id, rtcp_timestamp);
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
				logtw("RTCP timestamp is not monotonic: %u -> %u delta: %u", clock->_last_rtcp_timestamp, rtcp_timestamp, delta);
			}
		}

		clock->_extended_rtcp_timestamp += delta;
	}

	auto ntp = ov::Converter::NtpTsToSeconds(ntp_msw, ntp_lsw);
	clock->_last_rtcp_timestamp = rtcp_timestamp;
	clock->_pts = ntp / clock->_timebase;

	logtd("Update SR : id(%u) NTP(%u/%u) pts(%lld) rtp timestamp(%u) extended timestamp (%llu)", 
			id, ntp_msw, ntp_lsw, clock->_pts, clock->_last_rtcp_timestamp, clock->_extended_rtcp_timestamp);

	return true;
}