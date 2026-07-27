//==============================================================================
//
//  OvenMediaEngine
//
//  Created by Getroot
//  Copyright (c) 2023 AirenSoft. All rights reserved.
//
//==============================================================================
#pragma once

#include <base/ovlibrary/ovlibrary.h>

#include "../rtp_packet.h"
#include "transport_cc.h"

#define TRANSPORT_CC_CYCLE_MS 50
#define TRANSPORT_CC_MAX_BUFFERING_TIME_MS 10

// https://datatracker.ietf.org/doc/html/draft-holmer-rmcat-transport-wide-cc-extensions-01

// RTP header extension format
//  0                   1                   2                   3
//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |       0xBE    |    0xDE       |           length=1            |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |  ID   | L=1   |transport-wide sequence number | zero padding  |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

// RTCP Message Format
//  0                   1                   2                   3
//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |V=2|P|  FMT=15 |    PT=205     |           length              |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |                     SSRC of packet sender                     |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |                      SSRC of media source                     |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |      base sequence number     |      packet status count      |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |                 reference time                | fb pkt. count |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |          packet chunk         |         packet chunk          |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// .                                                               .
// .                                                               .
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |         packet chunk          |  recv delta   |  recv delta   |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// .                                                               .
// .                                                               .
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |           recv delta          |  recv delta   | zero padding  |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class RtcpTransportCcFeedbackGenerator
{
public:
	RtcpTransportCcFeedbackGenerator(uint8_t extension_id, uint32_t _sender_ssrc);

	bool AddReceivedRtpPacket(const std::shared_ptr<RtpPacket>& packet);
	bool HasElapsedSinceLastTransportCc(uint32_t milliseconds);
	std::shared_ptr<TransportCc> PopAvailableTransportCc();
	bool StopCurrentTransportCc();
	std::shared_ptr<RtcpPacket> GenerateTransportCcMessage(std::shared_ptr<TransportCc> transport_cc);

	// Elapsed microseconds since the generator was created. This is the single reference point
	// shared by TransportCc::_reference_time_us and PacketFeedbackInfo::_received_time_us.
	// It must stay in plain microseconds: the wire encoding divides by 250us (recv deltas) and
	// by 64000us (reference time), and those conversions are done by TransportCc itself.
	int64_t GetTime(std::chrono::system_clock::time_point time) const
	{
		return std::chrono::duration_cast<std::chrono::microseconds>(time - _created_time).count();
	}

private:
	std::shared_ptr<TransportCc> CreateTransportCc(uint16_t wide_sequence_number);

	std::chrono::high_resolution_clock::time_point _created_time;
	uint8_t _extension_id = 0;
	uint32_t _sender_ssrc = 0;

	bool _is_first_packet = true;
	uint16_t _last_wide_sequence_number = 0;

	uint8_t _fb_pkt_count = 0;

	std::chrono::high_resolution_clock::time_point _last_rtp_received_time;
	std::shared_ptr<TransportCc> _transport_cc = nullptr;
	std::vector<std::shared_ptr<TransportCc>> _last_transport_ccs;

	std::chrono::high_resolution_clock::time_point _last_report_time;
};