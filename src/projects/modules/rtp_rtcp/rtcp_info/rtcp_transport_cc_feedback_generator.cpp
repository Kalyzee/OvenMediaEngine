//==============================================================================
//
//  OvenMediaEngine
//
//  Created by Getroot
//  Copyright (c) 2023 AirenSoft. All rights reserved.
//
//==============================================================================

#include "rtcp_transport_cc_feedback_generator.h"

#include "../rtp_header_extension/rtp_header_extension_transport_cc.h"

#define OV_LOG_TAG "transport-cc"

RtcpTransportCcFeedbackGenerator::RtcpTransportCcFeedbackGenerator(uint8_t extension_id, uint32_t sender_ssrc)
{
	_extension_id = extension_id;
	_sender_ssrc = sender_ssrc;
	_created_time = std::chrono::system_clock::now();
	_last_report_time = _created_time;
}

bool RtcpTransportCcFeedbackGenerator::AddReceivedRtpPacket(const std::shared_ptr<RtpPacket>& packet)
{
	auto wide_sequence_number_opt = packet->GetExtension<uint16_t>(_extension_id);
	if (wide_sequence_number_opt.has_value() == false)
	{
		// There is no transport-wide sequence number in the RTP header extension
		static int log_times = 10;
		if (log_times > 0)
		{
			logtw("AddReceivedRtpPacket: There is no transport-wide sequence number in the RTP header extension : %s", packet->Dump().CStr());
			log_times--;
		}
		return false;
	}

	// Read transport-wide sequence number
	auto wide_sequence_number = wide_sequence_number_opt.value();

	logtd("AddReceivedRtpPacket: wide_seq(%u) %s", wide_sequence_number, packet->Dump().CStr());

	// A reordered or retransmitted packet may belong to a feedback that is already stopped and
	// still waiting to be sent. A stopped feedback covers exactly its own reported range,
	// [base, base + status_count - 1]; the modular offset keeps the test correct across the
	// 16 bits rollover and includes the base itself (which may be a packet reported as lost
	// and retransmitted afterwards).
	auto transport_cc = _transport_cc;
	for (const auto& curr : _last_transport_ccs)
	{
		uint16_t offset = wide_sequence_number - curr->GetBaseSequenceNumber();
		if (offset < curr->GetPacketStatusCount())
		{
			// complete transport_cc buffered
			transport_cc = curr;
			break;
		}
	}

	// first packet of feedback message
	if (transport_cc == nullptr)
	{
		transport_cc = CreateTransportCc(wide_sequence_number);
		_transport_cc = transport_cc;
	}
	auto now = std::chrono::system_clock::now();
	transport_cc->AddPacketFeedbackInfo(std::make_shared<TransportCc::PacketFeedbackInfo>(wide_sequence_number, true, GetTime(now)));
	transport_cc->SetMediaSsrc(packet->Ssrc());

	_last_rtp_received_time = now;

	if (wide_sequence_number >= _last_wide_sequence_number)
	{
		auto last_wide_sequence_number_roll_over = wide_sequence_number - _last_wide_sequence_number > 0x8000;
		_last_wide_sequence_number = last_wide_sequence_number_roll_over ? _last_wide_sequence_number : wide_sequence_number;
	}
	else if (_last_wide_sequence_number > wide_sequence_number) 
	{
		// Roll over
		auto wide_sequence_number_roll_over = _last_wide_sequence_number - wide_sequence_number > 0x8000;
		_last_wide_sequence_number = wide_sequence_number_roll_over ? wide_sequence_number : _last_wide_sequence_number;
	}

	return true;
}

bool RtcpTransportCcFeedbackGenerator::HasElapsedSinceLastTransportCc(uint32_t milliseconds)
{
	auto now = std::chrono::system_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - _last_report_time).count();

	if (elapsed >= milliseconds)
	{
		return true;
	}

	return false;
}

std::shared_ptr<TransportCc> RtcpTransportCcFeedbackGenerator::PopAvailableTransportCc()
{
	// Oldest first: receivers use the feedback packet count to detect lost feedbacks, so
	// emitting them out of order would look like feedback loss to the remote estimator.
	for (size_t i = 0; i < _last_transport_ccs.size(); ++i)
	{
		auto transport_cc = _last_transport_ccs[i];
		if (transport_cc->AllPacketsReceived() || transport_cc->GetElasped() > TRANSPORT_CC_MAX_BUFFERING_TIME_MS)
		{
			_last_transport_ccs.erase(_last_transport_ccs.begin() + i);
			return transport_cc;
		}
	}
	return nullptr;
}

bool RtcpTransportCcFeedbackGenerator::StopCurrentTransportCc()
{
	if (_transport_cc == nullptr)
	{
		return false;
	}
	_transport_cc->Stop();
	_last_transport_ccs.push_back(_transport_cc);
	_transport_cc = nullptr;

	// The cycle restarts as soon as a feedback is closed, not when it is finally sent.
	// Otherwise a feedback held back in the buffer (waiting for a missing packet) would leave
	// HasElapsedSinceLastTransportCc() permanently true, and every single incoming RTP packet
	// would open a feedback, immediately close it and send it - one RTCP packet per RTP packet.
	_last_report_time = std::chrono::system_clock::now();

	return true;
}

std::shared_ptr<TransportCc> RtcpTransportCcFeedbackGenerator::CreateTransportCc(uint16_t wide_sequence_number)
{
	auto now = std::chrono::system_clock::now();
	auto transport_cc = std::make_shared<TransportCc>();
	auto reference_time_us = GetTime(now);

	transport_cc->SetSenderSsrc(_sender_ssrc);
	transport_cc->SetFeedbackPacketCount(_fb_pkt_count);
	_fb_pkt_count++;
	transport_cc->SetReferenceTimeUs(reference_time_us);

	// Base sequence number
	uint16_t base_sequence_number = 0;
	if (_is_first_packet == true)
	{
		_is_first_packet = false;
		base_sequence_number = wide_sequence_number;
	}
	else
	{
		if (wide_sequence_number != static_cast<uint16_t>(_last_wide_sequence_number + 1))
		{
			logtw("wide sequence number is not continuous : %u -> %u", _last_wide_sequence_number, wide_sequence_number);
		}

		base_sequence_number = _last_wide_sequence_number + 1;
	}

	transport_cc->SetBaseSequenceNumber(base_sequence_number);

	return transport_cc;
}

std::shared_ptr<RtcpPacket> RtcpTransportCcFeedbackGenerator::GenerateTransportCcMessage(std::shared_ptr<TransportCc> transport_cc)
{
	if (transport_cc == nullptr)
	{
		return nullptr;
	}

	logtd("Generate Transport CC message : Sender SSRC(%u), Media SSRC(%u), Base Sequence Number(%u), Reference Time(%u), Packet Feedback Count(%u)",
		  transport_cc->GetSenderSsrc(), transport_cc->GetMediaSsrc(), transport_cc->GetBaseSequenceNumber(), transport_cc->GetReferenceTime(), transport_cc->GetPacketStatusCount());

	auto rtcp_packet = std::make_shared<RtcpPacket>();
	transport_cc->CalculeDeltas();
	rtcp_packet->Build(transport_cc);

	// _last_report_time is not touched here: the cycle boundary is when the feedback is closed
	// (see StopCurrentTransportCc), otherwise the time spent buffering would stretch the cycle.

	return rtcp_packet;
}