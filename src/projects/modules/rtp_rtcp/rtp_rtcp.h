#pragma once

#include "base/info/media_track.h"
#include "base/ovlibrary/node.h"
#include "rtcp_info/receiver_report.h"
#include "rtcp_info/rtcp_sr_generator.h"
#include "rtcp_info/rtcp_transport_cc_feedback_generator.h"
#include "rtcp_info/sdes.h"
#include "rtp_frame_jitter_buffer.h"
#include "rtp_minimal_jitter_buffer.h"
#include "rtp_packetizer.h"
#include "rtp_receive_statistics.h"
#include "rtp_rtcp_defines.h"

#define RECEIVER_REPORT_CYCLE_MS 500
#define SDES_CYCLE_MS 500

class RtpRtcpInterface : public ov::EnableSharedFromThis<RtpRtcpInterface>
{
public:
	virtual void OnRtpFrameReceived(const std::vector<std::shared_ptr<RtpPacket>>& rtp_packets) = 0;
	virtual void OnRtcpReceived(const std::shared_ptr<RtcpInfo>& rtcp_info) = 0;
};

class RtpRtcp : public ov::Node
{
public:
	RtpRtcp(const std::shared_ptr<RtpRtcpInterface>& observer);
	~RtpRtcp() override;

	bool AddRtpSender(uint8_t payload_type, uint32_t ssrc, uint32_t codec_rate, ov::String cname);
	bool AddRtpReceiver(uint32_t track_id, const std::shared_ptr<MediaTrack>& track);
	bool Stop() override;

	bool SendRtpPacket(const std::shared_ptr<RtpPacket>& packet);
	bool SendPLI(uint32_t media_ssrc);
	bool SendFIR(uint32_t media_ssrc);
	bool SendNACK(uint32_t media_ssrc, const std::vector<uint16_t>& lost_sequences);

	bool IsTransportCcFeedbackEnabled(uint32_t ssrc);
	bool EnableTransportCcFeedback(uint32_t ssrc, uint8_t extension_id);
	void DisableTransportCcFeedback(uint32_t ssrc);
	bool SetContentMediaType(uint32_t ssrc, ov::String content);

	// These functions help the next node to not have to parse the packet again.
	// Because next node receives raw data format.
	std::shared_ptr<RtpPacket> GetLastSentRtpPacket();
	std::shared_ptr<RtcpPacket> GetLastSentRtcpPacket();

	// Implement Node Interface
	bool OnDataReceivedFromPrevNode(NodeType from_node, const std::shared_ptr<ov::Data>& data) override;
	bool OnDataReceivedFromNextNode(NodeType from_node, const std::shared_ptr<const ov::Data>& data) override;

private:
	struct RtpRtcpSsrcInfo 
	{
		uint32_t ssrc = 0;
		bool transport_cc_feedback_enabled = false;
		uint8_t transport_cc_feedback_extension_id = 0;
		std::shared_ptr<RtcpSRGenerator> rtcp_sr_generator;
		std::shared_ptr<RtpReceiveStatistics> receive_statistic;
		ov::String content;
	};
	bool OnRtpReceived(NodeType from_node, const std::shared_ptr<const ov::Data>& data);
	bool OnRtcpReceived(NodeType from_node, const std::shared_ptr<const ov::Data>& data);

	// Lookup only, returns nullptr when the SSRC has never been registered.
	RtpRtcpSsrcInfo* GetSsrcInfo(uint32_t ssrc);
	// Inserts a new entry when the SSRC is unknown. Only for the paths that legitimately
	// discover an SSRC (local sender registration, first received RTP packet, SDP negotiation).
	RtpRtcpSsrcInfo* GetOrCreateSsrcInfo(uint32_t ssrc);

	std::shared_mutex _state_lock;
	std::shared_ptr<RtpRtcpInterface> _observer;
	std::shared_ptr<Sdes> _sdes = nullptr;
	std::shared_ptr<RtcpPacket> _rtcp_sdes = nullptr;
	ov::StopWatch _rtcp_send_stop_watch;
	uint64_t _rtcp_sent_count = 0;

	// Guards the structure of _ssrc_map. The RtpRtcpSsrcInfo* returned by GetSsrcInfo() /
	// GetOrCreateSsrcInfo() stays usable after the lock is released because std::unordered_map
	// is node-based: rehashing invalidates iterators but never references to elements, and
	// entries are only ever removed in the destructor.
	// Lock ordering: always _state_lock (if held) then _ssrc_map_lock, never the opposite.
	std::shared_mutex _ssrc_map_lock;
	std::unordered_map<uint32_t, RtpRtcpSsrcInfo> _ssrc_map;

	// Transport-cc feedback
	std::shared_ptr<RtcpTransportCcFeedbackGenerator> _transport_cc_generator = nullptr;

	// Jitter buffer
	// track id : Jitter buffer
	// The key must be 32 bits wide: the WebRTC provider uses the SSRC as track id (the RTSP one
	// uses the channel id), and a uint8_t key silently truncated it - two SSRCs sharing their
	// low byte would collide and overwrite each other's track.
	std::unordered_map<uint32_t, std::shared_ptr<RtpFrameJitterBuffer>> _rtp_frame_jitter_buffers;
	std::unordered_map<uint32_t, std::shared_ptr<RtpMinimalJitterBuffer>> _rtp_minimal_jitter_buffers;

	// track id : MediaTrack Info
	std::unordered_map<uint32_t, std::shared_ptr<MediaTrack>> _tracks;
	bool _video_receiver_enabled = false;
	bool _audio_receiver_enabled = false;

	// Latest packet
	std::shared_ptr<RtpPacket> _last_sent_rtp_packet = nullptr;
	std::shared_ptr<RtcpPacket> _last_sent_rtcp_packet = nullptr;
};
