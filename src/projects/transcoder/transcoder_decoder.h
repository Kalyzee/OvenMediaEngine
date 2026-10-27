//==============================================================================
//
//  OvenMediaEngine
//
//  Created by Hyunjun Jang
//  Copyright (c) 2018 AirenSoft. All rights reserved.
//
//==============================================================================
#pragma once

#include "base/info/stream.h"
#include "codec/codec_base.h"

class TranscodeDecoder : public TranscodeBase<MediaPacket, MediaFrame>
{
public:
	typedef std::function<void(TranscodeResult, int32_t, std::shared_ptr<MediaFrame>)> CompleteHandler;

public:
	TranscodeDecoder(info::Stream stream_info);
	~TranscodeDecoder() override;

	static std::shared_ptr<std::vector<std::shared_ptr<CodecCandidate>>> GetCandidates(bool hwaccels_enable, ov::String hwaccles_moduels, std::shared_ptr<MediaTrack> track);
	static std::shared_ptr<TranscodeDecoder> Create(int32_t decoder_id, const info::Stream &info, std::shared_ptr<MediaTrack> track, std::shared_ptr<std::vector<std::shared_ptr<CodecCandidate>>> candidates, CompleteHandler complete_handler);

	void SetDecoderId(int32_t decoder_id);
	
	bool Configure(std::shared_ptr<MediaTrack> track) override;

	void SendBuffer(std::shared_ptr<const MediaPacket> packet) override;
	void Complete(TranscodeResult result, std::shared_ptr<MediaFrame> frame);
	
	std::shared_ptr<MediaTrack> &GetRefTrack();

	cmn::Timebase GetTimebase();

	virtual void CodecThread() = 0;

	virtual void Stop();

	void SetCompleteHandler(CompleteHandler complete_handler)
	{
		_complete_handler = move(complete_handler);
	}

protected:
	// FFmpeg parsers may read past the end of their input (up to AV_INPUT_BUFFER_PADDING_SIZE bytes).
	// Packet buffers carry no padding, so the input is copied into a zero-padded buffer before parsing.
	// The returned pointer stays valid until the next call.
	const uint8_t *GetPaddedParserInput(const uint8_t *data, size_t size)
	{
		_parser_input.resize(size + AV_INPUT_BUFFER_PADDING_SIZE);
		::memcpy(_parser_input.data(), data, size);
		::memset(_parser_input.data() + size, 0, AV_INPUT_BUFFER_PADDING_SIZE);

		return _parser_input.data();
	}

	int32_t _decoder_id;
	int32_t _gpu_id = -1;

	std::shared_ptr<MediaTrack> _track;

	AVCodecContext *_context = nullptr;
	AVCodecParserContext *_parser = nullptr;
	AVCodecParameters *_codec_par = nullptr;

	std::vector<uint8_t> _parser_input;

	bool _change_format = false;

	AVPacket *_pkt;
	AVFrame *_frame;

	info::Stream _stream_info;

	bool _kill_flag = false;
	std::thread _codec_thread;

	CompleteHandler _complete_handler;
};
