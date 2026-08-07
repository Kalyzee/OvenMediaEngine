//==============================================================================
//
//  MultiplexStream
//
//  Created by Getroot
//  Copyright (c) 2023 AirenSoft. All rights reserved.
//
//==============================================================================
#pragma once

#include <modules/ffmpeg/ffmpeg_conv.h>
#include <orchestrator/orchestrator.h>
#include <mediarouter/mediarouter_stream_tap.h>
#include <base/provider/stream.h>

#include "multiplex_profile.h"

namespace pvd
{
    class MultiplexStream : public Stream
    {
    public:
        static std::shared_ptr<MultiplexStream> Create(const std::shared_ptr<Application> &application, const info::Stream &stream_info, const std::shared_ptr<MultiplexProfile> &multiplex_profile);

        explicit MultiplexStream(const std::shared_ptr<Application> &application, const info::Stream &info, const std::shared_ptr<MultiplexProfile> &multiplex_profile);
        ~MultiplexStream();

        bool Start() override;
        bool Stop() override;
        bool Terminate() override;

        std::shared_ptr<MultiplexProfile> GetProfile() const;

        enum class MuxState
        {
            None,
            Pulling,
            Playing,
            Stopped,
        };

        MuxState GetMuxState() const;
        ov::String GetMuxStateStr() const;
        ov::String GetPullingStateMsg() const;

    private:
        void WorkerThread();

        bool PullSourceStreams();
        bool ReleaseSourceStreams();

        // ABR robustness: re-Mirror a single dropped source in place (reusing its existing tap, so the
        // tap id and the routing-map keys stay valid) and re-point its map entries onto the already
        // published output tracks — instead of Terminating the whole channel. Worker-thread only.
        bool RemirrorSourceStream(const std::shared_ptr<MultiplexProfile::SourceStream> &source_stream);
        void RebindSourceTrackMap(const std::shared_ptr<MultiplexProfile::SourceStream> &source_stream);

        uint64_t MakeSourceTrackIdUnique(uint32_t tap_id, uint32_t track_id) const;
        uint32_t GetNewTrackId(uint64_t source_track_id) const;

        std::shared_ptr<MultiplexProfile> _multiplex_profile;

        std::map<uint64_t, uint32_t> _source_track_id_to_new_id_map;

        // Per-source liveness, parallel to _multiplex_profile->GetSourceStreams(); worker-thread-owned.
        // A source whose tap goes untapped is marked inactive and re-mirrored in place (graceful
        // degradation) rather than tearing the whole multiplex channel down.
        std::vector<bool> _source_active;

        std::thread _worker_thread;
        bool _worker_thread_running = false;

        MuxState _mux_state = MuxState::None;
        ov::String _pulling_state_msg;
    };
}