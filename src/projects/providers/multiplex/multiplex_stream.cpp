//==============================================================================
//
//  MultiplexStream
//
//  Created by Getroot
//  Copyright (c) 2023 AirenSoft. All rights reserved.
//
//==============================================================================

#include "multiplex_stream.h"
#include "multiplex_private.h"

#include <base/provider/application.h>

namespace pvd
{
    // --- ABR robustness / worker-loop tuning ---------------------------------------------------
    // Bounded drain per source per loop turn (keeps one bursty source from starving the others).
    static constexpr int kMultiplexMaxDrainPerSource = 128;
    // Idle backoff so an idle/degraded channel does not busy-spin a core (mirrors upstream 6fb29554).
    static constexpr int kMultiplexIdleSleepMinMs = 1;
    static constexpr int kMultiplexIdleSleepMaxMs = 10;
    // How often to re-attempt mirroring a dropped source (fast rejoin, well under the 1s budget).
    static constexpr int kMultiplexRemirrorIntervalMs = 250;

    // Implementation of MultiplexStream
    std::shared_ptr<MultiplexStream> MultiplexStream::Create(const std::shared_ptr<Application> &application, const info::Stream &stream_info, const std::shared_ptr<MultiplexProfile> &multiplex_profile)
    {
        auto stream = std::make_shared<pvd::MultiplexStream>(application, stream_info, multiplex_profile);
        return stream;
    }

    MultiplexStream::MultiplexStream(const std::shared_ptr<Application> &application, const info::Stream &info, const std::shared_ptr<MultiplexProfile> &multiplex_profile)
        : Stream(application, info), _multiplex_profile(multiplex_profile)
    {
    }

    MultiplexStream::~MultiplexStream()
    {
        Stop();
    }

    bool MultiplexStream::Start()
    {
        // Create Worker
        _worker_thread_running = true;
        _worker_thread = std::thread(&MultiplexStream::WorkerThread, this);
        pthread_setname_np(_worker_thread.native_handle(), "Multiplex");

        return Stream::Start();
    }

    bool MultiplexStream::Stop()
    {
        // Stop and JOIN the worker BEFORE releasing taps. The Playing loop now re-Mirrors dropped
        // sources in place, so releasing first would let the still-running worker re-register a
        // just-released tap in the router (leaking it until the destructor's second release). Before
        // graceful re-bind the worker never re-mirrored during Playing, so the old order was safe.
        const bool was_running = _worker_thread_running;
        if (was_running)
        {
            _worker_thread_running = false;

            if (_worker_thread.joinable())
            {
                _worker_thread.join();
            }
        }

        ReleaseSourceStreams();

        if (was_running == false)
        {
            return true;
        }

        return Stream::Stop();
    }

    bool MultiplexStream::Terminate()
    {
        logti("Multiplex Channel : %s/%s: Terminated, it will be deleted by application", GetApplicationName(), GetName().CStr());
        ReleaseSourceStreams();

        return Stream::Terminate();
    }

    MultiplexStream::MuxState MultiplexStream::GetMuxState() const
    {
        return _mux_state;
    }

    ov::String MultiplexStream::GetMuxStateStr() const
    {
        switch (_mux_state)
        {
        case MuxState::None:
            return "None";
        case MuxState::Pulling:
            return "Pulling";
        case MuxState::Playing:
            return "Playing";
        case MuxState::Stopped:
            return "Stopped";
        }

        return "Unknown";
    }

    ov::String MultiplexStream::GetPullingStateMsg() const
    {
        return _pulling_state_msg;
    }

    std::shared_ptr<MultiplexProfile> MultiplexStream::GetProfile() const
    {
        return _multiplex_profile;
    }

    void MultiplexStream::WorkerThread()
    {
        // --- Pulling phase: block (all-or-nothing) until every source is tapped, then publish once. ---
        while (_worker_thread_running)
        {
            _mux_state = MuxState::Pulling;
            if (PullSourceStreams() == false)
            {
                // sleep and retry
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            break;
        }

        // The output stream is published with all sources live; track per-source liveness from here on.
        const auto &source_streams = _multiplex_profile->GetSourceStreams();
        _source_active.assign(source_streams.size(), true);
        auto last_remirror = std::chrono::steady_clock::now();
        int idle_sleep_ms = kMultiplexIdleSleepMinMs;

        // --- Playing phase: graceful degradation. ---
        // A single source dropping must NOT tear the channel down (the old all-or-nothing Terminate).
        // We keep the published output stream and its (frozen) tracks, freeze only the dropped
        // rendition, and re-Mirror that source in place on its EXISTING tap so viewers resume without
        // a reconnect. Survivors keep flowing throughout.
        while (_worker_thread_running)
        {
            _mux_state = MuxState::Playing;
            bool any_packet = false;

            // Pass 1 — always drain SURVIVORS first. Detection of a drop is a cheap tap-state read
            // (no orchestrator call), so a slow recovery attempt can never delay a healthy rung.
            for (size_t i = 0; i < source_streams.size(); i++)
            {
                const auto &source_stream = source_streams[i];
                auto stream_tap = source_stream->GetStreamTap();

                if (stream_tap == nullptr || stream_tap->GetState() != MediaRouterStreamTap::State::Tapped)
                {
                    // Source dropped. Do NOT Terminate — mark it inactive once; recovery is pass 2.
                    if (i < _source_active.size() && _source_active[i])
                    {
                        _source_active[i] = false;
                        logtw("Multiplex Channel : %s/%s: source [%s] dropped; serving survivors and re-mirroring in place", GetApplicationName(), GetName().CStr(), source_stream->GetUrlStr().CStr());
                    }
                    continue;
                }

                for (int drained = 0; drained < kMultiplexMaxDrainPerSource; drained++)
                {
                    auto media_packet = stream_tap->Pop(0);
                    if (media_packet == nullptr)
                    {
                        break;
                    }

                    auto source_track_id = MakeSourceTrackIdUnique(stream_tap->GetId(), media_packet->GetTrackId());
                    auto new_track_id = GetNewTrackId(source_track_id);
                    if (new_track_id == 0)
                    {
                        continue;
                    }

                    media_packet->SetTrackId(new_track_id);
                    SendFrame(media_packet);
                    any_packet = true;
                }
            }

            // Pass 2 (throttled) — re-mirror inactive sources AFTER survivors are drained, so the
            // inline orchestrator calls (CheckIfStreamExist / MirrorStream) never stall a live rung.
            const auto now = std::chrono::steady_clock::now();
            if ((now - last_remirror) >= std::chrono::milliseconds(kMultiplexRemirrorIntervalMs))
            {
                last_remirror = now;
                for (size_t i = 0; i < source_streams.size(); i++)
                {
                    if (i < _source_active.size() && _source_active[i])
                    {
                        continue;
                    }

                    const auto &source_stream = source_streams[i];
                    if (RemirrorSourceStream(source_stream))
                    {
                        RebindSourceTrackMap(source_stream);
                        if (i < _source_active.size())
                        {
                            _source_active[i] = true;
                        }
                        logti("Multiplex Channel : %s/%s: source [%s] re-mirrored; rendition resumed", GetApplicationName(), GetName().CStr(), source_stream->GetUrlStr().CStr());
                    }
                }
            }

            // Idle backoff so an all-idle / degraded channel doesn't peg a core.
            if (any_packet == false)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(idle_sleep_ms));
                idle_sleep_ms = (idle_sleep_ms * 2 > kMultiplexIdleSleepMaxMs) ? kMultiplexIdleSleepMaxMs : idle_sleep_ms * 2;
            }
            else
            {
                idle_sleep_ms = kMultiplexIdleSleepMinMs;
            }
        }

        _mux_state = MuxState::Stopped;
        logti("Multiplex Channel : %s/%s: Worker thread stopped", GetApplicationName(), GetName().CStr());
    }

    uint64_t MultiplexStream::MakeSourceTrackIdUnique(uint32_t tap_id, uint32_t track_id) const
    {
        return (static_cast<uint64_t>(tap_id) << 32) | static_cast<uint64_t>(track_id);
    }

    uint32_t MultiplexStream::GetNewTrackId(uint64_t source_track_id) const
    {
        auto it = _source_track_id_to_new_id_map.find(source_track_id);
        if (it == _source_track_id_to_new_id_map.end())
        {
            return 0;
        }

        return it->second;
    }

    bool MultiplexStream::PullSourceStreams()
    {
        auto source_streams = _multiplex_profile->GetSourceStreams();
        for (auto &source_stream : source_streams)
        {
            auto stream_tap = source_stream->GetStreamTap();
            if (stream_tap == nullptr)
            {
                continue;
            }

			stream_tap->SetNeedPastData(true);

            if (stream_tap->GetState() != MediaRouterStreamTap::State::Tapped)
            {
                auto stream_url = source_stream->GetUrl();
                auto vhost_app_name = info::VHostAppName(stream_url->Host(), stream_url->App());

                if (ocst::Orchestrator::GetInstance()->CheckIfStreamExist(vhost_app_name, stream_url->Stream()) == false)
                {
                    _pulling_state_msg = ov::String::FormatString("Multiplex Channel : %s/%s: Wait for stream %s", GetApplicationName(), GetName().CStr(), stream_url->Stream().CStr());
                    logti("%s", _pulling_state_msg.CStr());

                    return false;
                }

                auto result = ocst::Orchestrator::GetInstance()->MirrorStream(stream_tap, vhost_app_name, stream_url->Stream(), MediaRouterInterface::MirrorPosition::Outbound);

                if (result != CommonErrorCode::SUCCESS)
                {
                    _pulling_state_msg = ov::String::FormatString("Multiplex Channel : %s/%s: Failed to mirror stream %s (err : %d)", GetApplicationName(), GetName().CStr(), source_stream->GetUrlStr().CStr(), static_cast<int>(result));
                    logte("%s", _pulling_state_msg.CStr());
                    return false;
                }
            }
        }

        // Make tracks
        for (auto &source_stream : source_streams)
        {
            auto stream_tap = source_stream->GetStreamTap();
            if (stream_tap == nullptr)
            {
                continue;
            }

            auto stream_info = stream_tap->GetStreamInfo();
            if (stream_info == nullptr)
            {
                continue;
            }

            auto tracks = stream_info->GetTracks();
            for (auto &[source_track_id, source_track] : tracks)
            {
                auto source_track_name = source_track->GetVariantName();
                MultiplexProfile::NewTrackInfo new_track_info;

                if (source_stream->GetNewTrackInfo(source_track_name, new_track_info) == false)
                {
                    continue;
                }

                auto new_track = source_track->Clone();
                new_track->SetId(IssueUniqueTrackId());
                new_track->SetVariantName(new_track_info.new_track_name);
                if (new_track_info.bitrate_conf > 0)
                {
                    new_track->SetBitrateByConfig(new_track_info.bitrate_conf);
                }
                if (new_track_info.framerate_conf > 0)
                {
                    new_track->SetFrameRateByConfig(new_track_info.framerate_conf);
                }

                AddTrack(new_track);
                _source_track_id_to_new_id_map.emplace(MakeSourceTrackIdUnique(stream_tap->GetId(), source_track_id), new_track->GetId());

                logti("Multiplex Stream : %s/%s: Added track %s from %s/%s (%d)", GetApplicationName(), GetName().CStr(), new_track->GetVariantName().CStr(), source_stream->GetUrlStr().CStr(), source_track_name.CStr(), source_track_id);
            }
        }

        // Make Playlist
        auto playlists = _multiplex_profile->GetPlaylists();
        for (auto &playlist : playlists)
        {
            AddPlaylist(playlist);
        }

        // Publish stream
        if (GetApplication()->AddStream(GetSharedPtr()) == false)
        {
            logte("Multiplex Channel : %s/%s: Failed to publish stream", GetApplicationName(), GetName().CStr());
            Terminate();
            return false;
        }

        // Start all stream taps
        for (auto &source_stream : source_streams)
        {
            auto stream_tap = source_stream->GetStreamTap();
            if (stream_tap == nullptr)
            {
                continue;
            }

            stream_tap->Start();
        }

        logti("Multiplex Channel : %s/%s: Started\n%s", GetApplicationName(), GetName().CStr(), _multiplex_profile->InfoStr().CStr());

        return true;
    }

    bool MultiplexStream::ReleaseSourceStreams()
    {
        auto source_streams = _multiplex_profile->GetSourceStreams();
        for (auto &source_stream : source_streams)
        {
            auto stream_tap = source_stream->GetStreamTap();
            if (stream_tap == nullptr)
            {
                continue;
            }

            if (stream_tap->GetState() != MediaRouterStreamTap::State::Tapped)
            {
                continue;
            }

            stream_tap->Stop();

            auto result = ocst::Orchestrator::GetInstance()->UnmirrorStream(stream_tap);
            if (result != CommonErrorCode::SUCCESS)
            {
                logte("Multiplex Channel : %s/%s: Failed to unmirror stream %s (err : %d)", GetApplicationName(), GetName().CStr(), source_stream->GetUrlStr().CStr(), static_cast<int>(result));
                return false;
            }
        }

        return true;
    }

    bool MultiplexStream::RemirrorSourceStream(const std::shared_ptr<MultiplexProfile::SourceStream> &source_stream)
    {
        auto stream_tap = source_stream->GetStreamTap();
        if (stream_tap == nullptr)
        {
            return false;
        }

        // Defensive only: nothing but this worker ever sets a tap back to Tapped (the router only sets
        // UnTapped), so this is not reachable via a concurrent re-tap — kept as a cheap guard.
        if (stream_tap->GetState() == MediaRouterStreamTap::State::Tapped)
        {
            return true;
        }

        auto stream_url = source_stream->GetUrl();
        if (stream_url == nullptr)
        {
            return false;
        }
        auto vhost_app_name = info::VHostAppName(stream_url->Host(), stream_url->App());

        // The source republishes asynchronously after a drop; only mirror once it exists again.
        if (ocst::Orchestrator::GetInstance()->CheckIfStreamExist(vhost_app_name, stream_url->Stream()) == false)
        {
            return false;
        }

        // Discard packets buffered before the drop so we resume cleanly (bounded: an UnTapped tap is no
        // longer pushed to, so its buffer drains to empty). Avoids a stale-timestamp burst on resume.
        while (stream_tap->Pop(0) != nullptr)
        {
        }

        // Order matters: start the tap and arm past-data replay BEFORE it becomes Tapped, so the router's
        // first push after re-tap replays the recent GOP (resume on a keyframe). Start() is idempotent;
        // doing these after MirrorStream would race the first push and could drop the replayed keyframe.
        stream_tap->Start();
        stream_tap->SetNeedPastData(true);

        auto result = ocst::Orchestrator::GetInstance()->MirrorStream(stream_tap, vhost_app_name, stream_url->Stream(), MediaRouterInterface::MirrorPosition::Outbound);
        if (result != CommonErrorCode::SUCCESS)
        {
            // Outbound stream may not be registered yet even though the inbound exists — retry next tick.
            return false;
        }

        return (stream_tap->GetState() == MediaRouterStreamTap::State::Tapped);
    }

    void MultiplexStream::RebindSourceTrackMap(const std::shared_ptr<MultiplexProfile::SourceStream> &source_stream)
    {
        auto stream_tap = source_stream->GetStreamTap();
        if (stream_tap == nullptr)
        {
            return;
        }

        auto stream_info = stream_tap->GetStreamInfo();
        if (stream_info == nullptr)
        {
            return;
        }

        // The output track set is frozen at publish (AddStream). Re-point the routing map onto the
        // EXISTING output tracks, matched by their stable variant name — never AddTrack here.
        std::map<ov::String, uint32_t> output_id_by_name;
        for (const auto &[output_track_id, output_track] : GetTracks())
        {
            output_id_by_name.emplace(output_track->GetVariantName(), output_track_id);
        }

        // Drop this source's previous map entries (they share this tap's id) so a republished source
        // that reassigned its track ids cannot leave stale keys behind. The tap object is reused across
        // re-mirror, so its id — the high 32 bits of the key — is stable and identifies exactly this source.
        const uint64_t tap_prefix = static_cast<uint64_t>(stream_tap->GetId()) << 32;
        for (auto it = _source_track_id_to_new_id_map.begin(); it != _source_track_id_to_new_id_map.end();)
        {
            if ((it->first & 0xFFFFFFFF00000000ULL) == tap_prefix)
            {
                it = _source_track_id_to_new_id_map.erase(it);
            }
            else
            {
                ++it;
            }
        }

        int rebound = 0;
        for (const auto &[source_track_id, source_track] : stream_info->GetTracks())
        {
            MultiplexProfile::NewTrackInfo new_track_info;
            if (source_stream->GetNewTrackInfo(source_track->GetVariantName(), new_track_info) == false)
            {
                // Source track not in this source's TrackMap — not mapped by design (same as cold start).
                continue;
            }

            auto it = output_id_by_name.find(new_track_info.new_track_name);
            if (it == output_id_by_name.end())
            {
                // The output track was created at publish; if it is gone the profile changed under us.
                // Skip rather than mutate the frozen track set — but log it, since this rendition would
                // otherwise stay black/silent (GetNewTrackId -> 0 -> dropped) with no other trace.
                logtw("Multiplex Channel : %s/%s: re-bind found no output track [%s] for source [%s] track [%s] — that rendition stays unfed", GetApplicationName(), GetName().CStr(), new_track_info.new_track_name.CStr(), source_stream->GetUrlStr().CStr(), source_track->GetVariantName().CStr());
                continue;
            }

            _source_track_id_to_new_id_map[MakeSourceTrackIdUnique(stream_tap->GetId(), source_track_id)] = it->second;
            rebound++;
        }

        logti("Multiplex Channel : %s/%s: re-bound %d track(s) for source [%s]", GetApplicationName(), GetName().CStr(), rebound, source_stream->GetUrlStr().CStr());
    }
}