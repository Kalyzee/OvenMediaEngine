//==============================================================================
//
//  OvenMediaEngine
//
//  Created by Getroot
//  Copyright (c) 2023 AirenSoft. All rights reserved.
//
//==============================================================================
#pragma once

#include <atomic>

#include <base/ovlibrary/ovlibrary.h>
#include <base/ovsocket/ovsocket.h>
#include "ice_types.h"

class IceCandidatePair
{
public:
	IceCandidatePair(const ov::SocketAddressPair &pair, std::shared_ptr<ov::Socket> socket);

	std::shared_ptr<ov::Socket> GetSocket() const;

	// State management
	void SetState(IceConnectionState state);
	IceConnectionState GetState() const;

	// Socket Address Pair
    ov::SocketAddressPair GetAddressPair() const;

	ov::String ToString() const;

    void OnReceivedBindingRequest();
    void OnReceivedBindingResponse();

    // Valid candidate pair.
    // Note this is sticky: it says the path was validated once, not that it is alive now.
    // Combine it with GetElapsedMsSinceLastReceived() to know whether it is usable today.
    bool IsConnectable() const;

	// Consent freshness is tracked per path, because that is what ICE and RFC 7675 reason
	// about. Tracking it per session would let traffic arriving on any path vouch for the
	// nominated one - precisely wrong during a Wi-Fi/4G handover, where the peer probes its
	// new path while the nominated one is already dead.
	void Refresh();
	int64_t GetElapsedMsSinceLastReceived() const;

	void MarkConsentRequestSent();
	int64_t GetElapsedMsSinceLastConsentRequest() const;

private:

	// Atomic: written by the ICE receive threads (one per local port), read by the timer
	// thread through IsConnectable() / ToString() / the consent checks.
	std::atomic<IceConnectionState> _state { IceConnectionState::New };
    ov::SocketAddressPair _socket_address_pair;
	std::shared_ptr<ov::Socket> _socket = nullptr;

    std::atomic<bool> _received_binding_request { false };
    std::atomic<bool> _received_binding_response { false };

	// Steady-clock milliseconds; only differences are meaningful
	std::atomic<int64_t> _last_received_ms { 0 };
	std::atomic<int64_t> _last_consent_request_ms { 0 };
};