//==============================================================================
//
//  OvenMediaEngine
//
//  Created by Getroot
//  Copyright (c) 2023 AirenSoft. All rights reserved.
//
//==============================================================================
#include "ice_candidate_pair.h"

#include <limits>

IceCandidatePair::IceCandidatePair(const ov::SocketAddressPair &pair, std::shared_ptr<ov::Socket> socket)
	: _socket_address_pair(pair), _socket(socket)
{
	// A pair is always created on inbound activity, so it starts fresh. Leaving the timestamp
	// at 0 would make a brand new pair look infinitely idle to the consent checks.
	Refresh();
}

std::shared_ptr<ov::Socket> IceCandidatePair::GetSocket() const
{
    return _socket;
}

// State management
void IceCandidatePair::SetState(IceConnectionState state)
{
    _state = state;
}

IceConnectionState IceCandidatePair::GetState() const
{
    return _state;
}

// Socket Address Pair
ov::SocketAddressPair IceCandidatePair::GetAddressPair() const
{
    return _socket_address_pair;
}

ov::String IceCandidatePair::ToString() const
{
    return ov::String::FormatString("Socket: %s SocketAddressPair: %s State: %s",
                        _socket->ToString().CStr(),
                        _socket_address_pair.ToString().CStr(),
                        IceConnectionStateToString(_state.load()));
}

void IceCandidatePair::OnReceivedBindingRequest()
{
    _received_binding_request = true;
    // Authenticated STUN traffic on this path : it is alive
    Refresh();
}

void IceCandidatePair::OnReceivedBindingResponse()
{
    _received_binding_response = true;
    Refresh();
}

// Valid candidate pair
bool IceCandidatePair::IsConnectable() const
{
    return _received_binding_request && _received_binding_response;
}

void IceCandidatePair::Refresh()
{
    _last_received_ms = ov::Clock::NowSteadyMSec();
}

int64_t IceCandidatePair::GetElapsedMsSinceLastReceived() const
{
    return ov::Clock::NowSteadyMSec() - _last_received_ms.load();
}

void IceCandidatePair::MarkConsentRequestSent()
{
    _last_consent_request_ms = ov::Clock::NowSteadyMSec();
}

int64_t IceCandidatePair::GetElapsedMsSinceLastConsentRequest() const
{
    auto last = _last_consent_request_ms.load();
    if (last == 0)
    {
        // Never probed yet : allow one immediately
        return std::numeric_limits<int64_t>::max();
    }

    return ov::Clock::NowSteadyMSec() - last;
}