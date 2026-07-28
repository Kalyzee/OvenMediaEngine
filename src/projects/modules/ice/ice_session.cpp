//==============================================================================
//
//  OvenMediaEngine
//
//  Created by Getroot
//  Copyright (c) 2023 AirenSoft. All rights reserved.
//
//==============================================================================
#include "ice_session.h"
#include "ice_private.h"

#include <limits>

// Minimum delay between two path migrations (re-nominations while already Connected).
// This prevents flapping between candidate pairs, e.g. when the peer aggressively
// nominates several pairs in a short time during the initial connection.
static constexpr int64_t ICE_PATH_MIGRATION_MIN_INTERVAL_MS = 1000;

IceSession::IceSession(session_id_t session_id, IceSession::Role role, 
				const std::shared_ptr<const SessionDescription> &local_sdp, const std::shared_ptr<const SessionDescription> &peer_sdp,
				int expired_ms, uint64_t life_time_epoch_ms, 
				std::any user_data, const std::shared_ptr<IcePortObserver> &observer)
				: _session_id(session_id),  
				_local_sdp(local_sdp), _peer_sdp(peer_sdp), _role(role),
				_expire_after_ms(expired_ms), _lifetime_epoch_ms(life_time_epoch_ms), 
				_user_data(user_data), _observer(observer)
{
	Refresh();
}

ov::String IceSession::ToString() const
{
	auto connected_candidate_pair = GetConnectedCandidatePair();
	return ov::String::FormatString("IceSession: session_id=%u, role=%s, state=%s, local_ufrag=%s, expire_after_ms=%d, lifetime_epoch_ms=%llu, ConnectedCandidatePair=%s",
		_session_id,
		_role == Role::CONTROLLED ? "CONTROLLED" : "CONTROLLING",
		IceConnectionStateToString(GetState()),
		GetLocalUfrag().CStr(),
		_expire_after_ms,
		_lifetime_epoch_ms,
		connected_candidate_pair ? connected_candidate_pair->ToString().CStr() : "None");
}

void IceSession::Refresh()
{
	// Session-level liveness: any inbound activity, on any path, postpones the passive expiry.
	// This deliberately says nothing about consent, which is decided per path.
	_expire_at_ms = ov::Clock::NowSteadyMSec() + _expire_after_ms;
}

void IceSession::Refresh(const ov::SocketAddressPair& address_pair)
{
	Refresh();

	// Consent freshness belongs to the path the traffic arrived on. Stamping the session
	// instead would let a peer probing an alternate path vouch for the nominated one, so a
	// dead nominated path would never time out - the exact Wi-Fi/4G handover case.
	auto candidate_pair = FindCandidatePair(address_pair);
	if (candidate_pair != nullptr)
	{
		candidate_pair->Refresh();
	}
}

std::shared_ptr<IceCandidatePair> IceSession::FindFreshAlternatePair(int64_t max_idle_ms) const
{
	// Released before _candidate_pairs_mutex is taken, so the two are never held together
	auto connected_candidate_pair = GetConnectedCandidatePair();

	std::shared_lock<std::shared_mutex> lock(_candidate_pairs_mutex);

	std::shared_ptr<IceCandidatePair> best;
	int64_t best_idle_ms = max_idle_ms;

	for (const auto& item : _candidate_pairs)
	{
		const auto& candidate_pair = item.second;
		if (candidate_pair == nullptr || candidate_pair == connected_candidate_pair)
		{
			continue;
		}

		// IsConnectable() is sticky: it proves the path was validated at some point, not that
		// it works now. The idle time is what tells us it is usable today, so both are required.
		if (candidate_pair->IsConnectable() == false)
		{
			continue;
		}

		auto idle_ms = candidate_pair->GetElapsedMsSinceLastReceived();
		if (idle_ms <= best_idle_ms)
		{
			best_idle_ms = idle_ms;
			best = candidate_pair;
		}
	}

	return best;
}

bool IceSession::IsExpired() const
{
	if (_lifetime_epoch_ms != 0 && _lifetime_epoch_ms < ov::Clock::NowMSec())
	{
		return true;
	}

	return ov::Clock::NowSteadyMSec() > _expire_at_ms.load();
}

void IceSession::SetState(IceConnectionState state)
{
	_state = state;
}

IceConnectionState IceSession::GetState() const
{
	return _state;
}

IceSession::Role IceSession::GetRole() const
{
	return _role;
}

std::shared_ptr<const SessionDescription> IceSession::GetLocalSdp() const
{
	return _local_sdp;
}

std::shared_ptr<const SessionDescription> IceSession::GetPeerSdp() const
{
	return _peer_sdp;
}

uint32_t IceSession::GetSessionID() const
{
	return _session_id;
}

ov::String IceSession::GetLocalUfrag() const
{
	return _local_sdp->GetIceUfrag();
}

std::shared_ptr<IcePortObserver> IceSession::GetObserver() const
{
	return _observer;
}

std::any IceSession::GetUserData() const
{
	return _user_data;
}

void IceSession::SetTurnClient(bool is_turn_client)
{
	_is_turn_client = is_turn_client;
}

bool IceSession::IsTurnClient() const
{
	return _is_turn_client;
}

// Is data channel enabled
void IceSession::SetDataChannelEnabled(bool is_data_channel_enabled)
{
	_is_data_channel_enabled = is_data_channel_enabled;	
}

bool IceSession::IsDataChannelEnabled() const
{
	return _is_data_channel_enabled;
}

// Data channel number
void IceSession::SetDataChannelNumber(uint16_t data_channel_number)
{
	_data_channle_number = data_channel_number;
}

uint16_t IceSession::GetDataChannelNumber() const
{
	return _data_channle_number;
}

// TURN peer address
void IceSession::SetTurnPeerAddress(const ov::SocketAddress& peer_address)
{
	_turn_peer_address = peer_address;
}

ov::SocketAddress IceSession::GetTurnPeerAddress() const
{
	return _turn_peer_address;
}

std::shared_ptr<IceCandidatePair> IceSession::GetConnectedCandidatePair() const
{
	std::shared_lock<std::shared_mutex> lock(_connected_candidate_pair_mutex);
	return _connected_candidate_pair;
}

std::shared_ptr<ov::Socket> IceSession::GetConnectedSocket() const
{
	auto connected_candidate_pair = GetConnectedCandidatePair();
	if (connected_candidate_pair == nullptr)
	{
		return nullptr;
	}
	
	return connected_candidate_pair->GetSocket();
}

std::shared_ptr<IceCandidatePair> IceSession::FindCandidatePair(const ov::SocketAddressPair& address_pair) const
{
	std::shared_lock<std::shared_mutex> lock(_candidate_pairs_mutex);

	auto it = _candidate_pairs.find(address_pair);
	if (it != _candidate_pairs.end())
	{
		return it->second;
	}

	return nullptr;
}

std::shared_ptr<IceCandidatePair> IceSession::CreateAndAddCandidatePair(const ov::SocketAddressPair& address_pair, const std::shared_ptr<ov::Socket>& socket)
{
	std::lock_guard<std::shared_mutex> lock(_candidate_pairs_mutex);

	auto candidate_pair = std::make_shared<IceCandidatePair>(address_pair, socket);
	_candidate_pairs.insert(std::make_pair(address_pair, candidate_pair));

	return candidate_pair;
}

void IceSession::RemoveCandidatePair(const ov::SocketAddressPair& address_pair)
{
	std::lock_guard<std::shared_mutex> lock(_candidate_pairs_mutex);

	_candidate_pairs.erase(address_pair);
}

// Candidate pairs
void IceSession::OnReceivedStunBindingRequest(const ov::SocketAddressPair& address_pair, const std::shared_ptr<ov::Socket>& socket)
{
	auto candidate_pair = FindCandidatePair(address_pair);
	if (candidate_pair == nullptr)
	{
		// new candidate pair
		candidate_pair = CreateAndAddCandidatePair(address_pair, socket);
	}

	// candidate state
	candidate_pair->OnReceivedBindingRequest();

	if (candidate_pair->GetState() == IceConnectionState::New)
	{
		candidate_pair->SetState(IceConnectionState::Checking);	

		// Global state
		if (GetState() == IceConnectionState::New)
		{
			SetState(IceConnectionState::Checking);
		}
	}
}

void IceSession::OnReceivedStunBindingResponse(const ov::SocketAddressPair& address_pair, const std::shared_ptr<ov::Socket>& socket)
{
	auto candidate_pair = FindCandidatePair(address_pair);
	if (candidate_pair == nullptr)
	{
		// new candidate pair
		candidate_pair = CreateAndAddCandidatePair(address_pair, socket);
	}

	// candidate state
	candidate_pair->OnReceivedBindingResponse();

	if (candidate_pair->GetState() == IceConnectionState::New)
	{
		candidate_pair->SetState(IceConnectionState::Checking);	

		// Global state
		if (GetState() == IceConnectionState::New)
		{
			SetState(IceConnectionState::Checking);
		}
	}
}

void IceSession::OnReceivedStunBindingErrorResponse(const ov::SocketAddressPair& address_pair, const std::shared_ptr<ov::Socket>& socket)
{
	auto candidate_pair = FindCandidatePair(address_pair);
	if (candidate_pair == nullptr)
	{
		// Nothitng to do
		return;
	}

	candidate_pair->SetState(IceConnectionState::Failed);
}

bool IceSession::IsConnectable(const ov::SocketAddressPair& address_pair)
{
	auto candidate_pair = FindCandidatePair(address_pair);
	if (candidate_pair == nullptr)
	{
		return false;
	}

	return candidate_pair->IsConnectable();
}

bool IceSession::IsConnected(const ov::SocketAddressPair& address_pair)
{
	auto connected_candidate_pair = GetConnectedCandidatePair();
	if (connected_candidate_pair == nullptr)
	{
		return false;
	}

	return connected_candidate_pair->GetAddressPair() == address_pair;
}

// USE-CANDIDATE, used for controlling role
bool IceSession::UseCandidate(const ov::SocketAddressPair& address_pair, std::shared_ptr<IceCandidatePair>* previous_candidate_pair)
{
	std::lock_guard<std::shared_mutex> lock(_connected_candidate_pair_mutex);

	// Reported under the lock, together with the mutation below
	if (previous_candidate_pair != nullptr)
	{
		*previous_candidate_pair = _connected_candidate_pair;
	}

	auto state = GetState();

	// The peer can nominate a candidate pair during the initial connection (Checking),
	// but also while we are already Connected. The latter happens when the peer's network
	// path changes (e.g. switching Wi-Fi/4G) and it re-nominates a new candidate pair with
	// the same ICE credentials. In that case we must migrate to the new path instead of
	// staying pinned to the now-dead old path.
	if (state != IceConnectionState::Checking && state != IceConnectionState::Connected)
	{
		logte("ICE session : %u | UseCandidate() | Invalid state: %s", GetSessionID(), IceConnectionStateToString(state));
		return false;
	}

	auto candidate_pair = FindCandidatePair(address_pair);
	if (candidate_pair == nullptr)
	{
		logte("ICE session : %u | UseCandidate() | No candidate pair found for address pair: %s", GetSessionID(), address_pair.ToString().CStr());
		return false;
	}

	// Already nominated to the same candidate pair : nothing to do.
	if (_connected_candidate_pair != nullptr && _connected_candidate_pair->GetAddressPair() == address_pair)
	{
		return true;
	}

	if (state == IceConnectionState::Connected)
	{
		// RFC 8445 7.3.1.5 : only a validated pair may be nominated. FindCandidatePair() resolves
		// pairs that OnReceivedStunBindingRequest() created on the fly, so without this check a
		// single binding request with a correct integrity but a spoofed source address would move
		// the media path: the real path stops being routed to this session and the media is sent
		// to the spoofed address. IsConnectable() means we both received a request on that pair
		// and got a response to our own request, which a spoofer cannot obtain.
		if (candidate_pair->IsConnectable() == false)
		{
			logtw("ICE session : %u | Refuse path migration to %s : candidate pair is not validated yet",
				  GetSessionID(), address_pair.ToString().CStr());
			return false;
		}

		// Anti-flap : do not migrate too often. This also avoids bouncing between
		// candidate pairs during the initial connection, when the peer may nominate
		// several pairs in a very short time.
		auto elapsed_ms = ov::Clock::NowSteadyMSec() - _last_connected_pair_changed_ms;
		if (elapsed_ms < ICE_PATH_MIGRATION_MIN_INTERVAL_MS)
		{
			logtd("ICE session : %u | Skip path migration to %s (only %" PRId64 " ms since last candidate pair change)",
				  GetSessionID(), address_pair.ToString().CStr(), elapsed_ms);
			return false;
		}

		// Path migration : the peer nominated a different candidate pair while we were already connected.
		logti("ICE session : %u | Path migration : %s -> %s",
			  GetSessionID(),
			  _connected_candidate_pair != nullptr ? _connected_candidate_pair->GetAddressPair().ToString().CStr() : "None",
			  address_pair.ToString().CStr());
	}

	// candidate state
	candidate_pair->SetState(IceConnectionState::Connected);
	_connected_candidate_pair = candidate_pair;
	_last_connected_pair_changed_ms = ov::Clock::NowSteadyMSec();

	// Global state
	SetState(IceConnectionState::Connected);

	return true;
}