//==============================================================================
//
//  OvenMediaEngine
//
//  Created by Getroot
//  Copyright (c) 2022 AirenSoft. All rights reserved.
//
//==============================================================================
#include "web_socket_session.h"

#include "../http_connection.h"

#define OV_LOG_TAG "WebSocket"

namespace http
{
	using namespace prot::ws;

	namespace svr
	{
		namespace ws
		{
			// For Upgrade
			WebSocketSession::WebSocketSession(const std::shared_ptr<HttpExchange> &exchange)
				: HttpExchange(exchange)
			{
				ov::String str("OvenMediaEngine");
				_ping_data = str.ToData(false);

				_request = exchange->GetRequest();
				_request->SetConnectionType(ConnectionType::WebSocket);

				_ws_response = std::make_shared<WebSocketResponse>(exchange->GetResponse());
			}

			bool WebSocketSession::AddClient(std::shared_ptr<WebSocketSession::WebSocketSesssionInfo> client)
			{
				auto lock_guard = std::lock_guard(_client_list_mutex);
				if (multiple_clients == false && _client_list.size() >= 1)
				{
					return false;
				}
				_client_list[client->id] = client;
				return true;
			}

			std::shared_ptr<WebSocketSession::WebSocketSesssionInfo> WebSocketSession::GetFirstClient()
			{
				auto lock_guard = std::lock_guard(_client_list_mutex);
				auto it = _client_list.begin();
				if (it == _client_list.end())
				{
					return nullptr;
				}
				return it->second;
			}

			std::shared_ptr<WebSocketSession::WebSocketSesssionInfo> WebSocketSession::GetClient(ws_session_info_id id)
			{
				auto lock_guard = std::lock_guard(_client_list_mutex);
				auto it = _client_list.find(id);
				if (it == _client_list.end())
				{
					return nullptr;
				}
				return it->second;
			}

			std::vector<std::shared_ptr<WebSocketSession::WebSocketSesssionInfo>> WebSocketSession::GetClients()
			{
				std::vector<std::shared_ptr<WebSocketSession::WebSocketSesssionInfo>> clients;
				for (const auto &pair : _client_list)
				{
					clients.push_back(pair.second);
				}
				return clients;
			}

			void WebSocketSession::DeleteClient(ws_session_info_id id)
			{
				auto lock_guard = std::lock_guard(_client_list_mutex);
				_client_list.erase(id);
			}

			// Go Upgrade
			bool WebSocketSession::Upgrade()
			{
				// Find Interceptor
				auto interceptor = GetConnection()->FindInterceptor(GetSharedPtr());
				if (interceptor == nullptr)
				{
					SetStatus(Status::Error);
					GetResponse()->SetStatusCode(StatusCode::NotFound);
					GetResponse()->Response();
					return false;
				}

				interceptor->OnRequestPrepared(GetSharedPtr());

				SetStatus(HttpExchange::Status::Exchanging);

				_ping_timer.Start();

				return true;
			}

			std::shared_ptr<WebSocketResponse> WebSocketSession::GetWebSocketResponse() const
			{
				return _ws_response;
			}

			// Implement HttpExchange
			std::shared_ptr<HttpRequest> WebSocketSession::GetRequest() const
			{
				return _request;
			}
			std::shared_ptr<HttpResponse> WebSocketSession::GetResponse() const
			{
				return _ws_response;
			}

			bool WebSocketSession::Ping()
			{
				if (_ping_timer.IsElapsed(WEBSOCKET_PING_INTERVAL_MS) == false)
				{
					// If the timer is not expired, do not send ping
					return true;
				}

				_ping_timer.Update();

				return _ws_response->Send(_ping_data, FrameOpcode::Ping) > 0;
			}

			bool WebSocketSession::OnFrameReceived(const std::shared_ptr<const prot::ws::Frame> &frame)
			{
				auto interceptor = GetConnection()->FindInterceptor(GetSharedPtr());
				if (interceptor == nullptr)
				{
					SetStatus(Status::Error);
					return false;
				}
				const std::shared_ptr<const ov::Data> payload = frame->GetPayload();

				switch (static_cast<FrameOpcode>(frame->GetHeader().opcode))
				{
					case FrameOpcode::ConnectionClose:
						// The client requested close the connection
						logtd("Client requested close connection: reason:\n%s", payload->Dump("Reason").CStr());
						interceptor->OnRequestCompleted(GetSharedPtr());
						SetStatus(HttpExchange::Status::Completed);
						return true;

					case FrameOpcode::Ping:
						logtd("A ping frame is received:\n%s", payload->Dump().CStr());
						// Send a pong frame to the client
						_ws_response->Send(payload, FrameOpcode::Pong);
						break;

					case FrameOpcode::Pong:
						// Ignore pong frame
						logtd("A pong frame is received:\n%s", payload->Dump().CStr());
						break;

					default:
						logtd("%s:\n%s", frame->ToString().CStr(), payload->Dump("Frame", 0L, 1024L, nullptr).CStr());
						if (interceptor->OnDataReceived(GetSharedPtr(), payload) == false)
						{
							SetStatus(Status::Error);
							return false;
						}
						break;
				}

				return true;
			}
		}  // namespace ws
	}  // namespace svr
}  // namespace http