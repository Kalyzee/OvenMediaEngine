//==============================================================================
//
//  OvenMediaEngine
//
//  Created by Getroot
//  Copyright (c) 2022 AirenSoft. All rights reserved.
//
//==============================================================================
#pragma once

#include <base/info/vhost_app_name.h>
#include "../../protocol/web_socket/web_socket_frame.h"
#include "../http_exchange.h"
#include "web_socket_response.h"

#define WEBSOCKET_PING_INTERVAL_MS 20 * 1000

namespace http
{
	namespace svr
	{
		namespace ws
		{
			typedef int ws_session_info_id;
			// For WebSocket
			class WebSocketSession : public HttpExchange
			{
			public:
				struct WebSocketSesssionInfo
				{
					WebSocketSesssionInfo(const info::VHostAppName &vhost_app_name,
										  const ov::String &host_name, const ov::String &app_name, const ov::String &stream_name,
										  ws_session_info_id id, std::shared_ptr<ov::Url> &uri)
						: vhost_app_name(vhost_app_name),
						  host_name(host_name),
						  app_name(app_name),
						  stream_name(stream_name),
						  id(id),
						  uri(uri)
					{
					}

					// Ex) #default#app_name (host_name + app_name, airensoft.com + app)
					info::VHostAppName vhost_app_name;

					// Ex) airensoft.com
					ov::String host_name;
					// Ex) app
					ov::String app_name;
					// Ex) stream
					ov::String stream_name;

					ws_session_info_id id;

					// Uri
					std::shared_ptr<ov::Url> uri;

					// For User Data
					// key : data<int or string>
					std::map<ov::String, std::variant<bool, uint64_t, ov::String>> _data_map;

					void AddUserData(ov::String key, std::variant<bool, uint64_t, ov::String> value)
					{
						_data_map.emplace(key, value);
					}

					std::tuple<bool, std::variant<bool, uint64_t, ov::String>> GetUserData(ov::String key)
					{
						if (_data_map.find(key) == _data_map.end())
						{
							return {false, false};
						}

						return {true, _data_map[key]};
					}

					// Extra
					std::any _extra;
					template <typename T>
					void SetExtra(std::shared_ptr<T> extra)
					{
						_extra = std::move(extra);
					}

					std::any GetExtra() const
					{
						return _extra;
					}

					template <typename T>
					std::shared_ptr<T> GetExtraAs() const
					{
						try
						{
							return std::any_cast<std::shared_ptr<T>>(_extra);
						}
						catch ([[maybe_unused]] const std::bad_any_cast &e)
						{
						}

						return nullptr;
					}
				};

				friend class HttpConnection;

				// Only can be created by upgrade
				WebSocketSession(const std::shared_ptr<HttpConnection> &connection) = delete;
				WebSocketSession(const std::shared_ptr<HttpExchange> &exchange);
				virtual ~WebSocketSession() = default;

				// Go Upgrade
				bool Upgrade();

				// Ping
				bool Ping();

				bool OnFrameReceived(const std::shared_ptr<const prot::ws::Frame> &frame);

				std::shared_ptr<WebSocketResponse> GetWebSocketResponse() const;

				// Implement HttpExchange
				std::shared_ptr<HttpRequest> GetRequest() const override;
				std::shared_ptr<HttpResponse> GetResponse() const override;

				bool AddClient(std::shared_ptr<WebSocketSesssionInfo> client);
				std::shared_ptr<WebSocketSesssionInfo> GetFirstClient();
				std::shared_ptr<WebSocketSesssionInfo> GetClient(ws_session_info_id id);
				std::vector<std::shared_ptr<WebSocketSesssionInfo>> GetClients();
				void DeleteClient(ws_session_info_id id);

				bool multiple_clients = false;

			private:
				std::map<ws_session_info_id, std::shared_ptr<WebSocketSesssionInfo>> _client_list;
				std::shared_mutex _client_list_mutex;

				std::shared_ptr<const ov::Data> _ping_data;
				ov::StopWatch _ping_timer;

				std::shared_ptr<HttpRequest> _request;
				std::shared_ptr<WebSocketResponse> _ws_response;
			};
		}  // namespace ws
	}  // namespace svr
}  // namespace http