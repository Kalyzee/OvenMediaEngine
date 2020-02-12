//==============================================================================
//
//  OvenMediaEngine
//
//  Created by Hyunjun Jang
//  Copyright (c) 2019 AirenSoft. All rights reserved.
//
//==============================================================================
#include "orchestrator.h"
#include "orchestrator_private.h"

#include <base/media_route/media_route_interface.h>
#include <base/provider/stream.h>
#include <media_router/media_router.h>
#include <monitoring/monitoring.h>

bool Orchestrator::ApplyForVirtualHost(const std::shared_ptr<VirtualHost> &virtual_host)
{
	auto succeeded = true;

	logtd("Trying to apply new configuration of VirtualHost: %s...", virtual_host->name.CStr());

	if (virtual_host->state == ItemState::Delete)
	{
		logtd("VirtualHost is deleted");

		// Delete all apps that were created by this VirtualHost
		for (auto app_item : virtual_host->app_map)
		{
			auto &app_info = app_item.second->app_info;

			auto result = DeleteApplicationInternal(app_info);

			if (result != Result::Succeeded)
			{
				logte("Could not delete application: %s", app_info.GetName().CStr());
				succeeded = false;
			}
		}
	}
	else
	{
		logtd("VirtualHost is changed");

		// Check if there is a deleted domain, and then check if there are any apps created by that domain.
		auto &domain_list = virtual_host->domain_list;

		for (auto domain_item = domain_list.begin(); domain_item != domain_list.end();)
		{
			switch (domain_item->state)
			{
				default:
					// This situation should never happen here
					OV_ASSERT2(false);
					logte("Invalid domain state: %s, %d", domain_item->name.CStr(), domain_item->state);
					++domain_item;
					break;

				case ItemState::Applied:
				case ItemState::NotChanged:
				case ItemState::New:
					// Nothing to do
					logtd("Domain is not changed/just created: %s", domain_item->name.CStr());
					++domain_item;
					break;

				case ItemState::NeedToCheck:
					// This item was deleted because it was never processed in the above process
				case ItemState::Changed:
				case ItemState::Delete:
					logtd("Domain is changed/deleted: %s", domain_item->name.CStr());
					for (auto &stream_item : domain_item->stream_map)
					{
						auto &stream = stream_item.second;

						if (stream->is_valid)
						{
							logti("Trying to stop stream [%s]...", stream->full_name.CStr());

							if (stream->provider->StopStream(stream->app_info, stream->provider_stream) == false)
							{
								logte("Failed to stop stream [%s] in provider: %s", stream->full_name.CStr(), GetOrchestratorModuleTypeName(stream->provider->GetModuleType()));
							}

							stream->is_valid = false;
						}
					}

					domain_item = domain_list.erase(domain_item);
					break;
			}
		}

		// Check if there is a deleted origin, and then check if there are any apps created by that origin.
		auto &origin_list = virtual_host->origin_list;

		for (auto origin_item = origin_list.begin(); origin_item != origin_list.end();)
		{
			switch (origin_item->state)
			{
				default:
					// This situation should never happen here
					logte("Invalid origin state: %s, %d", origin_item->location.CStr(), origin_item->state);
					++origin_item;
					break;

				case ItemState::Applied:
				case ItemState::NotChanged:
				case ItemState::New:
					// Nothing to do
					logtd("Origin is not changed/just created: %s", origin_item->location.CStr());
					++origin_item;
					break;

				case ItemState::NeedToCheck:
					// This item was deleted because it was never processed in the above process
				case ItemState::Changed:
				case ItemState::Delete:
					logtd("Origin is changed/deleted: %s", origin_item->location.CStr());
					for (auto &stream_item : origin_item->stream_map)
					{
						auto &stream = stream_item.second;

						if (stream->is_valid)
						{
							logti("Trying to stop stream [%s]...", stream->full_name.CStr());

							if (stream->provider->StopStream(stream->app_info, stream->provider_stream) == false)
							{
								logte("Failed to stop stream [%s] in provider: %s", stream->full_name.CStr(), GetOrchestratorModuleTypeName(stream->provider->GetModuleType()));
							}

							stream->is_valid = false;
						}
					}

					origin_item = origin_list.erase(origin_item);
					break;
			}
		}
	}

	return succeeded;
}

bool Orchestrator::ApplyOriginMap(const std::vector<info::Host> &host_list)
{
	bool result = true;
	std::lock_guard<decltype(_virtual_host_map_mutex)> lock_guard(_virtual_host_map_mutex);

	// Mark all items as NeedToCheck
	for (auto &vhost_item : _virtual_host_map)
	{
		if (vhost_item.second->MarkAllAs(ItemState::Applied, ItemState::NeedToCheck) == false)
		{
			logtd("Something was wrong with VirtualHost: %s", vhost_item.second->name.CStr());

			OV_ASSERT2(false);
			result = false;
		}
	}

	logtd("- Processing for VirtualHosts");

	for (auto &host_info : host_list)
	{
		auto previous_vhost_item = _virtual_host_map.find(host_info.GetName());

		if (previous_vhost_item == _virtual_host_map.end())
		{
			logtd("  - %s: New", host_info.GetName().CStr());
			auto vhost = std::make_shared<VirtualHost>(host_info);
			
			vhost->name = host_info.GetName();

			logtd("    - Processing for domains: %d items", host_info.GetDomain().GetNameList().size());

			for (auto &domain_name : host_info.GetDomain().GetNameList())
			{
				logtd("      - %s: New", domain_name.GetName().CStr());
				vhost->domain_list.emplace_back(domain_name.GetName());
			}

			logtd("    - Processing for origins: %d items", host_info.GetOriginList().size());

			for (auto &origin_config : host_info.GetOriginList())
			{
				logtd("      - %s: New (%zu urls)",
					  origin_config.GetLocation().CStr(),
					  origin_config.GetPass().GetUrlList().size());

				vhost->origin_list.emplace_back(origin_config);
			}

			_virtual_host_map[host_info.GetName()] = vhost;
			_virtual_host_list.push_back(vhost);

			continue;
		}

		logtd("  - %s: Not changed", host_info.GetName().CStr());

		// Check the previous VirtualHost item
		auto &vhost = previous_vhost_item->second;

		logtd("    - Processing for domains");
		auto new_state_for_domain = ProcessDomainList(&(vhost->domain_list), host_info.GetDomain());
		logtd("    - Processing for origins");
		auto new_state_for_origin = ProcessOriginList(&(vhost->origin_list), host_info.GetOrigins());

		if ((new_state_for_domain == ItemState::NotChanged) && (new_state_for_origin == ItemState::NotChanged))
		{
			vhost->state = ItemState::NotChanged;
		}
		else
		{
			vhost->state = ItemState::Changed;
		}
	}

	// Dump all items
	for (auto vhost_item = _virtual_host_list.begin(); vhost_item != _virtual_host_list.end();)
	{
		auto &vhost = *vhost_item;

		switch (vhost->state)
		{
			case ItemState::Unknown:
			case ItemState::Applied:
			case ItemState::Delete:
			default:
				// This situation should never happen here
				logte("  - %s: Invalid VirtualHost state: %d", vhost->name.CStr(), vhost->state);
				result = false;
				OV_ASSERT2(false);

				// Delete the invalid item
				vhost_item = _virtual_host_list.erase(vhost_item);
				_virtual_host_map.erase(vhost->name);

				vhost->MarkAllAs(ItemState::Delete);
				ApplyForVirtualHost(vhost);

				break;

			case ItemState::NeedToCheck:
				// This item was deleted because it was never processed in the above process
				logtd("  - %s: Deleted", vhost->name.CStr());

				vhost_item = _virtual_host_list.erase(vhost_item);
				_virtual_host_map.erase(vhost->name);

				vhost->MarkAllAs(ItemState::Delete);
				ApplyForVirtualHost(vhost);

				break;

			case ItemState::NotChanged:
			case ItemState::New:
				// Nothing to do
				vhost->MarkAllAs(ItemState::Applied);
				++vhost_item;
				break;

			case ItemState::Changed:
				++vhost_item;

				if (ApplyForVirtualHost(vhost))
				{
					vhost->MarkAllAs(ItemState::Applied);
				}

				break;
		}
	}

	logtd("All items are applied");

	return result;
}

Orchestrator::ItemState Orchestrator::ProcessDomainList(std::vector<Domain> *domain_list, const cfg::Domain &domain_config) const
{
	bool is_changed = false;

	// TODO(dimiden): Is there a way to reduce the cost of O(n^2)?
	for (auto &domain_name : domain_config.GetNameList())
	{
		auto name = domain_name.GetName();
		bool found = false;

		for (auto &domain : *domain_list)
		{
			if (domain.state == ItemState::NeedToCheck)
			{
				if (domain.name == name)
				{
					domain.state = ItemState::NotChanged;
					found = true;
					break;
				}
			}
		}

		if (found == false)
		{
			logtd("      - %s: New", domain_name.GetName().CStr());
			// Adding items here causes unnecessary iteration in the for statement above
			// To avoid this, we need to create a separate list for each added item
			domain_list->push_back(name);
			is_changed = true;
		}
	}

	if (is_changed == false)
	{
		// There was no new item

		// Check for deleted items
		for (auto &domain : *domain_list)
		{
			switch (domain.state)
			{
				case ItemState::NeedToCheck:
					// This item was deleted because it was never processed in the above process
					logtd("      - %s: Deleted", domain.name.CStr());
					domain.state = ItemState::Delete;
					is_changed = true;
					break;

				case ItemState::NotChanged:
					// Nothing to do
					logtd("      - %s: Not changed", domain.name.CStr());
					break;

				case ItemState::Unknown:
				case ItemState::Applied:
				case ItemState::Changed:
				case ItemState::New:
				case ItemState::Delete:
				default:
					// This situation should never happen here
					OV_ASSERT2(false);
					is_changed = true;
					break;
			}
		}
	}

	return is_changed ? ItemState::Changed : ItemState::NotChanged;
}

Orchestrator::ItemState Orchestrator::ProcessOriginList(std::vector<Origin> *origin_list, const cfg::Origins &origins_config) const
{
	bool is_changed = false;

	// TODO(dimiden): Is there a way to reduce the cost of O(n^2)?
	for (auto &origin_config : origins_config.GetOriginList())
	{
		bool found = false;

		for (auto &origin : *origin_list)
		{
			if (origin.state == ItemState::NeedToCheck)
			{
				if (origin.location == origin_config.GetLocation())
				{
					auto &prev_pass_config = origin.origin_config.GetPass();
					auto &new_pass_config = origin_config.GetPass();

					if (prev_pass_config.GetScheme() != new_pass_config.GetScheme())
					{
						logtd("      - %s: Changed (Scheme does not the same: %s != %s)", origin.location.CStr(), prev_pass_config.GetScheme().CStr(), new_pass_config.GetScheme().CStr());
						origin.state = ItemState::Changed;
					}
					else
					{
						auto &first_url_list = new_pass_config.GetUrlList();
						auto &second_url_list = prev_pass_config.GetUrlList();

						bool is_equal = std::equal(
							first_url_list.begin(), first_url_list.end(), second_url_list.begin(),
							[&origin](const cfg::Url &url1, const cfg::Url &url2) -> bool {
								bool result = url1.GetUrl() == url2.GetUrl();

								if (result == false)
								{
									logtd("      - %s: Changed (URL does not the same: %s != %s)", origin.location.CStr(), url1.GetUrl().CStr(), url2.GetUrl().CStr());
								}

								return result;
							});

						origin.state = (is_equal == false) ? ItemState::Changed : ItemState::NotChanged;
					}

					if (origin.state == ItemState::Changed)
					{
						is_changed = true;
					}

					found = true;
					break;
				}
			}
		}

		if (found == false)
		{
			// Adding items here causes unnecessary iteration in the for statement above
			// To avoid this, we need to create a separate list for each added item
			logtd("      - %s: New (%zd urls)", origin_config.GetLocation().CStr(), origin_config.GetPass().GetUrlList().size());
			origin_list->push_back(origin_config);
			is_changed = true;
		}
	}

	if (is_changed == false)
	{
		// There was no new item

		// Check for deleted items
		for (auto &origin : *origin_list)
		{
			switch (origin.state)
			{
				case ItemState::NeedToCheck:
					// This item was deleted because it was never processed in the above process
					logtd("      - %s: Deleted", origin.location.CStr());
					origin.state = ItemState::Delete;
					is_changed = true;
					break;

				case ItemState::NotChanged:
					logtd("      - %s: Not changed (%zu)", origin.location.CStr(), origin.url_list.size());
					// Nothing to do
					break;

				case ItemState::Unknown:
				case ItemState::Applied:
				case ItemState::Changed:
				case ItemState::New:
				case ItemState::Delete:
				default:
					// This situation should never happen here
					OV_ASSERT2(false);
					is_changed = true;
					break;
			}
		}
	}

	return is_changed ? ItemState::Changed : ItemState::NotChanged;
}

bool Orchestrator::RegisterModule(const std::shared_ptr<OrchestratorModuleInterface> &module)
{
	if (module == nullptr)
	{
		return false;
	}

	auto type = module->GetModuleType();

	// Check if module exists
	std::lock_guard<decltype(_module_list_mutex)> lock_guard(_module_list_mutex);

	for (auto &info : _module_list)
	{
		if (info.module == module)
		{
			if (info.type == type)
			{
				logtw("%s module (%p) is already registered", GetOrchestratorModuleTypeName(type), module.get());
			}
			else
			{
				logtw("The module type was %s (%p), but now %s", GetOrchestratorModuleTypeName(info.type), module.get(), GetOrchestratorModuleTypeName(type));
			}

			OV_ASSERT2(false);
			return false;
		}
	}

	_module_list.emplace_back(type, module);
	auto &list = _module_map[type];
	list.push_back(module);

	if (module->GetModuleType() == OrchestratorModuleType::MediaRouter)
	{
		auto media_router = std::dynamic_pointer_cast<MediaRouter>(module);

		OV_ASSERT2(media_router != nullptr);

		_media_router = media_router;
	}

	logtd("%s module (%p) is registered", GetOrchestratorModuleTypeName(type), module.get());

	return true;
}

bool Orchestrator::UnregisterModule(const std::shared_ptr<OrchestratorModuleInterface> &module)
{
	if (module == nullptr)
	{
		OV_ASSERT2(module != nullptr);
		return false;
	}

	std::lock_guard<decltype(_module_list_mutex)> lock_guard(_module_list_mutex);

	for (auto info = _module_list.begin(); info != _module_list.end(); ++info)
	{
		if (info->module == module)
		{
			_module_list.erase(info);
			logtd("%s module (%p) is unregistered", GetOrchestratorModuleTypeName(info->type), module.get());
			return true;
		}
	}

	logtw("%s module (%p) not found", GetOrchestratorModuleTypeName(module->GetModuleType()), module.get());
	OV_ASSERT2(false);

	return false;
}

ov::String Orchestrator::GetVhostNameFromDomain(const ov::String &domain_name)
{
	// TODO(dimiden): I think it would be nice to create a VHost cache for performance

	if (domain_name.IsEmpty() == false)
	{
		std::lock_guard<decltype(_virtual_host_map_mutex)> lock_guard(_virtual_host_map_mutex);

		// Search for the domain corresponding to domain_name

		// CAUTION: This code is important to order, so don't use _virtual_host_map
		for (auto &vhost_item : _virtual_host_list)
		{
			for (auto &domain_item : vhost_item->domain_list)
			{
				if (std::regex_match(domain_name.CStr(), domain_item.regex_for_domain))
				{
					return vhost_item->name;
				}
			}
		}
	}

	return "";
}

ov::String Orchestrator::ResolveApplicationName(const ov::String &vhost_name, const ov::String &app_name)
{
	// Replace all # to _
	return ov::String::FormatString("#%s#%s", vhost_name.Replace("#", "_").CStr(), app_name.Replace("#", "_").CStr());
}

ov::String Orchestrator::ResolveApplicationNameFromDomain(const ov::String &domain_name, const ov::String &app_name)
{
	auto vhost_name = GetVhostNameFromDomain(domain_name);

	if (vhost_name.IsEmpty())
	{
		logtw("Could not find VirtualHost for domain: %s", domain_name.CStr());
	}

	auto resolved = ResolveApplicationName(vhost_name, app_name);

	logtd("Resolved application name: %s (from domain: %s, app: %s)", resolved.CStr(), domain_name.CStr(), app_name.CStr());

	return std::move(resolved);
}

info::application_id_t Orchestrator::GetNextAppId()
{
	return _last_application_id++;
}

std::shared_ptr<pvd::Provider> Orchestrator::GetProviderForScheme(const ov::String &scheme)
{
	auto lower_scheme = scheme.LowerCaseString();

	logtd("Obtaining ProviderType for scheme %s...", scheme.CStr());

	ProviderType type;

	if (lower_scheme == "rtmp")
	{
		type = ProviderType::Rtmp;
	}
	else if (lower_scheme == "rtsp")
	{
		type = ProviderType::Rtsp;
	}
	else if (lower_scheme == "rtspc")
	{
		type = ProviderType::RtspPull;
	}
	else if (lower_scheme == "ovt")
	{
		type = ProviderType::Ovt;
	}
	else
	{
		logte("Could not find a provider for scheme [%s]", scheme.CStr());
		return nullptr;
	}

	// Find the provider
	std::shared_ptr<OrchestratorProviderModuleInterface> module = nullptr;

	for (auto info = _module_list.begin(); info != _module_list.end(); ++info)
	{
		if (info->type == OrchestratorModuleType::Provider)
		{
			auto module = std::dynamic_pointer_cast<OrchestratorProviderModuleInterface>(info->module);
			auto provider = std::dynamic_pointer_cast<pvd::Provider>(module);

			if (provider == nullptr)
			{
				OV_ASSERT(provider != nullptr, "Provider must inherit from pvd::Provider");
				continue;
			}

			if (provider->GetProviderType() == type)
			{
				return provider;
			}
		}
	}

	logtw("Provider (%d) is not found for scheme %s", type, scheme.CStr());
	return nullptr;
}

std::shared_ptr<OrchestratorProviderModuleInterface> Orchestrator::GetProviderModuleForScheme(const ov::String &scheme)
{
	auto provider = GetProviderForScheme(scheme);
	auto provider_module = std::dynamic_pointer_cast<OrchestratorProviderModuleInterface>(provider);

	OV_ASSERT((provider == nullptr) || (provider_module != nullptr),
			  "Provider (%d) shouldmust inherit from OrchestratorProviderModuleInterface",
			  provider->GetProviderType());

	return provider_module;
}

std::shared_ptr<pvd::Provider> Orchestrator::GetProviderForUrl(const ov::String &url)
{
	// Find a provider type using the scheme
	auto parsed_url = ov::Url::Parse(url.CStr());

	if (url == nullptr)
	{
		logtw("Could not parse URL: %s", url.CStr());
		return nullptr;
	}

	logtd("Obtaining ProviderType for URL %s...", url.CStr());

	return GetProviderForScheme(parsed_url->Scheme());
}

bool Orchestrator::ParseVHostAppName(const ov::String &vhost_app_name, ov::String *vhost_name, ov::String *real_app_name) const
{
	auto tokens = vhost_app_name.Split("#");

	if (tokens.size() == 3)
	{
		// #<vhost_name>#<app_name>
		OV_ASSERT2(tokens[0] == "");

		if (vhost_name != nullptr)
		{
			*vhost_name = tokens[1];
		}

		if (real_app_name != nullptr)
		{
			*real_app_name = tokens[2];
		}
		return true;
	}

	OV_ASSERT2(false);
	return false;
}

std::shared_ptr<Orchestrator::VirtualHost> Orchestrator::GetVirtualHost(const ov::String &vhost_name)
{
	auto vhost_item = _virtual_host_map.find(vhost_name);

	if (vhost_item == _virtual_host_map.end())
	{
		return nullptr;
	}

	return vhost_item->second;
}

std::shared_ptr<const Orchestrator::VirtualHost> Orchestrator::GetVirtualHost(const ov::String &vhost_name) const
{
	auto vhost_item = _virtual_host_map.find(vhost_name);

	if (vhost_item == _virtual_host_map.end())
	{
		return nullptr;
	}

	return vhost_item->second;
}

std::shared_ptr<Orchestrator::VirtualHost> Orchestrator::GetVirtualHost(const ov::String &vhost_app_name, ov::String *real_app_name)
{
	ov::String vhost_name;

	if (ParseVHostAppName(vhost_app_name, &vhost_name, real_app_name))
	{
		return GetVirtualHost(vhost_name);
	}

	// Invalid format
	OV_ASSERT2(false);
	return nullptr;
}

std::shared_ptr<const Orchestrator::VirtualHost> Orchestrator::GetVirtualHost(const ov::String &vhost_app_name, ov::String *real_app_name) const
{
	ov::String vhost_name;

	if (ParseVHostAppName(vhost_app_name, &vhost_name, real_app_name))
	{
		return GetVirtualHost(vhost_name);
	}

	// Invalid format
	OV_ASSERT2(false);
	return nullptr;
}

bool Orchestrator::GetUrlListForLocation(const ov::String &vhost_app_name, const ov::String &stream_name, std::vector<ov::String> *url_list)
{
	std::lock_guard<decltype(_virtual_host_map_mutex)> lock_guard_for_app_map(_virtual_host_map_mutex);

	return GetUrlListForLocationInternal(vhost_app_name, stream_name, url_list, nullptr, nullptr);
}

bool Orchestrator::GetUrlListForLocationInternal(const ov::String &vhost_app_name, const ov::String &stream_name, std::vector<ov::String> *url_list, Origin **used_origin, Domain **used_domain)
{
	ov::String real_app_name;
	auto vhost = GetVirtualHost(vhost_app_name, &real_app_name);

	if (vhost == nullptr)
	{
		logte("Could not find VirtualHost for the stream: [%s/%s]", vhost_app_name.CStr(), stream_name.CStr());
		return false;
	}

	auto &domain_list = vhost->domain_list;
	auto &origin_list = vhost->origin_list;

	ov::String location = ov::String::FormatString("/%s/%s", real_app_name.CStr(), stream_name.CStr());

	// Find the origin using the location
	for (auto &domain : domain_list)
	{
		for (auto &origin : origin_list)
		{
			logtd("Trying to find the item that match location: %s", location.CStr());

			// TODO(dimien): Replace with the regex
			if (location.HasPrefix(origin.location))
			{
				// If the location has the prefix that configured in <Origins>, extract the remaining part
				// For example, if the settings is:
				//      <Origin>
				//      	<Location>/app/stream</Location>
				//      	<Pass>
				//              <Scheme>ovt</Scheme>
				//      		<Url>origin.airensoft.com:9000/another_app/and_stream</Url>
				//      	</Pass>
				//      </Origin>
				// And when the location is "/app/stream_o",
				//
				// <Location>: /app/stream
				// location:   /app/stream_o
				//                        ~~ <= remaining part
				auto remaining_part = location.Substring(origin.location.GetLength());

				logtd("Found: location: %s (app: %s, stream: %s), remaining_part: %s", origin.location.CStr(), real_app_name.CStr(), stream_name.CStr(), remaining_part.CStr());

				for (auto url : origin.url_list)
				{
					// Append the remaining_part to the URL

					// For example,
					//    url:     ovt://origin.airensoft.com:9000/another_app/and_stream
					//    new_url: ovt://origin.airensoft.com:9000/another_app/and_stream_o
					//                                                                   ~~ <= remaining part

					// Prepend "<scheme>://"
					url.Prepend("://");
					url.Prepend(origin.scheme);

					// Append remaining_part
					url.Append(remaining_part);

					url_list->push_back(url);
				}

				if(used_domain != nullptr)
				{
					*used_domain = &domain;
				}

				if(used_origin != nullptr)
				{
					*used_origin = &origin;
				}

				return (url_list->size() > 0) ? true : false;
			}
		}
	}

	return false;
}

Orchestrator::Result Orchestrator::CreateApplicationInternal(const ov::String &vhost_name, const info::Application &app_info)
{
	auto vhost = GetVirtualHost(vhost_name);

	if (vhost == nullptr)
	{
		return Result::Failed;
	}

	auto &app_map = vhost->app_map;
	auto &app_name = app_info.GetName();

	for (auto &app : app_map)
	{
		if (app.second->app_info.GetName() == app_name)
		{
			// The application does exists
			return Result::Exists;
		}
	}

	logti("Trying to create an application: [%s]", app_name.CStr());

	mon::Monitoring::GetInstance()->OnApplicationCreated(app_info);

	// Notify modules of creation events
	std::vector<std::shared_ptr<OrchestratorModuleInterface>> created_list;
	bool succeeded = true;

	auto new_app = std::make_shared<Application>(this, app_info);
	app_map.emplace(app_info.GetId(), new_app);

	for (auto &module : _module_list)
	{
		logtd("Notifying %p (%s) for the create event (%s)", module.module.get(), GetOrchestratorModuleTypeName(module.module->GetModuleType()), app_info.GetName().CStr());

		if (module.module->OnCreateApplication(app_info))
		{
			logtd("The module %p (%s) returns true", module.module.get(), GetOrchestratorModuleTypeName(module.module->GetModuleType()));

			created_list.push_back(module.module);
		}
		else
		{
			logte("The module %p (%s) returns error while creating the application [%s]",
				  module.module.get(), GetOrchestratorModuleTypeName(module.module->GetModuleType()), app_name.CStr());
			succeeded = false;
			break;
		}
	}

	if (_media_router != nullptr)
	{
		_media_router->RegisterObserverApp(app_info, new_app->GetSharedPtrAs<MediaRouteApplicationObserver>());
	}

	if (succeeded)
	{
		return Result::Succeeded;
	}

	logte("Trying to rollback for the application [%s]", app_name.CStr());
	return DeleteApplicationInternal(app_info);
}

Orchestrator::Result Orchestrator::CreateApplicationInternal(const ov::String &vhost_app_name, info::Application *app_info)
{
	OV_ASSERT2(app_info != nullptr);

	ov::String vhost_name;

	if (ParseVHostAppName(vhost_app_name, &vhost_name, nullptr))
	{
		auto vhost = GetVirtualHost(vhost_name);
		*app_info = info::Application(vhost->host_info, GetNextAppId(), vhost_app_name);
		return CreateApplicationInternal(vhost_name, *app_info);
	}

	return Result::Failed;
}

Orchestrator::Result Orchestrator::NotifyModulesForDeleteEvent(const std::vector<Module> &modules, const info::Application &app_info)
{
	Result result = Result::Succeeded;

	// Notify modules of deletion events
	for (auto &module : modules)
	{
		logtd("Notifying %p (%s) for the delete event (%s)", module.module.get(), GetOrchestratorModuleTypeName(module.module->GetModuleType()), app_info.GetName().CStr());

		if (module.module->OnDeleteApplication(app_info) == false)
		{
			logte("The module %p (%s) returns error while deleting the application %s",
				  module.module.get(), GetOrchestratorModuleTypeName(module.module->GetModuleType()), app_info.GetName().CStr());

			// Ignore this error
			result = Result::Failed;
		}
		else
		{
			logtd("The module %p (%s) returns true", module.module.get(), GetOrchestratorModuleTypeName(module.module->GetModuleType()));
		}
	}

	return result;
}

Orchestrator::Result Orchestrator::DeleteApplicationInternal(const ov::String &vhost_name, info::application_id_t app_id)
{
	auto vhost = GetVirtualHost(vhost_name);

	if (vhost == nullptr)
	{
		return Result::Failed;
	}

	auto &app_map = vhost->app_map;
	auto app_item = app_map.find(app_id);

	if (app_item == app_map.end())
	{
		logti("Application %d does not exists", app_id);
		return Result::NotExists;
	}

	auto app = app_item->second;
	auto &app_info = app->app_info;

	logti("Trying to delete the application: [%s] (%u)", app_info.GetName().CStr(), app_info.GetId());
	app_map.erase(app_id);

	if (_media_router != nullptr)
	{
		_media_router->UnregisterObserverApp(app_info, app->GetSharedPtrAs<MediaRouteApplicationObserver>());
	}

	logtd("Notifying modules for the delete event...");
	return NotifyModulesForDeleteEvent(_module_list, app_info);
}

Orchestrator::Result Orchestrator::DeleteApplicationInternal(const info::Application &app_info)
{
	ov::String vhost_name;

	if (ParseVHostAppName(app_info.GetName(), &vhost_name, nullptr) == false)
	{
		return Result::Failed;
	}

	return DeleteApplicationInternal(vhost_name, app_info.GetId());
}

Orchestrator::Result Orchestrator::CreateApplication(const info::Host &host_info, const cfg::Application &app_config)
{
	std::lock_guard<decltype(_module_list_mutex)> lock_guard_for_modules(_module_list_mutex);
	std::lock_guard<decltype(_virtual_host_map_mutex)> lock_guard_for_app_map(_virtual_host_map_mutex);

	auto vhost_name = host_info.GetName();

	info::Application app_info(host_info, GetNextAppId(), ResolveApplicationName(vhost_name, app_config.GetName()), app_config);

	return CreateApplicationInternal(vhost_name, app_info);
}

Orchestrator::Result Orchestrator::DeleteApplication(const info::Application &app_info)
{
	std::lock_guard<decltype(_module_list_mutex)> lock_guard_for_modules(_module_list_mutex);
	std::lock_guard<decltype(_virtual_host_map_mutex)> lock_guard_for_app_map(_virtual_host_map_mutex);

	mon::Monitoring::GetInstance()->OnApplicationDeleted(app_info);

	return DeleteApplicationInternal(app_info);
}

const info::Application &Orchestrator::GetApplicationInternal(const ov::String &vhost_app_name) const
{
	ov::String vhost_name;

	if (ParseVHostAppName(vhost_app_name, &vhost_name, nullptr))
	{
		auto vhost = GetVirtualHost(vhost_name);

		if (vhost != nullptr)
		{
			auto &app_map = vhost->app_map;

			for (auto app_item : app_map)
			{
				auto &app_info = app_item.second->app_info;

				if (app_info.GetName() == vhost_app_name)
				{
					return app_info;
				}
			}
		}
	}

	return info::Application::GetInvalidApplication();
}

const info::Application &Orchestrator::GetApplication(const ov::String &vhost_app_name) const
{
	std::lock_guard<decltype(_virtual_host_map_mutex)> lock_guard_for_app_map(_virtual_host_map_mutex);

	return GetApplicationInternal(vhost_app_name);
}

const info::Application &Orchestrator::GetApplicationInternal(const ov::String &vhost_name, info::application_id_t app_id) const
{
	auto vhost = GetVirtualHost(vhost_name);

	if (vhost != nullptr)
	{
		auto &app_map = vhost->app_map;
		auto app_item = app_map.find(app_id);

		if (app_item != app_map.end())
		{
			return app_item->second->app_info;
		}
	}

	return info::Application::GetInvalidApplication();
}

#if 0
bool Orchestrator::RequestPullStreamForUrl(const std::shared_ptr<const ov::Url> &url)
{
	// TODO(dimiden): The part that creates an app using the origin map is considered to handle different Origin's apps,
	// But, this part is not considered

	auto source = url->Source();
	auto provider = GetProviderForScheme(url->Scheme());

	if (provider == nullptr)
	{
		logte("Could not find provider for URL: %s", source.CStr());
		return false;
	}

	auto provider_module = std::dynamic_pointer_cast<OrchestratorProviderModuleInterface>(provider);

	if (provider_module->CheckOriginAvailability({source}) == false)
	{
		logte("The URL is not available: %s", source.CStr());
		return false;
	}

	info::Application app_info;
	auto result = CreateApplicationInternal(url->App(), &app_info);

	if (result != Result::Failed)
	{
		// The application is created successfully (or already exists)
		if (provider_module->PullStream(app_info, url->Stream(), {source}))
		{
			// The stream pulled successfully
			return true;
		}
	}
	else
	{
		// Could not create the application
		return false;
	}

	// Rollback if failed
	if (result == Result::Succeeded)
	{
		// If there is no existing app and it is created, delete the app
		DeleteApplicationInternal(app_info.GetId());
	}

	return false;
}

bool Orchestrator::RequestPullStream(const ov::String &url)
{
	std::lock_guard<decltype(_modules_mutex)> lock_guard_for_modules(_modules_mutex);
	std::lock_guard<decltype(_virtual_host_map_mutex)> lock_guard_for_app_map(_virtual_host_map_mutex);

	auto parsed_url = ov::Url::Parse(url.CStr());

	if (parsed_url != nullptr)
	{
		// The URL has a scheme
		return RequestPullStreamForUrl(parsed_url);
	}
	else
	{
		// Invalid URL
		logte("Pull stream is requested for invalid URL: %s", url.CStr());
	}

	return false;
}
#endif

bool Orchestrator::RequestPullStreamForLocation(const ov::String &vhost_app_name, const ov::String &stream_name, off_t offset)
{
	std::vector<ov::String> url_list;

	Origin *used_origin = nullptr;
	Domain *used_domain = nullptr;

	if (GetUrlListForLocationInternal(vhost_app_name, stream_name, &url_list, &used_origin, &used_domain) == false)
	{
		logte("Could not find Origin for the stream: [%s/%s]", vhost_app_name.CStr(), stream_name.CStr());
		return false;
	}

	if ((used_origin == nullptr) || (used_domain == nullptr))
	{
		// Origin/Domain can never be nullptr if origin is found
		OV_ASSERT2((used_origin != nullptr) && (used_domain != nullptr));

		logte("Could not find URL list for the stream: [%s/%s]", vhost_app_name.CStr(), stream_name.CStr());
		return false;
	}

	// This is a safe operation because origin_list is managed by Orchestrator

	auto provider_module = GetProviderModuleForScheme(used_origin->scheme);

	if (provider_module == nullptr)
	{
		logte("Could not find provider for the stream: [%s/%s]", vhost_app_name.CStr(), stream_name.CStr());
		return false;
	}

	// Check if the application does exists
	auto app_info = GetApplicationInternal(vhost_app_name);
	Result result;

	if (app_info.IsValid())
	{
		result = Result::Exists;
	}
	else
	{
		// Create a new application
		result = CreateApplicationInternal(vhost_app_name, &app_info);

		if (
			// Failed to create the application
			(result == Result::Failed) ||
			// result always must be Result::Succeeded
			(result != Result::Succeeded))
		{
			return false;
		}
	}

	logti("Trying to pull stream [%s/%s] from provider: %s", vhost_app_name.CStr(), stream_name.CStr(), GetOrchestratorModuleTypeName(provider_module->GetModuleType()));

	auto stream = provider_module->PullStream(app_info, stream_name, url_list, offset);

	if (stream != nullptr)
	{
		auto orchestrator_stream = std::make_shared<Stream>(app_info, provider_module, stream, ov::String::FormatString("%s/%s", vhost_app_name.CStr(), stream_name.CStr()));

		used_origin->stream_map[stream->GetId()] = orchestrator_stream;
		used_domain->stream_map[stream->GetId()] = orchestrator_stream;

		logti("The stream was pulled successfully: [%s/%s]", vhost_app_name.CStr(), stream_name.CStr());
		return true;
	}

	logte("Could not pull stream [%s/%s] from provider: %s", vhost_app_name.CStr(), stream_name.CStr(), GetOrchestratorModuleTypeName(provider_module->GetModuleType()));
	// Rollback if needed

	switch (result)
	{
		case Result::Failed:
		case Result::NotExists:
			// This is a bug - Must be handled above
			OV_ASSERT2(false);
			break;

		case Result::Succeeded:
			// New application is created. Rollback is required
			DeleteApplicationInternal(app_info);
			break;

		case Result::Exists:
			// Used a previously created application. Do not need to rollback
			break;
	}

	return false;
}

bool Orchestrator::RequestPullStream(const ov::String &vhost_app_name, const ov::String &stream, off_t offset)
{
	std::lock_guard<decltype(_module_list_mutex)> lock_guard_for_modules(_module_list_mutex);
	std::lock_guard<decltype(_virtual_host_map_mutex)> lock_guard_for_app_map(_virtual_host_map_mutex);

	return RequestPullStreamForLocation(vhost_app_name, stream, offset);
}

bool Orchestrator::OnCreateStream(const info::Application &app_info, const std::shared_ptr<info::Stream> &info)
{
	logtd("%s stream is created", info->GetName().CStr());
	return true;
}

bool Orchestrator::OnDeleteStream(const info::Application &app_info, const std::shared_ptr<info::Stream> &info)
{
	logtd("%s stream is deleted", info->GetName().CStr());
	return true;
}