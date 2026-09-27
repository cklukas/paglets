// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "services.hpp"

namespace paglets::services {

namespace {

class Installed final : public SystemServices {
public:
    std::shared_ptr<impl::Files> files;
    std::shared_ptr<impl::Directory> directory;
    std::shared_ptr<impl::UserInfo> user_info;
    runtime::PagletId files_id;

    std::expected<runtime::Cap, std::string> directory_capability(std::string_view root, std::string_view path,
                                                                  std::vector<std::string> rights,
                                                                  std::optional<std::string> grant,
                                                                  std::optional<std::int64_t> expires) const override {
        if (!files->roots().contains(std::string(root))) return std::unexpected("unknown root " + std::string(root));
        if (!impl::valid_relative(path, true)) return std::unexpected("invalid path " + std::string(path));
        runtime::Cap c;
        c.kind = runtime::Cap::Kind::resource;
        c.target = files_id;
        c.resource_type = "dir";
        c.resource = impl::join_relative(root, path);
        c.ops = std::move(rights);
        c.transferable = true;
        c.grant = std::move(grant);
        c.expires = expires;
        return c;
    }

    std::vector<Notification> notifications(std::string_view owner) const override {
        return user_info->notifications(owner);
    }

    void set_policy(ServicePolicy policy) override { directory->set_policy(std::move(policy)); }
};

}  // namespace

std::expected<std::shared_ptr<SystemServices>, std::string> install_system_services(runtime::Runtime& runtime,
                                                                                    ServicesConfig config) {
    namespace fs = std::filesystem;
    for (const auto& [name, path] : config.roots) {
        if (!impl::valid_name(name, 64)) return std::unexpected("invalid root name " + name);
        std::error_code ec;
        if (!fs::is_directory(path, ec))
            return std::unexpected("root " + name + " is not a directory: " + path.string());
    }
    fs::path state;
    fs::path artifacts_root;
    bool artifacts_temporary = false;
    if (!config.state_dir.empty()) {
        state = config.state_dir / "services";
        artifacts_root = config.state_dir / "artifacts";
    } else {
        std::error_code ec;
        artifacts_root = fs::temp_directory_path(ec) / ("paglets-artifacts-" + impl::random_hex(8));
        artifacts_temporary = true;
    }

    auto installed = std::make_shared<Installed>();
    installed->files = std::make_shared<impl::Files>(config.roots);
    installed->directory =
        std::make_shared<impl::Directory>(state.empty() ? fs::path() : state / "directory.state", config.policy);
    installed->user_info = std::make_shared<impl::UserInfo>(config.notifications_per_owner, config.notify);

    std::vector<std::shared_ptr<runtime::SystemPaglet>> all{
        installed->directory,
        installed->files,
        std::make_shared<impl::ServerInfo>(),
        std::make_shared<impl::Storage>(config.storage_quota),
        std::make_shared<impl::Artifacts>(artifacts_root, artifacts_temporary, config.artifacts_limit),
        std::make_shared<impl::PubSub>(state.empty() ? fs::path() : state / "pubsub.state"),
        installed->user_info,
    };
    for (auto& paglet : all) {
        auto id = runtime.add_system_paglet(paglet);
        if (!id) return std::unexpected(id.error());
        if (paglet == installed->files) installed->files_id = *id;
    }
    return installed;
}

}  // namespace paglets::services
