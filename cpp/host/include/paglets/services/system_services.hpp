// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The standard system paglets of a host (planning/cpp-system-paglets.md):
// files, server-info, directory, storage, artifacts, pubsub and user-info,
// with the contracts in common/include/paglets/services/. They need C++26
// reflection (services/contract.hpp).

#pragma once

#include <paglets/abi.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/services/user_info.hpp>

#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services {

// Which operations of a system service a paglet may use through the
// directory; `offered` lists all of them. The default allows every one.
using ServicePolicy = std::function<std::vector<std::string>(const abi::SenderRecord& caller, std::string_view service,
                                                             const std::vector<std::string>& offered)>;

// Called when a paglet reads (lists, finds, reads) content of a named root,
// for data residency marks (planning/cpp-residency.md).
using AccessObserver = std::function<void(const abi::SenderRecord& caller, std::string_view root)>;

struct Notification {
    std::uint64_t id = 0;
    std::string owner;
    std::string paglet;
    std::string title;
    std::string text;
    user_info::Level level = user_info::Level::info;
    std::int64_t time_ms = 0;
};

struct ServicesConfig {
    // Named roots of `files`: root name -> host directory. Names are 1 to 64
    // characters from [a-z0-9._-].
    std::map<std::string, std::filesystem::path> roots;
    // Service state (published names, topics, artifacts). Empty: in memory,
    // artifacts in a temporary directory.
    std::filesystem::path state_dir;
    std::uint64_t storage_quota = 16u * 1024 * 1024;      // per paglet
    std::uint64_t artifacts_limit = 1024u * 1024 * 1024;  // whole store
    std::size_t notifications_per_owner = 100;
    std::function<void(const Notification&)> notify;  // additional sink
    ServicePolicy policy;
};

class SystemServices {
public:
    virtual ~SystemServices() = default;

    // A `dir` capability of `files` for a named root (and a relative path
    // below it), to hand to a paglet (Runtime::add_capability). Grants (WP9)
    // create them; hosts and tests may too.
    virtual std::expected<runtime::Cap, std::string> directory_capability(
        std::string_view root, std::string_view path, std::vector<std::string> rights,
        std::optional<std::string> grant = std::nullopt, std::optional<std::int64_t> expires = std::nullopt) const = 0;

    // Notifications kept for an owner, oldest first.
    virtual std::vector<Notification> notifications(std::string_view owner) const = 0;

    // Replaces the policy of directory lookups (WP9 installs the ledger's).
    virtual void set_policy(ServicePolicy policy) = 0;
    // Hears which roots paglets read from (the mesh node marks them).
    virtual void set_access_observer(AccessObserver observer) = 0;
};

// Registers the standard system paglets with `runtime` (before paglets are
// created, so they receive their service endpoints).
std::expected<std::shared_ptr<SystemServices>, std::string> install_system_services(runtime::Runtime& runtime,
                                                                                    ServicesConfig config);

}  // namespace paglets::services
