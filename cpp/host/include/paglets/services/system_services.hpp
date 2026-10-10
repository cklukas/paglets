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
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services {

// Which operations of a system service a paglet may use through the
// directory; `offered` lists all of them. The default allows every one.
using ServicePolicy = std::function<std::vector<std::string>(const abi::SenderRecord& caller, std::string_view service,
                                                             const std::vector<std::string>& offered)>;

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

// Content exchange between hosts (planning/cpp-content-exchange.md). An
// `artifact` capability names its content by SHA-256 and, on a host of a
// mesh, the host that holds it: `<hash>@<host key ID>/<token>`. A host that
// lacks the content fetches it from that host, which serves it only with
// the token it made and only within the data residency of the content.
struct ContentFetch {
    std::string hash;   // hex SHA-256
    std::string host;   // key ID of the host that holds it
    std::string token;  // made by that host
    std::filesystem::path into;  // written here (created or replaced)
    std::uint64_t max_bytes = 0;
};

struct ContentFetched {
    std::int32_t status = 0;  // abi::ok or an abi::Error
    std::string error;
    std::uint64_t size = 0;
    std::vector<std::string> marks;  // data residency marks of the content
};

struct ContentExchange {
    std::string host;  // key ID of this host
    // The token of content `hash` on this host.
    std::function<std::string(std::string_view hash)> token;
    // Starts a fetch; `done` is called once, possibly on another thread.
    std::function<void(ContentFetch, std::function<void(ContentFetched)> done)> fetch;
};

// Content this host serves to others: a stored artifact or a shared file.
struct ServedContent {
    std::filesystem::path file;
    std::uint64_t size = 0;
    std::vector<std::string> marks;
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

    // Stores `data` as an artifact for other system paglets (web downloads,
    // AI results): an `artifact` capability (rights `read`), its content
    // carrying `marks` (data residency).
    virtual std::expected<runtime::Cap, std::int32_t> store_artifact(std::span<const std::uint8_t> data,
                                                                     std::string media_type,
                                                                     std::vector<std::string> marks) = 0;

    // Connects `artifacts` and `files` to the mesh (the node installs it):
    // their capabilities then name this host and work on every host.
    virtual void set_content_exchange(ContentExchange exchange) = 0;

    // The content with SHA-256 `hash` (hex) if this host holds it, for
    // serving it to another host.
    virtual std::optional<ServedContent> served_content(std::string_view hash) const = 0;
};

// Registers the standard system paglets with `runtime` (before paglets are
// created, so they receive their service endpoints).
std::expected<std::shared_ptr<SystemServices>, std::string> install_system_services(runtime::Runtime& runtime,
                                                                                    ServicesConfig config);

}  // namespace paglets::services
