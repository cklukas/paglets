// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Host-side capability entries (planning/cpp-security-and-communication.md,
// section 4). A paglet refers to them by handle; the host keeps the entry.

#pragma once

#include <paglets/abi.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace paglets::runtime {

struct Cap {
    enum class Kind : std::int32_t { endpoint = 0, reply = 1, timer = 2, resource = 3 };
    Kind kind = Kind::endpoint;
    // endpoint: paglet ID; reply: requester (empty: the host); timer: owner;
    // resource: the system paglet that provides it.
    std::string target;
    // endpoint: message names (`*` for all); resource: rights (read, write, ...).
    std::vector<std::string> ops;
    bool transferable = true;
    std::optional<std::string> badge;
    std::optional<std::int64_t> expires;  // Unix milliseconds
    std::optional<std::int64_t> uses_left;
    std::uint64_t correlation = 0;  // reply
    std::uint64_t timer_id = 0;     // timer
    std::int64_t fire_at = 0;       // timer, Unix milliseconds
    abi::OutMessage message;        // timer: the message to deliver

    // Resources: `dir`, `file`, `artifact`, `topic`; `resource` is defined
    // by the providing system paglet (for `dir`: a named root and a relative
    // path, `root:dir/sub`).
    std::string resource_type;
    std::string resource;

    // Revocation tree: every endpoint and resource capability has an ID;
    // derived capabilities list the IDs they descend from (root first) and
    // keep the grant their root came from. Revoking an ID or a grant
    // invalidates the whole subtree, wherever the copies went.
    std::string id;
    std::vector<std::string> lineage;
    std::optional<std::string> grant;
};

}  // namespace paglets::runtime
