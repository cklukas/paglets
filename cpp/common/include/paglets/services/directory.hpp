// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `directory` system paglet (planning/cpp-system-paglets.md):
// discovery of system services and of paglets that published themselves
// under a name. A successful lookup replies with an endpoint capability
// (the first capability of the reply) with the operations the caller may
// use: for services, as the host's policy allows (WP9); for published
// paglets, as the publisher chose. Published names are visible to paglets
// of the same owner, or to everybody if published `public`; they are
// removed when their paglet ends.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::directory {

enum class EntryKind { service, paglet };

struct LookupRequest {
    std::string name;
};

struct LookupReply {
    EntryKind kind = EntryKind::service;
    std::vector<std::string> ops;  // of the endpoint in the reply
};

struct PublishRequest {
    std::string name;              // 1 to 64 characters from [a-z0-9._-]; not a service name
    std::vector<std::string> ops;  // operations callers get; empty: all
    bool is_public = false;        // visible to every owner
};

struct PublishReply {
    std::string name;
};

struct UnpublishRequest {
    std::string name;
};

struct UnpublishReply {
    bool existed = false;
};

struct ListRequest {
    std::string prefix;
};

struct Listing {
    std::string name;
    EntryKind kind = EntryKind::service;
    std::string owner;  // empty for services
};

struct ListReply {
    std::vector<Listing> entries;  // visible to the caller, sorted by name
};

struct Contract {
    static constexpr std::string_view service = "directory";
    LookupReply lookup(const LookupRequest&);
    PublishReply publish(const PublishRequest&);
    UnpublishReply unpublish(const UnpublishRequest&);
    ListReply list(const ListRequest&);
};

}  // namespace paglets::services::directory
