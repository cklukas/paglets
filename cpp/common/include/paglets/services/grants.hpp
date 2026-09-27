// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `grants` system paglet (planning/cpp-policy.md): a paglet
// asks for access, the host decides by the mesh policy.
//
// - `granted`: the capability is the first capability of the reply (a `dir`
//   capability for files, an endpoint for other services).
// - `pending`: an admin decides later, from any host; the paglet then
//   receives the message `grants.granted` (Answer, with the capability) or
//   `grants.denied` (Answer).
// - `denied`: `rule` names the deciding rule, or is empty when no rule
//   matched.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::grants {

struct Item {
    std::string service;           // files, server-info, ...
    std::vector<std::string> ops;  // files: rights (read, write, create, delete)
    std::string root;              // files: named root
    std::string path;              // files: directory below the root ("" for the root)
};

struct Request {
    Item item;
    std::int64_t duration_ms = 3'600'000;
    std::string reason;
};

enum class Status { granted, pending, denied };

struct Answer {
    Status status = Status::denied;
    Item item;
    std::string grant;    // hex ID of the grant record (granted)
    std::string request;  // hex ID of the request record (pending, and later answers)
    std::string rule;     // hex ID of the deciding rule, if any
    std::int64_t expires_ms = 0;
    std::string reason;
};

struct StatusRequest {
    std::string request;
};

struct ReleaseRequest {
    std::string grant;
};

struct ReleaseReply {
    bool released = false;
};

struct ListRequest {};

struct GrantInfo {
    std::string grant;
    Item item;
    std::int64_t expires_ms = 0;
};

struct ListReply {
    std::vector<GrantInfo> grants;  // valid grants of the calling paglet
};

struct Contract {
    static constexpr std::string_view service = "grants";
    Answer request(const Request&);
    Answer status(const StatusRequest&);
    ReleaseReply release(const ReleaseRequest&);
    ListReply list(const ListRequest&);
};

}  // namespace paglets::services::grants
