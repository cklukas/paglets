// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Download Courier demo (planning/cpp-web-ai.md, section 5).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace courier_msgs {

// `start`: fetch `url` through a host offering `web` and bring it home.
struct Start {
    std::string url;
    std::string sha256;                         // expected content (hex); empty: not checked
    std::string via = "offer:web.download";     // the transfer ticket to the web host
    std::string deliver_to;                     // key ID of the host to deliver to; empty: the starting host
};

struct Started {
    bool accepted = false;
    std::string reason;
};

struct StatusRequest {};

// `status`: where the courier is in its errand.
struct Status {
    std::string state;  // idle, travelling, downloading, returning, delivered, failed
    std::string url;
    std::string home;      // key ID of the host it started on
    std::string artifact;  // delivered: hex SHA-256 of the artifact on the home host
    std::int64_t size = 0;
    std::int64_t http_status = 0;
    std::string error;
    std::vector<std::string> hosts;  // names of the hosts it was on, in order
};

}  // namespace courier_msgs
