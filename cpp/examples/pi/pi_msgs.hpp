// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the pi compute example (planning/cpp-compute.md, section 5).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace pi_msgs {

// `start`: compute `digits` hex digits of pi in chunks of `chunk` digits,
// at most `max_in_flight` chunks at a time on hosts mesh-info selects. A
// chunk not answered within `chunk_timeout_ms` (its host may be gone) is
// sent again elsewhere. `work_ms` makes each chunk take at least that long
// (for demonstrations and tests).
struct Start {
    std::int64_t digits = 1000;
    std::int64_t chunk = 60;
    std::int64_t max_in_flight = 8;
    std::int64_t chunk_timeout_ms = 30'000;
    std::int64_t work_ms = 0;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// The arguments of a worker (a clone of the job paglet on another host).
struct Task {
    std::int64_t start = 0;
    std::int64_t count = 0;
    std::int64_t attempt = 0;
    std::int64_t work_ms = 0;
};

// `result`: a worker's digits.
struct Result {
    std::int64_t start = 0;
    std::int64_t count = 0;
    std::int64_t attempt = 0;
    std::string digits;
    std::string host;  // key ID of the host that computed it
    std::string host_name;
};

struct StatusRequest {};

// `status`: progress (the digits so far, from the start, in order).
struct Status {
    std::int64_t digits = 0;  // asked for
    std::int64_t done = 0;    // contiguous from the start
    std::int64_t chunks = 0;
    std::int64_t chunks_done = 0;
    std::int64_t in_flight = 0;
    std::int64_t resent = 0;  // chunks sent again (no answer in time)
    bool finished = false;
    std::string hex;                 // "3." followed by `done` hex digits
    std::vector<std::string> hosts;  // names of the hosts that computed chunks
};

}  // namespace pi_msgs
