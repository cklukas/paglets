// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the patterns test paglet (WP18, planning/cpp-patterns.md).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace patterns_msgs {

// The task: the sum 1..n, after `delay_ms`; n < 0 fails.
struct CountRequest {
    std::int64_t n = 0;
    std::int64_t delay_ms = 0;
};

struct CountResult {
    std::int64_t sum = 0;
};

struct AddRequest {
    std::int64_t a = 0;
    std::int64_t b = 0;
};

struct AddReply {
    std::int64_t sum = 0;
};

// Fan-out: clones on `hosts` square `value`.
struct SpreadRequest {
    std::vector<std::string> hosts;
    std::int64_t value = 0;
    std::int64_t timeout_ms = 10'000;
};

struct SpreadStatus {
    bool finished = false;
    std::int64_t succeeded = 0;
    std::vector<std::string> results;  // "host_name=value" or "destination!error"
};

// Single-file mobility: `from_root`/`path` here to `to_root`/`to_path` on `to_host`.
struct CarryRequest {
    std::string from_root;
    std::string path;
    std::string to_host;
    std::string to_root;
    std::string to_path;
};

struct CarryStatus {
    std::string state;  // idle, carrying, delivered, failed
    std::int64_t size = 0;
    std::string error;
};

// Locate and pin itself: where it is, and whether a dispatch while pinned
// was refused.
struct PinReport {
    bool done = false;
    std::string host_name;
    std::int64_t moves = 0;
    std::string dispatch_while_pinned;  // the error name
    bool released = false;
};

struct NotifyRequest {
    std::string title;
};

}  // namespace patterns_msgs
