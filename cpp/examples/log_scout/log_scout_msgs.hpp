// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Log Scout demo (planning/cpp-demo-paglets.md, demo 7).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace log_scout_msgs {

// `start`: search the log files matching `files` below `root` on `hosts`
// (names or key IDs; empty: every host mesh-info knows) for lines that
// contain one of `patterns`, within the last `window_ms` (0: any time).
// A line with a leading timestamp (2026-10-09T12:00:00, UTC) counts by its
// time, other lines by their file's modification time. Only the last
// `tail_bytes` of each file are read. `live`: the scouts stay on their
// hosts, check for new lines every `interval_ms` and send them home.
struct Scout {
    std::vector<std::string> hosts;
    std::string root;
    std::string files = "**/*.log";
    std::vector<std::string> patterns;
    bool ignore_case = true;
    std::int64_t window_ms = 3'600'000;
    std::int64_t tail_bytes = 1024 * 1024;
    std::int64_t max_excerpts = 5;  // per host and pattern
    bool live = false;
    std::int64_t interval_ms = 60'000;
    std::int64_t timeout_ms = 20'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

struct Count {
    std::string host_name;
    std::string pattern;
    std::int64_t matches = 0;
};

struct Excerpt {
    std::string host_name;
    std::string path;
    std::string line;
};

// What a scout found on its host: in its report, and in live batches
// (`scout.live`, answered with a LiveReply).
struct Findings {
    std::string host_name;
    std::vector<Count> counts;
    std::vector<Excerpt> excerpts;
    std::int64_t files = 0;
    std::int64_t bytes_read = 0;
};

struct LiveReply {
    bool go_on = true;
};

// `report`: the search, then the live batches.
struct Report {
    std::string state;  // idle, searching, done, live, stopped
    std::vector<std::string> hosts;
    std::vector<Count> counts;      // per host and pattern
    std::vector<Excerpt> excerpts;  // live: the latest
    std::vector<std::string> errors;
    std::int64_t files = 0;
    std::int64_t bytes_read = 0;
    std::int64_t live_batches = 0;
    std::int64_t live_matches = 0;
};

}  // namespace log_scout_msgs
