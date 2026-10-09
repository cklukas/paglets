// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Latency Map demo (planning/cpp-demo-paglets.md, demo 13).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace latency_msgs {

// `start`: a probe on every host (or `hosts`); each pings every other probe
// `rounds` times.
struct Probe {
    std::vector<std::string> hosts;
    std::int64_t rounds = 5;
    std::int64_t timeout_ms = 30'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t probes = 0;
};

// A probe tells the parent where it is (its endpoint comes with it).
struct Ready {
    std::string host_name;
};

// The parent tells every probe about the others (their endpoints come
// with it, in this order).
struct Peers {
    std::vector<std::string> names;
};

struct Cell {
    std::string to;
    double avg_ms = 0;
    double min_ms = 0;
    std::int64_t replies = 0;
    std::string error;
};

struct Row {
    std::string from;
    std::vector<Cell> cells;
};

struct LatencyMap {
    bool finished = false;
    std::vector<Row> rows;
    std::vector<std::string> errors;
};

}  // namespace latency_msgs
