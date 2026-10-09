// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Mesh Benchmark demo (planning/cpp-demo-paglets.md, demo 12).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace benchmark_msgs {

// `start`: benchmark `hosts` (names or key IDs; empty: every host
// mesh-info knows). Each CPU and memory test runs for `budget_ms`; the disk
// test writes and reads `disk_mb` MB through the paglet's own storage.
struct Bench {
    std::vector<std::string> hosts;
    std::int64_t budget_ms = 300;
    std::int64_t memory_mb = 8;
    std::int64_t disk_mb = 8;
    std::int64_t timeout_ms = 60'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

// One host's numbers. Speeds are per second, sizes in MB (1024 * 1024
// bytes); the CPU tests run in the paglet (Wasm), as paglets' work does.
struct HostResult {
    std::string host_name;
    std::string os;
    std::string architecture;
    std::int64_t cpus = 0;
    double int_mops = 0;      // million integer operations
    double float_mflops = 0;  // million floating-point operations
    double memory_mbs = 0;    // copying in the paglet's memory
    double disk_write_mbs = 0;
    double disk_read_mbs = 0;
    std::int64_t bench_ms = 0;   // the benchmark on the host
    std::int64_t travel_ms = 0;  // the rest of the round trip: move, waiting for a slot, report
    bool slot = false;           // it ran in a compute slot of its own
    std::uint64_t check = 0;     // keeps the CPU loops honest
    std::string error;
};

// `ranking`: the hosts, fastest (integer CPU) first.
struct Ranking {
    std::string state;  // idle, running, done
    std::vector<HostResult> hosts;
    std::vector<std::string> errors;
};

}  // namespace benchmark_msgs
