// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `mesh-info` system paglet (planning/cpp-compute.md): what
// every host of the mesh looks like right now. Each host samples itself and
// its snapshots spread by gossip, so every host answers for the whole mesh.
//
// - `snapshot`: this host.
// - `landscape`: every host with a fresh snapshot (default: at most 20 s
//   old), this one first.
// - `select`: hosts that fit (load, memory, free compute slots), the best
//   first; with nothing fitting, the least loaded hosts are returned with
//   `fallback` set, so a job always gets somewhere.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::mesh_info {

struct Snapshot {
    std::string host;       // key ID
    std::string host_name;  // its name in the ledger
    std::vector<std::string> labels;
    std::int64_t observed_ms = 0;  // when the host sampled itself, Unix milliseconds
    std::string os;
    std::string architecture;
    std::int64_t cpus = 0;
    double load_per_cpu = 0;  // 1-minute load average per CPU (CPU use where there is none)
    double cpu_percent = 0;
    std::int64_t memory_total = 0;
    std::int64_t memory_available = 0;
    std::int64_t work_free = 0;  // free bytes where the host keeps paglets' state
    std::int64_t paglets = 0;    // paglets on the host
    // compute-slots on the host.
    std::int64_t slots = 0;
    std::int64_t slots_free = 0;
    std::int64_t queued = 0;
};

struct SnapshotRequest {};

struct LandscapeRequest {
    std::int64_t max_age_ms = 20'000;
};

struct Landscape {
    std::vector<Snapshot> hosts;
};

struct SelectRequest {
    std::int64_t limit = 1;
    double max_load_per_cpu = 1.0;
    std::int64_t min_memory_available = 0;
    std::int64_t min_slots_free = 0;
    std::vector<std::string> labels;  // all required
    bool include_self = true;
    std::int64_t max_age_ms = 20'000;
};

struct Selection {
    std::vector<Snapshot> hosts;  // the best first
    bool fallback = false;        // nothing fit: the least loaded hosts instead
};

struct Contract {
    static constexpr std::string_view service = "mesh-info";
    Snapshot snapshot(const SnapshotRequest&);
    Landscape landscape(const LandscapeRequest&);
    Selection select(const SelectRequest&);
};

}  // namespace paglets::services::mesh_info
