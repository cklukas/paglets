// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Mesh Chronicle (docs/examples/mesh-chronicle.md): a zombie
// paglet that writes the life log of its mesh.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace chronicle_msgs {

// `start`: begin the chronicle. It lives `lifetime_ms`, samples the mesh
// every `sample_ms` and delivers its life log to `sink` (`host:NAME`). The
// timing of the protocol can be shortened for tests (0: the defaults).
struct Start {
    std::string sink;
    std::int64_t lifetime_ms = 24LL * 3600 * 1000;
    std::int64_t sample_ms = 10'000;
    std::int64_t beat_ms = 0;
    std::int64_t suspect_ms = 0;
    std::int64_t round_ms = 0;
};

struct Started {
    std::string group;
    std::vector<std::string> hosts;  // where the replicas are
};

// The state of the work (replicated with every checkpoint).
struct Work {
    std::string sink;
    std::int64_t lifetime_ms = 0;
    std::int64_t sample_ms = 0;
    std::int64_t started_ms = 0;      // when the group was created
    std::int64_t samples = 0;         // progress
    std::int64_t last_sample_ms = 0;  // the latest sample of the lineage
    std::int64_t alive_ms = 0;        // time with a working main, summed over samples
    std::vector<std::string> members;     // host key IDs
    std::vector<std::string> names;       // their names, same order
};

// One entry of the journal: something the main observed.
struct Event {
    std::int64_t time_ms = 0;
    std::string kind;   // begin, host_up, host_down, link_down, link_up, takeover, end
    std::string host;   // key ID: the host it is about (takeover: the new main's host)
    std::string other;  // links: the other end; hosts: the main that saw it
    std::string text;   // details: a failure, the cause of a takeover
    std::int64_t gap_ms = 0;  // takeover: time since the lineage's latest sample
    std::uint64_t term = 0;   // takeover: the new reign
};

struct StatusRequest {};

struct Status {
    std::string group;
    std::string role;   // of this replica
    std::uint32_t replica = 0;
    std::uint32_t replicas = 0;
    std::string main;   // host name of the main this replica follows (itself if main)
    std::int64_t samples = 0;
    std::int64_t started_ms = 0;
    std::int64_t lifetime_ms = 0;
    std::int64_t events = 0;  // entries in this replica's journal
};

struct FinishRequest {};

struct FinishReply {
    bool accepted = false;  // only the acting main finishes; others name it
    std::string main;
};

}  // namespace chronicle_msgs
