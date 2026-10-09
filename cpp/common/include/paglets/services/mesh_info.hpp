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
// - `find_offers`: hosts whose system paglets offer a feature (service
//   offers, planning/cpp-offers.md), for example the `ai` service with the
//   operation `summarize` and a context of at least 32 000 tokens.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::mesh_info {

// An attribute of an offer: a text (lists are comma-separated) or a number.
struct Attribute {
    std::string name;
    std::string text;
    double number = 0;
    bool numeric = false;
};

// What a system paglet of a host offers: its service, operations, and
// typed attributes (for `ai`: backend, models, tasks, context, queue).
// Offers are dynamic; a host withdraws one when its backend goes away.
struct Offer {
    std::string service;
    std::vector<std::string> ops;
    std::vector<Attribute> attributes;
};

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
    std::vector<Offer> offers;
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

// A requirement on an attribute of an offer: `is` is "=" (text), "has" (the
// comma-separated list contains `text`), ">=" or "<=" (number).
struct Requirement {
    std::string name;
    std::string is = "=";
    std::string text;
    double number = 0;
};

struct OffersRequest {
    std::string service;
    std::string op;  // an operation the offer must have (empty: any)
    std::vector<Requirement> require;
    // The order: an attribute's number, smallest first ("-name": largest
    // first); empty: the least loaded host first.
    std::string prefer;
    std::int64_t limit = 8;
    bool include_self = true;
    std::int64_t max_age_ms = 20'000;
};

struct OfferMatch {
    std::string host;  // key ID
    std::string host_name;
    std::vector<std::string> labels;
    double load_per_cpu = 0;
    Offer offer;
};

struct Offers {
    std::vector<OfferMatch> matches;  // the best first
};

struct Contract {
    static constexpr std::string_view service = "mesh-info";
    Snapshot snapshot(const SnapshotRequest&);
    Landscape landscape(const LandscapeRequest&);
    Selection select(const SelectRequest&);
    Offers find_offers(const OffersRequest&);
};

}  // namespace paglets::services::mesh_info
