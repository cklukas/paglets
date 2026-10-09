// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `compute-slots` system paglet (planning/cpp-compute.md):
// admission of compute work on this host. Every host has its own; there is
// no central scheduler or job queue. Hosts tell each other how many slots
// they have free (through mesh-info), so a host with a queue can send work
// on to a host with free slots.
//
// A job paglet asks the compute-slots of the host it is on:
//
// - `run_now`: it holds a lease (`lease`) for `cores` slots and runs; it
//   releases the lease when done (also when it ends or leaves the host).
// - `queued`: it waits (it may deactivate); when slots are free it receives
//   the message `compute.granted` (a Decision with run_now and the lease).
//   While it waits, the host may find a host with free slots instead: then
//   it receives `compute.redirect` (a Decision with the host) and should
//   dispatch itself there and ask again. The scheduler never moves paglets.
// - `rejected`: this host can never run it (more cores than it has).

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::compute_slots {

struct SlotRequest {
    std::string job;  // the job it belongs to (for status)
    std::int64_t cores = 1;
    std::int64_t estimated_ms = 0;
};

enum class Verdict { run_now, queued, redirect, rejected };

struct Decision {
    Verdict verdict = Verdict::rejected;
    std::string lease;  // run_now
    std::int64_t cores = 0;
    std::string host;           // redirect: key ID of the host to go to
    std::string host_name;      // its name
    std::int64_t position = 0;  // queued: place in the queue (0: next)
    std::string reason;
};

struct ReleaseRequest {
    std::string lease;
};

struct ReleaseReply {
    bool released = false;
};

struct StatusRequest {};

struct Lease {
    std::string lease;
    std::string paglet;
    std::string job;
    std::int64_t cores = 0;
    std::int64_t granted_ms = 0;
};

struct Waiting {
    std::string paglet;
    std::string job;
    std::int64_t cores = 0;
    std::int64_t since_ms = 0;
};

struct HostSlots {
    std::string host;
    std::string host_name;
    std::int64_t slots = 0;
    std::int64_t free = 0;
    std::int64_t queued = 0;
    std::int64_t observed_ms = 0;
};

struct Status {
    HostSlots self;
    std::vector<Lease> leases;
    std::vector<Waiting> queue;
    std::vector<HostSlots> peers;  // as mesh-info last heard
};

struct CandidatesRequest {
    std::int64_t cores = 1;
    std::int64_t limit = 3;
    bool include_self = true;
};

struct Candidates {
    std::vector<HostSlots> hosts;  // most free slots first
};

struct Contract {
    static constexpr std::string_view service = "compute-slots";
    Decision request_slot(const SlotRequest&);
    ReleaseReply release_slot(const ReleaseRequest&);
    Status status(const StatusRequest&);
    Candidates candidates(const CandidatesRequest&);
};

}  // namespace paglets::services::compute_slots
