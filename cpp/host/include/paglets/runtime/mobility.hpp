// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// What the runtime hands to and takes from the mesh when paglets and
// messages cross hosts (planning/cpp-networking.md, sections 4 and 5).
//
// - Remote messages: endpoints can name paglets on other hosts (a host hint
//   travels with the capability); sends, requests and replies to them leave
//   through the remote delivery hook, and arrive through deliver_remote().
// - Departures: a paglet that dispatches (or a clone sent to another host)
//   leaves as a Departure: its state and memory image. The mesh transfers
//   it and reports the outcome with finish_departure(); meanwhile messages
//   to the paglet are held, afterwards they follow it.
// - Arrivals: prepare_arrival() creates the paglet without running it;
//   commit_arrival() starts it (`arrived` or `cloned` event), abort_arrival()
//   discards it.

#pragma once

#include <paglets/abi.hpp>
#include <paglets/runtime/capability.hpp>
#include <paglets/wasm/snapshot.hpp>

#include <cstdint>
#include <chrono>
#include <expected>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace paglets::runtime {

using Bytes = std::vector<std::uint8_t>;
using PagletId = std::string;

// Syntax shared by guest lifecycle calls, embedders and mesh dispatch.
struct TransferTicket {
    std::string target;  // host name, key ID, label:<label>, offer:<slot> or any
    int retries = 3;
    bool activate = true;
};
std::expected<TransferTicket, std::string> parse_transfer_ticket(std::string_view text);

// A message between hosts. Capabilities in it are endpoints only.
struct RemoteMessage {
    enum class Kind { message = 0, request = 1, reply = 2, undelivered = 3 };
    Kind kind = Kind::message;
    PagletId target;   // the receiving paglet (the requester, for replies)
    std::string host;  // where the target is believed to be (key ID of a host)
    std::string name;
    Bytes payload;
    std::int32_t priority = abi::default_priority;
    std::optional<abi::SenderRecord> sender;  // messages and requests
    std::optional<std::string> badge;
    std::vector<Cap> caps;
    // Only asynchronous one-way sends ask for a receiving-host refusal report.
    // The original handle is diagnostic metadata, never a granted capability.
    struct FailureRoute {
        PagletId paglet;
        std::string host;
        std::int32_t handle = 0;
    };
    std::optional<FailureRoute> failure;
    // Requests: where the answer goes.
    PagletId requester;
    std::string requester_host;
    std::uint64_t correlation = 0;  // requests and replies
    std::int64_t timeout_ms = 0;  // remaining budget; legacy peers may omit the absolute deadline
    std::int64_t deadline_ms = 0;  // request expiry (Unix ms), retained through forwarding
    std::int32_t status = 0;  // replies
    // Host-controlled provenance of computed results; independent of opaque
    // guest payloads, and retained when a reply follows a moving paglet.
    std::vector<std::string> residency_marks;
    std::int32_t hops = 0;    // forwards so far (limited)
    // Process-local queue budget. Never serialized as a clock value.
    std::optional<std::chrono::steady_clock::time_point> expires_at;
};

// Host notifications already accepted before capture. They travel atomically
// with the image, rather than depending on a later forwarding frame.
struct ReadyMessage {
    abi::Delivered delivered;
    std::vector<Cap> caps;
    std::vector<std::string> residency_marks;
};
Bytes encode_ready(const ReadyMessage& message);
std::expected<ReadyMessage, std::string> decode_ready(std::span<const std::uint8_t> bytes);

// A paglet's state apart from its memory image, as it travels.
struct TravelState {
    PagletId id;
    std::string module;  // hex hash
    std::string trust;   // source class, informational; every accepted arrival is roaming
    std::string owner;
    std::int32_t next_handle = 2;
    std::uint64_t next_correlation = 1;
    std::uint64_t next_timer = 1;
    std::int64_t checkpoint_ms = -1;
    std::vector<std::pair<std::string, std::int32_t>> services;   // default service endpoints: name, handle
    std::vector<std::pair<std::int32_t, Cap>> caps;               // every capability, endpoints with host hints
    std::vector<std::pair<std::uint64_t, std::int64_t>> pending;  // correlation, ms left; v4: -1 unlimited
    // State v3: when pending remaining budgets were captured (Unix ms).
    // Zero identifies legacy state with no transit-time metadata.
    std::int64_t pending_captured_ms = 0;
    std::vector<ReadyMessage> ready;  // state v2: queued replies, timers and host reports
    std::vector<std::string> marks;                               // data residency marks
    bool restart = true;                                          // restart policy (abi::RestartArg)
    bool notify_recovered = false;
    std::int64_t wake_at_ms = 0;  // timed deactivation, Unix ms; 0: none
};
Bytes encode_state(const TravelState& state);
std::expected<TravelState, std::string> decode_state(std::span<const std::uint8_t> bytes);
// A host-generated refusal notification, with no capabilities or guest sender.
std::optional<RemoteMessage> undelivered_report(const RemoteMessage& message, std::int32_t status);
Bytes encode_remote(const RemoteMessage& message);
std::expected<RemoteMessage, std::string> decode_remote(std::span<const std::uint8_t> bytes);
Bytes encode_caps(const std::vector<Cap>& caps);
std::expected<std::vector<Cap>, std::string> decode_caps(std::span<const std::uint8_t> bytes);

// A paglet created by another on this host (a child or a local clone), for
// the mesh to extend the creator's passport (Runtime::take_spawns).
struct Spawn {
    PagletId parent;
    PagletId child;
    std::string kind;    // "child" or "clone"
    std::string module;  // hex hash
};

struct Departure {
    std::uint64_t move = 0;  // for finish_departure
    TravelState state;
    wasm::Snapshot image;
    std::string destination;  // as the paglet named it
    // A clone for another host: the original stays; the clone starts with
    // `cloned` (args, the capabilities passed to it, the original's ID).
    bool clone = false;
    Bytes clone_args;
    std::vector<Cap> clone_caps;
    PagletId original;
    // Local measurement only; neither field travels in the offer.
    std::chrono::steady_clock::time_point capture_started{};
    std::uint64_t capture_ns = 0;
};

struct Arrival {
    TravelState state;
    wasm::Snapshot image;
    std::string from;  // the host it comes from (key ID)
    bool clone = false;
    Bytes clone_args;
    std::vector<Cap> clone_caps;
    PagletId original;
    std::uint64_t request = 0;  // host's transfer ID, for an arrival receipt
};

struct MobilityHooks {
    std::string host_id;  // this host's key ID, for host hints
    // A message for a paglet on another host. Called with the runtime's lock
    // held: queue it, do not call the runtime.
    std::function<void(RemoteMessage)> deliver;
    // A departure to carry out; called without the runtime's lock.
    std::function<void(Departure)> depart;
    // A message, request or reply for a paglet that is not here
    // and that this host has no tombstone for: the mesh locates the paglet
    // and sends it there, or answers a request with `not_found`. Called
    // with the runtime's lock held: queue it. Without it, requests are
    // answered with `not_found` at once.
    std::function<void(RemoteMessage)> unresolved;
    // The outcome of a paglet's move (not of clones): the key ID of the host
    // it went to, or nullopt and why it stays here. Called with the
    // runtime's lock held: do not call the runtime.
    std::function<void(const PagletId& paglet, const std::optional<std::string>& host, const std::string& reason)>
        moved;
    // The arrival hook returned (or the arrival failed before it could).
    // Called with the runtime lock held; queue transport work only.
    std::function<void(const std::string& from, std::uint64_t request, bool succeeded,
                       std::uint64_t activation_ns)> arrived;
};

// On arrival: capabilities the runtime cannot re-create itself (resource
// capabilities, endpoints to services with ambient authority): the mesh's
// replacement, or nullopt if the capability is lost here. Called with the
// runtime's lock held.
using RecreateCap = std::function<std::optional<Cap>(const Cap&)>;

}  // namespace paglets::runtime
