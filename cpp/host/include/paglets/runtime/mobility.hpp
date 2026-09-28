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
#include <expected>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace paglets::runtime {

using Bytes = std::vector<std::uint8_t>;
using PagletId = std::string;

// A message between hosts. Capabilities in it are endpoints only.
struct RemoteMessage {
    enum class Kind { message = 0, request = 1, reply = 2 };
    Kind kind = Kind::message;
    PagletId target;   // the receiving paglet (the requester, for replies)
    std::string host;  // where the target is believed to be (key ID of a host)
    std::string name;
    Bytes payload;
    std::int32_t priority = abi::default_priority;
    std::optional<abi::SenderRecord> sender;  // messages and requests
    std::optional<std::string> badge;
    std::vector<Cap> caps;
    // Requests: where the answer goes.
    PagletId requester;
    std::string requester_host;
    std::uint64_t correlation = 0;  // requests and replies
    std::int64_t timeout_ms = 0;
    std::int32_t status = 0;  // replies
    std::int32_t hops = 0;    // forwards so far (limited)
};

// A paglet's state apart from its memory image, as it travels.
struct TravelState {
    PagletId id;
    std::string module;  // hex hash
    std::string trust;   // roaming, resident
    std::string owner;
    std::int32_t next_handle = 2;
    std::uint64_t next_correlation = 1;
    std::uint64_t next_timer = 1;
    std::int64_t checkpoint_ms = -1;
    std::vector<std::pair<std::string, std::int32_t>> services;   // default service endpoints: name, handle
    std::vector<std::pair<std::int32_t, Cap>> caps;               // every capability, endpoints with host hints
    std::vector<std::pair<std::uint64_t, std::int64_t>> pending;  // requests in flight: correlation, ms left
};
Bytes encode_state(const TravelState& state);
std::expected<TravelState, std::string> decode_state(std::span<const std::uint8_t> bytes);
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
};

struct Arrival {
    TravelState state;
    wasm::Snapshot image;
    std::string from;  // the host it comes from (key ID)
    bool clone = false;
    Bytes clone_args;
    std::vector<Cap> clone_caps;
    PagletId original;
};

struct MobilityHooks {
    std::string host_id;  // this host's key ID, for host hints
    // A message for a paglet on another host. Called with the runtime's lock
    // held: queue it, do not call the runtime.
    std::function<void(RemoteMessage)> deliver;
    // A departure to carry out; called without the runtime's lock.
    std::function<void(Departure)> depart;
    // A message or request from another host for a paglet that is not here
    // and that this host has no tombstone for: the mesh locates the paglet
    // and sends it there, or answers a request with `not_found`. Called
    // with the runtime's lock held: queue it. Without it, requests are
    // answered with `not_found` at once.
    std::function<void(RemoteMessage)> unresolved;
};

// On arrival: capabilities the runtime cannot re-create itself (resource
// capabilities, endpoints to services with ambient authority): the mesh's
// replacement, or nullopt if the capability is lost here. Called with the
// runtime's lock held.
using RecreateCap = std::function<std::optional<Cap>(const Cap&)>;

}  // namespace paglets::runtime
