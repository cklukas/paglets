// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `zombies` system paglet (docs/zombie-protocol.md): the
// host's registry of zombie groups. Every host keeps it; tombstones and
// sink redirects spread to every host by gossip, so a replica that was cut
// off learns that its group ended when it reconnects, even if every other
// replica is gone.
//
// - `join`: a replica registers for its group on this host. At most one
//   replica per group and host: a second one is refused (it ends). A group
//   that ended is refused too. The first replica on any host gives the
//   sink. A replica that joined receives `zombies.update` (GroupState) when
//   the group ends or its sink changes.
// - `trace`: a replica records its protocol steps (role changes and sink
//   answers); a bounded ring per group, kept with the host's state.
// - `ended`: the replica's group delivered its result (its Tombstone
//   output); the tombstone spreads.
// - `state`: what this host knows about a group.
// - `cancel`: the owner (or an admin) ends a group; `redirect`: the owner
//   names another sink (the sink number grows).
// - `list`: the groups this host knows; `traces`: a group's ring here.
// - `deliver`: the main's result goes to the group's sink, which is the
//   `zombies` system paglet of a host: sink_spec `host:NAME` (a host name in
//   the ledger or a key ID). The sink accepts the first result per group and
//   sink number and answers every later one `accepted: false`; the result is
//   stored on that host before the answer. `result`: a result stored here.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::zombies {

enum class Ending { none, delivered, cancelled };

struct GroupState {
    std::string group;
    Ending ending = Ending::none;
    std::string reason;
    std::int64_t ended_ms = 0;  // Unix milliseconds
    std::int64_t sink = 1;      // the sink's number: 1, raised by every redirect
    std::string sink_spec;      // where results go (the paglet's own format)
};

struct JoinRequest {
    std::string group;      // 32 lowercase hex digits
    std::string sink_spec;  // the sink, if this host does not know the group yet
};

struct JoinReply {
    bool accepted = false;
    std::string reason;  // refused: why
    GroupState state;
};

// One protocol step of a replica.
struct TraceRecord {
    std::string host;    // key ID (set by the host that records it)
    std::string paglet;  // the replica (set by the host)
    std::int64_t time_ms = 0;
    std::uint32_t replica = 0;      // its number in the group
    std::uint32_t incarnation = 0;  // raised by every restart
    std::uint64_t seq = 0;          // counts the replica's records within an incarnation
    std::string event;              // transition, accepted, already
    std::string from;               // transition: roles and cause
    std::string cause;
    std::string to;
    bool done = false;  // the replica's rank after the step
    std::int64_t progress = 0;
    std::uint64_t term = 0;
    std::uint64_t ckpt = 0;  // checkpoint number within the reign
    std::uint32_t author = 0;
    std::int64_t sink = 0;  // accepted, already: the sink's number
};

struct TraceRequest {
    std::string group;
    std::vector<TraceRecord> records;
};

struct Done {};

struct EndedRequest {
    std::string group;
};

struct StateRequest {
    std::string group;
};

struct CancelRequest {
    std::string group;
    std::string reason;
};

struct RedirectRequest {
    std::string group;
    std::string sink_spec;
};

struct DeliverRequest {
    std::string group;
    std::string name;  // a file name for the result, for example chronicle.md
    std::vector<std::uint8_t> result;
};

struct DeliverReply {
    bool accepted = false;  // false: the sink had a result already
    std::int64_t sink = 1;
    std::string host;  // key ID of the sink's host
};

struct ResultRequest {
    std::string group;
};

struct StoredResult {
    bool found = false;
    std::int64_t sink = 0;
    std::int64_t accepted_ms = 0;
    std::string from;  // key ID of the host whose replica delivered it
    std::string name;
    std::vector<std::uint8_t> data;
};

struct ListRequest {};

struct GroupInfo {
    GroupState state;
    std::string owner;   // key ID
    std::string paglet;  // the replica on this host; empty if none
    std::string module;
    std::int64_t joined_ms = 0;
    std::int64_t traces = 0;  // records kept here
};

struct Groups {
    std::vector<GroupInfo> groups;
};

struct TracesRequest {
    std::string group;
};

struct Traces {
    std::vector<TraceRecord> records;
};

struct Contract {
    static constexpr std::string_view service = "zombies";
    JoinReply join(const JoinRequest&);
    Done trace(const TraceRequest&);
    GroupState ended(const EndedRequest&);
    GroupState state(const StateRequest&);
    GroupState cancel(const CancelRequest&);
    GroupState redirect(const RedirectRequest&);
    Groups list(const ListRequest&);
    Traces traces(const TracesRequest&);
    DeliverReply deliver(const DeliverRequest&);
    StoredResult result(const ResultRequest&);
};

// Message to a joined replica when its group ended or its sink changed.
inline constexpr std::string_view update_message = "zombies.update";

}  // namespace paglets::services::zombies
