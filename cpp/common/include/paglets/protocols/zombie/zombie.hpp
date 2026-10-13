// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The zombie protocol (planning/cpp-zombie.md, docs/zombie-protocol.md): a
// group of replicas of one paglet, one per host, of which one (the main)
// works while the others sleep, hold its latest checkpoint and take over
// when it disappears. This header is the specification: the replica state,
// the messages, and step(), the transition function of one replica. It is
// pure: no ABI calls, no clock, no network. The guest SDK runs it, the
// model checker and the simulator (paglets/verify) explore it, and the
// `zombies` system paglet replays recorded traces through it.
//
// A replica is driven by inputs (a timer fired, a message arrived, the
// paglet worked or observed something, the sink answered, the host's
// registry changed) and answers with outputs, which the environment carries
// out in order. One rule binds the environment: a Persist output completes
// (the replica's state is durable) before any later output of the same step
// takes effect. Everything a replica sends is therefore covered by its
// durable state.

#pragma once

#include <paglets/msgpack.hpp>

#include <algorithm>
#include <array>
#include <compare>
#include <cstdint>
#include <map>
#include <optional>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace paglets::protocols::zombie {

using Bytes = std::vector<std::uint8_t>;

// --8<-- [start:members]
// A replica is named by its host: the first 8 bytes of the host's key ID
// (replica_id()). Names are stable and the same on every replica, so a
// group can grow while it runs: the membership is a set that only grows
// (a union, like the journal). Beats carry it, and a message from a replica
// not in the set adds it.
using ReplicaId = std::uint64_t;
// --8<-- [end:members]

// --8<-- [start:roles]
enum class Role : std::uint8_t {
    recovering,  // restored after a restart; listens before it may run for election
    sleeping,    // follows a main, holds its latest checkpoint
    candidate,   // the main went silent (or a worse one appeared): asks everybody
    main,        // works; sends beats and checkpoints
    delivering,  // the work is done; delivers the result to the sink
    ended,       // the group ended here (delivered or cancelled)
};
// --8<-- [end:roles]

enum class Timer : std::uint8_t {
    beat,        // main and delivering: send a beat (delivering: and deliver again)
    checkpoint,  // main: make a checkpoint if anything changed
    suspect,     // sleeping and recovering: no beat for a while
    round,       // candidate: the election round ends; main: a freeze ends
};
inline constexpr std::size_t timer_count = 4;

// Why a replica changed its role (recorded in traces).
enum class Cause : std::uint8_t {
    created,     // the group was created
    recovered,   // the host restarted
    suspect,     // no beat for the suspect timeout
    beat,        // a beat of a main at least as far as this replica
    adopted,     // a checkpoint at least as far as this replica
    worse_main,  // a beat of a main behind this replica
    better_main, // a main heard a main further along
    round_won,   // best in its election round
    round_lost,  // somebody else is best
    takeover,    // asked to take over by a candidate
    done,        // the work is done
    delivered,   // the sink accepted the result or already had one
    tomb,        // the group ended (tombstone in the host's registry)
};

inline std::string_view to_string(Role r);
inline std::string_view to_string(Timer t);
inline std::string_view to_string(Cause c);

// --8<-- [start:rank]
// How far a lineage got. Ranks are compared lexicographically: a finished
// lineage beats any other, then more progress, then a later reign and
// checkpoint, then the author with the lower host key ID (lower replica
// ID). The author is the main that made the checkpoint, so replicas
// holding the same checkpoint have equal ranks.
struct Rank {
    bool done = false;
    std::int64_t progress = 0;
    std::uint64_t term = 0;  // the reign of the author that made the checkpoint
    std::uint64_t seq = 0;   // checkpoint number within that reign
    ReplicaId author = 0;

    friend std::strong_ordering operator<=>(const Rank& a, const Rank& b) {
        if (auto c = a.done <=> b.done; c != 0) return c;
        if (auto c = a.progress <=> b.progress; c != 0) return c;
        if (auto c = a.term <=> b.term; c != 0) return c;
        if (auto c = a.seq <=> b.seq; c != 0) return c;
        return b.author <=> a.author;  // lower author ranks higher
    }
    friend bool operator==(const Rank&, const Rank&) = default;
};
// --8<-- [end:rank]

// --8<-- [start:journal]
// Observations are entries keyed by who made them: the observing replica,
// its incarnation (raised on every restart, so numbers are never reused),
// and a number counting from 1. Journals merge by union, which is
// commutative, associative and idempotent: replicas that exchanged their
// entries hold the same journal, whatever the order of exchanges.
struct EntryKey {
    ReplicaId observer = 0;
    std::uint32_t incarnation = 0;
    std::uint64_t n = 0;
    friend auto operator<=>(const EntryKey&, const EntryKey&) = default;
};

struct Entry {
    EntryKey key;
    Bytes data;
    friend bool operator==(const Entry&, const Entry&) = default;
};

// Per observer and incarnation: how many entries, counted from 1 without a
// gap, a journal holds.
struct Prefix {
    ReplicaId observer = 0;
    std::uint32_t incarnation = 0;
    std::uint64_t n = 0;
    friend auto operator<=>(const Prefix&, const Prefix&) = default;
};
using VersionVector = std::vector<Prefix>;  // sorted by observer, incarnation

struct Journal {
    std::map<EntryKey, Bytes> entries;

    VersionVector version() const;
    // Entries beyond `vv` (what a holder of `vv` lacks).
    std::vector<Entry> beyond(const VersionVector& vv) const;
    // True if this journal holds an entry beyond `vv`.
    bool has_beyond(const VersionVector& vv) const;
    // Adds entries; returns true if anything was new.
    bool merge(const std::vector<Entry>& more);
    friend bool operator==(const Journal&, const Journal&) = default;
};
// --8<-- [end:journal]

// -- messages -----------------------------------------------------------------

// Every message carries a term: StatusReq, Status and Takeover the sender's
// highest known term, the others the reign of the checkpoint they describe.
struct Beat {  // main and delivering -> everybody
    std::uint64_t term = 0;
    Rank rank;  // of the main's latest checkpoint
    VersionVector vv;
    std::vector<ReplicaId> members;  // the main's membership
    friend bool operator==(const Beat&, const Beat&) = default;
};
struct Ack {  // a replica -> the main whose beat it answers
    std::uint64_t term = 0;
    Rank rank;  // of the replica's checkpoint
    VersionVector vv;
    friend bool operator==(const Ack&, const Ack&) = default;
};
struct CheckpointMsg {  // main -> a replica that lacks something
    std::uint64_t term = 0;
    Rank rank;
    std::optional<Bytes> work;    // only if the replica is behind
    std::vector<Entry> entries;   // the entries the replica lacks
    friend bool operator==(const CheckpointMsg&, const CheckpointMsg&) = default;
};
struct Entries {  // a replica -> the main, which lacks them
    std::uint64_t term = 0;
    std::vector<Entry> entries;
    friend bool operator==(const Entries&, const Entries&) = default;
};
struct StatusReq {  // candidate -> everybody
    std::uint64_t term = 0;
    friend bool operator==(const StatusReq&, const StatusReq&) = default;
};
struct Status {  // answer to StatusReq
    std::uint64_t term = 0;
    Role role = Role::sleeping;
    Rank rank;
    friend bool operator==(const Status&, const Status&) = default;
};
struct Takeover {  // candidate -> the best replica of its round
    std::uint64_t term = 0;
    friend bool operator==(const Takeover&, const Takeover&) = default;
};
using Message = std::variant<Beat, Ack, CheckpointMsg, Entries, StatusReq, Status, Takeover>;

inline std::uint64_t term_of(const Message& m);
inline std::string_view message_name(const Message& m);

// -- inputs and outputs -------------------------------------------------------

struct Start {  // the group was created; replica `main` works first
    bool main = false;
    Bytes work;  // the main's initial work state
};
struct Recovered {};  // the host restarted; the state is the last durable one
struct Fired {
    Timer timer = Timer::beat;
};
struct Received {
    ReplicaId from = 0;
    Message message;
};
struct Worked {  // the main's paglet did a step
    std::int64_t progress = 0;
    bool done = false;
    Bytes work;
};
struct Observed {  // the main's paglet observed something
    Bytes data;
};
struct SinkAnswer {  // the sink took the result, or already had one
    std::uint64_t sink = 1;
    bool accepted = false;
};
struct RegistryUpdate {  // the host's registry knows more
    bool tomb = false;
    std::uint64_t sink = 1;
};
struct AddMembers {  // replicas made on hosts that joined the mesh
    std::vector<ReplicaId> members;
};
using Input = std::variant<Start, Recovered, Fired, Received, Worked, Observed, SinkAnswer, RegistryUpdate, AddMembers>;

struct Send {
    ReplicaId to = 0;
    Message message;
};
struct Persist {};  // make the state durable before the following outputs take effect
struct SetTimer {
    Timer timer = Timer::beat;
};
struct StopTimer {
    Timer timer = Timer::beat;
};
struct StartWork {  // the paglet works from this state (main, not frozen)
    Bytes work;
    std::int64_t progress = 0;
};
struct StopWork {};
struct Deliver {  // deliver the result (from the latest work and journal) to the sink
    std::uint64_t sink = 1;
};
struct Tombstone {};  // record in the host's registry that the group ended (delivered)
struct End {};        // dispose of the replica
struct Transition {
    Role from = Role::sleeping;
    Role to = Role::sleeping;
    Cause cause = Cause::created;
};
using Output = std::variant<Send, Persist, SetTimer, StopTimer, StartWork, StopWork, Deliver, Tombstone, End, Transition>;
using Outputs = std::vector<Output>;

// -- the replica --------------------------------------------------------------

struct State {
    ReplicaId self = 0;
    std::vector<ReplicaId> members;  // sorted, with self; only grows

    Role role = Role::sleeping;
    std::uint32_t incarnation = 0;
    std::uint64_t term = 0;   // highest term seen
    std::uint64_t reign = 0;  // the term this replica is main in (main, delivering)
    ReplicaId leader = 0;     // the main it follows (itself as main)

    // The latest checkpoint this replica holds (made or adopted).
    Rank rank;
    Bytes work;

    // The main's working state, ahead of its checkpoint.
    std::int64_t progress = 0;
    bool done = false;
    Bytes work_now;
    bool dirty = false;   // progress or journal changed since the checkpoint
    bool frozen = false;  // main: paused while a candidate's round is open

    Journal journal;
    std::uint64_t next_entry = 1;

    std::map<ReplicaId, Status> replies;  // candidate: answers of this round
    std::uint64_t sink = 1;               // the sink to deliver to (raised by redirects)
    std::uint8_t timers = 0;              // armed timers (bit per Timer)

    bool armed(Timer t) const { return (timers >> static_cast<unsigned>(t)) & 1u; }
    friend bool operator==(const State&, const State&) = default;
};

// A new replica, before its Start input; `members` as far as it knows.
inline State initial(ReplicaId self, std::vector<ReplicaId> members);
// The replica ID of a host: the first 16 hex digits of its key ID (0 if
// that is not hex).
inline ReplicaId replica_id(std::string_view host_key_id);

// --8<-- [start:transitions]
// Every role change the protocol makes (from, cause, to). step() makes no
// other; the model checker confirms that each one occurs.
struct Allowed {
    Role from;
    Cause cause;
    Role to;
};
inline constexpr std::array allowed_transitions = {
    Allowed{Role::sleeping, Cause::created, Role::main},
    Allowed{Role::recovering, Cause::suspect, Role::candidate},
    Allowed{Role::sleeping, Cause::suspect, Role::candidate},
    Allowed{Role::recovering, Cause::beat, Role::sleeping},
    Allowed{Role::candidate, Cause::beat, Role::sleeping},
    Allowed{Role::recovering, Cause::adopted, Role::sleeping},
    Allowed{Role::candidate, Cause::adopted, Role::sleeping},
    Allowed{Role::recovering, Cause::worse_main, Role::candidate},
    Allowed{Role::sleeping, Cause::worse_main, Role::candidate},
    Allowed{Role::main, Cause::better_main, Role::sleeping},
    Allowed{Role::candidate, Cause::round_won, Role::main},
    Allowed{Role::candidate, Cause::round_won, Role::delivering},
    Allowed{Role::candidate, Cause::round_lost, Role::sleeping},
    Allowed{Role::recovering, Cause::takeover, Role::main},
    Allowed{Role::recovering, Cause::takeover, Role::delivering},
    Allowed{Role::sleeping, Cause::takeover, Role::main},
    Allowed{Role::sleeping, Cause::takeover, Role::delivering},
    Allowed{Role::candidate, Cause::takeover, Role::main},
    Allowed{Role::candidate, Cause::takeover, Role::delivering},
    Allowed{Role::main, Cause::done, Role::delivering},
    Allowed{Role::sleeping, Cause::recovered, Role::recovering},
    Allowed{Role::recovering, Cause::recovered, Role::recovering},
    Allowed{Role::candidate, Cause::recovered, Role::recovering},
    Allowed{Role::main, Cause::recovered, Role::recovering},
    Allowed{Role::delivering, Cause::recovered, Role::recovering},
    Allowed{Role::recovering, Cause::delivered, Role::ended},
    Allowed{Role::sleeping, Cause::delivered, Role::ended},
    Allowed{Role::candidate, Cause::delivered, Role::ended},
    Allowed{Role::delivering, Cause::delivered, Role::ended},
    Allowed{Role::recovering, Cause::tomb, Role::ended},
    Allowed{Role::sleeping, Cause::tomb, Role::ended},
    Allowed{Role::candidate, Cause::tomb, Role::ended},
    Allowed{Role::main, Cause::tomb, Role::ended},
    Allowed{Role::delivering, Cause::tomb, Role::ended},
};
// --8<-- [end:transitions]

constexpr bool is_allowed(Role from, Cause cause, Role to) {
    for (const auto& a : allowed_transitions) {
        if (a.from == from && a.cause == cause && a.to == to) return true;
    }
    return false;
}

// The transition function: applies one input to a replica and returns what
// the environment must do, in order.
inline Outputs step(State& s, const Input& input);

// -- encoding (MessagePack arrays) ----------------------------------------------

inline void paglets_encode(msgpack::Writer& w, const Rank& v);
inline bool paglets_decode(msgpack::Reader& r, Rank& v);
inline void paglets_encode(msgpack::Writer& w, const Entry& v);
inline bool paglets_decode(msgpack::Reader& r, Entry& v);
inline void paglets_encode(msgpack::Writer& w, const Prefix& v);
inline bool paglets_decode(msgpack::Reader& r, Prefix& v);
inline void paglets_encode(msgpack::Writer& w, const Message& v);
inline bool paglets_decode(msgpack::Reader& r, Message& v);
inline void paglets_encode(msgpack::Writer& w, const State& v);
inline bool paglets_decode(msgpack::Reader& r, State& v);

}  // namespace paglets::protocols::zombie

#include <paglets/protocols/zombie/zombie_impl.hpp>
