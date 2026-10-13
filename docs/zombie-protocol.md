# The zombie protocol

A **zombie paglet** keeps running as long as one host of its mesh is
alive. It has one **replica** on every host of the group. One replica, the
**main**, does the work. The other replicas sleep: they hold the main's
latest checkpoint and take over when the main disappears. When the work is
done, exactly one result is delivered and every replica ends.

This page specifies the protocol and the guarantees it was checked
against. The specification is code: the pure C++ state machine in
`cpp/common/include/paglets/protocols/zombie/zombie.hpp`, from which the
excerpts below are taken. The guest SDK runs this code, the model checker and the
simulator explore it, and the `zombies` system paglet replays recorded
traces through it.

## Ground rules

- **Several mains may run for a while.** A replica that hears nothing
  cannot tell dead hosts from a cut network. So a cut-off replica takes
  over after a timeout, and during a partition each side may have its own
  main.
- **One result counts.** Uniqueness is decided where it can be decided
  atomically: at the **sink**. The sink accepts the first result for a group
  and answers every later one with `already_delivered`. A file written
  "only if absent" and a sink paglet that records accepted groups durably
  both have this property.
- **Mains that meet agree on one.** The one further along wins. The other
  forgets its work, sleeps and takes checkpoints from the winner. Its
  observations are not lost; journals are merged.
- **Work repeats from the last checkpoint.** A replica that takes over
  continues from the checkpoint it holds, so work since then is done again
  (at least once). Side effects other than the result should be
  idempotent.

## Roles

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie.hpp#roles -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie.hpp)

```cpp
enum class Role : std::uint8_t {
    recovering,  // restored after a restart; listens before it may run for election
    sleeping,    // follows a main, holds its latest checkpoint
    candidate,   // the main went silent (or a worse one appeared): asks everybody
    main,        // works; sends beats and checkpoints
    delivering,  // the work is done; delivers the result to the sink
    ended,       // the group ended here (delivered or cancelled)
};
```
<!-- /snippet -->

A replica changes its role only as listed in this table (from, cause, to).
The model checker confirms that every entry occurs and that no other change
does.

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie.hpp#transitions -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie.hpp)

```cpp
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
    Allowed{Role::main, Cause::delivered, Role::ended},
    Allowed{Role::delivering, Cause::delivered, Role::ended},
    Allowed{Role::recovering, Cause::tomb, Role::ended},
    Allowed{Role::sleeping, Cause::tomb, Role::ended},
    Allowed{Role::candidate, Cause::tomb, Role::ended},
    Allowed{Role::main, Cause::tomb, Role::ended},
    Allowed{Role::delivering, Cause::tomb, Role::ended},
};
```
<!-- /snippet -->

## Ranks: which lineage is further along

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie.hpp#rank -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie.hpp)

```cpp
// How far a lineage got. Ranks are compared lexicographically: a finished
// lineage beats any other, then more progress, then a later reign and
// checkpoint, then the author with the lower host key ID (lower replica
// number). The author is the main that made the checkpoint, so replicas
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
```
<!-- /snippet -->

`progress` is a number the paglet defines. It only grows within a lineage.
A main's rank is the rank of its latest checkpoint. A new reign gets a term
above every term its main has seen, so the same content under a newer main
ranks higher, and replicas follow the newer main.

## Journals: observations survive every partition

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie.hpp#journal -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie.hpp)

```cpp
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
```
<!-- /snippet -->

The state of a zombie has two parts:

- **work**, the state of the job. The winning lineage keeps its work; the
  others drop theirs.
- **journal**, the observations. Journals are always merged. When a replica
  hears a beat, it compares the main's version vector with its own: it asks
  for what it lacks (`ack`, answered with a `checkpoint` message), and hands
  over what the main lacks (`entries`). Entries are never removed while the
  group lives.

## Messages

| Message | From, to | Content |
|---|---|---|
| `beat` | main to every replica | rank of the main's checkpoint, its journal's version vector |
| `ack` | replica to main | the replica's rank and version vector, when it lacks something |
| `checkpoint` | main to a replica | the work (if the replica is behind) and the entries it lacks |
| `entries` | replica to main | entries the main lacks |
| `status?` | candidate to every replica | asks for role and rank |
| `status` | answer to `status?` | role, rank, highest term seen |
| `takeover` | candidate to the best replica of its round | take over as main |

Timers drive everything else: `beat` (main: send a beat; delivering: also
deliver again), `checkpoint` (main: make a checkpoint if anything
changed), `suspect` (sleeping: no beat for a while), and `round`
(candidate: the round ends; main: a pause ends).

## Rules

**Durable before visible.** A step's outputs are carried out in order, and
a `Persist` output completes before any later output of the same step
takes effect. Everything a replica sends is therefore covered by its
durable state: a main's checkpoint is stored before any replica hears of
it.

**Becoming main.** A replica takes over with a new reign. The checkpoint
keeps its content and gets the new term.

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp#become_main -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp)

```cpp
// Takes over with a new reign above every term seen. The checkpoint gets
// the new reign (same content), and is durable before the first beat.
void become_main(Cause cause, std::uint64_t seen) {
    see(seen);
    see(s_.rank.term);
    s_.term += 1;
    s_.reign = s_.term;
    s_.rank = Rank{s_.rank.done, s_.rank.progress, s_.reign, 0, s_.self};
    s_.progress = s_.rank.progress;
    s_.done = s_.rank.done;
    s_.work_now = s_.work;
    s_.dirty = false;
    s_.frozen = false;
    s_.leader = s_.self;
    s_.replies.clear();
    stop(Timer::suspect);
    stop(Timer::round);
    become(s_.done ? Role::delivering : Role::main, cause);
    persist();
    broadcast(beat());
    set(Timer::beat);
    if (s_.done) {
        out_.push_back(Deliver{s_.sink});
    } else {
        set(Timer::checkpoint);
        out_.push_back(StartWork{s_.work_now, s_.progress});
    }
}
```
<!-- /snippet -->

**Election.** A sleeping replica without a beat for the suspect timeout
(with random jitter) becomes a candidate and asks everybody for their
status. When the round ends, the best replica of the round takes over: the
candidate itself, or the replica it asks with `takeover`. If a replica that
already leads is as good as any other, nothing changes.

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp#decide -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp)

```cpp
// The end of an election round: the best of the candidate and every
// replica that answered. Equal ranks mean the same checkpoint; then a
// replica that already leads is preferred, then the lower number.
void decide() {
    ReplicaId best = s_.self;
    Rank best_rank = s_.rank;
    bool best_leads = false;
    std::uint64_t seen = s_.term;
    for (const auto& [r, st] : s_.replies) {
        seen = std::max(seen, st.term);
        const bool leads = st.role == Role::main || st.role == Role::delivering;
        if (st.rank > best_rank || (st.rank == best_rank && leads && !best_leads) ||
            (st.rank == best_rank && leads == best_leads && r < best)) {
            best = r;
            best_rank = st.rank;
            best_leads = leads;
        }
    }
    s_.replies.clear();
    if (best == s_.self) {
        become_main(Cause::round_won, seen);
        return;
    }
    see(seen);
    if (!best_leads) send(best, Takeover{s_.term});
    follow(best, Role::candidate, Cause::round_lost);
}
```
<!-- /snippet -->

A main asked for its status pauses its work for one round, so the rank it
reported stays true while the round is open.

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp#status -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp)

```cpp
// A main asked for its status checkpoints first and pauses its work for
// one round, so the rank it reports stays true while the round is open.
void receive(ReplicaId from, const StatusReq&) {
    if (s_.role == Role::main) {
        checkpoint();
        if (!s_.frozen) {
            s_.frozen = true;
            out_.push_back(StopWork{});
        }
        set(Timer::round);
    }
    send(from, Status{s_.term, s_.role, s_.rank});
}
```
<!-- /snippet -->

**Following.** A sleeping replica follows the main whose beats it hears,
as long as that main is at least as far along as itself. A beat of a main
that is behind makes it a candidate: it holds something better.

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp#beat -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp)

```cpp
void receive(ReplicaId from, const Beat& m) {
    if (from == s_.self) return;
    if (s_.role == Role::main) {
        meet(from, m.rank, m.vv);
        return;
    }
    if (!passive()) return;  // delivering: the sink decides
    if (m.rank < s_.rank) {
        if (s_.role != Role::candidate) become_candidate(Cause::worse_main);
        return;
    }
    const Role from_role = s_.role;
    follow(from, from_role, Cause::beat);
    // Ask for what is missing; hand over what the main lacks.
    const VersionVector mine = s_.journal.version();
    if (m.rank > s_.rank || detail::lacks(mine, m.vv)) send(from, Ack{s_.rank.term, s_.rank, mine});
    if (s_.journal.has_beyond(m.vv)) send(from, Entries{0, s_.journal.beyond(m.vv)});
}
```
<!-- /snippet -->

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp#adopt -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp)

```cpp
// A checkpoint at least as far as this replica's replaces its work; its
// entries are merged in any case. Durable before anything else happens.
void receive(ReplicaId from, const CheckpointMsg& m) {
    const bool merged = s_.journal.merge(m.entries);
    if (s_.role == Role::main) {
        if (merged) s_.dirty = true;
        meet(from, m.rank, s_.journal.version());
        return;
    }
    if (!passive() || m.rank < s_.rank) {
        if (merged) persist();
        return;
    }
    const bool adopt = m.rank > s_.rank && m.work;
    if (adopt) {
        s_.rank = m.rank;
        s_.work = *m.work;
    }
    if (adopt || merged) persist();
    if (s_.role != Role::sleeping) {
        follow(from, s_.role, Cause::adopted);
    } else {
        s_.leader = from;
    }
}
```
<!-- /snippet -->

**Two mains meet.** When the partition heals, the main that is behind steps
down at once. The main that is ahead answers with its own beat and pauses
for a round, so its rank stays what the other one compares with. Such a
conflict is settled in at most two exchanges, even while both mains keep
working.

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp#meet -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp)

```cpp
// Two mains met. The one behind steps down and follows; the one ahead
// tells the other and pauses its work for a round, so that its rank
// stays what the other compares with.
void meet(ReplicaId from, const Rank& theirs, const VersionVector& vv) {
    if (from == s_.self) return;
    checkpoint();
    if (theirs > s_.rank) {
        out_.push_back(StopWork{});
        stop(Timer::beat);
        stop(Timer::checkpoint);
        stop(Timer::round);
        s_.frozen = false;
        s_.leader = from;
        become(Role::sleeping, Cause::better_main);
        set(Timer::suspect);
        send(from, Ack{s_.rank.term, s_.rank, s_.journal.version()});
        if (s_.journal.has_beyond(vv)) send(from, Entries{0, s_.journal.beyond(vv)});
        return;
    }
    send(from, beat());
    if (!s_.frozen) {
        s_.frozen = true;
        out_.push_back(StopWork{});
    }
    set(Timer::round);
}
```
<!-- /snippet -->

**Restarts.** After a restart, a replica comes back as `recovering`,
whatever it was. It gets a new incarnation and listens before it may run
for election. A former main does not continue on its own; if nobody is
further along, it wins the election.

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp#recovered -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp)

```cpp
// After a restart the replica comes back as `recovering`, whatever it
// was, with a new incarnation (durable before it observes anything), and
// drops the working state beyond its checkpoint.
void on(const Recovered&) {
    const Role from = s_.role;
    s_.incarnation += 1;
    s_.next_entry = 1;
    s_.timers = 0;
    s_.frozen = false;
    s_.replies.clear();
    s_.progress = s_.rank.progress;
    s_.done = s_.rank.done;
    s_.work_now = s_.work;
    s_.dirty = false;
    if (from == Role::main) out_.push_back(StopWork{});
    s_.role = Role::recovering;
    out_.push_back(Transition{from, Role::recovering, Cause::recovered});
    persist();
    set(Timer::suspect);
}
```
<!-- /snippet -->

**The end.** When the work is done, the main becomes `delivering` and
delivers the result to the sink, retrying with every beat. Whatever the sink
answers, the group is done. The host's registry records a tombstone and
spreads it to the other hosts by gossip. A replica learns of the tombstone
from its host, even after being cut off for a long time, and ends. The owner
can end a group at any time by writing a tombstone (cancel), and can
redirect it to another sink if the sink cannot come back.

<!-- snippet: cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp#sink -->
Source: [`cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/common/include/paglets/protocols/zombie/zombie_impl.hpp)

```cpp
// Whatever the sink says, the group is done: it holds a result.
void on(const SinkAnswer&) {
    out_.push_back(Tombstone{});
    end(Cause::delivered);
}

void on(const RegistryUpdate& in) {
    s_.sink = std::max(s_.sink, in.sink);
    if (in.tomb) end(Cause::tomb);
}
```
<!-- /snippet -->

## The hosts' registry and live checks

Every host runs the `zombies` system paglet (see
[System paglets](system-paglets.md)). It is the host's registry of the
groups it knows:

- **One replica per host.** A replica joins its group on its host. A second
  replica of the same group on that host is refused and ends, and so is a
  replica of a group that ended.
- **Tombstones and redirects reach every host.** A replica that delivered
  the result records a tombstone on its host. The owner cancels a group or
  redirects it to another sink on any host. Every host sends what it knows
  about ended and redirected groups to every other host every few seconds,
  and keeps it in its state directory. A merge keeps an ending and the
  highest sink number, so all hosts agree as soon as they talk, whatever the
  order. A host that was offline learns it when it returns, and so do its
  replicas. Tombstones are kept for 30 days.
- **Traces.** Replicas record their protocol steps on their host: every role
  change with its cause and the replica's rank, and the sink's answers.
  `paglets-host remote zombies traces GROUP` collects them from every live
  host. `paglets-host remote zombies check GROUP` checks them against the
  specification: every role change in the table, no replica's rank falling
  except across a restart, nothing after a replica ended, and at most one
  result accepted (one more for every redirect).

```bash
H=build/macos-arm64/host/paglets-host
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
$H remote zombies list $R
$H remote zombies check 0123456789abcdef0123456789abcdef $R
$H remote zombies cancel 0123456789abcdef0123456789abcdef $R
```

## Restarts

Every paglet on a host with a state directory comes back from its last
memory image after the host restarts, the zombie's replicas included. Three
runtime features (ABI v1.2) make that usable for the protocol; see
[SDK reference](sdk-reference.md#lifecycle-children-and-timers):

- `checkpoint()` stores the image right after the current handler. A
  replica calls it whenever a step outputs `Persist`, and sends that step's
  messages only from its next handler, after the image is stored. This is
  how "durable before visible" is kept.
- `set_restart(true, true)` asks for `on_recovered` after a restart: the
  replica's `Recovered` input.
- `set_restart(false)` is for paglets that should not come back.

## Guarantees

The protocol is checked against these properties:

| Property | Kind | Meaning |
|---|---|---|
| `one_result` | safety | at most one sink holds a result (one more for every redirect the owner made) |
| `best_progress_kept` | safety | while the group lives, some replica durably holds the best checkpoint that was ever durable |
| `observations_kept` | safety | while the group lives, every observation that was ever durable is durable on some replica |
| `legal_transitions` | safety | every role change is in the table |
| `one_main` | recovery | from every reachable state, once faults stop: a single main, followed by every other replica with its checkpoint and journal (or the group ends) |
| `group_ends` | recovery | from every reachable state, once faults stop: every replica ended, after a sink accepted the result (or the owner cancelled) |

Safety properties hold in every reachable state, whatever the faults. A
recovery property states that the goal stays reachable: whatever the faults
did, once they stop, the group can still get there. No state is a dead end
and no state traps the group. On a finite state graph, any scheduler that
eventually takes every possible step reaches the goal with probability 1.

## How it is checked

`paglets-protocol-check` (`cpp/tools/protocol_check`) checks the protocol
in two ways, both built on `paglets::verify`
(`cpp/common/include/paglets/verify/`):

- **Exhaustively**: every reachable state of a small world, with hosts that
  crash and restart (losing what was not durable), links that are cut and
  healed, lost and duplicated messages, sinks that go down, cancels and
  redirects, and timeouts that fire too early. Faults are limited by a
  budget per world. Messages travel in one ordered channel per sender and
  receiver, as the transport's queues per peer do.
- **Timed**: thousands of random runs with virtual clocks, the real timer
  durations, message latency, crashes, cuts, losses and duplicates. They
  check the same properties at scale and measure how long takeovers take.

```bash
cpp/build/macos-arm64/tools/protocol_check/paglets-protocol-check --suite full
cpp/build/macos-arm64/tools/protocol_check/paglets-protocol-check --simulate --seeds 10000
cpp/build/macos-arm64/tools/protocol_check/paglets-protocol-check --trace 42
```

A violated property is reported with the shortest sequence of steps that
leads to it. `--trace SEED` prints the role changes and faults of one
simulated run. The ctest `unit` runs small worlds and 200 timed runs on
every build; `cpp/tools/update_results.sh` records the larger runs below.

<!-- results: zombie_protocol -->
> [!NOTE]
> No results are recorded yet. Run `cpp/tools/update_results.sh` (see
> [Example paglets](index.md#test-results)).
<!-- /results -->

> [!NOTE]
> The model has limits: a few replicas, a few faults per world, a short
> job. The timed runs cover long jobs and many faults, but they sample
> instead of enumerating. Both run the same transition function that the
> guest SDK runs.
