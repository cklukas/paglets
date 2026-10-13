# paglets/cpp: zombie paglets (concept, WP23–WP30)

Status: WP23–WP30 implemented (2026-10-13; live run on three hosts in
docs/examples/mesh-chronicle-log.md); the protocol is specified in
`cpp/common/include/paglets/protocols/zombie/zombie.hpp` and documented in
`docs/zombie-protocol.md`, which take precedence over this concept where they
differ. Decisions made while implementing are listed in section 12. A
**zombie paglet** keeps running as long as at least one host of its mesh is
alive, and resumes after all of them were down. One replica, the **main**,
does the work; replicas on the other hosts stay silent, hold its latest
checkpoint, and take over when it disappears. When the work is done,
exactly one result is delivered and every replica ends.

## 1. What the runtime already gives us

- **Restart from images.** On a host with a state directory (every `serve`
  host), each active paglet is checkpointed after a handler when its last
  image is older than `checkpoint_interval` (default 1 s); deactivation and
  shutdown store images; `recover()` brings every stored paglet back after a
  restart, with its capabilities and its timers re-armed. Requests in flight
  are answered `timeout`. What is missing: the paglet **cannot tell** that it
  was restored, **cannot ask** for an image at a point of its choosing, and
  **cannot opt out** (section 7).
- **Clones on other hosts** (ABI v1.1, `clone_raw(..., destination)`),
  remote endpoints with host hints, forwarding after moves.
- **Content exchange**: `artifacts` capabilities name content by hash and
  holder; any host fetches it directly. Large checkpoints travel this way.
- **Gossip** between nodes (ledger anti-entropy, mesh-info snapshots); node
  frames share the transport.
- **`files` `create_new`**: an atomic "only if absent" write, usable as a
  result sink.

## 2. The ground rule: several mains may run, one result counts

A replica that hears nothing cannot tell "the others are dead" from "my
network is cut"; a main cannot tell "I am alone" from "I am cut off". The
group therefore stays **available**: a cut-off replica becomes main after the
election timeout, so during a partition each side may have its own main.

Uniqueness is moved to where it can be decided atomically, the **sink**:

- Every result carries the group ID. A sink accepts the **first** result for
  a group and answers every later one with `already_delivered` (and who
  delivered it). Sinks offered by the framework:
  - a file on a named host, written with `files` `create_new`
    (`<root>/<path>`; the existing file means "already delivered");
  - a sink paglet identified by its ID: its handlers run serially, it stores
    the accepted group IDs durably (`storage`) **before** it answers;
  - custom sinks implement the same two-answer contract.
- A main that delivers, or learns `already_delivered`, finishes the group:
  either way, the work is done.
- When mains meet again, they agree on one (section 5); the loser **forgets**
  its work and goes back to sleep, receiving checkpoints from the winner. What
  it **observed** meanwhile (its journal) is not lost: it is handed to the
  winner and merged first (section 5), so that, for example, the connections
  lost on both sides of a partition end up in one record of the whole mesh.

Work restarted from a checkpoint repeats everything since that checkpoint
(at least once). Side effects other than the result should be idempotent;
the framework gives each one a key (group ID, term, sequence number).

## 3. Group, replicas, roles

- **Group**: a 128-bit group ID, the owner, the module, the replica policy
  (every host, the default, or at most N), the sink. Fixed at creation.
- **Replica**: one paglet per host and group (its own paglet ID). The host's
  `zombies` system paglet (WP26) enforces one per host: a second replica of
  the same group arriving on a host is refused; if two exist after restored
  images, the one with the lower paglet ID stays.
- **Roles**: `recovering`, `sleeping`, `candidate`, `main`, `handing-over`,
  `delivering`, and the end (disposed). Every role has a timeout or an event that leaves
  it (section 6).
- **Term**: a counter that only grows. Every message carries the sender's
  term and lineage; old terms are ignored, so delayed beats of a former main
  have no effect.
- **Checkpoint**: `(term, seq, progress, work, journal)`. `work` and
  `journal` are the paglet's state (msgpack; section 5), `progress` a number the
  paglet defines (monotonic within a lineage), `seq` counts checkpoints.
  Small states go inline, large ones as an artifact capability. Memory
  images stay the local restart mechanism; replication uses the paglet's own
  state, which is smaller and independent of the module version.
- **Membership**: the replica hosts, part of the replicated state, changed
  by the main only: hosts that join the mesh (ledger) get a replica by clone,
  hosts removed from the ledger lose theirs. A replica that is not in the
  membership of the current main ends.

## 4. Protocol

**Beats.** The main sends `beat(term, seq, progress, membership digest)` to
every replica every `H` (default 2 s) and a checkpoint every `C` (default
10 s, or when the paglet calls `checkpoint()`). Replicas answer `ack(term,
seq)` and ask for the latest checkpoint when they missed one.

**Suspicion.** A sleeping replica without a beat for `E` (default 5·H plus
random jitter, measured on its monotonic clock) becomes a candidate.

**Election.** The candidate asks every member it knows for its status
(latest checkpoint, host key ID, role) and waits one round (`R`, default
2·H). Among itself and the answers it ranks by the order of section 5; if it
is the best, it takes term `max(seen) + 1` and becomes main; otherwise it
goes back to sleeping and the best one, asked by name, takes over. Nobody
answering means it is the sole survivor: it becomes main. Two candidates
that claim at once are resolved by section 5 the moment they hear each
other.

**Restart.** A replica restored from an image (one host or the whole mesh
restarted) starts as `recovering`, whatever its role was, and listens for
beats for `E` before it may run for election. A former main does not resume
on its own; with its possibly newer local checkpoint it wins the election if
nobody is further. After a full mesh shutdown the group is down until the
hosts come back, then the most advanced replica continues.

## 5. When mains meet again

Two mains (or a main and a replica that ran alone) meet when a beat, status
or vote of another lineage arrives. Both compute the same winner:

1. a group that delivered (or learnt `already_delivered`) beats any other;
2. otherwise the higher `progress`;
3. then the higher `(term, seq)`;
4. then the lower host key ID.

The order is total, so any number of partitions converge to one main, in
any order of meeting.

**State in two parts.** A zombie's state is split:

- **work**: what the main computes or collects as the job's own state;
  winner takes all, the loser's work is forgotten;
- **journal**: observations, a set of entries keyed by `(observer replica,
  entry number)`. Journals are merged by union, which is commutative,
  associative and idempotent, so the merged journal is the same whatever the
  order of meetings and however often an entry arrives. Each checkpoint
  carries a version vector: per observer replica, the highest entry number
  it contains.

**Hand-over.** The loser becomes `handing-over`: it sends the winner its
journal entries beyond the winner's version vector and keeps them (durably,
in its own state) until a checkpoint of the winner covers them. Only then
does it forget its work and sleep. If the winner disappears meanwhile, the
entries are still with the loser and go to whoever wins next; a crash at any
point loses no observation that reached a checkpoint. The winner then calls
the paglet's `reconciled(work, merged journal, conflict)` hook once, with
both lineages' terms, members and divergence time, so the paglet can
summarize what happened (the chronicle: section 9).

Observations arriving after the result was delivered cannot change it; they
are counted in the tombstone (`late entries`).

## 6. Roles, timeouts, and exits (no dead ends)

| Role | Leaves on | To |
| --- | --- | --- |
| `recovering` | beat of a current lineage | `sleeping` |
| | no beat for `E` | `candidate` |
| | tombstone for the group | end |
| `sleeping` | no beat for `E` | `candidate` |
| | tombstone, or not in the main's membership | end |
| `candidate` | best in its round | `main` |
| | someone better | `sleeping` |
| | better lineage seen | `sleeping` |
| `main` | better lineage seen (section 5) | `handing-over` |
| `handing-over` | a winner's checkpoint covers its journal | `sleeping` (forgets its work) |
| | winner gone (no beat for `E`) | `candidate`, journal kept |
| | delivered lineage seen | end (entries counted as late) |
| | work done | `delivering` |
| | owner cancels | end, tombstone |
| `delivering` | sink: accepted or `already_delivered` | tombstone, end |
| | sink unreachable | retry with back-off; replicas keep sleeping on its beats |
| | higher sink number (redirect) | `delivering` to the new sink |
| | better lineage seen (a delivered one) | end |

Every role has a timer, so a lost or duplicated message never leaves a
replica stuck. The only unbounded wait is an unreachable sink; it is visible
to the owner, who can fix the sink or cancel.

**Tombstones.** When a group ends (delivered or cancelled), the hosts'
`zombies` system paglets record a tombstone `(group, final term, reason)`
and spread it by gossip with anti-entropy, so a replica that was cut off at
the end learns it when it reconnects, even if every other replica is gone,
and ends instead of working on. Tombstones expire after `T_tomb` (default
30 days). A replica that was cut off for longer can run again, but it cannot
deliver a second result, because the sink refuses it.

**Redirecting the sink.** If a sink cannot come back (for example its host
was removed from the mesh), the owner redirects the group:
`paglets-host mesh zombies redirect GROUP SINK` writes a signed sink record
with a sink number that only grows; it spreads by gossip like tombstones and
reaches offline replicas when they return. Mains deliver only to the sink
with the highest number they know; a main that learns a higher number while
`delivering` switches to it. The old sink, if it is ever reached again, is
told that it was replaced. Redirecting is the owner's statement that the old
sink did not accept a result: if it had in fact accepted one before it
became unreachable, two sinks hold a result; the group's status and the
tombstone record this case ("delivered to sinks 1 and 2"), it cannot go
unnoticed.

**Kill switch.** The owner (or an admin) can always end a group:
`paglets-host mesh zombies cancel GROUP` writes the tombstone, which reaches
offline replicas when they come back. A module that loses the mesh's trust
ends all replicas, as for any paglet.

## 7. Runtime features (any paglet, not only zombies)

- **`recovered` event** (EventKind 8, ABI v1.2): delivered once after
  `recover()`, with the time the image was taken and the restart time.
  Paglets that care about the outside world revalidate here.
- **`checkpoint()`**: store the image after this handler, regardless of the
  interval (a durable point before an effect).
- **Restart policy** `set_restart(always | never)`: `always` is the default
  and today's behavior; `never` paglets are not stored and do not come back
  after a restart.
- CLI: `paglets-host list` shows the policy and the time of the last image.
- **Links in `mesh-info`**: each host's snapshot lists its peers with the
  link state (up or down, since when, the transport's last failure reason,
  time of the last frame received). Today the transport knows this
  (`send_failure`), but nobody can ask. With it, any paglet sees the link
  graph of the hosts it can reach.

**Hosts without zombie support.** A group can be created only if every
host enrolled in the mesh offers the `zombies` service (mesh-info offers);
otherwise creation fails, naming the hosts. A host without it that enrolls
later gets no replica; the group's status lists it, and it gets a replica
once it offers the service.

## 8. SDK

```cpp
struct Chronicle : paglets::patterns::Zombie<ChronicleWork, ChronicleEntry> {
    // One slice of work while this replica is the acting main (well within
    // the handler budget); observations go to journal(). Returns true when
    // the work is done.
    bool step(ChronicleWork& w, Journal& journal) override;
    std::int64_t progress(const ChronicleWork& w) const override;
    // After lineages met: the winner's work, the merged journal, and what
    // conflicted (both lineages, their members, divergence time).
    void reconciled(ChronicleWork& w, const Journal& journal, const Conflict& c) override;
    // The result for the sink, delivered once.
    paglets::Bytes result(const ChronicleWork& w, const Journal& journal) override;
};
```

`Journal` is the framework's set with version vector; entries are the
paglet's own type (msgpack). A paglet without observations uses an empty
entry type and never hands anything over.

The base class runs the roles, beats, elections, checkpoints, membership,
placement and delivery; the paglet sees `step()` only while it is the acting
main, and asynchronous work (requests to services) is fenced by the term, so
replies to a demoted main are dropped. The protocol core is a pure C++ state
machine without ABI calls (inputs: messages and clock ticks; outputs:
messages and actions), so the same code runs in the guest and in a
deterministic simulator on the host.

## 9. Demo: the Mesh Chronicle

A zombie that writes down the life of its mesh and nothing else:

- **Lifetime of the mesh**: its genesis time and hosts enrolled and removed
  (from the ledger), hosts appearing and disappearing, how many were up over
  time, restarts of the whole mesh, total time the chronicle was alive and
  dead.
- **Connectivity**: every 10 s the main samples the links between hosts and
  journals each change: link `A–B` lost (with the transport's reason) or
  restored, with time and observer. Partitions are derived from the link
  graph as connected components: which groups of hosts were cut from each
  other, from when to when.
- **Its own life**: takeovers (from which host, term, time without a main),
  and conflicts.
- **Reconciliation**: when two lineages meet, both journals are merged
  (section 5), so the lost connections observed on either side of a
  partition are kept for the full mesh; `reconciled()` then adds a summary
  entry: the partition (its sides, from the merged link changes), its
  duration, the mains on each side and their terms, the winner, and the
  links that were down on each side.
- `progress` is the number of samples of the winning lineage; checkpoints
  carry work and journal.
- it ends after a configured span (for example 24 hours of chronicle) or
  when the owner sends `finish`, and delivers the chronicle as a Markdown
  file through a `create_new` sink on a chosen host, plus a `user-info`
  notification; every replica then ends.

Kill hosts, cut the network, restart the whole mesh: the chronicle shows
each event, and exactly one file appears.

## 10. Specification and checking in the code base

The protocol is specified, checked and monitored inside paglets/cpp, not in
an outside tool, so specification, implementation and checks cannot drift
apart.

**The specification is code.** `cpp/common/include/paglets/protocols/zombie/`
holds the protocol as a pure C++ state machine: replica state, messages,
the transition function `step(state, input) -> (state, outputs)` for every
role, and the properties as named predicates. The guest SDK (section 8) runs
exactly this code; nothing in it calls the ABI, a clock or the network.
The docs page `docs/zombie-protocol.md` explains the protocol and takes the
transition table, the ranking rule and the properties from the sources with
`<!-- snippet: ... -->` markers, so the published specification is the
compiled one (`paglets-docs-sync`, checked by the `docs_sync` ctest).

**The checker is a library and a tool.**

- `paglets::verify` (`cpp/common/include/paglets/verify/`): a generic
  explicit-state model checker for protocols written this way: a fault
  model (message loss, duplication, reordering, crash, restart, partitions
  and their healing, all as nondeterministic events), breadth-first search
  with state hashing and symmetry reduction over replicas, safety
  properties on every state, liveness on the finite state graph (from every
  reachable state, once faults stop and the network is connected, the goal
  is reachable and no cycle avoids it), and counterexamples as the shortest
  event trace. Other protocols of the mesh (moves, location records) can be
  specified and checked the same way later.
- A deterministic simulator in the same library: the protocol under timed
  random fault schedules with virtual clocks, thousands of seeds per run,
  timing bounds (a main within `E + R` after the main disappears),
  reproducible by seed.
- `paglets-protocol-check` (`cpp/tools/protocol_check`): runs both for a
  protocol and its parameters (`--protocol zombie --replicas 3 --terms 3`,
  `--simulate --seeds 10000`), prints counterexamples as readable traces, and
  writes `docs/results/zombie-protocol.json` (states explored, properties,
  seeds, bounds), which the docs page shows via `<!-- results: ... -->`. The
  ctest runs it with small bounds on every build; `update_results.sh` runs
  the larger bounds.

**Properties checked** (named in the specification): at most one result
accepted; no observation that reached a checkpoint is lost by a hand-over;
journals of connected replicas converge; after partitions heal, one main;
after a tombstone in a connected network, every replica ends; no dead end
(every state has a way out without new faults), no cycle without progress.

**Live conformance (the services).** The `zombies` system paglet records
each replica's protocol events on its host (role changes with term and
cause, checkpoints, hand-overs, deliveries; a bounded ring, kept in its
state directory). `paglets-host mesh zombies check GROUP` collects these
traces from the hosts and replays them through the same transition function
and properties: every recorded step must be a legal transition, and the
properties must hold on the merged trace. Violations are reported to the
owner through `user-info` and by the CLI, with the offending trace. The same
check runs on the traces of the `cli_mesh` fault tests and the live runs.

**Mesh tests**: real hosts on one machine (ctest `cli_mesh`), with
`kill -9`, restarts of all hosts, and a test-only peer block in the
transport for partitions; then live machines.

## 11. Work packages

| WP | Content |
| --- | --- |
| WP23 | Protocol specification as code (`protocols/zombie`: states, messages, transitions, properties) and the docs page `docs/zombie-protocol.md` with snippets from it |
| WP24 | Runtime/ABI v1.2: `recovered` event, `checkpoint()`, restart policy; store and CLI; tests |
| WP25 | `paglets::verify` (model checker with fault model, symmetry reduction, liveness, counterexamples; deterministic simulator) and `paglets-protocol-check`; results JSON; ctest with small bounds |
| WP26 | `zombies` system paglet: per-host group registry, one replica per host, tombstones with gossip, `cancel`, protocol event traces, `mesh zombies check` (live conformance), contract; node frames |
| WP27 | SDK `patterns::Zombie<Work, Entry>` on the specification's state machine: roles on timers, journals and hand-over, endpoints between replicas, inline and artifact checkpoints, placement by clone, sinks (file `create_new`, sink paglet), term fencing |
| WP28 | Links in `mesh-info` snapshots; Mesh Chronicle example (lifetime, link changes, partitions, reconciliation summaries), peer block in the transport (tests only), `cli_mesh` fault tests |
| WP29 | CLI (`mesh zombies list/status/cancel/redirect/check`), docs pages (concept, SDK, example with results), README updates |
| WP30 | Live runs on real machines; results recorded |

## 12. Decisions made while implementing

- **Journals are always merged.** There is no `handing-over` role: every
  replica sends the main the entries it lacks with each beat, and journals
  only grow while the group lives. The loser of a conflict forgets its work,
  not its observations. There is no `reconciled()` hook; a paglet derives
  partitions from the merged journal when it builds its result.
- **Two mains meet:** the one behind steps down at once and follows; the one
  ahead answers with its own beat and pauses its work for one round, so its
  rank stays fixed while the other compares (an earlier version ran an
  election round, which the model checker showed to cause needless repeated
  elections from stale beats).
- **Membership** is fixed when the group is created: the hosts with fresh
  `mesh-info` snapshots, all of which must offer `zombies`. Hosts that join
  later get no replica (planned for a later version).
- **The sink is a host's `zombies` system paglet** (`host:NAME`): it accepts
  the first result per group and sink number and stores it durably before it
  answers; `paglets-host remote zombies result GROUP` fetches it. Redirects
  raise the sink number.
- **Verification is C++ only:** `paglets::verify` (explicit-state model
  checker with parallel levels and hash compaction) and a timed simulator,
  both in the code base, run by `paglets-protocol-check`. Messages travel in
  per-link FIFO channels (as the transport's queues per peer); a resend equal
  to a queued message is merged with it; timeouts that fire before what they
  detect is true count as faults (eventually accurate timeouts). Two-replica
  worlds are checked completely, three-replica worlds to a bounded depth
  (safety only), and the timed runs cover three replicas with many faults.
- **ABI v1.2:** `checkpoint()` stores the image right after the handler; the
  SDK defers the sends of a step that outputs `Persist` to its next handler,
  which keeps "durable before visible". `set_restart(restart,
  notify_recovered)` and `on_recovered`.
