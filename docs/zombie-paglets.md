# Zombie paglets

A zombie paglet survives host failures by keeping replicas on eligible
hosts of its mesh: hosts with a fresh snapshot that offer `zombies`. One
replica, the main, works; the others sleep with
its latest checkpoint and take over when it disappears. After a partition,
the replicas agree on the one that got furthest, and what both sides
observed is merged. After the whole mesh was down, the most advanced
replica goes on. When the work is done, exactly one result reaches the
sink, and every replica ends.

This page shows how to write one with `paglets::patterns::Zombie<Work>`
(`cpp/sdk/include/paglets/patterns/zombie.hpp`). The protocol behind it,
and how it was checked, is in [The zombie protocol](zombie-protocol.md).
The [Mesh Chronicle](examples/mesh-chronicle.md) is a complete example.

## The class

```cpp
#include <paglets/patterns/zombie.hpp>

struct Chronicle : paglets::patterns::Zombie<Work> {
    // One step of the work on the acting main; call `finished` once, with true when done.
    void step(Work& work, std::function<void(bool done)> finished) override;
    // How far the work got (it only grows).
    std::int64_t progress(const Work& work) const override;
    // The result for the sink, from the final work and the merged journal.
    paglets::Bytes result(const Work& work, const std::vector<paglets::protocols::zombie::Entry>& journal) override;
    std::string result_name() const override;  // optional label; default "result.bin"
};
```

`Work` is the job's state, encoded with `paglets_encode` and
`paglets_decode` (types from a schema header have them). The main
replicates it with every checkpoint. A replica that takes over continues
from the work of the latest checkpoint it holds.

A step may wait for replies, for example from `mesh-info`. If the main
stopped meanwhile (it met a main further along, it was asked to pause for
an election), its `finished` call is ignored:

<!-- snippet: cpp/sdk/include/paglets/patterns/zombie.hpp#run_step -->
Source: [`cpp/sdk/include/paglets/patterns/zombie.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/sdk/include/paglets/patterns/zombie.hpp)

```cpp
void run_step(std::uint64_t gen) {
    // The completion owns the step's storage, including while an async
    // callback holds Work&. A stale step never aliases the next lineage.
    if (!failure_reason_.empty()) return;
    // A C++ copy could retain shared mutable members. The Work codec
    // defines the independent state that is also used by other replicas.
    auto local = std::make_shared<Work>();
    if (!decode(encode(work_), *local)) {
        failure_reason_ = "cannot copy step work";
        ++work_gen_;
        record_failure();
        return;
    }
    auto completed = std::make_shared<bool>(false);
    step(*local, [this, gen, local, completed](bool done) {
        if (std::exchange(*completed, true) || gen != work_gen_ || !acting()) return;
        auto bytes = encode(*local);
        Work committed{};
        if (!decode(bytes, committed)) {
            failure_reason_ = "cannot commit step work";
            ++work_gen_;
            record_failure();
            return;
        }
        // Keep the callback's storage independent and usable even after
        // completion: a repeated late callback cannot mutate work_.
        work_ = std::move(committed);
        run(zp::Worked{progress(work_), done, std::move(bytes)});
        if (!done && gen == work_gen_ && acting()) schedule_step();
    });
}
```
<!-- /snippet -->

## Starting a group

The creator is a paglet like any other. It starts the group, typically
from a request handler:

```cpp
start_group("host:mac", initial_work, timing);
```

- **Startup outcome.** `start_group` returns while the landscape request
  and registry join are still pending. The creator's `on_zombie_started()`
  hook runs after its local registry accepts the join; other replicas may
  still be arriving. Clones and recovered replicas do not get this hook.
  Defer a startup request's reply until this hook, and answer refusals from
  `on_zombie_failed(reason)`. Its default implementation logs the reason and
  makes a best-effort error notification through the local `user-info`
  service. Overrides can call the base implementation to retain that behavior.
- **Placement.** The group gets a replica on every host with a fresh
  `mesh-info` snapshot. Every one of them must offer `zombies` (every host
  of this version does); otherwise the group does not start, and
  `on_zombie_failed` names the hosts that lack it. The creator starts as
  the main and clones itself to the other hosts. Every replica joins the
  `zombies` registry of its host; the hosts tell each other which replica
  they hold, and each replica gets endpoints to the others from its own
  host. It asks again until it knows all of them, and after a restart.
- **Growth.** Every `expand_ms` (30 s by default) the acting main looks for
  hosts of the mesh that offer `zombies` but hold no replica of the group,
  makes one there and adds it to the membership. A host that joins the mesh
  later is occupied this way within about a minute; a host that does not
  offer `zombies` (an older version) is left out. If the mains of two sides
  of a partition both make a replica on the same new host, that host's
  registry refuses the second one, which ends. A replica is named by its
  host's key ID, so replicas made by different mains never get the same
  name. Replica IDs use the first 16 hex digits of the host key ID. If two
  hosts collide in those 64 bits, expansion logs a warning and skips the
  additional replica; use hosts with distinct replica IDs.
- **The sink** is `host:NAME`, a host of the mesh by its ledger name, full key
  ID or an unambiguous prefix of at least 8 hex digits. Full key IDs take
  precedence over names; an unknown full ID does not resolve as a name.
  Ambiguous names or name/prefix collisions return `invalid_argument`,
  a permanent refusal. An unknown host remains retryable (`not_found`).
  That host's `zombies` system paglet accepts the first result per
  group and stores it. `paglets-host remote zombies result GROUP` fetches
  it when connected to the sink host. The owner can redirect a group to
  another sink. Results live in the host's zombie registry, keyed by group
  and sink number. `result_name()` is an optional label, not a filesystem
  path or deduplication key; two groups may use the same label. The CLI
  prints the bytes to stdout and the label to stderr.
- **Result refusal.** `too_large`, `quota`, `denied`, `invalid_argument`,
  `malformed` and `unsupported` end the group as failed. Each running replica
  calls `on_zombie_failed(reason)` once before disposal. The failure ending
  is stored before notification; a storage failure delays ending and retries.
  Make application effects in this hook idempotent across recovery.
  `timeout`, `gone` and `not_found` remain retryable. A missing sink produces
  a warning; the owner can redirect or cancel the group.
  An immediate `quota` from a full service mailbox is retryable; a quota
  refusal returned by the sink operation is permanent.
  The encoded delivery request must fit the SDK's 1 MB document limit,
  including its group and filename. The host's result registry has a
  separate 4 MB limit for mesh frames. Work and journal entries exchanged
  between replicas must also fit the document limit.
- **Timing** (`ZombieTiming`): beats every 2 s, a checkpoint at most every
  10 s, a takeover after 10 s without a beat (plus up to 5 s of random
  jitter), election rounds of 4 s, and a pause of 1 s between two steps.
  With the simulator's default latency bound, `takeover_bound()` checks a
  25.2 s upper bound after a main crashes. The recorded 10 000-run campaign
  in [the protocol results](zombie-protocol.md#how-it-is-checked) observed
  a longest takeover of 18.9 s; that is a sampled measurement, not the
  bound. The main looks for new hosts every 30 s (`expand_ms`).

## Observations

`observe(bytes)` (or `observe_value(value)`) adds an entry to the journal,
on the acting main only. It returns `false` while the replica is sleeping,
frozen for an election, delivering, or otherwise not the acting main. Keep
a pending observation until a later call returns `true`. Journals are merged across replicas and across
partitions: when two mains meet, the one behind forgets its work but hands
over its journal. So a result can describe what happened on every side,
which is what the Mesh Chronicle does with lost connections.

Keep the work small and the journal for what must not be lost: the work is
replaced by the winner's after a partition, the journal is not.
An observation held only by a crashed host reaches the result only if that
host returns and shares it before the group ends. Persistence on one host
does not guarantee inclusion in a result produced while that host is down.

## Restarts and durability

A replica survives a restart of its host from its memory image (ABI v1.2).
The base class asks for `on_recovered` and comes back as `recovering`:
it listens for beats first and does not resume as main on its own. The
[SDK restart rules](sdk-reference.md#lifecycle-children-and-timers) explain
which capability/timer/request state survives and why the ordinary mailbox
is not replayed.

On a host with a state directory and restart enabled, the rule "durable
before visible" is kept by the base class: when the
protocol makes a checkpoint (a `Persist` output), it calls
`paglets::checkpoint()` and sends that step's messages only from its next
handler, after the host stored the image:

How the main occupies new hosts:

<!-- snippet: cpp/sdk/include/paglets/patterns/zombie.hpp#expand -->
Source: [`cpp/sdk/include/paglets/patterns/zombie.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/sdk/include/paglets/patterns/zombie.hpp)

```cpp
// The acting main makes a replica on every host of the mesh that offers
// zombies and has none yet (as far as the registry knows; the registry
// of that host refuses a second one, for example from the main of
// another side of a partition), and adds it to the membership. A host
// whose clone did not arrive is tried again after a while.
void expand(const std::vector<services::mesh_info::Snapshot>& hosts) {
    if (!acting()) return;
    const std::int64_t now = now_ns() / 1'000'000;
    std::vector<zp::ReplicaId> added;
    for (const auto& h : hosts) {
        const auto id = zp::replica_id(h.host);
        if (id == 0 || id == core_.self || registered_.contains(id) || !offers_zombies(h)) continue;
        if (auto it = hosts_.find(id); it != hosts_.end() && it->second != h.host) {
            log(abi::LogLevel::warning, "zombie: hosts " + it->second.substr(0, 16) + " and " +
                                            h.host.substr(0, 16) + " have the same replica ID; no replica");
            continue;
        }
        if (auto it = cloned_at_.find(h.host); it != cloned_at_.end() && now - it->second < 2 * timing_.expand_ms) {
            continue;  // on its way
        }
        auto me = self().derive(abi::DeriveSpec{});
        if (!me) return;
        std::vector<std::string> known = members();
        if (std::ranges::find(known, h.host) == known.end()) known.push_back(h.host);
        const detail::ZombieCloneArgs a{group_, known, h.host, hosts_[core_.self], sink_, timing_};
        auto clone = clone_raw(encode(a), {*me}, h.host);
        if (!clone) {
            (void)me->drop();
            log(abi::LogLevel::warning, "zombie: no replica on " + h.host.substr(0, 8) + ": " +
                                            std::string(abi::error_name(clone.error())));
            continue;
        }
        cloned_at_[h.host] = now;
        hosts_[id] = h.host;
        if (auto old = peers_.find(id); old != peers_.end()) (void)old->second.drop();
        peers_[id] = *clone;
        added.push_back(id);
    }
    if (!added.empty()) run(zp::AddMembers{added});
}
```
<!-- /snippet -->

And how the protocol's outputs are carried out:

<!-- snippet: cpp/sdk/include/paglets/patterns/zombie.hpp#carry_out -->
Source: [`cpp/sdk/include/paglets/patterns/zombie.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/sdk/include/paglets/patterns/zombie.hpp)

```cpp
// Carries out the outputs from `i` on. After a Persist the rest waits
// for the next handler, when the image is stored.
void carry_out(const zp::Outputs& outs, std::size_t i) {
    for (; i < outs.size(); ++i) {
        const auto& o = outs[i];
        if (std::holds_alternative<zp::Persist>(o)) {
            (void)checkpoint();
            deferred_.insert(deferred_.end(), outs.begin() + static_cast<std::ptrdiff_t>(i) + 1, outs.end());
            if (!flush_pending_) {
                flush_pending_ = true;
                SendOptions options;
                options.async = true;
                (void)self().send("zombie.flush", std::move(options));
            }
            return;
        }
        if (!deferred_.empty()) {  // keep the order behind earlier deferred outputs
            deferred_.push_back(o);
            continue;
        }
        apply(o);
    }
}
```
<!-- /snippet -->

## What to keep in mind

- **At least once.** A replica that takes over repeats the work since the
  last checkpoint. Effects other than the result should be idempotent.
- **One result, decided by the sink.** During a partition, both sides may
  finish and deliver. The sink accepts the first result; the other main
  learns `already delivered` and ends too.
- **Ending everywhere.** The host of the replica that delivered records a
  tombstone. Every host passes it on, so replicas end on every host, also on
  hosts that were offline and come back later. The owner can end a group at
  any time: `paglets-host remote zombies cancel GROUP`.
  A permanent result refusal also spreads an ending, with its failure reason.
  A replica waits for the registry acknowledgement before disposing. Hosts
  with a state directory write the result before acknowledging acceptance;
  hosts embedded without one retain results and endings only in memory.
- **Checking a run.** `paglets-host remote zombies check GROUP` collects the
  protocol steps that every host recorded and checks them against the
  specification.

## Building

A module with a zombie paglet can use `PATTERNS`, which includes the
`zombies`, `mesh_info` and `user_info` clients:

```cmake
paglets_add_module(chronicle
    SOURCES chronicle/chronicle.cpp
    SCHEMA_HEADER chronicle/chronicle_msgs.hpp
    SCHEMA_NAMESPACE chronicle_msgs
    PATTERNS)
```
