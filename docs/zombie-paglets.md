# Zombie paglets

A zombie paglet cannot die while one host of its mesh is alive. It has a
replica on every host. One replica, the main, works; the others sleep with
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
    std::string result_name() const override;  // a file name for it at the sink
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
    step(work_, [this, gen](bool done) {
        if (gen != work_gen_ || !acting()) return;  // stopped meanwhile: the result is void
        run(zp::Worked{progress(work_), done, encode(work_)});
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

- **Placement.** The group gets a replica on every host with a fresh
  `mesh-info` snapshot. Every one of them must offer `zombies` (every host
  of this version does); otherwise the group does not start, and
  `on_zombie_failed` names the hosts that lack it. The creator clones
  itself to the other hosts. Every replica joins the `zombies` registry of
  its host; the hosts tell each other which replica they hold, and each
  replica gets endpoints to the others from its own host. It asks again
  until it knows all of them, and after a restart.
- **The sink** is `host:NAME`, a host of the mesh by its ledger name or key
  ID. That host's `zombies` system paglet accepts the first result per
  group and stores it. `paglets-host remote zombies result GROUP` fetches
  it. The owner can redirect a group to another sink.
- **Timing** (`ZombieTiming`): beats every 2 s, a checkpoint at most every
  10 s, a takeover after 10 s without a beat (plus up to 5 s of random
  jitter), election rounds of 4 s, and a pause of 1 s between two steps.
  The simulator measures takeovers within 20 s with these values.

## Observations

`observe(bytes)` (or `observe_value(value)`) adds an entry to the journal,
on the acting main only. Journals are merged across replicas and across
partitions: when two mains meet, the one behind forgets its work but hands
over its journal. So a result can describe what happened on every side,
which is what the Mesh Chronicle does with lost connections.

Keep the work small and the journal for what must not be lost: the work is
replaced by the winner's after a partition, the journal is not.

## Restarts and durability

A replica survives a restart of its host from its memory image (ABI v1.2).
The base class asks for `on_recovered` and comes back as `recovering`:
it listens for beats first and does not resume as main on its own.

The rule "durable before visible" is kept by the base class: when the
protocol makes a checkpoint (a `Persist` output), it calls
`paglets::checkpoint()` and sends that step's messages only from its next
handler, after the host stored the image:

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
- **Checking a run.** `paglets-host remote zombies check GROUP` collects the
  protocol steps that every host recorded and checks them against the
  specification.

## Building

A module with a zombie paglet lists the services it uses:

```cmake
paglets_add_module(chronicle
    SOURCES chronicle/chronicle.cpp
    SCHEMA_HEADER chronicle/chronicle_msgs.hpp
    SCHEMA_NAMESPACE chronicle_msgs
    SERVICES zombies mesh_info user_info
    PATTERNS)
```
