# Mesh Benchmark

The Mesh Benchmark answers a simple question: which machines of the mesh
are fast for which kind of work? It sends a clone of itself to every host.
Each clone waits for a compute slot on its host, then measures integer and
floating-point speed and memory copying in its own memory, and writing and
reading through its own storage. The paglet at home collects the numbers
and ranks the hosts.

Directory: [`cpp/examples/benchmark/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/benchmark)

## What it shows

- **Compute slots.** Every host admits compute work through its
  `compute-slots` system paglet. A host offers a number of slots (one per
  CPU unless it is started with `--slots N`). A job asks for a slot and
  runs at once, waits in the host's queue, or is sent on to a host with
  free slots. A clone of the benchmark takes one slot, so it does not run
  beside more compute jobs than the host has slots, and it gives the slot
  back when it is done (see
  [System paglets](../system-paglets.md#services) and the
  [compute design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-compute.md)).
- **Fan-out.** The `FanOut` pattern sends a clone to each host, collects
  one report from each, and counts clones that do not report before a
  deadline as failed (see
  [The patterns library](../writing-paglets.md#the-patterns-library)).
- **The paglet's own storage.** The disk test writes and reads through the
  `storage` system paglet: a key-value store of each paglet, which the host
  keeps in the paglet's scratch directory, within a quota of 16 MB. The
  paglet opens no files.
- **Work in steps.** A host gives every handler call at most 5 s. The
  benchmark runs one test per handler call and schedules the next test
  with a timer, so a test may take up to that budget.

## Why it is useful

The numbers measure what paglets actually do: the CPU and memory tests run
as WebAssembly inside the paglet, under the same runtime as any other
paglet, and the disk test goes through the same service paglets use for
their state. So the ranking tells where compute paglets such as
[Pi](pi.md) run fastest, and it shows at a glance a host that is much
slower than expected, for example one with a slow disk. Because each
clone holds a compute slot, the benchmark does not crowd out compute
jobs, and with `--slots 1` it runs alone among them.

## How it works

The paglet starts at home with a `start` message. Without a list of
hosts, it asks `mesh-info` for the landscape and sends a clone to every
host it knows, this one included. Each clone gets the `Bench` parameters
and the parent's endpoint. The parent handles the reports as they come
in: it adds the time of the round trip that the benchmark itself did not
take (the move, the wait for a slot, the report), and once every clone has
reported or failed, it sorts the hosts by integer speed:

<!-- snippet: cpp/examples/benchmark/benchmark.cpp#fanout -->
Source: [`cpp/examples/benchmark/benchmark.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/benchmark/benchmark.cpp)

```cpp
fan_.attach(router(), [this](const pt::FanOut::Child& c) {
    HostResult r;
    if (!c.ok || !paglets::decode(c.result, r)) {
        ranking_.errors.push_back((c.host_name.empty() ? c.destination : c.host_name) + ": " +
                                  (c.error.empty() ? "no report" : c.error));
    } else {
        r.travel_ms = std::max<std::int64_t>(0, (mono_ns() - started_ns_) / 1'000'000 - r.bench_ms);
        ranking_.hosts.push_back(std::move(r));
    }
    if (fan_.finished() && ranking_.state == "running") {
        std::ranges::stable_sort(ranking_.hosts, [](const HostResult& a, const HostResult& b) { return a.int_mops > b.int_mops; });
        ranking_.state = "done";
    }
});
```
<!-- /snippet -->

On its host, a clone asks `compute-slots` for one core, with an estimate
of three times the time budget. With `run_now` it holds a lease and
starts. With `queued` it waits for the message `compute.granted`. A host
may also send a queued job on to a host with free slots
(`compute.redirect`); the benchmark stays, because this host is the one
to measure, and runs without a slot. The report then says `slot: false`:

<!-- snippet: cpp/examples/benchmark/benchmark.cpp#slot -->
Source: [`cpp/examples/benchmark/benchmark.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/benchmark/benchmark.cpp)

```cpp
void ask_for_slot() {
    auto slots = paglets::service("compute-slots");
    if (!slots) return run({});
    svc::compute_slots::Client client{*slots};
    svc::compute_slots::SlotRequest q;
    q.job = "benchmark";
    q.cores = 1;
    q.estimated_ms = 3 * bench_.budget_ms;
    auto sent = client.request_slot(q, [this](paglets::Result<svc::compute_slots::Decision> d, Message&) {
        if (!d) return run({});
        if (d->verdict == svc::compute_slots::Verdict::run_now) return run(d->lease);
        if (d->verdict != svc::compute_slots::Verdict::queued) run({});  // compute.granted comes later
    });
    if (!sent) run({});
}

void run(const std::string& lease) {
    if (running_) return;
    running_ = true;
    lease_ = lease;
    result_.slot = !lease.empty();
    bench_start_ns_ = mono_ns();
    next_step();
}
```
<!-- /snippet -->

The CPU and memory tests run one per handler call. Each repeats a short
loop until `budget_ms` has passed and counts the operations: three
xor-shifts, an `and` and an add per integer step; the iteration
`z = z^2 + c` of a bounded orbit for floating point; `memcpy` between two
buffers of `memory_mb` MB for memory (4 MB by default, at most 6 MB, so
both fit the paglet's 16 MB of memory). The results go into `check`, so the
compiler cannot drop the loops. Then the clone asks `mesh-info` about its
host (name, operating system, architecture, CPUs):

<!-- snippet: cpp/examples/benchmark/benchmark.cpp#steps -->
Source: [`cpp/examples/benchmark/benchmark.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/benchmark/benchmark.cpp)

```cpp
void next_step() {
    if (!paglets::after_raw(0, "bench.step", {})) step();
}

void step() {
    switch (step_++) {
        case 0: cpu_int(); return next_step();
        case 1: cpu_float(); return next_step();
        case 2: memory(); return next_step();
        default: break;
    }
    pt::here([this](paglets::Result<svc::mesh_info::Snapshot> s) {
        if (s) {
            result_.host_name = s->host_name;
            result_.os = s->os;
            result_.architecture = s->architecture;
            result_.cpus = s->cpus;
        }
        disk();
    });
}

void cpu_int() {
    const std::int64_t budget = bench_.budget_ms * 1'000'000;
    std::uint64_t x = 88172645463325252ull, acc = 0, iterations = 0;
    const auto t = mono_ns();
    while (mono_ns() - t < budget) {
        for (int i = 0; i < 10'000; ++i) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            acc += x & 0xff;
        }
        iterations += 10'000;
    }
    result_.int_mops = static_cast<double>(iterations) * 5 / seconds_since(t) / 1e6;  // 3 xor-shifts, an and, an add
    result_.check ^= acc;
}
```
<!-- /snippet -->

The disk test writes `disk_mb` MB in blocks of 256 KB as storage keys
`bench-0`, `bench-1`, ..., reads them back, and removes them. A message
holds at most 1 MB, so the blocks stay well below that:

<!-- snippet: cpp/examples/benchmark/benchmark.cpp#disk -->
Source: [`cpp/examples/benchmark/benchmark.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/benchmark/benchmark.cpp)

```cpp
// Writes and reads `disk_mb` MB in blocks of 256 KB through the paglet's storage.
void disk() {
    pt::endpoint("storage", [this](paglets::Result<paglets::Endpoint> st) {
        if (!st) {
            result_.error = pt::error_text("storage", st.error());
            return finish();
        }
        storage_ = *st;
        block_.assign(block, 0x5a);
        disk_start_ns_ = mono_ns();
        write_block(0);
    });
}

std::int64_t blocks() const { return std::max<std::int64_t>(1, bench_.disk_mb) * static_cast<std::int64_t>(mb / block); }

void write_block(std::int64_t i) {
    if (i >= blocks()) {
        result_.disk_write_mbs = static_cast<double>(blocks() * block) / static_cast<double>(mb) / seconds_since(disk_start_ns_);
        disk_start_ns_ = mono_ns();
        return read_block(0);
    }
    svc::storage::Client client{storage_};
    block_[0] = static_cast<std::uint8_t>(i);
    auto sent = client.put(svc::storage::PutRequest{"bench-" + std::to_string(i), block_},
                           [this, i](paglets::Result<svc::storage::Usage> r, Message&) {
                               if (!r) {
                                   result_.error = pt::error_text("storage put", r.error());
                                   return clean_up(0);
                               }
                               write_block(i + 1);
                           });
    if (!sent) {
        result_.error = pt::error_text("storage put", sent.error());
        clean_up(0);
    }
}
```
<!-- /snippet -->

At the end, the clone releases its lease, reports to the parent and
disposes of itself.

The messages and the ranking:

<!-- snippet: cpp/examples/benchmark/benchmark_msgs.hpp -->
Source: [`cpp/examples/benchmark/benchmark_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/benchmark/benchmark_msgs.hpp)

```cpp
// Messages of the Mesh Benchmark demo (planning/cpp-demo-paglets.md, demo 12).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace benchmark_msgs {

// `start`: benchmark `hosts` (names or key IDs; empty: every host
// mesh-info knows). Each CPU and memory test runs for `budget_ms`; the disk
// test writes and reads `disk_mb` MB through the paglet's own storage.
struct Bench {
    std::vector<std::string> hosts;
    std::int64_t budget_ms = 300;
    std::int64_t memory_mb = 4;  // copied between two buffers of this size; at most 6

    std::int64_t disk_mb = 8;
    std::int64_t timeout_ms = 60'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

// One host's numbers. Speeds are per second, sizes in MB (1024 * 1024
// bytes); the CPU tests run in the paglet (Wasm), as paglets' work does.
struct HostResult {
    std::string host_name;
    std::string os;
    std::string architecture;
    std::int64_t cpus = 0;
    double int_mops = 0;      // million integer operations
    double float_mflops = 0;  // million floating-point operations
    double memory_mbs = 0;    // copying in the paglet's memory
    double disk_write_mbs = 0;
    double disk_read_mbs = 0;
    std::int64_t bench_ms = 0;   // the benchmark on the host
    std::int64_t travel_ms = 0;  // the rest of the round trip: move, waiting for a slot, report
    bool slot = false;           // it ran in a compute slot of its own
    std::uint64_t check = 0;     // keeps the CPU loops honest
    std::string error;
};

// `ranking`: the hosts, fastest (integer CPU) first.
struct Ranking {
    std::string state;  // idle, running, done
    std::vector<HostResult> hosts;
    std::vector<std::string> errors;
};

}  // namespace benchmark_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The benchmark needs no policy rule: `compute-slots`, `mesh-info` and the
paglet's own storage are open to every paglet. Start hosts with
`--slots 1` if the benchmark should not share a host with other compute
jobs (see [serve](../cli-reference.md#serve)). Launch the paglet on any
host and start it:

```bash
H=build/macos-arm64/host/paglets-host
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/benchmark.wasm)
$H remote call $R $ID start '{"hosts": [], "budget_ms": 300, "memory_mb": 4, "disk_mb": 8, "timeout_ms": 60000}'
$H remote call $R $ID ranking
```

An empty `hosts` measures every host of the mesh; otherwise name the hosts
by name or key ID. `ranking` answers with `state` `running` until every
clone has reported or the `timeout_ms` has passed, then `done`, with the
hosts fastest first and the clones that failed under `errors`. Keep
`budget_ms` well below 5000 (each test runs in one handler call), and
`disk_mb` below 16 (the storage quota of a paglet). `remote slots` shows
the leases while the clones run.

> [!NOTE]
> Speeds are per second. `memory_mbs`, `disk_write_mbs` and
> `disk_read_mbs` are in MB/s, with 1 MB = 1024 KB. `int_mops` and
> `float_mflops` count millions of operations of the paglet's WebAssembly
> code, so they depend on the runtime mode of the host (for example
> WAMR's fast interpreter) as much as on the CPU.

## On real machines

`cpp/tools/mesh_bench.sh` (see
[File Courier](file-courier.md#transfer-speed-between-real-machines))
runs this example on a mesh of real machines on a gigabit LAN (a Mac
Studio, a Raspberry Pi 4 and a Raspberry Pi Compute Module 5). Each host
is measured in a compute slot of its own: CPU and memory tests of 1 s
each, and 8 MB through the paglet's storage. The CPU numbers are of the
WAMR fast interpreter that runs paglets, not of native code. The last
run:

<!-- results: live_benchmark -->
| Test | Mode | Outcome | Duration | Platform | Recorded | integer (Mops) | floating point (Mflops) | memory copying (MB/s) | storage write, 8 MB (MB/s) | storage read, 8 MB (MB/s) |
|---|---|---|---|---|---|---|---|---|---|---|
| mac | in a compute slot, worker processes | passed | 3.5 s | macos arm64, 10 CPUs, Release build | 2026-10-12 | 250 | 266 | 27275 | 45 | 102 |
| pi | in a compute slot, worker processes | passed | 3.9 s | linux arm64, 4 CPUs, Release build | 2026-10-12 | 67 | 102 | 2344 | 89 | 68 |
| uconsole | in a compute slot, worker processes | passed | 3.3 s | linux arm64, 4 CPUs, Release build | 2026-10-12 | 180 | 331 | 4973 | 397 | 194 |
<!-- /results -->

## Test results

The test runs the benchmark on a mesh of three hosts inside one test
process, each host with one compute slot
(`example: Mesh Benchmark ...` in `cpp/tests/test_examples.cpp`). With a
budget of 30 ms per test and 2 MB for memory and disk, it checks that:

- the start reply accepts the job for three hosts, and the ranking reaches
  `done` without errors;
- every host reports once, each ran in a compute slot of its own, and
  every speed is positive;
- each host's benchmark took at least three budgets (the three timed
  tests), and every host reports its operating system;
- the hosts are sorted by integer speed, fastest first;
- the clones end after their report, so only the parent is left.

<!-- results: benchmark -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts measured, each in a compute slot | integer speed of the fastest host (fast interpreter) (Mops) | floating point of the fastest host (Mflops) | memory copying of the fastest host (MB/s) |
|---|---|---|---|---|---|---|---|---|---|
| example: Mesh Benchmark measures every host in a compute slot and ranks them | in-process | passed | 0.4 s | Darwin arm64, Release build | 2026-10-12 | 3 | 258.26 | 383.61 | 15135.63 |
| example: Mesh Benchmark measures every host in a compute slot and ranks them | worker processes | passed | 0.3 s | Darwin arm64, Release build | 2026-10-12 | 3 | 348.46 | 443.28 | 35466.67 |
<!-- /results -->

The test hosts share one machine and use the in-memory transport between
hosts, so the three results describe the same CPU.
