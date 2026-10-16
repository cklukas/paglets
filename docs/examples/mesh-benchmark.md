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
both fit the paglet's default memory allowance). The results go into `check`, so the
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
`bench-0`, `bench-1`, ..., reads them back, and removes them. Reads verify
the block length, sequence marker and every payload byte; missing or
corrupt blocks fail the benchmark. The read time includes this validation.
A message
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

// `jobs`: repeat the same integer kernel locally. Every sample reports
// its actual execution mode; a host without a compiler stays interpreted.
struct JobBench {
    std::int64_t iterations = 3'000'000;
    std::int64_t samples = 20;
};
struct JobSample {
    bool aot = false;
    std::int64_t elapsed_ms = 0;
    std::int64_t queued_ms = 0;
    std::uint64_t check = 0;
};
struct JobComparison {
    std::vector<JobSample> samples;
    std::string error;
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
runs this example on a mesh of real machines on the recorded local network (a Mac
Studio, a Raspberry Pi 4 and a Raspberry Pi Compute Module 5). Each host
is measured in a compute slot of its own: CPU and memory tests of 1 s
each, and 8 MB through the paglet's storage. The CPU numbers are of the
WAMR fast interpreter that runs paglets, not of native code. The table below is the historical 2026-10-12 run.

The older collector labelled every row as holding a compute slot without
retaining the clone's confirmation. Current runs check the actual `slot`
flag and fail a controlled sample without it; the historical rows alone
do not establish that allocation.

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
| example: Mesh Benchmark measures every host in a compute slot and ranks them | in-process | passed | 0.3 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 3 | 275.87 | 474.27 | 14297.6 |
| example: Mesh Benchmark measures every host in a compute slot and ranks them | worker processes | passed | 0.4 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 3 | 355.64 | 475.76 | 31251.04 |
<!-- /results -->

The test hosts share one machine and use the in-memory transport between
hosts, so the three results describe the same CPU.

## Interpreter and AOT jobs

The `jobs` message runs the same integer kernel repeatedly in separate
compute-job instances on the current host. It accepts `iterations`
(default 3,000,000) and `samples` (default 20, maximum 100), and replies
with samples containing `aot`, `elapsed_ms`, `queued_ms` and a checksum.
Each checksum must match; `error` reports a mismatch or execution failure.

```bash
paglets-host remote call PAGLET jobs '{"iterations":3000000,"samples":20}'
```

<!-- snippet: cpp/examples/benchmark/benchmark.cpp#jobs_kernel -->
Source: [`cpp/examples/benchmark/benchmark.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/benchmark/benchmark.cpp)

```cpp
void integer_job(paglets::compute::JobContext& job) {
    std::int64_t iterations = 0;
    if (!job.input(iterations) || iterations < 1) return job.fail(paglets::abi::invalid_argument);
    std::uint64_t x = 88172645463325252ull;
    for (std::int64_t i = 0; i < iterations; ++i) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
    }
    (void)job.output(x);
}
PAGLETS_JOB(integer_job)
```
<!-- /snippet -->

A host with a compiler may switch from interpreter to AOT during the
samples. Compare only samples whose `aot` flags identify the intended
modes; compilation time and queue time are not kernel time. Increase the
sample count or repeat the command if compilation is not ready. With
`serve --no-aot` every sample remains interpreted. Use the same input and
record the host build for a live comparison.

Run the jobs comparison on each physical host with the `jobs` step of
`cpp/tools/mesh_bench.sh`. It writes `live_jobs.json` and preserves the raw
samples. The comparison waits up to five minutes per host for compiled jobs;
missing or failed AOT compilation makes the comparison fail. Compiled jobs
use explicit software bounds checks on every platform, including hosts whose
interpreted paglets use hardware checks. This avoids a reproducible memory-growth
failure in the guard-page AOT path. Compiler options are part of the cache key.

The driver also preserves each host's `--info` output. The first samples can be interpreted
while the module compiles; later samples report their actual AOT mode. A
host without the compiler produces interpreter samples only and fails the comparison. Compare the
same iteration count and report both elapsed and queued time.

```bash
cpp/tools/mesh_bench.sh --remote pi=klukas@rpi4:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
  --remote uconsole=uconsole:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
  --steps "jobs pi" --slots 2 --pi-digits 6000 --sizes "1"
```

On a Mac, set `TMPDIR` to a unique directory under a mounted
`/Volumes/S8T/tmp` or `/Volumes/PRO-BLADE/tmp` before running the script.
`--results-dir DIR` preserves a separate run without replacing the published
results. `--jobs-iterations` and `--jobs-samples` select the kernel size and
sample count. `--pi-work-ms` adds a minimum chunk hold for scheduling
experiments; its default is zero and any nonzero hold must be reported with
Pi timings.

### Recorded compute-job run

The October 2026 run repeated 30 million integer iterations per job on a
Mac Studio, Raspberry Pi 4 and Compute Module 5. Each host has at least
three samples in each mode, including a complete warm AOT batch. The
checksum agrees across every sample and every host. Elapsed time includes
instance setup and worker startup; queued time is separate. Cold interpreter
samples overlap background compilation, so these figures describe the
recorded job execution rather than isolated kernel speed. The Mac uses
hardware bounds checks; the Pis use software checks. Compiler versions,
binary/guest fingerprints and raw samples are preserved in
`planning/validation/cpp-jobs-live-20261014.json` and its host records.

## Timing moves

`cpp/tools/mesh_bench.sh --steps move` moves the counter between every
ordered pair of physical hosts. The default ballast sizes are 0, 1, 4 and
16 MB, with ten samples per size/direction. The script uses a 32 MB paglet
cap because 16 MB of ballast also needs space for the SDK and allocator.
Use `--move-sizes`, `--move-samples` and `--move-memory-mb` to change these.

Before each outbound trial it clears both hosts' image-page caches; modules
remain cached after their first use. The return move benefits from retained
pages. Ballast is uniformly filled with a nonzero byte, so compressed bytes
are reported alongside logical image size and pages sent/reused.

The recorded phases are capture, source processing/transfer until the
destination is prepared, destination host-record preparation, activation/restore and commit. The
source's capture-to-arrival receipt ends after the guest's `on_arrived`
returns and includes the acknowledgement's network return. CLI dispatch
plus a confirmed destination call is timed separately. Local monotonic
durations are used; timestamps from different hosts are never subtracted.
Results preserve medians/ranges and every raw sample in `live_move.json`
and `raw/move-samples.json`. Host compiler, build and WAMR configurations
are collected from each executable, rather than copied from the Mac.

The final three-host campaign on 2026-10-15 completed 480 moves: all six
ordered pairs, four image sizes, ten cold/return samples per group. Every
arrival preserved the counter and memory. Full phase medians/ranges are in
`docs/results/live_move.json`, with raw evidence and fingerprints in
`planning/validation/cpp-three-host-20261015.json`. A later run on the AOT
bounds correction passed twelve control moves, both job modes on all hosts,
and 100,000-digit Pi with decimal-prefix verification. Its separate topics
are named `live_three_host_*`; the earlier two-host data below is historical.

The two-host campaign on 2026-10-15 completed 160 moves between the Mac
and uconsole. Each size and direction has ten cold/return pairs, with a
32 MB host limit and uniform nonzero ballast. This ballast compresses
well; the experiment measures movement and page reuse, rather than bulk
network throughput. Arrival time includes the acknowledgement's return
trip; CLI confirmation is recorded separately. The source Mac used
RelWithDebInfo and the uconsole Release, with their actual compiler and
WAMR configuration recorded in the validation receipt. This first campaign
measured host-record preparation separately; instance restoration is
included in acknowledgement time. The follow-up collector reports
activation/restore as its own destination measurement.

<!-- results: live_move_two_host -->
| Test | Mode | Outcome | Duration | Platform | Recorded |
|---|---|---|---|---|---|
| mac → uconsole, 0 MB ballast, cold pages | LAN, worker processes; uniform nonzero ballast | passed | 2.3 s | Darwin arm64, Mac13,1, 10 CPUs → Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, mac=RelWithDebInfo; uconsole=Release build | 2026-10-15 |
| mac → uconsole, 0 MB ballast, return | LAN, worker processes; uniform nonzero ballast | passed | 2.3 s | Darwin arm64, Mac13,1, 10 CPUs → Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, mac=RelWithDebInfo; uconsole=Release build | 2026-10-15 |
| mac → uconsole, 1 MB ballast, cold pages | LAN, worker processes; uniform nonzero ballast | passed | 2.6 s | Darwin arm64, Mac13,1, 10 CPUs → Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, mac=RelWithDebInfo; uconsole=Release build | 2026-10-15 |
| mac → uconsole, 1 MB ballast, return | LAN, worker processes; uniform nonzero ballast | passed | 2.4 s | Darwin arm64, Mac13,1, 10 CPUs → Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, mac=RelWithDebInfo; uconsole=Release build | 2026-10-15 |
| mac → uconsole, 4 MB ballast, cold pages | LAN, worker processes; uniform nonzero ballast | passed | 3.3 s | Darwin arm64, Mac13,1, 10 CPUs → Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, mac=RelWithDebInfo; uconsole=Release build | 2026-10-15 |
| mac → uconsole, 4 MB ballast, return | LAN, worker processes; uniform nonzero ballast | passed | 2.9 s | Darwin arm64, Mac13,1, 10 CPUs → Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, mac=RelWithDebInfo; uconsole=Release build | 2026-10-15 |
| mac → uconsole, 16 MB ballast, cold pages | LAN, worker processes; uniform nonzero ballast | passed | 7.6 s | Darwin arm64, Mac13,1, 10 CPUs → Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, mac=RelWithDebInfo; uconsole=Release build | 2026-10-15 |
| mac → uconsole, 16 MB ballast, return | LAN, worker processes; uniform nonzero ballast | passed | 5.8 s | Darwin arm64, Mac13,1, 10 CPUs → Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, mac=RelWithDebInfo; uconsole=Release build | 2026-10-15 |
| uconsole → mac, 0 MB ballast, cold pages | LAN, worker processes; uniform nonzero ballast | passed | 2.3 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs → Darwin arm64, Mac13,1, 10 CPUs, uconsole=Release; mac=RelWithDebInfo build | 2026-10-15 |
| uconsole → mac, 0 MB ballast, return | LAN, worker processes; uniform nonzero ballast | passed | 2.3 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs → Darwin arm64, Mac13,1, 10 CPUs, uconsole=Release; mac=RelWithDebInfo build | 2026-10-15 |
| uconsole → mac, 1 MB ballast, cold pages | LAN, worker processes; uniform nonzero ballast | passed | 2.7 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs → Darwin arm64, Mac13,1, 10 CPUs, uconsole=Release; mac=RelWithDebInfo build | 2026-10-15 |
| uconsole → mac, 1 MB ballast, return | LAN, worker processes; uniform nonzero ballast | passed | 2.6 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs → Darwin arm64, Mac13,1, 10 CPUs, uconsole=Release; mac=RelWithDebInfo build | 2026-10-15 |
| uconsole → mac, 4 MB ballast, cold pages | LAN, worker processes; uniform nonzero ballast | passed | 3.6 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs → Darwin arm64, Mac13,1, 10 CPUs, uconsole=Release; mac=RelWithDebInfo build | 2026-10-15 |
| uconsole → mac, 4 MB ballast, return | LAN, worker processes; uniform nonzero ballast | passed | 3.1 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs → Darwin arm64, Mac13,1, 10 CPUs, uconsole=Release; mac=RelWithDebInfo build | 2026-10-15 |
| uconsole → mac, 16 MB ballast, cold pages | LAN, worker processes; uniform nonzero ballast | passed | 8.3 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs → Darwin arm64, Mac13,1, 10 CPUs, uconsole=Release; mac=RelWithDebInfo build | 2026-10-15 |
| uconsole → mac, 16 MB ballast, return | LAN, worker processes; uniform nonzero ballast | passed | 6.7 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs → Darwin arm64, Mac13,1, 10 CPUs, uconsole=Release; mac=RelWithDebInfo build | 2026-10-15 |

mac → uconsole, 0 MB ballast, cold pages:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 1.97 ms |
| capture, minimum | 1.86 ms |
| capture, maximum | 2.22 ms |
| transfer and source control, excluding destination prepare, median | 6.18 ms |
| transfer and source control, excluding destination prepare, minimum | 5.92 ms |
| transfer and source control, excluding destination prepare, maximum | 15.28 ms |
| destination host-record preparation, median | 0.06 ms |
| destination host-record preparation, minimum | 0.06 ms |
| destination host-record preparation, maximum | 0.09 ms |
| destination commit, median | 0.29 ms |
| destination commit, minimum | 0.24 ms |
| destination commit, maximum | 0.32 ms |
| capture to arrival-hook acknowledgement, median | 13.54 ms |
| capture to arrival-hook acknowledgement, minimum | 13.23 ms |
| capture to arrival-hook acknowledgement, maximum | 39.33 ms |
| CLI dispatch plus confirmed destination call, median | 227.24 ms |
| CLI dispatch plus confirmed destination call, minimum | 223.65 ms |
| CLI dispatch plus confirmed destination call, maximum | 238.44 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 128 KB |
| linear memory, minimum | 128 KB |
| linear memory, maximum | 128 KB |
| compressed page bytes, median | 2568 bytes |
| compressed page bytes, minimum | 2566 bytes |
| compressed page bytes, maximum | 2570 bytes |

mac → uconsole, 0 MB ballast, return:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 1.99 ms |
| capture, minimum | 1.93 ms |
| capture, maximum | 3.88 ms |
| transfer and source control, excluding destination prepare, median | 6.21 ms |
| transfer and source control, excluding destination prepare, minimum | 4.93 ms |
| transfer and source control, excluding destination prepare, maximum | 7.46 ms |
| destination host-record preparation, median | 0.07 ms |
| destination host-record preparation, minimum | 0.05 ms |
| destination host-record preparation, maximum | 0.07 ms |
| destination commit, median | 0.29 ms |
| destination commit, minimum | 0.25 ms |
| destination commit, maximum | 1.38 ms |
| capture to arrival-hook acknowledgement, median | 13.78 ms |
| capture to arrival-hook acknowledgement, minimum | 11.54 ms |
| capture to arrival-hook acknowledgement, maximum | 16.87 ms |
| CLI dispatch plus confirmed destination call, median | 230.16 ms |
| CLI dispatch plus confirmed destination call, minimum | 223.11 ms |
| CLI dispatch plus confirmed destination call, maximum | 238.85 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 128 KB |
| linear memory, minimum | 128 KB |
| linear memory, maximum | 128 KB |
| compressed page bytes, median | 2956 bytes |
| compressed page bytes, minimum | 2954 bytes |
| compressed page bytes, maximum | 2957 bytes |

mac → uconsole, 1 MB ballast, cold pages:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 17.94 ms |
| capture, minimum | 17.46 ms |
| capture, maximum | 18.53 ms |
| transfer and source control, excluding destination prepare, median | 22.21 ms |
| transfer and source control, excluding destination prepare, minimum | 21.04 ms |
| transfer and source control, excluding destination prepare, maximum | 22.38 ms |
| destination host-record preparation, median | 0.07 ms |
| destination host-record preparation, minimum | 0.07 ms |
| destination host-record preparation, maximum | 0.08 ms |
| destination commit, median | 1.52 ms |
| destination commit, minimum | 1.47 ms |
| destination commit, maximum | 1.67 ms |
| capture to arrival-hook acknowledgement, median | 64.62 ms |
| capture to arrival-hook acknowledgement, minimum | 62.33 ms |
| capture to arrival-hook acknowledgement, maximum | 65.85 ms |
| CLI dispatch plus confirmed destination call, median | 257.07 ms |
| CLI dispatch plus confirmed destination call, minimum | 253.01 ms |
| CLI dispatch plus confirmed destination call, maximum | 265.49 ms |
| pages sent, median | 18 |
| pages sent, minimum | 18 |
| pages sent, maximum | 18 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 1152 KB |
| linear memory, minimum | 1152 KB |
| linear memory, maximum | 1152 KB |
| compressed page bytes, median | 3404 bytes |
| compressed page bytes, minimum | 3403 bytes |
| compressed page bytes, maximum | 3407 bytes |

mac → uconsole, 1 MB ballast, return:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 17.84 ms |
| capture, minimum | 17.53 ms |
| capture, maximum | 18.57 ms |
| transfer and source control, excluding destination prepare, median | 6.6 ms |
| transfer and source control, excluding destination prepare, minimum | 6.08 ms |
| transfer and source control, excluding destination prepare, maximum | 6.71 ms |
| destination host-record preparation, median | 0.07 ms |
| destination host-record preparation, minimum | 0.07 ms |
| destination host-record preparation, maximum | 0.08 ms |
| destination commit, median | 1.34 ms |
| destination commit, minimum | 1.3 ms |
| destination commit, maximum | 1.43 ms |
| capture to arrival-hook acknowledgement, median | 48.87 ms |
| capture to arrival-hook acknowledgement, minimum | 45.77 ms |
| capture to arrival-hook acknowledgement, maximum | 51.88 ms |
| CLI dispatch plus confirmed destination call, median | 243.03 ms |
| CLI dispatch plus confirmed destination call, minimum | 237.62 ms |
| CLI dispatch plus confirmed destination call, maximum | 252.56 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 16 |
| pages reused, minimum | 16 |
| pages reused, maximum | 16 |
| linear memory, median | 1152 KB |
| linear memory, minimum | 1152 KB |
| linear memory, maximum | 1152 KB |
| compressed page bytes, median | 2942 bytes |
| compressed page bytes, minimum | 2942 bytes |
| compressed page bytes, maximum | 2943 bytes |

mac → uconsole, 4 MB ballast, cold pages:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 65.41 ms |
| capture, minimum | 63.69 ms |
| capture, maximum | 68.47 ms |
| transfer and source control, excluding destination prepare, median | 46.44 ms |
| transfer and source control, excluding destination prepare, minimum | 45.15 ms |
| transfer and source control, excluding destination prepare, maximum | 71.02 ms |
| destination host-record preparation, median | 0.06 ms |
| destination host-record preparation, minimum | 0.06 ms |
| destination host-record preparation, maximum | 0.08 ms |
| destination commit, median | 4.7 ms |
| destination commit, minimum | 4.27 ms |
| destination commit, maximum | 5.27 ms |
| capture to arrival-hook acknowledgement, median | 169.31 ms |
| capture to arrival-hook acknowledgement, minimum | 167.06 ms |
| capture to arrival-hook acknowledgement, maximum | 209.15 ms |
| CLI dispatch plus confirmed destination call, median | 330.63 ms |
| CLI dispatch plus confirmed destination call, minimum | 300.72 ms |
| CLI dispatch plus confirmed destination call, maximum | 355.89 ms |
| pages sent, median | 66 |
| pages sent, minimum | 66 |
| pages sent, maximum | 66 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 4224 KB |
| linear memory, minimum | 4224 KB |
| linear memory, maximum | 4224 KB |
| compressed page bytes, median | 4356 bytes |
| compressed page bytes, minimum | 4354 bytes |
| compressed page bytes, maximum | 4357 bytes |

mac → uconsole, 4 MB ballast, return:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 66.25 ms |
| capture, minimum | 65.27 ms |
| capture, maximum | 71.38 ms |
| transfer and source control, excluding destination prepare, median | 6.17 ms |
| transfer and source control, excluding destination prepare, minimum | 6.08 ms |
| transfer and source control, excluding destination prepare, maximum | 9 ms |
| destination host-record preparation, median | 0.05 ms |
| destination host-record preparation, minimum | 0.05 ms |
| destination host-record preparation, maximum | 0.06 ms |
| destination commit, median | 4.23 ms |
| destination commit, minimum | 4.16 ms |
| destination commit, maximum | 4.6 ms |
| capture to arrival-hook acknowledgement, median | 127.53 ms |
| capture to arrival-hook acknowledgement, minimum | 126.21 ms |
| capture to arrival-hook acknowledgement, maximum | 134.63 ms |
| CLI dispatch plus confirmed destination call, median | 292.57 ms |
| CLI dispatch plus confirmed destination call, minimum | 286.76 ms |
| CLI dispatch plus confirmed destination call, maximum | 303.07 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 64 |
| pages reused, minimum | 64 |
| pages reused, maximum | 64 |
| linear memory, median | 4224 KB |
| linear memory, minimum | 4224 KB |
| linear memory, maximum | 4224 KB |
| compressed page bytes, median | 2934 bytes |
| compressed page bytes, minimum | 2932 bytes |
| compressed page bytes, maximum | 2934 bytes |

mac → uconsole, 16 MB ballast, cold pages:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 258.87 ms |
| capture, minimum | 255.09 ms |
| capture, maximum | 265.62 ms |
| transfer and source control, excluding destination prepare, median | 182.4 ms |
| transfer and source control, excluding destination prepare, minimum | 164.63 ms |
| transfer and source control, excluding destination prepare, maximum | 203.68 ms |
| destination host-record preparation, median | 0.06 ms |
| destination host-record preparation, minimum | 0.06 ms |
| destination host-record preparation, maximum | 0.07 ms |
| destination commit, median | 17.08 ms |
| destination commit, minimum | 15.86 ms |
| destination commit, maximum | 18.26 ms |
| capture to arrival-hook acknowledgement, median | 656.26 ms |
| capture to arrival-hook acknowledgement, minimum | 632.27 ms |
| capture to arrival-hook acknowledgement, maximum | 678.53 ms |
| CLI dispatch plus confirmed destination call, median | 767.1 ms |
| CLI dispatch plus confirmed destination call, minimum | 736.01 ms |
| CLI dispatch plus confirmed destination call, maximum | 787.44 ms |
| pages sent, median | 258 |
| pages sent, minimum | 258 |
| pages sent, maximum | 258 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 16512 KB |
| linear memory, minimum | 16512 KB |
| linear memory, maximum | 16512 KB |
| compressed page bytes, median | 8192.5 bytes |
| compressed page bytes, minimum | 8192 bytes |
| compressed page bytes, maximum | 8197 bytes |

mac → uconsole, 16 MB ballast, return:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 257.53 ms |
| capture, minimum | 251.04 ms |
| capture, maximum | 263.04 ms |
| transfer and source control, excluding destination prepare, median | 10.11 ms |
| transfer and source control, excluding destination prepare, minimum | 9.76 ms |
| transfer and source control, excluding destination prepare, maximum | 12.2 ms |
| destination host-record preparation, median | 0.06 ms |
| destination host-record preparation, minimum | 0.05 ms |
| destination host-record preparation, maximum | 0.07 ms |
| destination commit, median | 16.9 ms |
| destination commit, minimum | 15.43 ms |
| destination commit, maximum | 17.5 ms |
| capture to arrival-hook acknowledgement, median | 479.72 ms |
| capture to arrival-hook acknowledgement, minimum | 472.88 ms |
| capture to arrival-hook acknowledgement, maximum | 490.02 ms |
| CLI dispatch plus confirmed destination call, median | 587.2 ms |
| CLI dispatch plus confirmed destination call, minimum | 567.41 ms |
| CLI dispatch plus confirmed destination call, maximum | 594.45 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 256 |
| pages reused, minimum | 256 |
| pages reused, maximum | 256 |
| linear memory, median | 16512 KB |
| linear memory, minimum | 16512 KB |
| linear memory, maximum | 16512 KB |
| compressed page bytes, median | 2929 bytes |
| compressed page bytes, minimum | 2928 bytes |
| compressed page bytes, maximum | 2931 bytes |

uconsole → mac, 0 MB ballast, cold pages:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 4 ms |
| capture, minimum | 2.57 ms |
| capture, maximum | 4.17 ms |
| transfer and source control, excluding destination prepare, median | 5.42 ms |
| transfer and source control, excluding destination prepare, minimum | 4.57 ms |
| transfer and source control, excluding destination prepare, maximum | 8.44 ms |
| destination host-record preparation, median | 0.04 ms |
| destination host-record preparation, minimum | 0.03 ms |
| destination host-record preparation, maximum | 0.09 ms |
| destination commit, median | 0.52 ms |
| destination commit, minimum | 0.41 ms |
| destination commit, maximum | 1.65 ms |
| capture to arrival-hook acknowledgement, median | 14.28 ms |
| capture to arrival-hook acknowledgement, minimum | 11.71 ms |
| capture to arrival-hook acknowledgement, maximum | 17.15 ms |
| CLI dispatch plus confirmed destination call, median | 232.38 ms |
| CLI dispatch plus confirmed destination call, minimum | 221.99 ms |
| CLI dispatch plus confirmed destination call, maximum | 241.5 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 128 KB |
| linear memory, minimum | 128 KB |
| linear memory, maximum | 128 KB |
| compressed page bytes, median | 2557 bytes |
| compressed page bytes, minimum | 2549 bytes |
| compressed page bytes, maximum | 2557 bytes |

uconsole → mac, 0 MB ballast, return:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 4.02 ms |
| capture, minimum | 3.98 ms |
| capture, maximum | 4.07 ms |
| transfer and source control, excluding destination prepare, median | 5.34 ms |
| transfer and source control, excluding destination prepare, minimum | 4.97 ms |
| transfer and source control, excluding destination prepare, maximum | 5.77 ms |
| destination host-record preparation, median | 0.04 ms |
| destination host-record preparation, minimum | 0.03 ms |
| destination host-record preparation, maximum | 0.04 ms |
| destination commit, median | 0.74 ms |
| destination commit, minimum | 0.42 ms |
| destination commit, maximum | 2.44 ms |
| capture to arrival-hook acknowledgement, median | 14.21 ms |
| capture to arrival-hook acknowledgement, minimum | 12.97 ms |
| capture to arrival-hook acknowledgement, maximum | 16.43 ms |
| CLI dispatch plus confirmed destination call, median | 228.85 ms |
| CLI dispatch plus confirmed destination call, minimum | 225.75 ms |
| CLI dispatch plus confirmed destination call, maximum | 245.74 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 128 KB |
| linear memory, minimum | 128 KB |
| linear memory, maximum | 128 KB |
| compressed page bytes, median | 2963 bytes |
| compressed page bytes, minimum | 2962 bytes |
| compressed page bytes, maximum | 2970 bytes |

uconsole → mac, 1 MB ballast, cold pages:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 36.19 ms |
| capture, minimum | 34.93 ms |
| capture, maximum | 36.83 ms |
| transfer and source control, excluding destination prepare, median | 14.75 ms |
| transfer and source control, excluding destination prepare, minimum | 14.14 ms |
| transfer and source control, excluding destination prepare, maximum | 17.43 ms |
| destination host-record preparation, median | 0.04 ms |
| destination host-record preparation, minimum | 0.04 ms |
| destination host-record preparation, maximum | 0.07 ms |
| destination commit, median | 6.49 ms |
| destination commit, minimum | 0.65 ms |
| destination commit, maximum | 7.21 ms |
| capture to arrival-hook acknowledgement, median | 69.68 ms |
| capture to arrival-hook acknowledgement, minimum | 62.25 ms |
| capture to arrival-hook acknowledgement, maximum | 74.95 ms |
| CLI dispatch plus confirmed destination call, median | 273.51 ms |
| CLI dispatch plus confirmed destination call, minimum | 264.57 ms |
| CLI dispatch plus confirmed destination call, maximum | 280.12 ms |
| pages sent, median | 18 |
| pages sent, minimum | 18 |
| pages sent, maximum | 18 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 1152 KB |
| linear memory, minimum | 1152 KB |
| linear memory, maximum | 1152 KB |
| compressed page bytes, median | 3403 bytes |
| compressed page bytes, minimum | 3402 bytes |
| compressed page bytes, maximum | 3403 bytes |

uconsole → mac, 1 MB ballast, return:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 36.24 ms |
| capture, minimum | 34.28 ms |
| capture, maximum | 36.56 ms |
| transfer and source control, excluding destination prepare, median | 6.09 ms |
| transfer and source control, excluding destination prepare, minimum | 5.54 ms |
| transfer and source control, excluding destination prepare, maximum | 8.92 ms |
| destination host-record preparation, median | 0.04 ms |
| destination host-record preparation, minimum | 0.03 ms |
| destination host-record preparation, maximum | 0.05 ms |
| destination commit, median | 1.09 ms |
| destination commit, minimum | 0.65 ms |
| destination commit, maximum | 6.75 ms |
| capture to arrival-hook acknowledgement, median | 57.12 ms |
| capture to arrival-hook acknowledgement, minimum | 53.81 ms |
| capture to arrival-hook acknowledgement, maximum | 63.69 ms |
| CLI dispatch plus confirmed destination call, median | 259.84 ms |
| CLI dispatch plus confirmed destination call, minimum | 255.34 ms |
| CLI dispatch plus confirmed destination call, maximum | 269.92 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 16 |
| pages reused, minimum | 16 |
| pages reused, maximum | 16 |
| linear memory, median | 1152 KB |
| linear memory, minimum | 1152 KB |
| linear memory, maximum | 1152 KB |
| compressed page bytes, median | 2948.5 bytes |
| compressed page bytes, minimum | 2947 bytes |
| compressed page bytes, maximum | 2950 bytes |

uconsole → mac, 4 MB ballast, cold pages:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 85.95 ms |
| capture, minimum | 85.48 ms |
| capture, maximum | 117.81 ms |
| transfer and source control, excluding destination prepare, median | 40.6 ms |
| transfer and source control, excluding destination prepare, minimum | 39.19 ms |
| transfer and source control, excluding destination prepare, maximum | 58.15 ms |
| destination host-record preparation, median | 0.04 ms |
| destination host-record preparation, minimum | 0.04 ms |
| destination host-record preparation, maximum | 0.06 ms |
| destination commit, median | 13.49 ms |
| destination commit, minimum | 12.49 ms |
| destination commit, maximum | 24.49 ms |
| capture to arrival-hook acknowledgement, median | 183.21 ms |
| capture to arrival-hook acknowledgement, minimum | 176.25 ms |
| capture to arrival-hook acknowledgement, maximum | 231.99 ms |
| CLI dispatch plus confirmed destination call, median | 349.07 ms |
| CLI dispatch plus confirmed destination call, minimum | 339.36 ms |
| CLI dispatch plus confirmed destination call, maximum | 405.27 ms |
| pages sent, median | 66 |
| pages sent, minimum | 66 |
| pages sent, maximum | 66 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 4224 KB |
| linear memory, minimum | 4224 KB |
| linear memory, maximum | 4224 KB |
| compressed page bytes, median | 4367 bytes |
| compressed page bytes, minimum | 4365 bytes |
| compressed page bytes, maximum | 4367 bytes |

uconsole → mac, 4 MB ballast, return:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 87.17 ms |
| capture, minimum | 86.51 ms |
| capture, maximum | 87.99 ms |
| transfer and source control, excluding destination prepare, median | 5.62 ms |
| transfer and source control, excluding destination prepare, minimum | 4.99 ms |
| transfer and source control, excluding destination prepare, maximum | 6.44 ms |
| destination host-record preparation, median | 0.04 ms |
| destination host-record preparation, minimum | 0.03 ms |
| destination host-record preparation, maximum | 0.06 ms |
| destination commit, median | 13.32 ms |
| destination commit, minimum | 12.52 ms |
| destination commit, maximum | 25.25 ms |
| capture to arrival-hook acknowledgement, median | 145.88 ms |
| capture to arrival-hook acknowledgement, minimum | 141.36 ms |
| capture to arrival-hook acknowledgement, maximum | 159.04 ms |
| CLI dispatch plus confirmed destination call, median | 313.83 ms |
| CLI dispatch plus confirmed destination call, minimum | 291.95 ms |
| CLI dispatch plus confirmed destination call, maximum | 321.6 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 64 |
| pages reused, minimum | 64 |
| pages reused, maximum | 64 |
| linear memory, median | 4224 KB |
| linear memory, minimum | 4224 KB |
| linear memory, maximum | 4224 KB |
| compressed page bytes, median | 2949 bytes |
| compressed page bytes, minimum | 2948 bytes |
| compressed page bytes, maximum | 2951 bytes |

uconsole → mac, 16 MB ballast, cold pages:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 356.76 ms |
| capture, minimum | 338.08 ms |
| capture, maximum | 382.28 ms |
| transfer and source control, excluding destination prepare, median | 149.02 ms |
| transfer and source control, excluding destination prepare, minimum | 142.1 ms |
| transfer and source control, excluding destination prepare, maximum | 156.02 ms |
| destination host-record preparation, median | 0.05 ms |
| destination host-record preparation, minimum | 0.04 ms |
| destination host-record preparation, maximum | 0.07 ms |
| destination commit, median | 77.83 ms |
| destination commit, minimum | 45.78 ms |
| destination commit, maximum | 118.8 ms |
| capture to arrival-hook acknowledgement, median | 725.66 ms |
| capture to arrival-hook acknowledgement, minimum | 663.03 ms |
| capture to arrival-hook acknowledgement, maximum | 780.33 ms |
| CLI dispatch plus confirmed destination call, median | 834.54 ms |
| CLI dispatch plus confirmed destination call, minimum | 769.89 ms |
| CLI dispatch plus confirmed destination call, maximum | 894.22 ms |
| pages sent, median | 258 |
| pages sent, minimum | 258 |
| pages sent, maximum | 258 |
| pages reused, median | 0 |
| pages reused, minimum | 0 |
| pages reused, maximum | 0 |
| linear memory, median | 16512 KB |
| linear memory, minimum | 16512 KB |
| linear memory, maximum | 16512 KB |
| compressed page bytes, median | 8207 bytes |
| compressed page bytes, minimum | 8204 bytes |
| compressed page bytes, maximum | 8207 bytes |

uconsole → mac, 16 MB ballast, return:

| Measured | LAN, worker processes; uniform nonzero ballast |
|---|---|
| samples | 10 |
| capture, median | 339.92 ms |
| capture, minimum | 336.75 ms |
| capture, maximum | 343.67 ms |
| transfer and source control, excluding destination prepare, median | 7.98 ms |
| transfer and source control, excluding destination prepare, minimum | 6.63 ms |
| transfer and source control, excluding destination prepare, maximum | 11.42 ms |
| destination host-record preparation, median | 0.08 ms |
| destination host-record preparation, minimum | 0.04 ms |
| destination host-record preparation, maximum | 0.15 ms |
| destination commit, median | 50.17 ms |
| destination commit, minimum | 45.85 ms |
| destination commit, maximum | 126.02 ms |
| capture to arrival-hook acknowledgement, median | 539.34 ms |
| capture to arrival-hook acknowledgement, minimum | 528.21 ms |
| capture to arrival-hook acknowledgement, maximum | 616.3 ms |
| CLI dispatch plus confirmed destination call, median | 652.17 ms |
| CLI dispatch plus confirmed destination call, minimum | 634.51 ms |
| CLI dispatch plus confirmed destination call, maximum | 727.74 ms |
| pages sent, median | 2 |
| pages sent, minimum | 2 |
| pages sent, maximum | 2 |
| pages reused, median | 256 |
| pages reused, minimum | 256 |
| pages reused, maximum | 256 |
| linear memory, median | 16512 KB |
| linear memory, minimum | 16512 KB |
| linear memory, maximum | 16512 KB |
| compressed page bytes, median | 2944 bytes |
| compressed page bytes, minimum | 2941 bytes |
| compressed page bytes, maximum | 2944 bytes |
<!-- /results -->

<!-- results: live_jobs -->
| Test | Mode | Outcome | Duration | Platform | Recorded | samples | median elapsed (ms) | elapsed range (ms) | median queued (ms) |
|---|---|---|---|---|---|---|---|---|---|
| mac | AOT job | passed | 0.1 s | Darwin arm64, Mac13,1, 10 CPUs, RelWithDebInfo build | 2026-10-14 | 4 | 84.5 | 83 to 86 | 0.0 |
| mac | interpreter job | passed | 0.4 s | Darwin arm64, Mac13,1, 10 CPUs, RelWithDebInfo build | 2026-10-14 | 5 | 438 | 433 to 448 | 0 |
| pi | AOT job | passed | 0.1 s | Linux aarch64, Raspberry Pi 4 Model B Rev 1.4, 4 CPUs, Release build | 2026-10-14 | 4 | 116.0 | 116 to 117 | 0.0 |
| pi | interpreter job | passed | 1.6 s | Linux aarch64, Raspberry Pi 4 Model B Rev 1.4, 4 CPUs, Release build | 2026-10-14 | 20 | 1558.5 | 1508 to 2929 | 0.0 |
| uconsole | AOT job | passed | 0.0 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, Release build | 2026-10-14 | 4 | 45.0 | 45 to 45 | 0.0 |
| uconsole | interpreter job | passed | 0.6 s | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs, Release build | 2026-10-14 | 17 | 635 | 588 to 646 | 0 |
<!-- /results -->
