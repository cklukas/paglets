<!-- Copyright (c) 2026 by C. Klukas.
     Licensed under the MIT License. See LICENSE for details. -->

# Compute jobs

A compute job runs a function of a WebAssembly module in a fresh instance,
separate from the paglet's handler lane. It can take minutes or hours while
its paglet keeps answering messages. The job sees its input, not the
paglet's memory; its result arrives in a later handler. Jobs require ABI
v1.3. Paglets and their memory images remain interpreted and portable.

## Register and submit

Include `paglets/compute.hpp`. A function receives `compute::JobContext`;
register it with `PAGLETS_JOB(function_name)`:

```cpp
void sum(paglets::compute::JobContext& job) {
    std::vector<std::int64_t> numbers;
    if (!job.input(numbers)) return job.fail(paglets::abi::malformed);
    std::int64_t total = 0;
    for (auto n : numbers) {
        if (job.cancelled()) return job.fail(paglets::abi::cancelled);
        total += n;
    }
    (void)job.output(total);
}
PAGLETS_JOB(sum)
```

Submit from a handler. `submit` encodes its typed input; `submit_raw` takes
bytes. The returned `Result<compute::Job>` reports admission errors:

```cpp
auto submitted = paglets::compute::submit("sum", numbers,
    [this](paglets::Result<paglets::compute::Output> result, paglets::Message&) {
        if (!result) { paglets::log("sum failed"); return; }
        std::int64_t total = 0;
        if (result->decode(total)) use_total(total);
    }, paglets::compute::Options{.hosts = {"any"}});
if (submitted) job_ = *submitted;
```

`Job::cancel()` requests cancellation; the continuation receives
`abi::cancelled`. `Job::status()` returns the state, host, progress,
elapsed milliseconds and AOT flag. A `Job` is a correlation handle, not an
RAII capability: dropping a C++ copy does not cancel it. Cancel explicitly.
After the result is delivered, status may return `not_found`.

## Options and limits

<!-- snippet: cpp/sdk/include/paglets/compute.hpp#options -->
Source: [`cpp/sdk/include/paglets/compute.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/sdk/include/paglets/compute.hpp)

```cpp
struct Options {
    std::int64_t cores = 1;      // slots the job takes while it runs
    std::int64_t max_ms = 0;     // wall time; 0: the host's default (1 h), capped by the host and the policy
    std::int64_t memory_mb = 0;  // linear-memory ceiling; 0: the host's default (1 GB)
    // Where it runs: empty for this host; {"any"} for the host of the mesh
    // with the most free slots (this one first); else host names or key IDs
    // (the one of them with the most free slots).
    std::vector<std::string> hosts;
    // When the paglet moves: stop the job (default), or let it finish and
    // have its result follow the paglet.
    bool finish_on_departure = false;
    std::optional<std::string> module;  // hex hash of the module; default: this module
    // An artifact with the input (lent, it stays valid), instead of bytes.
    std::optional<Capability> input_artifact;
};
```
<!-- /snippet -->

Empty `hosts` runs locally. `{"any"}` selects a compatible live host with
available compute slots; explicit names or key IDs restrict selection.
Names must identify one enrolled host. A full, lowercase key ID selects
only that host, and an unknown ID never falls back to a display name.
Ambiguous or unknown entries are skipped; if no listed host is eligible,
the job reports `not_found`. Job host lists do not accept key-ID prefixes.
Jobs queue when their host has no slot. `cores` accounts for slots; it does
not create guest threads or parallelize a function.

Runtime defaults are one hour of elapsed running time, 1 GB of linear
memory and at most 256 queued or running jobs per owner. The default caps
are 24 hours and 1 GB. The memory value is a ceiling: a job starts with
its module's initial memory and grows only when needed. It does not reserve
1 GB of physical RAM at startup. Embedders can change these in `runtime::Config`.
The reply deadline is the capped running limit plus one minute, measured
from submission. A long queue wait can exhaust it before the job starts;
expiry cancels queued or running work and replies `timeout`.

Mesh policy for service `compute-jobs`, operation `submit`, can refuse a
job or further cap its duration. Queuing time is reported separately.
Running jobs share slot accounting with compute-slot leases.
Both queues compare their oldest request's arrival time. Later work can
use only capacity beyond an older request's needs; existing running work
continues. Cancelling a queued head lets the next fitting job start.
If a host lowers its slot count, queued jobs needing more than the new
total end with `quota`; running jobs keep their allocations until they end.

Jobs inherit the residency marks of artifact input. Inline results mark
the receiving paglet, and output artifacts retain the same marks.

Input and output over the 1 MB message limit use artifact capabilities.
Set `input_artifact` instead of inline input. The result's `artifact` is
present for a large output; its `data` is then empty. Artifact support must
be installed on the host. JobContext reads and writes in chunks of up to
256 KB. The current host buffers job input and output in memory; these
chunk APIs do not provide unlimited constant-memory streaming.

A job can log, read clocks and random bytes, read its input, append output,
report progress and check cancellation. It has no paglet capabilities for
messaging, files or network access. With worker execution it runs in its
own worker process, under the configured sandbox.

## Lifecycle and recovery

| Event | Outcome |
|---|---|
| Another handler, checkpoint or deactivation | the job continues; its reply wakes an inactive paglet |
| Explicit cancellation, wall-time expiry or trap | `cancelled`, `timeout` or `failed` |
| Paglet ends | its jobs stop |
| Successful move | jobs cancel by default; `finish_on_departure` keeps them running and forwards their replies |
| Failed move | jobs stay with the paglet |
| Clone | the original retains its jobs; the clone must submit its own work |
| Host restart | pending jobs are lost and answer `gone`; resubmit from saved input |
| Remote execution host leaves the mesh | its jobs answer `gone` |

A pending result's reply timeout keeps counting through a paglet move.
Moving an unresolved request or job continuation requires travel-state
version 3; a destination with an older reader refuses the move.

The current finish-on-departure implementation forwards the result, but
does not transfer the job-status/cancel record to the destination runtime.
Use admin cancellation on the execution host for such a job. Its original
requester host must also remain reachable while it runs; transfer of full
job ownership across departures is follow-up work.

Jobs are restartable computations, not migrating call stacks. Retrying
can execute the same input more than once; commit external effects in the
paglet with the required deduplication. A coroutine waiting for a reply
lives in the paglet's linear memory and can be imaged with the same module.

Include `paglets/async.hpp` for `paglets::Async`, `co_await
compute::run(...)` and `co_await paglets::ask(...)`. The coroutine starts
immediately, suspends when a job or request is accepted, and resumes in the
reply handler. Immediate submission failure resumes it in the current
handler. Each resumed handler still has the ordinary handler budget.

## Ahead-of-time compilation

If `paglets-wamrc` is available, the host compiles each job module for its
own CPU in the background. Earlier jobs use the interpreter; later jobs
use the compiled cache. Results report `info.aot`, `elapsed_ms` and
`queued_ms`; never infer the execution mode just from elapsed time.
Compiler failure falls back to interpreted jobs and is logged. Windows
currently runs jobs in the interpreter; host-side AOT compilation is not
implemented there.

`serve --aot-compiler PATH` selects the compiler; `--no-aot` disables it.
The default looks beside `paglets-host`. The cache is in `aot/` below the
state directory, or temporary storage without a state directory. AOT code
is local to that host and is not a portable paglet image.
Compiled jobs always use explicit software memory bounds checks. This
also applies on hosts whose interpreted paglets use hardware checks:
the guard-page AOT path failed a large memory-growth regression on macOS.
Compiler options are included in the cache key, so the corrected code
does not reuse an older compiled module.

Build options and installation are described in [Building](building.md).
`paglets-host remote jobs` lists jobs; `remote jobs cancel JOB` cancels one
with admin authorization. See [CLI reference](cli-reference.md).
The [Pi example](examples/pi.md) uses remote chunk jobs and one conversion
job; [Zombie paglets](zombie-paglets.md) can resubmit work after takeover.
