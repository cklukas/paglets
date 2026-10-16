# Long-running agents and tasks

A paglet can work for minutes, hours, or indefinitely. Memory and execution
time requirements default to **unknown**. By default the host imposes no
job duration, job memory, or handler-time policy ceiling. Developers do not
need resource declarations to run ordinary agents.

An operator can enable a handler watchdog, for example five seconds for
one uninterrupted turn of the agent's code. That is separate from an agent's
lifetime or the duration of its background work. The earlier five-second,
one-hour and 1 GB defaults were policies, not inherent requirements of mobile
agents; they are now opt-in host settings.

Waiting for a file scan, inference, a download, or a compute job does not
spend handler time. Submit the operation, return, and receive progress or
the result in later handlers. The framework supplies this lifecycle;
applications do not need to implement a polling protocol for every operation.

## Unknown requirements and optional host policy

For compute jobs, `Options::max_ms = 0` and `memory_mb = 0` mean unknown.
The unrestricted host default allows them to run until completion or
cancellation, and to grow memory as needed. `max_ms = -1` explicitly asks
for unlimited duration; a positive value declares an elapsed-time budget,
including a week or longer. `memory_mb = 2048` declares a 2 GB allowance;
`-1` asks for the maximum addressable memory. These values are not upfront
allocations. The current Wasm32 runtime addresses at most 4 GB per instance;
that is an address-space boundary, not a server policy or a claim that RAM
is available. Memory allocation can still fail if the actual machine runs out.

An operator can enable limits and require resource declarations:

| Host option | Default | Effect when enabled |
|---|---|---|
| `--job-max-ms N` | `-1`, none | Enforce a maximum elapsed job duration, including for an unknown/unlimited request |
| `--job-max-memory-mb N` | `0`, none | Reject an explicit job memory declaration that does not fit; constrain growth of jobs with unknown requirements |
| `--require-job-resources` | off | Refuse jobs without finite positive time and memory declarations |
| `--memory-mb N` | `0`, none | Per-paglet memory ceiling; reject modules declaring a larger maximum |
| `--require-paglet-memory` | off | Refuse paglets whose Wasm module does not declare a memory maximum |
| `--handler-ms N` | `-1`, off | Enforce a watchdog on each paglet handler |

Declare a paglet/module memory allowance with
`paglets_add_module(my_agent ... MEMORY_MB 2048)`. The build records a Wasm
memory maximum, which travels with the module and is checked before creation,
cloning or arrival on a host. Omitting it leaves the requirement unknown.
Jobs declare their own optional budgets in `compute::Options`. Host and mesh
policy decide admission; no declaration can bypass an enabled owner policy.

An admitted declared memory ceiling is enforced by Wasm: growth beyond it
is refused, and an out-of-bounds access traps and fails the paglet or job.
This ceiling covers linear memory, not total process RSS, host-side I/O
buffers, or an upfront reservation of physical RAM.
Merely trying an allocation that fails does not mean the agent consumed
memory beyond its allowance. A configured time budget terminates work that
exceeds it. `max_ms` and the handler watchdog measure elapsed host time;
the current API does not meter cumulative CPU time.

## Why handlers are short

A paglet handles one message at a time. While a handler is running, that
paglet cannot answer another message, process cancellation, checkpoint, or
move. Memory images capture state between handlers; they do not capture a
running call stack. A handler that waits synchronously for a ten-minute
operation would make its agent unresponsive for ten minutes, and all work
since its last image would be lost after a crash.

An enabled watchdog protects responsiveness and other paglets sharing the host.
It is not a judgement that useful operations finish quickly. A slow API
call and a long computation are both normal work for an autonomous agent.
Use a background operation for that work and let the agent keep making
decisions while it runs. Embedded hosts can enable or enlarge
`runtime::Config::handler_budget`. Allowing longer handlers also delays
cancellation and image capture; it is usually unnecessary for long work.

## One task model, two execution backends

`patterns::Task<Request, Result>` in `paglets/patterns/task.hpp` is the
existing paglet-level task abstraction. It provides `start`, `status`,
`wait`, and `cancel` messages, with `run`, `progress`, `complete`, and
`fail` for the implementation. One such task can coordinate many operations
and travel through many hosts.

`tasks::TaskHandle` in `paglets/tasks.hpp` is a control handle for work. It
is not another class named `Task`. Copies control the same operation;
`status()` returns its state and progress, and `cancel()` requests that it
stop. A task can `track(handle)` so cancelling the overall task cancels its
child operations. `Task::handle()` exposes the same control interface for
the overall task itself.

There are two backends because authority and execution placement differ:

| Property | Service operation | Compute job |
|---|---|---|
| Use for | Filesystem scanning and other capability-controlled I/O | Long CPU work, numerical calculation, conversion |
| Start | A helper such as `patterns::scan_files`; service authors use `tasks::start<Update>` | `compute::submit` or `co_await compute::run` |
| Shared control | Returns `tasks::TaskHandle` | `job.task()` returns `tasks::TaskHandle` |
| Execution | Native background work on the host owning the resource | Fresh isolated Wasm instance, on this host or selected mesh hosts |
| Authority | Service checks the caller's resource capabilities | Input, output, clocks, logs and cancellation; no files, network or paglet messaging |
| Progress | Bounded typed updates; next update follows processing of the previous one | `JobContext::progress(done, total)`; inspect the handle or job status |
| Result | Typed updates plus one completion callback | One result callback, or a coroutine result; large data uses artifacts |
| Movement | Scan cursors stay with the filesystem host; finish or cancel before moving, then start on arrival if needed | Select placement on submission; `finish_on_departure` can keep work running when the agent moves |

Compute is an execution backend for task work, not a second agent lifetime.
The shared handle deliberately does not expose a universal `move()`:
a running computation is not a migratable checkpoint, and a directory
iterator cannot be moved to a different filesystem. Compute jobs can run
remotely; their running stacks are not transferred between hosts. A
paglet's ordinary state remains mobile between its handlers.

The older `compute-slots` service reserves capacity for cooperative work;
it does not execute a job. Prefer compute jobs for new long CPU operations.

## Scan files with the framework

With a `files` endpoint and a readable directory capability:

```cpp
scan_ = track(paglets::patterns::scan_files(files, dir, query,
    [this](const paglets::services::files::ScanBatch& batch) {
        progress(batch.completed);  // entries visited; total is unknown
        for (const auto& entry : batch.entries) consume(entry);
    },
    [this](paglets::Result<void> result) {
        if (result) complete(summary_);
        else fail(paglets::patterns::error_text("scan", result.error()));
    }));
```

Here `scan_` is a `tasks::TaskHandle` member of a class derived from
`patterns::Task<Request, Result>`. The helper handles starting, requesting
bounded updates, deadlines, completion, and cancellation. Each callback
runs in a later handler. `consume` must do short work; submit substantial
CPU processing as a compute job. Cancel the overall task with its `cancel`
message, or cancel this operation with `scan_.cancel()`.

`scan_files` has no overall deadline by default. Pass
an explicit final `timeout_ms` argument when the application wants one. A progress
exchange normally has a 30-second timeout, independent of the overall
deadline. Filesystem work runs on a two-thread host pool. Each pull inspects
at most 2048 directory entries and returns at most 128 matches, with a
128 KB payload target and a 50 ms work target. An empty batch can still
report progress when the pattern has no matches. Updates provide natural
backpressure and do not accumulate a directory tree in the agent's memory.

`patterns::find_files` uses the same task backend when a small collected
list is more convenient. Its result is bounded by the requested limit,
at most 10000 entries and 512 KB; inspect `truncated`. For a complete large
tree, use `scan_files` and consume batches, as
[Mesh File Finder](examples/file-finder.md) does.

## Three clocks with different purposes

| Clock | What it limits | What to choose |
|---|---|---|
| Optional handler watchdog, off by default | One uninterrupted handler, including imports and worker IPC | Operators can enable it; background tasks keep agents responsive either way |
| Request timeout, default 30 s | How long one caller waits for one reply | A suitable service deadline, or short progress exchanges for a task |
| Overall task deadline | How long an operation or fan-out is allowed to keep trying | The application's time budget; unrelated to handler time |

There is no general paglet lifetime deadline. An autonomous agent can run
another operation after one completes, wait between checks, retry after
failures, or continue until its owner cancels it. `Task::wait` has a bounded
waiting interval; receiving an unfinished status does not cancel the task.

For a slow single-reply service such as AI inference, pass
`patterns::long_request(timeout_ms)` as the generated client's request
options. The example AI clients use 30 minutes, rather than the default
30 seconds. Configure the gateway/backend timeout too: a longer caller
timeout cannot extend the provider's own limit. Embedded hosts configure
`gateway::AiConfig::timeout` (the default is two minutes). Ordinary requests do not
gain progress or remote cancellation simply by having a longer timeout;
those require a task-capable service.

Compute jobs have no duration ceiling by default. Set
`compute::Options::max_ms` when the developer wants an execution budget;
an operator can independently impose one. A parent waiting
for many jobs must allow enough time for queuing, execution, and collection.

## Cancellation, movement and recovery

Cancellation is cooperative for native I/O. A task stops at its next check;
an OS filesystem call that is already blocked must return first. Local
service-task cancellation marks its handle cancelled immediately and sends
the service a cancellation message. It does not promise the OS operation
has already returned. Compute jobs also support enforced termination when
they ignore cancellation.

File scans check directory authority during the scan, and cancel when their
owning paglet ends or the host shuts down. They are transient host resources:
host restart loses the cursor; an expired or missing cursor requires a new
scan. Idle cursors can be reaped after a minute when another scan starts.
At most four cursors per paglet and 64 cursors/queued work items per host
bound abandoned resource use; these are concurrency limits, not scan-duration
limits. Completed, cancelled and failed scans release their cursors.

Finish or cancel a scan before moving its agent. A paglet can deactivate
while it awaits progress; a reply wakes it. Checkpointing the agent saves
its own state, not the native directory iterator. For recovery, persist
intent and accumulated application state with the paglet, handle
`gone`, `not_found`, and `timeout`, and restart or retry idempotently.
See [compute-job recovery](compute-jobs.md#lifecycle-and-recovery) for the
equivalent rules for computations. Neither backend promises a running job
will survive loss of its execution host.

## Example audit

The examples use the following long-operation strategies. Bounded result
selection and payload limits protect agent memory; they are separate from
the duration of the operation.

| Examples | Strategy and current boundary |
|---|---|
| Mesh File Finder / Storage Analyzer | Background task batches, live counters, cancellation, 30-minute default fan-out; totals cover the whole scan while returned samples stay bounded |
| Duplicate Finder, Tree Compare, Log Scout | Same background scan-task helper; fan-out defaults to 30 minutes; collected file lists stay bounded. Duplicate Finder and Log Scout report truncation as an error; Tree Compare marks it explicitly |
| File Courier, AI Document Digest | Same scan-task helper with deliberately bounded file selection; sharing/hashing/copying use background services and long request deadlines |
| Semantic Mesh Search, Image Describer | Same scan-task helper; explicitly fail on truncated input rather than silently claiming a complete index/catalogue; AI requests allow 30 minutes |
| Web Researcher, Log Explainer | Background gateway requests and continuation chains; AI requests allow 30 minutes. Web requests also obey gateway/provider limits |
| Pi | Compute jobs for chunks and decimal conversion, progress and retries; file output is written in ranges |
| Mesh Benchmark | Handler-based measurements split into slices of at most 200 ms even for long requested measurements; isolated job measurements use compute jobs; storage is read and written in ranges |
| Volume Guard, Release Watcher, live Log Scout, Mesh Chronicle | Timers and deactivation between monitoring steps; their configured monitoring lifetime is independent of handler time |
| Mesh Journey, Inventory Collector, Latency Map, Hide and Seek | Short message exchanges, clone reports, timers and explicit exchange deadlines; a slow/missing host must not stall the whole mesh indefinitely |
| Hello, Counter, Ping Pong, out-of-tree greeter and adder | Short handlers; Counter's history grows with use, so it is a teaching example rather than an unbounded event store |

For new autonomous agents, start with `Task<Request, Result>`, track the
handles of long operations, and keep only application state between
callbacks. There is no need to make a long operation finish within five
seconds or to block the agent while it runs.
