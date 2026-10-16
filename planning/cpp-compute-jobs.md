# paglets/cpp: long-running compute jobs (WP31–WP36)

Status: implemented and merged into cpp at `9fa07fd`; Mac and both Pis
validated, including a physical three-host run (2026-10-14), starting from
`cpp-jobs` commit f9c11c6. The current public contract is in
[Compute jobs](../docs/compute-jobs.md); this document preserves the design
rationale. The implemented imports all use module `paglets`; `Job` is a
correlation token, not a drop-cancels capability. The Pi example uses jobs.
The takeover fixes interpreter/AOT cancellation, shutdown, recovery ordering
and admission shared with compute-slot leases. Tests cover local/remote
work, host loss, moves, artifacts, coroutine awaits and AOT result identity.
Build/test records and live measurements are in `planning/validation/`.
Sections 1 to 6 preserve the original proposal; use the public jobs page
for current function names, defaults and ownership rules.

Compute jobs are a general capability of the framework: part of the ABI,
the runtime, the host's services and the SDK, available to every paglet on
every host. Pi is the first example rewritten with them (the original plan is in
section 4).

## 1. The problem

A handler may run 5 s at most, and nothing can interrupt it in a way that
can be continued: a memory image holds linear memory and globals, but not
the engine's call stack, so a paglet can be moved, checkpointed or
deactivated only between handlers. Long computations must therefore be
sliced by hand (the Pi example converts and computes in slices of 200 ms,
each started by a timer). That is awkward for authors, slow (the slice loop,
the per-slice scheduling, the interpreter), and it does not fit work that
is naturally one long call into a library.

We need long-running computations that

- run as long as they need (minutes, hours), at full speed;
- do not block the paglet: it keeps answering messages, can deactivate,
  can be checkpointed, and its host stays responsive;
- are bounded and accounted: CPU slots, memory, wall time, per owner;
- end cleanly when they are no longer wanted: cancelled by the paglet, or
  when the paglet ends or leaves;
- are as safe as paglets: sandboxed, no ambient authority;
- are pleasant to use: submit, then continue when the result is there
  ("await").

Typical uses, none of them possible today without slicing by hand:

- **Data processing** next to the data: hashing, compressing or indexing
  large files, parsing logs, deduplication, format conversion (the File
  Finder, Duplicate Finder and Log Scout examples do such work in small
  steps today).
- **Numerics and simulation**: long solvers, Monte Carlo runs, parameter
  sweeps where every point is a job.
- **Media and analysis**: image and signal processing, feature extraction,
  local inference with a model compiled to Wasm.
- **Search**: exhaustive or heuristic searches that run until they find
  something or a deadline passes, with progress the paglet can watch.
- **Batch work across the mesh**: a coordinator paglet spreading many jobs
  over the hosts with free slots (remote jobs, section 3.7), collecting the
  results, surviving host failures (as a zombie, section 3.6).

## 2. Options

| Option | How | Verdict |
|---|---|---|
| A. Longer budgets | Allow long handlers on dedicated lanes, for paglets with a compute-slot lease | Solves nothing but the 5 s: during the handler the paglet is deaf (no replies, no dispose, no beats), cannot be imaged, and a crash loses everything |
| B. Yield points (Asyncify) | Binaryen's Asyncify transform lets `paglets::yield()` unwind the call stack into linear memory; the runtime images and even moves the paglet mid-computation | Real strong mobility, but every module needs the transform (larger, slower code), the author still places yield points, and the paglet is still busy while it computes |
| C. Engine checkpoints | Capture the interpreter's internal stacks | Tied to one engine build and architecture; images would no longer move between Mac and Pi |
| D. Native host kernels | The host offers fixed fast functions (hashing, BBP digits, ...) | Fast, but only for what hosts ship; no user code |
| **E. Compute jobs** | **The paglet submits a call of a job function of its module; the host runs it in a separate, fresh Wasm instance in a job pool, off the paglet's lane; the result comes back as a message** | **Proposed.** The paglet stays a small, responsive, movable state machine; the long work is a pure function that needs no mobility |

E separates what must be mobile (the paglet: small state, messages) from
what must be fast and long (the job: a function from input to output). It
needs no change to images, keeps weak mobility for paglets, and suits the
fault-tolerance model: a job is restartable from its input, and the
paglet's checkpoints record which jobs it waits for.

## 3. Compute jobs

### 3.1 What a job is

- **A function of a module**: by default the paglet's own module, or
  another module by hash (like `create_child`). Job functions are
  registered in the guest with `PAGLETS_JOB(name, fn)`; the module exports
  one entry point, `paglets_on_job`, which dispatches by name.
- **Input to output**: `Bytes fn(std::span<const std::uint8_t> input,
  JobContext&)`. Input and output up to 1 MB go inline; larger ones as
  `artifact` capabilities (content-addressed, readable on every host).
- **A fresh instance**: the job gets its own linear memory, initialized
  from the module; it does not see the paglet's memory. Nothing is shared,
  so the job cannot corrupt the paglet, and the paglet's images stay small
  and consistent while the job runs.
- **No capabilities**: a job cannot send messages, open files or reach the
  network. Its imports (module `paglets_job`) are `log`, `now`, `random`,
  `progress(done, total)`, `cancelled()` (a cheap check for long loops), and
  `read_input`/`write_output` for artifact streams. Whatever it needs comes
  in its input; whatever it produces goes out in its result.

### 3.2 The paglet's side

```cpp
// Continuation style, as requests: the reply arrives in a later handler.
paglets::compute::Job job = paglets::compute::submit(
    "bbp_digits", encode(Chunk{start, count}),
    paglets::compute::Options{.cores = 1, .max_ms = 3'600'000},
    [this](paglets::Result<paglets::Bytes> r, paglets::Message&) { done(r); });

job.cancel();                 // or drop the Job capability
job.status(...);              // queued / running, progress, elapsed
```

- `submit` returns at once with a `job` capability. The result arrives as
  the reply (status `ok` with the output, or an error: `cancelled`,
  `timeout`, `quota`, `failed` with the trap, `gone` after a restart).
- **Await.** C++20 coroutines give `co_await paglets::compute::run(...)`:
  coroutine frames live on the paglet's heap, in linear memory, so a
  paglet suspended in `co_await` is an ordinary memory image; it can be
  checkpointed, deactivated and, by policy, moved while the job runs.
  (Proposed as a second step on top of the continuation API.)
- **Progress** reaches the paglet on request (`status`) or, optionally, as
  messages every N seconds, so a UI or a zombie journal can follow it.

### 3.3 Lifecycle

| Event | The job |
|---|---|
| paglet handles other messages | keeps running |
| paglet deactivates | keeps running; the result activates the paglet (as timers and grants do) |
| paglet is checkpointed | keeps running; the image records the job as pending |
| paglet cancels or drops the `job` capability | is stopped (engine terminate); the reply is `cancelled` |
| paglet ends (disposed, failed, module distrusted) | is stopped |
| paglet moves to another host | `on_departure: cancel` (default): stopped, the paglet gets `cancelled` after arriving. `finish`: keeps running, and the result follows the paglet like any message (forwarding) |
| paglet is a clone | its jobs stay with the original |
| host restarts | jobs are lost; the recovered paglet gets `gone` for each pending job (as for requests in flight), and submits again |
| job exceeds `max_ms`, memory or traps | is stopped; the reply says why |

### 3.4 Execution on the host

- **Job pool**: separate from the scheduler lanes, so jobs never delay
  paglets. With worker processes, jobs run in their own sandboxed
  `paglets-worker` processes (job mode); a crashing or runaway job takes
  down only itself.
- **Admission by compute-slots**: a job takes `cores` slots for its
  runtime (the existing service decides: run now, queue, or reject). The
  paglet no longer needs to hold a lease itself; the job's lease ends with
  the job.
- **Limits**: wall time (`max_ms`, capped by the host and the mesh policy;
  default 1 h), memory pages, slots, and jobs per owner. The current default
  memory ceiling and maximum are 1 GB per job, separately from the 256 MB
  paglet default. These bound growth; they are not initial allocations.
- **Speed**: jobs run in the same interpreter as paglets at first. Because
  jobs never move, they can later be compiled ahead of time for the host's
  CPU (WAMR AOT, `wamrc`), cached by module hash. That is typically
  several times faster than the interpreter and changes nothing for
  paglets or images.

### 3.5 Policy and accounting

- A mesh rule allows the service `compute-jobs` operation `submit` (by
  owner, module, host label), with `max_duration` capping `max_ms`.
- Jobs count against the owner's quota and appear in `compute-slots`
  status and in `paglets-host remote slots` (job, paglet, cores, progress,
  elapsed); admins can cancel them.
- Residency: a job inherits its paglet's data residency marks (its input
  may contain marked data), and its output carries them.

### 3.6 Zombie paglets

A zombie's `step()` can submit a job and call `finished` when the result
arrives; the base class already ignores the completion of a main that
stopped meanwhile. The job's input is in the replicated work, so a new main
that takes over simply submits it again: at-least-once, as for all zombie
work. The Mesh Chronicle needs no jobs; a long computation (Pi as a zombie)
would.

### 3.7 Remote jobs (second step)

A paglet may submit a job to another host: `Options{.host = "..."}` or
`.host = "any"` (compute-slots' `candidates`). The module travels by hash
(code mobility), input and output as inline bytes or artifacts, and the
result returns as a message to the paglet's endpoint, wherever it is. This
turns the Pi example's worker paglets into plain jobs spread over the mesh,
with no slicing.

## 4. First users

Every example that slices today moves to jobs (Pi, the conversion steps of
Duplicate Finder and Log Scout where they compute), and the SDK reference
gets a chapter on jobs. The Pi example shows the most:

Today: a coordinator, one worker paglet per slot on every host, each
computing chunks of 500 hex digits in slices of 200 ms; the decimal
conversion in slices too.

With jobs: the coordinator submits jobs of any size (for example 10 000
digits each) to the hosts with free slots (remote jobs), keeps at most a
few in flight per slot, writes results in order as they come, and submits
the decimal conversion as one long job. No slicing, no worker paglets;
the coordinator stays responsive (status, progress, cancellation) and can
be a zombie.

## 5. Work packages

| WP | Content |
|---|---|
| WP31 | ABI v1.3: `job_submit` (job capability, reply), cancel by drop, `paglets_on_job` export, the `paglets_job` import module; documents; conformance tests |
| WP32 | Runtime: job pool (threads in-process, `paglets-worker` job mode with workers), fresh instances from the module cache, limits, cancellation, results as replies, pending jobs in the paglet record (`gone` after a restart), departure policy |
| WP33 | compute-slots admission of jobs, quotas, policy rule `compute-jobs`, status and admin cancel in the CLI |
| WP34 | SDK: `compute::submit` / `Job`, `PAGLETS_JOB`, `JobContext` (progress, cancelled, artifact streams); coroutine `co_await` layer |
| WP35 | Remote jobs (host or any), artifacts for large input and output; the Pi example rewritten with jobs; tests and live benchmark (interpreter) |
| WP36 | Ahead-of-time compiled jobs (WAMR AOT per host CPU, cached by module hash); benchmark against the interpreter |

## 6. Questions for review

1. **Departure policy default**: cancel the job when its paglet moves
   (frees the host at once), or finish it and forward the result?
2. **Jobs in a fresh instance (pure function)**: acceptable, or do some
   jobs need read access to the paglet's state (we would pass it in the
   input, or an image of it)?
3. **Order**: local jobs first (WP31–WP34) with Pi rewritten for local
   jobs on each host, then remote jobs and AOT? Or remote jobs in the first
   step?
4. **Coroutines**: offer `co_await` in the SDK from the start, or keep the
   continuation style first?
5. **Limits**: default job wall time 1 h, cap by policy; per-owner job
   quota?

## 7. Example review during the takeover

Pi's numerical chunks and decimal conversion are compute jobs. The Mesh
Benchmark adds `jobs`, measuring the same integer kernel with the actual
interpreter/AOT mode and matching checksums for every sample. Duplicate
Finder already uses `files.hash` on the owning host, without moving file
bytes into a guest; there is no sliced guest computation to replace. Log
Scout reads asynchronously in 256 KB chunks but accumulates a configurable
tail before matching. For very large tails, review incremental complete-line
matching and bounded buffering first; a whole-tail job is not automatically
a better memory or I/O contract. No conversion of those two examples is
required for the compute-jobs baseline.
