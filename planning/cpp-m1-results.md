# paglets/cpp: milestone M1 progress

Status: in progress. WP3, WP4, WP6 and WP7 are done. WP5 is done except
worker processes and their sandbox on Windows and macOS, and log
redirection. Companion to
[cpp-edition-plan.md](cpp-edition-plan.md); M0 is closed in
[cpp-m0-results.md](cpp-m0-results.md).

## Summary

| Work package | Exit criterion | State |
|---|---|---|
| WP3 Paglet ABI v1 | Written spec, reviewed; conformance test list | **Done**: [cpp-abi-v1.md](cpp-abi-v1.md), conformance tests C01–C30 all automated. Review by the owner pending |
| WP4 Guest SDK | Samples build and pass ABI conformance tests | **Done**: `cpp/sdk` (`paglets/paglet.hpp`), `paglets_add_module(...)`, samples hello, counter, ping-pong, plus the conformance guest |
| WP5 Host core and workers | Create, message, deactivate, activate, dispose locally; killing a worker only affects its paglets | **Met** on Linux and macOS: registry, trust classes, lifecycle, priority mailboxes, scheduler lanes, time budgets, memory limits, trap handling, import validation per trust class; worker processes (`paglets-worker`, one per lane) with restart, and paglets of a killed worker resume from their last image (test "a killed worker process only affects its paglets"). Workers on Linux run in a seccomp sandbox (`paglets-worker --check-sandbox`, a CTest case). Open: sandbox on macOS and Windows, workers on Windows (in-process there), stdout/stderr to the log, shared-memory rings |
| WP6 Capabilities and messaging | Paglets can only message what they hold capabilities for; revoked and expired capabilities fail; conformance tests for every operation | **Done** for ABI v1: endpoint rights, badges, expiry, use limits, derive, transfer, drop, inspect, one-shot reply capabilities with timeouts, host-stamped sender records, mailbox quota. Open: revocation tree (needs grants from WP9), per-sender rate quotas |
| WP7 Image store and persistence | kill -9 the host; on restart, paglets resume from their last image | **Done**: state directory with modules and paglet files (record + image, replaced atomically), checkpoints after handlers (policy by interval), recovery of paglets, capabilities and timers; requests in flight at the crash are answered `timeout`. Open: per-paglet storage and scratch directories |

## What was built

- **ABI v1** (`cpp/common/include/paglets/abi.hpp`): constants and the
  MessagePack documents of the spec, shared by guest and host. Written by hand
  on top of `msgpack.hpp`, because guests cannot use reflection.
- **Engine** (`cpp/host/.../wasm/engine.hpp`): the 14 `paglets` imports as
  WAMR natives that forward to a `HostImports` object per instance; WAMR
  validates every (pointer, length) argument, out-of-bounds pairs trap the
  paglet (C27). `DirectHarness` drives one instance without the runtime (tools
  and engine tests).
- **Guest SDK** (`cpp/sdk`): `Paglet` base class with lifecycle hooks, a
  router with typed handlers (bodies decoded by `schema_gen` code), endpoints
  with `send` and `request` plus reply continuations, deferred replies,
  capabilities (`derive`, `drop`, `inspect`), children, clones, timers,
  `self_info`. A handled request that is neither answered nor deferred is
  answered `gone` automatically.
- **Runtime** (`cpp/host/.../runtime/runtime.hpp`): paglet registry, lifecycle
  (create, clone, deactivate with optional wake-up, activate on message or
  timer, dispose), priority mailboxes with serial handling per paglet,
  scheduler lanes, capability tables, request/reply with timeouts, timers,
  handler time budgets, failure handling (pending requests answered
  `failed`), the state directory, checkpoints and recovery.
- **Worker processes** (`paglets-worker`): each scheduler lane owns one
  worker process running the lane's instances; calls, host imports and
  memory images travel as MessagePack frames over a socket pair, terminations
  over a second one. A paglet stays on its lane while active, so its WAMR
  execution environment never changes threads. When a worker ends, the
  paglet in the call fails, the others resume from their last image (or fail
  without one), and the worker is started again. `paglets-host` uses workers
  when `paglets-worker` is next to it (`--in-process` turns them off).
- **Worker sandbox** (Linux): after startup a worker sets `no_new_privs` and
  a seccomp filter: file opening and creation, sockets, `execve`, process
  creation (`clone` without `CLONE_THREAD`, `fork`), `ptrace`, `mount`, BPF,
  key and module system calls return `EPERM`; other architectures' system
  call numbers kill the process. `paglets-worker --check-sandbox` verifies
  the denials (CTest `worker_sandbox`).
- **`paglets-host`** (M1 command line): `run` a module with JSON messages,
  `list` and `call` paglets resumed from a state directory. CTest runs a
  paglet across two host processes.
- **Tests**: 73 unit tests (conformance C01–C30, samples, restart and crash
  recovery, a killed worker, concurrency), run twice by CTest: in-process and
  with worker processes; clean under ASan + UBSan and ThreadSanitizer (new CI
  job). The runtime also builds and passes all tests without C++26
  reflection (`linux-host-only`). CI is green on Linux x86-64 and arm64,
  macOS arm64 and, for the first time, Windows (MinGW-w64, in-process).

## Measurements

Linux x86-64 (4 vCPUs, cloud container), GCC 16.2, WAMR 2.4.5 fast
interpreter with hardware bound checks, RelWithDebInfo. `paglets-spike
call-bench` and `paglets-spike runtime [--worker ...]`.

| Measurement | 1 lane, in-process | 4 lanes, in-process | 1 lane, workers | 4 lanes, workers |
|---|---|---|---|---|
| Host request/reply, sequential | 57 µs | 54 µs | 136 µs | 143 µs |
| Host requests spread over 200 paglets | 22,700/s | 54,900/s | 7,100/s | 19,800/s |
| Paglet-to-paglet request/reply (ping-pong round trip) | 42 µs | 89 µs | 258 µs | 191 µs |
| Create a paglet and answer its first request | 150 µs | 255 µs | 384 µs | 586 µs |
| Deactivate and activate by message (image in memory) | 0.8 ms | 0.8 ms | 2.9 ms | 2.7 ms |

Without the runtime, a request through the ABI (counter `status`) takes
16 µs; a message the paglet does not handle 8 µs (M0 ABI v0 on the same
machine: 8 µs).

## Findings

1. **Allocations dominate message cost in interpreted guests.** Every
   `malloc` runs in the Wasm interpreter. Reserving 256 bytes in
   `msgpack::Writer` instead of growing byte by byte cut a request with reply
   from 29 µs to 16 µs. The remaining difference to ABI v0 comes from the
   richer envelope (sender record, capabilities) and the SDK's routing; worth
   another pass in WP20 (for example an arena for handler-scoped allocations).
2. **WAMR's thread manager is not safe for many instances on many threads.**
   `wasm_exec_env_destroy` unlinks the execution environment from its cluster
   without the cluster lock while other threads search all clusters when an
   exception is set or cleared (found by ThreadSanitizer). `patch-wamr.cmake`
   takes the lock; worth an upstream report together with M0 finding 12.
3. **Exceptions must be read under WAMR's lock.** `wasm_runtime_get_exception`
   reads the exception without the lock and races with
   `wasm_runtime_terminate` from the watchdog; the engine uses
   `wasm_runtime_copy_exception`. Clearing an exception searches every
   cluster under a global lock, so the engine only clears when one is set.
4. **Execution environments are bound to a thread.** WAMR records the native
   stack of the first thread that runs an execution environment; the engine
   creates a new one when an instance runs on another thread. Scheduler lanes
   keep a paglet on one thread while it is active, which also gives each
   worker process exactly one thread that talks to it. Paglets on different
   lanes pay a thread wake-up per message (ping-pong: 42 µs on one lane,
   89 µs across lanes).
5. **`wasm_runtime_thread_env_inited()` is not a reliable check** outside AOT
   builds; the engine tracks initialized threads itself.
6. **Watchdog without per-call wake-ups.** Scheduling one budget deadline per
   handler call woke the clock thread on every call; it now checks calls in
   progress every 5 ms instead, which also halved the ping-pong latency.
7. **Guest module size** with the SDK is 80–136 KB (C++ standard library
   containers, `std::function`); `wasm-opt` and `-Oz` are options for WP20.
8. **Worker IPC costs about 20–60 µs per crossing here.** Every host import
   (`send`, `request`, `reply`, ...) is a synchronous round trip to the control
   process, so a ping-pong round trip grows from 42 µs to 258 µs with one
   lane. Next steps: the shared-memory rings of the plan, and returning
   `send`/`reply` results asynchronously (batched at the end of the call).
9. **WAMR modifies the module buffer while loading**, so the host keeps an
   unmodified copy to send to workers (and to stores).
10. **A killed process closes its sockets long before it becomes a
    zombie** (tens of milliseconds in this VM), so an idle worker's end is
    detected by polling its channel before each delivery, not by `waitpid`.
11. **Instances end outside the runtime lock.** A worker instance talks to
    its worker when it is destroyed while the worker's calls take the runtime
    lock for imports (ThreadSanitizer: lock-order inversion); destroyed
    instances are collected and released after the lock.
12. **Sanitizer exit handlers need system calls the sandbox refuses**
    (LeakSanitizer forks at exit); sandboxed processes leave with `_Exit`.
13. **Every write of a paglet's instance pointer happens under the runtime
    lock**, since `info()` and the watchdog read it (ThreadSanitizer, found
    through a test that polls `info()`).

## Next steps

1. WP5 remainder: worker sandbox on macOS (sandbox profile) and Windows
   (job object, restricted token), worker processes on Windows, stdout/stderr
   of guests to the log, shared-memory rings and asynchronous imports
   (finding 8), placement that keeps communicating paglets on one lane. The
   seccomp denylist should become an allowlist once the set of system calls
   a worker needs is known on every supported libc (review in WP21).
2. The allocation work of finding 1.
3. Windows (MinGW-w64): the CI job is green; decide whether it leaves
   experimental.
4. Review of the ABI v1 spec by the owner, then start M2 (WP8 ledger and
   identity).
