# paglets/cpp: milestone M1 progress

Status: in progress. WP3, WP4, WP6 and WP7 are done; WP5 is done for paglets
running inside the host process, while worker processes, OS sandboxing and
log redirection are still open. Companion to
[cpp-edition-plan.md](cpp-edition-plan.md); M0 is closed in
[cpp-m0-results.md](cpp-m0-results.md).

## Summary

| Work package | Exit criterion | State |
|---|---|---|
| WP3 Paglet ABI v1 | Written spec, reviewed; conformance test list | **Done**: [cpp-abi-v1.md](cpp-abi-v1.md), conformance tests C01–C30 all automated. Review by the owner pending |
| WP4 Guest SDK | Samples build and pass ABI conformance tests | **Done**: `cpp/sdk` (`paglets/paglet.hpp`), `paglets_add_module(...)`, samples hello, counter, ping-pong, plus the conformance guest |
| WP5 Host core and workers | Create, message, deactivate, activate, dispose locally; killing a worker only affects its paglets | **Partly done**: registry, trust classes, lifecycle, priority mailboxes, scheduler threads, time budgets, memory limits, trap handling, import validation per trust class. Open: worker processes and IPC, restart of crashed workers, OS sandboxing, stdout/stderr to the log |
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
  timer, dispose), priority mailboxes with serial handling per paglet, a
  scheduler thread pool, capability tables, request/reply with timeouts,
  timers, handler time budgets, failure handling (pending requests answered
  `failed`), the state directory, checkpoints and recovery.
- **`paglets-host`** (M1 command line): `run` a module with JSON messages,
  `list` and `call` paglets resumed from a state directory. CTest runs a
  paglet across two host processes.
- **Tests**: 71 unit tests (conformance C01–C30, samples, restart and crash
  recovery, concurrency) and 7 CTest cases; clean under ASan + UBSan and
  ThreadSanitizer (new CI job). The runtime also builds and passes all tests
  without C++26 reflection (`linux-host-only`).

## Measurements

Linux x86-64 (4 vCPUs, cloud container), GCC 16.2, WAMR 2.4.5 fast
interpreter with hardware bound checks, RelWithDebInfo. `paglets-spike
call-bench` and `paglets-spike runtime`.

| Measurement | Result |
|---|---|
| Request through the ABI without the runtime (counter `status`) | 16 µs (M0 ABI v0 on the same machine: 8 µs) |
| Message the paglet does not handle (no reply) | 8 µs |
| Host request/reply through the runtime, sequential | 56 µs (1 scheduler thread), 72 µs (4 threads) |
| Host requests spread over 200 paglets, 4 threads | 51,700 requests/s |
| Paglet-to-paglet request/reply (ping-pong round trip) | 40 µs (1 thread), 95 µs (4 threads) |
| Create a paglet and answer its first request | 205 µs |
| Deactivate and activate by message (image kept in memory) | 1.0 ms |

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
   creates a new one when a paglet moves to another scheduler thread. Moving
   between threads costs about 50 µs per round trip (ping-pong: 40 µs with one
   thread, 95 µs with four). Scheduler affinity (prefer the thread that ran
   the paglet last) is the next step.
5. **`wasm_runtime_thread_env_inited()` is not a reliable check** outside AOT
   builds; the engine tracks initialized threads itself.
6. **Watchdog without per-call wake-ups.** Scheduling one budget deadline per
   handler call woke the clock thread on every call; it now checks calls in
   progress every 5 ms instead, which also halved the ping-pong latency.
7. **Guest module size** with the SDK is 80–136 KB (C++ standard library
   containers, `std::function`); `wasm-opt` and `-Oz` are options for WP20.

## Next steps

1. WP5 worker processes: an executor interface between the scheduler and the
   instances, a worker process running WAMR, IPC for calls, imports and
   images, restart of a crashed worker with its paglets resumed from their
   checkpoints; OS sandboxing per platform; stdout/stderr of guests to the log.
2. Scheduler affinity (finding 4) and the allocation work of finding 1.
3. Windows (MinGW-w64) CI job green; decide whether it leaves experimental.
4. Review of the ABI v1 spec by the owner, then start M2 (WP8 ledger and
   identity).
