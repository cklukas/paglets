# Plan: paglets/cpp (Wasm + WAMR + C++26 reflection)

Status: milestone M0 (WP0–WP2) done, results in
[cpp-m0-results.md](cpp-m0-results.md). Milestone M1 (WP3–WP7) closed, results in
[cpp-m1-results.md](cpp-m1-results.md); the ABI v1 spec is
[cpp-abi-v1.md](cpp-abi-v1.md). Milestone M2 (WP8–WP10) in progress: WP8
done ([cpp-ledger.md](cpp-ledger.md)); see
[cpp-m2-results.md](cpp-m2-results.md).

This document plans **paglets/cpp**, a new implementation of paglets in C++.
It keeps the concepts that proved useful in the Python edition (hosts, mobile
paglets, messages, proxies, mesh, services, compute slots) and deliberately
redesigns the parts that Python forced on us. The central changes:

- **Paglet code and paglet memory both travel.** Hosts no longer need the
  application code installed in advance.
- **Roaming paglets are confined.** They reach other paglets and host resources
  only through capabilities, and use host resources through portable system
  paglets.
- **Security is governed by the mesh.** Mesh admins, identified by their keys,
  make all security decisions from any live host; every host enforces the same
  replicated, signed security state.

Communication, location and security are designed in
[cpp-security-and-communication.md](cpp-security-and-communication.md).

The code lives in this repository under `cpp/`, developed on the `cpp` branch.
paglets/cpp is self-contained: it does not interoperate with the Python edition.
Releases are tagged `cpp-vX.Y.Z`, starting with `cpp-v0.1.0`.

## 1. Goals and non-goals

Goals:

- A native C++ host that runs paglets as WebAssembly modules inside the
  embedded WAMR runtime, in pooled worker processes.
- True code mobility: a paglet's module is shipped to a host that does not
  have it yet, identified and verified by content hash.
- Memory-image mobility: a paglet moves, clones, deactivates and checkpoints as
  a snapshot of its complete Wasm memory, not as a hand-serialized state object.
- Capability-based security with mesh-wide governance by mesh admins.
- Dynamic location of paglets in the mesh, including locate-and-pin.
- Portable host services (files, system information, load, volumes) offered by
  system paglets on every platform, plus host-specific features such as local
  AI and web access announced as service offers.
- C++26 reflection for all schema-driven code (wire types, service contracts,
  ledger records, state inspection, state migration), with generated code
  where the compiler cannot reflect.
- Runs on macOS (arm64), Linux (x86-64, arm64 including Raspberry Pi 4) and
  Windows (x86-64).
- No Rust anywhere in the toolchain or dependencies.

Non-goals (for the first release):

- Any interoperability with the Python edition (wire format, meshes, paglets).
- Moving paglets between different meshes.
- Suspending a paglet in the middle of a running handler (true stack
  migration). The design leaves room for it; see WP-R1.
- Command execution on hosts, and raw network sockets for paglets (web access
  exists only through the `web` system paglet).
- Uploading or posting data to external sites (the `web` system paglet is
  read-only in release 1).

## 2. What changes compared to the Python edition

| Concept | Python edition | paglets/cpp |
|---|---|---|
| Paglet code | Python class, must be importable on every host | Wasm module, shipped on demand, content-addressed (SHA-256) |
| Paglet state | Dataclass, converted to a wire dict, pickled | Entire Wasm linear memory + globals ("memory image") |
| Transient attributes | Ordinary instance attributes, lost on move | Host-side capability handles with per-kind mobility rules |
| Isolation | One child process per active paglet, full Python access | Sandboxed WAMR instances in pooled worker processes |
| Access to files and OS | Anything Python can do | Only via portable system paglets and mesh grants |
| Paglet-to-paglet messages | Any paglet with a proxy ref can message any other | Only through endpoint capabilities; one-shot reply capabilities |
| Finding a paglet | Proxy ref with a fixed host URL | Mesh-wide location (distributed location records, tombstones, mesh query), locate-and-pin |
| Security administration | Shared API key per mesh, per-host configuration | Public-key cryptography only; signed, replicated mesh ledger; admins act from any host |
| Host roles | Relay hub and seed hosts are configured specially | All hosts equal; no central, hub or home host |
| Activation cost | Process spawn + imports | Instantiate module in a running worker (cached, AOT where available) + copy image |
| Clone | Serialize state, new ID | Copy image (page dedupe on the wire) |
| Deactivate / persist | Store wire state as a record | Store the image; identical mechanism to dispatch |
| Crash recovery | Paglet is lost or restarted from scratch | Resume from the last checkpoint image |
| Code version gate | `--mesh-version` string must match | Not needed for paglets (module hash is exact); ABI version must match |
| Deployment | Same package on all hosts, or git auto-update | Only the host binary is installed; paglet modules travel |

What stays conceptually the same: hosts, mesh with seed gossip and beacons,
relay/connect mode, asynchronous messages with priorities and serial handling
per paglet, typed service contracts, compute slots, artifacts, user-info
notifications, the CLI surface.

## 3. Core design

### 3.1 Architecture

```text
  Mesh admin CLI (admin key)
          |
          | signed records
          v
+------------------------------------------------------------------+
| paglets/cpp host                                                 |
|                                                                  |
|  Host control process                                            |
|  +------------------------+     +------------------------------+ |
|  | HTTP API/relay client  |<--->| Mesh + ledger replication    | |
|  +-----------+------------+     +------------------------------+ |
|              |                                                   |
|  +-----------v------------+     +------------------------------+ |
|  | Scheduler, mailboxes,  |<--->| System paglets: files,       | |
|  | capability tables      |     | server-info, locator, ...    | |
|  +-----+------------+-----+     +------------------------------+ |
|        |            |                                            |
|        |            +--------->  Module store, image store,      |
|        |                         ledger, artifacts               |
|        | IPC                                                     |
|  Worker processes (one pool)                                     |
|  +----------------------+   +----------------------+             |
|  | Worker 1: WAMR       |   | Worker 2: WAMR       |             |
|  | paglets A, B         |   | paglet C             |             |
|  +----------------------+   +----------------------+             |
+------------------------------------------------------------------+
```

- The **host control process** is native C++. It owns networking, the mesh,
  ledger replication, the scheduler, capability tables, storage and the native
  system paglets. It never runs paglet code.
- **Worker processes** run WAMR and the paglet instances.
- A **paglet** is a Wasm module plus a memory image plus host-side metadata
  (ID, owner, passport, trust class, capability table, mailbox, pins).
- The **paglet ABI** is the only contract between host and paglet: a small set
  of exports the paglet provides and imports the host provides, as plain core
  Wasm (no component model), versioned as `paglets_abi_v1`. The import set
  depends on the trust class.

### 3.2 Memory-image mobility

A paglet is only ever snapshotted at a **quiescent point**: between two handler
calls, when no host call is in progress and the native stack of the instance is
empty. At that point the complete paglet state is:

- the linear memory (including the C/C++ shadow stack and heap),
- the mutable globals (including `__stack_pointer`),
- the table contents, if the module mutates tables at runtime,
- the module hash,
- the host-side capability table (mobility rules in the security design, 4.6).

Consequences:

- **Dispatch** = snapshot, send image + module hash + portable capabilities,
  restore on the target, re-create grant capabilities there, deliver the
  `arrived` event. No state serialization code in the paglet.
- **Clone** = the same, with a new paglet ID and a `cloned` event.
- **Deactivate** = snapshot to the image store. **Activate** = restore.
- **Checkpoint** = periodic snapshot to the image store, optionally replicated
  to a peer. After a host or worker crash, a paglet resumes from its last
  checkpoint on the same or another host. This is especially valuable for long
  compute jobs.
- **Live rebalancing**: compute-slots can move a running (between work chunks)
  job to a less loaded host, instead of only placing jobs at submission time.

Transfer efficiency:

- Images are split into pages (the 64 KB Wasm page size) and each page is
  hashed. The sender offers page hashes; the receiver requests only pages it
  does not have. Repeated moves of the same paglet, and clones fanned out to
  many hosts, transfer mostly hashes.
- Pages are compressed (zstd) on the wire. Zero pages are never sent.

Limits of image mobility and how the design handles them:

- The image is bound to its exact module. Code upgrades need an explicit
  **state migration**: the old module exports its state in a structured
  (reflection-derived) form, the new module imports it. This is optional per
  paglet type and is the one place where paglets still serialize state.
- Anything outside linear memory does not move: open files, mounts, timers.
  These are host-side capabilities with defined mobility rules.
- Images of paglets with large heaps are large. Quotas cap memory per paglet;
  big data belongs in artifacts, as in the Python edition.

### 3.3 Communication, location, system paglets and security

Designed in detail in
[cpp-security-and-communication.md](cpp-security-and-communication.md). Summary:

- **Meshes and trust classes**: a mesh is a defined set of hosts sharing one
  security state. Paglets are system paglets (privileged, never move),
  resident paglets (admin-assigned to a host) or roaming paglets.
- **Mesh ledger**: an append-only set of records signed by admin, host or owner
  keys, replicated to all hosts by gossip. It holds admins, enrolled hosts and
  owners, module trust, policy rules, grants, revocations and pending requests.
  Admins sign decisions on their own machine and submit them through any live
  host.
- **Capabilities**: per-paglet handle tables; endpoints with allowed
  operations, badges, expiry and a transferable flag; derive, transfer, revoke.
  Grants are mesh-wide and follow the paglet; host-local handles do not.
- **Messages**: asynchronous, priority mailboxes, request/reply through
  one-shot reply capabilities, quotas.
- **Equal hosts**: no central, hub or home host; where a function needs
  specific hosts, they are chosen by consistent hashing over the enrolled
  hosts.
- **Keys and encryption**: Ed25519 keys for admins, hosts and owners; private
  keys never leave their machine; challenge-response authentication; HTTPS
  plus end-to-end encrypted channels, so proxies and relaying peers see only
  ciphertext. No passwords or API keys.
- **Location**: distributed location records, tombstones and mesh queries let
  the `locator` system paglet find any paglet; `locate_and_pin` also holds it on
  its current host for a requested time.
- **System paglets**: files, server-info, directory, locator, grants,
  artifacts, storage, pubsub, user-info, mesh-info, compute-slots; portable
  across macOS, Linux and Windows through named roots and uniform schemas.
- **Service offers**: system paglets announce host features (for example
  local AI through Apple Foundation Models or Ollama, or web access on hosts
  allowed through the firewall); paglets move to "the best host offering X"
  instead of a named host.
- **Data residency**: content from restricted roots marks the paglet, which
  can then only move to allowed hosts.
- **Enforcement**: Wasm sandbox, worker processes, import validation per trust
  class, capability checks against the ledger, quotas, enrolled host keys,
  audit.

### 3.4 Execution model: worker processes

- The host control process starts a pool of **worker processes**, by default
  one per CPU core. Each worker runs WAMR and many paglet instances on its own
  threads, so the host uses all cores and a failure in a worker cannot take
  down the control process.
- A paglet is placed in a worker when it is activated. Compute-heavy paglets
  can request a dedicated worker (subject to compute-slots and quotas).
- Workers talk to the control process over IPC: a control channel (pipe or
  Unix domain socket / named pipe on Windows) plus shared-memory rings for
  message payloads and memory images, so large images are not copied through
  sockets.
- Host calls from a paglet (send, request, capability operations) go to the
  control process asynchronously; the handler does not wait for them.
- Message handling is **serial per paglet** and priority-ordered.
- Handlers must not block. Anything that waits (service calls, remote replies,
  timers) is asynchronous: the result arrives as a later message. This matches
  the direction the Python edition already took (no polling, results via
  messages and the user-info service).
- A trap in a paglet (out of bounds, unreachable, stack overflow) terminates
  only that paglet. A crashing worker is restarted; its paglets resume from
  their last checkpoint and their owners are notified.
- CPU limits: handlers get a time budget; the worker terminates a running
  instance from another thread. Memory limits: maximum linear memory per
  paglet, plus OS limits per worker process.
- OS sandboxing of workers where the platform supports it (for example seccomp
  on Linux, sandbox profiles on macOS, job objects and restricted tokens on
  Windows): workers need no direct file or network access of their own.
- An in-process mode (no workers) exists only for tests and debugging.

### 3.5 Code mobility and trust

- Every module is stored by its SHA-256 hash in a host-local module store.
- A movement envelope names the module hash. If the target does not have it, it
  fetches it from the source host (or any mesh peer that has it) and verifies
  the hash before use.
- Module trust is a mesh decision: the ledger lists trusted module signer keys
  and allowed hashes. Separately, the owner's passport and mesh policy decide
  what the running paglet may do.
- Compiled forms (AOT or JIT output) are cached per host and CPU architecture,
  keyed by module hash and WAMR version. They are never sent between hosts.

### 3.6 Where C++26 reflection is used

Reflection is needed in two places that are compiled by different compilers:

- **Host** (GCC 16 with `-std=c++26 -freflection`): wire types (envelopes, HTTP
  JSON, relay frames, IPC frames), ledger records, launch config, service
  contract descriptors and CLI output are plain structs whose
  encoders/decoders come from reflection. No hand-written serializers.
- **Paglet guest code** (compiled to `wasm32-wasi`): the only C/C++ toolchain
  for Wasm is clang-based (`wasi-sdk`), and clang does not implement reflection
  yet. Therefore guest-side schema code is **generated at build time** by a
  small tool compiled with GCC 16 that reflects over the paglet's message,
  service and migration types and emits plain C++ encoders plus a schema
  descriptor. When clang gains reflection, the generator can be replaced by
  direct reflection without changing paglet source code.

Because memory images carry paglet state, the guest only needs schema code for
messages, service calls, state inspection and state migration, which keeps the
generated surface small.

### 3.7 Wire formats

- Transport: HTTPS (TLS 1.3) for all traffic, with an end-to-end encrypted,
  mutually authenticated channel on top (Noise-style handshake with the host,
  admin or owner keys; ChaCha20-Poly1305 frames). See the security design,
  section 3.1.
- Control plane: JSON frames generated from reflected structs, carried inside
  the encrypted channel.
- Messages, service payloads and ledger records: MessagePack. Ledger records
  use a canonical encoding so signatures are reproducible.
- A schema descriptor is published per module so tools can decode and display
  messages and state.
- Movement: a binary envelope (header in MessagePack) with passport,
  portable capabilities and page manifest, followed by the requested pages,
  streamed.
- All formats carry explicit version fields.

### 3.8 Platforms and toolchain

| Platform | Host compiler | Notes |
|---|---|---|
| macOS arm64 | Homebrew GCC 16 (libstdc++) | Apple clang has no reflection |
| Linux x86-64, arm64 | GCC 16 | Raspberry Pi 4 included |
| Windows x86-64 | MinGW-w64 GCC 16 (for example via MSYS2) | MSVC has no reflection; to be verified in WP2 |

Guest modules are built with wasi-sdk on any of these platforms; the resulting
`.wasm` files are platform-independent.

### 3.9 Dependencies (no Rust)

Candidates, to be confirmed in WP1/WP2:

| Need | Candidate | License |
|---|---|---|
| Wasm runtime | WAMR | Apache-2.0 WITH LLVM-exception |
| Guest toolchain | wasi-sdk (clang + wasi-libc) | Apache-2.0 WITH LLVM-exception |
| Async I/O | standalone Asio | BSL-1.0 |
| HTTP | cpp-httplib, or Boost.Beast | MIT / BSL-1.0 |
| TOML config | toml++ | MIT |
| CLI parsing | CLI11 | BSD-3-Clause |
| Signatures, key exchange, encryption, Argon2id, hashing | libsodium | ISC |
| TLS 1.3 transport | OpenSSL 3 or Mbed TLS | Apache-2.0 / Apache-2.0 (dual-licensed with GPL-2.0) |
| Compression | zstd | BSD-3-Clause |
| Tests | doctest or Catch2 | MIT / BSL-1.0 |

JSON and MessagePack encoders are generated from reflection, so no JSON or
MessagePack library is required on the host. C++ dependencies must be built
with the same compiler and standard library as the host (GCC 16 / libstdc++);
C dependencies are unaffected.

## 4. Work packages

Each work package lists its goal, main deliverables and exit criteria.

### Milestone M0: feasibility (go / no-go)

**WP0: Project setup**

- `cpp/` subtree in this repository: `cpp/host`, `cpp/sdk` (guest SDK),
  `cpp/tools` (schema generator, CLI), `cpp/examples`, `cpp/tests`.
- CMake with presets for macOS arm64 (Homebrew GCC 16), Linux x86-64, Linux
  arm64 and Windows x86-64 (MinGW-w64 GCC 16); wasi-sdk preset for guest
  modules.
- CI builds for all presets, sanitizers (ASan, UBSan) on Linux.
- clang-format, license headers (`// Copyright (c) 2026 by C. Klukas.` /
  `// Licensed under the MIT License. See LICENSE for details.`).
- Release tagging scheme `cpp-vX.Y.Z`, kept separate from the Python `vX.Y.Z`
  tags and their PyPI publish workflow.
- Exit: an empty host binary and an empty guest module build in CI on all
  targets.

**WP1: WAMR spike**

- Embed WAMR, load and run a trivial guest module on macOS arm64, Linux x86-64,
  Raspberry Pi 4 and Windows x86-64.
- Measure per running mode (interpreter, fast interpreter, fast JIT, LLVM JIT,
  AOT) which modes exist per architecture, instantiate latency, call overhead,
  memory per instance.
- Verify: terminating a running instance from another thread, memory limits,
  trap reporting, many instances per worker process, rejecting modules by
  import list.
- Verify snapshot/restore: read and write linear memory and all mutable
  globals (non-exported globals such as `__stack_pointer` may need linker
  flags or WAMR internals); evaluate any built-in WAMR checkpoint/restore
  support against doing it in paglets/cpp.
- Exit: a guest counter paglet is snapshotted in one worker process and resumes
  with the correct count in another process, on two architectures.

**WP2: Toolchain and reflection spike**

- Confirm GCC 16 C++26 reflection on macOS, Linux arm64 (Raspberry Pi) and
  Windows (MinGW-w64) for host wire types (JSON + MessagePack encoders from
  reflection).
- Build the guest schema generator prototype: GCC 16 tool that reflects over a
  guest header and emits encoders compilable by wasi-sdk clang.
- Check that the host builds with GCC 16 + libstdc++ against all chosen C++
  dependencies on every platform.
- Exit: one message struct round-trips host (reflection) to guest (generated)
  and back.

Go / no-go review after M0: continue, adjust (runtime mode, platform scope), or
stop.

### Milestone M1: single-host runtime

**WP3: Paglet ABI v1 specification**

- Exports: allocation, lifecycle entry points (created, arrived, cloned,
  activated, deactivating, dispatching, disposing), message handler, optional
  state export/import for migration.
- Standard import set: messaging (send, request, reply), capability operations
  (derive, drop, inspect), own lifecycle, child creation, timers, clock,
  random, logging, minimal WASI subset.
- Privileged import set for trusted Wasm system paglets.
- Memory ownership rules, error codes, ABI version negotiation.
- Exit: written spec, reviewed; conformance test list.

**WP4: Guest SDK (C++)**

- Header library for paglet authors: `Paglet` base, message dispatch by type,
  typed endpoints, request/reply helpers with continuations, capability
  wrappers, typed service clients.
- Build integration: CMake function `paglets_add_module(...)` running the
  schema generator and wasi-sdk, producing `.wasm` + schema descriptor.
- Sample paglets: hello, counter, ping-pong.
- Exit: samples build and pass ABI conformance tests.

**WP5: Host core and worker processes**

- Host control process: paglet registry, trust classes, lifecycle state
  machine, mailboxes with priority + FIFO, context event log.
- Worker pool: spawning, placement, dedicated workers, IPC control channel and
  shared-memory rings, restart on crash.
- WAMR instance management in workers, import validation per trust class, time
  budgets, memory limits, trap reporting; OS sandboxing of workers per
  platform.
- Exit: create, message, deactivate, activate and dispose paglets locally;
  killing a worker only affects its paglets, which are reported and resumed.

**WP6: Capabilities and paglet-to-paglet messaging**

- Per-paglet capability tables, endpoint rights, badges, expiry, use limits.
- Derive, transfer in messages, drop, inspect; revocation tree.
- Request/reply with one-shot reply capabilities, correlation IDs, timeouts.
- Host-stamped sender records; per-sender and per-receiver quotas.
- Exit: paglets can only message what they hold capabilities for; revoked and
  expired capabilities fail; conformance tests for every capability operation.

**WP7: Image store and persistence**

- Snapshot/restore at quiescent points; paged image format with page hashes.
- Durable inactive paglets, activation on incoming message, startup recovery.
- Periodic checkpoints (policy per paglet or per service), per-paglet storage
  and scratch directories.
- Exit: kill -9 the host; on restart, paglets resume from their last image.

### Milestone M2: mesh governance and system paglets

**WP8: Mesh ledger and identity**

- Keys: admin, host, owner (`paglets keys init`); private keys encrypted at
  rest with an Argon2id-derived key or kept in the platform key store; local
  key agent; key rotation and revocation; canonical record encoding and
  signatures. No passwords or API keys anywhere.
- Ledger: genesis (`paglets mesh create`), record types, logical clocks,
  deterministic state derivation, revocations winning, admin quorum rules,
  checkpoints and pruning.
- Gossip replication and anti-entropy between hosts; partition and merge
  behavior.
- Host and owner enrollment flows (request from any host, admin approval).
- Passports and passport verification.
- Exit: three hosts converge on the same state after partitions; an admin
  approves an enrollment from any host; records from a removed admin are
  ignored.

**WP9: Policy and grants**

- Policy rules in the ledger (allow / ask / deny, host selectors, scopes,
  limits), local evaluation on every host.
- `grants` system paglet: request, status, release; pending requests visible
  to admins mesh-wide; materialization of capabilities from grants on the
  paglet's current host.
- Capability manifests: transfer preflight and early approval.
- Default grants; audit records.
- Exit: a roaming paglet gets file access only after a matching rule or an
  admin approval given from a different host; access ends on expiry or
  revocation everywhere; every decision is audited.

**WP10: System paglet framework and portable host services**

- `SystemPaglet` interface for native system paglets; loading of trusted Wasm
  system paglets from the ledger's module trust; privileged import set.
- Service contracts from reflected descriptors, typed operations, service
  registry; application services use the same mechanism.
- Host platform layer for macOS, Linux and Windows: files, volumes, system
  information, load, processes, with uniform schemas and named roots.
- System paglets: `files` (find, list, stat, read, write, mkdir, move, delete,
  mount), `server-info` (summary, load, volumes, processes), `directory`,
  `storage`, `artifacts`, `pubsub`, `user-info`.
- Exit: the same roaming paglet finds files, reads content and reports load
  and free space on a macOS, a Linux and a Windows host with identical result
  schemas.

### Milestone M3: movement between hosts

**WP11: Module store and code mobility**

- Content-addressed module store, fetch-on-miss from source or peers, hash
  verification, module trust from the ledger, compiled-code cache, garbage
  collection of unused modules.
- Exit: a host with no application code receives and runs a paglet.

**WP12: Networking and movement**

- HTTPS server and client; end-to-end encrypted, mutually authenticated
  channels between hosts and for CLI sessions (challenge-response with enrolled
  keys, forward secrecy); signed envelopes.
- Movement protocol with page manifest and page dedupe, zstd, streaming.
- Capability mobility: re-creating grant capabilities on arrival, dropping
  host-local handles, `arrived` event with capabilities that could not be
  re-created; holding messages during a move.
- Transfer tickets: destination, retries, host selectors, manifest preflight,
  arrival mode.
- Exit: dispatch and clone between two hosts; repeated moves transfer only
  changed pages; grants follow the paglet; failed transfers leave the paglet on
  the source.

**WP13: Location and pinning**

- Distributed location records on responsible hosts chosen by consistent
  hashing of the paglet ID, majority update on every move commit, move
  counters, rebalancing when hosts join, leave or fail; location caches,
  tombstones, mesh-wide "who holds P" queries.
- `locator` system paglet: locate, locate_and_pin, release; redirect following;
  pin leases, persistence across restarts, policy limits, admin force-release.
- Exit: a paglet moving continuously between hosts can be located and pinned,
  also after any one host is shut down; while pinned, its dispatch returns
  `pinned` until the pin ends.

**WP14: Mesh**

- Peer discovery by gossip and multicast beacons; any enrolled host can serve
  as a bootstrap contact (the ledger's host list replaces fixed seed hosts);
  host registry, online state, restricted to enrolled hosts.
- Compatibility gate on mesh protocol version and paglet ABI version.
- Exit: three hosts discover each other and move paglets by host name.

**WP15: Relaying for hosts without inbound ports**

- A host without inbound ports keeps outbound connections to several reachable
  peers, chosen dynamically; every reachable host relays for others as a normal
  duty, not as a designated hub. Relayed traffic stays end-to-end encrypted.
- Exit: a host behind NAT participates in the mesh, and keeps participating
  when one of its relaying peers goes down.

### Milestone M4: compute, AI and web services, parity and release

**WP16: Mesh information and compute services**

- `mesh-info` and `compute-slots` as native system paglets on every host.
  Scheduling stays per host with peer coordination (queues, spillover,
  rebalancing); there is no central scheduler or job queue.
- compute-slots extensions enabled by images: checkpointed jobs, resumption
  after host loss, live rebalancing of chunked jobs, dedicated workers.
- Exit: the pi compute example schedules across three hosts and survives the
  loss of one host mid-run.

**WP17: Service offers, AI and web system paglets**

- Service offers: typed feature announcements by system paglets, dynamic
  updates, gossip through `mesh-info`, `find_offers` queries, offer targets in
  transfer tickets (resolved at departure with manifest and residency checks).
- `ai` system paglet with pluggable backends:
  - Apple Foundation Models on macOS 27 (Golden Gate), Apple silicon with Apple
    Intelligence enabled, through a small Swift bridge with a C interface
    (built only on macOS).
  - Ollama through its local HTTP API on hosts where it is installed; admins
    choose the exposed models.
  - llama.cpp embedded as a later option for hosts without Ollama.
- Operations: capabilities, summarize, classify, extract, generate, embed,
  describe_image; asynchronous replies for long requests; quotas per owner.
- `web` system paglet on hosts allowed to reach the internet or specific
  sites: search (host-configured backend), fetch, extract_text, download into
  artifacts; URL-scoped grants; refusal of internal destinations by default;
  use of the host's proxy settings.
- Data residency marks on paglets and artifacts (security design 7.4).
- Exit: the Download Courier demo, started on a host without internet access,
  fetches a file through the only web-enabled host and delivers it to its
  starting host; a fetch of an intranet address through `web` is refused.
- Exit: the AI Document Digest demo finds files on a Linux and a Windows host,
  moves to the only macOS 27 host to summarize them, and writes the digest on a
  third host; a paglet carrying content from a `host-only` root is refused
  when it tries to leave.

**WP18: Patterns library (guest SDK)**

- Task/result routing, multi-operation routing, mesh fan-out coordination,
  locate-and-pin helpers, notifications, single-file mobility.

**WP19: CLI and tooling**

- `paglets` CLI for paglets/cpp: host, mesh, jobs, artifacts, examples, status.
- Module management: build, sign, push, list, inspect (schema-driven decoding
  of state and messages).
- `paglets admin`: works against any live host with the admin key: pending
  requests, approve/deny, policy rules, enrollments, module trust, grants,
  revocations, pins, mesh-wide audit.
- `paglets keys`: owner key creation and enrollment requests.

**WP20: Examples and benchmarks**

- Demo paglets as proposed in [cpp-demo-paglets.md](cpp-demo-paglets.md)
  (Mesh Journey, Mesh File Finder, Storage Analyzer, Duplicate Finder, File
  Courier, Tree Compare, Log Scout, Mesh Top, Volume Guard, Inventory
  Collector, Process Finder, Mesh Benchmark, Latency Map, Pi Marathon, Hide and
  Seek, AI Document Digest, Semantic Mesh Search, Image Describer, Log
  Explainer, Download Courier, Web Researcher, Release Watcher), each with a `paglets demo <name>` command and a multi-host CI test.
  Demos that only need earlier milestones are built alongside them.
- Benchmarks: activation latency, message throughput (including worker IPC
  overhead), move latency, memory per paglet, image transfer sizes with and
  without dedupe.

**WP21: Security hardening**

- Fuzzing of all wire decoders, channel handshakes, ledger records, the image
  loader and capability transfer; quota, pin and clone-bomb tests; ledger
  partition tests; key handling review (storage, agent, rotation); review of
  worker sandboxing per platform; external review of the cryptographic
  protocol.
- Exit: fuzzers run in CI; threat model reviewed against the implementation.

**WP22: Documentation and release**

- Documentation site for paglets/cpp built with **ckdocs** (from
  ck-git-hosting): `ckdocs.yml` plus a `docs/` tree, `ckdocs check` (strict:
  broken links and heading fragments fail) as a CI step, published on ck-git
  Pages and GitHub Pages. Pages stay within ckdocs' Markdown subset: no raw
  HTML, footnotes, definition lists, math or Mermaid; diagrams as text or
  image files.
- README updates, packaging (release archives per platform, Homebrew
  formula), changelog, first release `cpp-v0.1.0`.

### Research work packages

- **WP-R1: Mid-handler migration.** Suspend a paglet inside a handler (for
  example via Asyncify or Wasm stack switching) so long computations can move
  without chunking.
- **WP-R2: Other guest languages.** Any language that compiles to core Wasm
  with the paglet ABI (C, Zig, AssemblyScript, TinyGo) could provide paglets.
- **WP-R3: Deterministic replay.** Record imports per handler to replay a
  paglet from a checkpoint for debugging.
- **WP-R4: Round 2 paglets.** After the first release: 19 further mobile agents
  (federated statistics, instrument harvester, pipeline traveller, data mule,
  follow the awake, clean room, mission agent, MCP gateway and more) and the
  platform features they need, collected in
  [cpp-round2-paglets.md](cpp-round2-paglets.md).
- **WP-R5: Persona agents (round 3).** Chat-capable persona paglets with a
  charter, memory and mission that travel to AI hosts to think, prepare
  executable plans there and carry them out on other hosts; concept and work
  packages P1–P12 in [cpp-persona-agents.md](cpp-persona-agents.md).

## 5. Risks

| Risk | Mitigation or decision point |
|---|---|
| Clang lacks C++26 reflection, so guest code cannot reflect directly | Build-time generator compiled with GCC 16 (WP2); revisit when clang ships reflection |
| GCC 16 is the only reflecting host compiler; ties macOS to Homebrew GCC and Windows to MinGW-w64 | Keep reflection in the wire/schema layer; verified per platform in WP2 |
| WAMR running modes differ per architecture (JIT/AOT availability, speed) | Measured in WP1; interpreter as universal fallback, AOT cache where possible |
| Snapshot of non-exported globals and internal runtime state | Verified in WP1 before any further work; this is the go / no-go item |
| Worker IPC overhead | Shared-memory rings for payloads and images; measured in WP20 |
| Large memory images | Page dedupe, zero-page elision, compression, per-paglet memory quotas |
| Code upgrades invalidate images | Explicit migration hooks (export/import state via generated schema) |
| Custom end-to-end channel protocol could contain design flaws | Follow an established pattern (Noise framework) on libsodium primitives; external review in WP21 |
| Replicated ledger adds distributed-state complexity | Append-only signed records, deterministic derivation, revocations win; partition tests in WP8 and WP21 |
| Capability model makes simple paglets harder to write | Default grants, guest SDK wrappers, typed service clients, good examples |
| Apple Foundation Models is Swift-only and tied to macOS 27 with Apple Intelligence | Small Swift bridge behind the `ai` backend interface; Ollama and later llama.cpp as portable backends |
| A web gateway host could be abused for SSRF or data exfiltration | Destination checks against private ranges, URL-scoped grants, read-only operations in release 1, residency marks, fuzzing in WP21 |
| Local AI backends change their APIs and model formats | Backends behind one typed `ai` contract; backend versions reported in offers |
| Platform differences in files, volumes and processes | Named roots, uniform schemas, per-platform conformance tests in WP10 |
| A compromised enrolled host can read paglet memory | Out of scope to prevent; admins revoke its enrollment; owners restrict paglets to host labels |

## 6. Decisions

Decided by the owner:

| Topic | Decision |
|---|---|
| Name and location | paglets/cpp, in this repository under `cpp/`, branch `cpp` |
| Interop | None with the Python edition |
| Versioning | Own tags `cpp-vX.Y.Z`, starting at `cpp-v0.1.0` |
| Isolation | All paglets run in pooled worker processes |
| Owner keys | Generated by the CLI (`paglets keys init`) |
| Security governance | Per mesh, by mesh admins identified by admin keys, acting from any live host |
| Cryptography | Public-key only (Ed25519 / X25519); no passwords or API keys; private keys never leave their machine; end-to-end encrypted channels |
| Host roles | All hosts equal; no central, hub or home host |
| Grant scope | Mesh-wide; grants follow paglets across hosts of the mesh |
| Location | Dynamic mesh-wide location, with `locate_and_pin` for a requested duration |
| Host services | Portable system paglets (files, system info, load, volumes); no command execution |
| Platforms | macOS, Linux and Windows |

Still open: see the security design,
[section 11](cpp-security-and-communication.md#11-decisions).
