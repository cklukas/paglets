# Configuration and troubleshooting

paglets/cpp hosts have no configuration file. Everything a host needs is
given on its command line (see the [command-line reference](cli-reference.md))
or comes from the mesh ledger. This page describes what hosts keep on disk,
how keys are handled, which ports they use, their fixed limits, the error
codes, and the problems that come up most often.

## State directories

`run`, `list` and `call` keep their state in `--state-dir DIR`. `serve`
keeps its state in `--state DIR` and puts the runtime's part in a
subdirectory, so the two layouts differ. Do not use one directory for both.

`paglets-host run --state-dir DIR` (and `list`, `call`):

| Path | Contents |
|---|---|
| `DIR/paglets/<id>.paglet` | each paglet's record (module, owner, capabilities, timers) and last memory image, replaced atomically |
| `DIR/modules/<hash>.wasm`, `<hash>.meta` | the module store: modules by SHA-256 hash, with their metadata |
| `DIR/storage/<id>/` | a paglet's durable storage (the `storage` service) |
| `DIR/work/<id>/` | a paglet's scratch space; cleared when the host starts |
| `DIR/revoked` | the IDs of capabilities revoked on this host |
| `DIR/services/directory.state`, `pubsub.state` | published names and topics |
| `DIR/services/shares.state` | the files this host shares with `files.share` |
| `DIR/artifacts/` | the `artifacts` store |

`paglets-host serve --state DIR`:

| Path | Contents |
|---|---|
| `DIR/runtime/` | the runtime's state, laid out as `paglets/`, `modules/`, `storage/`, `work/` and `revoked` above |
| `DIR/services/services/` | `directory.state`, `pubsub.state` and `shares.state` |
| `DIR/services/artifacts/` | the `artifacts` store |
| `DIR/node/node.state` | the node's state: committed moves, held messages, pins |
| `DIR/node/hosts` | the host registry, so a restarted host reaches its peers without a contact |
| `DIR/node/locations` | the location records this host keeps for the mesh |
| `DIR/node/passports/<id>.passport` | the passports of the paglets on this host |

Files and directories appear when they are first needed. Without a state
directory, `run` keeps everything in memory and uses temporary
directories, which are gone when it ends.

A **ledger directory** (`--ledger`) holds one file per record,
`<record-id>.rec`. A host's ledger copy grows as records arrive by gossip.
`ledger show` prints what the records add up to.

## Keys and passphrases

`keys init` makes four kinds of keys:

| Role | Used by | Stored |
|---|---|---|
| `admin` | `mesh create`, the `ledger` commands that need `--admin`, `remote` sessions as an admin | encrypted with a passphrase |
| `owner` | `ledger request`, `remote launch` and the other `remote` sessions as an owner | encrypted with a passphrase |
| `signer` | `ledger sign-module` | encrypted with a passphrase |
| `host` | `serve` | unencrypted, readable only by the file's owner |

- Encrypted keys use Argon2id and XChaCha20-Poly1305. `--kdf moderate`
  (the default) uses libsodium's moderate limits, `--kdf interactive` its
  faster interactive limits.
- Commands ask for passphrases on the terminal, without echo. Scripts give
  `--passphrase-file FILE` instead: its first line is the passphrase for
  every key of the command. Keep such files as private as the keys.
- `serve` opens its host key without a passphrase, so a host key created
  with `--passphrase-file` cannot be served. The error is
  `the key in FILE needs its passphrase`.
- On POSIX systems key files are created with mode `0600`. On Windows they
  rely on the user's profile directory being private.
- `keys init` refuses to overwrite an existing file.
- `keys show FILE` shows a key's ID, role and name without the passphrase.

## Ports and firewalls

| Traffic | Protocol and port | Direction |
|---|---|---|
| Host channels, CLI sessions | HTTPS on the `--listen` address (default `127.0.0.1:0`: loopback only, a free port) | inbound to every host that listens; outbound from every host and from `paglets-host remote` |
| Multicast beacons | UDP, group `239.255.80.71`, port 47471 (`--beacon-port`), TTL 1, every 5 s | the local network only |
| `web` gateway | HTTPS and HTTP to the sites paglets fetch, or to the proxy | outbound, on `--web` hosts |
| `ai` gateway | HTTP to Ollama (default `http://127.0.0.1:11434`) | local, on `--ai` hosts |

- The default `--listen` accepts connections from the same machine only.
  For hosts on other machines, listen on `0.0.0.0:PORT` or a specific
  address, open that TCP port in the firewall, and give `--advertise` with
  a URL the other hosts can reach.
- A host behind NAT or a firewall that allows no inbound connections runs
  with `--no-listen`. It needs only outbound HTTPS to some hosts that
  listen. They relay for it.
- Beacons only help on one network segment (TTL 1). Where multicast is
  blocked, hosts still find each other through `--join` and gossip. A host
  that cannot join the multicast group logs
  `[mesh] no multicast beacons: <reason>` and runs without them.
- Several hosts on one machine can share the beacon port.

TLS certificates and CA checks are described in
[Hosts and meshes](hosts-and-meshes.md#tls-certificates).

## Limits

These limits protect a host and its other paglets. They are defaults, not
limits imposed by the machine's installed RAM. The `serve --memory-mb N`
option in `paglets-host` changes the per-paglet linear-memory ceiling. Other entries
below are configured by a program embedding the runtime through the named
configuration structures.

| Limit | Default | Where it is set |
|---|---|---|
| Paglets of one owner on a host | 10 000 | `runtime::Config::owner_paglet_limit` |
| Children and clones per handler call | 16 | `runtime::Config::spawn_limit_per_call` |
| Pins on one paglet | 64 | `runtime::Config::pin_limit` |
| Time per handler call | 5 s; a handler that runs longer is stopped and its paglet ends | `runtime::Config::handler_budget` |
| Messages in a paglet's mailbox | 1024 | `runtime::Config::mailbox_limit` |
| Serialized ready notifications captured by one move | 64 MB | `runtime::Config::move_ready_bytes` |
| Capabilities per paglet | 1024 | `runtime::Config::cap_limit` |
| Pending timers per paglet | 64 | `runtime::Config::timer_limit` |
| Linear memory per paglet | 256 MB (4096 pages) | `serve --memory-mb N`, or `wasm::Limits::max_memory_pages` |
| Pages cached for later moves | 256 MB per host | `node::Node::set_page_cache_bytes` |
| Message size | 1 MB | `abi::max_document_size` |
| Storage per paglet | 16 MB | `services::ServicesConfig::storage_quota` |
| The whole artifacts store, including content fetched from other hosts | 1 GB | `services::ServicesConfig::artifacts_limit` |
| Files a host remembers as shared | 10 000; the oldest are forgotten first | `max_shares` in `cpp/host/src/services/artifacts.cpp` |
| Notifications kept per owner | 100 | `services::ServicesConfig::notifications_per_owner` |
| Compiled modules cached without users | 64 MB | `runtime::Config::module_cache_bytes` |
| Forwarding tombstone lifetime after movement | 10 min | `runtime::Config::tombstone_ttl` |
| Message forwarding bound | 4 | compiled in (`max_forwards`) |
| Location heartbeat / host-silence timeout | 2 s / 10 s | `node::Node::LocationTiming::heartbeat`, `host_timeout` |
| Holder record refresh / record expiry | 5 min / 1 h | `LocationTiming::refresh`, `record_ttl` |
| Per-phase location lookup deadline | 3 s | `LocationTiming::lookup_timeout` |
| Message-routing location cache | 10 s | compiled in (`cache_ttl`) |

An embedder applies location timings with `Node::set_location_timing`;
`serve` has no flags for them. Pin authorization, duration limits and lookup
fallbacks are described in [Location and pins](hosts-and-meshes.md#location-and-pins).

For example, add `--memory-mb 256` to a host's `serve` command to allow up
to 256 MB per paglet. This is a ceiling, not a reservation of that much
physical RAM for every paglet. The heap, static data and C/C++ linear-memory
stack share it, so usable application memory is smaller; host and worker
overhead is additional. The default does not automatically scale with host
RAM. A destination host must allow enough memory to restore a moving
paglet's image, so configure every intended destination accordingly.

A move captures already queued replies, timer deliveries and host reports
with the memory image. The 64 MB notification limit bounds this extra
serialized state; it does not allocate that memory for every paglet. The
encoded move offer must also fit `net::TransportConfig::max_frame`.
Exceeding either limit reports `on_move_failed` on the source host and
leaves the queued deliveries there. Queued notifications require travel-state
version 2; unresolved requests use version 3, which also carries their budget
capture time. Arrival subtracts elapsed transit time before arming local
monotonic deadlines, including the wait for commit. Across hosts the capture
time uses Unix milliseconds, like timer and capability expiry times. The
remaining duration never increases beyond the captured budget. An older
destination that cannot read the required version refuses the move.

At the linear-memory ceiling, `memory.grow` fails. Code that checks a
failed `malloc` can handle it; ordinary failing C++ `new` in guests built
without exceptions aborts. A trap or abort ends the paglet as `failed`
(status -14 for waiting callers). Host delivery also fails the paglet if
its `paglets_alloc` export cannot provide a valid input buffer.

Linear memory never shrinks; freeing heap objects usually leaves nonzero
pages that still contribute to the image. Keep large data outside the
paglet heap; see [Memory and image size](writing-paglets.md#memory-and-image-size).
The page cache is a separate host-wide budget for reusing previously seen
pages across moves.

Compute jobs have a separate default ceiling and maximum of 1 GB per job,
configured through `runtime::Config`; `serve --memory-mb` does not change
them. See [Compute jobs](compute-jobs.md).

The runtime's configuration is in
`cpp/host/include/paglets/runtime/runtime.hpp`, the services' in
`cpp/host/include/paglets/services/system_services.hpp`, the memory limit
in `cpp/host/include/paglets/wasm/engine.hpp` and the message size in
`cpp/common/include/paglets/abi.hpp`. The
[hardening design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-hardening.md)
explains them.

## Error codes

Calls that fail report one of these codes: to a paglet as a `Result`
error or a reply status, and on the command line as `error: <name>`.

| Code | Name | Meaning |
|---|---|---|
| -1 | `invalid_argument` | a parameter is out of range, or a document is incomplete |
| -2 | `bad_handle` | the capability handle does not exist, or has the wrong kind |
| -3 | `denied` | the capability, the mesh policy or the paglet's trust class does not allow the operation |
| -4 | `expired` | the capability has expired |
| -5 | `quota` | a quota or limit is exhausted: a full mailbox, too many paglets of an owner, too many pins, the storage quota, the uses of a capability |
| -6 | `too_large` | a document or message exceeds its size limit |
| -7 | `malformed` | a body is not valid MessagePack, or does not decode into the expected type |
| -8 | `not_found` | the target paglet, module or service does not exist |
| -9 | `bad_state` | not possible in the current lifecycle state, for example disposing while a dispatch is pending |
| -10 | `unsupported` | not supported by this host |
| -11 | `internal` | an error of the host |
| -12 | `timeout` | reply status: no reply within the request's timeout |
| -13 | `unknown_message` | reply status: the receiver has no handler for the message |
| -14 | `failed` | reply status: the receiver trapped or its handler exceeded its elapsed time budget (5 s by default); the receiver has ended, and the other requests waiting for its replies fail too |
| -15 | `gone` | reply status: the receiver was disposed of, or did not answer the request |
| -16 | `revoked` | the capability, one it was derived from, or its grant was revoked |
| -17 | `pinned` | `dispatch` while the paglet is pinned to its host |
| -18 | `cancelled` | a compute job was cancelled |

A message that a paglet sent asynchronously and that could not be
delivered is reported to the sender's `on_undelivered` with one of these
codes. See the [guest SDK reference](sdk-reference.md#lifecycle-hooks).

`paglets-host` itself exits with status 0 on success, 1 on a failure (with
`paglets-host: <reason>` on stderr) and 2 on a usage error.

## Common problems

### Guests are not built

The configure step prints

```text
paglets: no WASI clang/sysroot found (set PAGLETS_WASI_CLANG and PAGLETS_WASI_SYSROOT); guests are not built
```

or `paglets: WASI toolchain probe failed, guests are not built`, followed
by the compiler's error. The host builds anyway, but there are no
`guests/*.wasm` and the tests that need them are skipped.

- Install the guest toolchain (see [Building and testing](building.md#toolchains)),
  or point CMake at it:

  ```bash
  cmake --preset linux-gcc16 -DPAGLETS_WASI_CLANG=/opt/wasi-sdk/bin/clang \
      -DPAGLETS_WASI_SYSROOT=/opt/wasi-sdk/share/wasi-sysroot
  ```

- On Linux, `WASI_SDK_PATH` pointing at a wasi-sdk installation also works.
- The found paths are cached in `CMakeCache.txt`. After installing a
  toolchain, configure again with the two options, or remove the build
  directory.
- When the probe fails, its error usually names a missing piece of the
  sysroot (for example the C++ library from `wasi-runtimes`) or a missing
  `clang++` next to `clang`.
- Hosts without a guest toolchain can use modules built elsewhere: Wasm
  modules are platform-independent (`PAGLETS_PREBUILT_GUEST_DIR` for the
  tests).

A module with a `SCHEMA_HEADER`, `SERVICES` or `PATTERNS` is skipped with
`guest NAME needs the schema generator (C++26 reflection); skipped` when
the host compiler has no reflection.

### A host is not enrolled yet

A host whose key is not enrolled in its ledger copy still starts with
`serve`. It treats its `--join` and `--peer` hosts as seeds and waits. The
enrolled hosts refuse its channels until they know it. While it waits, it
logs lines such as

```text
[mesh] https://lab-1:7443: channel to https://lab-1:7443 refused: HTTP 403: not a host of this mesh
```

and the enrolled host logs
`[net] channel refused for host <id>: not a host of this mesh`. The joining
host retries every 2 seconds. It logs the error again when it changes, and
otherwise once a minute, ending with `(still; retried every 2 s)`.

To let it in, an admin enrolls its key and brings the record to a live
host:

```bash
$H keys show lab-2.key                                           # the key ID
$H ledger enroll --ledger ledger --admin alice.key host <lab-2-key-id> lab-2
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

The next attempt succeeds (`[mesh] joined through ...`), and the new host
receives the ledger records, its own enrollment among them, by gossip.

### A remote command refuses the host

`paglets-host remote` accepts a host only if it is enrolled in the ledger
copy given with `--ledger`. The error is
`host <id> is not enrolled in this ledger copy`. Use a ledger copy that
has the host's enrollment, or name the host's key with `--host-key KEY-ID`.

### Hosts on other machines cannot connect

`serve` listens on `127.0.0.1` unless told otherwise. Start it with
`--listen 0.0.0.0:PORT` and `--advertise https://NAME:PORT`, and open the
port in the firewall. A host that announces an address the others cannot
reach shows as `(no address)`, `offline` or `one-way` in `remote hosts`.

### A host with --tls-ca cannot reach another host

A host started with `--tls-ca` drops the frames for a host whose
certificate fails the check. It logs the first failure with the reason,
then `still dropped (N more times, failing for S s)` at most once a
minute, and `delivered again` when frames get through. `remote hosts` on
that host shows the other one as `one-way`, with `send failed:` and the
reason.

```text
[net] frames to 35115e1c0c962d6e dropped: opening a channel to https://lab-3:7443: HTTP error: SSL server verification failed (unable to get local issuer certificate)
```

The reasons:

- `unable to get local issuer certificate`: the certificate is
  self-signed (the default), or from a CA that is not in the CA file. If
  it comes from an intermediate CA, put the intermediate certificate after
  the host's certificate in its `--tls-cert` file.
- `certificate has expired`: renew it. Its host warns about this when it
  starts.
- `SSL server hostname verification failed`: the certificate does not
  name the host or IP address of the URL that the host is reached at (its
  `--advertise` URL).

See [TLS certificates](hosts-and-meshes.md#tls-certificates).

### quota

`quota` comes from one of the [limits](#limits). Typical causes:

- a paglet that clones or creates children in a loop reached the owner's
  10 000 paglets on that host. A paglet that cannot arrive on such a host
  stays where it was and gets `on_move_failed`;
- a paglet's mailbox holds 1024 messages that it does not handle fast
  enough;
- a paglet's storage reached its 16 MB.

### Builds without reflection

A host built without C++26 reflection (`linux-host-only`) runs paglets
with `run`, but it has no system paglets and no mesh host. `serve` fails
with `this build has no mesh host (no reflection)`, and `--root` with
`this build has no system paglets (no reflection)`. `paglets-host --info`
shows `reflection: no`.

### Raspberry Pi and other 39-bit address spaces

Guard-page bounds checks reserve 8 GB of virtual address space per Wasm
instance. On a 39-bit kernel, a process can hold only about 60 instances.
This reservation is separate from physical RAM and the configured 256 MB
paglet / 1 GB compute-job linear-memory ceilings.

On Linux ARM64, a hardware-check build probes a 1 TB virtual reservation
once when WAMR starts, in the host and each worker. It immediately releases
the reservation and commits no physical pages. If the probe fails, startup
warns about restricted density and names the software-check build option.
Worker startup warnings appear in the host's log.
The failure may come from a small address space or a process limit; the
diagnostic does not identify an exact kernel address width.

Build with `-DPAGLETS_WAMR_HW_BOUND_CHECK=OFF` to use software bounds checks
(see [Building and testing](building.md#build-and-test)). Both modes enforce
the configured linear-memory ceilings; physical memory grows with use.

### Modules and memory images

The module store accepts the imports implemented for roaming and resident
paglets. `paglets_sys` imports are rejected: system paglets currently run
as native C++ services.

Memory images preserve linear memory and exported numeric mutable globals.
Mutable reference or vector globals are refused, because their state is
not represented by the image format.
Modules that mutate tables or drop passive segments are rejected, because
that state is not captured. The check decodes instruction boundaries;
unknown instruction extensions are refused until their snapshot safety
can be checked. Static function tables used for C++ virtual calls remain
supported.

Before calling an export, the engine checks the exact i32 argument list
and the expected void/i32/i64 return type. Wrong signatures and multi-value
returns are rejected before the guest can write into the host call buffer.
Random bytes and generated IDs use libsodium's OS-backed secure source.

### Worker sandbox limits

macOS workers apply an SBPL profile after their IPC connection and runtime
resources are established. It denies process creation/execution, network
operations, file writes and file-content reads. Already held resources and
mapped memory remain usable. The profile otherwise allows operations by
default; it is not a general syscall allow list.

Linux workers use a seccomp allow list for memory management, threads,
clocks, anonymous pipes and IPC over inherited descriptors. Anonymous pipes
also let sanitizer builds check C++ object pointers inside the sandbox.
Other system calls return
`EPERM`; thread creation is restricted to pthread flags, signals to this
worker, and resource-limit calls to reading this process's limits.

Windows workers start in a distinct Less Privileged AppContainer (LPAC),
with no network capabilities. A job object limits each worker to one
process and restricts UI access; its token has Low integrity and no
privileges. The only capability is `registryRead`, needed to initialize
Winsock. Runtime executables and DLLs are copied into a protected,
read/execute-only bundle. Windows still permits system resources granted
to LPACs and the worker's own private AppContainer storage; this is not a
blanket denial of all file access. Profiles and runtime bundles are
removed when their owning processes exit normally.

Worker launches pass only their explicit IPC handles. Sandboxed workers
do not inherit host standard input, output or error. They receive selected
runtime settings (temporary directories, locale, time zone, library paths
and sanitizer options), plus the Windows system/profile paths needed for
AppContainer startup. Other host environment variables, such as service
credentials, are omitted. The host must be stopped before replacing its
Windows worker executable or DLLs, whose source bytes remain pinned for
its lifetime.

`paglets-worker --check-sandbox` checks the platform restrictions. On
Windows it launches a child through the real LPAC path, probes IPv4 and
IPv6 denial before removing token privileges, then checks the restricted
token, process creation, host-file access and host-memory access. Network
probing precedes privilege removal because removing the traversal
privilege can prevent Winsock from reopening its provider.
