# paglets/cpp: paglet ABI v1

Status: **final for ABI v1** (WP3 closed; review in section 12). Implemented
by the guest SDK (`cpp/sdk`) and the host runtime (`cpp/host`); every
conformance test of section 11 runs in CI (`cpp/tests/test_runtime.cpp`),
in-process and with worker processes. Companion to
[cpp-edition-plan.md](cpp-edition-plan.md) and
[cpp-security-and-communication.md](cpp-security-and-communication.md).

The paglet ABI is the only contract between a host and a paglet module. It is
plain core WebAssembly (no component model): a few exports the paglet
provides, a few imports the host provides, and MessagePack documents passed
through linear memory. The ABI v0 of the M0 spike (`paglets_handle` returning
a reply) is replaced entirely.

## 1. Module requirements

A paglet module for ABI v1:

- is a core Wasm module for `wasm32` with exactly one linear memory, exported
  as `memory`;
- is a reactor: it may export `_initialize`, which the host runs once when the
  paglet is created (never again after a memory-image restore);
- exports every mutable global it defines, so memory images can capture them
  (C/C++ guests: `-Wl,--export=__stack_pointer`);
- does not mutate its tables at runtime (function pointers stay valid across
  memory images; C and C++ guests satisfy this; the host cannot verify it);
- exports the version marker `paglets_abi_v1` (section 2);
- imports only functions from the import set of its trust class (section 5):
  the host rejects the module at load time otherwise.

The host checks all of this statically before instantiating the module.

## 2. Version negotiation

The ABI version is declared by an exported function named
`paglets_abi_v<N>` with signature `() -> ()`. The host never calls it; it only
looks at the export names. A module may declare several versions; the host
uses the highest version it supports and rejects the module if it supports
none. Within a major version, additions are compatible:

- new imports: a module using an import the host does not know is rejected at
  load time with an error naming the import;
- new keys in MessagePack documents: both sides ignore keys they do not know;
- new event kinds, message kinds and error codes: the guest treats unknown
  event kinds as no-ops and unknown error codes as generic errors.

`self_info` (section 5.3) reports the ABI version in use and, since v1.1,
the minor version of the host (`minor`), which counts the compatible
additions of section 13.

## 3. Exports

| Export | Signature | Required | Purpose |
|---|---|---|---|
| `paglets_abi_v1` | `() -> ()` | yes | Version marker |
| `paglets_alloc` | `(size: i32) -> i32` | yes | Allocate `size` bytes for data from the host; 0 on failure |
| `paglets_free` | `(ptr: i32) -> ()` | yes | Release a block from `paglets_alloc` |
| `paglets_on_event` | `(kind: i32, ptr: i32, len: i32) -> i32` | yes | Lifecycle events (section 6) |
| `paglets_on_message` | `(ptr: i32, len: i32) -> i32` | yes | Message delivery (section 7) |
| `paglets_state_export` | `() -> i64` | no | State migration (reserved): returns `(ptr << 32) \| len` of a state document |
| `paglets_state_import` | `(ptr: i32, len: i32) -> i32` | no | State migration (reserved): installs a state document |
| `_initialize` | `() -> ()` | no | Reactor initializer (C/C++ static constructors) |

The state migration exports are reserved for code upgrades (plan, section
3.2); ABI v1 hosts accept but do not call them yet.

### 3.1 Handler results

`paglets_on_event`, `paglets_on_message` and `paglets_state_import` return:

| Value | Meaning |
|---|---|
| `0` | Handled |
| `1` | Not handled: the paglet does not know this message or event |
| negative | An error code (section 9) |

For a request (section 7.2) whose handler returns `1` or a negative value
without having replied, the host replies to the requester with status
`unknown_message` or the returned error code.

A trap (unreachable, out-of-bounds access, stack exhaustion, exceeded time
budget, `proc_exit`) fails the paglet: the host discards the instance, answers
every pending request of the paglet and every reply capability it holds with
status `failed`, and ends the paglet. A trap is the paglet's own fault, so it
does not resume from a checkpoint. When the worker process running a paglet
ends instead (plan, section 3.4), the paglet in the call fails and the
others on that worker resume from their last image; without an image they
fail as well.

## 4. Memory ownership

- **Host to guest**: to pass a document of `len` bytes, the host calls
  `paglets_alloc(len)`, copies the document, calls the handler with
  `(ptr, len)` and calls `paglets_free(ptr)` after the handler returns. The
  guest must not keep pointers into the buffer. For `len == 0` the host
  passes `(0, 0)` without allocating.
- **Guest to host**: imports take `(ptr, len)` pairs. The host validates that
  the range lies inside linear memory and copies what it keeps before the
  import returns.
- **Host results**: imports that return a document write it to a guest buffer
  `(out, cap)` and return the document length. If the length is larger than
  `cap`, nothing is written and the guest retries with a larger buffer. A
  negative result is an error code.
- **State export**: the buffer returned by `paglets_state_export` stays valid
  until the next call into the guest.

The host calls into a paglet only from one thread at a time, and only at
quiescent points takes memory images: between two handler calls, never while
an import is running.

## 5. Imports

### 5.1 Import sets per trust class

| Trust class | Import modules |
|---|---|
| roaming, resident | `paglets` (section 5.2), WASI subset (section 5.4) |
| system (Wasm) | the above plus `paglets_sys` (section 5.5) |

### 5.2 Module `paglets`

All pointers are `i32` offsets into linear memory; `endpoint`, `handle` and
`reply` are capability handles (section 8).

| Import | Signature | Result |
|---|---|---|
| `log` | `(level: i32, ptr: i32, len: i32) -> ()` | Writes a log line (level 0 debug, 1 info, 2 warning, 3 error) |
| `self_info` | `(out: i32, cap: i32) -> i32` | Length of the self document (5.3) |
| `send` | `(endpoint: i32, msg: i32, len: i32) -> i32` | 0 or error |
| `request` | `(endpoint: i32, msg: i32, len: i32) -> i64` | Correlation ID (> 0) or error |
| `reply` | `(reply: i32, msg: i32, len: i32) -> i32` | 0 or error; consumes the reply capability |
| `cap_derive` | `(handle: i32, spec: i32, len: i32) -> i32` | New handle or error |
| `cap_drop` | `(handle: i32) -> i32` | 0 or error |
| `cap_inspect` | `(handle: i32, out: i32, cap: i32) -> i32` | Length of the capability document |
| `cap_list` | `(out: i32, cap: i32) -> i32` | Length of the list of held handles |
| `create_child` | `(spec: i32, len: i32) -> i32` | Endpoint handle to the new paglet, or error |
| `lifecycle` | `(op: i32, arg: i32, len: i32) -> i32` | 0 (or an endpoint handle for `clone`), or error |
| `timer_set` | `(delay_ms: i64, msg: i32, len: i32) -> i32` | Timer handle or error |
| `now` | `(clock: i32) -> i64` | Nanoseconds; clock 0 wall time since the Unix epoch, 1 monotonic |
| `random` | `(out: i32, len: i32) -> i32` | 0; fills the buffer with secure random bytes |

Lifecycle operations (`op`):

| Op | Name | Argument document | Effect |
|---|---|---|---|
| 1 | `dispose` | none | The paglet is disposed after the handler returns |
| 2 | `deactivate` | `{wake_after_ms?: int}` | Snapshot to the image store after the handler returns; activated again by the next message or after `wake_after_ms` |
| 3 | `clone` | `{args?: bin, caps?: [handle]}` | A clone is made from the image taken after the handler returns; returns an endpoint to the clone at once |
| 4 | `dispatch` | `{destination: str}` | Move to another host (milestone M3; `unsupported` before) |

At most one of `dispose`, `deactivate` and `dispatch` can be pending per
handler call (`bad_state` otherwise). They take effect at the next quiescent
point, after any clones of the same call. Children and clones are roaming
paglets of the creator's owner (clones of a system paglet as well) and count
against the per-call limit of section 10 (`quota` beyond it).

A clone starts with a copy of the original's memory image. Its capability
table holds its own endpoint (handle 1), copies of the original's
transferable endpoint capabilities under the same handle numbers (so handles
stored in its memory stay meaningful), and the capabilities passed in `caps`.
Non-transferable, reply and timer capabilities are not copied.

### 5.3 Documents

All documents are MessagePack maps with string keys. Required keys are marked;
absent optional keys take the stated defaults.

**Outgoing message** (`send`, `request`, `reply`, `timer_set`):

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | str | required (`reply` for `reply`) | 1 to 128 bytes; names starting with `paglets.` are reserved |
| `payload` | bin | empty | Application payload, usually MessagePack |
| `caps` | [int] | none | Handles to transfer; each must be transferable and is moved to the receiver on success |
| `priority` | int | 3 | 0 (lowest) to 7 (highest) |
| `timeout_ms` | int | 30000 | `request` only, positive: after this time the requester gets a reply with status `timeout` |

**Delivered message** (argument of `paglets_on_message`):

| Key | Type | Meaning |
|---|---|---|
| `kind` | int | 0 message, 1 request, 2 reply, 3 timer |
| `name` | str | Message name |
| `payload` | bin | Payload |
| `caps` | [int] | Transferred capabilities, already in the receiver's table |
| `priority` | int | Priority |
| `sender` | map | Host-stamped sender record: `id`, `owner`, `trust`, `module`, `host` (absent for timers and replies; a reply belongs to the request named by `correlation`) |
| `badge` | str | Badge of the endpoint the sender used, if any |
| `reply` | int | Requests: the one-shot reply capability |
| `correlation` | int | Replies: the correlation ID returned by `request` |
| `status` | int | Replies: 0, or an error code (`timeout`, `unknown_message`, `failed`, `gone`, ...) |

**Self document** (`self_info`): `id` (str), `module` (hex SHA-256), `owner`
(str), `trust` (`roaming`, `resident` or `system`), `host` (str), `abi` (int).

**Derive specification** (`cap_derive`): `ops` ([str], subset of the
source's operations), `expires_ms` (int, relative, cannot extend the source's
expiry), `uses` (int, cannot exceed the source's remaining uses),
`transferable` (bool, can only be cleared), `badge` (str, only if the source
has none).

**Capability document** (`cap_inspect`): `kind` (`endpoint`, `reply` or
`timer`), `target` (str), `ops` ([str]), `transferable` (bool), and when set
`badge` (str), `expires` (int, Unix milliseconds), `uses_left` (int).

**Child specification** (`create_child`): `module` (hex hash, default: the
creator's own module), `args` (bin), `caps` ([int], transferred). The child
gets exactly these capabilities; it receives no endpoint to its creator unless
one is passed in `caps`.

### 5.4 WASI subset

`wasi_snapshot_preview1` functions available to every paglet: `args_get`,
`args_sizes_get`, `environ_get`, `environ_sizes_get` (always empty),
`clock_res_get`, `clock_time_get`, `random_get`, `sched_yield`, `proc_exit`
(fails the paglet), `fd_write` (stdout and stderr go to the host log line by
line, stdout at level info and stderr at level warning; a partial line is
logged when the call that wrote it returns; other descriptors are `EBADF`),
`fd_close`,
`fd_seek`, `fd_fdstat_get`, `fd_prestat_get`, `fd_prestat_dir_name` (no
directories are preopened). File access inside mounted `dir` capabilities is
added with the files system paglet (WP10).

### 5.5 Module `paglets_sys`

Reserved for trusted Wasm system paglets (WP10). It will hold the privileged
host platform calls (files, volumes, system information) and minting of
service capabilities. A roaming or resident module that imports anything from
`paglets_sys` is rejected.

## 6. Lifecycle events

`paglets_on_event(kind, ptr, len)` with an event document:

| Kind | Event | Document | Delivered |
|---|---|---|---|
| 1 | `created` | `args` (bin), `caps` ([int]) | Once, before any message |
| 2 | `activated` | none | After a restore from the image store |
| 3 | `deactivating` | none | Before a snapshot to the image store |
| 4 | `arrived` | `from` (str), `lost` ([str]) | On the new host after a move (M3) |
| 5 | `cloned` | `original` (str), `args` (bin), `caps` ([int]) | In the clone, instead of `created` |
| 6 | `dispatching` | `destination` (str) | Before a move (M3) |
| 7 | `disposing` | none | Before disposal |

`created` and `cloned` are the paglet's first delivery, ahead of any message
already queued. The other events are delivered at the transition itself:
`activated` right before the delivery that activated the paglet,
`deactivating` and `disposing` after the handler call that requested them.
A paglet never sees two calls at the same time. Negative results are logged;
they do not stop the transition.

## 7. Messages

### 7.1 Delivery

- Each paglet has a mailbox; messages are delivered by priority (highest
  first), FIFO within a priority, one at a time.
- An inactive paglet is activated by the next message.
- A message to an endpoint is only accepted if the endpoint's operations
  contain the message name or `*`, the endpoint has not expired and has uses
  left; the host checks this in `send`/`request` and returns `denied`,
  `expired` or `quota` otherwise, `not_found` for an ended target and `quota`
  for a full mailbox.

### 7.2 Request and reply

- `request` returns a correlation ID. The receiver gets the message with
  `kind` 1 and a one-shot reply capability `reply`.
- The receiver answers with `reply(reply, msg)`, at once or from a later
  handler (the reply capability survives deactivation and memory images).
  A reply capability can be transferred like any other capability, so a
  paglet can delegate the answer.
- The requester gets exactly one reply message (`kind` 2, priority 7) with
  the correlation ID and a status: 0 for a real reply, or `timeout`,
  `unknown_message`, `failed` (the receiver trapped), `gone` (the receiver was
  disposed or dropped the reply capability) or the error code the handler
  returned.
- A reply capability dropped without replying produces status `gone`.

### 7.3 Timers

`timer_set(delay_ms, msg)` delivers `msg` to the paglet itself (`kind` 3)
after the delay (not negative; `invalid_argument` otherwise). The result is a timer handle; `cap_drop` cancels the timer.
Timers survive deactivation: a deactivated paglet is activated when its timer
fires.

## 8. Capability handles

- Handles are positive `i32` values indexing the paglet's host-side
  capability table; 0 is never valid. The guest cannot forge them: every use
  is checked against the table.
- Handle 1 is always the paglet's endpoint to itself (`*` operations,
  transferable). It cannot be dropped (`denied`); transferring it passes a
  copy.
- Kinds in ABI v1: `endpoint`, `reply`, `timer`; v1.1 adds resource
  capabilities provided by system paglets (`dir`, `file`, `artifact`,
  `topic`; section 13). `pin` arrives with M3.
- Transferring a capability moves it: the sender's handle becomes invalid and
  the receiver gets a new handle number.
- The capability table is host state and travels with the paglet as part of
  its record, not inside the memory image.

## 9. Error codes

| Code | Name | Meaning |
|---|---|---|
| -1 | `invalid_argument` | A parameter is out of range or a document is incomplete |
| -2 | `bad_handle` | The handle does not exist or has the wrong kind |
| -3 | `denied` | The capability does not allow the operation |
| -4 | `expired` | The capability has expired |
| -5 | `quota` | A quota or use limit is exhausted |
| -6 | `too_large` | A document exceeds the size limit |
| -7 | `malformed` | A document is not valid MessagePack or has wrong types |
| -8 | `not_found` | The target paglet or module does not exist |
| -9 | `bad_state` | Not possible in the current lifecycle state |
| -10 | `unsupported` | Not supported by this host or in this milestone |
| -11 | `internal` | Host error |
| -12 | `timeout` | Reply status: no reply within the timeout |
| -13 | `unknown_message` | Reply status: the receiver did not handle the message |
| -14 | `failed` | Reply status: the receiver trapped |
| -15 | `gone` | Reply status: the receiver was disposed or dropped the reply capability |
| -16 | `revoked` | v1.1: the capability, one it was derived from, or its grant was revoked |

## 10. Limits

Defaults of the host, configurable per host and later per mesh policy:

| Limit | Default |
|---|---|
| Message document size | 1 MB |
| Mailbox length per paglet | 1024 messages |
| Capabilities per paglet | 1024 |
| Pending timers per paglet | 64 |
| Children and clones per handler call | 16 |
| CPU time per handler call | 5 s (the call is terminated and the paglet fails) |
| Linear memory per paglet | 16 MB |

## 11. Conformance tests

Each test is run against the host runtime with the conformance guest
(`cpp/tests/guests/conformance.cpp`) and the samples.

| ID | Test |
|---|---|
| C01 | A module without `paglets_abi_v1` is rejected at load time |
| C02 | A module declaring only unknown ABI versions is rejected |
| C03 | A module importing a `paglets` function the host does not know is rejected, naming the import |
| C04 | A roaming module importing from `paglets_sys` is rejected |
| C05 | A module missing a required export is rejected |
| C06 | `created` is delivered once, before the first message, with `args` and `caps` |
| C07 | Messages are delivered by priority, FIFO within a priority |
| C08 | A request is answered exactly once; the requester sees the reply payload and the correlation ID |
| C09 | A handler returning 1 for a request produces status `unknown_message` |
| C10 | A handler returning a negative code for a request produces that status |
| C11 | A reply from a later handler (deferred reply) reaches the requester |
| C12 | A request without a reply within `timeout_ms` produces status `timeout`; a later reply is discarded |
| C13 | A dropped reply capability produces status `gone` |
| C14 | A trap fails the paglet; its pending requests get status `failed`; other paglets continue |
| C15 | A handler exceeding the CPU time budget is terminated and the paglet fails |
| C16 | `send` to an endpoint without the operation returns `denied` |
| C17 | `send` through an expired endpoint returns `expired`; a use-limited endpoint returns `quota` after its last use |
| C18 | `cap_derive` can only shrink rights; attempts to widen them return `denied` |
| C19 | Transferring a non-transferable capability returns `denied`; a transferred capability is gone from the sender |
| C20 | A child gets exactly the capabilities passed to it and no endpoint to its creator |
| C21 | `cap_inspect` and `cap_list` describe the table; invalid handles return `bad_handle` |
| C22 | `self_info` reports ID, module hash, trust class and ABI version 1 |
| C23 | A timer delivers its message after the delay; a dropped timer does not fire |
| C24 | `deactivate` snapshots the paglet; the next message activates it with its state intact and `activated` delivered |
| C25 | `clone` creates a paglet with a copy of the state that receives `cloned` |
| C26 | `dispose` delivers `disposing` and removes the paglet; later sends return `not_found` |
| C27 | Out-of-bounds `(ptr, len)` arguments to imports trap the paglet instead of reading host memory |
| C28 | Documents larger than the limit return `too_large`; malformed documents return `malformed` |
| C29 | Output buffers that are too small return the required length and are not written |
| C30 | Reserved message names (`paglets.*`) are refused in `send` with `invalid_argument` |
| C31 | v1.1: new paglets get the service endpoints of system paglets, listed in `self_info.services`; children too |
| C32 | v1.1: lent capabilities reach the system paglet and stay with the sender; lending elsewhere fails; use limits count |
| C33 | v1.1: derive paths narrow `dir` capabilities; invalid paths and paths on other kinds fail |
| C34 | v1.1: revoking a capability or its grant revokes every descendant; parents and siblings stay valid |
| C35 | v1.1: resource capabilities, service endpoints and revocations survive a restart |
| C36 | v1.1: asynchronous `send` and `reply` return 0 at once; failures arrive as `paglets.undelivered` with the error, transferred capabilities are released, a failed reply answers `gone`; `request` refuses `async` |

## 12. Review

Reviewed against the implementation after milestone M1's runtime, worker
processes and conformance tests were complete. Changes from the review:

- Traps never resume from a checkpoint (the plan's rule, section 3.4); only
  paglets of an ended worker process do. The earlier text said otherwise.
- The delivery of events was described as going through the mailbox; only
  `created` and `cloned` do, the others belong to their transition.
- Negative results of every event are now logged by the host, as the
  section says (`activated`, `deactivating` and `disposing` were not).
- Rules the implementation had without the text saying so: handle 1 cannot
  be dropped and is copied on transfer; replies carry no sender record and
  arrive at priority 7; request timeouts must be positive and timer delays
  not negative; children and clones are roaming; the send errors for ended
  targets and full mailboxes.
- The state migration exports are reserved until code upgrades exist.

Open for later ABI versions (compatible additions): capability kinds for
files, artifacts, topics and pins (WP9, WP10, M3), `dispatch` (M3), and the
privileged `paglets_sys` module (WP10).

## 13. Additions in v1.1

Compatible additions for system paglets (WP10) and grants (WP9); a v1.0
guest keeps working, and `self_info.minor` is 1 on hosts that have them.

- **Resource capabilities.** System paglets hand out capabilities of kinds
  `dir`, `file`, `artifact` and `topic` (planning/cpp-system-paglets.md).
  Their `ops` are rights (`read`, `write`, `create`, `delete`, `list`,
  ...). `cap_inspect` reports the kind and the target `<service>:<resource>`,
  for example `files:shared-data/projects`.
- **Lending.** The outgoing message document takes `lend` (list of handles):
  capabilities shown to the receiving system paglet for this one message.
  The sender keeps them; each lend counts one use of a capability with a use
  limit. Lending to a paglet that is not a system paglet, and lending in a
  reply, fail with `invalid_argument`; a revoked, expired or used-up
  capability fails with `revoked`, `expired` or `quota`.
- **Derive paths.** `DeriveSpec.path` narrows a `dir` capability to a
  relative path below it (segments separated by `/`, none empty, `.`, `..`,
  or containing `\` or `:`); other kinds fail with `invalid_argument`.
  Rights shrink as for endpoint operations.
- **Service endpoints.** Every new paglet receives non-transferable
  endpoints to the system services that offer one (section 7.5 of the
  security design); `self_info.services` maps service names to their
  handles. Clones keep the handles of their original.
- **Revocation.** Every endpoint and resource capability has an ID in the
  host; derived capabilities descend from theirs. Revoking a capability or
  the grant it came from makes it and every descendant fail with `revoked`
  wherever they are; `cap_inspect` reports `revoked: true`.
- **Endpoint targets.** `cap_inspect` reports endpoints to system paglets as
  `service:<name>`.
- **Asynchronous send and reply.** The outgoing message document takes
  `async` (bool, default false) in `send` and `reply`; `request` refuses it
  with `invalid_argument`. An asynchronous call returns 0 once the document
  is well-formed and does not wait for delivery, which lets a worker process
  hand the message to the host without a round trip. If the message cannot
  be delivered, the host sends the paglet itself a message
  `paglets.undelivered` (kind 0, priority 7, no sender record) with the
  payload `{name: str, handle: int, status: int}`: the name of the
  undelivered message, the endpoint or reply capability it was sent with,
  and the error the synchronous call would have returned. What the paglet
  gave up is released as after a successful call: transferred capabilities
  are dropped, and a failed reply consumes the reply capability, so the
  requester is answered `gone`.
