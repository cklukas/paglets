# Guest SDK reference

The guest SDK is one header, `<paglets/paglet.hpp>` (`cpp/sdk/include/`),
and one source file, `cpp/sdk/src/paglet.cpp`, that `paglets_add_module`
compiles into every module. An installed package has them in
`include/paglets/` and `share/paglets/sdk/` (see
[Modules outside the repository](building.md#modules-outside-the-repository)). Everything is in the namespace `paglets`. The
types of the binary interface (`paglets::abi::...`) come from
`cpp/common/include/paglets/abi.hpp`, which the header includes.

This page lists the SDK's classes and functions with their signatures. The
[ABI specification](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-abi-v1.md)
describes the binary interface underneath: imports, exports, documents and
conformance tests. [Writing paglets](writing-paglets.md) is the
introduction.

## WASI subset

The standard import policy permits the following WASI functions, in
addition to the paglet ABI imports. The list is generated from the host
source; matching a name never authorizes a non-function import.

<!-- snippet: cpp/host/src/wasm_binary.cpp#wasi_allowlist -->
Source: [`cpp/host/src/wasm_binary.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/host/src/wasm_binary.cpp)

```cpp
std::vector<std::string> allowed = {
    "wasi_snapshot_preview1.args_get",
    "wasi_snapshot_preview1.args_sizes_get",
    "wasi_snapshot_preview1.environ_get",
    "wasi_snapshot_preview1.environ_sizes_get",
    "wasi_snapshot_preview1.clock_res_get",
    "wasi_snapshot_preview1.clock_time_get",
    "wasi_snapshot_preview1.random_get",
    "wasi_snapshot_preview1.sched_yield",
    "wasi_snapshot_preview1.proc_exit",
    "wasi_snapshot_preview1.fd_write",
    "wasi_snapshot_preview1.fd_close",
    "wasi_snapshot_preview1.fd_seek",
    "wasi_snapshot_preview1.fd_fdstat_get",
    "wasi_snapshot_preview1.fd_prestat_get",
    "wasi_snapshot_preview1.fd_prestat_dir_name",
};
```
<!-- /snippet -->

Arguments and environment are empty. No files are preopened. Writes to
stdout and stderr become host log lines; writes to other descriptors fail.
Paglets use capability-based system services for file access
and use compute jobs for long work. `proc_exit` fails the paglet.

## Results and encoding

```cpp
using Bytes = std::vector<std::uint8_t>;

template <class T>
using Result = std::expected<T, std::int32_t>;

template <class T> Bytes encode(const T& value);
template <class T> bool decode(std::span<const std::uint8_t> bytes, T& value);
```

- A `Result` holds a value, or on failure one of the
  [error codes](configuration.md#error-codes) (`paglets::abi::Error`, a
  negative number). `paglets::abi::error_name(code)` gives its name.
- `encode` and `decode` convert between C++ values and MessagePack. They
  handle `bool`, signed and unsigned integers, `float`, `double`,
  `std::string`, `Bytes` (as MessagePack binary), `std::vector<T>`,
  `std::optional<T>` (nil when empty), and every struct and enum of a
  schema namespace (see [your own contracts](#defining-your-own-typed-service-contract)).
  `decode` fails unless the whole input is one valid value. Empty start
  arguments therefore make `Started::decode_args` return false quietly.

## The paglet class

```cpp
class Paglet {
public:
    virtual ~Paglet() = default;

    virtual void on_created(Started&) {}
    virtual void on_cloned(Started&) {}
    virtual void on_activated() {}
    virtual void on_deactivating() {}
    virtual void on_arrived(const abi::ArrivedEvent&) {}
    virtual void on_dispatching(const abi::DispatchingEvent&) {}
    virtual void on_disposing() {}
    virtual void on_undelivered(const abi::Undelivered& u);
    virtual void on_move_failed(const abi::MoveFailed& f);
    virtual void on_recovered(const abi::Recovered&) {}

    virtual std::int32_t on_message(Message& m) { return router_.dispatch(m); }

    Router& router();
};

#define PAGLETS_PAGLET(Type)
```

A module defines exactly one class derived from `Paglet` and registers it
with `PAGLETS_PAGLET(Type)` at namespace scope. The SDK keeps one static
instance of it. The object and everything it owns live in the module's
linear memory, so memory images save and move it as it is. The constructor
is part of module initialization; register message handlers there. On a
restore the engine instantiates the same module, then overwrites its
memory and mutable globals with the image. WAMR runs a WASI reactor's
`_initialize` during instantiation even on restore; without WASI imports,
the host calls it only at creation. Initializer memory effects are replaced
by the saved C++ object and registered handlers. Do not log,
send, set timers or otherwise call host imports in static constructors: the host attaches the imports after
instantiation. Use `on_created` or the appropriate lifecycle hook.

### Lifecycle hooks

| Hook | Called |
|---|---|
| `on_created(Started&)` | once, in a new paglet, before any message |
| `on_cloned(Started&)` | in a clone, instead of `on_created` |
| `on_activated()` | after restoring an inactive paglet, before the message, timer, timed wake, report or host-side dispose/dispatch that woke it; also on the old host before `on_move_failed` after a failed move |
| `on_deactivating()` | after the handler that called `deactivate`, before the image is taken |
| `on_dispatching(const abi::DispatchingEvent&)` | after the handler that called `dispatch`, before the move |
| `on_arrived(const abi::ArrivedEvent&)` | on the new host, after a move |
| `on_disposing()` | after the handler that called `dispose`, before the paglet ends |
| `on_undelivered(const abi::Undelivered&)` | an asynchronous send or reply could not be delivered. The default logs a warning. |
| `on_move_failed(const abi::MoveFailed&)` | a dispatch, or a clone for another host, did not happen. After a failed dispatch the paglet continues on its host. The default logs a warning. |
| `on_recovered(const abi::Recovered&)` | the host restarted and the paglet came back from its last image, as its first message, after `on_activated` and before request `timeout` and job `gone` replies; only after `set_restart(true, true)` (ABI v1.2). `image_ms` says when that image was stored, `restarted_ms` when the host came back. The default does nothing. |
| `on_message(Message&)` | every message, except replies with a continuation and the reports above. The default passes it to the router. |

A WebAssembly trap or budget expiry in `on_dispatching`, `on_deactivating`
or `on_disposing` ends the paglet as failed. Operator termination during a
hook also ends it before an image can be published. A failed capture or a
refused transport after a successful dispatch hook instead reports
`on_move_failed` and retains the paglet.

End hooks may send, reply, log, inspect or release capabilities, and change
restart policy. They cannot request a checkpoint or another lifecycle transition,
clone, create a child or submit a compute job: these return `bad_state`.

The documents of the events:

```cpp
struct Started {
    Bytes args;                           // the arguments of the creator or cloner
    std::vector<Capability> caps;         // capabilities passed along
    std::optional<std::string> original;  // set for clones: the original's ID
    template <class T> bool decode_args(T& out) const;
};

namespace abi {
struct ArrivedEvent { std::string from; std::vector<std::string> lost; };
struct DispatchingEvent { std::string destination; };
struct Undelivered { std::string name; std::int32_t handle = 0; std::int32_t status = 0; };
struct MoveFailed { std::string destination; std::string reason; std::string clone; };
}
```

- `ArrivedEvent::from` names the previous host; `lost` lists the
  capabilities that could not be re-created on the new host.
- `Undelivered` names the message, the endpoint (for a send) or reply
  capability (for a reply), and the error the call would have returned.
  Its handle identifies the failed call; a consumed reply handle is already
  invalid and cannot be used to answer again.
- `MoveFailed::clone` is the clone's ID for a failed clone, and empty for a
  failed dispatch.

A paglet never sees two calls at once: hooks and handlers run one after
the other. Lifecycle hooks cannot veto a transition by returning a status;
a trap or budget expiry can fail the paglet instead. Created, cloned,
activated, arrived and the three end hooks are ABI events. Undelivered,
move-failed and recovered notifications are host messages at priority 7,
recognized by the SDK before ordinary routing. The `paglets.` message
prefix is reserved for the host; public sends and module descriptions
cannot claim it.

The handler watchdog is off by default. An enabled budget measures elapsed
host time per invocation, including imports and worker IPC. It does not limit the paglet's lifetime
or time spent waiting for background operations. A trap or exhausted budget
fails the paglet. Use `tasks::TaskHandle` for progress/status and cancellation
of service work or `compute::Job::task()` for the same controls over CPU work;
see [Long-running agents and tasks](long-running-tasks.md). Each continuation
runs as a fresh handler with its own budget.

`Started::caps` preserves the order supplied by the creator. Capability
roles, such as a parent endpoint in slot zero, belong to the application's
protocol; the runtime does not add a parent endpoint automatically.
Use `inspect()` when a capability's kind needs checking.

## Messages

```cpp
class Message {
public:
    abi::MessageKind kind() const;          // message, request, reply or timer
    bool is_request() const;
    bool is_reply() const;
    const std::string& name() const;
    std::span<const std::uint8_t> payload() const;
    std::int32_t priority() const;
    const std::optional<abi::SenderRecord>& sender() const;
    const std::optional<std::string>& badge() const;

    std::int32_t status() const;            // replies: 0, or an error code
    bool ok() const;                        // status() == 0
    std::uint64_t correlation() const;

    template <class T> bool decode(T& out) const;

    std::size_t cap_count() const;
    Capability take_cap(std::size_t index);

    Result<void> reply_raw(Bytes payload, std::vector<Capability> caps = {}, bool async = false);
    Result<void> reply();
    template <class T> Result<void> reply(const T& body, std::vector<Capability> caps = {});
    template <class T> Result<void> reply_async(const T& body, std::vector<Capability> caps = {});
    bool replied() const;

    Capability defer_reply();
    bool reply_pending() const;
    std::int32_t reply_handle() const;
};

Result<void> reply_to(Capability& reply, Bytes payload, std::vector<Capability> caps = {}, bool async = false);
template <class T> Result<void> reply_to(Capability& reply, const T& body, std::vector<Capability> caps = {});
```

- `sender()` is the host's record of the sending paglet: `id`, `owner`,
  `trust`, `module` and `host`. Replies and reports from the host have
  none. `badge()` is the badge of the endpoint the message came through
  (see `DeriveSpec`).
  On a mesh, `host` is the mobility host's key ID, as in `self_info().host`;
  without mobility both use the embedder's configured host name.
  Calls from the embedder or host tools use a synthetic sender: `id: host`,
  `trust: host`, empty `module`, and the executing host's identity. For an
  authenticated remote call or send, `owner` is the acting owner's or
  admin's key ID, taken from the authenticated channel. Message fields
  cannot override it. Embedded runtime calls default to `owner: local`;
  trusted embedders can supply `sender_owner` to `send`, `request` or `call`.
  Native services keep `id: system.NAME` and `trust: system`.
- `take_cap(i)` moves the `i`-th capability that came with the message out
  of it. A bad index or a second take returns an empty capability. Capabilities
  that are not taken stay in the paglet's table.
- A request is answered once: with `reply`, or later with `reply_to` on the
  capability that `defer_reply` returned. `reply_async` and `async = true`
  return at once. If the reply cannot be delivered, `on_undelivered` runs
  later.
- `defer_reply()` returns an empty capability when the message is not an
  unanswered, undeferred request. Check the returned wrapper before retaining
  it; unlike `reply_raw`, this method does not return an error code.
- A request that a handler handled, but neither answered nor deferred, is
  answered `gone`.
- Returning `not_handled` for a request makes the host answer
  `unknown_message`. For a one-way message or timer, the host logs it at
  debug level; there is no reply.

## Routing

```cpp
class Router {
public:
    template <class F> void on(std::string name, F&& f);           // void(Message&) or int32_t(Message&)
    template <class T, class F> void on(std::string name, F&& f);  // void(const T&, Message&) or int32_t(const T&, Message&)
    template <class F> void otherwise(F&& f);                      // names without a handler
    std::int32_t dispatch(Message& m) const;
};
```

- The typed form decodes the payload into a `T` first. A body that does
  not decode is answered `malformed`.
- A handler that returns `void` counts as handled. A handler that returns
  `std::int32_t` returns `abi::handled` (0), `abi::not_handled` (1), or a
  negative error code.
- For a request, `not_handled` makes the host answer `unknown_message`, and
  a negative result makes it answer with that error. Messages without a
  handler and without `otherwise` are not handled. If a one-way message or
  timer returns `not_handled`, the runtime logs its kind and name at debug
  level; requests still receive `unknown_message`.
- Timer messages arrive through the router too, under the name given to
  `after`.

## Capabilities

```cpp
class Capability {
public:
    Capability() = default;
    explicit Capability(std::int32_t handle);
    std::int32_t handle() const;
    explicit operator bool() const;                          // a valid handle
    Result<abi::CapInfo> inspect() const;
    Result<Capability> derive(const abi::DeriveSpec& spec) const;
    Result<void> drop();
    std::int32_t release();                                  // forget the handle without dropping it
};

Result<std::vector<Capability>> list_caps();
```

A capability is a handle into the paglet's host-side table. The paglet
cannot forge one: the host checks every use.

Copies of `Capability` and `Endpoint` alias one host-side handle. Dropping
or transferring that handle invalidates its aliases; their integer values
may still be positive. `release()` only clears that C++ wrapper and returns
the integer: it does not drop the host capability or invalidate other copies.
Handle 1 is copied rather than consumed when transferred.

- `inspect` returns `abi::CapInfo`: `kind`, `target`, `ops`,
  `transferable`, and optionally `badge`, `expires` (Unix milliseconds),
  `uses_left`, and whether it was `revoked`.
- `derive` makes a weaker copy. `abi::DeriveSpec` can narrow `ops`, set
  `expires_ms`, `uses`, `transferable` and a `badge`, and for `dir`
  capabilities a `path` below the directory. Rights only shrink. A granted
  directory that is recreated after a move keeps its narrowed path, rights,
  expiry, remaining uses, badge, identity and revocation ancestry. A badge
  can be set only when the parent has none; an existing badge cannot be
  replaced. Each host
  applies the revocations it knows; local capability-ID revocations are not
  automatically replicated. If the resource lies outside the grant, arrival
  reports it lost.
- `drop` releases a capability. Dropping a timer cancels it; dropping a
  reply capability answers the request `gone`. Handle 1 (the paglet's own
  endpoint) cannot be dropped.
- Passing a capability in a message, reply, child or clone transfers it:
  the sender's handle becomes invalid when the call succeeds. Pass it in
  `SendOptions::lend` instead to show it to a system paglet for one message
  only. An endpoint can be used and transferred by the same send or
  request; the receiver gets its remaining uses after that call. A reply
  cannot both consume and transfer its own reply capability: the call
  returns `invalid_argument` before transfer. An asynchronous rejection
  follows the usual `on_undelivered` and failed-reply rules.

### Capabilities across moves

`Runtime::prepare_arrival` and `Node::Impl::recreate` apply these rules to
an existing paglet's dispatch. Deactivation on the same host retains its
capability table, subject to normal expiry and revocation.

| Capability | On dispatch to another host | Can be reported lost |
|---|---|---|
| Endpoint to a paglet | retained; location lookup follows the target | no |
| Host's default service endpoint | rebound to that service on the destination | yes, if the service/default operations are absent |
| Granted system endpoint, directory or other resource | recreated only if the active grant selects the destination and covers the narrowed resource and rights | yes |
| Pin | retained through the destination's locator | yes, if the locator is absent |
| Mesh artifact | retained with the content host and access token | no |
| Reply | retained with its request correlation | no |
| Timer | retained and rescheduled; expiry in transit is delivered after arrival | no |
| Other host-local resource or ungranted nondefault system endpoint | cannot be recreated | yes |

The destination removes lost handles; their integer values in guest memory
are not rewritten. `ArrivedEvent::lost` describes each as `handle N: KIND`
with the target/resource when applicable, for example
`handle 12: endpoint system.server-info`. Ask for the destination's
service or a new grant as needed. A storage directory is separate from
these capabilities: its data stays on the source host and is removed when
a move commits; a clone starts with a new, empty storage directory. Carry
small state in memory and use file/artifact sharing for portable content.

## Endpoints

```cpp
struct SendOptions {
    std::int32_t priority = abi::default_priority;  // 3; 0 (lowest) to 7 (highest)
    std::vector<Capability> caps;                   // transferred
    std::vector<Capability> lend;                   // shown to a system paglet for this message
    bool async = false;                             // return at once; failures reach on_undelivered
};

struct RequestOptions {
    std::int32_t priority = abi::default_priority;
    std::vector<Capability> caps;
    std::int64_t timeout_ms = abi::default_timeout_ms;  // 30000
    std::vector<Capability> lend;
};

using ReplyHandler = std::function<void(Message& reply)>;

class Endpoint : public Capability {
public:
    Endpoint() = default;
    explicit Endpoint(std::int32_t handle);
    explicit Endpoint(const Capability& c);

    Result<void> send_raw(std::string_view name, Bytes payload, SendOptions options = {}) const;
    Result<void> send(std::string_view name, SendOptions options = {}) const;
    template <class T> Result<void> send(std::string_view name, const T& body, SendOptions options = {}) const;

    Result<std::uint64_t> request_raw(std::string_view name, Bytes payload, ReplyHandler on_reply,
                                      RequestOptions options = {}) const;
    template <class T> Result<std::uint64_t> request(std::string_view name, const T& body, ReplyHandler on_reply,
                                                     RequestOptions options = {}) const;
    Result<std::uint64_t> request(std::string_view name, ReplyHandler on_reply, RequestOptions options = {}) const;
};

Endpoint self();
Result<Endpoint> service(std::string_view name);
```

- `send` delivers a one-way message. For a local target, without `async`,
  it fails at once with `denied`, `expired` or `quota` when the endpoint does not allow the
  message, `not_found` when the target ended, and `quota` when the
  target's mailbox is full.
  With `async`, local failures and receiving-host refusals reach
  `on_undelivered` with the original message name, endpoint handle and error.
  Remote refusals include a missing target (after location lookup), a full
  mailbox, a remote system target and an exhausted forwarding bound. The
  report follows a moving sender; transferred capabilities stay consumed.
  The receiving host logs refusals at warning level for both `async` values.
- `request` returns the correlation ID. `on_reply` runs in a later handler
  with the reply: `ok()` for a real reply, otherwise `status()` is
  `timeout`, `unknown_message`, `failed` (the receiver trapped or exceeded
  its handler budget), `gone`, or the error the receiver's handler returned.
  Remote routing can also answer `not_found` after location lookup,
  `quota` for a full target mailbox, `denied` for a remote system target,
  `invalid_argument` for an invalid name, or `unsupported` for capabilities
  that cannot travel. Typed handlers can return `malformed` for bad input. The continuation lives in
  the paglet's memory, so it survives deactivation and moves.
  A queued request that expires is refused before its handler starts;
  forwarding and location lookup consume the original budget. Hosts use
  monotonic clocks for local queues and an absolute deadline in transit,
  so synchronize host wall clocks. A timeout does not cancel a handler
  that already started.
  Request submission has no `async` flag because it returns a correlation ID
  allocated by the host; the answer remains asynchronous.
- `self()` is the paglet's own endpoint, handle 1.
- `service(name)` returns the host's default endpoint to a system service,
  for example `"files"` or `"mesh-info"`, or `not_found`.
  Each lookup reads and decodes the self document through host imports;
  it is not a cached local map. Reuse a lookup within a handler rather than
  repeatedly querying it in a tight loop.

Across hosts, messages and replies can transfer endpoints to paglets and
artifact capabilities. Other capabilities return `unsupported`; lending
across hosts returns `invalid_argument`. A local request held during a move
is refused with `unsupported` if its capabilities cannot travel, instead
of being delivered with capabilities missing. Remote sends return after
submission to the sender's host; `async` additionally asks receiving hosts
for refusal reports. This is best effort: a lost connection, an old peer
without report support or an ended sender can prevent a report. A successful
submission is not an acknowledgement that a handler ran. Replies and
refusal reports follow location lookup if a movement hint has expired.

## Lifecycle, children and timers

```cpp
Result<Endpoint> create_child_raw(Bytes args, std::vector<Capability> caps = {},
                                  std::optional<std::string> module = std::nullopt);
template <class T> Result<Endpoint> create_child(const T& args, std::vector<Capability> caps = {},
                                                 std::optional<std::string> module = std::nullopt);

Result<void> dispose();
Result<void> deactivate(std::optional<std::int64_t> wake_after_ms = std::nullopt);
Result<Endpoint> clone_raw(Bytes args = {}, std::vector<Capability> caps = {},
                           std::optional<std::string> destination = std::nullopt);
Result<void> dispatch(std::string destination);
Result<void> checkpoint();                                              // ABI v1.2
Result<void> set_restart(bool restart, bool notify_recovered = false);  // ABI v1.2

Result<Capability> after_raw(std::int64_t delay_ms, std::string_view name, Bytes payload = {},
                             std::int32_t priority = abi::default_priority);
template <class T> Result<Capability> after(std::int64_t delay_ms, std::string_view name, const T& body,
                                            std::int32_t priority = abi::default_priority);
```

- `create_child` creates a paglet from `module` (a hex module hash; the
  default is this paglet's module). That module must already be stored on
  this host. The child gets the capabilities in `caps` in their supplied
  order, plus its own endpoint and the host's default service endpoints;
  its `on_created` receives `args`. Children and clones share the per-handler
  spawn limit, 16 by default.
- `dispose`, `deactivate`, `clone_raw` and `dispatch` take effect when the
  current handler returns. Once dispatch is accepted, pins and competing
  `dispose`, `deactivate` and `dispatch` requests are refused with
  `bad_state` until the move decision.
  Host controls are also refused while an arrival is prepared or an end
  hook is running. Retry after the transition.
- `deactivate` stores the paglet's memory image and unloads it. The next
  message activates it again, or `wake_after_ms` does. The automatic wake
  travels with a move and survives restart when the image is stored and
  restart is enabled. It is queued at priority 0 and delivers no message:
  restoring the image invokes `on_activated` only. An earlier message or
  timer consumes that wake, so it does nothing if the paglet is already
  active. With restart
  disabled, an inactive paglet's image stays in host memory and is lost
  when the host exits.
- `clone_raw` makes a copy of the paglet, which receives `on_cloned`. With
  a `destination` (a transfer ticket, as for `dispatch`), the clone is made
  on another host. Passed capabilities must support transfer across hosts.
  Pending SDK reply callbacks are discarded in the clone before
  `on_cloned`; suspended `Async` request/job frames are destroyed there.
  Their operations and continuations remain owned by the original paglet.
- `dispatch` moves the paglet with a transfer ticket: a host name or key
  ID, `label:<label>`, `offer:<service>[.<op>]` or `any`, optionally with
  `?retries=N&arrival=active|inactive`. While the paglet is pinned, it
  fails with `pinned`. When the move fails later, `on_move_failed` runs on
  the old host. Malformed ticket parameters return `invalid_argument`
  before dispatch or cloning is accepted; `retries` is 1 to 16.
  There is no retract operation. Return by dispatching to the starting
  host, or let the owner or an admin use `remote dispatch PAGLET HOST`.
  With `arrival=inactive`, the destination holds the image unloaded;
  `on_arrived` runs before the first delivery that wakes it, using the
  capability-loss list recorded at arrival.
  Replies already accepted by the host, including job results and request
  timeouts, travel with their continuations and portable capabilities;
  queued host reports travel too. `on_arrived` runs before these deliveries.
  If this serialized state exceeds the configured notification budget or
  transport frame limit, the move is refused and deliveries remain on the
  source host. Queued notifications require travel-state version 2;
  finite unresolved requests require version 3; unlimited job continuations require version 4. Older peers refuse moves requiring
  a version they cannot read. Outstanding request timeouts count time spent
  moving and waiting for commit; a late reply stays late after arrival.
  A queued reply containing capabilities that cannot travel becomes
  `unsupported`, with no payload or capabilities, on a successful move.

- `checkpoint` schedules an image after the current handler, before its
  next delivery, on a host with a state directory and restart enabled.
  It returns `unsupported` without storage and `bad_state` with restart
  disabled or a lifecycle transition already pending or running. Acceptance
  is not a synchronous durability barrier: it does not protect an effect in
  the same handler. A later lifecycle request can supersede the checkpoint.
  If the explicit checkpoint cannot be captured or written, the paglet ends
  as failed before its next delivery. Automatic checkpoints retry on later
  handlers after write failure; their default interval is 1 s. A paglet
  without a stored image is automatically due for a checkpoint after its
  first completed handler, subject to the same storage and restart rules.
  A stored record includes the image, capability table, timers and timed
  wake, restart policy and pending request/job IDs. After restart, pending
  requests receive `timeout` and pending jobs `gone`, after `on_recovered`
  when enabled. The ordinary mailbox is not stored or replayed. Outgoing
  effects are not rolled back, so work resumed from an earlier image can
  repeat them; use application-level deduplication where needed.
- `set_restart` sets the restart policy and marks a checkpoint due after
  the current handler: enabling restart stores an image, while disabling
  it removes an older stored record through the normal persistence path.
  This is not a synchronous barrier and a later lifecycle operation may
  supersede it. By default every paglet on a host
  with a state directory comes back from its last image after the host
  restarts. With `restart` false the paglet is never stored (an image stored
  earlier is removed) and ends with its host process. With
  `notify_recovered`, a paglet that came back receives `on_recovered` first,
  to check what changed while it was down. Clones inherit the policy, and it
  travels with the paglet. `self_info()` reports it (`restart`,
  `notify_recovered`).
- `after` delivers a message named `name` to the paglet itself after
  `delay_ms`. Drop the returned capability to cancel it. Timers survive
  deactivation: a deactivated paglet is activated when its timer fires.
  Moves preserve one-shot delivery with `MessageKind::timer`: a timer
  expiring in transit fires on the destination after arrival, or on the
  source if the move fails. Already queued timers travel with the image.
  Negative delays are refused. Extremely large delays are capped at the
  scheduler clock's representable horizon (one quarter of its maximum duration).

Immediate failures are returned by the SDK operation before acceptance:

| Operation | Errors |
|---|---|
| `dispatch` | `invalid_argument` for an empty or malformed ticket; `unsupported` without mesh movement; `denied` for a resident or system paglet; `bad_state` during an end hook or a competing end operation/move; `pinned` while pinned; `too_large` for an ABI argument document over 1 MB |
| `clone_raw` | `quota` for spawn, capability or local-owner limits; `invalid_argument` for a malformed destination, duplicate capability handles or capabilities that cannot travel to the destination; `unsupported` for a remote clone without mesh movement; `denied` for remote cloning by a non-roaming paglet, module-policy refusal or a non-transferable capability; `bad_handle` for an unknown passed handle; `bad_state` during an end hook; `too_large` for an ABI argument document over 1 MB; `internal` if local clone record creation fails |

The raw clone ABI also returns `malformed` for an invalid encoded argument
document; the typed SDK constructs that document itself. Acceptance does
not mean the move or clone has completed. Later transfer failures arrive
through `on_move_failed`.

After a handler returns normally, the runtime captures and prepares its accepted
clones first. It then performs the original paglet's one pending end
operation: `on_disposing`, `on_dispatching` or `on_deactivating` runs before
that operation completes, and before an image is captured when one is
needed. If there is no such end operation, a due checkpoint may run.
`terminate` schedules no lifecycle hook.

An unloaded, already started source is activated before a later
`on_move_failed` delivery; a failure that leaves its instance loaded needs
no activation. Initial creation, cloning and arrival receive their start
hook without `on_activated`. With `arrival=inactive`, `on_arrived` precedes
the first waking delivery. An automatic `wake_after_ms` wake invokes
`on_activated` without a message callback. Disposing an already started
inactive paglet also activates its image before its disposal hook.

## Host information, logging, clocks

```cpp
Result<abi::SelfInfo> self_info();

void log(abi::LogLevel level, std::string_view text);  // debug, info, warning, error
void log(std::string_view text);                       // info

std::int64_t now_ns(abi::Clock clock = abi::Clock::wall);  // wall or monotonic
void random_bytes(std::span<std::uint8_t> out);
```

`abi::SelfInfo` has the paglet's `id`, `module`, `owner`, `trust` and
`host`, the ABI version `abi` and the host's minor version `minor`, the
default service endpoints `services` (name and handle), and `pinned_until`
(Unix milliseconds; 0 when the paglet is not pinned).

`log` writes to the host log. So does anything the paglet writes to stdout
or stderr; those streams are forwarded in lines capped at 4096 bytes.
The wall clock is Unix time. The monotonic clock belongs to the current
host: compare it only within a stay on that host, never across a move or
between different hosts.

## Defining your own typed service contract

A **contract** names a service's operations with their request and reply
types. The system services are contracts in
`cpp/common/include/paglets/services/`. You can write your own in the same
form, and serve and call it from guests with generated code. The test
guest `calc_user` (`cpp/tests/guests/`) does both.

### 1. The schema header

Put the types and the contract in a namespace of their own, in a header
that both sides include (`calc_contract.hpp`):

```cpp
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace calc {

struct AddRequest { std::int64_t a = 0; std::int64_t b = 0; };
struct AddReply { std::int64_t sum = 0; };
struct DivRequest { std::int64_t a = 0; std::int64_t b = 0; };
struct DivReply { std::int64_t quotient = 0; std::string served_by; };

struct Contract {
    static constexpr std::string_view service = "calc";
    AddReply add(const AddRequest&);
    DivReply divide(const DivRequest&);  // invalid_argument for b == 0
};

}  // namespace calc
```

The rules:

- Every struct and enum of the namespace is a wire type. Fields may have
  the types that `encode` handles: the standard types listed
  [above](#results-and-encoding), and other wire types of the namespace.
- A struct is encoded as a MessagePack map with the field names as keys,
  and an enum as the name of its enumerator. Decoders skip keys they do
  not know, so adding a field later stays compatible.
- A contract is a struct with a `static constexpr std::string_view service`.
  Each member function is one operation. It takes exactly one parameter,
  the request type, and returns the reply type. Declare the functions;
  they are never defined.
- A namespace may hold several contracts.

### 2. The build

Name the header and its namespace in CMake:

```cmake
paglets_add_module(calc_user
    SOURCES guests/calc_user.cpp
    SCHEMA_HEADER guests/calc_contract.hpp
    SCHEMA_NAMESPACE calc)
```

The guest compiler has no C++26 reflection, so the build compiles the
schema generator (`cpp/tools/schema_gen/`, or `share/paglets/schema_gen/`
of an installed package) with GCC 16 for this header and runs it. It writes:

- `calc_user.schema.gen.hpp`, included by the guest as
  `#include "calc_user.schema.gen.hpp"`: the encoders and decoders of every
  wire type, the schema descriptor `paglets_schema_json`, and for each
  contract a client and a `serve` function;
- `guests/calc_user.schema.json`: the same descriptor, for tools.

Without reflection on the host compiler, the build skips the module with a
warning.

### 3. What is generated

For a contract named `Contract` the generator emits `Client` and
`serve(router, impl)`. For any other name `Foo`, it emits `FooClient` and
`serve_foo(router, impl)`.

```cpp
struct Client {
    paglets::Endpoint endpoint;

    // one function per operation; on_reply(paglets::Result<Reply>, paglets::Message&)
    template <class F>
    paglets::Result<std::uint64_t> divide(const DivRequest& request, F&& on_reply,
                                          paglets::RequestOptions options = {}) const;
};

// Registers impl.<op>(const Request&, paglets::Message&) -> paglets::Result<Reply>
// for every operation, and `describe`, which answers the schema descriptor.
template <class Impl>
void serve(paglets::Router& router, Impl& impl);
```

The client's `on_reply` gets the decoded reply, or the error status
(`malformed` if the reply does not decode). `serve` decodes each request,
calls the implementation, and replies with its result or answers with its
error code. If the implementation already replied or deferred the reply,
`serve` does not reply again.

### 4. Serving and calling

```cpp
#include "calc_user.schema.gen.hpp"

#include <paglets/paglet.hpp>

class CalcUser : public paglets::Paglet {
public:
    CalcUser() {
        calc::serve(router(), *this);
    }

    paglets::Result<calc::AddReply> add(const calc::AddRequest& q, paglets::Message&) {
        return calc::AddReply{q.a + q.b};
    }

    paglets::Result<calc::DivReply> divide(const calc::DivRequest& q, paglets::Message&) {
        if (q.b == 0) return std::unexpected(paglets::abi::invalid_argument);
        return calc::DivReply{q.a / q.b, "guest"};
    }
};

PAGLETS_PAGLET(CalcUser)
```

Any paglet with an endpoint to this paglet can now call it with
`calc::Client{endpoint}.add(...)`. A paglet can hand out such endpoints
in messages. It can also publish itself under a name with the `directory`
service (`publish`, with the operations callers get), so that paglets of
the same owner, or everybody with `is_public`, find it with `lookup`.

The same header also serves on the host: a native system paglet derives
from `paglets::services::ContractPaglet<calc::Contract, Impl>`
(`cpp/host/include/paglets/services/contract.hpp`), and both sides answer
`describe` with the same descriptor. The `calc_user` guest calls such a
host service with `calc::Client{*paglets::service("calc")}`.

## Compute jobs and coroutines (ABI v1.3)

The separate `compute-slots` service issues cooperative slot leases. Renew
with the same job/core request, handle `compute.expired`, and request a new
lease after restart or arrival elsewhere. Its [lease rules](system-paglets.md)
include the default 10-minute duration and 24-hour continuous cap.

Include `paglets/compute.hpp` to register `PAGLETS_JOB` functions and submit
them with `compute::submit` or `submit_raw`. Submission returns
`Result<compute::Job>`; its continuation receives `Result<compute::Output>`
in a later handler. `Job::cancel()` stops work; `status()` reads progress.
A job handle is not a capability with automatic cancellation on destruction.

Include `paglets/async.hpp` for `paglets::Async` coroutines,
`co_await compute::run` and `co_await paglets::ask`. Each continuation or
coroutine resumption remains subject to the handler's elapsed-time budget.
See [Compute jobs](compute-jobs.md) for options, limits, lifecycle rules,
artifacts, recovery and AOT compilation.
