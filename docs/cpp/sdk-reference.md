# Guest SDK reference

The guest SDK is one header, `<paglets/paglet.hpp>` (`cpp/sdk/include/`),
and one source file, `cpp/sdk/src/paglet.cpp`, that `paglets_add_module`
compiles into every module. Everything is in the namespace `paglets`. The
types of the binary interface (`paglets::abi::...`) come from
`cpp/common/include/paglets/abi.hpp`, which the header includes.

This page lists the SDK's classes and functions with their signatures. The
[ABI specification](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-abi-v1.md)
describes the binary interface underneath: imports, exports, documents and
conformance tests. [Writing paglets](writing-paglets.md) is the
introduction.

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
  `decode` fails unless the whole input is one valid value.

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

    virtual std::int32_t on_message(Message& m) { return router_.dispatch(m); }

    Router& router();
};

#define PAGLETS_PAGLET(Type)
```

A module defines exactly one class derived from `Paglet` and registers it
with `PAGLETS_PAGLET(Type)` at namespace scope. The SDK keeps one static
instance of it. The object and everything it owns live in the module's
linear memory, so memory images save and move it as it is. The constructor
runs once, when the paglet is created; register message handlers there.

### Lifecycle hooks

| Hook | Called |
|---|---|
| `on_created(Started&)` | once, in a new paglet, before any message |
| `on_cloned(Started&)` | in a clone, instead of `on_created` |
| `on_activated()` | after the paglet was restored from its memory image, right before the delivery that woke it |
| `on_deactivating()` | after the handler that called `deactivate`, before the image is taken |
| `on_dispatching(const abi::DispatchingEvent&)` | after the handler that called `dispatch`, before the move |
| `on_arrived(const abi::ArrivedEvent&)` | on the new host, after a move |
| `on_disposing()` | after the handler that called `dispose`, before the paglet ends |
| `on_undelivered(const abi::Undelivered&)` | an asynchronous send or reply could not be delivered. The default logs a warning. |
| `on_move_failed(const abi::MoveFailed&)` | a dispatch, or a clone for another host, did not happen. After a failed dispatch the paglet continues on its host. The default logs a warning. |
| `on_message(Message&)` | every message, except replies with a continuation and the two reports above. The default passes it to the router. |

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
- `MoveFailed::clone` is the clone's ID for a failed clone, and empty for a
  failed dispatch.

A paglet never sees two calls at once: hooks and handlers run one after
the other.

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
- `take_cap(i)` moves the `i`-th capability that came with the message out
  of it. Capabilities that are not taken stay in the paglet's table.
- A request is answered once: with `reply`, or later with `reply_to` on the
  capability that `defer_reply` returned. `reply_async` and `async = true`
  return at once. If the reply cannot be delivered, `on_undelivered` runs
  later.
- A request that a handler handled, but neither answered nor deferred, is
  answered `gone`.

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
  handler and without `otherwise` are not handled.
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

- `inspect` returns `abi::CapInfo`: `kind`, `target`, `ops`,
  `transferable`, and optionally `badge`, `expires` (Unix milliseconds),
  `uses_left`, and whether it was `revoked`.
- `derive` makes a weaker copy. `abi::DeriveSpec` can narrow `ops`, set
  `expires_ms`, `uses`, `transferable` and a `badge`, and for `dir`
  capabilities a `path` below the directory. Rights only shrink.
- `drop` releases a capability. Dropping a timer cancels it; dropping a
  reply capability answers the request `gone`. Handle 1 (the paglet's own
  endpoint) cannot be dropped.
- Passing a capability in a message, reply, child or clone transfers it:
  the sender's handle becomes invalid when the call succeeds. Pass it in
  `SendOptions::lend` instead to show it to a system paglet for one message
  only.

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

- `send` delivers a one-way message. Without `async`, it fails at once
  with `denied`, `expired` or `quota` when the endpoint does not allow the
  message, `not_found` when the target ended, and `quota` when the
  target's mailbox is full.
- `request` returns the correlation ID. `on_reply` runs in a later handler
  with the reply: `ok()` for a real reply, otherwise `status()` is
  `timeout`, `unknown_message`, `failed` (the receiver trapped), `gone`,
  or the error the receiver's handler returned. The continuation lives in
  the paglet's memory, so it survives deactivation and moves.
- `self()` is the paglet's own endpoint, handle 1.
- `service(name)` returns the host's default endpoint to a system service,
  for example `"files"` or `"mesh-info"`, or `not_found`.

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

Result<Capability> after_raw(std::int64_t delay_ms, std::string_view name, Bytes payload = {},
                             std::int32_t priority = abi::default_priority);
template <class T> Result<Capability> after(std::int64_t delay_ms, std::string_view name, const T& body,
                                            std::int32_t priority = abi::default_priority);
```

- `create_child` creates a paglet from `module` (a hex module hash; the
  default is this paglet's module). The child gets exactly the
  capabilities in `caps`, and its `on_created` receives `args`.
- `dispose`, `deactivate`, `clone_raw` and `dispatch` take effect when the
  current handler returns.
- `deactivate` stores the paglet's memory image and unloads it. The next
  message activates it again, or `wake_after_ms` does.
- `clone_raw` makes a copy of the paglet, which receives `on_cloned`. With
  a `destination` (a transfer ticket, as for `dispatch`), the clone is made
  on another host, and only endpoints can be passed to it.
- `dispatch` moves the paglet with a transfer ticket: a host name or key
  ID, `label:<label>`, `offer:<service>[.<op>]` or `any`, optionally with
  `?retries=N&arrival=active|inactive`. While the paglet is pinned, it
  fails with `pinned`. When the move fails later, `on_move_failed` runs on
  the old host.
- `after` delivers a message named `name` to the paglet itself after
  `delay_ms`. Drop the returned capability to cancel it. Timers survive
  deactivation: a deactivated paglet is activated when its timer fires.

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
or stderr.

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
schema generator (`cpp/tools/schema_gen/`) with GCC 16 for this header
and runs it. It writes:

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
