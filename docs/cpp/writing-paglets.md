# Writing paglets

A paglet is a C++ class compiled to WebAssembly with the guest SDK
(`cpp/sdk/`). Its state is ordinary C++ objects. Memory images move it
between hosts without any serialization code.

## A first paglet

```cpp
#include <paglets/paglet.hpp>

class Hello : public paglets::Paglet {
public:
    Hello() {
        router().on<std::string>("greet", [this](const std::string& name, paglets::Message& m) {
            ++greetings_;
            m.reply("hello, " + name);
        });
    }

private:
    std::uint32_t greetings_ = 0;
};

PAGLETS_PAGLET(Hello)
```

Build it from CMake with `paglets_add_module`, then run it with the host:

```cmake
paglets_add_module(hello SOURCES hello.cpp)
```

Inside the repository, add the call to `cpp/examples/CMakeLists.txt`. In a
project of your own, install paglets/cpp and call
`find_package(paglets REQUIRED)` first; see
[Modules outside the repository](building.md#modules-outside-the-repository).

```bash
H=build/linux-gcc16/host/paglets-host
$H run build/linux-gcc16/guests/hello.wasm --call greet '"world"'
```

Message bodies and arguments on the command line are JSON. The host passes
them to the paglet as MessagePack and prints replies as JSON.

## Messages and schemas

Message types live in a header of their own. `schema_gen` generates their
encoders. The guest compiler has no reflection, so the host-side generator
writes the code. Name the header with `SCHEMA_HEADER` and its namespace
with `SCHEMA_NAMESPACE`:

```cmake
paglets_add_module(counter SOURCES counter/counter.cpp
    SCHEMA_HEADER counter/messages.hpp SCHEMA_NAMESPACE counter_msgs)
```

[`examples/counter`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/counter)
shows the whole setup.

## Capabilities

Paglets talk to other paglets only through capabilities:

- endpoints of paglets they created or were given;
- `request` with a reply continuation, and deferred replies;
- timers;
- clones;
- lifecycle operations: deactivate, dispose, and moves with transfer
  tickets.

Sends and replies can be asynchronous (`SendOptions::async`,
`Message::reply_async`). They return at once. A message that cannot be
delivered reaches `Paglet::on_undelivered`.

Lifecycle events arrive as virtual functions: `on_arrived`,
`on_move_failed`, `on_activated` and more. The
[guest SDK reference](sdk-reference.md) lists every class, function and
hook with its signature, and shows how to define a typed service contract
of your own.
[`examples/ping_pong`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/ping_pong)
shows them in use. The binary interface between host and guest is in the
[ABI specification](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-abi-v1.md).

## System services

Paglets reach host resources (files, processes, storage, the web, AI)
through system paglets. They call them with typed clients generated from
the contracts in `cpp/common/include/paglets/services/`. Name the services
a module uses with `SERVICES`:

```cmake
paglets_add_module(pi SOURCES pi/pi.cpp SERVICES mesh_info compute_slots)
```

```cpp
auto mi = paglets::service("mesh-info");
paglets::services::mesh_info::Client client{*mi};
client.landscape({}, [this](paglets::Result<paglets::services::mesh_info::Landscape> l, paglets::Message&) {
    // every host of the mesh, with its labels, load and offers
});
```

[System paglets](system-paglets.md) lists the services and their
operations. Access to resources follows the mesh policy: a paglet asks the
`grants` system paglet for a directory, and the policy rules decide
whether it gets one.

## The patterns library

The patterns library covers the common shapes of paglets. Include
`<paglets/patterns.hpp>` and build with `paglets_add_module(... PATTERNS)`.

| Header | Pattern |
|---|---|
| `services.hpp` | `endpoint(name, next)`, `lookup`, `lending(cap)`, `here(next)`: services by name, and this host as `mesh-info` sees it |
| `task.hpp` | `Task<Request, Result>`: a paglet that runs one task, with `start`, `status` and `wait`; `complete(result)` and `fail(why)` end it |
| `operations.hpp` | `serve<Request, Reply>(router, name, fn)` and `call<Reply>(endpoint, op, request, next)`: typed operations |
| `fanout.hpp` | `FanOut`: clones of the paglet on a set of hosts, one report each, a deadline, and failed clones |
| `locate.hpp` | `locate`, `pin`, `release`, `with_pinned(...)`: find a paglet and keep it in place while working with it |
| `notify.hpp` | `notify(level, title, text)`: notifications to the owner through `user-info` |
| `files.hpp` | `granted_dir`, `read_file`, `write_file`, `pick_up(root, path)`, `put_down(file, root, path)`: carrying one file between hosts |

```cpp
#include <paglets/patterns.hpp>

struct Count : paglets::patterns::Task<CountRequest, CountResult> {
    void run(const CountRequest& q) override { complete(CountResult{q.n * (q.n + 1) / 2}); }
};
```

> [!NOTE]
> Reading a file marks the paglet with the root it came from. If a
> residency rule applies to that root, the paglet only moves to hosts where
> the data may go. See [Hosts and meshes](hosts-and-meshes.md#data-residency).

The [patterns design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-patterns.md)
has the details. The [demos](demos.md) show the patterns in use.

## Running paglets locally

Paglets run in sandboxed worker processes (`paglets-worker`, found next to
`paglets-host`), one per scheduler lane (`--threads`, default 2). Use `--in-process`
to run them inside the host process instead, and `--no-sandbox` while
debugging. Anything a paglet writes to stdout or stderr goes to the host
log.

With a state directory, paglets outlive the host process. They resume from
their last memory image:

```bash
$H run build/linux-gcc16/guests/counter.wasm --state-dir state --keep \
    --call increment '{"by": 5, "note": "first"}'
$H list --state-dir state
$H call --state-dir state all increment '{"by": 1, "note": "again"}'
```

`--root NAME=DIR` gives the `files` service a named root. `module inspect`
shows a module's hash, its imports and exports, and the ABI version it was
built for:

```bash
$H module inspect build/linux-gcc16/guests/counter.wasm
```

The [command-line reference](cli-reference.md#run-list-and-call) lists
every option of `run`, `list` and `call`.
