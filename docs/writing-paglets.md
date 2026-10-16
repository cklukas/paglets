# Writing paglets

A paglet is a C++ class compiled to WebAssembly with the guest SDK
(`cpp/sdk/`). Its state is ordinary C++ objects. Memory images move it
between hosts without any serialization code.

A module must import functions from the host's permitted import set.
Admission checks both the name and the function kind, including for
wildcard policies, and names unsupported imports and their kinds in the
error. Tables, memories, globals and exception tags are not host imports.

## A first paglet

<!-- snippet: cpp/examples/hello/hello.cpp -->
Source: [`cpp/examples/hello/hello.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/hello/hello.cpp)

```cpp
// Smallest paglet: answers "greet" requests and counts them.

#include <paglets/paglet.hpp>

#include <string>

namespace {

class Hello : public paglets::Paglet {
public:
    Hello() {
        router().on<std::string>("greet", [this](const std::string& name, paglets::Message& m) {
            ++greetings_;
            m.reply("hello, " + name + " (greeting " + std::to_string(greetings_) + ")");
        });
    }

    void on_created(paglets::Started&) override {
        auto info = paglets::self_info();
        paglets::log("hello paglet created as " + (info ? info->id : std::string("?")));
    }

private:
    std::uint32_t greetings_ = 0;
};

}  // namespace

PAGLETS_PAGLET(Hello)
```
<!-- /snippet -->

Build it from CMake with `paglets_add_module`, then run it with the host:

```cmake
paglets_add_module(hello SOURCES hello/hello.cpp INTERFACE hello/interface.json)
```

The repository call also embeds `hello/interface.json` for tools that describe
the module. It belongs in `cpp/examples/CMakeLists.txt`. In a
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

To let tools and people use the paglet without reading its code, describe
its messages and start arguments in a JSON file and add
`INTERFACE counter/interface.json` to `paglets_add_module`. The build puts
the description into the module. [`paglets-web`](web-ui.md) then shows
forms for the messages, and `paglets-host remote describe` prints them
(see [Interface descriptions](interface-descriptions.md)).

A request whose message has no registered handler answers
`unknown_message`; handle it through the router or return a suitable
error from a raw message handler.

## When a paglet moves, and what moves

Moves, clones, deactivation and checkpoints capture the paglet between
handler calls. Its image holds WebAssembly linear memory and exported
mutable globals, including the constructed C++ object and its heap. No
active call stack or instruction position is resumed: the next handler
starts a new call. Images work across supported CPU architectures and
operating systems, provided the destination has the same module hash and
permits the image size.

Tables are not captured. A guest must not mutate its WebAssembly tables
at runtime. Admission decodes instructions and refuses table mutation
and passive-segment mutation, which memory images do not capture. The supported guest build also exports
all mutable globals needed by an image.

The handler watchdog is disabled by default. An operator can enable a
per-invocation budget, for example 5 s of elapsed host time.
The paglet itself can work for hours or indefinitely. Waiting for background
work does not spend handler time. Use the framework's task handles for long
I/O and [compute jobs](compute-jobs.md) for long CPU work; see
[Long-running agents and tasks](long-running-tasks.md) for the shared task
model, cancellation, progress, deadlines, movement and recovery. Exceeding
the handler watchdog ends the paglet as failed.
Capability authority travels according to the
[capability movement rules](sdk-reference.md#capabilities-across-moves).

There is no retract operation. A paglet returns by dispatching itself to
the starting host, or its owner or an admin uses
`paglets-host remote dispatch PAGLET HOST`.

A running paglet remains bound to its module hash. A new code version is a
new module: start new paglets and hand application state over explicitly
by messages or shared artifacts. Automatic migration of an old image into
new code is not implemented.

## Memory and image size

Wasm linear memory grows but does not shrink. Freeing C++ objects does not
reduce the image's page count and usually leaves nonzero bytes in freed
heap pages. Capture omits pages that are currently all zero; other pages
still contribute to an image. Repeated moves reuse pages cached by the
destination, but a host that has not seen them must fetch them. Keep large
data in files, artifacts or storage and carry capabilities to it instead
of copying it into the paglet heap. `paglets-spike image-save --bloat`
lets you measure this effect.

The module store validates the standard import set supported for stored
Wasm modules. Creation and arrival also check the requested trust class.
Native system paglets are host objects; storing a Wasm module does not
give it access to native system imports. `paglets-host module inspect`
reports the per-class admission checks.

## Capabilities

Capabilities carry authority: endpoints identify paglets and allowed
messages; reply capabilities permit one answer; timers permit cancellation;
resource capabilities name such things as directories, artifacts and pins.

Use endpoints for `send` and `request`, reply capabilities for deferred
answers, and resource capabilities with the corresponding system services.
Lifecycle operations such as deactivate, dispose, clone and dispatch are
host calls, rather than kinds of capability.

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
paglets_add_module(pi SOURCES pi/pi.cpp SERVICES mesh_info compute_slots artifacts)
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
| `task.hpp` | `Task<Request, Result>`: the paglet-level task abstraction, with `start`, `status`, `wait`, `cancel`, progress and tracked child work; `complete(result)` and `fail(why)` finish the task, while the paglet remains available |
| `operations.hpp` | `serve<Request, Reply>(router, name, fn)` and `call<Reply>(endpoint, op, request, next)`: typed operations |
| `fanout.hpp` | `FanOut`: clones on transfer tickets (host names/key IDs, `label:L`, `offer:S`, `any`, or empty for this host), one report each, a deadline, and failed clones; see [dispatch](sdk-reference.md#lifecycle-children-and-timers) |
| `locate.hpp` | `locate`, `pin`, `release`, `with_pinned(...)`: find a paglet and keep it in place while working with it; repeated `done()` calls release only once |
| `notify.hpp` | `notify(level, title, text)`: notifications to the owner through `user-info` |
| `files.hpp` | `granted_dir`; `hash_file(dir, path)`: a file's SHA-256, computed by the host; `share_file(dir, path)` and `save_file(dir, path, content)`: files of any size between hosts, as artifact capabilities; `read_file`, `write_file`, `pick_up(root, path)`, `put_down(file, root, path)`: small files carried in the paglet's memory |

```cpp
#include <paglets/patterns.hpp>

struct Count : paglets::patterns::Task<CountRequest, CountResult> {
    void run(const CountRequest& q) override { complete(CountResult{q.n * (q.n + 1) / 2}); }
};
```

> [!NOTE]
> Prefer `share_file` and `save_file` for files: the paglet carries a
> capability of a few bytes, and the content goes directly from host to
> host (see [Moving content between hosts](system-paglets.md#moving-content-between-hosts)).
> `pick_up` and `read_file` copy the content into the paglet's memory,
> which grows as needed within the Wasm32 address space. A host can opt into a ceiling with
> `serve --memory-mb N`; intended move destinations must allow the image
> too. The read and write helpers use 512 KB
> chunks, leaving room for message metadata below the 1 MB ABI document
> limit. A file can span several chunks; the read's `max_bytes` still caps
> the total content copied into memory.

> [!NOTE]
> Reading a file marks the paglet with the root it came from. If a
> residency rule applies to that root, the paglet only moves to hosts where
> the data may go. See [Hosts and meshes](hosts-and-meshes.md#data-residency).

The [patterns design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-patterns.md)
has the details. The [example paglets](examples/index.md) show the patterns in use.

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
shows a module's hash, its imports and exports, the ABI version it was
built for, and its interface description:

```bash
$H module inspect build/linux-gcc16/guests/counter.wasm
```

The [command-line reference](cli-reference.md#run-list-and-call) lists
every option of `run`, `list` and `call`.
