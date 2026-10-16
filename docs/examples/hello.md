# Hello

Hello is the smallest paglet: it answers `greet` requests with a greeting
and counts how many it gave. It is the starting point for a paglet of your
own, and the module the command-line tests use to check that a host runs
paglets at all.

Directory: [`cpp/examples/hello/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/hello)

## What it shows

- **A paglet is a C++ class.** It derives from `paglets::Paglet` and is
  registered with `PAGLETS_PAGLET(Hello)`. The SDK keeps one instance of
  it in the module's linear memory, the memory of the WebAssembly module
  (see [The paglet class](../sdk-reference.md#the-paglet-class)).
- **Handlers for messages.** The constructor registers a handler for
  `greet` with the router. The typed form `router().on<std::string>`
  decodes the body into a `std::string` before the handler runs.
- **State is an ordinary member.** The count of greetings is a
  `std::uint32_t` member. Nothing saves it explicitly: it is part of the
  paglet's memory, and its memory image keeps it.
- **A lifecycle hook.** `on_created` runs once, when the paglet is
  created, before any message. Hello asks for its own ID with
  `paglets::self_info()` and writes it to the host log.

## Why it is useful

Hello needs nothing but the guest SDK: no message header, no system
services, no mesh. It shows the parts every paglet has, so it is the file
to copy when you start a new paglet. Because it has no dependencies, it is
also the quickest check that the guest toolchain builds modules and that
`paglets-host` runs them.

## How it works

The whole paglet:

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

`m.reply(...)` answers the request. The SDK encodes the reply as
MessagePack; the command line prints it as JSON. A request that has no
handler is answered `unknown_message` by the host.

The module is built with one line in `cpp/examples/CMakeLists.txt`:

<!-- snippet: cpp/examples/CMakeLists.txt#hello_build -->
Source: [`cpp/examples/CMakeLists.txt`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/CMakeLists.txt)

```cmake
paglets_add_module(hello SOURCES hello/hello.cpp INTERFACE hello/interface.json)
```
<!-- /snippet -->

`INTERFACE` embeds the [interface description](../interface-descriptions.md),
so the CLI and web UI can describe the module and its messages.

See [Writing paglets](../writing-paglets.md#a-first-paglet) for the same
paglet step by step.

## Running it

`run` creates one paglet from the module, sends it the calls in order and
disposes of it at the end (see
[run, list and call](../cli-reference.md#run-list-and-call)). Bodies on the
command line are JSON, so the name is a JSON string:

```bash
H=build/macos-arm64/host/paglets-host
$H run build/macos-arm64/guests/hello.wasm --call greet '"world"' --call greet '"again"'
```

`run` prints the paglet's ID and module hash, then one line per call. The
two calls go to the same paglet, so the second reply is
`"hello, again (greeting 2)"`. The line `hello paglet created as <id>`
appears in the host log.

## Test results

The test `runtime: hello sample` in `cpp/tests/test_runtime.cpp` creates
the paglet in a runtime of the test process and sends it `greet` with
`"world"`. It checks that the reply has status 0 and is
`hello, world (greeting 1)`, and that the host log contains
`hello paglet created as` followed by the paglet's ID.

Two command-line tests in `cpp/tests/CMakeLists.txt` run the module with
`paglets-host run`: `host_run_hello` expects `hello, world` in the output,
and `host_run_services` does the same with the system paglets and a named
root (`--root`).

<!-- results: hello -->
| Test | Mode | Outcome | Platform | Recorded | reply to greet "world" |
|---|---|---|---|---|---|
| runtime: hello sample | in-process | passed | Darwin arm64, RelWithDebInfo build | 2026-10-16 | hello, world (greeting 1) |
| runtime: hello sample | worker processes | passed | Darwin arm64, RelWithDebInfo build | 2026-10-16 | hello, world (greeting 1) |
<!-- /results -->
