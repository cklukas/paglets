# Greeter and Adder

Greeter and Adder are two paglets in a CMake project of their own,
outside the paglets repository. The project finds an installed paglets
package with `find_package(paglets)` and builds its modules with the same
`paglets_add_module` function the repository uses. It is the template for
paglets you develop and ship separately from paglets/cpp.

Directory: [`cpp/examples/out-of-tree/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/out-of-tree)

## What it shows

- **The installed package.** `cmake --install` turns a paglets/cpp build
  into a CMake package with `paglets-host`, the guest SDK, the service
  contracts and the schema generator. The project needs nothing else from
  the repository (see
  [Modules outside the repository](../building.md#modules-outside-the-repository)).
- **A plain paglet with an option.** Greeter answers `greet`, and `digest`
  with the SHA-256 of a text. The option `SHA256` compiles the SHA-256
  code of the host into the module, so both compute the same digests.
- **A typed service contract.** Adder declares its operations as a
  contract in a header. The schema generator turns it into MessagePack
  codecs, a typed `adder::Client` and `adder::serve()`, which registers the
  operations with the router (see
  [Defining your own typed service contract](../sdk-reference.md#defining-your-own-typed-service-contract)).
- **A system service from outside the tree.** Adder's `host` operation asks
  the `server-info` system paglet for the host's operating system, through
  the client generated when the package was built.

## Why it is useful

Paglets for your own work belong in your own repository, built with your
own CMake project, not in the paglets source tree. This example is the
smallest complete such project: copy the directory, point
`CMAKE_PREFIX_PATH` at an installed package, and build. Because the test
suite builds it against a fresh installation, it also guards the package:
a header or file that the package forgets to install breaks this test.

## How it works

The project's `CMakeLists.txt` finds the package and adds the two
modules. `SCHEMA_HEADER` and `SCHEMA_NAMESPACE` name the contract,
`SERVICES` the system services it calls, and `PATTERNS` adds the patterns
library:

<!-- snippet: cpp/examples/out-of-tree/CMakeLists.txt -->
Source: [`cpp/examples/out-of-tree/CMakeLists.txt`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/out-of-tree/CMakeLists.txt)

```cmake
# A project outside the paglets repository that builds guest paglets against
# an installed paglets package:
#
#   cmake --install cpp/build/<preset> --prefix <prefix>
#   cmake -S cpp/examples/out-of-tree -B <build> -G Ninja \
#         -DCMAKE_PREFIX_PATH=<prefix> -DCMAKE_CXX_COMPILER=g++-16
#   cmake --build <build>
#   <prefix>/bin/paglets-host run <build>/guests/greeter.wasm --call greet '"world"'

cmake_minimum_required(VERSION 3.28)
project(paglets_out_of_tree LANGUAGES CXX)

find_package(paglets 0.1 REQUIRED CONFIG)

# A plain paglet: the guest SDK and SHA-256 (<paglets/sha256.hpp>), with an
# interface description (docs/interface-descriptions.md).
paglets_add_module(greeter SOURCES greeter.cpp SHA256 INTERFACE greeter.interface.json)

# A paglet with a typed service contract (needs a C++26 reflection compiler
# for the schema generator) that also calls a system service through the
# patterns library.
paglets_add_module(adder
    SOURCES adder.cpp
    SCHEMA_HEADER adder_contract.hpp
    SCHEMA_NAMESPACE adder
    SERVICES server_info
    PATTERNS)

if(NOT PAGLETS_GUESTS_AVAILABLE)
    message(WARNING "no WASI toolchain: set PAGLETS_WASI_CLANG and PAGLETS_WASI_SYSROOT")
endif()
```
<!-- /snippet -->

Greeter is a complete paglet in a few lines:

<!-- snippet: cpp/examples/out-of-tree/greeter.cpp -->
Source: [`cpp/examples/out-of-tree/greeter.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/out-of-tree/greeter.cpp)

```cpp
// A paglet built outside the paglets repository: answers "greet" requests,
// and "digest" requests with the SHA-256 of a text (option SHA256).

#include <paglets/paglet.hpp>
#include <paglets/sha256.hpp>

#include <string>

namespace {

class Greeter : public paglets::Paglet {
public:
    Greeter() {
        router().on<std::string>("greet", [](const std::string& name, paglets::Message& m) {
            m.reply("greetings, " + name + ", from outside the tree");
        });
        router().on<std::string>("digest", [](const std::string& text, paglets::Message& m) {
            const auto* bytes = reinterpret_cast<const std::uint8_t*>(text.data());
            m.reply("sha256:" + paglets::to_hex(paglets::sha256({bytes, text.size()})));
        });
    }
};

}  // namespace

PAGLETS_PAGLET(Greeter)
```
<!-- /snippet -->

Adder's contract is a header with plain structs and a `Contract` that
names the service and declares one function per operation:

<!-- snippet: cpp/examples/out-of-tree/adder_contract.hpp -->
Source: [`cpp/examples/out-of-tree/adder_contract.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/out-of-tree/adder_contract.hpp)

```cpp
// Service contract of the adder paglet. The schema generator turns it into
// MessagePack codecs, a typed adder::Client and adder::serve().

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace adder {

struct AddRequest {
    std::int64_t a = 0;
    std::int64_t b = 0;
};

struct AddReply {
    std::int64_t sum = 0;
};

struct HostRequest {};

struct HostReply {
    std::string os;  // of the host the paglet runs on
};

struct Contract {
    static constexpr std::string_view service = "adder";
    AddReply add(const AddRequest&);
    HostReply host(const HostRequest&);
};

}  // namespace adder
```
<!-- /snippet -->

The constructor calls `adder::serve(router(), *this)`. It registers `add`
and `host`, which decode the request and call the member functions of the
same names, and `describe`, which answers the schema descriptor. `add`
answers at once with the value it returns. `host` defers its reply, so
`serve` does not send the empty value it returns. It finds `server-info`
with the patterns helper `endpoint`, asks for the host summary, and
answers with the operating system when the summary arrives. If a step
fails, it answers with the text `error:` and the name of the error:

<!-- snippet: cpp/examples/out-of-tree/adder.cpp#operations -->
Source: [`cpp/examples/out-of-tree/adder.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/out-of-tree/adder.cpp)

```cpp
paglets::Result<adder::AddReply> add(const adder::AddRequest& q, Message&) { return adder::AddReply{q.a + q.b}; }

// Replies later, when server-info has answered.
paglets::Result<adder::HostReply> host(const adder::HostRequest&, Message& m) {
    paglets::Capability pending = m.defer_reply();
    paglets::patterns::endpoint("server-info", [pending](paglets::Result<paglets::Endpoint> endpoint) mutable {
        if (!endpoint) return fail(pending, endpoint.error());
        si::Client client{*endpoint};
        auto sent = client.summary({}, [pending](paglets::Result<si::Summary> r, Message&) mutable {
            if (!r) return fail(pending, r.error());
            paglets::reply_to(pending, adder::HostReply{r->os});
        });
        if (!sent) fail(pending, sent.error());
    });
    return adder::HostReply{};  // not sent: the reply is deferred
}
```
<!-- /snippet -->

## Running it

Install a paglets/cpp build, then configure the project with the package
on `CMAKE_PREFIX_PATH`. Adder has a schema header, so the project's C++
compiler must support C++26 reflection (GCC 16):

```bash
cd cpp
cmake --preset macos-arm64
cmake --build --preset macos-arm64
cmake --install build/macos-arm64 --prefix $HOME/opt/paglets
cd ..
cmake -S cpp/examples/out-of-tree -B out-of-tree-build -G Ninja \
    -DCMAKE_PREFIX_PATH=$HOME/opt/paglets -DCMAKE_CXX_COMPILER=g++-16
cmake --build out-of-tree-build
```

Run the modules with the installed `paglets-host`:

```bash
H=$HOME/opt/paglets/bin/paglets-host
$H run out-of-tree-build/guests/greeter.wasm --call greet '"world"' --call digest '"abc"'
$H run out-of-tree-build/guests/adder.wasm --call add '{"a": 40, "b": 2}' --call host '{}'
```

Greeter answers `greetings, world, from outside the tree` and
`sha256:ba7816bf...`; Adder answers `{"sum":42}` and the host's operating
system, such as `{"os":"macos"}`.

## Test results

This example has no results of its own. It is covered by the ctest
`package_out_of_tree` (in `cpp/tests/CMakeLists.txt`, with the script
`cpp/tests/cli_install.cmake`), which runs when guests can be built and
reflection is enabled, unless `PAGLETS_TEST_INSTALL` is off. The test:

- installs the current build into a temporary prefix and checks that
  `paglets-host`, `pagletsConfig.cmake`, the SDK header `paglet.hpp`, the
  SHA-256 source and the generated `server-info` client are there;
- configures and builds this project in a fresh build directory, with only
  the installed package;
- runs Greeter with the installed `paglets-host` and expects
  `greetings, world, from outside the tree` for `greet "world"`, and the
  SHA-256 of `abc`
  (`sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad`)
  for `digest`;
- runs Adder and expects `"sum":42` for `add` with 40 and 2, and `macos`,
  `linux` or `windows` as the operating system for `host`;
- checks that the build wrote `adder.schema.json`.
