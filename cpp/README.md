# paglets/cpp

C++ implementation of paglets: mobile agents as WebAssembly modules, run by
the embedded [WAMR](https://github.com/bytecodealliance/wasm-micro-runtime)
runtime, moving between hosts as **memory images**. The design and work
packages are in [`../planning/`](../planning/cpp-edition-plan.md).

Status: milestone M0 (feasibility) is closed
([results](../planning/cpp-m0-results.md)). Milestone M1 (single-host
runtime) is closed ([results](../planning/cpp-m1-results.md)): the paglet
ABI v1 ([specification](../planning/cpp-abi-v1.md)), the guest SDK,
capabilities and messaging, persistence with crash recovery, and sandboxed
worker processes on Linux, macOS and Windows. Milestone M2 (mesh identity,
policy and system paglets) is closed ([results](../planning/cpp-m2-results.md)):
keys, the signed ledger with gossip, enrollment and passports; allow/ask/deny
policy with grants and an audit log; and the system paglets. Milestone M3
(movement between hosts) has started: WP11 gives hosts a module store with
a cache of compiled modules and garbage collection, module trust in the
ledger, and code mobility: a host fetches modules it lacks from its sources
or other hosts, verified by hash, and runs paglets launched from another
host ([design](../planning/cpp-modules.md)). WP12 puts hosts on the network: HTTPS with end-to-end
encrypted, mutually authenticated Noise channels between hosts and for CLI
sessions, and paglets that move between hosts with their state (only
changed memory pages travel again), their grants and transfer tickets
([design](../planning/cpp-networking.md)). WP13 finds them anywhere: location
records on hosts chosen by consistent hashing, updated by a majority on
every move, with failover when hosts go down, and pins that keep a paglet on
its host for a while ([design](../planning/cpp-location.md)).

## Layout

```text
common/include/paglets/   MessagePack primitives, the paglet ABI v1 documents and the service
                          contracts (host and guests)
host/                     host library: Wasm engine, memory images, runtime, wire codecs;
                          mesh library: keys, ledger, gossip, passports;
                          services library: system paglets and the platform layer;
                          paglets-host, paglets-worker and paglets-spike
sdk/                      guest SDK (paglets/paglet.hpp)
tools/schema_gen/         guest schema generator (C++26 reflection -> codecs, service clients)
examples/                 sample paglets: hello, counter, ping_pong
tests/                    unit and conformance tests, test guests
cmake/                    WAMR build (with source fixes) and guest build functions
```

## Toolchains

| Part | Compiler | Why |
|---|---|---|
| Host | GCC 16 with `-std=c++26 -freflection` | C++26 reflection for wire codecs and the schema generator |
| Host without reflection | a C++23 compiler whose standard library has `<expected>` | everything except the reflection codecs and guest schema generation |
| Guests | clang targeting `wasm32-wasip1` with a WASI sysroot | the only C/C++ compiler for Wasm; no reflection, so guest schema code is generated |

macOS (Homebrew):

```bash
brew install gcc@16 llvm lld wasi-libc wasi-runtimes cmake ninja libsodium openssl@3 zstd pkgconf
```

Linux: GCC 16 plus [wasi-sdk](https://github.com/WebAssembly/wasi-sdk) in
`/opt/wasi-sdk` (or set `WASI_SDK_PATH`), libsodium 1.0.18 or later, OpenSSL 3
and zstd (`apt install libsodium-dev libssl-dev libzstd-dev pkg-config`).
Windows (MSYS2 UCRT64): `mingw-w64-ucrt-x86_64-libsodium`,
`mingw-w64-ucrt-x86_64-openssl`, `mingw-w64-ucrt-x86_64-zstd` and
`mingw-w64-ucrt-x86_64-pkgconf`. The build fetches WAMR and cpp-httplib at
pinned releases. The guest toolchain can also be given explicitly with
`-DPAGLETS_WASI_CLANG=... -DPAGLETS_WASI_SYSROOT=...`.

## Build and test

From this directory:

```bash
cmake --preset macos-arm64
cmake --build --preset macos-arm64
ctest --preset macos-arm64
```

Other presets: `linux-gcc16`, `windows-mingw-gcc16`, the sanitizer variants
`macos-arm64-asan`, `linux-gcc16-asan` (AddressSanitizer and
UndefinedBehaviorSanitizer) and `linux-gcc16-tsan` (ThreadSanitizer), and
`linux-host-only` for hosts without GCC 16 or a Wasm toolchain (reflection
off, guests taken from `-DPAGLETS_PREBUILT_GUEST_DIR=...`; Wasm modules are
platform-independent, so guests built anywhere can be used).

Build options:

| Option | Default | Meaning |
|---|---|---|
| `PAGLETS_ENABLE_REFLECTION` | on if the compiler supports it | reflection codecs and guest schema generation |
| `PAGLETS_BUILD_GUESTS` | `ON` | build guest `.wasm` modules (needs the WASI toolchain) |
| `PAGLETS_WAMR_FAST_INTERP` | `ON` | WAMR fast interpreter (`OFF`: classic interpreter) |
| `PAGLETS_WAMR_HW_BOUND_CHECK` | `ON` (`OFF` with MinGW-w64) | guard-page bound checks; turn `OFF` on kernels with a 39-bit address space (Raspberry Pi OS) to run more than ~60 instances per process. MinGW-w64 GCC cannot build them (WAMR catches the faults with MSVC `__try`/`__except`) |
| `PAGLETS_SANITIZE` | `OFF` | AddressSanitizer and UndefinedBehaviorSanitizer |
| `PAGLETS_SANITIZE_THREAD` | `OFF` | ThreadSanitizer |

## Writing a paglet

A paglet is a C++ class compiled to Wasm with the guest SDK. Its state is
ordinary C++ objects; memory images move it without serialization code.

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

Build it with `paglets_add_module(hello SOURCES hello.cpp)` in CMake; message
types in a separate header get encoders generated by `schema_gen`
(`SCHEMA_HEADER` and `SCHEMA_NAMESPACE`, see `examples/counter`). Paglets talk
to other paglets only through capabilities: endpoints to paglets they created
or were given, `request` with a reply continuation, deferred replies, timers,
clones and lifecycle operations (see `examples/ping_pong` and the
[ABI specification](../planning/cpp-abi-v1.md)). Sends and replies can be
asynchronous (`SendOptions::async`, `Message::reply_async`): they return at
once, and a message that cannot be delivered reaches
`Paglet::on_undelivered`.

## Tools

```bash
build/macos-arm64/host/paglets-host --info
build/macos-arm64/host/paglets-host run build/macos-arm64/guests/hello.wasm --call greet '"world"'
build/macos-arm64/host/paglets-host run build/macos-arm64/guests/ping_pong.wasm --call run 100
```

Message bodies and arguments are JSON, passed to the paglet as MessagePack;
replies are printed as JSON. Paglets run in worker processes
(`paglets-worker`, found next to `paglets-host`; one per scheduler lane, set
with `--threads`); `--in-process` runs them in the host process instead. The
workers run in an operating-system sandbox (Linux: seccomp; macOS: a sandbox
profile; Windows: a job object and a restricted token) and cannot open
files, create sockets or start programs (`--no-sandbox` for debugging;
`paglets-worker --check-sandbox` verifies it). Output a paglet writes to
stdout or stderr goes to the host log. The host runs the standard system paglets
(files, server-info, directory, storage, artifacts, pubsub, user-info;
[design](../planning/cpp-system-paglets.md)); `--root NAME=DIR` gives the
files service a named root. Guests call services through typed clients
generated from the contracts in `common/include/paglets/services/`
(`paglets_add_module(... SERVICES files server_info ...)`). With a state directory, paglets outlive the host
process and resume from their last memory image:

```bash
build/macos-arm64/host/paglets-host run build/macos-arm64/guests/counter.wasm --state-dir state --keep \
    --call increment '{"by": 5, "note": "first"}'
build/macos-arm64/host/paglets-host list --state-dir state
build/macos-arm64/host/paglets-host call --state-dir state all increment '{"by": 1, "note": "again"}'
```

`paglets-spike` holds the M0 and M1 measurements and the memory-image
experiments:

```bash
build/macos-arm64/host/paglets-spike info build/macos-arm64/guests/counter.wasm
build/macos-arm64/host/paglets-spike bench build/macos-arm64/guests/testbed.wasm build/macos-arm64/guests/counter.wasm
build/macos-arm64/host/paglets-spike runtime build/macos-arm64/guests/counter.wasm build/macos-arm64/guests/ping_pong.wasm \
    --worker build/macos-arm64/host/paglets-worker
build/macos-arm64/host/paglets-spike call-bench build/macos-arm64/guests/counter.wasm status
```

Keys and the mesh security ledger (M2, [design](../planning/cpp-ledger.md)).
The ledger commands work on a local ledger directory (a host's copy, or an
admin's); admin and owner keys are encrypted with a passphrase (read from
the terminal, or `--passphrase-file`):

```bash
H=build/macos-arm64/host/paglets-host
$H keys init --role admin --name alice --out alice.key
$H keys init --role host --name lab-1 --out lab-1.key
$H mesh create --name lab --ledger ledger --admin alice.key
$H ledger request --ledger ledger --key lab-1.key --label linux   # prints the request ID
$H ledger show --ledger ledger                                     # lists pending requests
$H ledger approve --ledger ledger --admin alice.key <request-id>
$H ledger revoke --ledger ledger --admin alice.key <key-id> --reason retired
```

`ledger enroll`, `remove`, `deny`, `admins` (add or remove admins, change the
quorum) and `sign` (co-sign a record that needs several admins) complete the
set; `paglets-host ledger` prints the full usage.

Access to host resources follows the mesh policy
([design](../planning/cpp-policy.md)): rules decide allow, ask or deny;
paglets ask the `grants` system paglet; admins approve or deny requests from
any host; every decision lands in the audit log:

```bash
$H ledger rule --ledger ledger --admin alice.key --name "staff docs" --decision ask \
    --service files --op read --group staff --root data --path "docs/**"
$H ledger show --ledger ledger                     # rules, grants, pending grant requests
$H ledger approve --ledger ledger --admin alice.key <grant-request-id>
$H ledger audit --ledger ledger
```

Module trust ([design](../planning/cpp-modules.md)): signers vouch for
modules, admins decide which modules may run as roaming, resident or system
paglets; `module-policy trusted` makes roaming modules need trust as well.
Hosts end paglets whose module loses trust:

```bash
$H keys init --role signer --name build-bot --out build-bot.key
$H ledger sign-module --ledger ledger --key build-bot.key --name calc --version 1.2 calc.wasm
$H ledger trust --ledger ledger --admin alice.key --name "lab apps" --class roaming --signer <signer-key-id>
$H ledger module-policy --ledger ledger --admin alice.key trusted
$H ledger revoke --ledger ledger --admin alice.key --module calc.wasm --reason vulnerable
```

Hosts on the network ([design](../planning/cpp-networking.md)):
`paglets-host serve` runs a host of the mesh with its own copy of the
ledger; `paglets-host remote` opens an end-to-end channel to a host as an
admin or owner, to show its status, push ledger records, launch a paglet
(the module travels with the request), call it or move it with a transfer
ticket (a host name, a key ID, `label:<label>` or `any`, with
`?retries=N&arrival=active|inactive`):

```bash
$H keys init --role owner --name olga --out olga.key
$H keys init --role host --name lab-2 --out lab-2.key
$H ledger enroll --ledger ledger --admin alice.key host <lab-2-key-id> lab-2   # lab-1 is enrolled above
$H ledger enroll --ledger ledger --admin alice.key owner <olga-key-id> olga
cp -r ledger ledger-lab-1 && cp -r ledger ledger-lab-2
$H serve --key lab-1.key --ledger ledger-lab-1 --state state-1 --listen 0.0.0.0:7443 \
    --peer <lab-2-key-id>=https://lab-2:7443 &
$H serve --key lab-2.key --ledger ledger-lab-2 --state state-2 --listen 0.0.0.0:7443 \
    --peer <lab-1-key-id>=https://lab-1:7443 &
$H remote launch --connect https://lab-1:7443 --key olga.key --ledger ledger \
    build/macos-arm64/guests/counter.wasm                          # prints the paglet ID
$H remote call --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> \
    increment '{"by": 5, "note": "first"}'
$H remote dispatch --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> lab-2
$H remote call --connect https://lab-2:7443 --key olga.key --ledger ledger <paglet-id> \
    increment '{"by": 1, "note": "moved"}'
$H remote status --connect https://lab-2:7443 --key alice.key --ledger ledger
$H remote locate --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id>   # from any host
$H remote pin --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> --minutes 30
$H remote unpin --connect https://lab-1:7443 --key alice.key --ledger ledger <paglet-id>   # admins
```

Paglets locate and pin each other through the `locator` system paglet;
pinning needs a policy rule for service `locator`, operation `pin`.

Moving a paglet as a memory image between processes or hosts (the module
must be the same file on both sides; images are independent of CPU
architecture and operating system):

```bash
build/macos-arm64/host/paglets-spike image-save build/macos-arm64/guests/counter.wasm counter.pgimg --increments 5
build/macos-arm64/host/paglets-spike image-resume build/macos-arm64/guests/counter.wasm counter.pgimg --increments 2 --expect 18
```
