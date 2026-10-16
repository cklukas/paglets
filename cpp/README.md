# paglets/cpp

C++ implementation of paglets: mobile agents as WebAssembly modules, run by
the embedded [WAMR](https://github.com/bytecodealliance/wasm-micro-runtime)
runtime, moving between hosts as **memory images**. The design and work
packages are in [`../planning/`](../planning/cpp-edition-plan.md); the user
documentation is the `paglets/cpp` section of the documentation site
([`../docs/`](../docs/index.md)), with a
[command-line reference](../docs/cli-reference.md), a
[guest SDK reference](../docs/sdk-reference.md) and
[configuration and troubleshooting](../docs/configuration.md).

Compute jobs (ABI v1.3) run functions in separate fresh Wasm instances,
with slot admission, cancellation, remote execution, artifacts and optional
AOT compilation. The SDK provides continuations and C++ coroutines; Pi uses
jobs for its chunks and decimal conversion. See
[Compute jobs](../docs/compute-jobs.md). `paglets-wamrc` is built and installed
when a supported LLVM is available; jobs otherwise use the interpreter.
For the separate `compute-slots` service, release an existing lease before
changing its job or core count.
Leases and native jobs share admission priority: later work leaves capacity
for an older waiting request as running allocations finish.
Leases expire after 10 minutes by default, extended for longer estimates;
renewal stays within a 24-hour continuous limit.

Queued requests retain their expiry across hosts, timed deactivation wakes
travel and survive stored recovery, and clones discard inherited SDK reply
waits. Modules that mutate state outside the memory-image format are
refused at admission. Import policies authorize function imports only,
checking their kind as well as their name. Interface descriptions are checked against their C++
schema, including examples and defaults. See the
[SDK reference](../docs/sdk-reference.md) and
[remaining issues](../planning/cpp-paper-audit.md).

New root identities bind the owner, mesh and launch nonce. Mesh protocol 2
requires upgraded hosts and clients together; saved paglets keep their IDs
and can recover and move. `remote launch --nonce` replaces `--id`; the host
C++ launch/passport APIs take a nonce too. Web launch forms use the same
nonce and show the derived global ID. See [identity upgrades](../docs/hosts-and-meshes.md#paglet-identities-and-upgrades).

Moves preserve replies already waiting for their SDK continuations, host
reports and one-shot timers, including the timer message kind. A move that
exceeds its notification or transport limit is refused without consuming
those deliveries. Outstanding request budgets also count time spent moving;
peers that cannot restore their timing metadata refuse the move. See [movement limits](../docs/configuration.md).

Status: milestone M0 (feasibility) is closed
([results](../planning/cpp-m0-results.md)). Milestone M1 (single-host
runtime) is closed ([results](../planning/cpp-m1-results.md)): the paglet
ABI v1 ([specification](../planning/cpp-abi-v1.md)), the guest SDK,
capabilities and messaging, persistence with crash recovery, and sandboxed
worker processes on Linux, macOS and Windows. Milestone M2 (mesh identity,
policy and system paglets) is closed ([results](../planning/cpp-m2-results.md)):
keys, the signed ledger with gossip, enrollment and passports; allow/ask/deny
policy with grants and an audit log; and the system paglets. Milestone M3
(movement between hosts) is closed: WP11 gives hosts a module store with
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
its host for a while ([design](../planning/cpp-location.md)). WP14 makes the
hosts a mesh: they find each other through any enrolled host, by gossip of
signed host announcements and by multicast beacons on the local network,
keep a registry of the hosts with their addresses, online state and
versions, and refuse hosts of another mesh protocol or paglet ABI
([design](../planning/cpp-mesh.md)). WP15 brings in hosts without inbound
ports (behind NAT): they poll a few reachable hosts that relay for them, and
everybody else reaches them through end-to-end encrypted channels tunneled
through those relays ([design](../planning/cpp-relay.md)). This completes
milestone M3. In milestone M4, WP16 adds `mesh-info` (snapshots of every host
by gossip) and `compute-slots` (admission of compute work per host, with
queues and redirects to hosts with free slots; no central scheduler), and
the pi example: hex digits of pi computed in chunks on the hosts of the
mesh, surviving the loss of a host ([design](../planning/cpp-compute.md)).
WP17 adds service offers (hosts announce what their system paglets can do;
paglets find them with `mesh-info` and move with tickets such as
`offer:ai.summarize`), the `web` and `ai` gateway system paglets, the
Download Courier and AI Document Digest examples
([design](../planning/cpp-web-ai.md)), and data residency: paglets that
read a restricted root carry its mark and move only where its content may
go ([design](../planning/cpp-residency.md)). WP18 adds the patterns library
of the guest SDK ([design](../planning/cpp-patterns.md)); WP19 adds admin
work against live hosts to the CLI (pull, requests, approve, deny, audit,
modules, dispose, `module inspect`); WP20 adds example paglets from the
[catalogue](../planning/cpp-demo-paglets.md) (Mesh Journey, File Finder and
Storage Analyzer, Duplicate Finder, File Courier, Tree Compare, Log Scout,
Inventory and Process Finder, Volume Guard, Mesh Benchmark, Latency Map,
Hide and Seek, Semantic Mesh Search, Image Describer, Log Explainer, Web
Researcher, Release Watcher), tested on meshes of several hosts, and the first
[benchmarks](../planning/cpp-benchmarks.md); WP21 fuzzes every decoder of
data from outside (ledger records, frames between hosts, memory images,
modules, the Noise handshake, contracts) in CI
([design](../planning/cpp-hardening.md)). The content exchange moves files
and artifacts of any size directly between hosts: paglets carry artifact
capabilities that work on every host, within data residency
([design](../planning/cpp-content-exchange.md)). Zombie paglets keep running
as long as one host of the mesh is alive: a replica on every host, takeovers,
journals merged across partitions, one result at the sink, restarts from
memory images (ABI v1.2). Permanent result refusals end the group with a
failure notification. The protocol is specified as a C++ state machine
and checked by `paglets-protocol-check`, a model checker and simulator in
the code base; the Mesh Chronicle example writes the life log of its mesh
([design](../planning/cpp-zombie.md), [docs](../docs/zombie-protocol.md)).

Revocations are rechecked on established channels, relay tunnels and
polled results after the host learns the ledger change; bootstrap seeds
and explicit host pins do not override a known revocation. Chronicle waits
for creator admission and replies with its group ID, `accepted` and a refusal
`reason`; other replicas form asynchronously. Inventory acknowledges dispatch
startup; read its `report` for completion.

## Layout

`paglets-host serve --memory-mb N` sets the per-paglet linear-memory cap
(default 256 MB), separately from compute jobs (default and maximum 1 GB).
Instances start with their module’s initial memory and grow as needed.
`remote moves` exposes move
and phase counters. `tools/mesh_bench.sh --steps move` measures 0, 1, 4 and
16 MB ballast, with ten cold-page and return samples per direction and
an explicit 32 MB host cap for the largest image.
The script stores TCP baselines separately and reuses matching host pairs
in partial transfer runs, retaining the baseline measurement date.
Live benchmark records require the clone to confirm its compute slot.
Chronicle records use each host's actual build; `--results-dir` and
`--log-file` can keep a separate run outside the committed measurements.

The memory limits are ceilings, not reservations of physical RAM for each
instance. Intended move destinations must allow the image too.

```text
common/include/paglets/   MessagePack primitives, the paglet ABI v1 documents and the service
                          contracts (host and guests)
host/                     host library: Wasm engine, memory images, runtime, wire codecs;
                          mesh library: keys, ledger, gossip, passports;
                          services library: system paglets and the platform layer;
                          paglets-host, paglets-worker and paglets-spike;
                          client library: sessions with live hosts (remote, paglets-web)
web/                      paglets-web: its HTTP server, the JSON API and the UI (assets/)
sdk/                      guest SDK (paglets/paglet.hpp)
tools/schema_gen/         guest schema generator (C++26 reflection -> codecs, service clients)
tools/interface/          paglets-interface: interface descriptions into modules
tools/protocol_check/     paglets-protocol-check: protocols checked exhaustively and in timed runs
examples/                 example paglets: hello, counter, ping_pong, pi (chunked compute across the mesh),
                          courier, digest, journey, finder, dupes, inventory, guard,
                          latency, seek, semantic, researcher, file_courier,
                          tree_compare, log_scout, describer, watcher, explainer,
                          benchmark, chronicle (a zombie paglet); out-of-tree (a project built against the installed package)
tests/                    unit and conformance tests, test guests
cmake/                    WAMR build (with source fixes) and guest build functions
```

## Installing

macOS (Homebrew, the tap [cklukas/homebrew-paglets](https://github.com/cklukas/homebrew-paglets)):

```bash
brew install cklukas/paglets/paglets
```

To build your own paglets against the installed package, add the guest
toolchain (host-only users do not need it):

```bash
brew install llvm lld wasi-libc wasi-runtimes
```

Releases are tags `cpp-vMAJOR.MINOR.PATCH` matching `project(paglets_cpp VERSION ...)`;
`.github/workflows/cpp-release.yml` builds and tests them, publishes the source archive
and updates the tap's formula (`packaging/homebrew/`).

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

`build/<preset>/tests/paglets_tests --list [FILTER]` lists the cases compiled
into that build without executing them. Filters match test-name substrings;
an unmatched filter exits with status 2. `--help` shows the invocation forms.

Other presets: `linux-gcc16`, `windows-mingw-gcc16`, the sanitizer variants
`macos-arm64-asan`, `linux-gcc16-asan` (AddressSanitizer and
UndefinedBehaviorSanitizer) and `linux-gcc16-tsan` (ThreadSanitizer), and
`linux-host-only` for hosts without GCC 16 or a Wasm toolchain (reflection
off, guests taken from `-DPAGLETS_PREBUILT_GUEST_DIR=...`; Wasm modules are
platform-independent, so guests built anywhere can be used).

ASan/UBSan presets keep debug symbols and use `-O1`; CTest treats
undefined-behavior reports as failures.

Build options:

| Option | Default | Meaning |
|---|---|---|
| `PAGLETS_ENABLE_REFLECTION` | on if the compiler supports it | reflection codecs and guest schema generation |
| `PAGLETS_BUILD_GUESTS` | `ON` | build guest `.wasm` modules (needs the WASI toolchain) |
| `PAGLETS_BUILD_TESTS` | `ON` | build `paglets_tests` and register the CTest tests |
| `PAGLETS_TEST_INSTALL` | `ON` | register `package_out_of_tree`, a test of the installed CMake package |
| `PAGLETS_WAMR_FAST_INTERP` | `ON` | WAMR fast interpreter (`OFF`: classic interpreter) |
| `PAGLETS_WAMR_HW_BOUND_CHECK` | `ON` (`OFF` with MinGW-w64) | guard-page bound checks; turn `OFF` on kernels with a 39-bit address space (Raspberry Pi OS) to run more than ~60 instances per process. MinGW-w64 GCC cannot build them (WAMR catches the faults with MSVC `__try`/`__except`) |
| `PAGLETS_SANITIZE` | `OFF` | AddressSanitizer and UndefinedBehaviorSanitizer |
| `PAGLETS_SANITIZE_THREAD` | `OFF` | ThreadSanitizer |

To build paglets in a project of your own, install the build as a CMake
package and use `find_package(paglets)` there:

```bash
cmake --install build/macos-arm64 --prefix $HOME/opt/paglets
```

```cmake
find_package(paglets REQUIRED)
paglets_add_module(my_paglet SOURCES my_paglet.cpp)
```

The package has `paglets-host`, the guest SDK, the service clients and the
schema generator; `examples/out-of-tree` is a complete project (see
[Building and testing](../docs/building.md#modules-outside-the-repository)).
Guest builds track included SDK and local headers, so header edits rebuild
the affected modules automatically.
`tests/guests/plain_counter.c` demonstrates the ABI in C without the guest
SDK; the runtime test checks its lifecycle and saved state in both modes.

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

Build it with `paglets_add_module(hello SOURCES hello.cpp)` in CMake (in this
tree or, after `cmake --install`, in any project with `find_package(paglets)`); message
types in a separate header get encoders generated by `schema_gen`
(`SCHEMA_HEADER` and `SCHEMA_NAMESPACE`, see `examples/counter`). Paglets talk
to other paglets only through capabilities: endpoints to paglets they created
or were given, `request` with a reply continuation, deferred replies, timers,
clones and lifecycle operations (see `examples/ping_pong` and the
[ABI specification](../planning/cpp-abi-v1.md)). Sends and replies can be
asynchronous (`SendOptions::async`, `Message::reply_async`): they return at
once, and a message that cannot be delivered reaches
`Paglet::on_undelivered`. For remote asynchronous sends, receiving-host
refusals report the original endpoint handle and follow a moving sender.
Transport loss and old peers can still prevent a report.

The patterns library ([design](../planning/cpp-patterns.md);
`#include <paglets/patterns.hpp>`, `paglets_add_module(... PATTERNS)`)
covers the common shapes: a task paglet with `start`, `status` and `wait`,
typed operations, mesh fan-out of clones, locate and pin, notifications to
the owner, and moving files of any size between hosts (`share_file`,
`save_file`):

```cpp
#include <paglets/patterns.hpp>

struct Count : paglets::patterns::Task<CountRequest, CountResult> {
    void run(const CountRequest& q) override { complete(CountResult{q.n * (q.n + 1) / 2}); }
};
```

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
workers run in an operating-system sandbox (Linux: a seccomp allow list;
macOS: a sandbox profile; Windows: an LPAC, job object and restricted token).
Workers cannot access the network or start programs. Windows permits necessary
runtime resources and private AppContainer storage; see
[worker sandbox limits](../docs/configuration.md#worker-sandbox-limits).
Use `--no-sandbox` for debugging;
`paglets-worker --check-sandbox` verifies the restrictions. Output a paglet writes to
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
set; the [command-line reference](../docs/cli-reference.md#ledger)
lists every command and option.

`mesh create` defaults admin-set changes to two signatures with three or
more initial admins, otherwise one. Other admin records still default to
one signature; explicit quorum options override either threshold. Existing
ledgers retain their recorded quorums.

`remote call --timeout SECONDS` sets the reply deadline (default 30 s,
range 0.1–120 s, up to three decimal places). Web-generated call commands
preserve the effective timeout.

The shared session client validates known reply fields before the CLI
formats them. Missing or wrong-type fields produce `malformed answer`;
the CLI also rejects malformed service records before printing a list.

Host-tool messages use sender `trust: host`; authenticated remote calls
and sends carry the acting owner's or admin's key ID in `sender.owner`.
Native services keep `trust: system`.

Linux ARM64 hardware-check builds warn at startup when a virtual
address-space probe fails; the warning names the software bounds-check
build option. This is separate from the paglet and job RAM ceilings.

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

Data residency ([design](../planning/cpp-residency.md)): a paglet that
reads from a named root carries the root's mark for good; a residency rule
keeps it on the host it read on (`host-only`) or lets it move only to some
hosts (by name, key ID or `--host-label`):

```bash
$H ledger residency --ledger ledger --admin alice.key clinic hosts --host-label site
$H ledger residency --ledger ledger --admin alice.key vault host-only
```

Hosts on the network ([design](../planning/cpp-networking.md)):
`paglets-host serve` runs a host of the mesh with its own copy of the
ledger (its state directory is `--state`, unlike `--state-dir` of `run`,
`list` and `call`; TLS uses a self-signed certificate unless `--tls-cert`
and `--tls-key` are given); `paglets-host remote` opens an end-to-end
channel to a host as an admin or owner, to show its status, push ledger records, launch a paglet
(the module travels with the request), call it or move it with a transfer
ticket (a host name, a key ID, `label:<label>`, `offer:<service>[.<op>]` or
`any`, with `?retries=N&arrival=active|inactive`):

Full host key IDs take precedence over display names; an unknown full ID
never falls back to a name. Names and permitted prefixes must identify
one enrolled host across both forms. Ambiguous shorthand is refused;
use the full key ID. Compute-job host lists accept names or full IDs only.

```bash
$H keys init --role owner --name olga --out olga.key          # prints <olga-key-id>
$H keys init --role host --name lab-2 --out lab-2.key         # prints <lab-2-key-id>
$H keys init --role host --name laptop --out laptop.key       # prints <laptop-key-id>
$H ledger enroll --ledger ledger --admin alice.key host <lab-2-key-id> lab-2   # lab-1 is enrolled above
$H ledger enroll --ledger ledger --admin alice.key host <laptop-key-id> laptop
$H ledger enroll --ledger ledger --admin alice.key owner <olga-key-id> olga
cp -r ledger ledger-lab-1 && cp -r ledger ledger-lab-2 && cp -r ledger ledger-laptop
$H serve --key lab-1.key --ledger ledger-lab-1 --state state-1 --listen 0.0.0.0:7443 \
    --advertise https://lab-1:7443 &
$H serve --key lab-2.key --ledger ledger-lab-2 --state state-2 --listen 0.0.0.0:7443 \
    --advertise https://lab-2:7443 --join https://lab-1:7443 &     # any enrolled host; beacons find the rest
$H serve --key laptop.key --ledger ledger-laptop --state state-3 --no-listen \
    --join https://lab-1:7443 &                                    # behind NAT: reached through relays
$H remote hosts --connect https://lab-2:7443 --key alice.key --ledger ledger
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
$H remote requests --connect https://lab-1:7443 --key alice.key --ledger ledger   # pulls the host's records
$H remote approve --connect https://lab-1:7443 --key alice.key --ledger ledger <request-id>
$H remote modules --connect https://lab-2:7443 --key olga.key --ledger ledger
$H remote send --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> increment '{"by": 1}'
$H remote describe --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id>
$H module inspect build/macos-arm64/guests/counter.wasm
```

`paglets-web` ([docs](../docs/web-ui.md), [design](../planning/cpp-web-ui.md))
shows the mesh in the browser: every host with its properties and system and
user paglets, forms for the messages paglets describe in their
[interface descriptions](../docs/interface-descriptions.md)
(`paglets_add_module(... INTERFACE file.json)`), launching modules with start
arguments, moving, pinning and ending paglets, and next to each view the
`paglets-host remote` command that does the same. It serves its pages itself on
127.0.0.1 (port 8470, or the next free one) and opens the browser:

```bash
build/macos-arm64/web/paglets-web --connect https://lab-1:7443 --key olga.key --ledger ledger \
    --module-dir build/macos-arm64/guests
```

Paglets locate and pin each other through the `locator` system paglet;
pinning needs a policy rule for service `locator`, operation `pin`.

Gateway hosts ([design](../planning/cpp-web-ai.md)) offer `web` (mediated
web access; internal addresses are refused unless allowed) or `ai` (local
inference through Ollama); `remote landscape` lists every host's offers.
Paglets use them within policy rules for the services `web` and `ai`:

```bash
$H serve --key gw.key --ledger ledger-gw --state state-gw --web --join https://lab-1:7443 &
$H serve --key mac.key --ledger ledger-mac --state state-mac --ai ollama --ai-model llama3.2 \
    --join https://lab-1:7443 &
$H ledger rule --ledger ledger --admin alice.key --name downloads --decision allow \
    --service web --op download --root example.org --path "files/**"
$H remote landscape --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Moving a paglet as a memory image between processes or hosts (the module
must be the same file on both sides; images are independent of CPU
architecture and operating system):

```bash
build/macos-arm64/host/paglets-spike image-save build/macos-arm64/guests/counter.wasm counter.pgimg --increments 5
build/macos-arm64/host/paglets-spike image-resume build/macos-arm64/guests/counter.wasm counter.pgimg --increments 2 --expect 18
```

Worker execution requires `paglets-worker` beside `paglets-host`. Missing,
non-executable or unresponsive workers cause startup to fail; guests do not
silently fall back into the host process. `--in-process` explicitly selects
that mode. A requested worker sandbox must also initialize successfully;
`--no-sandbox` explicitly disables it.
