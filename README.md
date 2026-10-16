# paglets/cpp

[![C++](https://github.com/cklukas/paglets/actions/workflows/cpp.yml/badge.svg?branch=cpp)](https://github.com/cklukas/paglets/actions/workflows/cpp.yml)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

paglets/cpp is the C++ edition of paglets: mobile agents written in C++,
compiled to WebAssembly and run by the embedded
[WAMR](https://github.com/bytecodealliance/wasm-micro-runtime) runtime. A
paglet's state is ordinary C++ objects in its linear memory, and it moves
between hosts as a **memory image** (only the pages that changed travel
again). Hosts form a mesh with keys, a signed ledger, a policy for every
access to host resources, sandboxed worker processes and code mobility.

This branch (`cpp`) holds only the C++ edition. The Python edition, the
`paglets` package on PyPI, lives on the
[`main` branch](https://github.com/cklukas/paglets/tree/main). The two
editions share concepts but no wire formats, and do not exchange paglets.

Requests retain their expiry through mesh forwarding; timed deactivation
wakes survive stored recovery and movement. Clones discard inherited SDK
reply waits while the original keeps them. See the
[SDK reference](docs/sdk-reference.md) for these lifecycle rules and the
[remaining issues](planning/cpp-paper-audit.md) for outstanding work.

New root identities bind the owner, mesh and launch nonce. Mesh protocol 2
requires upgraded hosts and clients together; saved paglets keep their IDs
and can recover and move. `remote launch --nonce` replaces `--id`; the host
C++ launch/passport APIs take a nonce too. Web launch forms use the same
nonce and show the derived global ID. See [identity upgrades](docs/hosts-and-meshes.md#paglet-identities-and-upgrades).

Moves preserve replies already waiting for their SDK continuations, host
reports and one-shot timers, including the timer message kind. A move that
exceeds its notification or transport limit is refused without consuming
those deliveries. Outstanding request budgets also count time spent moving;
peers that cannot restore their timing metadata refuse the move. See [movement limits](docs/configuration.md).

Module admission validates function import names and kinds; matching a
permitted name cannot authorize a non-function import.

## Quick start

Paglets allow up to 256 MB of linear memory by default; compute jobs allow
up to 1 GB. Instances start with their module's initial memory and grow
as needed. `serve --memory-mb N` changes the paglet ceiling; configure move
destinations too. Compute-job limits are separate.
For the separate `compute-slots` service, release an existing lease before
changing its job or core count.
Leases and native jobs share admission priority: later work leaves capacity
for an older waiting request as running allocations finish.
Leases expire after 10 minutes by default, extended for longer estimates;
renewal stays within a 24-hour continuous limit.
`remote moves` reports transfer counters and phase timings; the live
benchmark script measures cold and return moves between real hosts. Partial transfer runs can reuse a matching
TCP baseline with its original measurement date. Live records use actual
host builds and confirmed compute-slot allocations.

Install the release on macOS:

```bash
brew install cklukas/paglets/paglets
```

To build your own paglets against the installed package, add the guest
toolchain (host-only users do not need it):

```bash
brew install llvm lld wasi-libc wasi-runtimes
```

Guest CMake builds track included SDK and local headers, so header edits
rebuild affected modules automatically.
The tests also include a plain C paglet that implements the ABI without
the C++ guest SDK (`cpp/tests/guests/plain_counter.c`).

Or build it: macOS with Homebrew (Linux and Windows: see
[Building and testing](docs/building.md)):

```bash
brew install gcc@16 llvm lld wasi-libc wasi-runtimes cmake ninja libsodium openssl@3 zstd pkgconf
cd cpp
cmake --preset macos-arm64
cmake --build --preset macos-arm64
ctest --preset macos-arm64
cd ..
```

The test executable supports `--list [FILTER]` to show its configured cases
without running them. An unmatched filter exits with status 2.

ASan/UBSan presets use `-g -O1` and make undefined-behavior reports fail CTest.

Run a paglet:

```bash
cpp/build/macos-arm64/host/paglets-host run cpp/build/macos-arm64/guests/hello.wasm --call greet '"world"'
```

Look at a running mesh in the browser, send paglets commands and launch
modules ([The web UI](docs/web-ui.md)):

```bash
cpp/build/macos-arm64/web/paglets-web --connect https://lab-1:7443 --key olga.key --ledger ledger
```

When creating a mesh with three or more initial admins, `mesh create`
defaults to two signatures for admin-set changes. Other admin records
default to one; explicit quorum options override these defaults.

`paglets-host remote call --timeout SECONDS` sets the reply deadline
(default 30 s, range 0.1–120 s, up to three decimal places). The web UI
includes a custom timeout in its generated shell command.

Remote CLI commands reject malformed host replies with a clear error
before printing their results.

Host addresses give full key IDs precedence over display names. Unknown
full IDs never resolve as names, and ambiguous names or permitted key-ID
prefixes are refused. Use full key IDs for unambiguous placement.

Host-tool messages use sender `trust: host`; authenticated remote calls
and sends carry the acting owner's or admin's key ID in `sender.owner`.
Native services keep `trust: system`.

Linux ARM64 hardware-check builds warn at startup when a virtual
address-space probe fails; the warning names the software bounds-check
build option. This is separate from the paglet and job RAM ceilings.

Remote asynchronous sends report receiving-host refusals through
`on_undelivered`, including the original endpoint handle. Reports follow
moving senders; transport loss and old peers remain best effort.

## Documentation

The documentation site has two parts, selected in its header: the C++
edition (from [`docs/`](docs/index.md) on this branch) and the Python edition
(from `docs/` on `main`). Pages on this branch:

- [Overview](docs/index.md)
- [Building and testing](docs/building.md)
- [Writing paglets](docs/writing-paglets.md) and the
  [guest SDK reference](docs/sdk-reference.md)
- [Hosts and meshes](docs/hosts-and-meshes.md) and the
  [command-line reference](docs/cli-reference.md)
- [The web UI](docs/web-ui.md) (`paglets-web`) and
  [interface descriptions](docs/interface-descriptions.md) of modules
- [Configuration and troubleshooting](docs/configuration.md)
- [System paglets](docs/system-paglets.md)
- [Compute jobs](docs/compute-jobs.md): long computations in fresh Wasm instances,
  local or remote, with cancellation, artifacts and optional AOT compilation
- [Zombie paglets](docs/zombie-paglets.md), which keep running while one host
  of the mesh is alive, and [the zombie protocol](docs/zombie-protocol.md)
  behind them, checked by `paglets-protocol-check`
  (permanent result refusal ends the group with a failure notification)
- [Example paglets](docs/examples/index.md)
- A paper on the C++ edition is in preparation on the `cpp-paper` branch

Build and check the site with
[ckdocs](https://github.com/cklukas/ck-git-hosting) from the repository root:

```bash
ckdocs check --root .
ckdocs serve --root .
```

Revocations apply to established mesh channels once the host learns them,
including relay and polling paths; bootstrap seeds and explicit host pins cannot bypass a known revocation.
Chronicle's startup reply reports whether the creator joined its group, with
a reason on refusal; other replicas form asynchronously. Inventory's
`report` shows collection progress after dispatch startup.

## Repository layout

```text
cpp/        the C++ edition: host, guest SDK, examples, tests (see cpp/README.md)
docs/       user documentation of the C++ edition
planning/   design documents and work packages
```


Worker execution requires `paglets-worker` beside `paglets-host`. Missing,
non-executable or unresponsive workers cause startup to fail; guests do not
silently fall back into the host process. `--in-process` explicitly selects
that mode. A requested worker sandbox must also initialize successfully;
`--no-sandbox` explicitly disables it.

Sandboxed Windows workers use a distinct LPAC without network capabilities,
a restricted token and a job object. Worker launches omit service environment
variables and unrelated handles; see [worker sandbox limits](docs/configuration.md#worker-sandbox-limits).

## License

MIT. See [LICENSE](LICENSE).
