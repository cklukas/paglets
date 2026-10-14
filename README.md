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

## Quick start

Install the release on macOS:

```bash
brew install cklukas/paglets/paglets
```

Or build it: macOS with Homebrew (Linux and Windows: see
[Building and testing](docs/building.md)):

```bash
brew install gcc@16 llvm lld wasi-libc wasi-runtimes cmake ninja libsodium openssl@3 zstd pkgconf
cd cpp
cmake --preset macos-arm64
cmake --build --preset macos-arm64
ctest --preset macos-arm64
```

Run a paglet:

```bash
cpp/build/macos-arm64/host/paglets-host run cpp/build/macos-arm64/guests/hello.wasm --call greet '"world"'
```

Look at a running mesh in the browser, send paglets commands and launch
modules ([The web UI](docs/web-ui.md)):

```bash
cpp/build/macos-arm64/web/paglets-web --connect https://lab-1:7443 --key olga.key --ledger ledger
```

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
- [Zombie paglets](docs/zombie-paglets.md), which keep running while one host
  of the mesh is alive, and [the zombie protocol](docs/zombie-protocol.md)
  behind them, checked by `paglets-protocol-check`
- [Example paglets](docs/examples/index.md)

Build and check the site with
[ckdocs](https://github.com/cklukas/ck-git-hosting) from the repository root:

```bash
ckdocs check --root .
ckdocs serve --root .
```

## Repository layout

```text
cpp/        the C++ edition: host, guest SDK, examples, tests (see cpp/README.md)
docs/       user documentation of the C++ edition
planning/   design documents and work packages
```

## License

MIT. See [LICENSE](LICENSE).
