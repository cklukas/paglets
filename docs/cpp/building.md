# Building and testing

paglets/cpp builds with CMake presets from the `cpp/` directory of the `cpp`
branch. The build fetches WAMR and cpp-httplib at pinned releases.

## Toolchains

| Part | Compiler | Why |
|---|---|---|
| Host | GCC 16 with `-std=c++26 -freflection` | C++26 reflection for wire codecs and the schema generator |
| Host without reflection | a C++23 compiler whose standard library has `<expected>` | everything except the reflection codecs and guest schema generation |
| Guests | clang targeting `wasm32-wasip1` with a WASI sysroot | the only C/C++ compiler for Wasm; it has no reflection, so guest schema code is generated |

macOS (Homebrew):

```bash
brew install gcc@16 llvm lld wasi-libc wasi-runtimes cmake ninja libsodium openssl@3 zstd pkgconf
```

Linux:

- GCC 16;
- [wasi-sdk](https://github.com/WebAssembly/wasi-sdk) in `/opt/wasi-sdk`, or
  set `WASI_SDK_PATH`;
- libsodium 1.0.18 or later, OpenSSL 3 and zstd:

  ```bash
  apt install libsodium-dev libssl-dev libzstd-dev pkg-config
  ```

Windows (MSYS2 UCRT64): `mingw-w64-ucrt-x86_64-libsodium`,
`mingw-w64-ucrt-x86_64-openssl`, `mingw-w64-ucrt-x86_64-zstd` and
`mingw-w64-ucrt-x86_64-pkgconf`.

You can also name the guest toolchain explicitly with
`-DPAGLETS_WASI_CLANG=...` and `-DPAGLETS_WASI_SYSROOT=...`.

## Build and test

```bash
cd cpp
cmake --preset linux-gcc16
cmake --build --preset linux-gcc16
ctest --preset linux-gcc16
```

| Preset | Use |
|---|---|
| `linux-gcc16`, `macos-arm64`, `windows-mingw-gcc16` | the regular builds |
| `linux-gcc16-asan`, `macos-arm64-asan` | AddressSanitizer and UndefinedBehaviorSanitizer |
| `linux-gcc16-tsan` | ThreadSanitizer |
| `linux-host-only` | hosts without GCC 16 or a Wasm toolchain (see below) |

`linux-host-only` builds the host without reflection. It takes its guests
from `-DPAGLETS_PREBUILT_GUEST_DIR=...`. Wasm modules are
platform-independent, so guests built on any machine work there.

| Option | Default | Meaning |
|---|---|---|
| `PAGLETS_ENABLE_REFLECTION` | on if the compiler supports it | reflection codecs and guest schema generation |
| `PAGLETS_BUILD_GUESTS` | `ON` | build the guest `.wasm` modules (needs the WASI toolchain) |
| `PAGLETS_WAMR_FAST_INTERP` | `ON` | WAMR fast interpreter (`OFF`: the classic interpreter) |
| `PAGLETS_WAMR_HW_BOUND_CHECK` | `ON` (`OFF` with MinGW-w64) | guard-page bound checks. See the note below. |
| `PAGLETS_SANITIZE` | `OFF` | AddressSanitizer and UndefinedBehaviorSanitizer |
| `PAGLETS_SANITIZE_THREAD` | `OFF` | ThreadSanitizer |

> [!NOTE]
> On kernels with a 39-bit address space, such as Raspberry Pi OS, turn
> `PAGLETS_WAMR_HW_BOUND_CHECK` off. With it on, a process can run only
> about 60 instances. MinGW-w64 GCC cannot build the guard-page checks at
> all, because WAMR catches the faults with MSVC `__try`/`__except`.

## The test suite

`ctest` runs the unit and conformance tests twice:

- once with paglets inside the test process;
- once with paglets in sandboxed worker processes (`PAGLETS_TEST_WORKER`
  names `paglets-worker`).

Multi-host tests run several hosts in one process. They cover the mesh,
moves, location, relays, the demos and the gateways, connected by an
in-memory transport or by HTTPS on loopback.

To run one group by name, call the test binary from the build directory:

```bash
cd build/linux-gcc16
./tests/paglets_tests guests "demo:"
PAGLETS_TEST_WORKER=$PWD/host/paglets-worker ./tests/paglets_tests guests "demo:"
```

## Fuzzing

The `fuzz:` tests mutate valid inputs for every decoder of data from
outside:

- MessagePack and mesh values;
- ledger records and passports;
- travelling state, capabilities and remote messages;
- Wasm modules and memory images;
- contracts;
- the Noise handshake;
- JSON, URLs and HTML;
- frames between hosts.

In CI they run briefly. To work with them locally:

- `PAGLETS_FUZZ_SECONDS` sets how long each target runs;
- `PAGLETS_FUZZ_SEED` repeats the run from a reported seed;
- `PAGLETS_FUZZ_REPLAY` decodes a saved input.

See the
[hardening design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-hardening.md).

## Benchmarks

The `bench:` tests measure:

- request and reply to an active paglet;
- the activation of an inactive one;
- memory per paglet;
- moves between hosts, with page reuse.

In CI they run as short smoke tests. To run them at full size:

```bash
PAGLETS_BENCH=1 ./tests/paglets_tests guests "bench:"
```

`PAGLETS_BENCH_JSON=FILE` appends one JSON object per benchmark to `FILE`.
The numbers are in [Demos and benchmarks](demos.md#benchmarks).
