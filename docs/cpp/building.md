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
| `PAGLETS_BUILD_TESTS` | `ON` | build `paglets_tests` and register the CTest tests |
| `PAGLETS_PREBUILT_GUEST_DIR` | empty | directory of prebuilt guest `.wasm` files, used by the tests when guests are not built here |
| `PAGLETS_WASI_CLANG`, `PAGLETS_WASI_SYSROOT` | found automatically | the guest compiler and its WASI sysroot (see [Guest modules](#guest-modules)) |
| `PAGLETS_WAMR_FAST_INTERP` | `ON` | WAMR fast interpreter (`OFF`: the classic interpreter) |
| `PAGLETS_WAMR_HW_BOUND_CHECK` | `ON` (`OFF` with MinGW-w64) | guard-page bound checks. See the note below. |
| `PAGLETS_SANITIZE` | `OFF` | AddressSanitizer and UndefinedBehaviorSanitizer |
| `PAGLETS_SANITIZE_THREAD` | `OFF` | ThreadSanitizer |

> [!NOTE]
> On kernels with a 39-bit address space, such as Raspberry Pi OS, turn
> `PAGLETS_WAMR_HW_BOUND_CHECK` off. With it on, a process can run only
> about 60 instances. MinGW-w64 GCC cannot build the guard-page checks at
> all, because WAMR catches the faults with MSVC `__try`/`__except`.

`paglets-host --info` shows what a build contains: version, platform,
compiler, whether reflection is on, the WAMR release and mode, the paglet
ABI and the mesh protocol.

## Guest modules

`cpp/cmake/PagletsGuest.cmake` builds guest modules with custom commands,
because the guest compiler is not the host compiler. It looks for the guest
toolchain in this order:

- `PAGLETS_WASI_CLANG` and `PAGLETS_WASI_SYSROOT`, if set;
- `$WASI_SDK_PATH/bin/clang` and `$WASI_SDK_PATH/share/wasi-sysroot`;
- `/opt/wasi-sdk/bin/clang` and `/opt/wasi-sdk/share/wasi-sysroot`;
- Homebrew's clang (`/opt/homebrew/opt/llvm/bin/clang` or
  `/usr/local/opt/llvm/bin/clang`) and a sysroot in
  `/opt/homebrew/share/wasi-sysroot`, `/usr/local/share/wasi-sysroot` or
  `/usr/share/wasi-sysroot`.

The compiler and the sysroot are searched independently; C++ sources are
compiled with the `clang++` next to the chosen `clang`.

It then compiles and links a small C++ probe for `wasm32-wasip1`. Guests are
built only if the probe succeeds. Otherwise the configure step warns and
the build goes on without guests. See
[Configuration and troubleshooting](configuration.md#guests-are-not-built).

Two functions add guests:

| Function | Builds |
|---|---|
| `paglets_add_module(NAME SOURCES ... [SCHEMA_HEADER H SCHEMA_NAMESPACE NS] [SERVICES ...] [PATTERNS])` | a C++ paglet with the guest SDK |
| `paglets_add_guest(NAME SOURCES ... [LANGUAGE L] [NO_SDK] ...)` | the general form, with `L` either `C` or `CXX` (the default); `paglets_add_module` calls it with `LANGUAGE CXX` |

Every module ends up in `build/<preset>/guests/NAME.wasm`. With a schema
header, the build also writes `NAME.schema.json` there and the generated
`NAME.schema.gen.hpp` under `build/<preset>/generated/NAME/`.

C++ guests are compiled with `-std=c++23 -fno-exceptions -fno-rtti`. C
guests (`LANGUAGE C`) are compiled with `-std=c17` and never get the SDK,
which is C++. A C module must therefore export the paglet ABI itself, or
it is not a paglet at all. The test suite builds two such modules:

```cmake
paglets_add_guest(testbed LANGUAGE C NO_SDK SOURCES guests/testbed.c)
paglets_add_guest(forbidden LANGUAGE C NO_SDK SOURCES guests/forbidden.c)
```

`testbed` exports plain functions for the engine tests (traps, endless
loops, memory growth), and `forbidden` imports functions a roaming paglet
may not have. The exports and imports a paglet needs are in the
[ABI specification](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-abi-v1.md).
`paglets-host module inspect FILE.wasm` shows what a module exports and
imports, and whether it is a paglet.

### Modules outside the repository

The guest functions are not installed or exported. They use the paglets/cpp
source tree (`PROJECT_SOURCE_DIR`) and its targets: the schema generator is
built from `cpp/tools/schema_gen/` and linked against `paglets_host`. So
they work only inside the paglets/cpp CMake project. To build your own
paglets with them, add their sources and a `paglets_add_module(...)` call
to `cpp/examples/CMakeLists.txt`, as the demos do.

A module that needs no generated code (no `SCHEMA_HEADER`, `SERVICES` or
`PATTERNS`) can also be built outside CMake. These are the flags that
`PagletsGuest.cmake` passes:

```bash
P=path/to/paglets/cpp
clang++ --target=wasm32-wasip1 --sysroot=$WASI_SYSROOT -O2 -mexec-model=reactor \
    -Wl,--export=__stack_pointer -Wl,--strip-debug -std=c++23 -fno-exceptions -fno-rtti \
    -I$P/common/include -I$P/sdk/include $P/sdk/src/paglet.cpp hello.cpp -o hello.wasm
```

## The test suite

`ctest` runs these tests (`cpp/tests/CMakeLists.txt`):

| Test | What it runs |
|---|---|
| `unit` | `paglets_tests`, the unit, conformance and multi-host tests, with paglets inside the test process |
| `unit_workers` | the same tests with paglets in sandboxed worker processes (`PAGLETS_TEST_WORKER` names `paglets-worker`) |
| `worker_sandbox` | `paglets-worker --check-sandbox`: the operating-system sandbox refuses what it must |
| `image_save`, `image_resume` | `paglets-spike` saves a counter paglet's memory image in one process and resumes it in another |
| `host_run_hello`, `host_run_services` | `paglets-host run` with the hello paglet, without and with a named root |
| `host_state_clean`, `host_state_save`, `host_state_resume` | a paglet survives its host process in a state directory (`run --keep`, then `call ... all`) |
| `host_serve_move` | two `paglets-host serve` processes; a paglet is launched with `remote launch` and moved back and forth |
| `host_mesh_discovery` | several `serve` processes that each know one contact find each other, including a host without an inbound port |
| `host_cli_ledger` | keys, mesh creation, enrollment requests and admin decisions with the CLI |

The image, `host_run_*` and `host_state_*` tests need guest modules (built
or from `PAGLETS_PREBUILT_GUEST_DIR`). `host_serve_move` and
`host_mesh_discovery` also need reflection.

The multi-host tests inside `paglets_tests` run several hosts in one
process. They cover the mesh, moves, location, relays, the demos and the
gateways, connected by an in-memory transport or by HTTPS on loopback.

`paglets_tests` takes the guest directory and, optionally, a text. It then
runs only the tests whose names contain that text:

```bash
cd build/linux-gcc16
./tests/paglets_tests guests "demo:"
PAGLETS_TEST_WORKER=$PWD/host/paglets-worker ./tests/paglets_tests guests "demo:"
```

Environment variables of the test suite:

| Variable | Effect |
|---|---|
| `PAGLETS_TEST_WORKER` | runs paglets in this `paglets-worker` executable instead of in the test process |
| `PAGLETS_TEST_LOG` | prints the log lines of the runtimes under test (paglet and host messages) to stderr |
| `PAGLETS_PARTITION_SEED` | the seed of the random ledger partition test (`gossip: random partitions ...`); a failing run prints its seed |
| `PAGLETS_FUZZ_SECONDS`, `PAGLETS_FUZZ_SEED`, `PAGLETS_FUZZ_REPLAY` | see [Fuzzing](#fuzzing) |
| `PAGLETS_BENCH`, `PAGLETS_BENCH_JSON` | see [Benchmarks](#benchmarks) |

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
