# paglets/cpp

C++ implementation of paglets: mobile agents as WebAssembly modules, run by
the embedded [WAMR](https://github.com/bytecodealliance/wasm-micro-runtime)
runtime, moving between hosts as **memory images**. The design and work
packages are in [`../planning/`](../planning/cpp-edition-plan.md).

Status: **milestone M0 (feasibility)**: WP0 project setup, WP1 WAMR spike and
WP2 toolchain and reflection spike. Results and measurements:
[`../planning/cpp-m0-results.md`](../planning/cpp-m0-results.md).

## Layout

```text
common/include/paglets/   MessagePack primitives shared by host and guests
host/                     host library (Wasm engine, memory images, wire codecs), paglets-host, paglets-spike
sdk/                      guest SDK (paglet ABI v0 exports and host imports)
tools/schema_gen/         guest schema generator (C++26 reflection -> plain C++)
examples/counter/         example paglet
tests/                    unit tests and test guests
cmake/                    WAMR build and guest build functions
```

## Toolchains

| Part | Compiler | Why |
|---|---|---|
| Host | GCC 16 with `-std=c++26 -freflection` | C++26 reflection for wire codecs and the schema generator |
| Host without reflection | any C++23 compiler (GCC 14, Apple clang) | everything except the reflection codecs and guest schema generation |
| Guests | clang targeting `wasm32-wasip1` with a WASI sysroot | the only C/C++ compiler for Wasm; no reflection, so guest schema code is generated |

macOS (Homebrew):

```bash
brew install gcc@16 llvm lld wasi-libc wasi-runtimes cmake ninja
```

Linux: GCC 16 plus [wasi-sdk](https://github.com/WebAssembly/wasi-sdk) in
`/opt/wasi-sdk` (or set `WASI_SDK_PATH`). The guest toolchain can also be
given explicitly with `-DPAGLETS_WASI_CLANG=... -DPAGLETS_WASI_SYSROOT=...`.

## Build and test

From this directory:

```bash
cmake --preset macos-arm64
cmake --build --preset macos-arm64
ctest --preset macos-arm64
```

Other presets: `linux-gcc16`, `windows-mingw-gcc16`, sanitizer variants
`macos-arm64-asan` and `linux-gcc16-asan`, and `linux-host-only` for hosts
without GCC 16 or a Wasm toolchain (reflection off, guests taken from
`-DPAGLETS_PREBUILT_GUEST_DIR=...`; Wasm modules are platform-independent, so
guests built anywhere can be used).

Build options:

| Option | Default | Meaning |
|---|---|---|
| `PAGLETS_ENABLE_REFLECTION` | on if the compiler supports it | reflection codecs and guest schema generation |
| `PAGLETS_BUILD_GUESTS` | `ON` | build guest `.wasm` modules (needs the WASI toolchain) |
| `PAGLETS_WAMR_FAST_INTERP` | `ON` | WAMR fast interpreter (`OFF`: classic interpreter) |
| `PAGLETS_WAMR_HW_BOUND_CHECK` | `ON` | guard-page bound checks; turn `OFF` on kernels with a 39-bit address space (Raspberry Pi OS) to run more than ~60 instances per process |
| `PAGLETS_SANITIZE` | `OFF` | AddressSanitizer and UndefinedBehaviorSanitizer |

## Tools

```bash
build/macos-arm64/host/paglets-host --info
build/macos-arm64/host/paglets-spike info build/macos-arm64/guests/counter.wasm
build/macos-arm64/host/paglets-spike bench build/macos-arm64/guests/testbed.wasm build/macos-arm64/guests/counter.wasm
```

Moving a paglet as a memory image between processes or hosts (the module
must be the same file on both sides; images are independent of CPU
architecture and operating system):

```bash
build/macos-arm64/host/paglets-spike image-save build/macos-arm64/guests/counter.wasm counter.pgimg --increments 5
build/macos-arm64/host/paglets-spike image-resume build/macos-arm64/guests/counter.wasm counter.pgimg --increments 2 --expect 18
```
