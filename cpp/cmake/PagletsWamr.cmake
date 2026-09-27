# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Fetches WAMR (WebAssembly Micro Runtime) at a pinned release and builds it
# as the static library paglets_vmlib, configured for paglets:
#   - interpreter (fast interpreter by default, classic as an option),
#   - WASI libc (the host grants no directories, arguments or environment),
#   - thread manager (needed to terminate a running instance from another thread),
#   - reference types and bulk memory (default features of current clang).

include(FetchContent)

set(PAGLETS_WAMR_TAG "WAMR-2.4.5" CACHE STRING "WAMR release tag")
option(PAGLETS_WAMR_FAST_INTERP "Use the WAMR fast interpreter (OFF: classic interpreter)" ON)
# Hardware bound checks reserve 8 GB of virtual address space per instance and
# use guard pages instead of explicit checks. With a 39-bit address space (for
# example Raspberry Pi OS kernels) that limits a process to about 60 instances;
# software bound checks have no such limit.
# On Windows WAMR catches guard-page faults with MSVC structured exception
# handling (__try/__except), which MinGW-w64 GCC does not support.
if(MINGW)
    set(_paglets_hw_bound_default OFF)
else()
    set(_paglets_hw_bound_default ON)
endif()
option(PAGLETS_WAMR_HW_BOUND_CHECK "Use guard-page (hardware) memory bound checks" ${_paglets_hw_bound_default})

FetchContent_Declare(
    wamr
    GIT_REPOSITORY https://github.com/bytecodealliance/wasm-micro-runtime.git
    GIT_TAG ${PAGLETS_WAMR_TAG}
    GIT_SHALLOW TRUE
    PATCH_COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_LIST_DIR}/patch-wamr.cmake
    # WAMR's own top-level project is not used; runtime_lib.cmake is included below.
    SOURCE_SUBDIR paglets-no-cmake-project)
FetchContent_MakeAvailable(wamr)

set(WAMR_ROOT_DIR ${wamr_SOURCE_DIR})

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(WAMR_BUILD_PLATFORM "darwin")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(WAMR_BUILD_PLATFORM "windows")
else()
    set(WAMR_BUILD_PLATFORM "linux")
endif()

# On macOS the target architecture can differ from the build machine
# (for example an x86-64 build run under Rosetta 2).
set(_paglets_arch "${CMAKE_SYSTEM_PROCESSOR}")
if(APPLE AND CMAKE_OSX_ARCHITECTURES)
    set(_paglets_arch "${CMAKE_OSX_ARCHITECTURES}")
endif()
if(_paglets_arch MATCHES "^(arm64|aarch64|ARM64)$")
    set(WAMR_BUILD_TARGET "AARCH64")
elseif(_paglets_arch MATCHES "^(x86_64|AMD64|amd64)$")
    set(WAMR_BUILD_TARGET "X86_64")
else()
    message(FATAL_ERROR "paglets: unsupported processor ${_paglets_arch}")
endif()

set(WAMR_BUILD_INTERP 1)
if(PAGLETS_WAMR_FAST_INTERP)
    set(WAMR_BUILD_FAST_INTERP 1)
else()
    set(WAMR_BUILD_FAST_INTERP 0)
endif()
set(WAMR_BUILD_AOT 0)
set(WAMR_BUILD_JIT 0)
set(WAMR_BUILD_FAST_JIT 0)
set(WAMR_BUILD_LIBC_BUILTIN 0)
set(WAMR_BUILD_LIBC_WASI 1)
set(WAMR_BUILD_THREAD_MGR 1)
set(WAMR_BUILD_LIB_PTHREAD 0)
set(WAMR_BUILD_LIB_WASI_THREADS 0)
set(WAMR_BUILD_SHARED_MEMORY 0)
set(WAMR_BUILD_MULTI_MODULE 0)
set(WAMR_BUILD_MINI_LOADER 0)
set(WAMR_BUILD_REF_TYPES 1)
set(WAMR_BUILD_BULK_MEMORY 1)
set(WAMR_BUILD_SIMD 0)
set(WAMR_BUILD_DUMP_CALL_STACK 0)
if(NOT PAGLETS_WAMR_HW_BOUND_CHECK)
    set(WAMR_DISABLE_HW_BOUND_CHECK 1)
endif()

include(${WAMR_ROOT_DIR}/build-scripts/runtime_lib.cmake)

# WAMR's invokeNative_*.s files use C preprocessor conditionals (for example
# BH_PLATFORM_DARWIN). Clang preprocesses them implicitly, GCC does not.
foreach(src IN LISTS WAMR_RUNTIME_LIB_SOURCE)
    if(src MATCHES "\\.s$")
        set_source_files_properties(${src} PROPERTIES COMPILE_OPTIONS "-x;assembler-with-cpp")
    endif()
endforeach()

add_library(paglets_vmlib STATIC ${WAMR_RUNTIME_LIB_SOURCE})
target_include_directories(paglets_vmlib PUBLIC ${WAMR_ROOT_DIR}/core/iwasm/include)
target_compile_options(paglets_vmlib PRIVATE $<$<COMPILE_LANGUAGE:C>:-w>)
if(PAGLETS_SANITIZE)
    # The fast interpreter reads and writes its compiled bytecode unaligned by
    # design; keep ASan and the other UBSan checks, skip the alignment check.
    target_compile_options(paglets_vmlib PRIVATE $<$<COMPILE_LANGUAGE:C>:-fno-sanitize=alignment>)
    # WAMR detects native stack overflow by comparing the address of a local
    # variable with the thread's stack boundary. ASan's use-after-return
    # detection (on by default in GCC 16's libasan) moves locals to a heap
    # "fake stack", so every call looked like an overflow. Keep locals of the
    # runtime on the real stack; host code keeps the check.
    include(CheckCCompilerFlag)
    set(CMAKE_REQUIRED_FLAGS -fsanitize=address)
    check_c_compiler_flag(-fsanitize-address-use-after-return=never PAGLETS_HAS_ASAN_UAR_NEVER)
    unset(CMAKE_REQUIRED_FLAGS)
    if(PAGLETS_HAS_ASAN_UAR_NEVER)
        target_compile_options(paglets_vmlib PRIVATE -fsanitize-address-use-after-return=never)
    else()
        target_compile_options(paglets_vmlib PRIVATE --param=asan-use-after-return=0)
    endif()
endif()
find_package(Threads REQUIRED)
target_link_libraries(paglets_vmlib PUBLIC Threads::Threads)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(paglets_vmlib PUBLIC m dl)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    target_link_libraries(paglets_vmlib PUBLIC ws2_32 ntdll)
endif()

if(PAGLETS_WAMR_FAST_INTERP)
    set(PAGLETS_WAMR_MODE "fast-interp")
else()
    set(PAGLETS_WAMR_MODE "classic-interp")
endif()
if(PAGLETS_WAMR_HW_BOUND_CHECK)
    string(APPEND PAGLETS_WAMR_MODE ", hardware bound checks")
else()
    string(APPEND PAGLETS_WAMR_MODE ", software bound checks")
endif()
message(STATUS "paglets: WAMR ${PAGLETS_WAMR_TAG}, ${WAMR_BUILD_PLATFORM}/${WAMR_BUILD_TARGET}, ${PAGLETS_WAMR_MODE}")
