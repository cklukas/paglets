# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

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

# A plain paglet: the guest SDK and SHA-256 (<paglets/sha256.hpp>).
paglets_add_module(greeter SOURCES greeter.cpp SHA256)

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
