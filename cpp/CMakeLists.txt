# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

cmake_minimum_required(VERSION 3.28)

project(paglets_cpp
    VERSION 0.1.0
    DESCRIPTION "paglets/cpp: mobile Wasm paglets with memory-image mobility"
    LANGUAGES C CXX ASM)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(PAGLETS_BUILD_GUESTS "Build guest paglets (needs a WASI clang toolchain)" ON)
option(PAGLETS_BUILD_TESTS "Build tests" ON)
option(PAGLETS_SANITIZE "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
set(PAGLETS_PREBUILT_GUEST_DIR "" CACHE PATH
    "Directory with prebuilt guest .wasm files, used when guests are not built here")

# C++26 reflection (GCC 16: -freflection). Reflection is confined to the wire
# and schema layer; the rest of the host also builds without it.
include(CheckCXXSourceCompiles)
set(CMAKE_REQUIRED_FLAGS "-std=c++26 -freflection")
check_cxx_source_compiles("
#include <meta>
struct S { int a; };
static_assert(std::meta::nonstatic_data_members_of(^^S, std::meta::access_context::unchecked()).size() == 1);
int main() {}" PAGLETS_COMPILER_HAS_REFLECTION)
unset(CMAKE_REQUIRED_FLAGS)
option(PAGLETS_ENABLE_REFLECTION "Use C++26 reflection for wire codecs and the schema generator"
       ${PAGLETS_COMPILER_HAS_REFLECTION})
if(PAGLETS_ENABLE_REFLECTION AND NOT PAGLETS_COMPILER_HAS_REFLECTION)
    message(FATAL_ERROR "paglets: PAGLETS_ENABLE_REFLECTION requires a compiler with -freflection (GCC 16)")
endif()
message(STATUS "paglets: C++26 reflection ${PAGLETS_ENABLE_REFLECTION}")

set(PAGLETS_WARNINGS -Wall -Wextra -Wshadow -Wnon-virtual-dtor -Wno-missing-field-initializers)
if(PAGLETS_SANITIZE)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
include(PagletsWamr)
include(PagletsGuest)

add_subdirectory(host)
add_subdirectory(examples)

if(PAGLETS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
