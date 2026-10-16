# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# The installed package used from outside the repository: installs this
# build, builds examples/out-of-tree against it with find_package(paglets)
# and runs the modules with the installed paglets-host.
#   cmake -DBUILD=<paglets build dir> -DCONFIG=<config> -DDIR=<work directory>
#         -DSOURCE=<examples/out-of-tree> -DGENERATOR=<generator>
#         -DCXX=<C++ compiler> [-DMAKE=<make program>] [-DWASI_CLANG=...] [-DWASI_SYSROOT=...]
#         -P cli_install.cmake

foreach(var BUILD DIR SOURCE GENERATOR CXX)
    if(NOT ${var})
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()
file(REMOVE_RECURSE "${DIR}")
set(prefix "${DIR}/prefix")
set(consumer "${DIR}/consumer")

function(run)
    execute_process(COMMAND ${ARGN} RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    string(STRIP "${out}" out)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "failed (${rc}): ${ARGN}\n${out}\n${err}")
    endif()
    set(OUT "${out}" PARENT_SCOPE)
endfunction()

function(expect text)
    string(FIND "${OUT}" "${text}" at)
    if(at EQUAL -1)
        message(FATAL_ERROR "expected '${text}' in:\n${OUT}")
    endif()
endfunction()

set(config_args "")
if(CONFIG)
    set(config_args --config "${CONFIG}")
endif()
run("${CMAKE_COMMAND}" --install "${BUILD}" --prefix "${prefix}" ${config_args})
foreach(file bin/paglets-host lib/cmake/paglets/pagletsConfig.cmake include/paglets/paglet.hpp
             share/paglets/sdk/sha256.cpp
             include/paglets/services/server_info.gen.hpp)
    if(NOT EXISTS "${prefix}/${file}" AND NOT EXISTS "${prefix}/${file}.exe")
        message(FATAL_ERROR "not installed: ${file}")
    endif()
endforeach()

# A fresh configure: only the installed package, no paglets source tree.
set(consumer_source "${DIR}/source")
file(COPY "${SOURCE}/" DESTINATION "${consumer_source}")
file(READ "${consumer_source}/greeter.cpp" greeter_source)
string(REPLACE "\"greetings, \"" "paglets_dependency_greeting" greeter_source "${greeter_source}")
file(WRITE "${consumer_source}/greeter.cpp" "#include \"greeter_dependency.hpp\"\n${greeter_source}")
file(WRITE "${consumer_source}/greeter_dependency.hpp"
    "inline constexpr char paglets_dependency_greeting[] = \"greetings, \";\n")
# This shared header is deliberately absent from SOURCES. Changing it must
# rebuild a guest that includes it, including when the SDK is not linked.
set(dependency_header "${prefix}/include/paglets/dependency_probe.h")
file(WRITE "${dependency_header}" "#define PAGLETS_DEPENDENCY_VALUE 1\n")
file(WRITE "${consumer_source}/dependency_local.h" "#define PAGLETS_LOCAL_VALUE 10\n")
file(WRITE "${consumer_source}/dependency_probe.c"
    "#include <paglets/dependency_probe.h>\n"
    "#include \"dependency_local.h\"\n"
    "__attribute__((export_name(\"dependency_probe\"))) int dependency_probe(void) { return PAGLETS_DEPENDENCY_VALUE + PAGLETS_LOCAL_VALUE; }\n")
file(APPEND "${consumer_source}/CMakeLists.txt"
    "\npaglets_add_guest(dependency_probe LANGUAGE C NO_SDK SOURCES dependency_probe.c)\n")
set(configure_args -S "${consumer_source}" -B "${consumer}" -G "${GENERATOR}" "-DCMAKE_PREFIX_PATH=${prefix}"
    "-DCMAKE_CXX_COMPILER=${CXX}")
if(MAKE)
    list(APPEND configure_args "-DCMAKE_MAKE_PROGRAM=${MAKE}")
endif()
if(CONFIG)
    list(APPEND configure_args "-DCMAKE_BUILD_TYPE=${CONFIG}")
endif()
if(WASI_CLANG)
    list(APPEND configure_args "-DPAGLETS_WASI_CLANG=${WASI_CLANG}")
endif()
if(WASI_SYSROOT)
    list(APPEND configure_args "-DPAGLETS_WASI_SYSROOT=${WASI_SYSROOT}")
endif()
run("${CMAKE_COMMAND}" ${configure_args})
run("${CMAKE_COMMAND}" --build "${consumer}" ${config_args})

set(host "${prefix}/bin/paglets-host")
run("${host}" run "${consumer}/guests/greeter.wasm" --call greet "\"world\"")
expect("greetings, world, from outside the tree")
run("${host}" run "${consumer}/guests/greeter.wasm" --call digest "\"abc\"")
expect("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
run("${host}" run "${consumer}/guests/adder.wasm" --call add "{\"a\": 40, \"b\": 2}" --call host "{}")
expect("\"sum\":42")
if(NOT OUT MATCHES "\"os\":\"(macos|linux|windows)\"")
    message(FATAL_ERROR "expected the host's os in:\n${OUT}")
endif()
if(NOT EXISTS "${consumer}/guests/adder.schema.json")
    message(FATAL_ERROR "adder.schema.json was not generated")
endif()
# Interface descriptions: from the INTERFACE file, and from the contract.
run("${host}" module inspect "${consumer}/guests/greeter.wasm")
expect("digest  call  string -> string")
run("${host}" module inspect "${consumer}/guests/adder.wasm")
expect("Operation add of service")
file(SHA256 "${consumer}/guests/dependency_probe.wasm" before)
file(WRITE "${dependency_header}" "#define PAGLETS_DEPENDENCY_VALUE 2\n")
run("${CMAKE_COMMAND}" --build "${consumer}" --target dependency_probe_wasm ${config_args})
file(SHA256 "${consumer}/guests/dependency_probe.wasm" after)
if(before STREQUAL after)
    message(FATAL_ERROR "shared-header edit left a stale guest: ${before}: ${OUT}")
endif()
message(STATUS "shared-header edit rebuilt guest: ${before} -> ${after}")
set(before "${after}")
file(WRITE "${consumer_source}/dependency_local.h" "#define PAGLETS_LOCAL_VALUE 20\n")
run("${CMAKE_COMMAND}" --build "${consumer}" --target dependency_probe_wasm ${config_args})
file(SHA256 "${consumer}/guests/dependency_probe.wasm" after)
if(before STREQUAL after)
    message(FATAL_ERROR "local-header edit left a stale guest: ${before}: ${OUT}")
endif()
message(STATUS "local-header edit rebuilt guest: ${before} -> ${after}")
# Bring all consumers up to date before editing only the first input's
# header. A single depfile overwritten by the SDK's later input misses it.
run("${CMAKE_COMMAND}" --build "${consumer}" ${config_args})
file(SHA256 "${consumer}/guests/greeter.wasm" before)
file(WRITE "${consumer_source}/greeter_dependency.hpp"
    "inline constexpr char paglets_dependency_greeting[] = \"welcome, \";\n")
run("${CMAKE_COMMAND}" --build "${consumer}" --target greeter_wasm ${config_args})
file(SHA256 "${consumer}/guests/greeter.wasm" after)
if(before STREQUAL after)
    message(FATAL_ERROR "multi-source SDK build missed the guest-local header: ${before}: ${OUT}")
endif()
run("${host}" run "${consumer}/guests/greeter.wasm" --call greet "\"world\"")
expect("welcome, world, from outside the tree")
message(STATUS "multi-source local-header edit rebuilt guest: ${before} -> ${after}")
message(STATUS "out-of-tree package build ok")
