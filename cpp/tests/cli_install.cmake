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
set(configure_args -S "${SOURCE}" -B "${consumer}" -G "${GENERATOR}" "-DCMAKE_PREFIX_PATH=${prefix}"
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
message(STATUS "out-of-tree package build ok")
