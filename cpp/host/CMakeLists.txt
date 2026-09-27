# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

add_library(paglets_host STATIC
    src/json.cpp
    src/json_msgpack.cpp
    src/runtime.cpp
    src/runtime_store.cpp
    src/sha256.cpp
    src/wasm_binary.cpp
    src/wasm_direct.cpp
    src/wasm_engine.cpp
    src/wasm_snapshot.cpp)
target_include_directories(paglets_host PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/common/include)
target_link_libraries(paglets_host PUBLIC paglets_vmlib)
target_compile_options(paglets_host PRIVATE ${PAGLETS_WARNINGS})
target_compile_definitions(paglets_host PUBLIC
    PAGLETS_VERSION="${PROJECT_VERSION}"
    PAGLETS_WAMR_TAG="${PAGLETS_WAMR_TAG}"
    PAGLETS_WAMR_MODE="${PAGLETS_WAMR_MODE}")
if(PAGLETS_ENABLE_REFLECTION)
    target_compile_options(paglets_host PUBLIC -freflection)
    target_compile_definitions(paglets_host PUBLIC PAGLETS_HAVE_REFLECTION=1)
endif()

# The host binary (M0: build and platform information only).
add_executable(paglets-host src/host_main.cpp)
target_link_libraries(paglets-host PRIVATE paglets_host)
target_compile_options(paglets-host PRIVATE ${PAGLETS_WARNINGS})

# M0 spike tool: module inspection, memory images across processes and hosts,
# measurements.
add_executable(paglets-spike src/spike_main.cpp src/process_stats.cpp)
target_link_libraries(paglets-spike PRIVATE paglets_host)
target_compile_options(paglets-spike PRIVATE ${PAGLETS_WARNINGS})
