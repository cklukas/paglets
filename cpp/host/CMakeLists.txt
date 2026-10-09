# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# The JSON value type of the wire layer. The schema generator links only this
# library, so it is installed (as paglets::wire) for projects that build
# guest paglets against an installed paglets package.
add_library(paglets_wire STATIC src/json.cpp)
add_library(paglets::wire ALIAS paglets_wire)
set_target_properties(paglets_wire PROPERTIES EXPORT_NAME wire)
target_include_directories(paglets_wire PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/common/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_options(paglets_wire PRIVATE ${PAGLETS_WARNINGS})
if(PAGLETS_ENABLE_REFLECTION)
    target_compile_options(paglets_wire PRIVATE -freflection)
endif()

add_library(paglets_host STATIC
    src/json_msgpack.cpp
    src/ipc.cpp
    src/runtime.cpp
    src/runtime_exec.cpp
    src/runtime_modules.cpp
    src/runtime_store.cpp
    src/sandbox.cpp
    src/shm_ring.cpp
    src/sha256.cpp
    src/wasm_binary.cpp
    src/wasm_direct.cpp
    src/wasm_engine.cpp
    src/wasm_snapshot.cpp)
target_include_directories(paglets_host PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/common/include)
target_link_libraries(paglets_host PUBLIC paglets_wire paglets_vmlib)
target_compile_options(paglets_host PRIVATE ${PAGLETS_WARNINGS})
target_compile_definitions(paglets_host PUBLIC
    PAGLETS_VERSION="${PROJECT_VERSION}"
    PAGLETS_WAMR_TAG="${PAGLETS_WAMR_TAG}"
    PAGLETS_WAMR_MODE="${PAGLETS_WAMR_MODE}")
if(PAGLETS_ENABLE_REFLECTION)
    target_compile_options(paglets_host PUBLIC -freflection)
    target_compile_definitions(paglets_host PUBLIC PAGLETS_HAVE_REFLECTION=1)
endif()

# Mesh identity: keys, the security ledger, gossip replication, passports.
# Worker processes do not link it.
add_library(paglets_mesh STATIC
    src/mesh/crypto.cpp
    src/mesh/gossip.cpp
    src/mesh/ledger.cpp
    src/mesh/passport.cpp
    src/mesh/module_trust.cpp
    src/mesh/policy.cpp
    src/mesh/record.cpp
    src/mesh/value.cpp)
target_link_libraries(paglets_mesh PUBLIC paglets_host PRIVATE Sodium::Sodium)
target_compile_options(paglets_mesh PRIVATE ${PAGLETS_WARNINGS})

# Networking (WP12, WP14): Noise channels between identity keys, carried
# over HTTPS (cpp-httplib with OpenSSL); multicast beacons.
add_library(paglets_net STATIC
    src/net/beacon.cpp
    src/net/channel.cpp
    src/net/noise.cpp
    src/net/transport.cpp)
target_link_libraries(paglets_net PUBLIC paglets_mesh PRIVATE Sodium::Sodium OpenSSL::SSL OpenSSL::Crypto)
target_include_directories(paglets_net SYSTEM PRIVATE ${PAGLETS_HTTPLIB_INCLUDE_DIR})
target_compile_options(paglets_net PRIVATE ${PAGLETS_WARNINGS})
if(WIN32)
    # cpp-httplib needs Windows 10 APIs.
    target_compile_definitions(paglets_net PRIVATE _WIN32_WINNT=0x0A00)
    target_link_libraries(paglets_net PRIVATE ws2_32 crypt32)
endif()

# The standard system paglets and the host platform layer (WP10); they use
# C++26 reflection for their service contracts.
if(PAGLETS_ENABLE_REFLECTION)
    add_library(paglets_services STATIC
        src/platform/platform_common.cpp
        src/platform/platform_linux.cpp
        src/platform/platform_macos.cpp
        src/platform/platform_windows.cpp
        src/services/artifacts.cpp
        src/services/common.cpp
        src/services/directory.cpp
        src/services/files.cpp
        src/services/install.cpp
        src/services/pubsub.cpp
        src/services/server_info.cpp
        src/services/storage.cpp
        src/services/user_info.cpp)
    target_link_libraries(paglets_services PUBLIC paglets_host)
    target_compile_options(paglets_services PRIVATE ${PAGLETS_WARNINGS})
    if(WIN32)
        target_link_libraries(paglets_services PRIVATE psapi)
    endif()

    # Gateway system paglets (WP17): `web` and `ai`, on hosts configured
    # for them (cpp-httplib with OpenSSL).
    add_library(paglets_gateway STATIC
        src/gateway/ai.cpp
        src/gateway/html.cpp
        src/gateway/install.cpp
        src/gateway/url.cpp
        src/gateway/web.cpp)
    target_link_libraries(paglets_gateway PUBLIC paglets_services PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    target_include_directories(paglets_gateway SYSTEM PRIVATE ${PAGLETS_HTTPLIB_INCLUDE_DIR})
    target_compile_options(paglets_gateway PRIVATE ${PAGLETS_WARNINGS})
    if(WIN32)
        target_compile_definitions(paglets_gateway PRIVATE _WIN32_WINNT=0x0A00)
        target_link_libraries(paglets_gateway PRIVATE ws2_32 crypt32)
    endif()

    # A host in a mesh: ledger and gossip tied to the runtime, the grants
    # system paglet (WP9).
    add_library(paglets_node STATIC src/node/compute.cpp src/node/location.cpp src/node/movement.cpp src/node/node.cpp src/node/registry.cpp
                                    src/node/session.cpp)
    target_link_libraries(paglets_node PUBLIC paglets_services paglets_mesh PRIVATE Zstd::Zstd)
    target_compile_options(paglets_node PRIVATE ${PAGLETS_WARNINGS})
endif()

# The host binary and its worker processes.
add_executable(paglets-host src/host_main.cpp src/mesh_cli.cpp src/serve_cli.cpp)
target_link_libraries(paglets-host PRIVATE paglets_host paglets_mesh paglets_net)
if(TARGET paglets_node)
    target_link_libraries(paglets-host PRIVATE paglets_node paglets_gateway)
endif()
target_compile_options(paglets-host PRIVATE ${PAGLETS_WARNINGS})
add_executable(paglets::paglets-host ALIAS paglets-host)
add_executable(paglets-worker src/worker_main.cpp)
target_link_libraries(paglets-worker PRIVATE paglets_host)
target_compile_options(paglets-worker PRIVATE ${PAGLETS_WARNINGS})
add_dependencies(paglets-host paglets-worker)

# M0 spike tool: module inspection, memory images across processes and hosts,
# measurements.
add_executable(paglets-spike src/spike_main.cpp src/process_stats.cpp)
target_link_libraries(paglets-spike PRIVATE paglets_host)
target_compile_options(paglets-spike PRIVATE ${PAGLETS_WARNINGS})
