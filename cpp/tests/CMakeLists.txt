# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

paglets_add_guest(testbed LANGUAGE C NO_SDK SOURCES guests/testbed.c)
paglets_add_guest(forbidden LANGUAGE C NO_SDK SOURCES guests/forbidden.c)
paglets_add_module(conformance SOURCES guests/conformance.cpp)
paglets_add_module(explorer
    SOURCES guests/explorer.cpp
    SCHEMA_HEADER guests/explorer_msgs.hpp
    SCHEMA_NAMESPACE explorer
    SERVICES files server_info directory storage artifacts pubsub user_info grants locator)
paglets_add_module(calc_user
    SOURCES guests/calc_user.cpp
    SCHEMA_HEADER guests/calc_contract.hpp
    SCHEMA_NAMESPACE calc)

if(PAGLETS_GUESTS_AVAILABLE)
    set(PAGLETS_TEST_GUEST_DIR "${PAGLETS_GUEST_OUTPUT_DIR}")
else()
    set(PAGLETS_TEST_GUEST_DIR "${PAGLETS_PREBUILT_GUEST_DIR}")
endif()

set(test_sources
    test_main.cpp
    test_sha256.cpp
    test_msgpack.cpp
    test_json.cpp
    test_wasm_binary.cpp
    test_engine.cpp
    test_snapshot.cpp
    test_runtime.cpp
    test_ipc.cpp
    test_mesh.cpp
    test_system.cpp
    test_modules.cpp
    test_noise.cpp
    test_network.cpp
    test_mobility.cpp)
if(PAGLETS_ENABLE_REFLECTION)
    list(APPEND test_sources test_reflect.cpp test_guest_roundtrip.cpp test_contract.cpp test_services.cpp test_policy.cpp test_code_mobility.cpp test_movement.cpp test_location.cpp test_discovery.cpp)
endif()

add_executable(paglets_tests ${test_sources})
target_link_libraries(paglets_tests PRIVATE paglets_host paglets_mesh paglets_net)
if(TARGET paglets_node)
    target_link_libraries(paglets_tests PRIVATE paglets_node)
endif()
target_include_directories(paglets_tests PRIVATE ${PROJECT_SOURCE_DIR}/examples/counter ${CMAKE_CURRENT_SOURCE_DIR}/guests)
target_compile_options(paglets_tests PRIVATE ${PAGLETS_WARNINGS})
foreach(guest counter hello ping_pong conformance testbed forbidden calc_user explorer)
    if(TARGET ${guest}_wasm)
        add_dependencies(paglets_tests ${guest}_wasm)
    endif()
endforeach()

add_test(NAME unit COMMAND paglets_tests "${PAGLETS_TEST_GUEST_DIR}")
add_dependencies(paglets_tests paglets-worker)
add_test(NAME worker_sandbox COMMAND paglets-worker --check-sandbox)
# The same tests with paglet instances in (sandboxed) worker processes.
add_test(NAME unit_workers COMMAND paglets_tests "${PAGLETS_TEST_GUEST_DIR}")
set_tests_properties(unit_workers PROPERTIES ENVIRONMENT "PAGLETS_TEST_WORKER=$<TARGET_FILE:paglets-worker>")

# Memory image moves between processes: saved by one, resumed by another.
if(PAGLETS_TEST_GUEST_DIR)
    set(image "${CMAKE_CURRENT_BINARY_DIR}/counter.pgimg")
    add_test(NAME image_save
             COMMAND paglets-spike image-save "${PAGLETS_TEST_GUEST_DIR}/counter.wasm" "${image}" --increments 5)
    add_test(NAME image_resume
             COMMAND paglets-spike image-resume "${PAGLETS_TEST_GUEST_DIR}/counter.wasm" "${image}"
                     --increments 2 --expect 18)
    set_tests_properties(image_save PROPERTIES FIXTURES_SETUP counter_image)
    set_tests_properties(image_resume PROPERTIES FIXTURES_REQUIRED counter_image)
endif()

# paglets-host: a paglet survives its host process in the state directory.
if(PAGLETS_TEST_GUEST_DIR)
    set(state "${CMAKE_CURRENT_BINARY_DIR}/host-state")
    add_test(NAME host_run_hello
             COMMAND paglets-host run "${PAGLETS_TEST_GUEST_DIR}/hello.wasm" --call greet "\"world\"")
    set_tests_properties(host_run_hello PROPERTIES PASS_REGULAR_EXPRESSION "hello, world")
    # With the system paglets and a named root.
    add_test(NAME host_run_services
             COMMAND paglets-host run "${PAGLETS_TEST_GUEST_DIR}/hello.wasm" --root "data=${CMAKE_CURRENT_BINARY_DIR}"
                     --call greet "\"roots\"")
    set_tests_properties(host_run_services PROPERTIES PASS_REGULAR_EXPRESSION "hello, roots")
    add_test(NAME host_state_clean COMMAND ${CMAKE_COMMAND} -E rm -rf "${state}")
    add_test(NAME host_state_save
             COMMAND paglets-host run "${PAGLETS_TEST_GUEST_DIR}/counter.wasm" --state-dir "${state}" --keep
                     --call increment "{\"by\": 5, \"note\": \"a\"}"
                     --call increment "{\"by\": 2, \"note\": \"b\"}")
    add_test(NAME host_state_resume
             COMMAND paglets-host call --state-dir "${state}" all increment "{\"by\": 1, \"note\": \"c\"}"
                     --expect "\"value\":8")
    set_tests_properties(host_state_clean PROPERTIES FIXTURES_SETUP host_state_clean)
    set_tests_properties(host_state_save PROPERTIES FIXTURES_REQUIRED host_state_clean FIXTURES_SETUP host_state)
    set_tests_properties(host_state_resume PROPERTIES FIXTURES_REQUIRED host_state)
endif()

# WP12 end to end: two hosts serve over HTTPS; the CLI launches a paglet
# and moves it between them.
if(PAGLETS_TEST_GUEST_DIR AND PAGLETS_ENABLE_REFLECTION)
    add_test(NAME host_serve_move
             COMMAND ${CMAKE_COMMAND} -DHOST=$<TARGET_FILE:paglets-host> -DDIR=${CMAKE_CURRENT_BINARY_DIR}/cli-serve
                     -DGUEST=${PAGLETS_TEST_GUEST_DIR}/counter.wasm -P ${CMAKE_CURRENT_SOURCE_DIR}/cli_serve.cmake)
    set_tests_properties(host_serve_move PROPERTIES TIMEOUT 240)
    # Three hosts that know one contact each find each other (WP14).
    add_test(NAME host_mesh_discovery
             COMMAND ${CMAKE_COMMAND} -DHOST=$<TARGET_FILE:paglets-host> -DDIR=${CMAKE_CURRENT_BINARY_DIR}/cli-mesh
                     -DGUEST=${PAGLETS_TEST_GUEST_DIR}/counter.wasm -P ${CMAKE_CURRENT_SOURCE_DIR}/cli_mesh.cmake)
    set_tests_properties(host_mesh_discovery PROPERTIES TIMEOUT 240)
endif()

# paglets-host: keys, mesh creation, enrollment requests and decisions.
add_test(NAME host_cli_ledger
         COMMAND ${CMAKE_COMMAND} -DHOST=$<TARGET_FILE:paglets-host> -DDIR=${CMAKE_CURRENT_BINARY_DIR}/cli-ledger
                 -P ${CMAKE_CURRENT_SOURCE_DIR}/cli_ledger.cmake)
