# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

paglets_add_guest(testbed LANGUAGE C NO_SDK SOURCES guests/testbed.c)
paglets_add_guest(forbidden LANGUAGE C NO_SDK SOURCES guests/forbidden.c)
paglets_add_module(conformance SOURCES guests/conformance.cpp)

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
    test_runtime.cpp)
if(PAGLETS_ENABLE_REFLECTION)
    list(APPEND test_sources test_reflect.cpp test_guest_roundtrip.cpp)
endif()

add_executable(paglets_tests ${test_sources})
target_link_libraries(paglets_tests PRIVATE paglets_host)
target_include_directories(paglets_tests PRIVATE ${PROJECT_SOURCE_DIR}/examples/counter ${CMAKE_CURRENT_SOURCE_DIR}/guests)
target_compile_options(paglets_tests PRIVATE ${PAGLETS_WARNINGS})
foreach(guest counter hello ping_pong conformance testbed forbidden)
    if(TARGET ${guest}_wasm)
        add_dependencies(paglets_tests ${guest}_wasm)
    endif()
endforeach()

add_test(NAME unit COMMAND paglets_tests "${PAGLETS_TEST_GUEST_DIR}")
add_dependencies(paglets_tests paglets-worker)
if(NOT WIN32)
    # The same tests with paglet instances in worker processes.
    add_test(NAME unit_workers COMMAND paglets_tests "${PAGLETS_TEST_GUEST_DIR}")
    set_tests_properties(unit_workers PROPERTIES ENVIRONMENT "PAGLETS_TEST_WORKER=$<TARGET_FILE:paglets-worker>")
endif()

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
