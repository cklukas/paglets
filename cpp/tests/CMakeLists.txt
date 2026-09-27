# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

paglets_add_guest(testbed LANGUAGE C NO_SDK SOURCES guests/testbed.c)
paglets_add_guest(forbidden LANGUAGE C NO_SDK SOURCES guests/forbidden.c)

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
    test_snapshot.cpp)
if(PAGLETS_ENABLE_REFLECTION)
    list(APPEND test_sources test_reflect.cpp test_guest_roundtrip.cpp)
endif()

add_executable(paglets_tests ${test_sources})
target_link_libraries(paglets_tests PRIVATE paglets_host)
target_include_directories(paglets_tests PRIVATE ${PROJECT_SOURCE_DIR}/examples/counter)
target_compile_options(paglets_tests PRIVATE ${PAGLETS_WARNINGS})
foreach(guest counter testbed forbidden)
    if(TARGET ${guest}_wasm)
        add_dependencies(paglets_tests ${guest}_wasm)
    endif()
endforeach()

add_test(NAME unit COMMAND paglets_tests "${PAGLETS_TEST_GUEST_DIR}")

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
