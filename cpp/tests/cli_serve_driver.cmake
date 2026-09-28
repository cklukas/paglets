# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# The CLI side of cli_serve.cmake: runs while hosts a (A) and b (B) serve.
# Whatever happens, it creates the stop file at the end.

set(P --passphrase-file "${DIR}/pass")
set(L --ledger "${DIR}/ledger")

function(stop_and_fail text)
    file(WRITE "${DIR}/stop" "")
    message(FATAL_ERROR "${text}")
endfunction()

# Runs paglets-host until it succeeds (and prints EXPECT, if given), at
# most TRIES times; stores stdout in OUT.
function(until)
    cmake_parse_arguments(U "" "TRIES;EXPECT" "ARGS" ${ARGN})
    if(NOT U_TRIES)
        set(U_TRIES 1)
    endif()
    foreach(i RANGE 1 ${U_TRIES})
        execute_process(COMMAND "${HOST}" ${U_ARGS} RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
        string(STRIP "${out}" out)
        if(rc EQUAL 0)
            if(NOT U_EXPECT)
                set(OUT "${out}" PARENT_SCOPE)
                return()
            endif()
            string(FIND "${out}" "${U_EXPECT}" at)
            if(NOT at EQUAL -1)
                set(OUT "${out}" PARENT_SCOPE)
                return()
            endif()
        endif()
        execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 0.25)
    endforeach()
    stop_and_fail("paglets-host ${U_ARGS} did not succeed:\n${out}\n${err}")
endfunction()

# Both hosts answer an owner's session.
until(TRIES 80 ARGS remote status --connect ${A} --key "${DIR}/olga.key" ${P} ${L})
until(TRIES 80 ARGS remote status --connect ${B} --key "${DIR}/olga.key" ${P} ${L})

# The owner launches a paglet on a (its module travels with the request).
until(ARGS remote launch --connect ${A} --key "${DIR}/olga.key" ${P} ${L} "${GUEST}")
set(paglet "${OUT}")
until(ARGS remote call --connect ${A} --key "${DIR}/olga.key" ${P} ${L} ${paglet} increment
      "{\"by\": 5, \"note\": \"a\"}" EXPECT "\"value\":5")

# It moves to b (b has no application code: it fetches the module from a)
# and keeps its state.
until(ARGS remote dispatch --connect ${A} --key "${DIR}/olga.key" ${P} ${L} ${paglet} b)
until(TRIES 80 ARGS remote call --connect ${B} --key "${DIR}/olga.key" ${P} ${L} ${paglet} increment
      "{\"by\": 2, \"note\": \"b\"}" EXPECT "\"value\":7")

# And back to a, sent by the admin.
until(ARGS remote dispatch --connect ${B} --key "${DIR}/alice.key" ${P} ${L} ${paglet} a)
until(TRIES 80 ARGS remote call --connect ${A} --key "${DIR}/olga.key" ${P} ${L} ${paglet} increment
      "{\"by\": 1, \"note\": \"c\"}" EXPECT "\"value\":8")
until(ARGS remote status --connect ${A} --key "${DIR}/alice.key" ${P} ${L} EXPECT "${paglet}")

# b finds it on a (WP13); its owner pins it there, so it cannot leave.
until(TRIES 40 ARGS remote locate --connect ${B} --key "${DIR}/olga.key" ${P} ${L} ${paglet} EXPECT "host:  a ")
until(ARGS remote pin --connect ${B} --key "${DIR}/olga.key" ${P} ${L} ${paglet} --minutes 5 --reason e2e
      EXPECT "pin:")
execute_process(COMMAND "${HOST}" remote dispatch --connect ${A} --key "${DIR}/olga.key" ${P} ${L} ${paglet} b
                RESULT_VARIABLE rc ERROR_VARIABLE err)
if(rc EQUAL 0 OR NOT err MATCHES "pinned")
    stop_and_fail("a pinned paglet was dispatched: ${err}")
endif()
until(ARGS remote pins --connect ${A} --key "${DIR}/alice.key" ${P} ${L} EXPECT "${paglet}")
# An admin ends the pin through the other host; then it moves again.
until(ARGS remote unpin --connect ${B} --key "${DIR}/alice.key" ${P} ${L} ${paglet} EXPECT "1 pins ended")
until(ARGS remote dispatch --connect ${A} --key "${DIR}/olga.key" ${P} ${L} ${paglet} b)
until(TRIES 80 ARGS remote locate --connect ${A} --key "${DIR}/alice.key" ${P} ${L} ${paglet} EXPECT "host:  b ")

file(WRITE "${DIR}/stop" "")
message(STATUS "paglet ${paglet} went from a to b and back, was located, pinned and released")
