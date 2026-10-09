# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# The CLI side of cli_mesh.cmake: runs while hosts a (A), b (B) and c (C)
# serve. Whatever happens, it creates the stop file at the end.

set(P --passphrase-file "${DIR}/pass")
set(L --ledger "${DIR}/ledger")
set(OLGA --key "${DIR}/olga.key" ${P} ${L})

function(stop_and_fail text)
    file(WRITE "${DIR}/stop" "")
    message(FATAL_ERROR "${text}")
endfunction()

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
            string(REGEX MATCH "${U_EXPECT}" found "${out}")
            if(found)
                set(OUT "${out}" PARENT_SCOPE)
                return()
            endif()
        endif()
        execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 0.25)
    endforeach()
    stop_and_fail("paglets-host ${U_ARGS} did not succeed:\n${out}\n${err}")
endfunction()

# a knew nobody: it learns c's address from b's gossip, and c learns a's.
until(TRIES 160 ARGS remote hosts --connect ${A} ${OLGA} EXPECT "c  [0-9a-f]+  online  ${C}")
until(TRIES 160 ARGS remote hosts --connect ${C} ${OLGA} EXPECT "a  [0-9a-f]+  online  ${A}")
message(STATUS "c sees:\n${OUT}")

# A paglet goes from a to c and on to b, by host name.
until(ARGS remote launch --connect ${A} ${OLGA} "${GUEST}")
set(paglet "${OUT}")
until(ARGS remote call --connect ${A} ${OLGA} ${paglet} increment "{\"by\": 5, \"note\": \"a\"}" EXPECT "\"value\":5")
until(ARGS remote dispatch --connect ${A} ${OLGA} ${paglet} c)
until(TRIES 80 ARGS remote call --connect ${C} ${OLGA} ${paglet} increment "{\"by\": 2, \"note\": \"c\"}"
      EXPECT "\"value\":7")
until(ARGS remote dispatch --connect ${C} ${OLGA} ${paglet} b)
until(TRIES 80 ARGS remote call --connect ${B} ${OLGA} ${paglet} increment "{\"by\": 1, \"note\": \"b\"}"
      EXPECT "\"value\":8")
until(TRIES 40 ARGS remote locate --connect ${A} ${OLGA} ${paglet} EXPECT "host:  b ")

# d has no inbound port: every host reaches it through its relays, and the
# paglet moves there by name (WP15).
until(TRIES 160 ARGS remote hosts --connect ${A} ${OLGA} EXPECT "d  [0-9a-f]+  online  \\(no address\\)  protocol 1 abi [0-9.]+  relays [0-9a-f]+ [0-9a-f]+")
message(STATUS "a sees:\n${OUT}")
until(ARGS remote dispatch --connect ${B} ${OLGA} ${paglet} d)
until(TRIES 80 ARGS remote locate --connect ${A} ${OLGA} ${paglet} EXPECT "host:  d ")

# mesh-info knows all four hosts; compute-slots answers (WP16).
until(TRIES 80 ARGS remote landscape --connect ${A} ${OLGA} EXPECT "d +[0-9]+ cpus")
message(STATUS "landscape:\n${OUT}")
# c offers `ai` (the test backend); every host knows (WP17).
until(TRIES 80 ARGS remote landscape --connect ${A} ${OLGA} EXPECT "offers ai summarize,classify")
until(ARGS remote slots --connect ${B} ${OLGA} EXPECT "slots [0-9]+/[0-9]+ free")

# Tooling (WP19): modules on a host, admin decisions against live hosts,
# disposing a paglet.
until(ARGS remote modules --connect ${A} ${OLGA} EXPECT "[0-9a-f]+  [0-9]+ KB  [0-9]+ paglets")
until(ARGS remote push-module --connect ${B} ${OLGA} "${GUEST}" EXPECT "^[0-9a-f]+$")
set(ALICE --key "${DIR}/alice.key" ${P})
# A new owner asks to join; the request reaches a from the requester's copy.
file(COPY "${DIR}/ledger/" DESTINATION "${DIR}/ledger-otto")
until(ARGS keys init --role owner --name otto --out "${DIR}/otto.key" ${P} --kdf interactive)
until(ARGS ledger request --ledger "${DIR}/ledger-otto" --key "${DIR}/otto.key" ${P})
string(SUBSTRING "${OUT}" 0 16 request)
until(ARGS remote push --connect ${A} ${ALICE} --ledger "${DIR}/ledger-otto")
# An admin copy that has not seen it: requests pulls it in, approve decides
# (through b) and every host learns the enrollment by gossip.
file(COPY "${DIR}/ledger/" DESTINATION "${DIR}/ledger-admin")
until(TRIES 40 ARGS remote requests --connect ${B} ${ALICE} --ledger "${DIR}/ledger-admin" EXPECT "owner  otto")
until(ARGS remote approve --connect ${B} ${ALICE} --ledger "${DIR}/ledger-admin" ${request})
until(TRIES 80 ARGS remote status --connect ${C} --key "${DIR}/otto.key" ${P} --ledger "${DIR}/ledger-admin"
      EXPECT "host: +c ")
until(ARGS remote audit --connect ${A} ${ALICE} --ledger "${DIR}/ledger-admin")
until(ARGS remote launch --connect ${A} ${OLGA} "${GUEST}")
set(short_lived "${OUT}")
until(ARGS remote dispose --connect ${A} ${OLGA} ${short_lived} EXPECT "disposed")
until(ARGS remote status --connect ${A} ${OLGA})
string(FIND "${OUT}" "${short_lived}" still_there)
if(NOT still_there EQUAL -1)
    stop_and_fail("the disposed paglet is still listed:\n${OUT}")
endif()

file(WRITE "${DIR}/stop" "")
message(STATUS "four hosts found each other; paglet ${paglet} went a -> c -> b -> d (relayed)")
