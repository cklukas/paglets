# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# WP14 and WP15 end to end: three paglets-host serve processes that know
# one contact each (b joins through a, c through b, a knows nobody) discover
# each other by gossip; an owner moves a paglet between them by host name
# (planning/cpp-mesh.md, section 7). A fourth host, d, has no inbound port:
# it joins through c and is reached through relays (planning/cpp-relay.md):
#   cmake -DHOST=<paglets-host> -DDIR=<work directory> -DGUEST=<counter.wasm> -P cli_mesh.cmake

if(NOT HOST OR NOT DIR OR NOT GUEST)
    message(FATAL_ERROR "HOST, DIR and GUEST are required")
endif()
file(REMOVE_RECURSE "${DIR}")
file(MAKE_DIRECTORY "${DIR}")
file(WRITE "${DIR}/pass" "test passphrase\n")
set(P --passphrase-file "${DIR}/pass")

function(host)
    execute_process(COMMAND "${HOST}" ${ARGN} RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    string(STRIP "${out}" out)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "paglets-host ${ARGN} failed (${rc}):\n${out}\n${err}")
    endif()
    set(OUT "${out}" PARENT_SCOPE)
endfunction()

host(keys init --role admin --name alice --out "${DIR}/alice.key" ${P} --kdf interactive)
host(keys init --role owner --name olga --out "${DIR}/olga.key" ${P} --kdf interactive)
set(olga "${OUT}")
host(mesh create --name e2e --ledger "${DIR}/ledger" --admin "${DIR}/alice.key" ${P})
foreach(name a b c d)
    host(keys init --role host --name ${name} --out "${DIR}/${name}.key")
    host(ledger enroll --ledger "${DIR}/ledger" --admin "${DIR}/alice.key" ${P} host ${OUT} ${name})
endforeach()
host(ledger enroll --ledger "${DIR}/ledger" --admin "${DIR}/alice.key" ${P} owner ${olga} olga)
foreach(name a b c d)
    file(COPY "${DIR}/ledger/" DESTINATION "${DIR}/ledger-${name}")
endforeach()

string(RANDOM LENGTH 4 ALPHABET 0123456789 r)
math(EXPR pa "20000 + (${r} % 20000)")
math(EXPR pb "${pa} + 1")
math(EXPR pc "${pa} + 2")

set(common --stop-file "${DIR}/stop" --in-process --no-beacon)
execute_process(
    COMMAND "${HOST}" serve --key "${DIR}/a.key" --ledger "${DIR}/ledger-a" --state "${DIR}/state-a"
            --listen 127.0.0.1:${pa} ${common}
    COMMAND "${HOST}" serve --key "${DIR}/b.key" --ledger "${DIR}/ledger-b" --state "${DIR}/state-b"
            --listen 127.0.0.1:${pb} --join https://127.0.0.1:${pa} ${common}
    COMMAND "${HOST}" serve --key "${DIR}/c.key" --ledger "${DIR}/ledger-c" --state "${DIR}/state-c"
            --listen 127.0.0.1:${pc} --join https://127.0.0.1:${pb} --ai test ${common}
    COMMAND "${HOST}" serve --key "${DIR}/d.key" --ledger "${DIR}/ledger-d" --state "${DIR}/state-d"
            --no-listen --join https://127.0.0.1:${pc} ${common}
    COMMAND "${CMAKE_COMMAND}" -DHOST=${HOST} -DDIR=${DIR} -DGUEST=${GUEST} -DA=https://127.0.0.1:${pa}
            -DB=https://127.0.0.1:${pb} -DC=https://127.0.0.1:${pc} -P "${CMAKE_CURRENT_LIST_DIR}/cli_mesh_driver.cmake"
    RESULTS_VARIABLE results
    OUTPUT_VARIABLE out
    ERROR_VARIABLE err
    TIMEOUT 180)
list(GET results 4 driver)
if(NOT driver EQUAL 0 OR NOT results STREQUAL "0;0;0;0;0")
    message(FATAL_ERROR "end-to-end run failed (${results}):\n${out}\n${err}")
endif()
message(STATUS "${out}")
