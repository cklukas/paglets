# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# WP12 end to end: two paglets-host serve processes on the loopback
# interface and the CLI. An owner launches a paglet on host a, calls it,
# moves it to host b and back (planning/cpp-networking.md, section 9):
#   cmake -DHOST=<paglets-host> -DDIR=<work directory> -DGUEST=<counter.wasm> -P cli_serve.cmake

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

# The mesh: an admin, hosts a and b, an owner; every host gets a copy of
# the ledger.
host(keys init --role admin --name alice --out "${DIR}/alice.key" ${P} --kdf interactive)
host(keys init --role owner --name olga --out "${DIR}/olga.key" ${P} --kdf interactive)
set(olga "${OUT}")
host(keys init --role host --name a --out "${DIR}/a.key")
set(a "${OUT}")
host(keys init --role host --name b --out "${DIR}/b.key")
set(b "${OUT}")
host(mesh create --name e2e --ledger "${DIR}/ledger" --admin "${DIR}/alice.key" ${P})
host(ledger enroll --ledger "${DIR}/ledger" --admin "${DIR}/alice.key" ${P} host ${a} a)
host(ledger enroll --ledger "${DIR}/ledger" --admin "${DIR}/alice.key" ${P} host ${b} b)
host(ledger enroll --ledger "${DIR}/ledger" --admin "${DIR}/alice.key" ${P} owner ${olga} olga)
file(COPY "${DIR}/ledger/" DESTINATION "${DIR}/ledger-a")
file(COPY "${DIR}/ledger/" DESTINATION "${DIR}/ledger-b")

# Ports from a random base, so parallel runs rarely collide.
string(RANDOM LENGTH 4 ALPHABET 0123456789 r)
math(EXPR pa "20000 + (${r} % 20000)")
math(EXPR pb "${pa} + 1")

# Both hosts and the driver run at the same time (execute_process runs its
# commands concurrently); the driver stops the hosts with stop files.
execute_process(
    COMMAND "${HOST}" serve --key "${DIR}/a.key" --ledger "${DIR}/ledger-a" --state "${DIR}/state-a"
            --listen 127.0.0.1:${pa} --peer ${b}=https://127.0.0.1:${pb} --stop-file "${DIR}/stop" --in-process
    COMMAND "${HOST}" serve --key "${DIR}/b.key" --ledger "${DIR}/ledger-b" --state "${DIR}/state-b"
            --listen 127.0.0.1:${pb} --peer ${a}=https://127.0.0.1:${pa} --stop-file "${DIR}/stop" --in-process
    COMMAND "${CMAKE_COMMAND}" -DHOST=${HOST} -DDIR=${DIR} -DGUEST=${GUEST} -DA=https://127.0.0.1:${pa}
            -DB=https://127.0.0.1:${pb} -P "${CMAKE_CURRENT_LIST_DIR}/cli_serve_driver.cmake"
    RESULTS_VARIABLE results
    OUTPUT_VARIABLE out
    ERROR_VARIABLE err
    TIMEOUT 180)
list(GET results 2 driver)
list(GET results 0 host_a)
list(GET results 1 host_b)
if(NOT driver EQUAL 0 OR NOT host_a EQUAL 0 OR NOT host_b EQUAL 0)
    message(FATAL_ERROR "end-to-end run failed (driver ${driver}, hosts ${host_a} ${host_b}):\n${out}\n${err}")
endif()
message(STATUS "${out}")
