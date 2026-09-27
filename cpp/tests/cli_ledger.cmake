# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# End-to-end test of the key and ledger commands of paglets-host:
#   cmake -DHOST=<paglets-host> -DDIR=<work directory> -P cli_ledger.cmake

if(NOT HOST OR NOT DIR)
    message(FATAL_ERROR "HOST and DIR are required")
endif()
file(REMOVE_RECURSE "${DIR}")
file(MAKE_DIRECTORY "${DIR}")
file(WRITE "${DIR}/pass" "test passphrase\n")
set(L "${DIR}/ledger")
set(P --passphrase-file "${DIR}/pass")

# Runs paglets-host; stores stdout in OUT. EXPECT_FAIL inverts the check.
function(host)
    cmake_parse_arguments(A "EXPECT_FAIL" "" "ARGS" ${ARGN})
    execute_process(COMMAND "${HOST}" ${A_ARGS} RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    string(STRIP "${out}" out)
    if(A_EXPECT_FAIL)
        if(rc EQUAL 0)
            message(FATAL_ERROR "expected a failure: paglets-host ${A_ARGS}\n${out}")
        endif()
    elseif(NOT rc EQUAL 0)
        message(FATAL_ERROR "paglets-host ${A_ARGS} failed (${rc}):\n${out}\n${err}")
    endif()
    set(OUT "${out}" PARENT_SCOPE)
endfunction()

function(expect text)
    string(FIND "${OUT}" "${text}" at)
    if(at EQUAL -1)
        message(FATAL_ERROR "expected '${text}' in:\n${OUT}")
    endif()
endfunction()

function(expect_not text)
    string(FIND "${OUT}" "${text}" at)
    if(NOT at EQUAL -1)
        message(FATAL_ERROR "did not expect '${text}' in:\n${OUT}")
    endif()
endfunction()

host(ARGS keys init --role admin --name alice --out "${DIR}/alice.key" ${P} --kdf interactive)
set(alice "${OUT}")
host(ARGS keys init --role host --name h1 --out "${DIR}/h1.key")
set(h1 "${OUT}")
host(ARGS keys init --role owner --name olga --out "${DIR}/olga.key" ${P} --kdf interactive)
host(EXPECT_FAIL ARGS keys init --role host --name again --out "${DIR}/h1.key")
host(ARGS keys show "${DIR}/alice.key")
expect("role:      admin")
expect("encrypted: yes")
expect("${alice}")

host(ARGS mesh create --name lab --ledger "${L}" --admin "${DIR}/alice.key" ${P})
host(EXPECT_FAIL ARGS mesh create --name lab --ledger "${L}" --admin "${DIR}/alice.key" ${P})

host(ARGS ledger request --ledger "${L}" --key "${DIR}/h1.key" --label linux)
string(SUBSTRING "${OUT}" 0 12 host_request)
host(ARGS ledger request --ledger "${L}" --key "${DIR}/olga.key" ${P})
string(SUBSTRING "${OUT}" 0 12 owner_request)
host(ARGS ledger show --ledger "${L}")
expect("mesh:    lab")
expect("host   h1  ${h1}")
expect("owner  olga")

# Only admins decide.
host(EXPECT_FAIL ARGS ledger approve --ledger "${L}" --admin "${DIR}/olga.key" ${P} ${host_request})
host(ARGS ledger approve --ledger "${L}" --admin "${DIR}/alice.key" ${P} ${host_request})
host(ARGS ledger deny --ledger "${L}" --admin "${DIR}/alice.key" ${P} ${owner_request} --reason unknown)
host(ARGS ledger show --ledger "${L}")
expect("${h1}  h1  linux")
expect_not("owner  olga")

host(ARGS ledger revoke --ledger "${L}" --admin "${DIR}/alice.key" ${P} ${h1} --reason retired)
host(ARGS ledger show --ledger "${L}")
expect("key    ${h1}")
expect_not("${h1}  h1  linux")

# Policy: a rule, listed by show; the audit log is empty without hosts at work.
host(ARGS ledger rule --ledger "${L}" --admin "${DIR}/alice.key" ${P} --name "staff docs" --decision ask
          --service files --op read --group staff --root data --path "docs/**")
host(EXPECT_FAIL ARGS ledger rule --ledger "${L}" --admin "${DIR}/alice.key" ${P} --name bad --decision maybe
          --service files --op read)
host(ARGS ledger show --ledger "${L}")
expect("ask  files  read  staff docs")
host(ARGS ledger audit --ledger "${L}")
