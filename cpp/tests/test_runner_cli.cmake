# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Exercise the built command, including its exit status, without guest modules.
execute_process(COMMAND "${TESTS}" --list sha256 RESULT_VARIABLE rc
                OUTPUT_VARIABLE inventory ERROR_VARIABLE err TIMEOUT 20)
if(NOT rc EQUAL 0 OR NOT inventory MATCHES "sha256 known vectors")
    message(FATAL_ERROR "cannot list configured tests: ${rc}\n${inventory}\n${err}")
endif()
if(inventory MATCHES "\\[ ok \\]|\\[run \\]|passed|skipped")
    message(FATAL_ERROR "--list executed tests instead of listing their names: ${inventory}")
endif()

execute_process(COMMAND "${TESTS}" --help RESULT_VARIABLE rc
                OUTPUT_VARIABLE help ERROR_VARIABLE err TIMEOUT 20)
if(NOT rc EQUAL 0 OR NOT help MATCHES "--list" OR NOT help MATCHES "FILTER")
    message(FATAL_ERROR "test runner help is missing: ${rc}\n${help}\n${err}")
endif()

foreach(args IN ITEMS "--list;__paglets_no_such_case__" ".;__paglets_no_such_case__" "--list;sha256;extra")
    execute_process(COMMAND "${TESTS}" ${args} RESULT_VARIABLE rc
                    OUTPUT_VARIABLE out ERROR_VARIABLE err TIMEOUT 20)
    if(NOT rc EQUAL 2)
        message(FATAL_ERROR "invalid/empty selection must exit 2: ${args}, ${rc}\n${out}\n${err}")
    endif()
endforeach()

execute_process(COMMAND "${TESTS}" . sha256 RESULT_VARIABLE rc
                OUTPUT_VARIABLE out ERROR_VARIABLE err TIMEOUT 20)
if(NOT rc EQUAL 0 OR NOT out MATCHES "passed, 0 failed, 0 skipped")
    message(FATAL_ERROR "ordinary filtered execution regressed: ${rc}\n${out}\n${err}")
endif()
