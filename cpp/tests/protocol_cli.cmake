# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

cmake_minimum_required(VERSION 3.19)

if(NOT TOOL OR NOT WORK)
    message(FATAL_ERROR "TOOL and WORK are required")
endif()
file(MAKE_DIRECTORY "${WORK}")

execute_process(COMMAND "${TOOL}" --replicas 1 --work 1 --observations 1 --in-flight 1 --max-states 100000
    RESULT_VARIABLE status OUTPUT_VARIABLE output ERROR_VARIABLE error TIMEOUT 30)
if(NOT status EQUAL 0)
    message(FATAL_ERROR "custom world failed: ${status}: ${output} ${error}")
endif()
if(NOT output MATCHES "faults: 0 crash, 0 cut, 0 loss, 0 duplicate, 0 sink down, 0 cancel, 0 redirect, 0 early timeout")
    message(FATAL_ERROR "custom world enabled unspecified faults: ${output}")
endif()
if(NOT error MATCHES "custom world: [0-9]+ states; frontier [0-9]+")
    message(FATAL_ERROR "custom world did not report exploration progress: ${error}")
endif()
execute_process(COMMAND "${TOOL}" --replicas 1 --work 1 --observations 1 --in-flight 1 --max-states 100000 --quiet
    RESULT_VARIABLE status OUTPUT_VARIABLE output ERROR_VARIABLE error TIMEOUT 30)
if(NOT status EQUAL 0 OR NOT error STREQUAL "" OR output MATCHES "checking custom world")
    message(FATAL_ERROR "quiet world failed or emitted progress: ${status}: ${output} ${error}")
endif()

# Exercise the real CLI's suite selection, writer and property counts.
set(results "${WORK}/quick.json")
file(REMOVE "${results}")
execute_process(COMMAND "${TOOL}" --suite quick --quiet --results "${results}"
    RESULT_VARIABLE status OUTPUT_VARIABLE output ERROR_VARIABLE error TIMEOUT 300)
if(NOT status EQUAL 0 OR NOT error STREQUAL "")
    message(FATAL_ERROR "quick suite failed: ${status}: ${output} ${error}")
endif()
file(READ "${results}" recorded)
string(JSON count ERROR_VARIABLE json_error LENGTH "${recorded}" tests)
if(NOT json_error STREQUAL "NOTFOUND" OR NOT count EQUAL 6)
    message(FATAL_ERROR "quick suite did not record six worlds: ${json_error}: ${recorded}")
endif()
foreach(index RANGE 0 5)
    string(JSON outcome GET "${recorded}" tests ${index} outcome)
    if(NOT outcome STREQUAL "passed")
        message(FATAL_ERROR "quick suite recorded a failed world: ${recorded}")
    endif()
    string(JSON values LENGTH "${recorded}" tests ${index} values)
    set(properties "")
    math(EXPR last "${values} - 1")
    foreach(value RANGE 0 ${last})
        string(JSON name GET "${recorded}" tests ${index} values ${value} name)
        if(name STREQUAL "properties")
            string(JSON properties GET "${recorded}" tests ${index} values ${value} value)
        endif()
    endforeach()
    if(NOT properties STREQUAL "6")
        message(FATAL_ERROR "quick world did not record four invariants and two recovery goals: ${recorded}")
    endif()
endforeach()

execute_process(COMMAND "${TOOL}" --replicas 1 --work 1 --observations 1 --in-flight 1 --max-states 100000
    --quiet --results "${WORK}/absent/failed.json"
    RESULT_VARIABLE status OUTPUT_QUIET ERROR_VARIABLE error TIMEOUT 30)
if(NOT status EQUAL 1 OR NOT error MATCHES "cannot write results")
    message(FATAL_ERROR "unwritable results did not fail clearly: ${status}: ${error}")
endif()
foreach(value IN ITEMS -1 2x 4294967296)
    execute_process(COMMAND "${TOOL}" --replicas "${value}"
        RESULT_VARIABLE status OUTPUT_QUIET ERROR_QUIET TIMEOUT 5)
    if(NOT status EQUAL 2)
        message(FATAL_ERROR "bad replica count '${value}' returned ${status}, expected usage error")
    endif()
endforeach()
