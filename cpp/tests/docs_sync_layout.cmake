# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

file(MAKE_DIRECTORY "${WORK}/docs/results")
set(record [=[{"tests":[{"test":"mac → pi","seconds":1,"values":[{"name":"latency","value":2.5,"unit":"ms"}]}]}]=])
file(WRITE "${WORK}/docs/results/sample.json" "${record}")
set(markers [=[% results: sample [layout={compact},columns={test|latency},headers={Pair|Latency},widths={0.4|0.4}]
% /results
% results: sample [layout={compact},columns={test|latency},strip-prefix={mac|→}]
% /results
% results: sample [layout={transpose},columns={latency},header={Measured}]
% /results
]=])
file(WRITE "${WORK}/table.tex" "${markers}")
function(sync expected)
    execute_process(COMMAND "${TOOL}" --root "${WORK}" --only table.tex ${ARGN}
        RESULT_VARIABLE status OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(expected STREQUAL "pass" AND NOT status EQUAL 0)
        message(FATAL_ERROR "docs-sync failed: ${output}${error}")
    elseif(expected STREQUAL "fail" AND status EQUAL 0)
        message(FATAL_ERROR "docs-sync accepted stale or invalid results")
    endif()
endfunction()
sync(pass)
sync(pass --check)
file(READ "${WORK}/table.tex" rendered)
foreach(needle IN ITEMS [=[mac $\rightarrow$ pi & 2.5]=] [=[Measured & mac $\rightarrow$ pi]=] [=[p{0.4\linewidth}p{0.4\linewidth}]=] [=[pi & 2.5]=])
    string(FIND "${rendered}" "${needle}" found)
    if(found LESS 0)
        message(FATAL_ERROR "missing rendered table content: ${needle}")
    endif()
endforeach()
string(REPLACE "2.5" "7.25" record "${record}")
file(WRITE "${WORK}/docs/results/sample.json" "${record}")
sync(fail --check)
sync(pass)
sync(pass --check)
file(READ "${WORK}/table.tex" rendered)
string(FIND "${rendered}" "7.25" found)
if(found LESS 0)
    message(FATAL_ERROR "changed result was not regenerated")
endif()
foreach(bad IN ITEMS "layout={compact},columns={test|unknown}" "layout={compact},columns={test|latency},widths={0.4}" "layout={compact},columns={test|latency},widths={0.4|nan}" "layout={unknown},columns={test|latency}")
    file(WRITE "${WORK}/table.tex" "% results: sample [${bad}]\n% /results\n")
    sync(fail)
endforeach()
