#!/usr/bin/env bash
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Prints the failed tests of the last ctest run in a build directory as
# GitHub Actions error annotations: the failed checks of each test and the
# end of its output (for crashes and aborts, which print no check). The
# annotations can be read through the API without the job's log.
#
#   cpp/tools/ci_failures.sh cpp/build/linux-gcc16

log="${1:?build directory}/Testing/Temporary/LastTest.log"
[ -f "$log" ] || exit 0
awk '
function flush(    i, msg, k, from) {
    if (name == "" || passed || !ended) return
    msg = ""
    k = 0
    for (i = 1; i <= n && k < 30; ++i) {
        if (lines[i] ~ /^\[FAIL\]/ || lines[i] ~ /^    [^ ].*:[0-9]+: /) { msg = msg lines[i] "\n"; ++k }
    }
    msg = msg "-- end of output --\n"
    from = n > 15 ? n - 14 : 1
    for (i = from; i <= n; ++i) msg = msg lines[i] "\n"
    gsub(/%/, "%25", msg)
    gsub(/\r/, "", msg)
    gsub(/\n/, "%0A", msg)
    printf "::error title=Test %s failed::%s\n", name, msg
}
/^[0-9]+\/[0-9]+ Test: / {
    flush()
    name = $0
    sub(/^[0-9]+\/[0-9]+ Test: /, "", name)
    n = 0; passed = 0; ended = 0; output = 0
    next
}
/^----------+$/ && name != "" && !ended { output = 1; next }
/^<end of output>/ { ended = 1; output = 0; next }
/^Test Pass/ { passed = 1; next }
output { lines[++n] = $0 }
END { flush() }
' "$log"
