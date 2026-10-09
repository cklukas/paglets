#!/usr/bin/env bash
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Prints the failed tests of the last ctest run in a build directory as
# GitHub Actions error annotations: what each failed case printed (up to 60
# lines) and the end of the output (for crashes and aborts, which print no check). The
# annotations can be read through the API without the job's log.
#
#   cpp/tools/ci_failures.sh cpp/build/linux-gcc16

log="${1:?build directory}/Testing/Temporary/LastTest.log"
[ -f "$log" ] || exit 0
awk '
function flush(    i, msg, k, from) {
    if (name == "" || passed || !ended) return
    # Each failed case: what it printed since the case before it ended.
    msg = ""
    from = 1
    for (i = 1; i <= n; ++i) {
        if (lines[i] ~ /^\[(ok|skip) *\]/ || lines[i] ~ /^\[ ok \]/) { from = i + 1; continue }
        if (lines[i] ~ /^\[FAIL\]/) {
            if (i - from > 60) from = i - 60
            for (k = from; k <= i; ++k) msg = msg lines[k] "\n"
            from = i + 1
        }
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
