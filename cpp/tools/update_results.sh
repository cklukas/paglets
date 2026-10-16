#!/usr/bin/env bash
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Runs the tests of the example paglets, with paglets in the host process and
# in sandboxed worker processes, and the protocol checks, records what they
# report in docs/results/,
# and writes the results and code excerpts into docs/ (paglets-docs-sync).
#
#   cpp/tools/update_results.sh [BUILD_DIR]     (default: cpp/build/macos-arm64)
#
# Run it from anywhere; commit docs/ afterwards.

set -euo pipefail
repo="$(cd "$(dirname "$0")/../.." && pwd)"
build="$(cd "${1:-$repo/cpp/build/macos-arm64}" && pwd)"
tests="$build/tests/paglets_tests"
guests="$build/guests"
[ -x "$tests" ] || { echo "no $tests: build first" >&2; exit 1; }

export PAGLETS_RESULTS_DIR="$repo/docs/results"
mkdir -p "$PAGLETS_RESULTS_DIR"
filters=("example:" "gateway:" "exchange:" "pi:" "runtime: hello sample" "runtime: ping-pong sample"
         "snapshot: counter state")
status=0
for worker in "" "$build/host/paglets-worker"; do
    for f in "${filters[@]}"; do
        echo "== ${worker:+workers: }$f"
        if [ -n "$worker" ]; then
            PAGLETS_TEST_WORKER="$worker" "$tests" "$guests" "$f" || status=1
        else
            "$tests" "$guests" "$f" || status=1
        fi
    done
done
# The zombie protocol: the larger worlds and 10 000 timed runs.
echo "== zombie protocol"
"$build/tools/protocol_check/paglets-protocol-check" --suite full --simulate --seeds 10000 --quiet \
    --results "$PAGLETS_RESULTS_DIR/zombie_protocol.json" || status=1
"$build/tools/docs_sync/paglets-docs-sync" --root "$repo"
exit $status
