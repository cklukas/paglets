#!/usr/bin/env bash
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Actual process contracts for the live drivers' shared identity check.
set -euo pipefail
source "${1:?live_process.sh is required}"
local_name=local
on() { return 255; }  # An unobservable remote, never a confirmed exit.

/bin/sleep 120 &
pid=$!
if [ "$(uname -s)" = Darwin ]; then expected=/bin/sleep;
else expected="$(readlink -f /bin/sleep)"; fi
cleanup() {
    if live_process_state local "$pid" "$expected" 120; then kill "$pid"; fi
    wait "$pid" 2>/dev/null || true
}
trap cleanup EXIT
sleep 0.1

check_state() {
    local wanted="$1" state; shift
    if live_process_state "$@"; then state=0; else state=$?; fi
    [ "$state" = "$wanted" ] || { echo "expected state $wanted, got $state" >&2; exit 1; }
}
check_state 0 local "$pid" "$expected" 120
check_state 2 local "$pid" /foreign/executable 120
check_state 2 local "$pid" "$expected" foreign-workspace
check_state 2 remote "$pid" "$expected" 120
ps() { return 2; }  # A failed local observer also leaves state unknown.
check_state 2 local "$pid" "$expected" 120
unset -f ps
check_state 0 local "$pid" "$expected" 120
kill "$pid"
wait "$pid" 2>/dev/null || true
check_state 1 local "$pid" "$expected" 120
check_state 1 local "" "$expected" 120
check_state 2 local invalid "$expected" 120
trap - EXIT
echo "live process identity: executable, workspace, SSH failure and exit checks passed"
