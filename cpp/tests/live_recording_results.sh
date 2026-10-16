#!/bin/sh
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

set -eu
tools=$1
work=$2
mkdir -p "$work"
info='paglets-host 0.2.0
  platform:   windows x86_64
  compiler:   gcc 16.2.0
  build:      RelWithDebInfo
  runtime:    WAMR-2.4.5 (fast-interp, software bound checks)'
jq -nc --arg host workstation --arg info "$info" -f "$tools/live_host_info.jq" > "$work/host.json"
jq -e '.name == "workstation" and .platform == "windows x86_64" and .build == "RelWithDebInfo"' "$work/host.json" > /dev/null
if jq -nc --arg host incomplete --arg info 'build: Release' -f "$tools/live_host_info.jq" > /dev/null 2>&1; then
    echo 'incomplete host metadata accepted' >&2; exit 1
fi
hosts="$(jq -sc '.[0] as $h | [$h, ($h | .name = "mac" | .platform = "macos arm64"),
    ($h | .name = "pi" | .platform = "linux arm64" | .build = "Release")]' "$work/host.json")"
render_chronicle() {
    jq -nc --arg outcome passed --argjson seconds 60 --arg date 2026-10-16 \
        --argjson values '[]' --arg name fixture --argjson hosts "$1" -f "$tools/chronicle_results.jq"
}
render_chronicle "$hosts" > "$work/chronicle.json"
jq -e '.tests[0] | .build == "mixed; see host configurations" and (.hosts | length) == 3
    and (.platform | contains("workstation: windows x86_64; mac: macos arm64; pi: linux arm64"))' "$work/chronicle.json" > /dev/null
render_chronicle "$(printf '%s\n' "$hosts" | jq -c 'map(.build = "RelWithDebInfo")')" |
    jq -e '.tests[0].build == "RelWithDebInfo"' > /dev/null
cat > "$work/ranking.json" <<'JSON'
{"hosts":[{"host_name":"mac","slot":true,"error":"","bench_ms":1500,
 "os":"macos","architecture":"arm64","cpus":10,"int_mops":1,"float_mflops":2,
 "memory_mbs":3,"disk_write_mbs":4,"disk_read_mbs":5}]}
JSON
render_benchmark() {
    jq -c --argjson builds '{"mac":"RelWithDebInfo"}' --arg date 2026-10-16 \
        -f "$tools/mesh_benchmark_results.jq" "$work/ranking.json"
}
render_benchmark | jq -e '.[0] | .outcome == "passed" and .mode == "in a compute slot, worker processes"
    and .build == "RelWithDebInfo" and .seconds == 1.5' > /dev/null
for edit in '.hosts[0].slot = false' 'del(.hosts[0].slot)' '.hosts[0].error = "quota"'; do
    jq "$edit" "$work/ranking.json" > "$work/changed.json"
    jq -c --argjson builds '{"mac":"RelWithDebInfo"}' --arg date 2026-10-16 \
        -f "$tools/mesh_benchmark_results.jq" "$work/changed.json" |
        jq -e '.[0].outcome == "failed"' > /dev/null
done
printf 'Actual host/build and benchmark slot reporting checks passed.\n'
