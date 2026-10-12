#!/usr/bin/env bash
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Live benchmarks of example paglets on a mesh of this machine and remote
# Linux hosts, reached over SSH (docs/examples/file-courier.md,
# latency-map.md, mesh-benchmark.md).
#
# It makes a fresh mesh of every host, with a named root `bench` on each,
# and then
#
# - measures the network between every ordered pair of hosts with iperf3
#   (TCP, 5 seconds), as the baseline;
# - lets a File Courier carry files of several sizes for every ordered pair
#   of hosts; the courier measures itself: sharing on the source host, and
#   saving on the destination host (fetching every part, checking the
#   SHA-256, writing);
# - runs the Latency Map: round trips of messages between paglets on every
#   pair of hosts;
# - runs the Mesh Benchmark: every host in a compute slot of its own.
#
# The results go to docs/results/live_*.json and, through paglets-docs-sync,
# into the docs. Every host needs paglets-host and paglets-worker built from
# the same sources, and iperf3; the modules travel from this machine.
#
#   cpp/tools/mesh_bench.sh --remote pi=klukas@rpi4:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
#       --remote uconsole=uconsole:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
#       [--local-name mac] [--build cpp/build/macos-arm64] [--sizes "16 64 256"] [--port 7443]

set -euo pipefail
repo="$(cd "$(dirname "$0")/../.." && pwd)"
build="$repo/cpp/build/macos-arm64"
sizes="16 64 256"
local_name="mac"
port=7443
names=()
declare -A ssh_of bin_of ip_of
while [ $# -gt 0 ]; do
    case "$1" in
        --remote)
            n="${2%%=*}"; rest="${2#*=}"
            names+=("$n"); ssh_of[$n]="${rest%%:*}"; bin_of[$n]="${rest#*:}"
            shift 2 ;;
        --build) build="$2"; shift 2 ;;
        --sizes) sizes="$2"; shift 2 ;;
        --local-name) local_name="$2"; shift 2 ;;
        --port) port="$2"; shift 2 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done
[ ${#names[@]} -gt 0 ] || { echo "at least one --remote NAME=SSH:BIN_DIR" >&2; exit 2; }
build="$(cd "$build" && pwd)"
H="$build/host/paglets-host"
for m in file_courier latency benchmark; do
    [ -f "$build/guests/$m.wasm" ] || { echo "build $build first (guests)" >&2; exit 1; }
done
for c in jq iperf3; do command -v $c >/dev/null || { echo "$c is needed" >&2; exit 1; }; done

all=("$local_name" "${names[@]}")
rwork="paglets-bench"  # below the remote home directories
work="$(mktemp -d "${TMPDIR:-/tmp}/paglets-bench.XXXXXX")"

# Runs a shell command on a host of the mesh.
on() {
    local h="$1"; shift
    if [ "$h" = "$local_name" ]; then bash -c "$*"; else ssh -n "${ssh_of[$h]}" "$*"; fi
}
dir_of() { if [ "$1" = "$local_name" ]; then echo "$work"; else echo "\$HOME/$rwork"; fi; }

logs="${TMPDIR:-/tmp}/paglets-bench-logs"
cleanup() {
    for h in "${all[@]}"; do on "$h" "touch $(dir_of "$h")/stop" 2>/dev/null || true; done
    sleep 3
    # The hosts' logs stay for a look afterwards.
    mkdir -p "$logs"
    for h in "${all[@]}"; do on "$h" "cat $(dir_of "$h")/host.log" > "$logs/$h.log" 2>/dev/null || true; done
    echo "host logs: $logs" >&2
    for h in "${names[@]}"; do on "$h" "rm -rf $rwork" 2>/dev/null || true; done
    rm -rf "$work"
}
trap cleanup EXIT

# Addresses and descriptions of the hosts.
first="${names[0]}"
ip_of[$local_name]="$(ssh -n "${ssh_of[$first]}" 'echo $SSH_CLIENT' | cut -d' ' -f1)"
# The address of the default route (wired before wireless where both are up).
for h in "${names[@]}"; do ip_of[$h]="$(on "$h" "ip -4 route get 1.1.1.1 | grep -o 'src [0-9.]*' | cut -d' ' -f2")"; done
describe() {
    on "$1" 'm=$({ tr -d "\0" </proc/device-tree/model; } 2>/dev/null || sysctl -n hw.model 2>/dev/null);
             c=$(nproc 2>/dev/null || sysctl -n hw.ncpu); echo "$(uname -s) $(uname -m), $m, $c CPUs"'
}
declare -A about
for h in "${all[@]}"; do about[$h]="$(describe "$h")"; echo "$h (${ip_of[$h]}): ${about[$h]}"; done

echo "== mesh"
echo "bench passphrase" > "$work/pass"
P=(--passphrase-file "$work/pass")
"$H" keys init --role admin --name alice --out "$work/alice.key" "${P[@]}" --kdf interactive >/dev/null
olga="$("$H" keys init --role owner --name olga --out "$work/olga.key" "${P[@]}" --kdf interactive)"
L=(--ledger "$work/ledger" --admin "$work/alice.key" "${P[@]}")
"$H" mesh create --name bench --ledger "$work/ledger" --admin "$work/alice.key" "${P[@]}" >/dev/null
declare -A key_of
for h in "${all[@]}"; do
    key_of[$h]="$("$H" keys init --role host --name "$h" --out "$work/$h.key")"
    "$H" ledger enroll "${L[@]}" host "${key_of[$h]}" "$h" >/dev/null
done
"$H" ledger enroll "${L[@]}" owner "$olga" olga >/dev/null
"$H" ledger rule "${L[@]}" --name bench --decision allow --service files --op read --op write --op create \
    --root bench >/dev/null

for h in "${all[@]}"; do
    d="$(dir_of "$h")"
    on "$h" "mkdir -p $d/bench/out"
    for s in $sizes; do on "$h" "head -c $((s * 1024 * 1024)) /dev/urandom > $d/bench/out/payload-${s}M.bin"; done
    peers=""
    for o in "${all[@]}"; do [ "$o" = "$h" ] || peers+=" --peer ${key_of[$o]}=https://${ip_of[$o]}:$port"; done
    if [ "$h" = "$local_name" ]; then
        cp -R "$work/ledger" "$work/ledger-$h"
        "$H" serve --key "$work/$h.key" --ledger "$work/ledger-$h" --state "$work/state" \
            --listen "0.0.0.0:$port" --advertise "https://${ip_of[$h]}:$port" $peers --root "bench=$work/bench" \
            --stop-file "$work/stop" --no-beacon > "$work/host.log" 2>&1 &
    else
        scp -q -r "$work/ledger" "${ssh_of[$h]}:$rwork/ledger"
        scp -q "$work/$h.key" "${ssh_of[$h]}:$rwork/host.key"
        on "$h" "nohup ${bin_of[$h]}/paglets-host serve --key $rwork/host.key --ledger $rwork/ledger \
            --state $rwork/state --listen 0.0.0.0:$port --advertise https://${ip_of[$h]}:$port $peers \
            --root bench=\$HOME/$rwork/bench --stop-file $rwork/stop --no-beacon > $rwork/host.log 2>&1 &"
    fi
done

R=(--connect "https://${ip_of[$local_name]}:$port" --key "$work/olga.key" --ledger "$work/ledger" "${P[@]}")
landscape_ok() {
    local l
    l="$("$H" remote landscape "${R[@]}" 2>/dev/null)" || return 1
    for h in "${names[@]}"; do grep -qw "$h" <<<"$l" || return 1; done
}
for _ in $(seq 180); do landscape_ok && break; sleep 1; done
landscape_ok || { echo "the hosts do not see each other" >&2; cat "$work/host.log" >&2; exit 1; }

today="$(date +%Y-%m-%d)"
build_type="$(grep -E '^CMAKE_BUILD_TYPE:' "$build/CMakeCache.txt" | cut -d= -f2)"
entry() {  # test mode outcome seconds platform values-json
    jq -nc --arg test "$1" --arg mode "$2" --arg outcome "$3" --argjson seconds "$4" --arg platform "$5" \
        --arg build "$build_type" --arg date "$today" --argjson values "$6" \
        '{test: $test, mode: $mode, outcome: $outcome, seconds: $seconds, platform: $platform, build: $build,
          date: $date, values: $values}'
}
value() { jq -nc --arg n "$1" --arg v "$2" --arg u "${3:-}" '{name: $n, value: $v, unit: $u}'; }
r1() { awk -v x="$1" 'BEGIN { printf "%.1f", x }'; }

# -- network baseline: iperf3 for every ordered pair ----------------------------------------------

declare -A tcp_mbits
echo "== iperf3"
for a in "${all[@]}"; do
    for b in "${all[@]}"; do
        [ "$a" = "$b" ] && continue
        on "$b" "iperf3 -s -1 -p 5201 > /dev/null 2>&1 &"
        sleep 1
        bps="$(on "$a" "iperf3 -c ${ip_of[$b]} -p 5201 -t 5 -J" | jq '.end.sum_received.bits_per_second')"
        tcp_mbits[$a,$b]="$(awk -v x="$bps" 'BEGIN { printf "%.0f", x / 1e6 }')"
        echo "$a -> $b: ${tcp_mbits[$a,$b]} Mbit/s"
    done
done

# -- File Courier for every ordered pair -------------------------------------------------------------

call() { "$H" remote call "${R[@]}" "$@"; }
launch() { "$H" remote launch "${R[@]}" "$build/guests/$1.wasm" | tail -1; }
errand() {  # from to pattern to_path; prints the final status
    local id st="" started
    id="$(launch file_courier)"
    started="$(call "$id" start "$(jq -nc --arg f "$1" --arg t "$2" --arg p "$3" --arg d "$4" \
        '{from_host: $f, from_root: "bench", pattern: $p, to_host: $t, to_root: "bench", to_path: $d}')")"
    [ "$(jq -r '.accepted' <<<"$started")" = true ] || { echo "not started: $started" >&2; return 1; }
    for _ in $(seq 1800); do
        sleep 1
        st="$(call "$id" status 2>/dev/null || true)"
        case "$(jq -r '.state // ""' <<<"$st" 2>/dev/null)" in delivered|refused|failed) break ;; esac
    done
    "$H" remote dispose "${R[@]}" "$id" >/dev/null 2>&1 || true
    echo "$st"
}

transfers="[]"
for s in $sizes; do
    for a in "${all[@]}"; do
        for b in "${all[@]}"; do
            [ "$a" = "$b" ] && continue
            echo "== File Courier, $s MB: $a -> $b"
            st="$(errand "$a" "$b" "out/payload-${s}M.bin" "from-$a")"
            jq -c '{state, bytes, share_ms, save_ms, total_ms, error}' <<<"$st"
            bytes="$(jq '.bytes' <<<"$st")"; save="$(jq '.save_ms' <<<"$st")"
            share="$(jq '.share_ms' <<<"$st")"; total="$(jq '.total_ms' <<<"$st")"
            outcome="$(jq -r 'if .state == "delivered" then "passed" else .state end' <<<"$st")"
            mbs="$(awk -v b="$bytes" -v ms="$save" 'BEGIN { printf "%.1f", (ms > 0 ? b / 1048576 / (ms / 1000) : 0) }')"
            mbit="$(awk -v b="$bytes" -v ms="$save" 'BEGIN { printf "%.0f", (ms > 0 ? b * 8 / 1e6 / (ms / 1000) : 0) }')"
            values="$(jq -sc . <<<"$(value "sharing (hashing) on the source" "$share" ms)
$(value "saving on the destination: transfer, SHA-256 check, write" "$save" ms)
$(value "transfer rate" "$mbs" MB/s)
$(value "transfer rate" "$mbit" Mbit/s)
$(value "TCP between the hosts (iperf3)" "${tcp_mbits[$a,$b]}" Mbit/s)")"
            transfers="$(jq -c --argjson e "$(entry "$a → $b, $s MB" "LAN, worker processes" "$outcome" \
                "$(r1 "$(awk -v t="$total" 'BEGIN { print t / 1000 }')")" "" "$values")" \
                '. + [$e]' <<<"$transfers")"
        done
    done
done

# -- Latency Map -------------------------------------------------------------------------------------------

echo "== Latency Map"
id="$(launch latency)"
call "$id" start '{"hosts": [], "rounds": 20, "timeout_ms": 60000}' >/dev/null
map=""
for _ in $(seq 120); do
    sleep 1
    map="$(call "$id" map 2>/dev/null || true)"
    [ "$(jq -r '.finished // false' <<<"$map" 2>/dev/null)" = true ] && break
done
"$H" remote dispose "${R[@]}" "$id" >/dev/null 2>&1 || true
jq -c '.rows[] | {from, cells: [.cells[] | {to, avg_ms, min_ms, replies, error}]}' <<<"$map"
latency="$(jq -c --arg build "$build_type" --arg date "$today" '[.rows[] as $r | $r.cells[] |
    {test: "\($r.from) → \(.to)", mode: "LAN, worker processes",
     outcome: (if .error == "" and .replies > 0 then "passed" else "failed" end), seconds: 0,
     platform: "", build: $build, date: $date,
     values: [{name: "round trip, mean of 20", value: (.avg_ms * 100 | round / 100 | tostring), unit: "ms"},
              {name: "round trip, fastest", value: (.min_ms * 100 | round / 100 | tostring), unit: "ms"}]}]' <<<"$map")"

# -- Mesh Benchmark --------------------------------------------------------------------------------------

echo "== Mesh Benchmark"
id="$(launch benchmark)"
call "$id" start '{"hosts": [], "budget_ms": 1000, "memory_mb": 4, "disk_mb": 8, "timeout_ms": 120000}' >/dev/null
ranking=""
for _ in $(seq 600); do
    sleep 1
    ranking="$(call "$id" ranking 2>/dev/null || true)"
    [ "$(jq -r '.state // ""' <<<"$ranking" 2>/dev/null)" = done ] && break
done
"$H" remote dispose "${R[@]}" "$id" >/dev/null 2>&1 || true
echo "ranking: $(jq -c '{state, errors, hosts: (.hosts | length)}' <<<"$ranking" 2>/dev/null || echo "$ranking")"
jq -c '.hosts[] | {host_name, int_mops, float_mflops, memory_mbs, disk_write_mbs, disk_read_mbs, error}' <<<"$ranking" ||
    echo "ranking: $ranking" >&2
benchmark="$(jq -c --arg build "$build_type" --arg date "$today" '[.hosts[] |
    {test: .host_name, mode: "in a compute slot, worker processes",
     outcome: (if .error == "" then "passed" else "failed" end), seconds: (.bench_ms / 100 | round / 10),
     platform: "\(.os) \(.architecture), \(.cpus) CPUs", build: $build, date: $date,
     values: [{name: "integer", value: (.int_mops | round | tostring), unit: "Mops"},
              {name: "floating point", value: (.float_mflops | round | tostring), unit: "Mflops"},
              {name: "memory copying", value: (.memory_mbs | round | tostring), unit: "MB/s"},
              {name: "storage write, 8 MB", value: (.disk_write_mbs | round | tostring), unit: "MB/s"},
              {name: "storage read, 8 MB", value: (.disk_read_mbs | round | tostring), unit: "MB/s"}]}]' <<<"$ranking")"

mkdir -p "$repo/docs/results"
jq -n --argjson t "$transfers" '{topic: "live_transfer", tests: $t}' > "$repo/docs/results/live_transfer.json"
jq -n --argjson t "$latency" '{topic: "live_latency", tests: $t}' > "$repo/docs/results/live_latency.json"
jq -n --argjson t "$benchmark" '{topic: "live_benchmark", tests: $t}' > "$repo/docs/results/live_benchmark.json"
for h in "${all[@]}"; do printf '%s\t%s\n' "$h" "${about[$h]}"; done |
    jq -Rs --arg date "$today" 'split("\n") | map(select(. != "") | split("\t") |
        {test: .[0], mode: "", outcome: "", seconds: 0, platform: .[1], build: "", date: $date, values: []}) |
    {topic: "live_hosts", tests: .}' > "$repo/docs/results/live_hosts.json"
echo "== wrote docs/results/live_*.json"
"$build/tools/docs_sync/paglets-docs-sync" --root "$repo"
