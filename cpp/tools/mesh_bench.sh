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
# - times cold-page and return moves of counters with 0, 1, 4 and 16 MB
#   ballast, including capture, transfer/prepare and arrival acknowledgement;
# - lets a File Courier carry files of several sizes for every ordered pair
#   of hosts; the courier measures itself: sharing on the source host, and
#   saving on the destination host (fetching every part, checking the
#   SHA-256, writing);
# - runs the Latency Map: round trips of messages between paglets on every
#   pair of hosts;
# - runs the Mesh Benchmark: every host in a compute slot of its own;
# - computes hex digits of pi in chunks on every host (the Pi example),
#   streamed in order to a file on this machine, and converts them into
#   decimal digits. Job-reported elapsed times are separate from coarse
#   observations by a controller that sleeps one second between polls.
#
# The results go to docs/results/live_*.json and, through paglets-docs-sync,
# into the docs. Every host needs paglets-host and paglets-worker built from
# the same sources, and iperf3; the modules travel from this machine.
#
#   cpp/tools/mesh_bench.sh --remote pi=klukas@rpi4:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
#       --remote uconsole=uconsole:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
#       [--local-name mac] [--build cpp/build/macos-arm64] [--sizes "16 64 256"] [--port 7443]
#       [--pi-digits 100000] [--steps "network move transfer latency benchmark jobs pi"]
#
# Optional: --results-dir DIR, --slots N, --pi-work-ms N,
# --jobs-iterations N and --jobs-samples N.
# --move-sizes "0 1 4 16", --move-samples 10 and --move-memory-mb 32
# control move measurements and the hosts' paglet memory limit.
# The jobs comparison waits up to five minutes per host for AOT; a missing
# compiler or failed compilation is a failed comparison, with raw samples retained.
# --steps runs only some steps; other measurements keep their dates.
# live_hosts.json is a current inventory, rewritten on every run.

set -euo pipefail
(( BASH_VERSINFO[0] >= 4 )) || { echo "bash 4 or newer is required" >&2; exit 2; }
repo="$(cd "$(dirname "$0")/../.." && pwd)"
source "$repo/cpp/tools/live_process.sh"
source "$repo/cpp/tools/mesh_network_baseline.sh"
build="$repo/cpp/build/macos-arm64"
sizes="16 64 256"
pi_digits=100000
steps="network move transfer latency benchmark jobs pi"
results_root="$repo/docs/results"
slots=0
pi_work_ms=0
jobs_iterations=3000000
jobs_samples=3
move_sizes="0 1 4 16"
move_samples=10
move_memory_mb=32
local_name="mac"
port=7443
names=()
declare -A ssh_of bin_of ip_of pid_of prepared run_dir
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
        --pi-digits) pi_digits="$2"; shift 2 ;;
        --steps) steps="$2"; shift 2 ;;
        --results-dir) results_root="$2"; shift 2 ;;
        --slots) slots="$2"; shift 2 ;;
        --pi-work-ms) pi_work_ms="$2"; shift 2 ;;
        --jobs-iterations) jobs_iterations="$2"; shift 2 ;;
        --jobs-samples) jobs_samples="$2"; shift 2 ;;
        --move-sizes) move_sizes="$2"; shift 2 ;;
        --move-samples) move_samples="$2"; shift 2 ;;
        --move-memory-mb) move_memory_mb="$2"; shift 2 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done
step() { [[ " $steps " == *" $1 "* ]]; }
[ ${#names[@]} -gt 0 ] || { echo "at least one --remote NAME=SSH:BIN_DIR" >&2; exit 2; }
[[ "$local_name" =~ ^[a-zA-Z0-9_-]+$ ]] || { echo "invalid local name" >&2; exit 2; }
[[ "$port" =~ ^[0-9]{1,5}$ ]] && (( 10#$port >= 1 && 10#$port <= 65535 )) || { echo "invalid port" >&2; exit 2; }
declare -A seen
seen[$local_name]=true
for h in "${names[@]}"; do
    [[ "$h" =~ ^[a-zA-Z0-9_-]+$ ]] && [ -z "${seen[$h]:-}" ] || { echo "invalid or duplicate host name: $h" >&2; exit 2; }
    [[ "${ssh_of[$h]}" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.@-]*$ ]] &&
        [[ "${bin_of[$h]}" =~ ^/[a-zA-Z0-9_./+-]+$ ]] || { echo "remote needs SSH name and absolute BIN_DIR without shell metacharacters" >&2; exit 2; }
    seen[$h]=true
done
build="$(cd "$build" && pwd -P)"
H="$build/host/paglets-host"
for m in counter file_courier latency benchmark pi; do
    [ -f "$build/guests/$m.wasm" ] || { echo "build $build first (guests)" >&2; exit 1; }
done
required=(jq)
step network && required+=(iperf3)
step move && required+=(python3)
for c in "${required[@]}"; do command -v "$c" >/dev/null || { echo "$c is needed" >&2; exit 1; }; done

all=("$local_name" "${names[@]}")
if [ "$(uname -s)" = Darwin ]; then
    tmp_mount="$(df -P "${TMPDIR:?Set a temporary workspace first}" | awk 'NR == 2 {print $6}')"
    case "${TMPDIR:-}" in
        /Volumes/S8T/tmp/*) [ "$tmp_mount" = /Volumes/S8T ] ;;
        /Volumes/PRO-BLADE/tmp/*) [ "$tmp_mount" = /Volumes/PRO-BLADE ] ;;
        *) echo "Set TMPDIR to a unique directory below a mounted S8T/tmp or PRO-BLADE/tmp" >&2; exit 2 ;;
    esac
fi
work="$(mktemp -d "${TMPDIR:-/tmp}/paglets-bench.XXXXXX")"
rwork="tmp/$(basename "$work")"  # unique, below each remote home
[[ "$work" =~ ^/[a-zA-Z0-9_./+-]+$ ]] && [[ "$build" =~ ^/[a-zA-Z0-9_./+-]+$ ]] || {
    rmdir "$work"; echo "workspace and build paths must not contain shell metacharacters or spaces" >&2; exit 2;
}
export TMPDIR="$work"
run_dir[$local_name]="$work"
prepared[$local_name]=true
slot_args=""
[ "$slots" -gt 0 ] && slot_args="--slots $slots"
step move && slot_args+=" --memory-mb $move_memory_mb"

# Runs a shell command on a host of the mesh.
on() {
    local h="$1"; shift
    if [ "$h" = "$local_name" ]; then bash -c "$*";
    else ssh -n -o BatchMode=yes -o ConnectTimeout=10 "${ssh_of[$h]}" "$*"; fi
}
dir_of() { echo "${run_dir[$1]}"; }

logs="$results_root/raw/host-logs"
recorded=false
active_host() {
    local h="$1" expected
    if [ "$h" = "$local_name" ]; then expected="$H"; else expected="${bin_of[$h]}/paglets-host"; fi
    live_process_state "$h" "${pid_of[$h]:-}" "$expected" "--stop-file ${run_dir[$h]}/stop"
}
cleanup() {
    local h state running failed=false
    for h in "${all[@]}"; do
        [ "${prepared[$h]:-}" = true ] || continue
        on "$h" "touch $(dir_of "$h")/stop" 2>/dev/null || failed=true
    done
    for _ in $(seq 10); do
        running=false
        for h in "${all[@]}"; do
            [ "${prepared[$h]:-}" = true ] || continue
            if active_host "$h"; then running=true;
            else state=$?; [ "$state" = 1 ] || failed=true; fi
        done
        $running || break
        sleep 1
    done
    $running && failed=true
    # The hosts' logs stay for a look afterwards.
    if $recorded; then
        mkdir -p "$logs"
        for h in "${all[@]}"; do
            [ -n "${pid_of[$h]:-}" ] || continue
            on "$h" "cat $(dir_of "$h")/host.log" > "$logs/$h.log" 2>/dev/null || true
        done
        echo "host logs: $logs" >&2
    fi
    if $failed; then echo "cleanup unconfirmed; retained $work and remote $rwork" >&2; return 1; fi
    for h in "${names[@]}"; do
        [ "${prepared[$h]:-}" = true ] || continue
        on "$h" "rm -rf $(dir_of "$h")" || { echo "cleanup failed on $h; retained $work" >&2; return 1; }
    done
    rm -rf "$work"
}
trap 'status=$?; cleanup || status=1; exit "$status"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for h in "${all[@]}"; do
    existing="$(on "$h" "ps -eo pid,args | awk '\$2 ~ /(^|\/)paglets-host\$/ && \$3 == \"serve\" {print}'")"
    [ -z "$existing" ] || { echo "$h already has a paglets-host serve process: $existing" >&2; exit 1; }
    if [ "$h" != "$local_name" ]; then
        bin_of[$h]="$(on "$h" "readlink -f '${bin_of[$h]}'")"
        [[ "${bin_of[$h]}" =~ ^/[a-zA-Z0-9_./+-]+$ ]] || exit 2
        on "$h" "test -x '${bin_of[$h]}/paglets-host'"
    fi
done

# Addresses and descriptions of the hosts.
first="${names[0]}"
ip_of[$local_name]="$(on "$first" 'echo $SSH_CLIENT' | cut -d' ' -f1)"
# The address of the default route (wired before wireless where both are up).
for h in "${names[@]}"; do ip_of[$h]="$(on "$h" "ip -4 route get 1.1.1.1 | grep -o 'src [0-9.]*' | cut -d' ' -f2")"; done
describe() {
    on "$1" 'm=$({ tr -d "\0" </proc/device-tree/model; } 2>/dev/null || sysctl -n hw.model 2>/dev/null);
             c=$(nproc 2>/dev/null || sysctl -n hw.ncpu); echo "$(uname -s) $(uname -m), $m, $c CPUs"'
}
declare -A about
for h in "${all[@]}"; do about[$h]="$(describe "$h")"; echo "$h (${ip_of[$h]}): ${about[$h]}"; done

mkdir -p "$results_root/raw"
: > "$results_root/raw/processes.tsv"
echo "== mesh"
echo "bench passphrase" > "$work/pass"
P=(--passphrase-file "$work/pass")
"$H" keys init --role admin --name alice --out "$work/alice.key" "${P[@]}" --kdf interactive >/dev/null
olga="$("$H" keys init --role owner --name olga --out "$work/olga.key" "${P[@]}" --kdf interactive)"
L=(--ledger "$work/ledger" --admin "$work/alice.key" "${P[@]}")
"$H" mesh create --name bench --ledger "$work/ledger" --admin "$work/alice.key" "${P[@]}" >/dev/null
declare -A key_of
for h in "${names[@]}"; do
    remote_home="$(on "$h" 'printf "%s" "$HOME"')"
    [[ "$remote_home" =~ ^/[a-zA-Z0-9_./+-]+$ ]] || { echo "unsupported remote home path" >&2; exit 2; }
    run_dir[$h]="$remote_home/$rwork"
    on "$h" "mkdir -p '$remote_home/tmp' && mkdir '${run_dir[$h]}'"
    prepared[$h]=true
done
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
    if step transfer; then
        for s in $sizes; do on "$h" "head -c $((s * 1024 * 1024)) /dev/urandom > $d/bench/out/payload-${s}M.bin"; done
    fi
    peers=""
    for o in "${all[@]}"; do [ "$o" = "$h" ] || peers+=" --peer ${key_of[$o]}=https://${ip_of[$o]}:$port"; done
    if [ "$h" = "$local_name" ]; then
        cp -R "$work/ledger" "$work/ledger-$h"
        "$H" serve --key "$work/$h.key" --ledger "$work/ledger-$h" --state "$work/state" \
            --listen "0.0.0.0:$port" --advertise "https://${ip_of[$h]}:$port" $peers --root "bench=$work/bench" \
            --stop-file "$work/stop" --no-beacon $slot_args > "$work/host.log" 2>&1 &
        host_pid=$!
    else
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -r "$work/ledger" "${ssh_of[$h]}:$rwork/ledger"
        scp -q -o BatchMode=yes -o ConnectTimeout=10 "$work/$h.key" "${ssh_of[$h]}:$rwork/host.key"
        host_pid="$(on "$h" "TMPDIR=$d nohup ${bin_of[$h]}/paglets-host serve --key $d/host.key --ledger $d/ledger \
            --state $d/state --listen 0.0.0.0:$port --advertise https://${ip_of[$h]}:$port $peers \
            --root bench=$d/bench --stop-file $d/stop --no-beacon $slot_args > $d/host.log 2>&1 & echo \$!")"
    fi
    pid_of[$h]="$host_pid"
    recorded=true
    for _ in $(seq 20); do active_host "$h" && break; sleep 0.05; done
    active_host "$h" || { echo "$h did not start the expected executable" >&2; exit 1; }
    if [ "$h" = "$local_name" ]; then actual_exe="$H"; else actual_exe="${bin_of[$h]}/paglets-host"; fi
    printf '%s\t%s\t%s\t%s\n' "$h" "$host_pid" "$actual_exe" "$d" >> "$results_root/raw/processes.tsv"
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
declare -A actual_build
builds="{}"
topology="{}"
for h in "${all[@]}"; do
    if [ "$h" = "$local_name" ]; then info="$("$H" --info)";
    else info="$(on "$h" "TMPDIR=\$HOME/$rwork ${bin_of[$h]}/paglets-host --info")"; fi
    printf '%s\n' "$info" > "$results_root/raw/host-info-$h.txt"
    actual_build[$h]="$(awk '/^[[:space:]]*build:/ {print $2}' <<<"$info")"
    actual_compiler="$(awk '/^[[:space:]]*compiler:/ {sub(/.*compiler:[ ]*/, ""); print}' <<<"$info")"
    actual_runtime="$(awk '/^[[:space:]]*runtime:/ {sub(/.*runtime:[ ]*/, ""); print}' <<<"$info")"
    [ -n "${actual_build[$h]}" ] || { echo "$h does not report its build configuration; rebuild it first" >&2; exit 1; }
    machine="$(on "$h" "$(declare -f mesh_network_machine); mesh_network_machine")"
    builds="$(jq -nc --argjson current "$builds" --arg h "$h" --arg b "${actual_build[$h]}" '$current + {($h): $b}')"
    topology="$(jq -nc --argjson current "$topology" --arg h "$h" --arg url "https://${ip_of[$h]}:$port" \
        --arg build "${actual_build[$h]}" --arg platform "${about[$h]}" --arg info "$info" --arg key "${key_of[$h]}" --arg machine "$machine" \
        --arg compiler "$actual_compiler" --arg runtime "$actual_runtime" \
        '$current + {($h): {url: $url, key: $key, machine: $machine, build: $build, platform: $platform, info: $info, compiler: $compiler, runtime: $runtime}}')"
done
printf '%s\n' "$topology" > "$results_root/raw/move-topology.json"
entry() {  # test mode outcome seconds platform values-json
    jq -nc --arg test "$1" --arg mode "$2" --arg outcome "$3" --argjson seconds "$4" --arg platform "$5" \
        --arg build "${7:-mixed; see live_hosts}" --arg date "$today" --argjson values "$6" \
        '{test: $test, mode: $mode, outcome: $outcome, seconds: $seconds, platform: $platform, build: $build,
          date: $date, values: $values}'
}
value() { jq -nc --arg n "$1" --arg v "$2" --arg u "${3:-}" '{name: $n, value: $v, unit: $u}'; }
r1() { awk -v x="$1" 'BEGIN { printf "%.1f", x }'; }

# -- network baseline: iperf3 for every ordered pair ----------------------------------------------

network_file="$results_root/live_network.json"
network_rows="[]"
step network && echo "== iperf3"
step network && for a in "${all[@]}"; do
    for b in "${all[@]}"; do
        [ "$a" = "$b" ] && continue
        on "$b" "iperf3 -s -1 -p 5201 > /dev/null 2>&1 &"
        sleep 1
        sample="$(on "$a" "iperf3 -c ${ip_of[$b]} -p 5201 -t 5 -J")"
        printf '%s\n' "$sample" > "$results_root/raw/network-$a-$b.json"
        bps="$(jq -er '.end.sum_received.bits_per_second | select(type == "number" and . > 0)' <<<"$sample")"
        seconds="$(jq -er '.end.sum_received.seconds | select(type == "number" and . > 0)' <<<"$sample")"
        mbits="$(awk -v x="$bps" 'BEGIN { printf "%.0f", x / 1e6 }')"
        values="$(jq -sc . <<<"$(value "TCP between the hosts (iperf3)" "$mbits" Mbit/s)")"
        row="$(entry "$a → $b" "LAN, iperf3" passed "$seconds" "" "$values")"
        network_rows="$(jq -nc --argjson rows "$network_rows" --argjson row "$row" --argjson hosts "$topology" \
            --arg a "$a" --arg b "$b" '$rows + [$row + {
                from: {name: $a, key: $hosts[$a].key, machine: $hosts[$a].machine, url: $hosts[$a].url},
                to: {name: $b, key: $hosts[$b].key, machine: $hosts[$b].machine, url: $hosts[$b].url}}]')"
        echo "$a -> $b: $mbits Mbit/s"
    done
done
if step network; then
    jq -n --argjson t "$network_rows" '{topic: "live_network", baseline_version: 1, tests: $t}' > "$network_file"
fi

# -- File Courier for every ordered pair -------------------------------------------------------------

if step move; then
    echo "== moves, $move_samples samples per size and direction"
    python3 "$repo/cpp/tools/mesh_move_bench.py" --host "$H" --guest "$build/guests/counter.wasm" \
        --owner "$work/olga.key" --admin "$work/alice.key" --ledger "$work/ledger" --passphrase "$work/pass" \
        --topology "$results_root/raw/move-topology.json" --raw-dir "$results_root/raw" \
        --results "$results_root/live_move.json" --sizes "$move_sizes" --samples "$move_samples"
fi

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
step transfer || sizes=""
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
$(value "transfer rate" "$mbit" Mbit/s)")"
            values="$(jq -nc --argjson transfer "$values" \
                --argjson baseline "$(mesh_network_values "$network_file" "$topology" "$a" "$b")" \
                '$transfer + $baseline')"
            transfers="$(jq -c --argjson e "$(entry "$a → $b, $s MB" "LAN, worker processes" "$outcome" \
                "$(r1 "$(awk -v t="$total" 'BEGIN { print t / 1000 }')")" "" "$values")" \
                '. + [$e]' <<<"$transfers")"
        done
    done
done

# -- Latency Map -------------------------------------------------------------------------------------------

latency="[]"
if step latency; then
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
latency="$(jq -c --argjson builds "$builds" --arg date "$today" '[.rows[] as $r | $r.cells[] |
    {test: "\($r.from) → \(.to)", mode: "LAN, worker processes",
     outcome: (if .error == "" and .replies > 0 then "passed" else "failed" end), seconds: 0,
     platform: "", build: "\($r.from)=\($builds[$r.from]); \(.to)=\($builds[.to])", date: $date,
     values: [{name: "round trip, mean of 20", value: (.avg_ms * 100 | round / 100 | tostring), unit: "ms"},
              {name: "round trip, fastest", value: (.min_ms * 100 | round / 100 | tostring), unit: "ms"}]}]' <<<"$map")"
fi

# -- Mesh Benchmark --------------------------------------------------------------------------------------

benchmark="[]"
if step benchmark; then
echo "== Mesh Benchmark"
id="$(launch benchmark)"
call "$id" start '{"hosts": [], "budget_ms": 1000, "memory_mb": 4, "disk_mb": 8, "timeout_ms": 120000}' >/dev/null
ranking=""
for _ in $(seq 600); do
    sleep 1
    ranking="$(call "$id" ranking 2>/dev/null || true)"
    [ "$(jq -r '.state // ""' <<<"$ranking" 2>/dev/null)" = done ] && break
done
printf '%s\n' "$ranking" > "$results_root/raw/benchmark-ranking.json"
"$H" remote dispose "${R[@]}" "$id" >/dev/null 2>&1 || true
echo "ranking: $(jq -c '{state, errors, hosts: (.hosts | length)}' <<<"$ranking" 2>/dev/null || echo "$ranking")"
jq -c '.hosts[] | {host_name, int_mops, float_mflops, memory_mbs, disk_write_mbs, disk_read_mbs, error}' <<<"$ranking" ||
    echo "ranking: $ranking" >&2
benchmark="$(jq -c --argjson builds "$builds" --arg date "$today" \
    -f "$repo/cpp/tools/mesh_benchmark_results.jq" <<<"$ranking")"
fi

# -- Jobs: identical integer kernel, with the actual execution mode per sample -----------

if step jobs; then
    mkdir -p "$results_root/raw"
    job_rows="[]"
    for h in "${all[@]}"; do
        R=(--connect "https://${ip_of[$h]}:$port" --key "$work/olga.key" --ledger "$work/ledger" "${P[@]}")
        id="$(launch benchmark)"
        echo "== compute jobs on $h"
        host_build="$(jq -r --arg host "$h" '.[$host].build' <<<"$topology")"
        aot_ready=0
        aot_began=$SECONDS
        aot_deadline=$((aot_began + 300))
        attempt=0
        while :; do
            attempt=$((attempt + 1))
            raw="$results_root/raw/jobs-$h-$attempt.json"
            call "$id" jobs "$(jq -nc --argjson i "$jobs_iterations" --argjson n "$jobs_samples" \
                '{iterations: $i, samples: $n}')" > "$raw"
            jq -e --argjson n "$jobs_samples" '.error == "" and (.samples | length) == $n' "$raw" >/dev/null
            rows="$(jq -c --arg h "$h" --arg platform "${about[$h]}" --arg date "$today" \
                --argjson iterations "$jobs_iterations" --arg build "$host_build" '[.samples[] |
                {test: $h, mode: (if .aot then "AOT job" else "interpreter job" end),
                 outcome: "passed", seconds: (.elapsed_ms / 1000), platform: $platform,
                 build: $build, date: $date,
                 values: [{name: "iterations", value: ($iterations | tostring), unit: ""},
                          {name: "queued", value: (.queued_ms | tostring), unit: "ms"}]}]' "$raw")"
            job_rows="$(jq -nc --argjson a "$job_rows" --argjson b "$rows" '$a + $b')"
            if jq -e 'all(.samples[]; .aot)' "$raw" >/dev/null; then
                aot_ready=1
                break
            fi
            [ "$SECONDS" -ge "$aot_deadline" ] && break
            sleep 1
        done
        if [ "$aot_ready" -eq 0 ]; then
            echo "AOT jobs did not become ready on $h within 300 seconds" >&2
            job_rows="$(jq -nc --argjson rows "$job_rows" --arg host "$h" \
                --arg platform "${about[$h]}" --arg date "$today" --arg build "$host_build" \
                --argjson elapsed "$((SECONDS - aot_began))" \
                '$rows + [{test: $host, mode: "AOT readiness", outcome: "failed", seconds: $elapsed,
                 platform: $platform, build: $build, date: $date, values: []}]')"
        fi
        "$H" remote dispose "${R[@]}" "$id" >/dev/null
        if [ "$h" = "$local_name" ]; then "$H" --info; else on "$h" "${bin_of[$h]}/paglets-host --info"; fi \
            > "$results_root/raw/info-$h.txt"
    done
    jq -n --argjson t "$job_rows" '{topic: "live_jobs", tests: $t}' > "$results_root/live_jobs.json"
    R=(--connect "https://${ip_of[$local_name]}:$port" --key "$work/olga.key" --ledger "$work/ledger" "${P[@]}")
fi

# -- Pi: hex digits on every host, in order into a file here, then decimal -----------------

pi_values="[]"
pi_outcome="failed"
pi_seconds=0
step pi || pi_digits=0
[ "$pi_digits" -gt 0 ] && echo "== Pi, $pi_digits hex digits"
if [ "$pi_digits" -gt 0 ]; then
    id="$(launch pi)"
    began=$(date +%s)
    call "$id" start "$(jq -nc --argjson d "$pi_digits" --argjson hold "$pi_work_ms" '{work_ms: $hold, digits: $d, chunk: 500, max_in_flight: 32,
        chunk_timeout_ms: 600000, decimal: true, output_root: "bench", output_path: "pi/pi"}')" >/dev/null
    computed=0
    st=""
    for _ in $(seq 7200); do
        sleep 1
        st="$(call "$id" status 2>/dev/null || true)"
        [ "$computed" -eq 0 ] && [ "$(jq -r '.finished // false' <<<"$st" 2>/dev/null)" = true ] && computed=$(date +%s)
        case "$(jq -r '.state // ""' <<<"$st" 2>/dev/null)" in done|failed) break ;; esac
    done
    ended=$(date +%s)
    printf '%s\n' "$st" > "$results_root/raw/pi-status.json"
    "$H" remote dispose "${R[@]}" "$id" >/dev/null 2>&1 || true
    jq -c '{state, done, chunks_done, resent, decimal_done, hosts, files, error}' <<<"$st"
    first100="3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679"
    hex_bytes=$(($(wc -c < "$work/bench/pi/pi.hex" 2>/dev/null || echo 2) - 2))
    dec_bytes=$(($(wc -c < "$work/bench/pi/pi.txt" 2>/dev/null || echo 2) - 2))
    if [ "$(jq -r .state <<<"$st")" = done ] && [ "$(head -c 102 "$work/bench/pi/pi.txt")" = "$first100" ]; then
        pi_outcome="passed"
    fi
    pi_seconds=$((ended - began))
    [ "$computed" -eq 0 ] && computed=$ended
    if ! pi_values="$(jq -e --argjson hex_bytes "$hex_bytes" --argjson decimal_bytes "$dec_bytes" \
        --argjson hold_ms "$pi_work_ms" --argjson began "$began" --argjson computed "$computed" \
        --argjson ended "$ended" -f "$repo/cpp/tools/mesh_pi_results.jq" <<<"$st")"; then
        pi_values='[]'
        pi_outcome=failed
        if [ "$pi_seconds" -lt 0 ]; then pi_seconds=0; fi
        echo "Pi measurement metadata is invalid" >&2
    fi
fi
pi="$(jq -nc --argjson e "$(entry "Pi example, $pi_digits hex digits" "LAN, compute jobs" "$pi_outcome" "$pi_seconds" "" \
    "$pi_values")" '[$e]')"

mkdir -p "$results_root"
step transfer && jq -n --argjson t "$transfers" '{topic: "live_transfer", tests: $t}' > "$results_root/live_transfer.json"
step latency && jq -n --argjson t "$latency" '{topic: "live_latency", tests: $t}' > "$results_root/live_latency.json"
step benchmark && jq -n --argjson t "$benchmark" '{topic: "live_benchmark", tests: $t}' > "$results_root/live_benchmark.json"
[ "$pi_digits" -gt 0 ] && jq -n --argjson t "$pi" '{topic: "live_pi", tests: $t}' > "$results_root/live_pi.json"
printf '%s\n' "$topology" |
    jq --arg date "$today" 'to_entries | map(
        {test: .key, mode: "", outcome: "", seconds: 0, platform: .value.platform,
         build: .value.build, date: $date,
         values: [{name: "compiler", value: .value.compiler, unit: ""},
                  {name: "WAMR configuration", value: .value.runtime, unit: ""}]}) |
    {topic: "live_hosts", tests: .}' > "$results_root/live_hosts.json"
echo "== wrote docs/results/live_*.json"
failed_steps=0
for topic in transfer latency benchmark jobs move; do
    if step "$topic"; then
        if ! jq -e '.tests | length > 0 and all(.[]; .outcome == "passed")' \
            "$results_root/live_$topic.json" >/dev/null; then
            echo "experiment failed or produced no results: $topic" >&2
            failed_steps=1
        fi
    fi
done
if [ "$pi_digits" -gt 0 ] && [ "$pi_outcome" != passed ]; then
    echo "experiment failed: Pi did not finish with the checked decimal prefix" >&2
    failed_steps=1
fi
[ "$failed_steps" -eq 0 ] || exit 1
if [ "$results_root" = "$repo/docs/results" ]; then
    "$build/tools/docs_sync/paglets-docs-sync" --root "$repo"
fi
