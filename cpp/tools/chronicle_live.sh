#!/usr/bin/env bash
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# The Mesh Chronicle on real machines (docs/examples/mesh-chronicle.md):
# a fresh mesh of this machine and remote Linux hosts (reached over SSH, as
# for mesh_bench.sh), one chronicle with a lifetime of 10 minutes, and a
# schedule of failures while it runs (times for 10 minutes; they scale with
# --lifetime-min):
#
#   1:30  the host of the main freezes (SIGSTOP): the others take over while
#         the old main, cut off, keeps its state; it thaws at 3:00 and the two
#         mains meet;
#   4:30  the host of the main crashes (SIGKILL); it restarts at 6:00;
#   7:00  another host crashes and stays down until 8:30;
#   8:45  the sink's host shuts down normally; it is back at 9:15.
#
# Afterwards it fetches the life log from the sink, checks the protocol
# steps every host recorded against the specification, and records the
# outcome in docs/results/live_chronicle.json and the life log in
# docs/examples/mesh-chronicle-log.md.
#
#   cpp/tools/chronicle_live.sh --remote pi=klukas@rpi4:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
#       --remote uconsole=uconsole:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
#       [--local-name mac] [--build cpp/build/macos-arm64] [--port 7443] [--lifetime-min 10]
#       [--no-failures] [--keep]

set -euo pipefail
repo="$(cd "$(dirname "$0")/../.." && pwd)"
build="$repo/cpp/build/macos-arm64"
local_name="mac"
port=7443
lifetime_min=10
failures=true
keep=false
names=()
declare -A ssh_of bin_of ip_of key_of pid_of
while [ $# -gt 0 ]; do
    case "$1" in
        --remote)
            n="${2%%=*}"; rest="${2#*=}"
            names+=("$n"); ssh_of[$n]="${rest%%:*}"; bin_of[$n]="${rest#*:}"
            shift 2 ;;
        --build) build="$2"; shift 2 ;;
        --local-name) local_name="$2"; shift 2 ;;
        --port) port="$2"; shift 2 ;;
        --lifetime-min) lifetime_min="$2"; shift 2 ;;
        --no-failures) failures=false; shift ;;
        --keep) keep=true; shift ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done
[ ${#names[@]} -ge 2 ] || { echo "at least two --remote NAME=SSH:BIN_DIR" >&2; exit 2; }
build="$(cd "$build" && pwd)"
H="$build/host/paglets-host"
[ -f "$build/guests/chronicle.wasm" ] || { echo "build $build first (guests)" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is needed" >&2; exit 1; }

all=("$local_name" "${names[@]}")
rwork="paglets-chronicle"  # below the remote home directories
work="$(mktemp -d "${TMPDIR:-/tmp}/paglets-chronicle.XXXXXX")"
t0=$(date +%s)
say() { printf '%5ds  %s\n' $(( $(date +%s) - t0 )) "$*"; }

on() {
    local h="$1"; shift
    if [ "$h" = "$local_name" ]; then bash -c "$*"; else ssh -n "${ssh_of[$h]}" "$*"; fi
}
dir_of() { if [ "$1" = "$local_name" ]; then echo "$work"; else echo "\$HOME/$rwork"; fi; }

logs="${TMPDIR:-/tmp}/paglets-chronicle-logs"
cleanup() {
    if $keep; then echo "kept: $work (hosts still running)" >&2; return; fi
    for h in "${all[@]}"; do
        on "$h" "pkill -CONT -f \"paglets-host serve --key $(dir_of "$h")/host.key\" ; touch $(dir_of "$h")/stop" 2>/dev/null || true
    done
    sleep 3
    mkdir -p "$logs"
    for h in "${all[@]}"; do on "$h" "cat $(dir_of "$h")/host.log" > "$logs/$h.log" 2>/dev/null || true; done
    echo "host logs: $logs" >&2
    for h in "${names[@]}"; do on "$h" "rm -rf $rwork" 2>/dev/null || true; done
    rm -rf "$work"
}
trap cleanup EXIT

first="${names[0]}"
ip_of[$local_name]="$(ssh -n "${ssh_of[$first]}" 'echo $SSH_CLIENT' | cut -d' ' -f1)"
for h in "${names[@]}"; do ip_of[$h]="$(on "$h" "ip -4 route get 1.1.1.1 | grep -o 'src [0-9.]*' | cut -d' ' -f2")"; done

say "mesh of ${all[*]}"
echo "chronicle passphrase" > "$work/pass"
P=(--passphrase-file "$work/pass")
"$H" keys init --role admin --name alice --out "$work/alice.key" "${P[@]}" --kdf interactive >/dev/null
olga="$("$H" keys init --role owner --name olga --out "$work/olga.key" "${P[@]}" --kdf interactive)"
L=(--ledger "$work/ledger" --admin "$work/alice.key" "${P[@]}")
"$H" mesh create --name chronicle --ledger "$work/ledger" --admin "$work/alice.key" "${P[@]}" >/dev/null
for h in "${all[@]}"; do
    key_of[$h]="$("$H" keys init --role host --name "$h" --out "$work/$h.key")"
    "$H" ledger enroll "${L[@]}" host "${key_of[$h]}" "$h" >/dev/null
done
"$H" ledger enroll "${L[@]}" owner "$olga" olga >/dev/null
for h in "${names[@]}"; do
    on "$h" "rm -rf $rwork; mkdir -p $rwork"
    scp -q -r "$work/ledger" "${ssh_of[$h]}:$rwork/ledger"
    scp -q "$work/$h.key" "${ssh_of[$h]}:$rwork/host.key"
done
cp -R "$work/ledger" "$work/ledger-$local_name"
cp "$work/$local_name.key" "$work/host.key"

# Starts (or restarts, with its state) the host process of `h`.
serve() {
    local h="$1" d peers=""
    d="$(dir_of "$h")"
    for o in "${all[@]}"; do [ "$o" = "$h" ] || peers+=" --peer ${key_of[$o]}=https://${ip_of[$o]}:$port"; done
    if [ "$h" = "$local_name" ]; then
        rm -f "$work/stop"
        "$H" serve --key "$work/host.key" --ledger "$work/ledger-$h" --state "$work/state" \
            --listen "0.0.0.0:$port" --advertise "https://${ip_of[$h]}:$port" $peers \
            --stop-file "$work/stop" --no-beacon >> "$work/host.log" 2>&1 &
    else
        on "$h" "rm -f $rwork/stop; nohup ${bin_of[$h]}/paglets-host serve --key $d/host.key --ledger $rwork/ledger \
            --state $rwork/state --listen 0.0.0.0:$port --advertise https://${ip_of[$h]}:$port $peers \
            --stop-file $rwork/stop --no-beacon >> $rwork/host.log 2>&1 < /dev/null &"
    fi
}
signal() {  # host signal (the pattern is expanded on the host: $HOME)
    on "$1" "pkill -$2 -f \"paglets-host serve --key $(dir_of "$1")/host.key\"" || true
}
stop_host() { on "$1" "touch $(dir_of "$1")/stop"; }
for h in "${all[@]}"; do serve "$h"; done

R_of() { echo "--connect https://${ip_of[$1]}:$port --key $work/olga.key --ledger $work/ledger --passphrase-file $work/pass"; }
landscape_ok() {
    local l
    l="$("$H" remote landscape $(R_of "$local_name") 2>/dev/null)" || return 1
    for h in "${names[@]}"; do grep -qw "$h" <<<"$l" || return 1; done
}
for _ in $(seq 180); do landscape_ok && break; sleep 1; done
landscape_ok || { echo "the hosts do not see each other" >&2; exit 1; }
sleep 5  # every host has fresh snapshots of every other

# The chronicle starts on the first remote host; its life log goes to this machine.
creator_host="$first"
id="$("$H" remote launch $(R_of "$creator_host") "$build/guests/chronicle.wasm" | tail -1)"
start="$("$H" remote call $(R_of "$creator_host") "$id" start \
    "$(jq -nc --arg s "host:$local_name" --argjson l $((lifetime_min * 60000)) '{sink: $s, lifetime_ms: $l, sample_ms: 10000}')")"
group="$(jq -r .group <<<"$start")"
say "chronicle $id started on $creator_host, group $group"
began=$(date +%s)
at() { local s=$1; while [ $(( $(date +%s) - began )) -lt "$s" ]; do sleep 1; done; }

# The host whose replica is the main now (asks each host's registry and replica).
main_host() {
    local h p st
    for h in "${all[@]}"; do
        p="$("$H" remote zombies list $(R_of "$h") 2>/dev/null | grep "^$group" | grep -o 'replica [0-9a-f]*' | cut -d' ' -f2)" || continue
        [ -n "$p" ] || continue
        st="$("$H" remote status $(R_of "$h") 2>/dev/null | grep -o "$p[0-9a-f]*" | head -1)" || continue
        [ -n "$st" ] || continue
        if "$H" remote call $(R_of "$h") "$st" status 2>/dev/null | jq -e '.role == "main" or .role == "delivering"' >/dev/null; then
            echo "$h"
            return
        fi
    done
}
other_than() {  # a host other than the given ones, not the sink's
    local h x
    for h in "${names[@]}"; do
        for x in "$@"; do [ "$h" = "$x" ] && continue 2; done
        echo "$h"; return
    done
}

events="[]"
event() { events="$(jq -c --arg t "$(( $(date +%s) - began ))" --arg w "$1" '. + [{at_s: ($t|tonumber), what: $w}]' <<<"$events")"; say "$1"; }

L=$((lifetime_min * 60))
$failures && {
at $((L * 15 / 100))
m="$(main_host)"; m="${m:-$creator_host}"
[ "$m" = "$local_name" ] && m="$creator_host"
event "the host of the main ($m) freezes (SIGSTOP): cut off, its main keeps its state"
signal "$m" STOP
frozen="$m"
at $((L * 30 / 100))
event "$frozen thaws (SIGCONT): two mains meet"
signal "$frozen" CONT
at $((L * 45 / 100))
m="$(main_host)"; m="${m:-$creator_host}"
[ "$m" = "$local_name" ] && m="$(other_than)"
event "the host of the main ($m) crashes (SIGKILL)"
signal "$m" KILL
crashed="$m"
at $((L * 60 / 100))
event "$crashed restarts with its state"
serve "$crashed"
at $((L * 70 / 100))
second="$(other_than "$crashed")"
event "$second crashes (SIGKILL) and stays down for $((L * 15 / 100)) s"
signal "$second" KILL
at $((L * 85 / 100))
event "$second restarts with its state"
serve "$second"
at $((L * 875 / 1000))
event "the sink's host ($local_name) shuts down normally"
stop_host "$local_name"
at $((L * 925 / 1000))
event "$local_name restarts with its state"
serve "$local_name"
}

# The end: the life log at the sink.
deadline=$(( began + lifetime_min * 60 + 240 ))
log=""
while [ "$(date +%s)" -lt "$deadline" ]; do
    if log="$("$H" remote zombies result "$group" $(R_of "$local_name") 2>/dev/null)" && [ -n "$log" ]; then break; fi
    log=""
    sleep 5
done
if [ -z "$log" ]; then
    echo "no life log arrived" >&2
    for h in "${all[@]}"; do
        echo "== $h" >&2
        "$H" remote zombies list $(R_of "$h") >&2 || true
    done
    "$H" remote zombies traces "$group" $(R_of "$local_name") >&2 || true
    exit 1
fi
ended=$(date +%s)
say "the life log arrived ($(wc -c <<<"$log") bytes)"
sleep 15  # the tombstone reaches every host
check="$("$H" remote zombies check "$group" $(R_of "$local_name") 2>&1)" && check_ok=true || check_ok=false
echo "$check"
remaining=0
for h in "${all[@]}"; do
    "$H" remote zombies list $(R_of "$h") 2>/dev/null | grep "^$group" | grep -q 'replica -' || remaining=$((remaining + 1))
done

out="$repo/docs/examples/mesh-chronicle-log.md"
{
    echo "# A life log of three hosts"
    echo
    echo "The [Mesh Chronicle](mesh-chronicle.md) on this machine (mac) and two"
    echo "Raspberry Pi class Linux hosts (pi, uconsole), with failures on schedule"
    echo "(\`cpp/tools/chronicle_live.sh\`). This is the life log it delivered, as it is."
    echo
    echo "What happened to the hosts (seconds after the start):"
    echo
    jq -r '.[] | "- \(.at_s) s: \(.what)"' <<<"$events"
    echo
    sed '1s/^# Mesh life log/## The life log/; s/^## /### /' <<<"$log" | sed '1s/^### The life log/## The life log/'
} > "$out"

takeovers="$(grep -c 'took over as main' <<<"$log" || true)"
partitions="$(grep -o '^[0-9]* partition(s) with a main on both sides' <<<"$log" | cut -d' ' -f1 || true)"
records="$(grep -o 'records [0-9]*' <<<"$check" | cut -d' ' -f2)"
outcome=passed
$check_ok || outcome=failed
[ "$remaining" -eq 0 ] || outcome=failed
values="$(jq -nc --arg hosts "${#all[@]}" --arg tk "$takeovers" --arg pt "${partitions:-0}" --arg rec "${records:-0}" \
    --arg ok "$check_ok" --arg rem "$remaining" --arg life "$lifetime_min" \
    '[{name: "hosts with a replica", value: $hosts, unit: ""},
      {name: "lifetime", value: $life, unit: "min"},
      {name: "takeovers in the life log", value: $tk, unit: ""},
      {name: "partitions with a main on both sides", value: $pt, unit: ""},
      {name: "protocol steps checked against the specification", value: $rec, unit: ""},
      {name: "replicas left after the end", value: $rem, unit: ""}]')"
jq -nc --arg outcome "$outcome" --argjson seconds $((ended - began)) --arg date "$(date +%Y-%m-%d)" --argjson values "$values" \
    '{topic: "live_chronicle", tests: [{test: "Mesh Chronicle, 10 minutes with freezes, crashes and restarts",
      mode: "LAN, worker processes", outcome: $outcome, seconds: $seconds, platform: "macOS arm64, 2 × Linux arm64",
      build: "Release", date: $date, values: $values}]}' > "$repo/docs/results/live_chronicle.json"
"$build/tools/docs_sync/paglets-docs-sync" --root "$repo"
say "done: $outcome"
[ "$outcome" = passed ]
