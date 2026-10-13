#!/usr/bin/env bash
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# paglets-web on real machines (docs/web-ui.md): a fresh mesh of this
# machine and remote Linux hosts (reached over SSH, as for mesh_bench.sh),
# a few example paglets on them (a counter with a start value on every host,
# hello, a Mesh Chronicle), and paglets-web on this machine. It checks the
# web UI's API (every host with its status and counts, a call to a paglet on
# a remote host through the UI), prints the page's address, and keeps
# everything running until Ctrl-C (or --check-only: stops after the checks).
#
#   cpp/tools/web_live.sh --remote pi=klukas@rpi4:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
#       --remote uconsole=uconsole:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
#       [--local-name mac] [--build cpp/build/macos-arm64] [--port 7443] [--check-only]

set -euo pipefail
repo="$(cd "$(dirname "$0")/../.." && pwd)"
build="$repo/cpp/build/macos-arm64"
local_name="mac"
port=7443
check_only=false
names=()
declare -A ssh_of bin_of ip_of key_of
while [ $# -gt 0 ]; do
    case "$1" in
        --remote)
            n="${2%%=*}"; rest="${2#*=}"
            names+=("$n"); ssh_of[$n]="${rest%%:*}"; bin_of[$n]="${rest#*:}"
            shift 2 ;;
        --build) build="$2"; shift 2 ;;
        --local-name) local_name="$2"; shift 2 ;;
        --port) port="$2"; shift 2 ;;
        --check-only) check_only=true; shift ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done
[ ${#names[@]} -ge 1 ] || { echo "at least one --remote NAME=SSH:BIN_DIR" >&2; exit 2; }
build="$(cd "$build" && pwd)"
H="$build/host/paglets-host"
W="$build/web/paglets-web"
[ -x "$W" ] && [ -f "$build/guests/counter.wasm" ] || { echo "build $build first (paglets-web, guests)" >&2; exit 1; }
command -v jq >/dev/null && command -v curl >/dev/null || { echo "jq and curl are needed" >&2; exit 1; }

all=("$local_name" "${names[@]}")
rwork="paglets-web-live"  # below the remote home directories
work="$(mktemp -d "${TMPDIR:-/tmp}/paglets-web-live.XXXXXX")"
on() {
    local h="$1"; shift
    if [ "$h" = "$local_name" ]; then bash -c "$*"; else ssh -n "${ssh_of[$h]}" "$*"; fi
}
web_pid=""
cleanup() {
    [ -n "$web_pid" ] && kill "$web_pid" 2>/dev/null || true
    touch "$work/stop"
    for h in "${names[@]}"; do on "$h" "touch $rwork/stop" 2>/dev/null || true; done
    sleep 3
    for h in "${names[@]}"; do on "$h" "rm -rf $rwork" 2>/dev/null || true; done
    rm -rf "$work"
}
trap cleanup EXIT

first="${names[0]}"
ip_of[$local_name]="$(ssh -n "${ssh_of[$first]}" 'echo $SSH_CLIENT' | cut -d' ' -f1)"
for h in "${names[@]}"; do ip_of[$h]="$(on "$h" "ip -4 route get 1.1.1.1 | grep -o 'src [0-9.]*' | cut -d' ' -f2")"; done

echo "mesh of ${all[*]}"
echo "web live passphrase" > "$work/pass"
P=(--passphrase-file "$work/pass")
"$H" keys init --role admin --name alice --out "$work/alice.key" "${P[@]}" --kdf interactive >/dev/null
olga="$("$H" keys init --role owner --name olga --out "$work/olga.key" "${P[@]}" --kdf interactive)"
L=(--ledger "$work/ledger" --admin "$work/alice.key" "${P[@]}")
"$H" mesh create --name web-live --ledger "$work/ledger" --admin "$work/alice.key" "${P[@]}" >/dev/null
for h in "${all[@]}"; do
    key_of[$h]="$("$H" keys init --role host --name "$h" --out "$work/$h.key")"
    "$H" ledger enroll "${L[@]}" host "${key_of[$h]}" "$h" --label live >/dev/null
done
"$H" ledger enroll "${L[@]}" owner "$olga" olga >/dev/null
for h in "${names[@]}"; do
    on "$h" "rm -rf $rwork; mkdir -p $rwork"
    scp -q -r "$work/ledger" "${ssh_of[$h]}:$rwork/ledger"
    scp -q "$work/$h.key" "${ssh_of[$h]}:$rwork/host.key"
done
cp -R "$work/ledger" "$work/ledger-$local_name"

for h in "${all[@]}"; do
    peers=""
    for o in "${all[@]}"; do [ "$o" = "$h" ] || peers+=" --peer ${key_of[$o]}=https://${ip_of[$o]}:$port"; done
    if [ "$h" = "$local_name" ]; then
        "$H" serve --key "$work/$h.key" --ledger "$work/ledger-$h" --state "$work/state" \
            --listen "0.0.0.0:$port" --advertise "https://${ip_of[$h]}:$port" $peers \
            --stop-file "$work/stop" --no-beacon >> "$work/host.log" 2>&1 &
    else
        on "$h" "nohup ${bin_of[$h]}/paglets-host serve --key $rwork/host.key --ledger $rwork/ledger \
            --state $rwork/state --listen 0.0.0.0:$port --advertise https://${ip_of[$h]}:$port $peers \
            --stop-file $rwork/stop --no-beacon >> $rwork/host.log 2>&1 < /dev/null &"
    fi
done

R() { echo "--connect https://${ip_of[$1]}:$port --key $work/olga.key --ledger $work/ledger --passphrase-file $work/pass"; }
landscape_ok() {
    local l
    l="$("$H" remote landscape $(R "$local_name") 2>/dev/null)" || return 1
    for h in "${names[@]}"; do grep -qw "$h" <<<"$l" || return 1; done
}
for _ in $(seq 180); do landscape_ok && break; sleep 1; done
landscape_ok || { echo "the hosts do not see each other" >&2; exit 1; }

# Paglets to look at: a counter on every host (its start value is the
# host's number), hello here, a chronicle whose log goes to this machine.
i=0
for h in "${all[@]}"; do
    i=$((i + 1))
    "$H" remote launch $(R "$h") "$build/guests/counter.wasm" --args "{\"value\": $((i * 100))}" >/dev/null
done
"$H" remote launch $(R "$local_name") "$build/guests/hello.wasm" >/dev/null
chronicle="$("$H" remote launch $(R "$first") "$build/guests/chronicle.wasm" | tail -1)"
"$H" remote call $(R "$first") "$chronicle" start \
    "{\"sink\": \"host:$local_name\", \"lifetime_ms\": 3600000, \"sample_ms\": 10000}" >/dev/null

"$W" --connect "https://${ip_of[$local_name]}:$port" --key "$work/olga.key" --ledger "$work/ledger" "${P[@]}" \
    --module-dir "$build/guests" --no-browser --port-range 8470-8499 > "$work/web.log" 2>&1 &
web_pid=$!
for _ in $(seq 40); do grep -q '^paglets-web: http' "$work/web.log" && break; sleep 0.5; done
url="$(grep -o 'http://[^ ]*' "$work/web.log")"
token="${url##*t=}"
base="${url%%/?t=*}"
api() { curl -sf -H "Authorization: Bearer $token" "$@"; }

# The checks: every host with its status and counts; a call through the UI
# to the counter on the first remote host.
sleep 4
mesh="$(api "$base/api/mesh")"
hosts="$(jq '.hosts | length' <<<"$mesh")"
reachable="$(jq '[.hosts[] | select(.status != null)] | length' <<<"$mesh")"
echo "hosts: $hosts, with status: $reachable, system paglets: $(jq .totals.system <<<"$mesh"), user paglets: $(jq .totals.user <<<"$mesh")"
jq -r '.hosts[] | "  \(.name)  \(.state)  \(.snapshot.os // "?") \(.snapshot.architecture // "")  load \(.snapshot.load_per_cpu // 0 | . * 100 | round / 100)  system \(.counts.system)  user \(.counts.user)"' <<<"$mesh"
[ "$hosts" = "${#all[@]}" ] && [ "$reachable" = "${#all[@]}" ] || { echo "not every host answers the web UI" >&2; exit 1; }
remote_key="$(jq -r --arg n "$first" '.hosts[] | select(.name == $n) | .key' <<<"$mesh")"
counter_hash="$(shasum -a 256 "$build/guests/counter.wasm" | cut -d' ' -f1)"
counter="$(jq -r --arg n "$first" --arg m "$counter_hash" '.hosts[] | select(.name == $n) | .status.paglets[] | select(.module == $m) | .id' <<<"$mesh" | head -1)"
reply="$(api -H 'Content-Type: application/json' \
    -d "{\"host\":\"$remote_key\",\"paglet\":\"$counter\",\"name\":\"increment\",\"payload\":{\"by\":1,\"note\":\"web live\"}}" \
    "$base/api/call")"
echo "call through the UI on $first: $(jq -c .reply <<<"$reply")"
echo "  $(jq -r .shell <<<"$reply")"
jq -e '.status == 0' <<<"$reply" >/dev/null || { echo "the call failed" >&2; exit 1; }
echo "web live check ok"

$check_only && exit 0
echo
echo "paglets-web: $url"
echo "Ctrl-C ends paglets-web and the mesh."
wait "$web_pid"
