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
(( BASH_VERSINFO[0] >= 4 )) || { echo "bash 4 or newer is required" >&2; exit 2; }
repo="$(cd "$(dirname "$0")/../.." && pwd)"
source "$repo/cpp/tools/live_process.sh"
build="$repo/cpp/build/macos-arm64"
local_name="mac"
port=7443
check_only=false
names=()
declare -A ssh_of bin_of ip_of key_of pid_of prepared dir_of
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
W="$build/web/paglets-web"
[ -x "$H" ] && [ -x "$W" ] && [ -f "$build/guests/counter.wasm" ] || { echo "build $build first (paglets-host, paglets-web, guests)" >&2; exit 1; }
command -v jq >/dev/null && command -v curl >/dev/null || { echo "jq and curl are needed" >&2; exit 1; }

all=("$local_name" "${names[@]}")
if [ "$(uname -s)" = Darwin ]; then
    tmp_mount="$(df -P "${TMPDIR:?Set a temporary workspace first}" | awk 'NR == 2 {print $6}')"
    case "${TMPDIR:-}" in
        /Volumes/S8T/tmp/*) [ "$tmp_mount" = /Volumes/S8T ] || exit 2 ;;
        /Volumes/PRO-BLADE/tmp/*) [ "$tmp_mount" = /Volumes/PRO-BLADE ] || exit 2 ;;
        *) echo "Set TMPDIR below a mounted S8T/tmp or PRO-BLADE/tmp" >&2; exit 2 ;;
    esac
fi
work="$(mktemp -d "${TMPDIR:-/tmp}/paglets-web-live.XXXXXX")"
[[ "$work" =~ ^/[a-zA-Z0-9_./+-]+$ ]] && [[ "$build" =~ ^/[a-zA-Z0-9_./+-]+$ ]] || {
    rmdir "$work"; echo "workspace and build paths must not contain shell metacharacters or spaces" >&2; exit 2;
}
rwork="tmp/$(basename "$work")"
logs="${work}-logs"
export TMPDIR="$work"
prepared[$local_name]=true
dir_of[$local_name]="$work"
on() {
    local h="$1"; shift
    if [ "$h" = "$local_name" ]; then bash -c "$*";
    else ssh -n -o BatchMode=yes -o ConnectTimeout=10 "${ssh_of[$h]}" "$*"; fi
}
web_pid=""
cleanup() {
    local h state expected failed=false running
    mkdir -p "$logs"
    if live_process_state "$local_name" "$web_pid" "$W" "--ledger $work/ledger"; then
        kill "$web_pid" 2>/dev/null || true
    else
        state=$?; [ "$state" = 1 ] || failed=true
    fi
    for h in "${all[@]}"; do
        [ "${prepared[$h]:-}" = true ] || continue
        on "$h" "touch '${dir_of[$h]}/stop'" 2>/dev/null || failed=true
    done
    for _ in $(seq 10); do
        running=false
        if live_process_state "$local_name" "$web_pid" "$W" "--ledger $work/ledger"; then running=true;
        else state=$?; [ "$state" = 1 ] || failed=true; fi
        for h in "${all[@]}"; do
            [ -n "${pid_of[$h]:-}" ] || continue
            if [ "$h" = "$local_name" ]; then expected="$H"; else expected="${bin_of[$h]}/paglets-host"; fi
            if live_process_state "$h" "${pid_of[$h]}" "$expected" "--stop-file ${dir_of[$h]}/stop"; then running=true;
            else state=$?; [ "$state" = 1 ] || failed=true; fi
        done
        $running || break
        sleep 1
    done
    $running && failed=true
    cp "$work/web.log" "$logs/" 2>/dev/null || true
    for h in "${all[@]}"; do
        [ "${prepared[$h]:-}" = true ] || continue
        on "$h" "cat '${dir_of[$h]}/host.log'" > "$logs/$h.log" 2>/dev/null || true
    done
    echo "web live logs and process records: $logs" >&2
    if $failed; then echo "cleanup unconfirmed; retained $work and remote $rwork" >&2; return 1; fi
    for h in "${names[@]}"; do
        [ "${prepared[$h]:-}" = true ] || continue
        on "$h" "rm -rf '${dir_of[$h]}'" || { echo "cleanup failed on $h; retained $work" >&2; return 1; }
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

first="${names[0]}"
ip_of[$local_name]="$(on "$first" 'echo $SSH_CLIENT' | cut -d' ' -f1)"
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
    remote_home="$(on "$h" 'printf "%s" "$HOME"')"
    [[ "$remote_home" =~ ^/[a-zA-Z0-9_./+-]+$ ]] || { echo "unsupported remote home path" >&2; exit 2; }
    dir_of[$h]="$remote_home/$rwork"
    on "$h" "mkdir -p '$remote_home/tmp' && mkdir '${dir_of[$h]}'"
    prepared[$h]=true
    scp -q -o BatchMode=yes -o ConnectTimeout=10 -r "$work/ledger" "${ssh_of[$h]}:$rwork/ledger"
    scp -q -o BatchMode=yes -o ConnectTimeout=10 "$work/$h.key" "${ssh_of[$h]}:$rwork/host.key"
done
cp -R "$work/ledger" "$work/ledger-$local_name"

for h in "${all[@]}"; do
    peers=""
    for o in "${all[@]}"; do [ "$o" = "$h" ] || peers+=" --peer ${key_of[$o]}=https://${ip_of[$o]}:$port"; done
    if [ "$h" = "$local_name" ]; then
        "$H" serve --key "$work/$h.key" --ledger "$work/ledger-$h" --state "$work/state" \
            --listen "0.0.0.0:$port" --advertise "https://${ip_of[$h]}:$port" $peers \
            --stop-file "$work/stop" --no-beacon >> "$work/host.log" 2>&1 &
        pid_of[$h]=$!
    else
        d="${dir_of[$h]}"
        pid_of[$h]="$(on "$h" "TMPDIR='$d' nohup ${bin_of[$h]}/paglets-host serve --key '$d/host.key' --ledger '$d/ledger' \
            --state '$d/state' --listen 0.0.0.0:$port --advertise https://${ip_of[$h]}:$port $peers \
            --stop-file '$d/stop' --no-beacon >> '$d/host.log' 2>&1 < /dev/null & echo \$!")"
    fi
    if [ "$h" = "$local_name" ]; then expected="$H"; else expected="${bin_of[$h]}/paglets-host"; fi
    for _ in $(seq 20); do live_process_state "$h" "${pid_of[$h]}" "$expected" "--stop-file ${dir_of[$h]}/stop" && break; sleep 0.05; done
    live_process_state "$h" "${pid_of[$h]}" "$expected" "--stop-file ${dir_of[$h]}/stop" || { echo "$h did not start the expected executable" >&2; exit 1; }
    mkdir -p "$logs"
    printf '%s\t%s\t%s\t%s\n' "$h" "${pid_of[$h]}" "$expected" "${dir_of[$h]}" >> "$logs/processes.tsv"
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
for _ in $(seq 20); do live_process_state "$local_name" "$web_pid" "$W" "--ledger $work/ledger" && break; sleep 0.05; done
live_process_state "$local_name" "$web_pid" "$W" "--ledger $work/ledger" || { echo "web process identity unconfirmed" >&2; exit 1; }
printf 'web\t%s\t%s\t%s\n' "$web_pid" "$W" "$work" >> "$logs/processes.tsv"
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
if command -v sha256sum >/dev/null; then counter_hash="$(sha256sum "$build/guests/counter.wasm" | cut -d' ' -f1)";
else counter_hash="$(shasum -a 256 "$build/guests/counter.wasm" | cut -d' ' -f1)"; fi
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
