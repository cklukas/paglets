#!/usr/bin/env bash
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

set -euo pipefail
source "$1"
work="$2"
mkdir -p "$work"
file="$work/network.json"
topology='{"mac":{"key":"aaaa","machine":"mac-installation","url":"https://mac:7443"},"pi":{"key":"bbbb","machine":"pi-installation","url":"https://pi:7443"}}'
check_empty() { [ "$(mesh_network_values "$1" "$2" mac pi)" = '[]' ]; }
check_empty "$work/absent.json" "$topology"
cat > "$file" <<'JSON'
{"topic":"live_network","baseline_version":1,"tests":[
 {"test":"mac → pi","outcome":"passed","date":"2026-10-14",
  "from":{"name":"mac","key":"aaaa","machine":"mac-installation","url":"https://mac:7443"},
  "to":{"name":"pi","key":"bbbb","machine":"pi-installation","url":"https://pi:7443"},
  "values":[{"name":"TCP between the hosts (iperf3)","value":"937","unit":"Mbit/s"}]}]}
JSON
mesh_network_values "$file" "$topology" mac pi | jq -e '
    length == 2 and .[0].value == "937" and .[0].unit == "Mbit/s"
    and .[1].name == "TCP baseline recorded" and .[1].value == "2026-10-14"' > /dev/null
[ "$(mesh_network_values "$file" "$topology" pi mac)" = '[]' ]
# Each mesh_bench invocation creates fresh mesh keys. Physical identity is
# stable, so rotating a fixture key must not discard the earlier baseline.
mesh_network_values "$file" "$(jq -c '.pi.key = "new-fixture-key"' <<<"$topology")" mac pi | jq -e '.[0].value == "937"' > /dev/null
check_empty "$file" "$(jq -c '.pi.machine = "replacement"' <<<"$topology")"
check_empty "$file" "$(jq -c '.pi.url = "https://elsewhere:7443"' <<<"$topology")"
cp "$file" "$work/original.json"
for edit in '.tests[0].outcome = "failed"' '.tests[0].date = ""' '.tests[0].values[0].value = ""' '.tests[0].values[0].value = "0"' '.tests[0].values[0].unit = "MB/s"'; do
    jq "$edit" "$work/original.json" > "$file"
    check_empty "$file" "$topology"
done
printf '{broken\n' > "$file"
if mesh_network_values "$file" "$topology" mac pi > /dev/null 2>&1; then
    echo 'malformed baseline was accepted' >&2; exit 1
fi
printf '{"tests":[]}\n' > "$file"
if mesh_network_values "$file" "$topology" mac pi > /dev/null 2>&1; then
    echo 'unknown baseline version was accepted' >&2; exit 1
fi
printf 'Network baseline reuse and provenance checks passed.\n'
