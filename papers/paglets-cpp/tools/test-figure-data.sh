#!/bin/sh
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Run the actual extractor, using an explicit disposable workspace.
set -eu
extractor=$1
work=$2
mkdir -p "$work/results" "$work/data"
same_text() {
    if [ "$(cat "$1")" != "$(cat "$2")" ]; then
        echo "unexpected figure data: $2" >&2
        cat "$2" >&2
        exit 1
    fi
}
cat > "$work/results/live_latency.json" <<'JSON'
{"tests":[{"test":"mac → pi","values":[
 {"name":"round trip, mean","value":"1.2"},
 {"name":"round trip, fastest","value":"1.0"}]}]}
JSON
cat > "$work/original.json" <<'JSON'
{"tests":[{"test":"mac → pi, 16 MB","values":[
 {"name":"saving on the destination","value":"1000","unit":"ms"},
 {"name":"transfer rate","value":"16","unit":"MB/s"},
 {"name":"TCP between the hosts (iperf3)","value":"937","unit":"Mbit/s"},
 {"name":"TCP baseline recorded","value":"2026-10-14","unit":""}]}]}
JSON
cp "$work/original.json" "$work/results/live_transfer.json"
sh "$extractor" "$work/results" "$work/data"
printf 'pair\ttcp_mbit\nmac → pi\t937\n' > "$work/expected.tsv"
same_text "$work/expected.tsv" "$work/data/transfer_tcp.tsv"
for edit in 'del(.tests[0].values[2])' '.tests[0].values[2].value = ""' '.tests[0].values[2].unit = "MB/s"' '.tests[0].values[2].value = "0"'; do
    jq "$edit" "$work/original.json" > "$work/results/live_transfer.json"
    sh "$extractor" "$work/results" "$work/data"
    printf 'pair\ttcp_mbit\nmac → pi\tNaN\n' > "$work/expected.tsv"
    same_text "$work/expected.tsv" "$work/data/transfer_tcp.tsv"
done
# Missing pairs must keep their column: later pairs must not shift left.
jq '.tests = [.tests[0], (.tests[0] | .test = "pi → mac, 16 MB"),
             (.tests[0] | .test = "pi → mac, 64 MB" | .values[0].value = "4000")]' \
    "$work/original.json" > "$work/results/live_transfer.json"
sh "$extractor" "$work/results" "$work/data"
printf 'size\tmac → pi\tpi → mac\n16\t16\t16\n64\tNaN\t16\n' > "$work/expected.tsv"
same_text "$work/expected.tsv" "$work/data/transfer_rate.tsv"
printf 'size\tmac → pi\tpi → mac\n16\t1\t1\n64\tNaN\t4\n' > "$work/expected.tsv"
same_text "$work/expected.tsv" "$work/data/transfer_time.tsv"
printf 'Figure data accepts dated baselines and marks unknown baselines NaN.\n'
