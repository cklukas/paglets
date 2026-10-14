#!/bin/sh
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
#
# Extracts the data of the figures from the recorded results:
#   data.sh RESULTS_DIR OUT_DIR
# writes OUT_DIR/*.tsv for the gnuplot scripts in figures/.
set -eu
results=$1
out=$2
mkdir -p "$out"

# The File Courier on the live mesh (docs/results/live_transfer.json): one
# row per payload size, one column per ordered pair of hosts. The rate is
# the payload divided by the time the destination took to save it; the
# time is that saving time in seconds. The "→" of the pair names is kept
# for the keys of the plots.
transfer_wide() {  # field
    jq -r --arg field "$1" '
      [.tests[]
       | (.test | capture("^(?<pair>.+), (?<size>[0-9]+) MB$")) as $c
       | {pair: $c.pair, size: ($c.size | tonumber),
          save_s: ([.values[] | select(.name | startswith("saving")) | .value | tonumber][0] / 1000),
          mbs: ([.values[] | select(.name == "transfer rate" and .unit == "MB/s") | .value | tonumber][0])}]
      | (map(.pair) | unique) as $pairs
      | (["size"] + $pairs | @tsv),
        (group_by(.size)[] | sort_by(.pair) as $g
         | [$g[0].size] + [$pairs[] as $p | ($g[] | select(.pair == $p) | .[$field])] | @tsv)
    ' "$results/live_transfer.json"
}
transfer_wide mbs > "$out/transfer_rate.tsv"
transfer_wide save_s > "$out/transfer_time.tsv"

# The TCP baseline of every ordered pair (iperf3, Mbit/s), once per pair.
jq -r '
  [.tests[]
   | (.test | capture("^(?<pair>.+), (?<size>[0-9]+) MB$")) as $c
   | {pair: $c.pair, tcp: ([.values[] | select(.name | startswith("TCP")) | .value | tonumber][0])}]
  | unique_by(.pair) | sort_by(.pair)
  | (["pair", "tcp_mbit"] | @tsv), (.[] | [.pair, .tcp] | @tsv)
' "$results/live_transfer.json" > "$out/transfer_tcp.tsv"

# The Latency Map on the live mesh (docs/results/live_latency.json): the
# mean and the fastest round trip of every ordered pair of hosts, ms.
jq -r '
  (["pair", "mean_ms", "fastest_ms"] | @tsv),
  (.tests | sort_by(.test) | .[] | [.test,
     ([.values[] | select(.name | startswith("round trip, mean")) | .value | tonumber][0]),
     ([.values[] | select(.name | startswith("round trip, fastest")) | .value | tonumber][0])] | @tsv)
' "$results/live_latency.json" > "$out/latency.tsv"
