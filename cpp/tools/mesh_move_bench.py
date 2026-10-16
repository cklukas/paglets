#!/usr/bin/env python3
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

"""Measure physical moves on the fresh mesh made by mesh_bench.sh."""

import argparse
import datetime
import itertools
import json
import os
import pathlib
import statistics
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--guest", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--admin", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--passphrase", required=True)
    parser.add_argument("--topology", required=True)
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--results", required=True)
    parser.add_argument("--sizes", default="0 1 4 16")
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--mode", default="LAN, worker processes; uniform nonzero ballast")
    args = parser.parse_args()
    if sys.platform == "darwin":
        temporary = pathlib.Path(os.environ.get("TMPDIR", "")).resolve()
        allowed = [pathlib.Path("/Volumes/S8T"), pathlib.Path("/Volumes/PRO-BLADE")]
        if not any(os.path.ismount(root) and temporary.is_relative_to(root / "tmp")
                   and temporary != root / "tmp" for root in allowed):
            parser.error("set TMPDIR below a mounted S8T/tmp or PRO-BLADE/tmp")
    sizes = [int(value) for value in args.sizes.split()]
    if args.samples < 1 or not sizes or any(value < 0 for value in sizes):
        parser.error("positive samples and nonnegative ballast sizes are required")
    topology = json.loads(pathlib.Path(args.topology).read_text())
    raw_dir = pathlib.Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    rows = []

    def cli(host, action, *values, admin=False):
        command = [args.host, "remote", action, "--connect", topology[host]["url"],
                   "--key", args.admin if admin else args.owner, "--ledger", args.ledger,
                   "--passphrase-file", args.passphrase, *values]
        result = subprocess.run(command, capture_output=True, text=True, timeout=150)
        if result.returncode:
            raise RuntimeError(f"{host} {action}: {result.stderr.strip()} {result.stdout.strip()}")
        return result.stdout.strip()

    def stats(host, clear=False):
        return json.loads(cli(host, "moves", *(["clear-cache"] if clear else []), admin=clear))

    def measure(source, destination, paglet, marker, ballast, sample, condition):
        before_source = stats(source)
        before_destination = stats(destination)
        began = time.perf_counter_ns()
        dispatch = cli(source, "dispatch", paglet, destination + "?retries=1")
        status = json.loads(cli(destination, "call", paglet, "status"))
        observed_ns = time.perf_counter_ns() - began
        if status["value"] != marker or status["memory_pages"] * 65536 < ballast * 1024 * 1024:
            raise RuntimeError("the destination did not preserve the counter and its allocated memory")
        deadline = time.monotonic() + 15
        while True:
            after_source = stats(source)
            if after_source["arrival_acks"] == before_source["arrival_acks"] + 1:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("missing receipt after the destination's arrival hook")
            time.sleep(0.02)
        after_destination = stats(destination)
        delta_source = {key: after_source[key] - before_source[key] for key in
                        ["moves_out", "moves_failed", "pages_sent", "bytes_sent", "image_pages",
                         "capture_ns", "ready_ns", "arrival_acks", "arrival_failures", "arrival_ack_ns",
                         "destination_activation_ns"]}
        delta_destination = {key: after_destination[key] - before_destination[key] for key in
                             ["moves_in", "pages_received", "pages_reused", "prepare_ns", "commit_ns"]}
        if delta_source["moves_out"] != 1 or delta_destination["moves_in"] != 1:
            raise RuntimeError("move counters do not describe exactly one successful transfer")
        if delta_source["moves_failed"] or delta_source["arrival_failures"]:
            raise RuntimeError("the transfer or arrival failed")
        if delta_source["destination_activation_ns"] <= 0:
            raise RuntimeError("the arrival receipt omitted its activation/restore duration")
        if condition == "cold pages" and delta_destination["pages_reused"]:
            raise RuntimeError("the supposedly cold destination reused pages")
        if delta_source["ready_ns"] < delta_destination["prepare_ns"]:
            raise RuntimeError("destination preparation exceeds the interval that contains it")
        row = {"from": source, "to": destination, "ballast_mb": ballast, "sample": sample,
               "condition": condition, "paglet": paglet, "marker": marker,
               "source": delta_source, "destination": delta_destination,
               "confirmed_cli_ns": observed_ns,
               "transfer_control_ns": delta_source["ready_ns"] - delta_destination["prepare_ns"],
               "dispatch": dispatch, "status": status,
               "source_before": before_source, "source_after": after_source,
               "destination_before": before_destination, "destination_after": after_destination}
        rows.append(row)
        (raw_dir / "move-samples.json").write_text(json.dumps(rows, indent=2) + "\n")

    marker = 0
    for source, destination in itertools.permutations(topology, 2):
        for ballast in sizes:
            for sample in range(1, args.samples + 1):
                marker += 1
                paglet = cli(source, "launch", args.guest, "--args", json.dumps({"value": marker}))
                holder = source
                try:
                    if ballast:
                        cli(source, "call", paglet, "bloat",
                            json.dumps({"kilobytes": ballast * 1024, "fill": 165}))
                    stats(source, clear=True)
                    stats(destination, clear=True)
                    measure(source, destination, paglet, marker, ballast, sample, "cold pages")
                    holder = destination
                    measure(destination, source, paglet, marker, ballast, sample, "return")
                    holder = source
                finally:
                    unwinding = sys.exc_info()[0] is not None
                    try:
                        cli(holder, "dispose", paglet)
                    except RuntimeError:
                        other = destination if holder == source else source
                        try:
                            cli(other, "dispose", paglet)
                        except RuntimeError:
                            if not unwinding:
                                raise
            print(f"moves: {source} → {destination}, {ballast} MB, {args.samples} cold/return pairs", flush=True)

    groups = {}
    for row in rows:
        key = (row["from"], row["to"], row["ballast_mb"], row["condition"])
        groups.setdefault(key, []).append(row)
    tests = []
    for (source, destination, ballast, condition), samples in sorted(groups.items()):
        values = [{"name": "samples", "value": str(len(samples)), "unit": ""}]
        measures = {
            "capture": lambda row: row["source"]["capture_ns"] / 1e6,
            "transfer and source control, excluding destination prepare": lambda row: row["transfer_control_ns"] / 1e6,
            "destination host-record preparation": lambda row: row["destination"]["prepare_ns"] / 1e6,
            "destination activation/restore": lambda row: row["source"]["destination_activation_ns"] / 1e6,
            "destination commit": lambda row: row["destination"]["commit_ns"] / 1e6,
            "capture to arrival-hook acknowledgement": lambda row: row["source"]["arrival_ack_ns"] / 1e6,
            "CLI dispatch plus confirmed destination call": lambda row: row["confirmed_cli_ns"] / 1e6,
            "pages sent": lambda row: row["source"]["pages_sent"],
            "pages reused": lambda row: row["destination"]["pages_reused"],
            "linear memory": lambda row: row["source"]["image_pages"] * 64,
            "compressed page bytes": lambda row: row["source"]["bytes_sent"],
        }
        for name, get in measures.items():
            numbers = [get(row) for row in samples]
            unit = "ms" if name not in ["pages sent", "pages reused", "linear memory", "compressed page bytes"] else (
                "KB" if name == "linear memory" else "bytes" if name == "compressed page bytes" else "")
            for label, value in [("median", statistics.median(numbers)), ("minimum", min(numbers)), ("maximum", max(numbers))]:
                values.append({"name": f"{name}, {label}", "value": f"{value:.3f}", "unit": unit})
        tests.append({"test": f"{source} → {destination}, {ballast} MB ballast, {condition}",
                      "mode": args.mode,
                      "outcome": "passed", "seconds": sum(row["confirmed_cli_ns"] for row in samples) / 1e9,
                      "platform": f"{topology[source]['platform']} → {topology[destination]['platform']}",
                      "build": f"{source}={topology[source]['build']}; {destination}={topology[destination]['build']}",
                      "date": datetime.date.today().isoformat(), "values": values})
    result = {"topic": "live_move", "tests": tests,
              "method": "Page caches cleared before each outbound trial; modules retained after first use. Return uses retained pages. Arrival acknowledgement includes its network return; CLI latency is reported separately. Phase durations use local monotonic clocks, never subtracted cross-host timestamps."}
    pathlib.Path(args.results).write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
