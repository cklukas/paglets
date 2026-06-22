# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from ...admin import PagletsAdminClient, ServerRef, select_reachable_entry_server
from ...client import HostClient
from ...messages import Message
from ...proxy import PagletProxy
from ...serde import dataclass_from_wire, dataclass_to_wire
from .agent import (
    DEFAULT_CLOCK_PROBES,
    DEFAULT_DIGITS,
    DEFAULT_TIMEOUT_SECONDS,
    ClockOffsetSummary,
    MeshBenchmarkCoordinatorAgent,
    MeshBenchmarkCoordinatorState,
    MeshBenchmarkHost,
    MeshBenchmarkRequest,
    MeshBenchmarkSummary,
    normalize_request,
    parse_size,
)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        client = HostClient(timeout=max(1.0, args.timeout + 10.0))
        entry = _select_entry_server(entry_name=args.entry, client=client)
        request = _benchmark_request(args)
        result = _run(entry, request, client=client)
        summary_payload = dict(result.get("summary") or {})
        if args.json:
            print(json.dumps(summary_payload, indent=2, sort_keys=True))
        elif _is_summary_payload(summary_payload):
            summary = dataclass_from_wire(MeshBenchmarkSummary, summary_payload)
            print(_format_markdown(summary, digits=request.digits, include_self=request.include_self))
        elif summary_payload.get("errors"):
            _print_errors(dict(summary_payload["errors"]))
        else:
            print("paglets-mesh-benchmark: no summary returned", file=sys.stderr)
        errors = dict(result.get("errors") or {})
        errors.update(dict(summary_payload.get("errors") or {}))
        return 1 if errors else 0
    except Exception as exc:
        print(f"paglets-mesh-benchmark: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Measure directed mobile-agent travel times across a paglets mesh")
    parser.add_argument("--entry", default=None, help="Discovered entry host name")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Seconds to wait for completion")
    parser.add_argument("--repeats", type=int, default=1, help="Repeat the directed mesh route this many times")
    parser.add_argument("--payload-size", default="0", help="Random ASCII payload size, e.g. 64K, 128K, 1M")
    parser.add_argument("--exclude-self", action="store_true", help="Skip self-pair movements such as A->A")
    parser.add_argument("--digits", type=int, default=DEFAULT_DIGITS, help="Digits after the decimal point in text output")
    parser.add_argument(
        "--clock-probes",
        type=int,
        default=DEFAULT_CLOCK_PROBES,
        help="Clock request/reply probes per arrival host",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    return parser


def _benchmark_request(args: argparse.Namespace) -> MeshBenchmarkRequest:
    return normalize_request(
        MeshBenchmarkRequest(
            repeats=args.repeats,
            payload_size_bytes=parse_size(args.payload_size),
            include_self=not bool(args.exclude_self),
            timeout_seconds=args.timeout,
            digits=args.digits,
            clock_probes=args.clock_probes,
        )
    )


def _select_entry_server(*, entry_name: str | None, client: HostClient) -> ServerRef:
    return select_reachable_entry_server(entry_name=entry_name, client=client)


def _run(entry: ServerRef, request: MeshBenchmarkRequest, *, client: HostClient) -> dict[str, Any]:
    admin = PagletsAdminClient([entry], client=client)
    proxy_wire = admin.create_agent(
        entry,
        "paglets.examples.mesh_benchmark.agent:MeshBenchmarkCoordinatorAgent",
        "paglets.examples.mesh_benchmark.agent:MeshBenchmarkCoordinatorState",
        {},
    )
    proxy = PagletProxy.from_wire(proxy_wire, client)
    try:
        proxy.send(Message("start", {"request": dataclass_to_wire(request)}))
        latest: dict[str, Any] = {}
        while True:
            latest = dict(proxy.send(Message("drain", {"wait_timeout": 0.0})) or {})
            if latest.get("done"):
                return latest
            time.sleep(0.25)
    finally:
        try:
            proxy.dispose()
        except Exception:
            pass


def _format_markdown(summary: MeshBenchmarkSummary, *, digits: int, include_self: bool) -> str:
    unit_name, multiplier = _select_duration_unit(summary.average_elapsed_seconds)
    lines = [f"unit: {unit_name}", ""]
    lines.extend(_matrix_table(summary, multiplier=multiplier, digits=digits, include_self=include_self))
    lines.append("")
    lines.append(f"average travel time: {_format_decimal(summary.average_elapsed_seconds * multiplier, digits)} {unit_name}")
    lines.append(
        f"measured round trip time: "
        f"{_format_decimal(summary.measured_round_trip_seconds * multiplier, digits)} {unit_name}"
    )
    lines.append(f"measured movements: {summary.movement_count}")
    if summary.clock_offsets:
        lines.append("")
        lines.append("clock offsets vs entry:")
        lines.extend(_clock_table(summary.clock_offsets, digits=digits))
    if summary.errors:
        lines.append("")
        lines.append("errors:")
        for host, error in sorted(summary.errors.items()):
            lines.append(f"- {host}: {error}")
    return "\n".join(lines)


def _is_summary_payload(payload: dict[str, Any]) -> bool:
    return bool(payload.get("run_id") and payload.get("hosts") is not None and payload.get("matrix_seconds") is not None)


def _print_errors(errors: dict[str, str]) -> None:
    print("errors:")
    for host, error in sorted(errors.items()):
        print(f"- {host}: {error}")


def _matrix_table(
    summary: MeshBenchmarkSummary,
    *,
    multiplier: float,
    digits: int,
    include_self: bool,
) -> list[str]:
    hosts = summary.hosts
    headers = ["from \\ to"] + [host.name for host in hosts]
    rows: list[list[str]] = []
    for source in hosts:
        row = [source.name]
        values = summary.matrix_seconds.get(source.name, {})
        for target in hosts:
            value = values.get(target.name)
            if value is None or (not include_self and source.name == target.name):
                row.append("-")
            else:
                row.append(_format_decimal(value * multiplier, digits))
        rows.append(row)
    return _aligned_markdown_table(headers, rows)


def _clock_table(offsets: list[ClockOffsetSummary], *, digits: int) -> list[str]:
    rows: list[list[str]] = []
    for offset in offsets:
        unit, multiplier = _select_offset_unit(abs(offset.median_offset_seconds))
        rtt_unit, rtt_multiplier = _select_duration_unit(offset.best_rtt_seconds)
        rows.append(
            [
                offset.host_name,
                f"{_format_signed(offset.median_offset_seconds * multiplier, digits)} {unit}",
                str(offset.sample_count),
                f"{_format_decimal(offset.best_rtt_seconds * rtt_multiplier, digits)} {rtt_unit}",
            ]
        )
    return _aligned_markdown_table(["host", "median offset", "samples", "best rtt"], rows)


def _aligned_markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def render(row: list[str]) -> str:
        cells = []
        for index, cell in enumerate(row):
            if index == 0:
                cells.append(cell.ljust(widths[index]))
            else:
                cells.append(cell.rjust(widths[index]))
        return "| " + " | ".join(cells) + " |"

    separator = "| " + " | ".join("-" * width for width in widths) + " |"
    return [render(headers), separator, *(render(row) for row in rows)]


def _select_duration_unit(seconds: float) -> tuple[str, float]:
    value = abs(seconds)
    if value < 0.001:
        return "us", 1_000_000.0
    if value < 1.0:
        return "ms", 1_000.0
    if value < 60.0:
        return "s", 1.0
    if value < 3600.0:
        return "min", 1.0 / 60.0
    return "h", 1.0 / 3600.0


def _select_offset_unit(seconds: float) -> tuple[str, float]:
    if seconds < 1.0:
        return "ms", 1_000.0
    if seconds < 60.0:
        return "s", 1.0
    if seconds < 3600.0:
        return "min", 1.0 / 60.0
    return "h", 1.0 / 3600.0


def _format_decimal(value: float, digits: int) -> str:
    return f"{value:.{max(0, digits)}f}"


def _format_signed(value: float, digits: int) -> str:
    return f"{value:+.{max(0, digits)}f}"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
