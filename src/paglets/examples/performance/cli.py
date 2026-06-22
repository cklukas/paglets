# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from typing import Any

from paglets.core.messages import Message
from paglets.remote.admin import (
    PagletsAdminClient,
    ServerRef,
    select_reachable_entry_server,
)
from paglets.remote.client import HostClient
from paglets.remote.proxy import PagletProxy
from paglets.serialization.serde import dataclass_from_wire, dataclass_to_wire

from .kernels import parse_size
from .models import (
    DEFAULT_BENCHMARK_DURATION_SECONDS,
    DEFAULT_DISK_SIZE_BYTES,
    DEFAULT_LOCK_TIMEOUT_SECONDS,
    BenchmarkMetric,
    BenchmarkRequest,
    HostBenchmarkResult,
)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)

    try:
        api_key = os.environ.get(args.api_key_env) if args.api_key_env else None
        if args.api_key_env and not api_key:
            raise ValueError(f"--api-key-env {args.api_key_env!r} is not set or is empty")
        client = HostClient(timeout=max(1.0, args.timeout + 10.0), api_key=api_key)
        entry = _select_entry_server(entry_name=args.entry, client=client)
        request = _benchmark_request(args)
        summary = _collect(entry, request, timeout=args.timeout, client=client)
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            _print_text(summary, verbose=bool(args.verbose or args.debug))
        return 1 if _has_failures(summary) else 0
    except Exception as exc:
        print(f"paglets-perf-test: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run performance benchmarks across a paglets mesh")
    parser.add_argument("--entry", default=None, help="Discovered entry host name")
    parser.add_argument("--timeout", type=float, default=120.0, help="Seconds to wait for mesh benchmark replies")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    parser.add_argument(
        "--api-key-env", default=None, help="Environment variable containing the paglets bearer API key"
    )
    parser.add_argument(
        "--duration", type=float, default=DEFAULT_BENCHMARK_DURATION_SECONDS, help="Seconds per CPU/memory kernel"
    )
    parser.add_argument(
        "--disk-size", default=_format_size(DEFAULT_DISK_SIZE_BYTES), help="Temporary file size per tested volume"
    )
    parser.add_argument("--workers", type=int, default=0, help="Multi-core worker count; default is logical CPU count")
    parser.add_argument("--path", action="append", default=[], help="Disk path to benchmark; repeat for multiple paths")
    parser.add_argument("--no-cpu", action="store_true", help="Skip CPU benchmarks")
    parser.add_argument("--no-memory", action="store_true", help="Skip memory benchmarks")
    parser.add_argument("--no-disk", action="store_true", help="Skip disk benchmarks")
    parser.add_argument(
        "--lock-timeout",
        type=float,
        default=DEFAULT_LOCK_TIMEOUT_SECONDS,
        help="Seconds to wait for local benchmark lock",
    )
    parser.add_argument("--verbose", action="store_true", help="Print skipped disk targets and cleanup diagnostics")
    parser.add_argument("--debug", action="store_true", help="Print verbose benchmark diagnostics")
    return parser


def _benchmark_request(args: argparse.Namespace) -> BenchmarkRequest:
    return BenchmarkRequest(
        include_cpu=not args.no_cpu,
        include_memory=not args.no_memory,
        include_disk=not args.no_disk,
        duration_seconds=max(0.01, args.duration),
        disk_size_bytes=parse_size(args.disk_size),
        workers=max(0, args.workers),
        paths=list(args.path),
        lock_timeout_seconds=max(0.0, args.lock_timeout),
    )


def _select_entry_server(*, entry_name: str | None, client: HostClient) -> ServerRef:
    return select_reachable_entry_server(
        entry_name=entry_name,
        client=client,
    )


def _collect(entry: ServerRef, request: BenchmarkRequest, *, timeout: float, client: HostClient) -> dict[str, Any]:
    admin = PagletsAdminClient([entry], client=client)
    proxy_wire = admin.create_agent(
        entry,
        "paglets.examples.performance.agent:PerformanceBenchmarkAgent",
        "paglets.examples.performance.agent:PerformanceBenchmarkState",
        {},
    )
    proxy = PagletProxy.from_wire(proxy_wire, client)
    try:
        proxy.send(
            Message(
                "collect",
                {
                    "request": dataclass_to_wire(request),
                    "timeout": timeout,
                },
            )
        )
        summary: dict[str, Any] = {}
        while True:
            reply = proxy.send(Message("drain", {"wait_timeout": 0.5}))
            summary = dict(reply.get("summary") or {})
            if reply.get("done"):
                return summary
    finally:
        with contextlib.suppress(Exception):
            proxy.send(Message("cleanup"))
        with contextlib.suppress(Exception):
            proxy.dispose()


def _print_text(summary: dict[str, Any], *, verbose: bool = False) -> None:
    print(
        f"{'host':<14} {'int/s':>10} {'float/s':>10} {'sha':>10} "
        f"{'multi-int/s':>12} {'mem copy':>10} {'disk wr':>10} {'disk rd':>10} {'err':>3}"
    )
    results = summary.get("results", {})
    for host, item in sorted(results.items()):
        result = dataclass_from_wire(HostBenchmarkResult, item["result"])
        single_int = _metric(result.cpu.single_core if result.cpu else [], "integer")
        single_float = _metric(result.cpu.single_core if result.cpu else [], "float")
        single_sha = _metric(result.cpu.single_core if result.cpu else [], "sha256")
        multi_int = _metric(result.cpu.multi_core if result.cpu else [], "integer-multi")
        mem_copy = _metric(result.memory.metrics if result.memory else [], "memory-copy")
        disk_write, disk_read = _best_disk_rates(result)
        error_count = len(result.errors)
        if result.cpu:
            error_count += len(result.cpu.errors)
        if result.memory:
            error_count += len(result.memory.errors)
        if result.disk:
            error_count += len(result.disk.errors)
        print(
            f"{host:<14} {_ops(single_int):>10} {_ops(single_float):>10} {_bytes_per_second(single_sha):>10} "
            f"{_ops(multi_int):>12} {_bytes_per_second(mem_copy):>10} {_bytes_per_second_value(disk_write):>10} "
            f"{_bytes_per_second_value(disk_read):>10} {error_count:>3}"
        )

    _print_disk_details(summary)
    _print_errors(summary, verbose=verbose)


def _print_disk_details(summary: dict[str, Any]) -> None:
    printed_header = False
    for host, item in sorted(summary.get("results", {}).items()):
        result = dataclass_from_wire(HostBenchmarkResult, item["result"])
        if not result.disk or not result.disk.volumes:
            continue
        if not printed_header:
            print("\ndisks:")
            print(f"{'host':<14} {'path':<32} {'size':>9} {'write':>10} {'read':>10} {'metadata':>10}")
            printed_header = True
        for volume in result.disk.volumes:
            print(
                f"{host:<14} {volume.path:<32.32} {_bytes(volume.benchmark_size_bytes):>9} "
                f"{_bytes_per_second_value(volume.write_bytes_per_second):>10} "
                f"{_bytes_per_second_value(volume.read_bytes_per_second):>10} "
                f"{volume.metadata_files_per_second:>9.0f}/s"
            )


def _print_errors(summary: dict[str, Any], *, verbose: bool = False) -> None:
    lines: list[str] = []
    for host, error in sorted((summary.get("errors") or {}).items()):
        lines.append(f"{host}: {error}")
    for host, error in sorted((summary.get("cleanup_errors") or {}).items()):
        lines.append(f"{host}: cleanup failed: {error}")
    for host, item in sorted((summary.get("results") or {}).items()):
        result = dataclass_from_wire(HostBenchmarkResult, item["result"])
        for error in result.errors:
            lines.append(f"{host}: {error}")
        if result.cpu:
            lines.extend(f"{host}: {error}" for error in result.cpu.errors)
        if result.memory:
            lines.extend(f"{host}: {error}" for error in result.memory.errors)
        if result.disk:
            lines.extend(f"{host}: {error}" for error in result.disk.errors)
            if verbose:
                lines.extend(f"{host}: skipped {skip.path}: {skip.reason}" for skip in result.disk.skipped)
    if lines:
        print("\nnotes:")
        for line in lines:
            print(f"  - {line}")


def _has_failures(summary: dict[str, Any]) -> bool:
    if summary.get("errors") or summary.get("cleanup_errors"):
        return True
    for item in (summary.get("results") or {}).values():
        result = dataclass_from_wire(HostBenchmarkResult, item["result"])
        if result.errors:
            return True
        if result.cpu and result.cpu.errors:
            return True
        if result.memory and result.memory.errors:
            return True
        if result.disk and result.disk.errors:
            return True
    return False


def _metric(metrics: list[BenchmarkMetric], name: str) -> BenchmarkMetric | None:
    for metric in metrics:
        if metric.name == name:
            return metric
    return None


def _best_disk_rates(result: HostBenchmarkResult) -> tuple[float, float]:
    if result.disk is None or not result.disk.volumes:
        return 0.0, 0.0
    return (
        max(volume.write_bytes_per_second for volume in result.disk.volumes),
        max(volume.read_bytes_per_second for volume in result.disk.volumes),
    )


def _ops(metric: BenchmarkMetric | None) -> str:
    if metric is None:
        return "-"
    return _rate(metric.operations_per_second)


def _rate(value: float) -> str:
    units = ("", "K", "M", "G")
    amount = float(value)
    for unit in units:
        if abs(amount) < 1000.0 or unit == units[-1]:
            return f"{amount:.1f}{unit}"
        amount /= 1000.0
    return f"{amount:.1f}G"


def _bytes_per_second(metric: BenchmarkMetric | None) -> str:
    if metric is None:
        return "-"
    return _bytes_per_second_value(metric.bytes_per_second)


def _bytes_per_second_value(value: float) -> str:
    return f"{_bytes(int(value))}/s" if value > 0 else "-"


def _bytes(value: int) -> str:
    units = ("B", "K", "M", "G", "T", "P")
    amount = float(value)
    for unit in units:
        if abs(amount) < 1024.0 or unit == units[-1]:
            return f"{amount:.1f}{unit}" if unit != "B" else f"{int(amount)}B"
        amount /= 1024.0
    return f"{amount:.1f}P"


def _format_size(value: int) -> str:
    if value % (1024**3) == 0:
        return f"{value // (1024**3)}G"
    if value % (1024**2) == 0:
        return f"{value // (1024**2)}M"
    if value % 1024 == 0:
        return f"{value // 1024}K"
    return str(value)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
