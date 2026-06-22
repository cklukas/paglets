# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys

from paglets.core.messages import Message
from paglets.remote.admin import (
    PagletsAdminClient,
    ServerRef,
    select_reachable_entry_server,
)
from paglets.remote.client import HostClient
from paglets.remote.proxy import PagletProxy
from paglets.serialization.serde import dataclass_to_wire

from .models import DEFAULT_STREAM_CHUNK_DIGITS, PiComputeRequest

DEFAULT_REQUEST_TIMEOUT_SECONDS = 300.0


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        api_key = os.environ.get(args.api_key_env) if args.api_key_env else None
        if args.api_key_env and not api_key:
            raise ValueError(f"--api-key-env {args.api_key_env!r} is not set or is empty")
        client = HostClient(timeout=max(1.0, float(args.request_timeout)), api_key=api_key)
        entry = _select_entry_server(entry_name=args.entry, client=client)
        request = PiComputeRequest(
            start=max(0, args.start),
            digits=max(0, args.digits),
            batch_size=max(1, args.batch_size),
            max_in_flight=max(0, args.max_in_flight),
            max_workers_per_host=max(0, args.max_workers_per_host),
            timeout=max(0.0, args.timeout),
            max_load_per_cpu=float(args.max_load_per_cpu),
            max_cpu_percent=float(args.max_cpu_percent),
            min_memory_available_bytes=max(0, int(args.min_memory)),
            min_work_free_bytes=max(0, int(args.min_work_free)),
        )
        if args.json:
            summary = _run(entry, request, client=client)
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            summary = _run_stream(
                entry,
                request,
                client=client,
                stream_chunk_size=max(0, int(args.stream_chunk_size)),
            )
        return 0 if summary.get("done") and not summary.get("errors") else 1
    except Exception as exc:
        print(f"paglets-pi-compute: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compute decimal Pi digits across a paglets mesh")
    parser.add_argument("--entry", default=None, help="Discovered entry host name")
    parser.add_argument("--start", type=int, default=0, help="Zero-based decimal digit position after the point")
    parser.add_argument("--digits", type=int, default=16, help="Number of decimal digits to compute")
    parser.add_argument("--batch-size", type=int, default=1, help="Chudnovsky terms per worker batch")
    parser.add_argument(
        "--max-in-flight", type=int, default=0, help="Global in-flight batch cap; 0 uses free load slots"
    )
    parser.add_argument(
        "--max-workers-per-host", type=int, default=0, help="Per-host worker cap; 0 uses free load slots"
    )
    parser.add_argument("--timeout", type=float, default=0.0, help="Whole-job timeout in seconds; 0 disables it")
    parser.add_argument(
        "--stream-chunk-size",
        type=int,
        default=DEFAULT_STREAM_CHUNK_DIGITS,
        help="Maximum newly available decimal digits to return per text-mode poll; 0 disables the cap",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=DEFAULT_REQUEST_TIMEOUT_SECONDS,
        help="HTTP request timeout in seconds for coordinator calls",
    )
    parser.add_argument("--max-load-per-cpu", type=float, default=1.0, help="Maximum 1-minute load divided by CPUs")
    parser.add_argument(
        "--max-cpu-percent", type=float, default=100.0, help="Maximum sampled CPU percent (unused for scheduling)"
    )
    parser.add_argument("--min-memory", type=_parse_size, default=0, help="Minimum available RAM, e.g. 512M")
    parser.add_argument("--min-work-free", type=_parse_size, default=0, help="Minimum free work storage, e.g. 1G")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    parser.add_argument(
        "--api-key-env", default=None, help="Environment variable containing the paglets bearer API key"
    )
    return parser


def _select_entry_server(*, entry_name: str | None, client: HostClient) -> ServerRef:
    return select_reachable_entry_server(
        entry_name=entry_name,
        client=client,
    )


def _run(entry: ServerRef, request: PiComputeRequest, *, client: HostClient) -> dict:
    proxy = _create_coordinator(entry, client=client)
    try:
        proxy.send(Message("start_async", {"request": dataclass_to_wire(request)}))
        summary: dict = {}
        while True:
            reply = proxy.send(Message("drain", {"after_digits": 0, "wait_timeout": 0.5}))
            summary = dict(reply.get("summary") or {})
            if reply.get("done"):
                return summary
    finally:
        _cleanup_coordinator(proxy)


def _run_stream(
    entry: ServerRef,
    request: PiComputeRequest,
    *,
    client: HostClient,
    stream_chunk_size: int = DEFAULT_STREAM_CHUNK_DIGITS,
) -> dict:
    proxy = _create_coordinator(entry, client=client)
    summary: dict = {}
    cursor = 0
    printed_digits = 0
    stream_chunk_size = max(0, int(stream_chunk_size))
    try:
        proxy.send(Message("start_async", {"request": dataclass_to_wire(request)}))
        _print_stream_header(request)
        while True:
            reply = proxy.send(
                Message(
                    "drain_stream",
                    {
                        "after_digits": cursor,
                        "wait_timeout": 0.5,
                        "max_digits": stream_chunk_size,
                    },
                )
            )
            summary = dict(reply.get("summary") or {})
            new_decimal_digits = str(reply.get("new_decimal_digits") or "")
            if new_decimal_digits:
                _write_stream_digits(new_decimal_digits, stream_chunk_size=stream_chunk_size)
                cursor = max(cursor, int(reply.get("cursor") or 0))
                printed_digits += len(new_decimal_digits)
            if reply.get("done"):
                break
        sys.stdout.write("\n")
        _print_status(summary)
        _print_run_diagnostics(summary, printed_digits)
        return summary
    finally:
        _cleanup_coordinator(proxy)


def _create_coordinator(entry: ServerRef, *, client: HostClient) -> PagletProxy:
    admin = PagletsAdminClient([entry], client=client)
    proxy_wire = admin.create_agent(
        entry,
        "paglets.examples.compute.agent:PiComputeCoordinatorAgent",
        "paglets.examples.compute.agent:PiComputeState",
        {},
    )
    return PagletProxy.from_wire(proxy_wire, client)


def _cleanup_coordinator(proxy: PagletProxy) -> None:
    with contextlib.suppress(Exception):
        proxy.send(Message("cleanup"))
    with contextlib.suppress(Exception):
        proxy.dispose()


def _print_stream_header(request: PiComputeRequest) -> None:
    if request.start == 0:
        sys.stdout.write("3.")
    else:
        sys.stdout.write(f"pi decimal digits [{request.start}:{request.start + request.digits}]\n")
    sys.stdout.flush()


def _write_stream_digits(digits: str, *, stream_chunk_size: int) -> None:
    if stream_chunk_size <= 0:
        sys.stdout.write(digits)
        sys.stdout.flush()
        return
    for index in range(0, len(digits), stream_chunk_size):
        sys.stdout.write(digits[index : index + stream_chunk_size])
        sys.stdout.flush()


def _print_summary(summary: dict) -> None:
    start = int(summary.get("start", 0))
    digits = int(summary.get("digits", 0))
    if start == 0:
        print(summary.get("pi", ""))
    else:
        print(f"pi decimal digits [{start}:{start + digits}]")
        print(summary.get("decimal_digits", ""))
        if summary.get("pi"):
            print(f"prefix: {summary['pi']}")
    _print_status(summary)


def _print_status(summary: dict) -> None:
    if summary.get("skipped_count"):
        print(f"skipped batches requeued: {summary['skipped_count']}")
    if summary.get("errors"):
        print("\nerrors:")
        for key, error in sorted(summary["errors"].items()):
            print(f"  - {key}: {error}")
    if summary.get("cleanup_errors"):
        print("\ncleanup errors:")
        for key, error in sorted(summary["cleanup_errors"].items()):
            print(f"  - {key}: {error}")


def _print_run_diagnostics(summary: dict, printed_digits: int) -> None:
    if summary.get("errors"):
        return
    terms = int(summary.get("terms", 0))
    completed_terms = int(summary.get("completed_terms", 0))
    pending = int(summary.get("pending", 0))
    in_flight = int(summary.get("in_flight", 0))
    done = bool(summary.get("done"))

    if done and pending == 0 and in_flight == 0 and completed_terms >= terms:
        print("pi compute diagnostic: all batches received", file=sys.stderr)
    print(f"pi compute diagnostic: digits printed={printed_digits}", file=sys.stderr)


def _parse_size(value: str) -> int:
    text = value.strip()
    if not text:
        raise argparse.ArgumentTypeError("size cannot be empty")
    unit = text[-1].upper()
    if unit in {"K", "M", "G"}:
        number = text[:-1]
        multiplier = {"K": 1024, "M": 1024**2, "G": 1024**3}[unit]
    else:
        number = text[:-1] if unit == "B" else text
        multiplier = 1
    try:
        amount = float(number)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid size {value!r}") from exc
    if amount < 0:
        raise argparse.ArgumentTypeError("size must be non-negative")
    return int(amount * multiplier)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
