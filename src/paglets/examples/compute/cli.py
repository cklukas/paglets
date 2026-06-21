# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import sys

from ...admin import DEFAULT_CONFIG_PATH, PagletsAdminClient, ServerRef, load_server_config, parse_server_arg, upsert_server_ref
from ...client import HostClient
from ...messages import Message
from ...proxy import PagletProxy
from ...serde import dataclass_to_wire
from .agent import PiComputeRequest


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        servers = load_server_config(args.config)
        for server_arg in args.server:
            servers = upsert_server_ref(servers, parse_server_arg(server_arg))
        client = HostClient(timeout=args.timeout)
        entry = _select_entry_server(servers, entry_name=args.entry, client=client)
        request = PiComputeRequest(
            start=max(0, args.start),
            digits=max(0, args.digits),
            batch_size=max(1, args.batch_size),
            max_in_flight=max(0, args.max_in_flight),
            timeout=max(0.1, args.timeout),
            max_load_per_cpu=float(args.max_load_per_cpu),
            max_cpu_percent=float(args.max_cpu_percent),
            min_memory_available_bytes=max(0, int(args.min_memory)),
            min_work_free_bytes=max(0, int(args.min_work_free)),
        )
        if args.json:
            summary = _run(entry, request, client=client)
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            summary = _run_stream(entry, request, client=client)
        return 0 if summary.get("done") and not summary.get("errors") else 1
    except Exception as exc:
        print(f"paglets-pi-compute: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compute decimal Pi digits across a paglets mesh")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Server config path")
    parser.add_argument("--server", action="append", default=[], help="One-off server in NAME=URL format")
    parser.add_argument("--entry", default=None, help="Entry server name from config")
    parser.add_argument("--start", type=int, default=0, help="Zero-based decimal digit position after the point")
    parser.add_argument("--digits", type=int, default=16, help="Number of decimal digits to compute")
    parser.add_argument("--batch-size", type=int, default=1, help="Chudnovsky terms per worker batch")
    parser.add_argument("--max-in-flight", type=int, default=0, help="Global in-flight batch cap; 0 uses target count")
    parser.add_argument("--timeout", type=float, default=60.0, help="Seconds to wait for the whole job")
    parser.add_argument("--max-load-per-cpu", type=float, default=1.0, help="Maximum 1-minute load divided by CPUs")
    parser.add_argument("--max-cpu-percent", type=float, default=90.0, help="Maximum sampled CPU percent")
    parser.add_argument("--min-memory", type=_parse_size, default=0, help="Minimum available RAM, e.g. 512M")
    parser.add_argument("--min-work-free", type=_parse_size, default=0, help="Minimum free work storage, e.g. 1G")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    return parser


def _select_entry_server(servers: list[ServerRef], *, entry_name: str | None, client: HostClient) -> ServerRef:
    candidates = servers if entry_name is None else [server for server in servers if server.name == entry_name]
    if entry_name is not None and not candidates:
        raise ValueError(f"No server named {entry_name!r} in config; pass --server {entry_name}=URL")
    if entry_name is None:
        candidates = [server for server in candidates if server.enabled]
    if not candidates:
        raise ValueError(f"No enabled servers configured in {DEFAULT_CONFIG_PATH}; pass --server NAME=URL")
    errors: list[str] = []
    for server in candidates:
        try:
            client.get_json(f"{server.url.rstrip('/')}/health", timeout=2.0)
            return server
        except Exception as exc:
            errors.append(f"{server.name}: {exc}")
    raise ValueError(f"No reachable entry server found ({'; '.join(errors)})")


def _run(entry: ServerRef, request: PiComputeRequest, *, client: HostClient) -> dict:
    proxy = _create_coordinator(entry, client=client)
    try:
        return proxy.send(Message("start", {"request": dataclass_to_wire(request)}))
    finally:
        _cleanup_coordinator(proxy)


def _run_stream(entry: ServerRef, request: PiComputeRequest, *, client: HostClient) -> dict:
    proxy = _create_coordinator(entry, client=client)
    summary: dict = {}
    cursor = 0
    try:
        proxy.send(Message("start_async", {"request": dataclass_to_wire(request)}))
        _print_stream_header(request)
        while True:
            reply = proxy.send(Message("drain", {"after_digits": cursor, "wait_timeout": 0.5}))
            summary = dict(reply.get("summary") or {})
            decimal_digits = str(summary.get("decimal_digits") or "")
            if len(decimal_digits) > cursor:
                sys.stdout.write(decimal_digits[cursor:])
                sys.stdout.flush()
                cursor = len(decimal_digits)
            if reply.get("done"):
                break
        sys.stdout.write("\n")
        _print_status(summary)
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
    try:
        proxy.send(Message("cleanup"))
    except Exception:
        pass
    try:
        proxy.dispose()
    except Exception:
        pass


def _print_stream_header(request: PiComputeRequest) -> None:
    if request.start == 0:
        sys.stdout.write("3.")
    else:
        sys.stdout.write(f"pi decimal digits [{request.start}:{request.start + request.digits}]\n")
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
