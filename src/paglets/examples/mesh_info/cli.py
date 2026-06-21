# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import sys
import time

from ...admin import DEFAULT_CONFIG_PATH, ServerRef, load_server_config, parse_server_arg, upsert_server_ref
from ...client import HostClient
from ...runtime_values import ServiceScope
from ...services import ServiceHandle, ServiceRecord
from .agent import (
    GET_LANDSCAPE,
    MESH_INFO,
    SELECT_TARGETS,
    LandscapeRequest,
    TargetSelectionRequest,
)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        servers = load_server_config(args.config)
        for server_arg in args.server:
            servers = upsert_server_ref(servers, parse_server_arg(server_arg))
        client = HostClient(timeout=args.timeout)
        entry = _select_entry_server(servers, entry_name=args.entry, client=client)
        handle = _mesh_info_handle(entry, client)
        if args.command == "targets":
            reply = handle.call(SELECT_TARGETS, _selection_request(args))
            payload = SELECT_TARGETS.encode_reply(reply)
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                _print_targets(reply)
            return 0 if reply.targets else 1

        reply = handle.call(
            GET_LANDSCAPE,
            LandscapeRequest(fresh_only=True, max_age_seconds=max(0.0, args.max_age), limit=max(0, args.limit)),
        )
        payload = GET_LANDSCAPE.encode_reply(reply)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _print_summary(reply)
        return 0 if reply.hosts else 1
    except Exception as exc:
        print(f"paglets-mesh-info: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show the mesh-info resource landscape")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Server config path")
    parser.add_argument("--server", action="append", default=[], help="One-off server in NAME=URL format")
    parser.add_argument("--entry", default=None, help="Entry server name from config")
    parser.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout in seconds")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    summary = subparsers.add_parser("summary", help="Show known fresh mesh snapshots")
    summary.add_argument("--max-age", type=float, default=0.0, help="Freshness cutoff in seconds; 0 uses service default")
    summary.add_argument("--limit", type=int, default=0, help="Maximum hosts to print")

    targets = subparsers.add_parser("targets", help="Show ranked eligible compute targets")
    targets.add_argument("--limit", type=int, default=5, help="Maximum targets to print")
    targets.add_argument("--max-age", type=float, default=0.0, help="Freshness cutoff in seconds; 0 uses service default")
    targets.add_argument("--max-load-per-cpu", type=float, default=1.0, help="Maximum 1-minute load divided by CPUs")
    targets.add_argument("--max-cpu-percent", type=float, default=90.0, help="Maximum sampled CPU percent")
    targets.add_argument("--min-memory", type=_parse_size, default=0, help="Minimum available RAM, e.g. 512M")
    targets.add_argument("--min-work-free", type=_parse_size, default=0, help="Minimum free work storage, e.g. 1G")
    targets.add_argument("--exclude-self", action="store_true", help="Exclude the entry host")
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


def _mesh_info_handle(entry: ServerRef, client: HostClient) -> ServiceHandle:
    payload = client.get_json(
        f"{entry.url.rstrip('/')}/services?name={MESH_INFO.name}&capability={GET_LANDSCAPE.name}&scope={ServiceScope.MESH.value}"
    )
    records = [ServiceRecord.from_wire(item) for item in payload.get("services", []) if isinstance(item, dict)]
    if not records:
        raise ValueError(f"No {MESH_INFO.name!r} service advertised on {entry.name}")
    return ServiceHandle(MESH_INFO, records[0], client)


def _selection_request(args: argparse.Namespace) -> TargetSelectionRequest:
    return TargetSelectionRequest(
        limit=max(1, args.limit),
        max_age_seconds=max(0.0, args.max_age),
        max_load_per_cpu=float(args.max_load_per_cpu),
        max_cpu_percent=float(args.max_cpu_percent),
        min_memory_available_bytes=max(0, int(args.min_memory)),
        min_work_free_bytes=max(0, int(args.min_work_free)),
        include_self=not bool(args.exclude_self),
    )


def _print_summary(reply) -> None:
    print(f"{'host':<14} {'age':>6} {'cpu%':>6} {'load/cpu':>8} {'ram free':>10} {'work free':>10} errors")
    now = time.time()
    for snapshot in reply.hosts:
        age = max(0.0, now - snapshot.observed_at)
        errors = "; ".join(snapshot.errors)
        print(
            f"{snapshot.host_name:<14} {age:>5.1f}s {snapshot.cpu_percent:>6.1f} "
            f"{snapshot.load_per_cpu:>8.2f} {_bytes(snapshot.memory_available_bytes):>10} "
            f"{_bytes(snapshot.work_free_bytes):>10} {errors}"
        )
    _print_errors(reply.errors)


def _print_targets(reply) -> None:
    print(f"{'host':<14} {'score':>7} {'cpu%':>6} {'load/cpu':>8} {'ram free':>10} {'work free':>10}")
    for target in reply.targets:
        snapshot = target.snapshot
        print(
            f"{snapshot.host_name:<14} {target.score:>7.3f} {snapshot.cpu_percent:>6.1f} "
            f"{snapshot.load_per_cpu:>8.2f} {_bytes(snapshot.memory_available_bytes):>10} "
            f"{_bytes(snapshot.work_free_bytes):>10}"
        )
    if reply.rejected:
        print("\nrejected:")
        for host, reason in sorted(reply.rejected.items()):
            print(f"  - {host}: {reason}")
    _print_errors(reply.errors)


def _print_errors(errors: dict[str, str]) -> None:
    if not errors:
        return
    print("\nerrors:")
    for host, error in sorted(errors.items()):
        print(f"  - {host}: {error}")


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


def _bytes(value: int) -> str:
    units = ("B", "K", "M", "G", "T", "P")
    amount = float(value)
    for unit in units:
        if abs(amount) < 1024.0 or unit == units[-1]:
            return f"{amount:.1f}{unit}" if unit != "B" else f"{int(amount)}B"
        amount /= 1024.0
    return f"{value}B"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
