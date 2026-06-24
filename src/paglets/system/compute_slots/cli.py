# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import os
import sys

from paglets.core.runtime_values import ServiceScope
from paglets.remote.admin import ServerRef, select_reachable_entry_server
from paglets.remote.client import HostClient
from paglets.services.contracts import ServiceHandle, ServiceRecord

from .agent import (
    CANDIDATE_HOSTS,
    COMPUTE_SLOTS,
    SCHEDULER_STATUS,
    CandidateHostsRequest,
    ComputeSlotRequest,
    SchedulerStatusRequest,
)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        api_key = os.environ.get(args.api_key_env) if args.api_key_env else None
        if args.api_key_env and not api_key:
            raise ValueError(f"--api-key-env {args.api_key_env!r} is not set or is empty")
        client = HostClient(timeout=args.timeout, api_key=api_key)
        entry = select_reachable_entry_server(entry_name=args.entry, client=client)
        handle = _handle(entry, client, SCHEDULER_STATUS.name)
        if args.command == "status":
            reply = handle.call(
                SCHEDULER_STATUS,
                SchedulerStatusRequest(include_queue=args.queue, include_jobs=args.jobs),
            )
            payload = SCHEDULER_STATUS.encode_reply(reply)
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                _print_status(payload)
            return 0
        request = CandidateHostsRequest(
            slot=ComputeSlotRequest(
                cpu_cores=max(1, args.cpu_cores),
                memory_bytes=max(0, args.memory),
                temp_storage_bytes=max(0, args.temp_storage),
                requires_gpu=bool(args.gpu),
                gpu_memory_mb=max(0, args.gpu_memory),
            ),
            limit=max(0, args.limit),
        )
        reply = handle.call(CANDIDATE_HOSTS, request)
        payload = CANDIDATE_HOSTS.encode_reply(reply)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _print_candidates(payload)
        return 0 if reply.candidates else 1
    except Exception as exc:
        print(f"paglets-compute-slots: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect compute-slot scheduler services")
    parser.add_argument("--entry", default=None, help="Discovered entry host name")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    parser.add_argument(
        "--api-key-env", default=None, help="Environment variable containing the paglets bearer API key"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser("status", help="Show local scheduler status")
    status.add_argument("--queue", action="store_true", help="Include queued requests and leases")
    status.add_argument("--jobs", action="store_true", help="Include active leased job process metrics")
    candidates = subparsers.add_parser("candidates", help="Find hosts suitable for a slot request")
    candidates.add_argument("--limit", type=int, default=5, help="Maximum candidates to print")
    candidates.add_argument("--cpu-cores", type=int, default=1, help="Requested CPU cores")
    candidates.add_argument("--memory", type=_parse_size, default=0, help="Requested RAM, e.g. 512M")
    candidates.add_argument("--temp-storage", type=_parse_size, default=0, help="Requested temp storage")
    candidates.add_argument("--gpu", action="store_true", help="Require GPU support")
    candidates.add_argument("--gpu-memory", type=int, default=0, help="Requested GPU memory in MB")
    return parser


def _handle(entry: ServerRef, client: HostClient, capability: str) -> ServiceHandle:
    payload = client.get_json(
        f"{entry.url.rstrip('/')}/services?name={COMPUTE_SLOTS.name}"
        f"&capability={capability}&scope={ServiceScope.MESH.value}"
    )
    records = [ServiceRecord.from_wire(item) for item in payload.get("services", []) if isinstance(item, dict)]
    if not records:
        raise ValueError(f"No {COMPUTE_SLOTS.name!r} service advertised on {entry.name}")
    return ServiceHandle(COMPUTE_SLOTS, records[0], client)


def _print_status(payload: dict) -> None:
    status = payload["status"]
    print(
        f"{status['host_name']} cores_free={status['free_cpu_cores']} "
        f"cores_reserved={status['reserved_cpu_cores']} "
        f"ram_free={_bytes(status['free_memory_bytes'])} "
        f"ram_reserved={_bytes(status['reserved_memory_bytes'])} "
        f"temp_free={_bytes(status['free_temp_storage_bytes'])} "
        f"waiting={status['queue_length']} leases={status['active_leases']}"
    )
    if payload.get("queued_requests"):
        print("\nqueued:")
        print(f"{'request':<20} {'job':<22} {'agent':<14} {'cpu':>4} {'mem':>9} {'temp':>9}")
        for item in payload["queued_requests"]:
            print(
                f"{_short(item.get('request_id'), 20):<20} "
                f"{_short(item.get('job_id'), 22):<22} "
                f"{_short(item.get('agent_id'), 14):<14} "
                f"{int(item.get('cpu_cores') or 0):>4} "
                f"{_bytes(int(item.get('memory_bytes') or 0)):>9} "
                f"{_bytes(int(item.get('temp_storage_bytes') or 0)):>9}"
            )
    if payload.get("leases"):
        print("\nleases:")
        print(f"{'lease':<18} {'job':<22} {'decl':>4} {'reserved':>10} {'assigned':>10} {'mem':>9}")
        for item in payload["leases"]:
            request = item.get("request") or {}
            reserved = item.get("reserved_cpu_core_ids") or item.get("cpu_core_ids") or []
            assigned = item.get("cpu_core_ids") or []
            print(
                f"{_short(item.get('lease_id'), 18):<18} "
                f"{_short(request.get('job_id'), 22):<22} "
                f"{int(request.get('cpu_cores') or 0):>4} "
                f"{_core_count(reserved):>10} "
                f"{_core_count(assigned):>10} "
                f"{_bytes(int(request.get('memory_bytes') or 0)):>9}"
            )
    if payload.get("active_jobs"):
        print("\nactive jobs:")
        print(
            f"{'job':<22} {'agent':<14} {'pid':>7} {'decl':>4} {'assigned':>8} "
            f"{'mem decl':>9} {'rss':>9} {'cpu%':>7} {'mem%':>7} {'status':<10}"
        )
        for item in payload["active_jobs"]:
            print(
                f"{_short(item.get('job_id'), 22):<22} "
                f"{_short(item.get('agent_id'), 14):<14} "
                f"{int(item.get('pid') or 0):>7} "
                f"{int(item.get('declared_cpu_cores') or 0):>4} "
                f"{_core_count(item.get('assigned_cpu_core_ids') or []):>8} "
                f"{_bytes(int(item.get('declared_memory_bytes') or 0)):>9} "
                f"{_bytes(int(item.get('current_memory_rss_bytes') or 0)):>9} "
                f"{float(item.get('current_cpu_percent') or 0.0):>7.1f} "
                f"{float(item.get('current_memory_percent') or 0.0):>7.2f} "
                f"{_short(item.get('process_status') or item.get('error'), 10):<10}"
            )


def _print_candidates(payload: dict) -> None:
    print(f"{'host':<14} {'score':>7} {'cpu':>5} {'ram free':>10} {'temp free':>10} {'queue':>5}")
    for item in payload["candidates"]:
        status = item["status"]
        print(
            f"{status['host_name']:<14} {item['score']:>7.3f} {status['free_cpu_cores']:>5} "
            f"{_bytes(status['free_memory_bytes']):>10} {_bytes(status['free_temp_storage_bytes']):>10} "
            f"{status['queue_length']:>5}"
        )
    if payload.get("rejected"):
        print("\nrejected:")
        for host, reason in sorted(payload["rejected"].items()):
            print(f"  - {host}: {reason}")


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


def _short(value: object, width: int) -> str:
    text = str(value or "")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _core_count(cpu_core_ids: list[int]) -> str:
    if not cpu_core_ids:
        return "-"
    return f"{len(cpu_core_ids)}:{','.join(str(item) for item in cpu_core_ids)}"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
