# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import sys
import time

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.core.runtime_values import ServiceScope
from paglets.remote.admin import AgentRecord, PagletsAdminClient, ServerRef, select_reachable_entry_server
from paglets.remote.client import HostClient
from paglets.services.contracts import ServiceHandle, ServiceRecord

from .agent import (
    CANCEL_SLOT_REQUESTS,
    CANDIDATE_HOSTS,
    COMPUTE_SLOTS,
    SCHEDULER_STATUS,
    CancelSlotRequestsRequest,
    CandidateHostsRequest,
    ComputeSlotRequest,
    SchedulerStatusRequest,
)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        api_key = resolve_api_key(args.api_key_env)
        client = HostClient(timeout=args.timeout, api_key=api_key)
        entry = select_reachable_entry_server(entry_name=args.entry, client=client)
        if args.command == "status":
            handle = _handle(entry, client, SCHEDULER_STATUS.name)
            reply = handle.call(
                SCHEDULER_STATUS,
                SchedulerStatusRequest(
                    include_queue=args.queue or args.blocked or args.usage,
                    include_jobs=args.jobs or args.usage,
                    include_usage=args.usage,
                ),
            )
            payload = SCHEDULER_STATUS.encode_reply(reply)
            if args.usage:
                _apply_lease_times_to_active_jobs(payload)
            if args.blocked:
                payload["blocked_requests"] = _blocked_request_payload(payload)
                if not args.queue and not args.json:
                    payload["_hide_queued_requests"] = True
            if args.usage and not args.queue and not args.blocked and not args.json:
                payload["_hide_queued_requests"] = True
                payload["_hide_leases"] = True
            if args.usage and not args.json:
                payload["_include_usage"] = True
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                _print_status(payload)
            return 0
        if args.command == "cancel":
            return _run_cancel(args, entry, client)
        if args.command == "jobs":
            return _run_jobs(args, entry, client)
        handle = _handle(entry, client, CANDIDATE_HOSTS.name)
        request = CandidateHostsRequest(
            slot=ComputeSlotRequest(
                cpu_cores=max(1, args.cpu_cores),
                memory_bytes=max(0, args.memory),
                temp_storage_bytes=max(0, args.temp_storage),
                requires_gpu=bool(args.gpu),
                gpu_memory_mb=max(0, args.gpu_memory),
                required_host_tags=tuple(args.require_tag),
                excluded_host_tags=tuple(args.exclude_tag),
                preferred_host_tags=tuple(args.prefer_tag),
                excluded_host_names=_excluded_host_names(args.exclude_host),
                excluded_host_urls=_excluded_host_urls(args.exclude_host),
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
        "--api-key-env",
        default=None,
        help=f"Environment variable to read the paglets bearer API key from; defaults to {DEFAULT_API_KEY_ENV}",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser("status", help="Show local scheduler status")
    _add_json_arg(status)
    status.add_argument("--queue", action="store_true", help="Include queued requests and leases")
    status.add_argument("--jobs", action="store_true", help="Include active leased job process metrics")
    status.add_argument("--blocked", action="store_true", help="Explain resource limits blocking queued requests")
    status.add_argument("--usage", action="store_true", help="Include process-tree memory and Paglets work-dir usage")
    cancel = subparsers.add_parser("cancel", help="Cancel queued slot requests")
    _add_json_arg(cancel)
    cancel.add_argument("--request-id", action="append", default=[], help="Cancel a queued request ID; repeatable")
    cancel.add_argument(
        "--agent-id", action="append", default=[], help="Cancel queued requests for an agent; repeatable"
    )
    cancel.add_argument("--job-id", action="append", default=[], help="Cancel queued requests for a job; repeatable")
    cancel.add_argument("--all", action="store_true", help="Cancel all queued requests")
    cancel.add_argument("--include-leases", action="store_true", help="Also cancel matching active leases")
    cancel.add_argument("--dry-run", action="store_true", help="Print matching queued requests without cancelling")
    cancel.add_argument("--confirm", action="store_true", help="Confirm cancellation")
    jobs = subparsers.add_parser("jobs", help="List or clear compute job paglets")
    jobs_subparsers = jobs.add_subparsers(dest="jobs_command", required=True)
    jobs_list = jobs_subparsers.add_parser("list", help="List compute job paglets")
    _add_json_arg(jobs_list)
    _add_job_filter_args(jobs_list)
    jobs_list.add_argument("--active", action="store_true", help="Include active compute jobs")
    jobs_list.add_argument("--inactive", action="store_true", help="Include inactive compute jobs")
    jobs_clear = jobs_subparsers.add_parser("clear", help="Dispose inactive compute job paglets")
    _add_json_arg(jobs_clear)
    _add_job_filter_args(jobs_clear)
    jobs_clear.add_argument("--dry-run", action="store_true", help="Print matching jobs without disposing")
    jobs_clear.add_argument("--confirm", action="store_true", help="Confirm disposal")
    jobs_history = jobs_subparsers.add_parser("history", help="Show recent finished compute job usage")
    _add_json_arg(jobs_history)
    jobs_history.add_argument("--limit", type=int, default=20, help="Maximum finished jobs to print; 0 prints all")
    candidates = subparsers.add_parser("candidates", help="Find hosts suitable for a slot request")
    _add_json_arg(candidates)
    candidates.add_argument("--limit", type=int, default=5, help="Maximum candidates to print")
    candidates.add_argument("--cpu-cores", type=int, default=1, help="Requested CPU cores")
    candidates.add_argument("--memory", type=_parse_size, default=0, help="Requested RAM, e.g. 512M")
    candidates.add_argument("--temp-storage", type=_parse_size, default=0, help="Requested temp storage")
    candidates.add_argument("--gpu", action="store_true", help="Require GPU support")
    candidates.add_argument("--gpu-memory", type=int, default=0, help="Requested GPU memory in MB")
    candidates.add_argument("--require-tag", action="append", default=[], help="Require a host tag; repeatable")
    candidates.add_argument("--exclude-tag", action="append", default=[], help="Reject hosts with this tag; repeatable")
    candidates.add_argument("--prefer-tag", action="append", default=[], help="Prefer hosts with this tag; repeatable")
    candidates.add_argument("--exclude-host", action="append", default=[], help="Reject a host name or URL; repeatable")
    return parser


def _add_json_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        help="Print machine-readable JSON",
    )


def _add_job_filter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--agent-id", action="append", default=[], help="Filter by agent ID; repeatable")
    parser.add_argument("--job-id", action="append", default=[], help="Filter by compute job ID; repeatable")
    parser.add_argument("--status", action="append", default=[], help="Filter by compute or job status; repeatable")
    parser.add_argument("--class-name", action="append", default=[], help="Filter by class name or suffix; repeatable")


def _run_cancel(args: argparse.Namespace, entry: ServerRef, client: HostClient) -> int:
    request = CancelSlotRequestsRequest(
        request_ids=tuple(args.request_id),
        agent_ids=tuple(args.agent_id),
        job_ids=tuple(args.job_id),
        all=bool(args.all),
        include_leases=bool(args.include_leases),
    )
    if not request.all and not request.request_ids and not request.agent_ids and not request.job_ids:
        raise ValueError("cancel requires at least one filter or --all")
    handle = _handle(entry, client, CANCEL_SLOT_REQUESTS.name)
    if args.dry_run:
        status_reply = _handle(entry, client, SCHEDULER_STATUS.name).call(
            SCHEDULER_STATUS,
            SchedulerStatusRequest(include_queue=True, include_jobs=False),
        )
        payload = _cancel_preview_payload(status_reply, request)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _print_cancel_preview(payload)
        return 0
    if not args.confirm:
        raise ValueError("cancel requires --confirm (or use --dry-run)")
    reply = handle.call(CANCEL_SLOT_REQUESTS, request)
    payload = CANCEL_SLOT_REQUESTS.encode_reply(reply)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"cancelled queued_requests={payload['cancelled_requests']} leases={payload['cancelled_leases']}")
    return 0


def _run_jobs(args: argparse.Namespace, entry: ServerRef, client: HostClient) -> int:
    if args.jobs_command == "history":
        handle = _handle(entry, client, SCHEDULER_STATUS.name)
        reply = handle.call(SCHEDULER_STATUS, SchedulerStatusRequest(include_usage_history=True))
        history = SCHEDULER_STATUS.encode_reply(reply).get("finished_usage") or []
        limit = max(0, int(args.limit))
        if limit:
            history = history[-limit:]
        if args.json:
            print(json.dumps({"finished_usage": history}, indent=2, sort_keys=True))
        else:
            _print_usage_history(history)
        return 0

    admin = PagletsAdminClient([entry], client=client)
    if args.jobs_command == "list":
        include_active, include_inactive = _jobs_list_inclusion(args)
        jobs = _load_compute_jobs(
            admin,
            entry,
            include_active=include_active,
            include_inactive=include_inactive,
            agent_ids=tuple(args.agent_id),
            job_ids=tuple(args.job_id),
            statuses=tuple(args.status),
            class_names=tuple(args.class_name),
        )
        if args.json:
            print(json.dumps({"jobs": _public_jobs(jobs)}, indent=2, sort_keys=True))
        else:
            _print_jobs(jobs)
        return 0

    statuses = tuple(args.status) if args.status else ("WAITING_FOR_SLOT",)
    jobs = _load_compute_jobs(
        admin,
        entry,
        include_active=False,
        include_inactive=True,
        agent_ids=tuple(args.agent_id),
        job_ids=tuple(args.job_id),
        statuses=statuses,
        class_names=tuple(args.class_name),
    )
    if args.dry_run:
        if args.json:
            print(
                json.dumps(
                    {"dry_run": True, "matched": len(jobs), "jobs": _public_jobs(jobs)},
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print(f"matched {len(jobs)} inactive compute job(s)")
            _print_jobs(jobs)
        return 0
    if not args.confirm:
        raise ValueError("jobs clear requires --confirm (or use --dry-run)")
    disposed = 0
    errors: dict[str, str] = {}
    by_agent_id = {job["agent_id"]: job["_agent"] for job in jobs}
    for agent_id, agent in by_agent_id.items():
        try:
            admin.dispose(agent)
        except Exception as exc:
            errors[agent_id] = str(exc)
        else:
            disposed += 1
    payload = {"disposed": disposed, "errors": errors}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"disposed {disposed} inactive compute job(s)")
        for agent_id, error in sorted(errors.items()):
            print(f"  - {agent_id}: {error}")
    return 1 if errors else 0


def _jobs_list_inclusion(args: argparse.Namespace) -> tuple[bool, bool]:
    include_active = bool(args.active)
    include_inactive = bool(args.inactive)
    if not include_active and not include_inactive:
        return True, True
    return include_active, include_inactive


def _handle(entry: ServerRef, client: HostClient, capability: str) -> ServiceHandle:
    payload = client.get_json(
        f"{entry.url.rstrip('/')}/services?name={COMPUTE_SLOTS.name}"
        f"&capability={capability}&scope={ServiceScope.MESH.value}"
    )
    records = [ServiceRecord.from_wire(item) for item in payload.get("services", []) if isinstance(item, dict)]
    if not records:
        raise ValueError(f"No {COMPUTE_SLOTS.name!r} service advertised on {entry.name}")
    return ServiceHandle(COMPUTE_SLOTS, records[0], client)


def _load_compute_jobs(
    admin: PagletsAdminClient,
    entry: ServerRef,
    *,
    include_active: bool,
    include_inactive: bool,
    agent_ids: tuple[str, ...] = (),
    job_ids: tuple[str, ...] = (),
    statuses: tuple[str, ...] = (),
    class_names: tuple[str, ...] = (),
) -> list[dict[str, object]]:
    jobs: list[dict[str, object]] = []
    for item in admin.list_agent_payloads(entry, include_state=True):
        agent_active = bool(item.get("active"))
        if agent_active and not include_active:
            continue
        if not agent_active and not include_inactive:
            continue
        agent_id = str(item.get("agent_id") or "")
        if agent_ids and agent_id not in agent_ids:
            continue
        class_name = str(item.get("class_name") or "")
        if class_names and not _matches_class_name(class_name, class_names):
            continue
        state = item.get("state")
        if state is None:
            try:
                payload = admin.get_agent_state(_agent_record_from_payload(entry, item))
            except Exception:
                continue
            state = payload.get("state") if isinstance(payload, dict) else None
        if not isinstance(state, dict) or "compute_status" not in state:
            continue
        job_id = str(state.get("job_id") or state.get("slot_request_id") or "")
        if job_ids and job_id not in job_ids:
            continue
        compute_status = str(state.get("compute_status") or "")
        job_status = str(state.get("status") or "")
        if statuses and compute_status not in statuses and job_status not in statuses:
            continue
        jobs.append(
            {
                "agent_id": agent_id,
                "active": agent_active,
                "class_name": class_name,
                "state_class_name": str(item.get("state_class_name") or ""),
                "host": str(item.get("server_name") or entry.name),
                "host_url": str(item.get("host_url") or item.get("address") or entry.url),
                "job_id": job_id,
                "compute_status": compute_status,
                "status": job_status,
                "request_id": str(state.get("slot_request_id") or ""),
                "lease_id": str(state.get("slot_lease_id") or ""),
                "deactivated_at": item.get("deactivated_at"),
                "_agent": _agent_record_from_payload(entry, item),
            }
        )
    jobs.sort(key=lambda item: (str(item["host"]), str(item["job_id"]), str(item["agent_id"])))
    return jobs


def _agent_record_from_payload(entry: ServerRef, item: dict[str, object]) -> AgentRecord:
    return AgentRecord(
        server_name=str(item.get("server_name") or entry.name),
        host_url=str(item.get("host_url") or item.get("address") or entry.url),
        agent_id=str(item.get("agent_id") or ""),
        class_name=str(item.get("class_name") or ""),
        state_class_name=str(item.get("state_class_name") or ""),
        active=bool(item.get("active")),
    )


def _public_jobs(jobs: list[dict[str, object]]) -> list[dict[str, object]]:
    return [{key: value for key, value in item.items() if key != "_agent"} for item in jobs]


def _matches_class_name(class_name: str, filters: tuple[str, ...]) -> bool:
    return any(class_name == item or class_name.endswith(item) for item in filters)


def _cancel_preview_payload(reply, request: CancelSlotRequestsRequest) -> dict[str, object]:
    queued = [item for item in reply.queued_requests if _matches_cancel_preview(item, request)]
    leases = [
        item for item in reply.leases if request.include_leases and _matches_cancel_preview(item.request, request)
    ]
    return {
        "dry_run": True,
        "matched_requests": [_wire_dataclass(item) for item in queued],
        "matched_leases": [_wire_dataclass(item) for item in leases],
    }


def _wire_dataclass(value) -> dict:
    from paglets.serialization.codec import dataclass_to_wire

    return dataclass_to_wire(value)


def _matches_cancel_preview(slot_request: ComputeSlotRequest, request: CancelSlotRequestsRequest) -> bool:
    if request.all:
        return True
    if slot_request.request_id and slot_request.request_id in request.request_ids:
        return True
    if slot_request.agent_id and slot_request.agent_id in request.agent_ids:
        return True
    return bool(slot_request.job_id and slot_request.job_id in request.job_ids)


def _print_cancel_preview(payload: dict[str, object]) -> None:
    requests = payload.get("matched_requests") or []
    leases = payload.get("matched_leases") or []
    print(f"matched queued_requests={len(requests)} leases={len(leases)}")


def _blocked_request_payload(payload: dict) -> list[dict[str, object]]:
    status = payload.get("status") or {}
    queued = payload.get("queued_requests") or []
    result: list[dict[str, object]] = []
    free_cpu = int(status.get("free_cpu_cores") or 0)
    free_memory = int(status.get("free_memory_bytes") or 0)
    free_temp = int(status.get("free_temp_storage_bytes") or 0)
    for item in queued:
        blockers = _request_blockers(status, item, free_cpu=free_cpu, free_memory=free_memory, free_temp=free_temp)
        if not blockers:
            blockers = ["grantable"]
            free_cpu -= int(item.get("cpu_cores") or 0)
            free_memory -= int(item.get("memory_bytes") or 0)
            free_temp -= int(item.get("temp_storage_bytes") or 0)
        result.append(
            {
                "request_id": item.get("request_id") or "",
                "job_id": item.get("job_id") or "",
                "agent_id": item.get("agent_id") or "",
                "blockers": blockers,
            }
        )
    return result


def _request_blockers(
    status: dict,
    request: dict,
    *,
    free_cpu: int,
    free_memory: int,
    free_temp: int,
) -> list[str]:
    blockers: list[str] = []
    if status.get("errors"):
        blockers.append("host-status")
    max_load_per_cpu = float(status.get("max_load_per_cpu") or 0.0)
    if max_load_per_cpu > 0 and float(status.get("load_per_cpu") or 0.0) >= max_load_per_cpu:
        blockers.append("load")
    if int(request.get("cpu_cores") or 0) > free_cpu:
        blockers.append("cpu")
    if int(request.get("memory_bytes") or 0) > free_memory:
        blockers.append("memory")
    if int(request.get("temp_storage_bytes") or 0) > free_temp:
        blockers.append("temp-storage")
    return blockers


def _apply_lease_times_to_active_jobs(payload: dict) -> None:
    leases = {item.get("lease_id"): item for item in payload.get("leases") or []}
    for item in payload.get("active_jobs") or []:
        lease = leases.get(item.get("lease_id"))
        if lease is not None and not item.get("granted_at"):
            item["granted_at"] = lease.get("granted_at") or 0.0


def _print_jobs(jobs: list[dict[str, object]]) -> None:
    _print_table(
        ["agent", "state", "compute", "status", "job", "request", "class"],
        [
            [
                _short(item.get("agent_id"), 14),
                "active" if item.get("active") else "inactive",
                _short(item.get("compute_status"), 18),
                _short(item.get("status"), 22),
                _short(item.get("job_id"), 22),
                _short(item.get("request_id"), 20),
                _short(str(item.get("class_name")).split(":")[-1], 24),
            ]
            for item in jobs
        ],
    )


def _print_status(payload: dict) -> None:
    status = payload["status"]
    print(
        f"{status['host_name']} cores_free={status['free_cpu_cores']} "
        f"cores_reserved={status['reserved_cpu_cores']} "
        f"load={float(status.get('load_per_cpu') or 0.0):.2f}/{float(status.get('max_load_per_cpu') or 0.0):.2f} "
        f"ram_free={_bytes(status['free_memory_bytes'])} "
        f"ram_reserved={_bytes(status['reserved_memory_bytes'])} "
        f"temp_free={_bytes(status['free_temp_storage_bytes'])} "
        f"temp_reserved={_bytes(int(status.get('reserved_temp_storage_bytes') or 0))} "
        f"waiting={status['queue_length']} leases={status['active_leases']}"
    )
    if payload.get("queued_requests") and not payload.get("_hide_queued_requests"):
        print("\nqueued:")
        _print_table(
            ["request", "job", "agent", "cpu", "mem", "temp"],
            [
                [
                    _short(item.get("request_id"), 20),
                    _short(item.get("job_id"), 22),
                    _short(item.get("agent_id"), 14),
                    int(item.get("cpu_cores") or 0),
                    _bytes(int(item.get("memory_bytes") or 0)),
                    _bytes(int(item.get("temp_storage_bytes") or 0)),
                ]
                for item in payload["queued_requests"]
            ],
            right={"cpu", "mem", "temp"},
        )
    if payload.get("blocked_requests"):
        counts: dict[str, int] = {}
        for item in payload["blocked_requests"]:
            for blocker in item.get("blockers") or []:
                counts[str(blocker)] = counts.get(str(blocker), 0) + 1
        summary = ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
        print(f"\nblocked: {summary}")
        _print_table(
            ["request", "job", "blockers"],
            [
                [
                    _short(item.get("request_id"), 20),
                    _short(item.get("job_id"), 22),
                    ",".join(str(value) for value in item.get("blockers") or []),
                ]
                for item in payload["blocked_requests"][:10]
            ],
        )
        remaining = len(payload["blocked_requests"]) - 10
        if remaining > 0:
            print(f"... {remaining} more queued request(s)")
    if payload.get("leases") and not payload.get("_hide_leases"):
        print("\nleases:")
        _print_table(
            ["lease", "job", "decl", "resv", "aff", "mem", "temp"],
            [_lease_row(item) for item in payload["leases"]],
            right={"decl", "resv", "aff", "mem", "temp"},
        )
        print("\nlease cpus:")
        _print_table(
            ["lease", "reserved cpus", "affinity cpus"],
            [_lease_cpu_row(item) for item in payload["leases"]],
        )
    if payload.get("active_jobs"):
        include_usage = bool(payload.get("_include_usage"))
        print("\nactive jobs:")
        headers = ["job", "agent", "pid", "cpu", "aff", "mem", "rss", "status"]
        if include_usage:
            usage_headers = [
                "job",
                "runtime",
                "tree rss",
                "max rss",
                "max tree",
                "work",
                "extra",
                "files",
                "max disk",
                "samples",
            ]
        else:
            headers.extend(["cpu%", "mem%", "affinity cpus"])
        _print_table(
            headers,
            [_active_job_row(item, include_usage=include_usage) for item in payload["active_jobs"]],
            right={
                "pid",
                "cpu",
                "aff",
                "mem",
                "rss",
                "tree rss",
                "work",
                "extra",
                "files",
                "max disk",
                "samples",
                "cpu%",
                "mem%",
            },
        )
        if include_usage:
            print("\nactive usage:")
            _print_table(
                usage_headers,
                [_active_usage_row(item) for item in payload["active_jobs"]],
                right={"tree rss", "max rss", "max tree", "work", "extra", "files", "max disk", "samples"},
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


def _print_usage_history(history: list[dict[str, object]]) -> None:
    _print_table(
        ["finished", "runtime", "reason", "job", "class", "max cpu", "max rss", "max disk"],
        [
            [
                _format_time(float(item.get("finished_at") or 0.0)),
                _duration(float(item.get("runtime_seconds") or 0.0)),
                _short(item.get("finish_reason"), 10),
                _short(item.get("job_id"), 20),
                _short(str(item.get("class_name") or "").split(":")[-1], 22),
                f"{float(item.get('max_cpu_percent') or 0.0):.1f}%",
                _bytes(int(item.get("max_process_tree_memory_rss_bytes") or 0)),
                _bytes(int(item.get("max_total_work_bytes") or 0)),
            ]
            for item in history
        ],
        right={"runtime", "max cpu", "max rss", "max disk"},
    )


def _lease_row(item: dict) -> list[object]:
    request = item.get("request") or {}
    reserved = item.get("reserved_cpu_core_ids") or item.get("cpu_core_ids") or []
    assigned = item.get("cpu_core_ids") or []
    return [
        _short(item.get("lease_id"), 18),
        _short(request.get("job_id"), 22),
        int(request.get("cpu_cores") or 0),
        len(reserved),
        len(assigned),
        _bytes(int(request.get("memory_bytes") or 0)),
        _bytes(int(request.get("temp_storage_bytes") or 0)),
    ]


def _lease_cpu_row(item: dict) -> list[object]:
    reserved = item.get("reserved_cpu_core_ids") or item.get("cpu_core_ids") or []
    assigned = item.get("cpu_core_ids") or []
    return [
        _short(item.get("lease_id"), 18),
        _core_summary(reserved),
        _core_summary(assigned),
    ]


def _active_job_row(item: dict, *, include_usage: bool) -> list[object]:
    assigned = item.get("assigned_cpu_core_ids") or []
    row: list[object] = [
        _short(item.get("job_id"), 20),
        _short(item.get("agent_id"), 12),
        int(item.get("pid") or 0),
        int(item.get("declared_cpu_cores") or 0),
        len(assigned),
        _bytes(int(item.get("declared_memory_bytes") or 0)),
        _bytes(int(item.get("current_memory_rss_bytes") or 0)),
        _short(item.get("process_status") or item.get("usage_error") or item.get("error"), 10),
    ]
    if include_usage:
        return row
    else:
        row.extend(
            [
                f"{float(item.get('current_cpu_percent') or 0.0):.1f}%",
                f"{float(item.get('current_memory_percent') or 0.0):.2f}%",
                _core_summary(assigned),
            ]
        )
    return row


def _active_usage_row(item: dict) -> list[object]:
    runtime_seconds = max(0.0, time.time() - float(item.get("granted_at") or time.time()))
    return [
        _short(item.get("job_id"), 20),
        _duration(runtime_seconds),
        _bytes(int(item.get("process_tree_memory_rss_bytes") or 0)),
        _bytes(int(item.get("max_memory_rss_bytes") or 0)),
        _bytes(int(item.get("max_process_tree_memory_rss_bytes") or 0)),
        _bytes(int(item.get("work_dir_bytes") or 0)),
        _bytes(int(item.get("extra_work_bytes") or 0)),
        int(item.get("work_dir_file_count") or 0) + int(item.get("extra_work_file_count") or 0),
        _bytes(int(item.get("max_total_work_bytes") or 0)),
        int(item.get("sample_count") or 0),
    ]


def _print_table(headers: list[str], rows: list[list[object]], *, right: set[str] | None = None) -> None:
    right = right or set()
    text_rows = [[str(value) for value in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in text_rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    print(_table_row(headers, widths, right=right, headers=headers))
    print(_table_separator(headers, widths, right=right))
    for row in text_rows:
        print(_table_row(row, widths, right=right, headers=headers))


def _table_row(values: list[str], widths: list[int], *, right: set[str], headers: list[str]) -> str:
    cells = []
    for index, value in enumerate(values):
        header = headers[index]
        cells.append(value.rjust(widths[index]) if header in right else value.ljust(widths[index]))
    return "| " + " | ".join(cells) + " |"


def _table_separator(headers: list[str], widths: list[int], *, right: set[str]) -> str:
    cells = []
    for header, width in zip(headers, widths, strict=True):
        marker = "-" * max(3, width)
        cells.append(marker[:-1] + ":" if header in right else marker)
    return "| " + " | ".join(cells) + " |"


def _format_time(timestamp: float) -> str:
    if timestamp <= 0.0:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def _duration(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    if seconds < 60.0:
        return f"{seconds:.0f}s"
    minutes = seconds / 60.0
    if minutes < 60.0:
        return f"{minutes:.1f}m"
    return f"{minutes / 60.0:.1f}h"


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


def _excluded_host_names(values: list[str]) -> tuple[str, ...]:
    return tuple(value for value in values if "://" not in value)


def _excluded_host_urls(values: list[str]) -> tuple[str, ...]:
    return tuple(value for value in values if "://" in value)


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


def _core_ids(cpu_core_ids: list[int]) -> str:
    return ",".join(str(item) for item in cpu_core_ids)


def _core_ranges(cpu_core_ids: list[int]) -> str:
    values = sorted({int(item) for item in cpu_core_ids})
    if not values:
        return "-"
    ranges: list[str] = []
    start = previous = values[0]
    for value in values[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = value
    ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return ",".join(ranges)


def _core_summary(cpu_core_ids: list[int], *, max_items: int = 4) -> str:
    values = sorted({int(item) for item in cpu_core_ids})
    if not values:
        return "-"
    ranges = _core_ranges(values)
    if len(values) <= max_items or len(ranges) <= 18:
        return ranges
    visible = ",".join(str(item) for item in values[:max_items])
    return f"{visible},+{len(values) - max_items}"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
