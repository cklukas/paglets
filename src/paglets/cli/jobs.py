# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from typing import Annotated

import typer

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.remote.admin import PagletsAdminClient, select_reachable_entry_server
from paglets.remote.client import HostClient
from paglets.system.compute_slots import cli as backend
from paglets.system.compute_slots import groups_cli
from paglets.system.compute_slots.agent import (
    CANCEL_SLOT_REQUESTS,
    CANDIDATE_HOSTS,
    SCHEDULER_STATUS,
    CancelSlotRequestsRequest,
    CandidateHostsRequest,
    ComputeSlotRequest,
    SchedulerStatusRequest,
)

from .console import bytes_text, console, duration_text, fail, print_json, print_table

app = typer.Typer(help="Inspect and manage compute jobs and slot queues.", no_args_is_help=True)


def _client(timeout: float, api_key_env: str | None) -> HostClient:
    return HostClient(timeout=timeout, api_key=resolve_api_key(api_key_env))


def _entry(client: HostClient, entry: str | None):
    return select_reachable_entry_server(entry_name=entry, client=client)


@app.command()
def status(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help=f"API key environment variable; defaults to {DEFAULT_API_KEY_ENV}."),
    ] = None,
) -> None:
    _status(entry, timeout, json_output, api_key_env, include_queue=False, include_jobs=False)


@app.command("queue")
def queue_command(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    _status(entry, timeout, json_output, api_key_env, include_queue=True, include_jobs=False)


@app.command("why")
def why_command(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    _status(entry, timeout, json_output, api_key_env, include_queue=True, include_jobs=False, blocked=True)


@app.command("top")
def top_command(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    _status(entry, timeout, json_output, api_key_env, include_queue=False, include_jobs=True, usage=True)


@app.command("ps")
def ps_command(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    active: Annotated[bool, typer.Option("--active", help="Include active compute jobs.")] = False,
    inactive: Annotated[bool, typer.Option("--inactive", help="Include inactive compute jobs.")] = False,
    job: Annotated[list[str] | None, typer.Option("--job", help="Filter by compute job ID; repeatable.")] = None,
    agent: Annotated[list[str] | None, typer.Option("--agent", help="Filter by agent ID; repeatable.")] = None,
    state: Annotated[list[str] | None, typer.Option("--state", help="Filter by compute or application status.")] = None,
    class_name: Annotated[list[str] | None, typer.Option("--class", help="Filter by class name or suffix.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = _client(timeout, api_key_env)
        entry_ref = _entry(client, entry)
        include_active = active or not inactive
        include_inactive = inactive or not active
        admin = PagletsAdminClient([entry_ref], client=client)
        jobs = backend._load_compute_jobs(
            admin,
            entry_ref,
            include_active=include_active,
            include_inactive=include_inactive,
            agent_ids=tuple(agent or ()),
            job_ids=tuple(job or ()),
            statuses=tuple(state or ()),
            class_names=tuple(class_name or ()),
        )
        if json_output:
            print_json({"jobs": backend._public_jobs(jobs)})
        else:
            _print_jobs(jobs)
    except Exception as exc:
        fail("paglets jobs ps", exc)


@app.command()
def history(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    limit: Annotated[int, typer.Option("--limit", help="Maximum finished jobs to print; 0 prints all.")] = 20,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = _client(timeout, api_key_env)
        entry_ref = _entry(client, entry)
        reply = backend._handle(entry_ref, client, SCHEDULER_STATUS.name).call(
            SCHEDULER_STATUS, SchedulerStatusRequest(include_usage_history=True)
        )
        records = SCHEDULER_STATUS.encode_reply(reply).get("finished_usage") or []
        if limit > 0:
            records = records[-limit:]
        if json_output:
            print_json({"finished_usage": records})
        else:
            _print_history(records)
    except Exception as exc:
        fail("paglets jobs history", exc)


@app.command("hosts")
def hosts_command(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    limit: Annotated[int, typer.Option("--limit", help="Maximum candidates to print.")] = 5,
    cores: Annotated[int, typer.Option("--cores", help="Requested CPU cores.")] = 1,
    mem: Annotated[str, typer.Option("--mem", help="Requested RAM, e.g. 512M.")] = "0",
    disk: Annotated[str, typer.Option("--disk", help="Requested temp storage, e.g. 1G.")] = "0",
    gpu: Annotated[bool, typer.Option("--gpu", help="Require GPU support.")] = False,
    gpu_memory: Annotated[int, typer.Option("--gpu-memory", help="Requested GPU memory in MB.")] = 0,
    tag: Annotated[list[str] | None, typer.Option("--tag", help="Require a host tag; repeatable.")] = None,
    prefer_tag: Annotated[list[str] | None, typer.Option("--prefer-tag", help="Prefer a host tag; repeatable.")] = None,
    exclude_tag: Annotated[
        list[str] | None, typer.Option("--exclude-tag", help="Reject a host tag; repeatable.")
    ] = None,
    exclude_host: Annotated[list[str] | None, typer.Option("--exclude-host", help="Reject a host name or URL.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = _client(timeout, api_key_env)
        entry_ref = _entry(client, entry)
        request = CandidateHostsRequest(
            slot=ComputeSlotRequest(
                cpu_cores=max(1, cores),
                memory_bytes=backend._parse_size(mem),
                temp_storage_bytes=backend._parse_size(disk),
                requires_gpu=gpu,
                gpu_memory_mb=max(0, gpu_memory),
                required_host_tags=tuple(tag or ()),
                preferred_host_tags=tuple(prefer_tag or ()),
                excluded_host_tags=tuple(exclude_tag or ()),
                excluded_host_names=backend._excluded_host_names(exclude_host or []),
                excluded_host_urls=backend._excluded_host_urls(exclude_host or []),
            ),
            limit=max(0, limit),
        )
        reply = backend._handle(entry_ref, client, CANDIDATE_HOSTS.name).call(CANDIDATE_HOSTS, request)
        payload = CANDIDATE_HOSTS.encode_reply(reply)
        if json_output:
            print_json(payload)
        else:
            _print_hosts(payload)
        raise typer.Exit(0 if reply.candidates else 1)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets jobs hosts", exc)


@app.command()
def groups(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    group: Annotated[str | None, typer.Option("--group", help="Restrict output to one group ID.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = _client(timeout, api_key_env)
        entry_ref = _entry(client, entry)
        summaries = groups_cli._collect_group_summaries(entry_ref, client, group_id=group)
        if json_output:
            print_json({"groups": summaries})
        else:
            _print_groups(summaries)
        raise typer.Exit(0 if summaries else 1)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets jobs groups", exc)


@app.command("rm")
def rm_command(
    job: Annotated[list[str] | None, typer.Option("--job", help="Remove queued/waiting job by job ID.")] = None,
    agent: Annotated[list[str] | None, typer.Option("--agent", help="Remove queued/waiting job by agent ID.")] = None,
    all_jobs: Annotated[bool, typer.Option("--all", help="Remove all queued/waiting jobs.")] = False,
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    state: Annotated[list[str] | None, typer.Option("--state", help="Inactive job status to dispose.")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-n", help="Preview matches without removing them.")] = False,
    force: Annotated[bool, typer.Option("--force", "-f", help="Remove without prompting.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    if not all_jobs and not job and not agent and not state:
        raise typer.BadParameter("Use --job, --agent, --state, or --all.")
    if not dry_run and not force and not typer.confirm("Remove matching queued/waiting compute jobs?", default=False):
        raise typer.Exit(1)
    try:
        client = _client(timeout, api_key_env)
        entry_ref = _entry(client, entry)
        cancel_request = CancelSlotRequestsRequest(
            job_ids=tuple(job or ()),
            agent_ids=tuple(agent or ()),
            all=all_jobs,
            include_leases=False,
        )
        status_reply = backend._handle(entry_ref, client, SCHEDULER_STATUS.name).call(
            SCHEDULER_STATUS, SchedulerStatusRequest(include_queue=True)
        )
        preview = backend._cancel_preview_payload(status_reply, cancel_request)
        admin = PagletsAdminClient([entry_ref], client=client)
        inactive = backend._load_compute_jobs(
            admin,
            entry_ref,
            include_active=False,
            include_inactive=True,
            agent_ids=tuple(agent or ()),
            job_ids=tuple(job or ()),
            statuses=tuple(state or ("WAITING_FOR_SLOT",)),
        )
        if dry_run:
            payload = {"dry_run": True, **preview, "inactive_jobs": backend._public_jobs(inactive)}
            if json_output:
                print_json(payload)
            else:
                console.print(f"Matched queued requests: {len(preview['matched_requests'])}")
                console.print(f"Matched inactive jobs: {len(inactive)}")
            return
        cancel_reply = backend._handle(entry_ref, client, CANCEL_SLOT_REQUESTS.name).call(
            CANCEL_SLOT_REQUESTS, cancel_request
        )
        disposed = 0
        errors: dict[str, str] = {}
        for item in inactive:
            try:
                admin.dispose(item["_agent"])
            except Exception as exc:
                errors[str(item["agent_id"])] = str(exc)
            else:
                disposed += 1
        payload = {
            "cancelled_requests": cancel_reply.cancelled_requests,
            "disposed_inactive_jobs": disposed,
            "errors": errors,
        }
        if json_output:
            print_json(payload)
        else:
            console.print(
                f"Removed queued requests={cancel_reply.cancelled_requests}; disposed inactive jobs={disposed}"
            )
            for agent_id, error in sorted(errors.items()):
                console.print(f"- {agent_id}: {error}")
        raise typer.Exit(1 if errors else 0)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets jobs rm", exc)


def _status(
    entry_name: str | None,
    timeout: float,
    json_output: bool,
    api_key_env: str | None,
    *,
    include_queue: bool,
    include_jobs: bool,
    blocked: bool = False,
    usage: bool = False,
) -> None:
    try:
        client = _client(timeout, api_key_env)
        entry_ref = _entry(client, entry_name)
        reply = backend._handle(entry_ref, client, SCHEDULER_STATUS.name).call(
            SCHEDULER_STATUS,
            SchedulerStatusRequest(
                include_queue=include_queue or blocked or usage,
                include_jobs=include_jobs or usage,
                include_usage=usage,
            ),
        )
        payload = SCHEDULER_STATUS.encode_reply(reply)
        if usage:
            backend._apply_lease_times_to_active_jobs(payload)
        if blocked:
            payload["blocked_requests"] = backend._blocked_request_payload(payload)
        if json_output:
            print_json(payload)
        else:
            _print_status(
                payload,
                show_queue=include_queue or blocked,
                show_jobs=include_jobs or usage,
                show_blocked=blocked,
                show_usage=usage,
            )
    except Exception as exc:
        fail("paglets jobs status", exc)


def _print_status(payload: dict, *, show_queue: bool, show_jobs: bool, show_blocked: bool, show_usage: bool) -> None:
    status = payload["status"]
    print_table(
        "Scheduler Status",
        ["host", "free cores", "reserved cores", "load/cpu", "ram free", "temp free", "waiting", "leases"],
        [
            [
                status["host_name"],
                status["free_cpu_cores"],
                status["reserved_cpu_cores"],
                f"{float(status.get('load_per_cpu') or 0.0):.2f}/{float(status.get('max_load_per_cpu') or 0.0):.2f}",
                bytes_text(status["free_memory_bytes"]),
                bytes_text(status["free_temp_storage_bytes"]),
                status["queue_length"],
                status["active_leases"],
            ]
        ],
        right={"free cores", "reserved cores", "ram free", "temp free", "waiting", "leases"},
    )
    if show_queue:
        _print_queue(payload.get("queued_requests") or [])
    if show_blocked:
        _print_blocked(payload.get("blocked_requests") or [])
    if show_jobs:
        _print_active(payload.get("active_jobs") or [], show_usage=show_usage)


def _print_queue(items: list[dict]) -> None:
    print_table(
        "Queue",
        ["request", "job", "agent", "cores", "mem", "temp"],
        [
            [
                item.get("request_id") or "",
                item.get("job_id") or "",
                item.get("agent_id") or "",
                item.get("cpu_cores") or 0,
                bytes_text(item.get("memory_bytes") or 0),
                bytes_text(item.get("temp_storage_bytes") or 0),
            ]
            for item in items
        ],
        right={"cores", "mem", "temp"},
    )


def _print_blocked(items: list[dict]) -> None:
    print_table(
        "Blocked Queue",
        ["request", "job", "blockers"],
        [
            [item.get("request_id") or "", item.get("job_id") or "", ", ".join(item.get("blockers") or [])]
            for item in items
        ],
    )


def _print_active(items: list[dict], *, show_usage: bool) -> None:
    columns = ["job", "agent", "pid", "cores", "rss", "status"]
    rows = [
        [
            item.get("job_id") or "",
            item.get("agent_id") or "",
            item.get("pid") or "",
            item.get("cpu_cores") or "",
            bytes_text(item.get("memory_rss_bytes") or item.get("rss_bytes") or 0),
            item.get("status") or "",
        ]
        for item in items
    ]
    print_table("Active Jobs", columns, rows, right={"pid", "cores", "rss"})
    if show_usage:
        print_table(
            "Active Usage",
            ["job", "runtime", "tree rss", "max rss", "work", "extra", "files", "samples"],
            [
                [
                    item.get("job_id") or "",
                    duration_text(item.get("runtime_seconds") or 0.0),
                    bytes_text(item.get("process_tree_rss_bytes") or 0),
                    bytes_text(item.get("max_rss_bytes") or 0),
                    bytes_text(item.get("work_dir_bytes") or 0),
                    bytes_text(item.get("extra_work_bytes") or 0),
                    item.get("work_file_count") or 0,
                    item.get("usage_sample_count") or 0,
                ]
                for item in items
            ],
            right={"tree rss", "max rss", "work", "extra", "files", "samples"},
        )


def _print_jobs(jobs: list[dict[str, object]]) -> None:
    print_table(
        "Compute Jobs",
        ["agent", "state", "compute", "status", "job", "request", "class"],
        [
            [
                item.get("agent_id"),
                "active" if item.get("active") else "inactive",
                item.get("compute_status"),
                item.get("status"),
                item.get("job_id"),
                item.get("request_id"),
                str(item.get("class_name") or "").split(":")[-1],
            ]
            for item in jobs
        ],
    )


def _print_history(records: list[dict]) -> None:
    print_table(
        "Finished Jobs",
        ["runtime", "reason", "job", "class", "max cpu", "max rss", "max disk"],
        [
            [
                duration_text(item.get("runtime_seconds") or 0.0),
                item.get("finish_reason") or "",
                item.get("job_id") or "",
                str(item.get("class_name") or "").split(":")[-1],
                f"{float(item.get('max_cpu_percent') or 0.0):.1f}%",
                bytes_text(item.get("max_rss_bytes") or 0),
                bytes_text(item.get("max_disk_bytes") or 0),
            ]
            for item in records
        ],
        right={"max cpu", "max rss", "max disk"},
    )


def _print_hosts(payload: dict) -> None:
    print_table(
        "Candidate Hosts",
        ["host", "score", "cores", "ram free", "temp free", "queue"],
        [
            [
                item["status"]["host_name"],
                f"{item['score']:.3f}",
                item["status"]["free_cpu_cores"],
                bytes_text(item["status"]["free_memory_bytes"]),
                bytes_text(item["status"]["free_temp_storage_bytes"]),
                item["status"]["queue_length"],
            ]
            for item in payload.get("candidates") or []
        ],
        right={"score", "cores", "ram free", "temp free", "queue"},
    )
    if payload.get("rejected"):
        console.print("\n[bold]Rejected[/bold]")
        for host, reason in sorted(payload["rejected"].items()):
            console.print(f"- {host}: {reason}")


def _print_groups(summaries: list[dict]) -> None:
    print_table(
        "Job Groups",
        ["group", "status", "expected", "done", "failed", "pending", "collector", "waiting-home"],
        [
            [
                summary["group_id"],
                summary["status"],
                summary["expected_count"],
                summary["completed_count"],
                summary["failed_count"],
                summary["pending_count"],
                summary["collector"]["host_name"] or summary["collector"]["host_url"],
                str(bool(summary["waiting_for_home"])).lower(),
            ]
            for summary in summaries
        ],
        right={"expected", "done", "failed", "pending"},
    )
