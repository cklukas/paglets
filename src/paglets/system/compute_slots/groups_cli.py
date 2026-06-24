# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from paglets.remote.admin import ServerRef, select_reachable_entry_server
from paglets.remote.client import HostClient


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        api_key = os.environ.get(args.api_key_env) if args.api_key_env else None
        if args.api_key_env and not api_key:
            raise ValueError(f"--api-key-env {args.api_key_env!r} is not set or is empty")
        client = HostClient(timeout=args.timeout, api_key=api_key)
        entry = select_reachable_entry_server(entry_name=args.entry, client=client)
        summaries = _collect_group_summaries(entry, client, group_id=args.group)
        if args.json:
            print(json.dumps({"groups": summaries}, indent=2, sort_keys=True))
        else:
            _print_group_summaries(summaries)
        return 0 if summaries else 1
    except Exception as exc:
        print(f"paglets-compute-groups: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect compute job-group collectors")
    parser.add_argument("--entry", default=None, help="Discovered entry host name")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    parser.add_argument("--group", default=None, help="Restrict output to one group ID")
    parser.add_argument(
        "--api-key-env", default=None, help="Environment variable containing the paglets bearer API key"
    )
    return parser


def _collect_group_summaries(
    entry: ServerRef, client: HostClient, *, group_id: str | None = None
) -> list[dict[str, Any]]:
    hosts = _visible_hosts(entry, client)
    summaries: list[dict[str, Any]] = []
    for host in hosts:
        host_url = str(host.get("url") or host.get("address") or entry.url).rstrip("/")
        if host.get("online") is False:
            continue
        try:
            agents = client.get_json(f"{host_url}/agents?state=all").get("agents", [])
        except Exception:
            continue
        for agent in agents:
            agent_id = str(agent.get("agent_id") or "")
            if not agent_id:
                continue
            try:
                state_payload = client.get_json(f"{host_url}/agents/{agent_id}/state")
            except Exception:
                continue
            state = dict(state_payload.get("state") or {})
            summary = _summary_from_collector_state(agent, state, host_url=host_url)
            if summary is None:
                continue
            if group_id is not None and summary["group_id"] != group_id:
                continue
            summaries.append(summary)
    return sorted(summaries, key=lambda item: (str(item["group_id"]), str(item["collector"]["host_name"])))


def _visible_hosts(entry: ServerRef, client: HostClient) -> list[dict[str, Any]]:
    try:
        hosts = client.get_json(f"{entry.url.rstrip('/')}/hosts").get("hosts", [])
    except Exception:
        hosts = []
    if not hosts:
        hosts = [{"name": entry.name, "url": entry.url, "online": True}]
    return [dict(host) for host in hosts if isinstance(host, dict)]


def _summary_from_collector_state(
    agent: dict[str, Any],
    state: dict[str, Any],
    *,
    host_url: str,
) -> dict[str, Any] | None:
    if "expected_jobs" not in state or "group_id" not in state:
        return None
    expected_jobs = dict(state.get("expected_jobs") or {})
    results = dict(state.get("results") or {})
    failures = dict(state.get("failures") or {})
    pending = sorted(set(expected_jobs) - set(results) - set(failures))
    return {
        "group_id": str(state.get("group_id") or ""),
        "status": str(state.get("status") or ""),
        "collector": {
            "agent_id": str(agent.get("agent_id") or ""),
            "host_name": str(agent.get("host") or ""),
            "host_url": host_url,
            "active": bool(agent.get("active")),
        },
        "home": {
            "host_name": str(state.get("home_host_name") or ""),
            "host_url": str(state.get("home_host_url") or ""),
        },
        "return_home_when_complete": bool(state.get("return_home_when_complete")),
        "waiting_for_home": str(state.get("status") or "") == "WAITING_FOR_HOME",
        "expected_count": len(expected_jobs),
        "completed_count": len(results),
        "failed_count": len(failures),
        "pending_count": len(pending),
        "pending_jobs": pending,
        "completed_at": float(state.get("completed_at") or 0.0),
        "last_report_at": float(state.get("last_report_at") or 0.0),
    }


def _print_group_summaries(summaries: list[dict[str, Any]]) -> None:
    print(
        f"{'group':<22} {'status':<18} {'expected':>8} {'done':>6} {'failed':>7} "
        f"{'pending':>7} {'collector':<14} {'waiting-home':>12}"
    )
    for summary in summaries:
        collector = summary["collector"]
        print(
            f"{_short(summary['group_id'], 22):<22} {_short(summary['status'], 18):<18} "
            f"{summary['expected_count']:>8} {summary['completed_count']:>6} "
            f"{summary['failed_count']:>7} {summary['pending_count']:>7} "
            f"{_short(collector['host_name'] or collector['host_url'], 14):<14} "
            f"{str(bool(summary['waiting_for_home'])).lower():>12}"
        )


def _short(value: Any, width: int) -> str:
    text = str(value or "")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
