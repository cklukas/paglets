# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.patterns.tasks import TaskClient, TaskSnapshot, TaskStatus
from paglets.remote.admin import PagletsAdminClient, ServerRef, normalize_server_url, select_reachable_entry_server
from paglets.remote.client import HostClient
from paglets.remote.proxy import PagletProxy

from .agent import (
    FileGrabberPaglet,
    FileGrabMode,
    FileGrabRequest,
    FileGrabResult,
)

DEFAULT_REQUEST_TIMEOUT_SECONDS = 60.0


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.error("a command is required")
    try:
        api_key = resolve_api_key(args.api_key_env)
        client = HostClient(timeout=max(1.0, float(args.request_timeout)), api_key=api_key)
        entry = _select_entry_server(entry_name=args.entry, client=client)
        remote = _resolve_remote(entry, args.remote, client=client)
        summary = run_transfer(entry, remote, args, client=client)
        if args.json:
            print(json.dumps(summary.to_wire(), indent=2, sort_keys=True))
        else:
            _print_summary(summary)
        return 0 if summary.status is TaskStatus.COMPLETED else 1
    except Exception as exc:
        print(f"paglets-file-grabber: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy or move one file between an entry host and one remote host")
    parser.add_argument("--entry", default=None, help="Discovered entry/start host name")
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=DEFAULT_REQUEST_TIMEOUT_SECONDS,
        help="HTTP request timeout in seconds",
    )
    parser.add_argument(
        "--api-key-env",
        default=None,
        help=f"Environment variable to read the paglets bearer API key from; defaults to {DEFAULT_API_KEY_ENV}",
    )
    subparsers = parser.add_subparsers(dest="command")
    for command, help_text in (
        ("push", "copy or move a file from the entry host to the remote host"),
        ("pull", "copy or move a file from the remote host to the entry host"),
    ):
        sub = subparsers.add_parser(command, help=help_text)
        sub.add_argument("source", help="Source file path on the source host")
        sub.add_argument("--remote", required=True, help="Remote host name or URL")
        sub.add_argument("--dest", default="", help="Destination file path; defaults to the source basename")
        sub.add_argument(
            "--mode",
            choices=tuple(mode.value for mode in FileGrabMode),
            default=FileGrabMode.COPY.value,
        )
        sub.add_argument("--dry", action="store_true", help="Only stat the source and report the planned destination")
        sub.add_argument("--overwrite", action="store_true", help="Replace the destination if it already exists")
        sub.add_argument("--json", action="store_true", help="Print machine-readable summary JSON")
    return parser


def run_transfer(
    entry: ServerRef,
    remote: ServerRef,
    args: argparse.Namespace,
    *,
    client: HostClient,
) -> TaskSnapshot[FileGrabResult]:
    source_server = entry if args.command == "push" else remote
    destination_server = remote if args.command == "push" else entry
    destination_path = args.dest or Path(args.source).name
    request = FileGrabRequest(
        source_path=args.source,
        destination_path=destination_path,
        target_host=destination_server.url,
        mode=FileGrabMode(args.mode),
        dry_run=bool(args.dry),
        overwrite=bool(args.overwrite),
        source_label=source_server.name,
        destination_label=destination_server.name,
    )
    proxy = _create_file_grabber(source_server, client=client)
    task = TaskClient.for_paglet(proxy, FileGrabberPaglet)
    try:
        return task.start_and_wait(
            request,
            wait_timeout=max(0.0, float(args.request_timeout)),
            timeout=max(1.0, float(args.request_timeout)),
        )
    finally:
        seen: set[tuple[str, str]] = set()
        for candidate in (proxy, task.proxy):
            key = (candidate.host_url.rstrip("/"), candidate.agent_id)
            if key in seen:
                continue
            seen.add(key)
            with contextlib.suppress(Exception):
                candidate.dispose()


def _create_file_grabber(server: ServerRef, *, client: HostClient) -> PagletProxy:
    admin = PagletsAdminClient([server], client=client)
    proxy_wire = admin.create_agent(
        server,
        "paglets.examples.file_grabber.agent:FileGrabberPaglet",
        "paglets.examples.file_grabber.agent:FileGrabberState",
        {},
    )
    return PagletProxy.from_wire(proxy_wire, client)


def _select_entry_server(*, entry_name: str | None, client: HostClient) -> ServerRef:
    return select_reachable_entry_server(entry_name=entry_name, client=client)


def _resolve_remote(entry: ServerRef, remote: str, *, client: HostClient) -> ServerRef:
    target = remote.strip()
    if "://" in target:
        url = normalize_server_url(target)
        health = client.get_json(f"{url}/health")
        return ServerRef(name=str(health.get("name") or target), url=str(health.get("address") or url).rstrip("/"))
    admin = PagletsAdminClient([entry], client=client)
    for host in admin.list_hosts(entry):
        if host.name == target or host.url.rstrip("/") == target.rstrip("/"):
            if not host.online:
                raise ValueError(f"remote host {target!r} is known but offline")
            return ServerRef(name=host.name, url=host.url.rstrip("/"))
    raise ValueError(f"remote host {target!r} was not found from entry host {entry.name!r}")


def _print_summary(summary: TaskSnapshot[FileGrabResult]) -> None:
    if summary.result is None:
        print(f"{summary.status.value}: {summary.error or 'file grab failed'}")
        return
    status = "DRY_RUN" if summary.result.dry_run else summary.status.value
    verb = "would save" if summary.result.dry_run else "saved"
    if summary.status is not TaskStatus.COMPLETED:
        print(f"{status}: {summary.error or 'file grab failed'}")
        return
    print(
        f"{status}: {summary.result.source.host_name}:{summary.result.source.path} "
        f"({summary.result.size}) -> {summary.result.destination.host_name}:{summary.result.destination.path}"
    )
    print(f"{verb} with mode={summary.result.mode.value}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
