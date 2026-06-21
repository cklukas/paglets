# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from ...admin import (
    DEFAULT_CONFIG_PATH,
    PagletsAdminClient,
    ServerRef,
    load_server_config,
    parse_server_arg,
    select_reachable_entry_server,
    upsert_server_ref,
)
from ...client import HostClient
from ...messages import Message
from ...proxy import PagletProxy
from .agent import (
    GET_DISK,
    GET_LOAD,
    GET_SUMMARY,
    LIST_PROCESSES,
    DiskRequest,
    LoadRequest,
    ProcessListRequest,
    SystemInfoCollectorAgent,
    SystemInfoCollectorState,
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
        operation, request = _operation_request(args)
        summary = _collect(entry, operation.name, request, timeout=args.timeout, client=client)
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            _print_text(summary, operation.name)
        return 0 if not summary.get("errors") else 1
    except Exception as exc:
        print(f"paglets-sysinfo: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query server-info services across a paglets mesh")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Server config path")
    parser.add_argument("--server", action="append", default=[], help="One-off server in NAME=URL format")
    parser.add_argument("--entry", default=None, help="Entry server name from config")
    parser.add_argument("--timeout", type=float, default=5.0, help="Seconds to wait for mesh replies")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    load = subparsers.add_parser("load", help="Show CPU, memory, swap, and GPU load")
    load.add_argument("--interval", type=float, default=0.0, help="CPU sampling interval per host")
    load.add_argument("--no-gpu", action="store_true", help="Skip best-effort GPU lookup")

    df = subparsers.add_parser("df", help="Show disk usage")
    df.add_argument("paths", nargs="*", help="Optional paths to inspect on every host")

    plist = subparsers.add_parser("plist", help="List matching processes")
    plist.add_argument("query", help="Case-insensitive process name/cmdline search")
    plist.add_argument("--limit", type=int, default=25, help="Maximum processes per host")
    plist.add_argument("--args", action="store_true", help="Include process command lines")

    subparsers.add_parser("summary", help="Show compact host summaries")
    return parser


def _select_entry_server(servers: list[ServerRef], *, entry_name: str | None, client: HostClient) -> ServerRef:
    return select_reachable_entry_server(
        servers,
        entry_name=entry_name,
        client=client,
        config_path=DEFAULT_CONFIG_PATH,
    )


def _operation_request(args: argparse.Namespace):
    if args.command == "load":
        request = LoadRequest(interval=max(0.0, args.interval), include_gpu=not args.no_gpu)
        return GET_LOAD, GET_LOAD.encode_request(request)
    if args.command == "df":
        request = DiskRequest(paths=list(args.paths), all_volumes=not bool(args.paths))
        return GET_DISK, GET_DISK.encode_request(request)
    if args.command == "plist":
        request = ProcessListRequest(query=args.query, limit=max(1, args.limit), include_args=bool(args.args))
        return LIST_PROCESSES, LIST_PROCESSES.encode_request(request)
    return GET_SUMMARY, GET_SUMMARY.encode_request()


def _collect(entry: ServerRef, operation: str, request: dict[str, Any], *, timeout: float, client: HostClient) -> dict[str, Any]:
    admin = PagletsAdminClient([entry], client=client)
    proxy_wire = admin.create_agent(
        entry,
        "paglets.examples.system_info.agent:SystemInfoCollectorAgent",
        "paglets.examples.system_info.agent:SystemInfoCollectorState",
        {},
    )
    proxy = PagletProxy.from_wire(proxy_wire, client)
    try:
        proxy.send(
            Message(
                "collect",
                {
                    "operation": operation,
                    "request": request,
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
        try:
            proxy.dispose()
        except Exception:
            pass


def _print_text(summary: dict[str, Any], operation: str) -> None:
    if operation == GET_LOAD.name:
        _print_load(summary)
    elif operation == GET_DISK.name:
        _print_disk(summary)
    elif operation == LIST_PROCESSES.name:
        _print_processes(summary)
    else:
        _print_summary(summary)
    _print_errors(summary)


def _print_load(summary: dict[str, Any]) -> None:
    print(f"{'host':<14} {'cpu%':>6} {'mem%':>6} {'swap%':>6} {'load':<18} {'gpu':<20}")
    for host, item in sorted(summary.get("results", {}).items()):
        reply = GET_LOAD.decode_reply(item["result"])
        load_average = " ".join(f"{value:.2f}" for value in reply.load_average) or "-"
        gpu = ", ".join(f"{gpu.name}:{gpu.utilization_percent or 0:.0f}%" for gpu in reply.gpus)
        if not gpu:
            gpu = reply.gpu_error or "-"
        print(
            f"{host:<14} {reply.cpu_percent:>6.1f} {reply.memory_percent:>6.1f} "
            f"{reply.swap_percent:>6.1f} {load_average:<18} {gpu:<20}"
        )


def _print_disk(summary: dict[str, Any]) -> None:
    print(f"{'host':<14} {'path':<32} {'size':>9} {'used':>9} {'free':>9} {'use%':>6}")
    for host, item in sorted(summary.get("results", {}).items()):
        reply = GET_DISK.decode_reply(item["result"])
        for volume in reply.volumes:
            print(
                f"{host:<14} {volume.path:<32.32} {_bytes(volume.total_bytes):>9} "
                f"{_bytes(volume.used_bytes):>9} {_bytes(volume.free_bytes):>9} {volume.percent_used:>6.1f}"
            )
        for path, error in sorted(reply.errors.items()):
            print(f"{host:<14} {path:<32.32} error: {error}")


def _print_processes(summary: dict[str, Any]) -> None:
    print(f"{'host':<14} {'pid':>7} {'rss':>9} {'mem%':>6} {'cpu%':>6} {'status':<12} name")
    for host, item in sorted(summary.get("results", {}).items()):
        reply = LIST_PROCESSES.decode_reply(item["result"])
        for process in reply.processes:
            name = " ".join(process.cmdline) if process.cmdline else process.name
            print(
                f"{host:<14} {process.pid:>7} {_bytes(process.memory_rss_bytes):>9} "
                f"{process.memory_percent:>6.1f} {process.cpu_percent:>6.1f} {process.status:<12} {name}"
            )


def _print_summary(summary: dict[str, Any]) -> None:
    print(f"{'host':<14} {'cpu':>5} {'mem':>9} {'booted':<19} platform")
    for host, item in sorted(summary.get("results", {}).items()):
        reply = GET_SUMMARY.decode_reply(item["result"])
        booted = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reply.boot_time))
        print(
            f"{host:<14} {reply.cpu_count_logical:>5} {_bytes(reply.memory_total_bytes):>9} "
            f"{booted:<19} {reply.platform}"
        )


def _print_errors(summary: dict[str, Any]) -> None:
    errors = summary.get("errors") or {}
    if not errors:
        return
    print("\nerrors:")
    for host, error in sorted(errors.items()):
        print(f"  - {host}: {error}")


def _bytes(value: int) -> str:
    units = ("B", "K", "M", "G", "T", "P")
    amount = float(value)
    for unit in units:
        if abs(amount) < 1024.0 or unit == units[-1]:
            return f"{amount:.1f}{unit}" if unit != "B" else f"{int(amount)}B"
        amount /= 1024.0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
