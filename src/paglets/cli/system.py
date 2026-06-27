# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from typing import Annotated, Any

import typer

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.remote.admin import select_reachable_entry_server
from paglets.remote.client import HostClient
from paglets.system.server_info import cli as backend
from paglets.system.server_info.agent import GET_DISK, GET_LOAD, GET_SUMMARY, LIST_PROCESSES

from .console import bytes_text, console, fail, print_json, print_table

app = typer.Typer(help="Inspect resources and processes across a Paglets mesh.", no_args_is_help=True)


@app.command()
def summary(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Seconds to wait for mesh replies.")] = 5.0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help=f"API key environment variable; defaults to {DEFAULT_API_KEY_ENV}."),
    ] = None,
) -> None:
    _run(GET_SUMMARY.name, GET_SUMMARY.encode_request(), entry, timeout, json_output, api_key_env)


@app.command()
def load(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Seconds to wait for mesh replies.")] = 5.0,
    interval: Annotated[float, typer.Option("--interval", help="CPU sampling interval per host.")] = 0.0,
    gpu: Annotated[bool, typer.Option("--gpu/--no-gpu", help="Include best-effort GPU lookup.")] = True,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    request = GET_LOAD.encode_request(backend.LoadRequest(interval=max(0.0, interval), include_gpu=gpu))
    _run(GET_LOAD.name, request, entry, timeout, json_output, api_key_env)


@app.command()
def df(
    disk: Annotated[list[str] | None, typer.Argument(help="Optional paths to inspect on every host.")] = None,
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Seconds to wait for mesh replies.")] = 5.0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    paths = list(disk or [])
    request = GET_DISK.encode_request(backend.DiskRequest(paths=paths, all_volumes=not paths))
    _run(GET_DISK.name, request, entry, timeout, json_output, api_key_env)


@app.command("ps")
def ps_command(
    query: Annotated[str, typer.Argument(help="Case-insensitive process name/cmdline search.")],
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Seconds to wait for mesh replies.")] = 5.0,
    limit: Annotated[int, typer.Option("--limit", help="Maximum processes per host.")] = 25,
    args: Annotated[bool, typer.Option("--args", help="Include process command lines.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    request = LIST_PROCESSES.encode_request(
        backend.ProcessListRequest(query=query, limit=max(1, limit), include_args=args)
    )
    _run(LIST_PROCESSES.name, request, entry, timeout, json_output, api_key_env)


def _run(
    operation: str,
    request: dict[str, Any],
    entry_name: str | None,
    timeout: float,
    json_output: bool,
    api_key_env: str | None,
) -> None:
    try:
        client = HostClient(timeout=timeout, api_key=resolve_api_key(api_key_env))
        entry = select_reachable_entry_server(entry_name=entry_name, client=client)
        with console.status("Collecting system information...", spinner="dots"):
            summary_payload = backend._collect(entry, operation, request, timeout=timeout, client=client)
        if json_output:
            print_json(summary_payload)
        else:
            _print_summary(summary_payload, operation)
        raise typer.Exit(0 if not summary_payload.get("errors") else 1)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets sys", exc)


def _print_summary(summary_payload: dict[str, Any], operation: str) -> None:
    if operation == GET_LOAD.name:
        _print_load(summary_payload)
    elif operation == GET_DISK.name:
        _print_disk(summary_payload)
    elif operation == LIST_PROCESSES.name:
        _print_processes(summary_payload)
    else:
        _print_host_summary(summary_payload)
    _print_errors(summary_payload)


def _print_host_summary(summary_payload: dict[str, Any]) -> None:
    rows = []
    for host, item in sorted(summary_payload.get("results", {}).items()):
        reply = GET_SUMMARY.decode_reply(item["result"])
        rows.append(
            [
                host,
                reply.cpu_count_logical,
                bytes_text(reply.memory_total_bytes),
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reply.boot_time)),
                reply.platform,
            ]
        )
    print_table("Host Summary", ["host", "cpu", "mem", "booted", "platform"], rows, right={"cpu", "mem"})


def _print_load(summary_payload: dict[str, Any]) -> None:
    rows = []
    for host, item in sorted(summary_payload.get("results", {}).items()):
        reply = GET_LOAD.decode_reply(item["result"])
        load_average = " ".join(f"{value:.2f}" for value in reply.load_average) or "-"
        gpu = ", ".join(f"{gpu.name}:{gpu.utilization_percent or 0:.0f}%" for gpu in reply.gpus)
        rows.append(
            [
                host,
                f"{reply.cpu_percent:.1f}",
                f"{reply.memory_percent:.1f}",
                f"{reply.swap_percent:.1f}",
                load_average,
                gpu or reply.gpu_error or "-",
            ]
        )
    print_table("Load", ["host", "cpu%", "mem%", "swap%", "load", "gpu"], rows, right={"cpu%", "mem%", "swap%"})


def _print_disk(summary_payload: dict[str, Any]) -> None:
    rows = []
    for host, item in sorted(summary_payload.get("results", {}).items()):
        reply = GET_DISK.decode_reply(item["result"])
        for volume in reply.volumes:
            rows.append(
                [
                    host,
                    volume.path,
                    bytes_text(volume.total_bytes),
                    bytes_text(volume.used_bytes),
                    bytes_text(volume.free_bytes),
                    f"{volume.percent_used:.1f}",
                ]
            )
        for path, error in sorted(reply.errors.items()):
            rows.append([host, path, "error", error, "", ""])
    print_table("Disk", ["host", "path", "size", "used", "free", "use%"], rows, right={"size", "used", "free", "use%"})


def _print_processes(summary_payload: dict[str, Any]) -> None:
    rows = []
    for host, item in sorted(summary_payload.get("results", {}).items()):
        reply = LIST_PROCESSES.decode_reply(item["result"])
        for process in reply.processes:
            name = " ".join(process.cmdline) if process.cmdline else process.name
            rows.append(
                [
                    host,
                    process.pid,
                    bytes_text(process.memory_rss_bytes),
                    f"{process.memory_percent:.1f}",
                    f"{process.cpu_percent:.1f}",
                    process.status,
                    name,
                ]
            )
    print_table(
        "Processes",
        ["host", "pid", "rss", "mem%", "cpu%", "status", "name"],
        rows,
        right={"pid", "rss", "mem%", "cpu%"},
    )


def _print_errors(summary_payload: dict[str, Any]) -> None:
    errors = summary_payload.get("errors") or {}
    if errors:
        console.print("\n[bold red]Errors[/bold red]")
        for host, error in sorted(errors.items()):
            console.print(f"- {host}: {error}")
