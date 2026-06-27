# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from typing import Annotated

import typer

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.remote.admin import select_reachable_entry_server
from paglets.remote.client import HostClient
from paglets.system.mesh_info import cli as backend
from paglets.system.mesh_info.agent import GET_LANDSCAPE, SELECT_TARGETS, LandscapeRequest, TargetSelectionRequest

from .console import bytes_text, console, fail, print_json, print_table

app = typer.Typer(help="Inspect mesh state and compute placement targets.", no_args_is_help=True)


@app.command()
def summary(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    max_age: Annotated[
        float, typer.Option("--max-age", help="Freshness cutoff in seconds; 0 uses service default.")
    ] = 0.0,
    limit: Annotated[int, typer.Option("--limit", help="Maximum hosts to print.")] = 0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help=f"API key environment variable; defaults to {DEFAULT_API_KEY_ENV}."),
    ] = None,
) -> None:
    try:
        client = HostClient(timeout=timeout, api_key=resolve_api_key(api_key_env))
        entry_ref = select_reachable_entry_server(entry_name=entry, client=client)
        handle = backend._mesh_info_handle(entry_ref, client)
        with console.status("Loading mesh landscape...", spinner="dots"):
            reply = handle.call(
                GET_LANDSCAPE,
                LandscapeRequest(fresh_only=True, max_age_seconds=max(0.0, max_age), limit=max(0, limit)),
            )
        if json_output:
            print_json(GET_LANDSCAPE.encode_reply(reply))
        else:
            _print_summary(reply)
        raise typer.Exit(0 if reply.hosts else 1)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets mesh summary", exc)


@app.command()
def targets(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    limit: Annotated[int, typer.Option("--limit", help="Maximum targets to print.")] = 5,
    max_age: Annotated[
        float, typer.Option("--max-age", help="Freshness cutoff in seconds; 0 uses service default.")
    ] = 0.0,
    max_load_per_cpu: Annotated[
        float, typer.Option("--max-load-per-cpu", help="Maximum 1-minute load divided by CPUs.")
    ] = 1.0,
    max_cpu_percent: Annotated[float, typer.Option("--max-cpu-percent", help="Maximum sampled CPU percent.")] = 100.0,
    mem: Annotated[str, typer.Option("--mem", help="Minimum available RAM, e.g. 512M.")] = "0",
    disk: Annotated[str, typer.Option("--disk", help="Minimum free work storage, e.g. 1G.")] = "0",
    include_self: Annotated[bool, typer.Option("--include-self/--exclude-self", help="Include the entry host.")] = True,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = HostClient(timeout=timeout, api_key=resolve_api_key(api_key_env))
        entry_ref = select_reachable_entry_server(entry_name=entry, client=client)
        handle = backend._mesh_info_handle(entry_ref, client)
        request = TargetSelectionRequest(
            limit=max(1, limit),
            max_age_seconds=max(0.0, max_age),
            max_load_per_cpu=float(max_load_per_cpu),
            max_cpu_percent=float(max_cpu_percent),
            min_memory_available_bytes=backend._parse_size(mem),
            min_work_free_bytes=backend._parse_size(disk),
            include_self=include_self,
        )
        with console.status("Ranking mesh targets...", spinner="dots"):
            reply = handle.call(SELECT_TARGETS, request)
        if json_output:
            print_json(SELECT_TARGETS.encode_reply(reply))
        else:
            _print_targets(reply)
        raise typer.Exit(0 if reply.targets else 1)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets mesh targets", exc)


def _print_summary(reply) -> None:
    now = time.time()
    rows = []
    for snapshot in reply.hosts:
        rows.append(
            [
                snapshot.host_name,
                f"{max(0.0, now - snapshot.observed_at):.1f}s",
                f"{snapshot.cpu_percent:.1f}",
                f"{snapshot.load_per_cpu:.3f}",
                bytes_text(snapshot.memory_available_bytes),
                bytes_text(snapshot.work_free_bytes),
                snapshot.active_count,
                snapshot.inactive_count,
                "; ".join(snapshot.errors),
            ]
        )
    print_table(
        "Mesh Summary",
        ["host", "age", "cpu%", "load/cpu", "ram free", "work free", "active", "inactive", "errors"],
        rows,
        right={"cpu%", "load/cpu", "ram free", "work free", "active", "inactive"},
    )
    _print_errors(reply.errors)


def _print_targets(reply) -> None:
    rows = []
    for target in reply.targets:
        snapshot = target.snapshot
        rows.append(
            [
                snapshot.host_name,
                f"{target.score:.3f}",
                f"{snapshot.cpu_percent:.1f}",
                f"{snapshot.load_per_cpu:.3f}",
                bytes_text(snapshot.memory_available_bytes),
                bytes_text(snapshot.work_free_bytes),
                snapshot.active_count,
                snapshot.inactive_count,
            ]
        )
    print_table(
        "Mesh Targets",
        ["host", "score", "cpu%", "load/cpu", "ram free", "work free", "active", "inactive"],
        rows,
        right={"score", "cpu%", "load/cpu", "ram free", "work free", "active", "inactive"},
    )
    if reply.rejected:
        console.print("\n[bold]Rejected[/bold]")
        for host, reason in sorted(reply.rejected.items()):
            console.print(f"- {host}: {reason}")
    _print_errors(reply.errors)


def _print_errors(errors: dict[str, str]) -> None:
    if errors:
        console.print("\n[bold red]Errors[/bold red]")
        for host, error in sorted(errors.items()):
            console.print(f"- {host}: {error}")
