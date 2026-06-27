# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.remote.admin import select_reachable_entry_server
from paglets.remote.client import HostClient

from .console import bytes_text, console, fail, print_json, print_table

app = typer.Typer(help="Inspect and recover Paglets artifacts.", no_args_is_help=True)


def _client(timeout: float, api_key_env: str | None) -> HostClient:
    return HostClient(timeout=timeout, api_key=resolve_api_key(api_key_env))


def _host_url(client: HostClient, entry: str | None, host: str | None) -> str:
    return host or select_reachable_entry_server(entry_name=entry, client=client).url


@app.command("list")
def list_artifacts(
    entry: Annotated[str | None, typer.Option("--entry", help="Entry host name for ambient discovery.")] = None,
    host: Annotated[str | None, typer.Option("--host", help="Explicit host URL.")] = None,
    owner: Annotated[str | None, typer.Option("--owner", "--agent", help="Filter by owner agent id.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Request timeout in seconds.")] = 10.0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help=f"API key environment variable; defaults to {DEFAULT_API_KEY_ENV}."),
    ] = None,
) -> None:
    try:
        client = _client(timeout, api_key_env)
        refs = client.list_artifacts(_host_url(client, entry, host), owner_agent_id=owner, timeout=timeout)
        if json_output:
            print_json({"artifacts": [ref.to_wire() for ref in refs]})
        elif refs:
            print_table(
                "Artifacts",
                ["artifact", "size", "owner", "name"],
                [[ref.artifact_id[:12], bytes_text(ref.size_bytes), ref.owner_agent_id[:12], ref.name] for ref in refs],
                right={"size"},
            )
        else:
            console.print("No artifacts.")
    except Exception as exc:
        fail("paglets artifacts list", exc, code=1)


@app.command("info")
def artifact_info(
    artifact: Annotated[str, typer.Argument(help="Artifact id.")],
    entry: Annotated[str | None, typer.Option("--entry", help="Entry host name for ambient discovery.")] = None,
    host: Annotated[str | None, typer.Option("--host", help="Explicit host URL.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Request timeout in seconds.")] = 10.0,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = _client(timeout, api_key_env)
        print_json(
            {"artifact": client.artifact_metadata(_host_url(client, entry, host), artifact, timeout=timeout).to_wire()}
        )
    except Exception as exc:
        fail("paglets artifacts info", exc, code=1)


@app.command("get")
def get_artifact(
    artifact: Annotated[str, typer.Argument(help="Artifact id.")],
    output: Annotated[Path, typer.Argument(help="Output path.")],
    entry: Annotated[str | None, typer.Option("--entry", help="Entry host name for ambient discovery.")] = None,
    host: Annotated[str | None, typer.Option("--host", help="Explicit host URL.")] = None,
    move: Annotated[bool, typer.Option("--move", help="Delete the source artifact after verified download.")] = False,
    timeout: Annotated[float, typer.Option("--timeout", help="Request timeout in seconds.")] = 10.0,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = _client(timeout, api_key_env)
        ref = client.artifact_metadata(_host_url(client, entry, host), artifact, timeout=timeout)
        with console.status("Downloading artifact...", spinner="dots"):
            client.download_artifact(ref, output, move=move, timeout=timeout)
        console.print(str(output))
    except Exception as exc:
        fail("paglets artifacts get", exc, code=1)


@app.command("rm")
def remove_artifact(
    artifact: Annotated[str, typer.Argument(help="Artifact id.")],
    entry: Annotated[str | None, typer.Option("--entry", help="Entry host name for ambient discovery.")] = None,
    host: Annotated[str | None, typer.Option("--host", help="Explicit host URL.")] = None,
    force: Annotated[bool, typer.Option("--force", "-f", help="Delete without prompting.")] = False,
    quiet: Annotated[bool, typer.Option("--quiet", "-q", help="Do not print confirmation.")] = False,
    timeout: Annotated[float, typer.Option("--timeout", help="Request timeout in seconds.")] = 10.0,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    if not force and not typer.confirm(f"Delete artifact {artifact}?", default=False):
        raise typer.Exit(1)
    try:
        client = _client(timeout, api_key_env)
        client.delete_artifact(_host_url(client, entry, host), artifact, timeout=timeout)
        if not quiet:
            console.print(f"Deleted {artifact}")
    except Exception as exc:
        fail("paglets artifacts rm", exc, code=1)
