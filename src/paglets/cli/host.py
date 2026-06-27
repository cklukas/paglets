# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from paglets.config.env import DEFAULT_API_KEY_ENV
from paglets.config.startup import DEFAULT_LAUNCH_CONFIG_PATH
from paglets.persistence.storage import DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES
from paglets.tooling import cli as legacy_host

app = typer.Typer(help="Start a Paglets host.", invoke_without_command=True, no_args_is_help=True)


@app.callback()
def run(
    name: Annotated[str, typer.Option("--name", "-n", help="Host/context name, e.g. alpha.")],
    host: Annotated[str, typer.Option("--host", help="Bind host.")] = "127.0.0.1",
    bind_public: Annotated[
        list[str] | None, typer.Option("--bind-public", help="Bind to a public/LAN host address; repeatable.")
    ] = None,
    port: Annotated[int, typer.Option("--port", "-p", help="Bind port.")] = 8765,
    peer: Annotated[list[str] | None, typer.Option("--peer", help="Peer host URL to join; repeatable.")] = None,
    mesh: Annotated[bool, typer.Option("--mesh/--no-mesh", help="Enable host mesh discovery.")] = True,
    mesh_multicast: Annotated[
        bool | None, typer.Option("--mesh-multicast/--no-mesh-multicast", help="Enable UDP multicast beacons.")
    ] = None,
    mesh_lan_discovery: Annotated[
        bool | None,
        typer.Option("--mesh-lan-discovery/--no-mesh-lan-discovery", help="Enable TCP LAN discovery."),
    ] = None,
    public_url: Annotated[str | None, typer.Option("--public-url", help="Externally reachable base URL.")] = None,
    connect_to: Annotated[str | None, typer.Option("--connect-to", help="Relay base URL for outbound mode.")] = None,
    relay_offline_after: Annotated[
        float, typer.Option("--relay-offline-after", help="Seconds before a relayed host is treated as offline.")
    ] = 30.0,
    relay_delivery_timeout: Annotated[
        float | None, typer.Option("--relay-delivery-timeout", help="Relayed delivery acknowledgment timeout.")
    ] = None,
    relay_queue_limit: Annotated[int, typer.Option("--relay-queue-limit", help="Relayed delivery queue limit.")] = 1024,
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help=f"API key environment variable; defaults to {DEFAULT_API_KEY_ENV}."),
    ] = None,
    mesh_version: Annotated[str | None, typer.Option("--mesh-version", help="Override mesh code-version gate.")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag", help="Advertise a host tag; repeatable.")] = None,
    property: Annotated[
        list[str] | None, typer.Option("--property", help="Advertise a host property as KEY=VALUE; repeatable.")
    ] = None,
    persistence_dir: Annotated[
        str | None, typer.Option("--persistence-dir", help="Durable inactive paglet directory.")
    ] = None,
    persistent_storage_quota: Annotated[
        str, typer.Option("--persistent-storage-quota", help="Persistent storage quota, e.g. 10M, or none.")
    ] = str(DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES),
    artifact_max_size: Annotated[
        str, typer.Option("--artifact-max-size", help="Maximum artifact size, e.g. 1G, or none.")
    ] = "1G",
    artifact_storage_quota: Annotated[
        str, typer.Option("--artifact-storage-quota", help="Total artifact storage quota, e.g. 10G, or none.")
    ] = "10G",
    artifact_spool_ttl: Annotated[
        float, typer.Option("--artifact-spool-ttl", help="Artifact spool cleanup TTL in seconds.")
    ] = 24 * 60 * 60,
    launch_config: Annotated[
        Path, typer.Option("--launch-config", help="Launch config TOML path.")
    ] = DEFAULT_LAUNCH_CONFIG_PATH,
    sync_launch_config: Annotated[
        bool, typer.Option("--sync-launch-config/--no-sync-launch-config", help="Copy/update bundled launch config.")
    ] = True,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Accept launch config update prompts.")] = False,
    auto_update_from_git: Annotated[
        bool,
        typer.Option("--auto-update-from-git", help="Run git update on startup and accept trusted update requests."),
    ] = False,
) -> None:
    argv = ["--name", name, "--host", host, "--port", str(port)]
    for value in bind_public or []:
        argv.extend(["--bind-public", value])
    for value in peer or []:
        argv.extend(["--peer", value])
    argv.append("--mesh" if mesh else "--no-mesh")
    if mesh_multicast is not None:
        argv.append("--mesh-multicast" if mesh_multicast else "--no-mesh-multicast")
    if mesh_lan_discovery is not None:
        argv.append("--mesh-lan-discovery" if mesh_lan_discovery else "--no-mesh-lan-discovery")
    _extend_optional(argv, "--public-url", public_url)
    _extend_optional(argv, "--connect-to", connect_to)
    argv.extend(["--relay-offline-after", str(relay_offline_after), "--relay-queue-limit", str(relay_queue_limit)])
    _extend_optional(argv, "--relay-delivery-timeout", relay_delivery_timeout)
    _extend_optional(argv, "--api-key-env", api_key_env)
    _extend_optional(argv, "--mesh-version", mesh_version)
    for value in tag or []:
        argv.extend(["--tag", value])
    for value in property or []:
        argv.extend(["--property", value])
    _extend_optional(argv, "--persistence-dir", persistence_dir)
    argv.extend(
        [
            "--persistent-storage-quota",
            persistent_storage_quota,
            "--artifact-max-size",
            artifact_max_size,
            "--artifact-storage-quota",
            artifact_storage_quota,
            "--artifact-spool-ttl",
            str(artifact_spool_ttl),
            "--launch-config",
            str(launch_config),
        ]
    )
    argv.append("--sync-launch-config" if sync_launch_config else "--no-sync-launch-config")
    if yes:
        argv.append("--yes")
    if auto_update_from_git:
        argv.append("--auto-update-from-git")
    raise typer.Exit(legacy_host.main(argv))


def _extend_optional(argv: list[str], option: str, value: object | None) -> None:
    if value is not None:
        argv.extend([option, str(value)])
