# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import os
import signal
import sys
import threading
from pathlib import Path
from typing import Annotated

import typer

import paglets.tooling.git_update as git_update
from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.config.startup import (
    DEFAULT_LAUNCH_CONFIG_PATH,
    load_launch_config,
)
from paglets.config.startup import (
    sync_launch_config as sync_launch_file,
)
from paglets.core.context_events import ContextEvent
from paglets.core.errors import PagletError
from paglets.core.runtime_values import LaunchConfigSyncAction
from paglets.persistence.storage import DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES
from paglets.runtime.host import Host
from paglets.tooling import cli as host_helpers

from .console import console, err_console

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
    if connect_to and auto_update_from_git:
        raise typer.BadParameter("--auto-update-from-git cannot be used with --connect-to")
    try:
        host_properties = host_helpers._parse_properties(list(property or []))
        api_key = resolve_api_key(api_key_env)
        persistent_quota = host_helpers._parse_size_or_none(persistent_storage_quota)
        artifact_max = host_helpers._parse_size_or_none(artifact_max_size)
        artifact_quota = host_helpers._parse_size_or_none(artifact_storage_quota)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if (connect_to or public_url) and not api_key:
        raise typer.BadParameter(f"--public-url and --connect-to require an API key from {DEFAULT_API_KEY_ENV}")

    reexec_args = list(sys.argv[1:])
    restart_requested = threading.Event()
    git_repo_root: Path | None = None
    git_process_start_head = ""

    if auto_update_from_git:
        try:
            git_repo_root = git_update.find_repo_root(Path.cwd())
            git_process_start_head = git_update.current_head(git_repo_root)
            update_result = git_update.update_checkout(
                git_repo_root,
                process_start_head=git_process_start_head,
                sync_dependencies=not host_helpers._defer_uv_sync_until_reexec(),
            )
        except git_update.GitUpdateError as exc:
            err_console.print(f"paglets host: git auto-update failed: {exc}")
            raise typer.Exit(1) from exc
        if not update_result.ok:
            host_helpers._print_git_update_failure(update_result)
            raise typer.Exit(1)
        if update_result.restart_required:
            err_console.print(
                f"paglets host: git auto-update moved HEAD from "
                f"{update_result.process_start_head} to {update_result.after_head}; restarting"
            )
            _reexec(reexec_args)

    try:
        sync_result = sync_launch_file(
            launch_config,
            enabled=sync_launch_config,
            yes=yes,
            interactive=sys.stdin.isatty(),
            output=sys.stderr,
        )
        if sync_result.action in (LaunchConfigSyncAction.COPIED, LaunchConfigSyncAction.UPDATED):
            err_console.print(f"paglets host: {sync_result.message}")
            if sync_result.backup_path is not None:
                err_console.print(f"paglets host: previous launch config moved to {sync_result.backup_path}")
        launch = load_launch_config(launch_config)
    except PagletError as exc:
        err_console.print(f"paglets host: {exc}")
        raise typer.Exit(1) from exc

    _validate_bind_public_values(bind_public)
    bind_host = bind_public if bind_public is not None else host
    effective_multicast = mesh_multicast
    effective_lan_discovery = mesh_lan_discovery
    if connect_to:
        effective_multicast = False if effective_multicast is None else effective_multicast
        effective_lan_discovery = False if effective_lan_discovery is None else effective_lan_discovery
    else:
        effective_multicast = True if effective_multicast is None else effective_multicast
        effective_lan_discovery = True if effective_lan_discovery is None else effective_lan_discovery

    runtime_host = Host(
        name=name,
        host=bind_host,
        port=port,
        api_key=api_key,
        public_url=public_url,
        connect_to=connect_to,
        mesh=mesh,
        peers=list(peer or []),
        mesh_multicast=effective_multicast,
        mesh_lan_discovery=effective_lan_discovery,
        mesh_version=mesh_version,
        persistence_dir=persistence_dir,
        persistent_storage_quota_bytes=persistent_quota,
        artifact_max_bytes=artifact_max,
        artifact_storage_quota_bytes=artifact_quota,
        artifact_spool_ttl_seconds=artifact_spool_ttl,
        launch_config=launch,
        launch_config_sync_result=sync_result,
        auto_update_from_git=auto_update_from_git,
        git_repo_root=git_repo_root,
        git_process_start_head=git_process_start_head,
        auto_update_restart_callback=restart_requested.set,
        auto_update_reporter=lambda message: err_console.print(f"paglets host auto-update: {message}"),
        relay_offline_after=relay_offline_after,
        relay_delivery_timeout=relay_delivery_timeout,
        relay_queue_limit=relay_queue_limit,
        tags=list(tag or []),
        properties=host_properties,
    )

    def shutdown(_signum, _frame):
        runtime_host.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    runtime_host.add_listener(_print_host_error_event)
    runtime_host.start_background()
    if runtime_host.mesh.version_warning:
        err_console.print(f"paglets host warning: {runtime_host.mesh.version_warning}")
    mode = "connected via" if connect_to else "listening at"
    console.print(
        f"paglets host {runtime_host.name!r} {mode} {connect_to or runtime_host.address} "
        f"(mesh {'on' if mesh else 'off'}, version {runtime_host.mesh.code_version})"
    )
    if auto_update_from_git and not getattr(runtime_host, "relay_mode", False):
        runtime_host.broadcast_git_update(
            targets=host_helpers._auto_update_discovery_targets(runtime_host.port),
            validate_targets=True,
            report_unreachable=False,
        )
    runtime_host.serve_forever()
    restart_scheduled = bool(getattr(runtime_host, "_auto_update_restart_scheduled", False))
    if restart_scheduled and not restart_requested.is_set():
        restart_requested.wait(timeout=5.0)
    if restart_requested.is_set():
        err_console.print("paglets host: git auto-update restart requested; restarting")
        _reexec(reexec_args)
    if restart_scheduled:
        err_console.print("paglets host: git auto-update restart was scheduled but no restart callback ran")
        raise typer.Exit(1)


def _reexec(argv: list[str]) -> None:
    executable = sys.executable
    os.execvp(executable, [executable, "-m", "paglets.cli.app", *argv])


_ERROR_EVENT_KINDS = {
    "event-listener-failed",
    "message-failed",
    "paglet-crashed",
    "relay-client-error",
    "relay-delivery-failed",
    "relay-target-offline",
    "resident-service-failed",
    "startup-agent-failed",
    "transfer-failed",
}


def _print_host_error_event(event: ContextEvent) -> None:
    text = _host_error_event_text(event)
    if text is not None:
        err_console.print(text)


def _host_error_event_text(event: ContextEvent) -> str | None:
    error = event.error or str(event.data.get("error") or "")
    if event.kind not in _ERROR_EVENT_KINDS and not error:
        return None
    details: list[str] = []
    if event.agent_id:
        details.append(f"agent={event.agent_id}")
    if event.service_name:
        details.append(f"service={event.service_name}")
    if event.class_name:
        details.append(f"class={event.class_name}")
    if event.message_id:
        details.append(f"message={event.message_id}")
    if event.data.get("destination"):
        details.append(f"destination={event.data['destination']}")
    if event.data.get("stage"):
        details.append(f"stage={event.data['stage']}")
    suffix = f" ({', '.join(details)})" if details else ""
    message = f": {error}" if error else ""
    return f"paglets host {event.kind}{suffix}{message}"


def _validate_bind_public_values(values: list[str] | None) -> None:
    for value in values or []:
        if str(value).strip().startswith("-"):
            raise typer.BadParameter(
                f"--bind-public value {value!r} looks like an option; use '--bind-public auto' or pass a host/IP"
            )
