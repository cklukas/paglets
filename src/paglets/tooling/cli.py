# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import os
import shutil
import signal
import sys
import threading
from ipaddress import ip_address
from pathlib import Path, PureWindowsPath
from urllib.parse import urlparse

import paglets.tooling.git_update as git_update
from paglets.config.startup import DEFAULT_LAUNCH_CONFIG_PATH, load_launch_config, sync_launch_config
from paglets.core.errors import PagletError
from paglets.core.runtime_values import LaunchConfigSyncAction
from paglets.persistence.storage import DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES
from paglets.remote.admin import discover_lan_entry_servers, discover_mesh_entry_servers
from paglets.runtime.host import Host


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.connect_to and args.auto_update_from_git:
        parser.error("--auto-update-from-git cannot be used with --connect-to")
    try:
        host_properties = _parse_properties(args.property)
    except ValueError as exc:
        parser.error(str(exc))
    api_key = os.environ.get(args.api_key_env) if args.api_key_env else None
    if args.api_key_env and not api_key:
        parser.error(f"--api-key-env {args.api_key_env!r} is not set or is empty")
    if (args.connect_to or args.public_url) and not api_key:
        parser.error("--public-url and --connect-to require --api-key-env")
    reexec_args = list(argv) if argv is not None else sys.argv[1:]
    restart_requested = threading.Event()
    git_repo_root: Path | None = None
    git_process_start_head = ""

    if args.auto_update_from_git:
        try:
            git_repo_root = git_update.find_repo_root(Path.cwd())
            git_process_start_head = git_update.current_head(git_repo_root)
            update_result = git_update.update_checkout(
                git_repo_root,
                process_start_head=git_process_start_head,
                sync_dependencies=not _defer_uv_sync_until_reexec(),
            )
        except git_update.GitUpdateError as exc:
            print(f"paglets-host: git auto-update failed: {exc}", file=sys.stderr)
            return 1
        if not update_result.ok:
            _print_git_update_failure(update_result)
            return 1
        if update_result.restart_required:
            print(
                f"paglets-host: git auto-update moved HEAD from "
                f"{update_result.process_start_head} to {update_result.after_head}; restarting",
                file=sys.stderr,
                flush=True,
            )
            _reexec(reexec_args)

    launch_config_path = Path(args.launch_config).expanduser()
    try:
        sync_result = sync_launch_config(
            launch_config_path,
            enabled=args.sync_launch_config,
            yes=args.yes,
            interactive=sys.stdin.isatty(),
            output=sys.stderr,
        )
        if sync_result.action in (LaunchConfigSyncAction.COPIED, LaunchConfigSyncAction.UPDATED):
            print(f"paglets host: {sync_result.message}", file=sys.stderr, flush=True)
            if sync_result.backup_path is not None:
                print(
                    f"paglets host: previous launch config moved to {sync_result.backup_path}",
                    file=sys.stderr,
                    flush=True,
                )
        launch_config = load_launch_config(launch_config_path)
    except PagletError as exc:
        print(f"paglets-host: {exc}", file=sys.stderr)
        return 1

    bind_host = args.bind_public if args.bind_public is not None else args.host
    mesh_multicast = args.mesh_multicast
    mesh_lan_discovery = args.mesh_lan_discovery
    if args.connect_to:
        mesh_multicast = False if mesh_multicast is None else mesh_multicast
        mesh_lan_discovery = False if mesh_lan_discovery is None else mesh_lan_discovery
    else:
        mesh_multicast = True if mesh_multicast is None else mesh_multicast
        mesh_lan_discovery = True if mesh_lan_discovery is None else mesh_lan_discovery
    host = Host(
        name=args.name,
        host=bind_host,
        port=args.port,
        api_key=api_key,
        public_url=args.public_url,
        connect_to=args.connect_to,
        mesh=args.mesh,
        peers=args.peer,
        mesh_multicast=mesh_multicast,
        mesh_lan_discovery=mesh_lan_discovery,
        mesh_version=args.mesh_version,
        persistence_dir=args.persistence_dir,
        persistent_storage_quota_bytes=args.persistent_storage_quota,
        artifact_max_bytes=args.artifact_max_size,
        artifact_storage_quota_bytes=args.artifact_storage_quota,
        artifact_spool_ttl_seconds=args.artifact_spool_ttl,
        launch_config=launch_config,
        launch_config_sync_result=sync_result,
        auto_update_from_git=args.auto_update_from_git,
        git_repo_root=git_repo_root,
        git_process_start_head=git_process_start_head,
        auto_update_restart_callback=restart_requested.set,
        auto_update_reporter=lambda message: print(f"paglets host auto-update: {message}", file=sys.stderr, flush=True),
        relay_offline_after=args.relay_offline_after,
        relay_delivery_timeout=args.relay_delivery_timeout,
        relay_queue_limit=args.relay_queue_limit,
        tags=args.tag,
        properties=host_properties,
    )

    def shutdown(_signum, _frame):
        host.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    host.start_background()
    if host.mesh.version_warning:
        print(f"paglets host warning: {host.mesh.version_warning}", file=sys.stderr, flush=True)
    mode = "connected via" if args.connect_to else "listening at"
    print(
        f"paglets host {host.name!r} {mode} {args.connect_to or host.address} "
        f"(mesh {'on' if args.mesh else 'off'}, version {host.mesh.code_version})",
        flush=True,
    )
    if args.auto_update_from_git and not getattr(host, "relay_mode", False):
        host.broadcast_git_update(
            targets=_auto_update_discovery_targets(host.port),
            validate_targets=True,
            report_unreachable=False,
        )
    host.serve_forever()
    restart_scheduled = bool(getattr(host, "_auto_update_restart_scheduled", False))
    if restart_scheduled and not restart_requested.is_set():
        restart_requested.wait(timeout=5.0)
    if restart_requested.is_set():
        print("paglets-host: git auto-update restart requested; restarting", file=sys.stderr, flush=True)
        _reexec(reexec_args)
    if restart_scheduled:
        print("paglets-host: git auto-update restart was scheduled but no restart callback ran", file=sys.stderr)
        return 1
    return 0


def _auto_update_discovery_targets(port: int) -> list[str]:
    discovered = discover_mesh_entry_servers(timeout=1.0)
    discovered.extend(discover_lan_entry_servers(ports={port}, timeout=0.25))
    return [server.url for server in discovered if _auto_update_discovery_target_allowed(server.url, port)]


def _defer_uv_sync_until_reexec() -> bool:
    return os.name == "nt"


def _auto_update_discovery_target_allowed(url: str, port: int) -> bool:
    try:
        parsed = urlparse(url if "://" in url else f"http://{url}")
    except ValueError:
        return False
    host = parsed.hostname or ""
    try:
        is_loopback = ip_address(host).is_loopback
    except ValueError:
        is_loopback = host.casefold() in {"localhost"}
    if not is_loopback:
        return True
    return parsed.port == int(port)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a paglets host")
    parser.add_argument("--name", required=True, help="Host/context name, e.g. alpha")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument(
        "--bind-public",
        action="append",
        nargs="?",
        const="auto",
        default=None,
        metavar="HOST",
        help=(
            "Bind to the detected LAN IP, refresh it if it changes, or bind to HOST; repeat for multiple explicit hosts"
        ),
    )
    parser.add_argument("--port", type=int, default=8765, help="Bind port")
    parser.add_argument("--peer", action="append", default=[], help="Peer host URL to join; repeatable")
    parser.add_argument(
        "--mesh", action=argparse.BooleanOptionalAction, default=True, help="Enable host mesh discovery"
    )
    parser.add_argument(
        "--mesh-multicast",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable UDP multicast mesh beacons",
    )
    parser.add_argument(
        "--mesh-lan-discovery",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable TCP LAN discovery for paglets hosts when multicast/seed peers are incomplete",
    )
    parser.add_argument("--public-url", default=None, help="Externally reachable base URL, e.g. https://host/paglets")
    parser.add_argument("--connect-to", default=None, help="Relay base URL for outbound-only connect mode")
    parser.add_argument(
        "--relay-offline-after",
        type=float,
        default=30.0,
        help="Seconds without relay polling before a connected host is treated as offline",
    )
    parser.add_argument(
        "--relay-delivery-timeout",
        type=float,
        default=None,
        help="Default seconds to wait for relayed delivery acknowledgments when no transfer timeout is supplied",
    )
    parser.add_argument(
        "--relay-queue-limit",
        type=int,
        default=1024,
        help="Maximum queued relay deliveries per connected host",
    )
    parser.add_argument(
        "--api-key-env", default=None, help="Environment variable containing the paglets bearer API key"
    )
    parser.add_argument("--mesh-version", default=None, help="Override mesh code-version gate")
    parser.add_argument("--tag", action="append", default=[], help="Advertise a host tag; repeatable")
    parser.add_argument(
        "--property",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Advertise a host property; repeatable",
    )
    parser.add_argument("--persistence-dir", default=None, help="Directory for this host's durable inactive paglets")
    parser.add_argument(
        "--persistent-storage-quota",
        type=_parse_size_or_none,
        default=DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES,
        help="Persistent storage quota per paglet class, e.g. 10M, or 'none'",
    )
    parser.add_argument(
        "--artifact-max-size",
        type=_parse_size_or_none,
        default=1024**3,
        help="Maximum accepted artifact size, e.g. 1G, or 'none'",
    )
    parser.add_argument(
        "--artifact-storage-quota",
        type=_parse_size_or_none,
        default=10 * 1024**3,
        help="Total host artifact storage quota, e.g. 10G, or 'none'",
    )
    parser.add_argument(
        "--artifact-spool-ttl",
        type=float,
        default=24 * 60 * 60,
        help="Seconds before abandoned artifact temp/spool files are eligible for cleanup",
    )
    parser.add_argument("--launch-config", default=str(DEFAULT_LAUNCH_CONFIG_PATH), help="Launch config TOML path")
    parser.add_argument(
        "--sync-launch-config",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Copy/update the bundled demo launch config before startup",
    )
    parser.add_argument("--yes", action="store_true", help="Accept launch config update prompts")
    parser.add_argument(
        "--auto-update-from-git",
        action="store_true",
        help="Run git fetch/pull on startup and accept trusted mesh update requests",
    )
    return parser


def _reexec(argv: list[str]) -> None:
    executable, reexec_argv = _reexec_command(argv)
    os.execvp(executable, reexec_argv)


def _reexec_command(
    argv: list[str],
    *,
    uv_executable: str | None = None,
    executable: str | None = None,
    windows: bool | None = None,
) -> tuple[str, list[str]]:
    uv = uv_executable if uv_executable is not None else shutil.which("uv")
    if uv:
        exec_path = uv if uv_executable is not None else "uv"
        return exec_path, [_argv0(exec_path, windows=windows), "run", "python", "-m", "paglets.tooling.cli", *argv]
    executable = executable or sys.executable
    return executable, _python_reexec_argv(argv, executable=executable, windows=windows)


def _python_reexec_argv(
    argv: list[str],
    *,
    executable: str | None = None,
    windows: bool | None = None,
) -> list[str]:
    executable = executable or sys.executable
    return [_argv0(executable, windows=windows), "-m", "paglets.tooling.cli", *argv]


def _argv0(executable: str, *, windows: bool | None = None) -> str:
    windows = os.name == "nt" if windows is None else windows
    return PureWindowsPath(executable).name if windows else executable


def _print_git_update_failure(result: git_update.GitUpdateResult) -> None:
    if result.status == "dirty-worktree":
        print(
            "paglets-host: --auto-update-from-git requires a clean git checkout; startup cancelled.",
            file=sys.stderr,
        )
        if result.stdout:
            print(f"paglets-host: git status:\n{result.stdout}", file=sys.stderr)
        return
    print(f"paglets-host: git auto-update failed: {result.error or result.status}", file=sys.stderr)
    if result.stdout:
        print(f"paglets-host: git stdout:\n{result.stdout}", file=sys.stderr)
    if result.stderr:
        print(f"paglets-host: git stderr:\n{result.stderr}", file=sys.stderr)


def _parse_size_or_none(value: str) -> int | None:
    text = value.strip()
    if text.casefold() in {"none", "unlimited"}:
        return None
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


def _parse_properties(values: list[str]) -> dict[str, str]:
    properties: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        key = key.strip()
        if not separator or not key:
            raise ValueError("--property values must use KEY=VALUE")
        properties[key] = item
    return properties


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
