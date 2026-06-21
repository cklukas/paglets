# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import sys

from . import git_update
from .admin import register_running_server
from .errors import PagletError
from .host import Host
from .runtime_values import LaunchConfigSyncAction
from .startup import DEFAULT_LAUNCH_CONFIG_PATH, load_launch_config, sync_launch_config
from .storage import DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    reexec_args = list(argv) if argv is not None else sys.argv[1:]
    git_repo_root: Path | None = None
    git_process_start_head = ""

    if args.auto_update_from_git:
        try:
            git_repo_root = git_update.find_repo_root(Path.cwd())
            git_process_start_head = git_update.current_head(git_repo_root)
            update_result = git_update.update_checkout(
                git_repo_root,
                process_start_head=git_process_start_head,
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
                print(f"paglets host: previous launch config moved to {sync_result.backup_path}", file=sys.stderr, flush=True)
        launch_config = load_launch_config(launch_config_path)
    except PagletError as exc:
        print(f"paglets-host: {exc}", file=sys.stderr)
        return 1

    bind_host = args.bind_public if args.bind_public is not None else args.host
    host = Host(
        name=args.name,
        host=bind_host,
        port=args.port,
        mesh=args.mesh,
        peers=args.peer,
        mesh_multicast=args.mesh_multicast,
        mesh_version=args.mesh_version,
        persistence_dir=args.persistence_dir,
        persistent_storage_quota_bytes=args.persistent_storage_quota,
        launch_config=launch_config,
        launch_config_sync_result=sync_result,
        auto_update_from_git=args.auto_update_from_git,
        git_repo_root=git_repo_root,
        git_process_start_head=git_process_start_head,
        auto_update_restart_callback=lambda: _reexec(reexec_args),
        auto_update_reporter=lambda message: print(f"paglets host auto-update: {message}", file=sys.stderr, flush=True),
    )

    def shutdown(_signum, _frame):
        host.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    host.start_background()
    try:
        register_running_server(host.name, host.address)
    except Exception as exc:
        print(f"paglets host warning: could not update server config: {exc}", file=sys.stderr, flush=True)
    if host.mesh.version_warning:
        print(f"paglets host warning: {host.mesh.version_warning}", file=sys.stderr, flush=True)
    print(
        f"paglets host {host.name!r} listening at {host.address} "
        f"(mesh {'on' if args.mesh else 'off'}, version {host.mesh.code_version})",
        flush=True,
    )
    if args.auto_update_from_git:
        host.broadcast_git_update()
    host.serve_forever()
    return 0


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
        help="Bind to the detected LAN IP, refresh it if it changes, or bind to HOST; repeat for multiple explicit hosts",
    )
    parser.add_argument("--port", type=int, default=8765, help="Bind port")
    parser.add_argument("--peer", action="append", default=[], help="Peer host URL to join; repeatable")
    parser.add_argument("--mesh", action=argparse.BooleanOptionalAction, default=True, help="Enable host mesh discovery")
    parser.add_argument(
        "--mesh-multicast",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable UDP multicast mesh beacons",
    )
    parser.add_argument("--mesh-version", default=None, help="Override mesh code-version gate")
    parser.add_argument("--persistence-dir", default=None, help="Directory for this host's durable inactive paglets")
    parser.add_argument(
        "--persistent-storage-quota",
        type=_parse_size_or_none,
        default=DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES,
        help="Persistent storage quota per paglet class, e.g. 10M, or 'none'",
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
    os.execv(sys.executable, [sys.executable, "-m", "paglets.cli", *argv])


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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
