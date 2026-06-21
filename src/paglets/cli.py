# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
from pathlib import Path
import signal
import sys

from .errors import PagletError
from .host import Host
from .runtime_values import LaunchConfigSyncAction
from .startup import DEFAULT_LAUNCH_CONFIG_PATH, load_launch_config, sync_launch_config
from .storage import DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a paglets host")
    parser.add_argument("--name", required=True, help="Host/context name, e.g. alpha")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
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
    args = parser.parse_args(argv)

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

    host = Host(
        name=args.name,
        host=args.host,
        port=args.port,
        mesh=args.mesh,
        peers=args.peer,
        mesh_multicast=args.mesh_multicast,
        mesh_version=args.mesh_version,
        persistence_dir=args.persistence_dir,
        persistent_storage_quota_bytes=args.persistent_storage_quota,
        launch_config=launch_config,
        launch_config_sync_result=sync_result,
    )

    def shutdown(_signum, _frame):
        host.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    host.start_background()
    if host.mesh.version_warning:
        print(f"paglets host warning: {host.mesh.version_warning}", file=sys.stderr, flush=True)
    print(
        f"paglets host {host.name!r} listening at {host.address} "
        f"(mesh {'on' if args.mesh else 'off'}, version {host.mesh.code_version})",
        flush=True,
    )
    host.serve_forever()
    return 0


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
