# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from paglets.artifacts import ArtifactRef
from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.remote.admin import select_reachable_entry_server
from paglets.remote.client import HostClient


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        api_key = resolve_api_key(args.api_key_env)
        client = HostClient(timeout=args.timeout, api_key=api_key)
        host_url = args.host or select_reachable_entry_server(entry_name=args.entry, client=client).url
        if args.command == "list":
            refs = client.list_artifacts(host_url, owner_agent_id=args.owner, timeout=args.timeout)
            if args.json:
                print(json.dumps({"artifacts": [ref.to_wire() for ref in refs]}, indent=2, sort_keys=True))
            else:
                _print_table(refs)
            return 0
        if args.command == "metadata":
            ref = client.artifact_metadata(host_url, args.artifact_id, timeout=args.timeout)
            print(json.dumps({"artifact": ref.to_wire()}, indent=2, sort_keys=True))
            return 0
        if args.command == "download":
            ref = client.artifact_metadata(host_url, args.artifact_id, timeout=args.timeout)
            client.download_artifact(ref, Path(args.output), move=args.move, timeout=args.timeout)
            print(str(Path(args.output)))
            return 0
        if args.command == "delete":
            client.delete_artifact(host_url, args.artifact_id, timeout=args.timeout)
            if not args.quiet:
                print(f"deleted {args.artifact_id}")
            return 0
    except Exception as exc:
        print(f"paglets-artifacts: {exc}", file=sys.stderr)
        return 1
    parser.error(f"unknown command {args.command!r}")
    return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect and recover Paglets artifacts")
    parser.add_argument("--entry", default=None, help="Entry host name for ambient discovery")
    parser.add_argument("--host", default=None, help="Explicit host URL; may be a relay host URL")
    parser.add_argument("--timeout", type=float, default=10.0, help="Request timeout in seconds")
    parser.add_argument(
        "--api-key-env",
        default=None,
        help=f"Environment variable to read the paglets bearer API key from; defaults to {DEFAULT_API_KEY_ENV}",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List artifacts")
    list_parser.add_argument("--owner", default=None, help="Filter by owner agent id")
    list_parser.add_argument("--json", action="store_true", help="Print JSON output")

    metadata = subparsers.add_parser("metadata", help="Print artifact metadata")
    metadata.add_argument("artifact_id")

    download = subparsers.add_parser("download", help="Download an artifact")
    download.add_argument("artifact_id")
    download.add_argument("output")
    download.add_argument("--move", action="store_true", help="Delete the source artifact after verified download")

    delete = subparsers.add_parser("delete", help="Delete an artifact")
    delete.add_argument("artifact_id")
    delete.add_argument("--quiet", action="store_true", help="Do not print confirmation")
    return parser


def _print_table(refs: list[ArtifactRef]) -> None:
    if not refs:
        print("No artifacts.")
        return
    print(f"{'artifact':12} {'size':>10} {'owner':12} name")
    for ref in refs:
        print(f"{ref.artifact_id[:12]:12} {_bytes(ref.size_bytes):>10} {ref.owner_agent_id[:12]:12} {ref.name}")


def _bytes(value: int) -> str:
    amount = float(max(0, int(value)))
    for unit in ("B", "KB", "MB", "GB"):
        if amount < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(amount)} B"
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{amount:.1f} GB"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
