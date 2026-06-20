# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import signal
import sys

from .host import Host


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
    args = parser.parse_args(argv)

    host = Host(
        name=args.name,
        host=args.host,
        port=args.port,
        mesh=args.mesh,
        peers=args.peer,
        mesh_multicast=args.mesh_multicast,
        mesh_version=args.mesh_version,
        persistence_dir=args.persistence_dir,
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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
