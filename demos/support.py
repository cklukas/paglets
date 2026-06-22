# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import importlib
import socket
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from paglets.runtime.host import Host


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run_importable_main(module_name: str) -> int | None:
    """Run a demo through its package module so paglet classes are importable."""

    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    module = importlib.import_module(module_name)
    return module.main()


@contextmanager
def local_hosts(
    *names: str,
    mesh: bool = False,
    mesh_version: str = "examples-local",
    mesh_multicast: bool = False,
    persistence_root: str | Path | None = None,
) -> Iterator[list[Host]]:
    tmpdir = tempfile.TemporaryDirectory(prefix="paglets-examples-") if persistence_root is None else None
    root = Path(tmpdir.name if tmpdir is not None else persistence_root).expanduser()
    hosts = [
        Host(
            name,
            port=free_port(),
            mesh=mesh,
            mesh_version=mesh_version,
            mesh_multicast=mesh_multicast,
            persistence_dir=root / name,
        )
        for name in names
    ]
    for host in hosts:
        host.start_background()
    if mesh:
        for host in hosts:
            for peer in hosts:
                if peer is not host:
                    host.mesh.add_seed(peer.address)
            host.mesh.gossip_once()
    try:
        yield hosts
    finally:
        for host in reversed(hosts):
            host.stop()
        if tmpdir is not None:
            tmpdir.cleanup()
