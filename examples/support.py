# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from contextlib import contextmanager
import socket
from typing import Iterator

from paglets import Host


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def local_hosts(
    *names: str,
    mesh: bool = False,
    mesh_version: str = "examples-local",
    mesh_multicast: bool = False,
) -> Iterator[list[Host]]:
    hosts = [
        Host(
            name,
            port=free_port(),
            mesh=mesh,
            mesh_version=mesh_version,
            mesh_multicast=mesh_multicast,
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
