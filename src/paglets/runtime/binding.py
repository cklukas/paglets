# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import socket
from collections.abc import Sequence


def _resolve_bind_host(host: str) -> str:
    value = str(host).strip()
    if value.casefold() == "auto":
        return _auto_lan_host()
    return value


def _bind_host_specs(host: str | Sequence[str]) -> list[str]:
    values = [host] if isinstance(host, str) else list(host)
    specs = [str(value).strip() for value in values if str(value).strip()]
    if not specs:
        raise ValueError("at least one bind host is required")
    return specs


def _resolve_bind_hosts(host: str | Sequence[str]) -> list[str]:
    values = _bind_host_specs(host)
    resolved: list[str] = []
    for value in values:
        bind_host = _resolve_bind_host(value)
        if bind_host not in resolved:
            resolved.append(bind_host)
    return resolved


def _resolve_public_host(bind_host: str) -> str:
    if bind_host in {"0.0.0.0", ""}:
        return _auto_lan_host()
    return bind_host


def _auto_lan_host() -> str:
    """Return the local IP used for outbound LAN/default-route traffic."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            address = sock.getsockname()[0]
            if address and not address.startswith("127."):
                return address
    except OSError:
        pass

    try:
        hostname = socket.gethostname()
        for family, _type, _proto, _canonname, sockaddr in socket.getaddrinfo(hostname, None, socket.AF_INET):
            if family != socket.AF_INET:
                continue
            address = sockaddr[0]
            if address and not address.startswith("127."):
                return address
    except OSError:
        pass
    return "127.0.0.1"
