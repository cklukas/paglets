# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .client import HostClient


@dataclass(frozen=True, slots=True)
class PagletProxyRef:
    """Serializable reference to a paglet proxy."""

    host_url: str
    agent_id: str

    @classmethod
    def from_proxy(cls, proxy: Any) -> "PagletProxyRef":
        return cls(host_url=proxy.host_url, agent_id=proxy.agent_id)

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "PagletProxyRef":
        return cls(host_url=str(payload["host_url"]), agent_id=str(payload["agent_id"]))

    def to_wire(self) -> dict[str, str]:
        return {"host_url": self.host_url, "agent_id": self.agent_id}

    def resolve(self, context_or_client: Any = None):
        from .agent import PagletContext
        from .proxy import PagletProxy

        if isinstance(context_or_client, PagletContext):
            return PagletProxy(self.host_url, self.agent_id, context_or_client.host.client)
        client = context_or_client if isinstance(context_or_client, HostClient) else HostClient()
        return PagletProxy(self.host_url, self.agent_id, client)
