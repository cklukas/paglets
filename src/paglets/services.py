# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Literal

from .references import PagletProxyRef


ServiceScope = Literal["local", "mesh"]


@dataclass(frozen=True, slots=True)
class ServiceRecord:
    name: str
    proxy: PagletProxyRef
    capabilities: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    scope: ServiceScope = "local"
    host_name: str = ""
    host_url: str = ""
    advertised_at: float = field(default_factory=time.time)
    expires_at: float | None = None

    @property
    def expired(self) -> bool:
        return self.expires_at is not None and self.expires_at <= time.time()

    def matches(self, name: str, capability: str | None = None) -> bool:
        if self.name != name or self.expired:
            return False
        return capability is None or capability in self.capabilities

    def to_wire(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "proxy": self.proxy.to_wire(),
            "capabilities": list(self.capabilities),
            "metadata": self.metadata,
            "scope": self.scope,
            "host_name": self.host_name,
            "host_url": self.host_url,
            "advertised_at": self.advertised_at,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "ServiceRecord":
        return cls(
            name=str(payload["name"]),
            proxy=PagletProxyRef.from_wire(payload["proxy"]),
            capabilities=tuple(str(item) for item in payload.get("capabilities", [])),
            metadata=dict(payload.get("metadata") or {}),
            scope=str(payload.get("scope") or "local"),  # type: ignore[arg-type]
            host_name=str(payload.get("host_name") or ""),
            host_url=str(payload.get("host_url") or ""),
            advertised_at=float(payload.get("advertised_at", time.time())),
            expires_at=float(payload["expires_at"]) if payload.get("expires_at") is not None else None,
        )


class ServiceRegistry:
    def __init__(self):
        self._records: dict[tuple[str, str], ServiceRecord] = {}

    def advertise(
        self,
        *,
        host_name: str,
        host_url: str,
        name: str,
        proxy: PagletProxyRef,
        capabilities: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
        scope: ServiceScope = "local",
        ttl: float | None = None,
    ) -> ServiceRecord:
        record = ServiceRecord(
            name=name,
            proxy=proxy,
            capabilities=tuple(capabilities or ()),
            metadata=metadata or {},
            scope=scope,
            host_name=host_name,
            host_url=host_url,
            expires_at=time.time() + ttl if ttl is not None else None,
        )
        self._records[(name, proxy.agent_id)] = record
        return record

    def unadvertise(self, name: str, agent_id: str | None = None) -> list[ServiceRecord]:
        removed: list[ServiceRecord] = []
        for key, record in list(self._records.items()):
            if record.name == name and (agent_id is None or record.proxy.agent_id == agent_id):
                removed.append(self._records.pop(key))
        return removed

    def remove_agent(self, agent_id: str) -> list[ServiceRecord]:
        removed: list[ServiceRecord] = []
        for key, record in list(self._records.items()):
            if record.proxy.agent_id == agent_id:
                removed.append(self._records.pop(key))
        return removed

    def lookup(self, name: str, capability: str | None = None) -> ServiceRecord | None:
        self._expire()
        matches = [record for record in self._records.values() if record.matches(name, capability)]
        return matches[0] if matches else None

    def lookup_all(
        self,
        name: str | None = None,
        capability: str | None = None,
        scope: ServiceScope | None = None,
    ) -> list[ServiceRecord]:
        self._expire()
        return [
            record
            for record in self._records.values()
            if (name is None or record.name == name)
            and (capability is None or capability in record.capabilities)
            and (scope is None or record.scope == scope)
        ]

    def _expire(self) -> None:
        for key, record in list(self._records.items()):
            if record.expired:
                self._records.pop(key, None)
