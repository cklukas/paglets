# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from paglets.core.runtime_values import ResidentLifecycle, ServiceScope, require_enum
from paglets.remote.client import HostClient
from paglets.services.contracts import ServiceContract

if TYPE_CHECKING:  # pragma: no cover
    from paglets.services.contracts import ServiceHandle


RESIDENT_SERVICE_METADATA_KEY = "paglets.resident_service"
DEFAULT_RESIDENT_IDLE_TIMEOUT_SECONDS = 30.0
DEFAULT_SERVICE_LEASE_TTL_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class ResidentServiceSpec:
    """Class-level declaration for a managed resident service."""

    contract: ServiceContract
    scope: ServiceScope = ServiceScope.LOCAL
    lifecycle: ResidentLifecycle = ResidentLifecycle.LAZY
    agent_id: str | None = None
    singleton: bool = True
    idle_timeout: float = DEFAULT_RESIDENT_IDLE_TIMEOUT_SECONDS
    state: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_enum(self.lifecycle, ResidentLifecycle, "lifecycle")
        require_enum(self.scope, ServiceScope, "scope")
        if self.idle_timeout < 0:
            raise ValueError("ResidentServiceSpec idle_timeout must be non-negative")


@dataclass(slots=True)
class ServiceLease:
    """TTL-backed lease that keeps a managed resident service active."""

    handle: ServiceHandle
    lease_id: str
    host_url: str
    expires_at: float
    client: HostClient = field(default_factory=HostClient)
    _released: bool = False

    def __enter__(self) -> ServiceHandle:
        return self.handle

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    @property
    def expired(self) -> bool:
        return self.expires_at <= time.time()

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self.client.post_json(
            f"{self.host_url.rstrip('/')}/services/leases/{self.lease_id}/release",
            {},
        )
