# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

from paglets.config.startup import ResolvedResidentService, resolve_resident_service
from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import (
    HostError,
    InvalidAgentError,
    PagletError,
    ServiceNotFoundError,
)
from paglets.core.runtime_values import (
    ResidentLifecycle,
    ServiceScope,
    require_enum,
)
from paglets.persistence.persistency import DeactivationPolicy, DeactivationRequest
from paglets.remote.proxy import PagletProxy
from paglets.remote.references import PagletProxyRef
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire, qualified_name
from paglets.services.contracts import ServiceContract, ServiceHandle, ServiceRecord
from paglets.services.resident import DEFAULT_SERVICE_LEASE_TTL_SECONDS, RESIDENT_SERVICE_METADATA_KEY, ServiceLease

SHUTDOWN_DEACTIVATE_TIMEOUT_SECONDS = 0.5
MESH_SERVICE_LOOKUP_TIMEOUT_SECONDS = 1.0


@dataclass(slots=True)
class _ManagedResidentService:
    agent_cls: type[Paglet]
    state_class: type[PagletState]
    state_wire: dict[str, Any]
    agent_id: str
    contract: ServiceContract
    scope: ServiceScope
    lifecycle: ResidentLifecycle
    idle_timeout: float
    singleton: bool = True
    init: Any = None
    in_flight: int = 0
    leases: dict[str, float] = field(default_factory=dict)
    last_used: float = field(default_factory=time.time)


class _ResidentServicesMixin:
    def advertise_service(
        self,
        agent_id: str,
        name: str,
        *,
        capabilities: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
        ttl: float | None = None,
    ) -> ServiceRecord:
        require_enum(scope, ServiceScope, "scope")
        self._require_agent(agent_id)
        existing = self._services.record(name, agent_id)
        merged_metadata = dict(metadata or {})
        if existing is not None and RESIDENT_SERVICE_METADATA_KEY in existing.metadata:
            merged_metadata[RESIDENT_SERVICE_METADATA_KEY] = existing.metadata[RESIDENT_SERVICE_METADATA_KEY]
        record = self._services.advertise(
            host_name=self.name,
            host_url=self.address,
            name=name,
            proxy=PagletProxyRef(self.address, agent_id),
            capabilities=capabilities,
            metadata=merged_metadata,
            scope=scope,
            ttl=ttl,
        )
        self._emit("service-advertise", agent_id=agent_id, service_name=name, data=record.to_wire())
        return record

    def unadvertise_service(self, name: str, *, agent_id: str | None = None) -> list[ServiceRecord]:
        removed = self._services.unadvertise(name, agent_id=agent_id)
        for record in removed:
            self._emit("service-remove", agent_id=record.proxy.agent_id, service_name=record.name)
        return removed

    def lookup_service(
        self,
        name: str,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> ServiceRecord | None:
        require_enum(scope, ServiceScope, "scope")
        matches = self.lookup_services(name, capability=capability, scope=scope)
        return matches[0] if matches else None

    def lookup_services(
        self,
        name: str | None = None,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> list[ServiceRecord]:
        require_enum(scope, ServiceScope, "scope")
        records = self._services.lookup_all(name, capability)
        if scope is ServiceScope.MESH:
            records.extend(self._lookup_mesh_services(name=name, capability=capability))
        return records

    def _lookup_mesh_services(self, *, name: str | None = None, capability: str | None = None) -> list[ServiceRecord]:
        records: list[ServiceRecord] = []
        for host_ref in self.mesh.hosts(online_only=True, include_self=False):
            query: dict[str, str] = {}
            if name is not None:
                query["name"] = name
            if capability is not None:
                query["capability"] = capability
            suffix = f"?{urlencode(query)}" if query else ""
            try:
                separator = "&" if suffix else "?"
                payload = self.client.get_json(
                    f"{host_ref.url.rstrip('/')}/services{suffix}{separator}scope=mesh",
                    timeout=MESH_SERVICE_LOOKUP_TIMEOUT_SECONDS,
                )
            except PagletError:
                continue
            for item in payload.get("services", []):
                if isinstance(item, dict):
                    records.append(ServiceRecord.from_wire(item))
        return records

    def _start_resident_services(self) -> None:
        config = self.launch_config
        if config is None:
            return
        for resident_service in config.resident_services:
            if not resident_service.enabled:
                self._emit(
                    "resident-service-skip",
                    agent_id=resident_service.agent_id,
                    data={"reason": "disabled", "use": resident_service.use, "class": resident_service.class_name},
                )
                continue
            try:
                resolved = resolve_resident_service(resident_service)
                self._declare_resident_service(resolved)
            except Exception as exc:
                self._emit(
                    "resident-service-failed",
                    agent_id=resident_service.agent_id,
                    data={
                        "use": resident_service.use,
                        "class": resident_service.class_name,
                        "error": str(exc),
                    },
                )

    def _declare_resident_service(self, resolved: ResolvedResidentService) -> None:
        contract = resolved.spec.contract
        managed = _ManagedResidentService(
            agent_cls=resolved.agent_cls,
            state_class=resolved.agent_cls.state_class(),
            state_wire=dataclass_to_wire(resolved.state),
            agent_id=resolved.agent_id,
            contract=contract,
            scope=resolved.scope,
            lifecycle=resolved.lifecycle,
            idle_timeout=resolved.idle_timeout,
            singleton=resolved.singleton,
            init=resolved.init,
        )
        with self._lock:
            self._resident_services[resolved.agent_id] = managed
        record = self._services.advertise(
            host_name=self.name,
            host_url=self.address,
            name=contract.name,
            proxy=PagletProxyRef(self.address, resolved.agent_id),
            capabilities=contract.capabilities,
            metadata=self._resident_service_metadata(managed),
            scope=resolved.scope,
        )
        self._emit(
            "resident-service-declare",
            agent_id=resolved.agent_id,
            class_name=qualified_name(resolved.agent_cls),
            service_name=contract.name,
            data=record.to_wire(),
        )
        self._emit("service-advertise", agent_id=resolved.agent_id, service_name=contract.name, data=record.to_wire())
        if resolved.lifecycle is ResidentLifecycle.EAGER:
            self._ensure_resident_service_active(resolved.agent_id)

    def _ensure_resident_service_active(self, agent_id: str) -> PagletProxy:
        lock = self._resident_activation_lock(agent_id)
        with lock:
            with self._lock:
                if agent_id in self._agents:
                    return PagletProxy(self.address, agent_id, self.client)
                managed = self._resident_services.get(agent_id)
                inactive = self._inactive.get(agent_id)
            if managed is None:
                raise InvalidAgentError(f"No managed resident service {agent_id!r} on {self.name}")
            if inactive is not None:
                proxy = self.activate(agent_id)
                self._mark_resident_service_used(agent_id)
                self._emit(
                    "resident-service-activate",
                    agent_id=agent_id,
                    class_name=qualified_name(managed.agent_cls),
                    service_name=managed.contract.name,
                    data={"lifecycle": managed.lifecycle.value},
                )
                return proxy

            state = dataclass_from_wire(managed.state_class, managed.state_wire)
            proxy = self.create(managed.agent_cls, state, init=managed.init, agent_id=agent_id)
            self._mark_resident_service_used(agent_id)
            self._emit(
                "resident-service-create",
                agent_id=agent_id,
                class_name=qualified_name(managed.agent_cls),
                service_name=managed.contract.name,
                data={"lifecycle": managed.lifecycle.value},
            )
            return proxy

    def lease_service_handle(
        self,
        handle: ServiceHandle,
        *,
        ttl: float = DEFAULT_SERVICE_LEASE_TTL_SECONDS,
    ) -> ServiceLease:
        record = handle.record
        host_url = record.host_url or record.proxy.host_url
        if host_url.rstrip("/") == self.address.rstrip("/"):
            payload = self.acquire_resident_service_lease(record.proxy.agent_id, record.name, ttl=ttl)
        else:
            payload = self.client.post_json(
                f"{host_url.rstrip('/')}/services/leases",
                {
                    "agent_id": record.proxy.agent_id,
                    "service_name": record.name,
                    "ttl": ttl,
                },
            )
        return ServiceLease(
            handle=handle,
            lease_id=str(payload["lease_id"]),
            host_url=host_url,
            expires_at=float(payload["expires_at"]),
            client=self.client,
        )

    def acquire_resident_service_lease(self, agent_id: str, service_name: str, *, ttl: float) -> dict[str, Any]:
        ttl = DEFAULT_SERVICE_LEASE_TTL_SECONDS if ttl is None else float(ttl)
        if ttl <= 0:
            raise HostError("service lease ttl must be positive")
        lease_id = uuid.uuid4().hex
        now = time.time()
        expires_at = now + ttl
        with self._lock:
            managed = self._resident_services.get(agent_id)
            if managed is None or managed.contract.name != service_name:
                raise ServiceNotFoundError(f"No managed resident service {service_name!r} for agent {agent_id!r}")
            managed.leases[lease_id] = expires_at
            managed.last_used = now
        try:
            self._ensure_resident_service_active(agent_id)
        except Exception:
            with self._lock:
                managed = self._resident_services.get(agent_id)
                if managed is not None:
                    managed.leases.pop(lease_id, None)
            raise
        self._emit(
            "service-lease-acquire",
            agent_id=agent_id,
            service_name=service_name,
            data={"lease_id": lease_id, "expires_at": expires_at, "ttl": ttl},
        )
        return {"lease_id": lease_id, "expires_at": expires_at}

    def release_resident_service_lease(self, lease_id: str) -> dict[str, bool]:
        released = False
        agent_id = None
        service_name = None
        with self._lock:
            for managed in self._resident_services.values():
                if lease_id in managed.leases:
                    managed.leases.pop(lease_id, None)
                    managed.last_used = time.time()
                    agent_id = managed.agent_id
                    service_name = managed.contract.name
                    released = True
                    break
        if released:
            self._emit(
                "service-lease-release",
                agent_id=agent_id,
                service_name=service_name,
                data={"lease_id": lease_id},
            )
        return {"released": released}

    def _resident_activation_lock(self, agent_id: str) -> threading.Lock:
        with self._lock:
            lock = self._resident_activation_locks.get(agent_id)
            if lock is None:
                lock = threading.Lock()
                self._resident_activation_locks[agent_id] = lock
            return lock

    def _resident_service_metadata(self, managed: _ManagedResidentService) -> dict[str, Any]:
        metadata = managed.contract.advertise_metadata()
        metadata[RESIDENT_SERVICE_METADATA_KEY] = {
            "agent_id": managed.agent_id,
            "agent_class_name": qualified_name(managed.agent_cls),
            "lifecycle": managed.lifecycle.value,
            "idle_timeout": managed.idle_timeout,
        }
        return metadata

    def _is_resident_service_record(self, record: ServiceRecord) -> bool:
        return RESIDENT_SERVICE_METADATA_KEY in record.metadata

    def _begin_resident_service_call(self, agent_id: str) -> None:
        with self._lock:
            managed = self._resident_services.get(agent_id)
            if managed is not None:
                managed.in_flight += 1

    def _end_resident_service_call(self, agent_id: str) -> None:
        with self._lock:
            managed = self._resident_services.get(agent_id)
            if managed is not None:
                managed.in_flight = max(0, managed.in_flight - 1)
                managed.last_used = time.time()

    def _mark_resident_service_used(self, agent_id: str) -> None:
        with self._lock:
            managed = self._resident_services.get(agent_id)
            if managed is not None:
                managed.last_used = time.time()

    def _resident_service_shutdown_policy(self, agent_id: str) -> DeactivationPolicy:
        with self._lock:
            managed = self._resident_services.get(agent_id)
        return DeactivationPolicy(activate_on_startup=managed is None or managed.lifecycle is ResidentLifecycle.EAGER)

    def _resident_maintenance(self, now: float) -> None:
        expired_leases: list[tuple[str, str, str]] = []
        due_agent_ids: list[str] = []
        with self._lock:
            for managed in self._resident_services.values():
                for lease_id, expires_at in list(managed.leases.items()):
                    if expires_at <= now:
                        managed.leases.pop(lease_id, None)
                        expired_leases.append((managed.agent_id, managed.contract.name, lease_id))
                if (
                    managed.lifecycle is ResidentLifecycle.LAZY
                    and managed.agent_id in self._agents
                    and managed.in_flight == 0
                    and not managed.leases
                    and now - managed.last_used >= managed.idle_timeout
                ):
                    due_agent_ids.append(managed.agent_id)

        for agent_id, service_name, lease_id in expired_leases:
            self._emit(
                "service-lease-expire",
                agent_id=agent_id,
                service_name=service_name,
                data={"lease_id": lease_id},
            )

        for agent_id in due_agent_ids:
            with self._lock:
                managed = self._resident_services.get(agent_id)
                still_due = (
                    managed is not None
                    and managed.lifecycle is ResidentLifecycle.LAZY
                    and agent_id in self._agents
                    and managed.in_flight == 0
                    and not managed.leases
                    and now - managed.last_used >= managed.idle_timeout
                )
            if not still_due or managed is None:
                continue
            try:
                self.deactivate(
                    agent_id,
                    DeactivationRequest(
                        reason="resident-service-idle",
                        source="resident-service-manager",
                        policy=DeactivationPolicy(
                            activate_on_message=True,
                            queue_messages_when_inactive=True,
                            activate_on_startup=False,
                        ),
                    ),
                )
                self._emit(
                    "resident-service-idle-deactivate",
                    agent_id=agent_id,
                    class_name=qualified_name(managed.agent_cls),
                    service_name=managed.contract.name,
                    data={"idle_timeout": managed.idle_timeout},
                )
            except PagletError as exc:
                self._emit(
                    "resident-service-failed",
                    agent_id=agent_id,
                    class_name=qualified_name(managed.agent_cls),
                    service_name=managed.contract.name,
                    error=str(exc),
                )
