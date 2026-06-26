# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import uuid
from pathlib import Path
from typing import Any

from paglets.artifacts import PagletFileRef
from paglets.core.agent import Paglet
from paglets.core.errors import (
    HostError,
    InvalidAgentError,
)
from paglets.core.events import CloneEvent, MobilityEvent, PersistencyEvent
from paglets.core.runtime_values import ServiceScope
from paglets.persistence.persistency import DeactivationPolicy, DeactivationRequest
from paglets.persistence.storage import StorageStatus
from paglets.remote.client import HostClient
from paglets.remote.proxy import PagletProxy
from paglets.remote.transfer import TransferTicket
from paglets.remote.transport import (
    release_local_pickle_sender,
    start_local_pickle_sender,
)
from paglets.runtime.child_endpoint import _ChildEndpoint
from paglets.runtime.process_protocol import ChildConfig, _target_to_wire
from paglets.serialization.codec import dataclass_to_wire, qualified_name
from paglets.services.contracts import ServiceRecord


class _ChildMeshFacade:
    def __init__(self, host: _ChildHostFacade):
        self._host = host

    @property
    def code_version(self) -> str:
        return str(self._host._call("mesh_code_version") or "")

    def hosts(self, *, online_only: bool = True, include_self: bool = True):
        from paglets.remote.mesh import HostRef

        payload = self._host._call(
            "available_hosts",
            {"online_only": online_only, "include_self": include_self},
        )
        return [HostRef.from_wire(item) for item in payload.get("hosts", [])]

    def lookup(self, name_or_url: str):
        from paglets.remote.mesh import HostRef

        payload = self._host._call("host_status", {"name_or_url": name_or_url})
        ref = payload.get("host")
        return HostRef.from_wire(ref) if ref is not None else None

    def is_online(self, name_or_url: str) -> bool:
        return bool(self._host._call("is_host_online", {"name_or_url": name_or_url}).get("online"))

    def wait_for_host(self, name_or_url: str, *, timeout: float = 10.0, interval: float = 0.25):
        from paglets.remote.mesh import HostRef

        payload = self._host._call(
            "wait_for_host",
            {"name_or_url": name_or_url, "timeout": timeout, "interval": interval},
        )
        return HostRef.from_wire(payload["host"])

    def resolve_url(self, name_or_url: str) -> str:
        return str(self._host._call("resolve_host_url", {"name_or_url": name_or_url})["url"])


class _ChildHostFacade:
    def __init__(self, endpoint: _ChildEndpoint, config: ChildConfig):
        self._endpoint = endpoint
        self.name = config.host_name
        self.address = config.host_address
        self.agent_id = config.agent_id
        self.client = HostClient(api_key=config.host_api_key)
        self.mesh = _ChildMeshFacade(self)
        self._message_condition = threading.Condition()
        self._agent: Paglet | None = None
        self._terminal = False

    def attach_agent(self, agent: Paglet) -> None:
        self._agent = agent

    @property
    def terminal(self) -> bool:
        return self._terminal

    def _call(self, op: str, payload: dict[str, Any] | None = None) -> Any:
        return self._endpoint.host_call(op, payload or {})

    def _call_with_state(self, op: str, payload: dict[str, Any], state: Any) -> Any:
        streamed = dict(payload)
        stream = start_local_pickle_sender(dataclass_to_wire(state))
        streamed["state_stream"] = stream
        try:
            return self._call(op, streamed)
        finally:
            release_local_pickle_sender(stream)

    def get_proxy(
        self,
        agent_id: str,
        host_url: str | None = None,
        *,
        include_inactive: bool = True,
    ) -> PagletProxy | None:
        if host_url is not None and host_url.rstrip("/") != self.address.rstrip("/"):
            return PagletProxy(host_url.rstrip("/"), agent_id, self.client)
        payload = self._call("get_proxy", {"agent_id": agent_id, "include_inactive": include_inactive})
        proxy = payload.get("proxy")
        return PagletProxy.from_wire(proxy, self.client) if proxy is not None else None

    def get_proxies(self, state: int = 1) -> list[PagletProxy]:
        payload = self._call("get_proxies", {"state": state})
        return [PagletProxy.from_wire(item, self.client) for item in payload.get("proxies", [])]

    def set_process_cpu_affinity(self, agent_id: str, cpu_core_ids: list[int]) -> dict[str, Any]:
        return dict(self._call("set_process_cpu_affinity", {"agent_id": agent_id, "cpu_core_ids": cpu_core_ids}))

    def get_property(self, key: str, default: Any = None) -> Any:
        return self._call("get_property", {"key": key, "default": default}).get("value")

    def set_property(self, key: str, value: Any) -> None:
        self._call("set_property", {"key": key, "value": value})

    def create(
        self,
        agent_cls: type[Paglet],
        state: Any = None,
        *,
        init: Any = None,
        agent_id: str | None = None,
    ) -> PagletProxy:
        state_cls = agent_cls.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        payload = self._call_with_state(
            "create_paglet",
            {
                "agent_class_name": qualified_name(agent_cls),
                "state_class_name": qualified_name(state_cls),
                "init": init,
                "agent_id": agent_id,
                "host_url": None,
            },
            state,
        )
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def create_remote(
        self,
        target: str,
        agent_cls: type[Paglet],
        state: Any = None,
        *,
        init: Any = None,
        agent_id: str | None = None,
    ) -> PagletProxy:
        state_cls = agent_cls.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        payload = self._call_with_state(
            "create_paglet",
            {
                "agent_class_name": qualified_name(agent_cls),
                "state_class_name": qualified_name(state_cls),
                "init": init,
                "agent_id": agent_id,
                "host_url": target,
            },
            state,
        )
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def dispatch(self, agent_id: str, target: str | TransferTicket) -> PagletProxy:
        self._require_self(agent_id)
        preflight = self._call("preflight_transfer", {"target": _target_to_wire(target)})
        target_info = dict(preflight["target_info"])
        ticket = TransferTicket.from_wire(preflight["ticket"])
        agent = self._require_agent()
        agent.on_dispatching(
            MobilityEvent(
                agent_id=agent_id,
                host_name=self.name,
                host_address=self.address,
                source_host_name=self.name,
                source_host_address=self.address,
                target_host_name=target_info["name"],
                target_host_address=target_info["address"],
                reason="dispatch",
            )
        )
        agent.resources.cleanup(reason="dispatch")
        payload = self._call_with_state(
            "complete_dispatch",
            {
                "ticket": ticket.to_wire(),
                "target_info": target_info,
                "resources": agent.resources.status(),
            },
            agent.state,
        )
        self._terminal = True
        self._endpoint.request_exit()
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def clone(self, agent_id: str, *, target: str | TransferTicket | None = None) -> PagletProxy:
        self._require_self(agent_id)
        preflight = self._call("preflight_transfer", {"target": _target_to_wire(target or self.address)})
        target_info = dict(preflight["target_info"])
        ticket = TransferTicket.from_wire(preflight["ticket"])
        clone_id = uuid.uuid4().hex
        agent = self._require_agent()
        event = CloneEvent(
            agent_id=agent_id,
            host_name=self.name,
            host_address=self.address,
            source_agent_id=agent_id,
            clone_agent_id=clone_id,
            source_host_name=self.name,
            source_host_address=self.address,
            target_host_name=target_info["name"],
            target_host_address=target_info["address"],
        )
        agent.on_cloning(event)
        payload = self._call_with_state(
            "complete_clone",
            {
                "ticket": ticket.to_wire(),
                "target_info": target_info,
                "clone_id": clone_id,
            },
            agent.state,
        )
        agent.on_cloned(event)
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def deactivate(self, agent_id: str, request: DeactivationRequest | None = None) -> PagletProxy:
        self._require_self(agent_id)
        agent = self._require_agent()
        request = request or DeactivationRequest()
        policy = agent.deactivation_policy(request)
        if not isinstance(policy, DeactivationPolicy):
            raise HostError(f"{agent.__class__.__name__}.deactivation_policy() must return DeactivationPolicy")
        agent.on_deactivating(
            PersistencyEvent(
                agent_id=agent_id,
                host_name=self.name,
                host_address=self.address,
                reason=request.reason,
                request=request,
                policy=policy,
            )
        )
        agent.resources.cleanup(reason="deactivate")
        payload = self._call_with_state(
            "complete_deactivate",
            {
                "request": request.to_wire(),
                "policy": policy.to_wire(),
                "resources": agent.resources.status(),
            },
            agent.state,
        )
        self._terminal = True
        self._endpoint.request_exit()
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def dispose(self, agent_id: str) -> None:
        self._require_self(agent_id)
        agent = self._require_agent()
        agent.on_disposing(
            PersistencyEvent(agent_id=agent_id, host_name=self.name, host_address=self.address, reason="dispose")
        )
        agent.resources.cleanup(reason="dispose")
        self._call_with_state(
            "complete_dispose",
            {
                "resources": agent.resources.status(),
            },
            agent.state,
        )
        self._terminal = True
        self._endpoint.request_exit()

    def wait_message(self, agent_id: str, *, timeout: float | None = None) -> bool:
        self._require_self(agent_id)
        with self._message_condition:
            return self._message_condition.wait(timeout)

    def notify_message(self, agent_id: str) -> None:
        self._require_self(agent_id)
        with self._message_condition:
            self._message_condition.notify(1)

    def notify_all_messages(self, agent_id: str) -> None:
        self._require_self(agent_id)
        with self._message_condition:
            self._message_condition.notify_all()

    def resources_for(self, agent_id: str):
        self._require_self(agent_id)
        return self._require_agent().resources

    def work_dir_for(self, agent_id: str, *, create: bool = True) -> Path:
        self._require_self(agent_id)
        payload = self._call("work_dir", {"agent_id": agent_id, "create": create})
        return Path(payload["path"])

    def persistent_storage_for(self, agent_id: str, *, quota_bytes: int | None = None):
        self._require_self(agent_id)
        payload = self._call("persistent_storage", {"agent_id": agent_id, "quota_bytes": quota_bytes})
        return _ChildManagedStorage(self, payload["root"], quota_bytes=payload.get("quota_bytes"))

    def register_file_for(
        self,
        agent_id: str,
        path: str | Path,
        *,
        name: str | None = None,
        mode: str = "copy",
    ) -> PagletFileRef:
        self._require_self(agent_id)
        payload = self._call("register_file", {"path": str(path), "name": name, "mode": mode})
        return PagletFileRef.from_wire(payload["file"])

    def registered_files_for(self, agent_id: str) -> list[PagletFileRef]:
        self._require_self(agent_id)
        payload = self._call("registered_files")
        return [PagletFileRef.from_wire(item) for item in payload.get("files", [])]

    def unregister_file_for(self, agent_id: str, name_or_ref: str | PagletFileRef) -> None:
        self._require_self(agent_id)
        name = name_or_ref.name if isinstance(name_or_ref, PagletFileRef) else str(name_or_ref)
        self._call("unregister_file", {"name": name})

    def registered_file_path_for(self, agent_id: str, name_or_ref: str | PagletFileRef) -> Path:
        self._require_self(agent_id)
        name = name_or_ref.name if isinstance(name_or_ref, PagletFileRef) else str(name_or_ref)
        payload = self._call("registered_file_path", {"name": name})
        return Path(payload["path"])

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
        payload = self._call(
            "advertise_service",
            {
                "agent_id": agent_id,
                "name": name,
                "capabilities": list(capabilities or []),
                "metadata": metadata or {},
                "scope": scope.value,
                "ttl": ttl,
            },
        )
        return ServiceRecord.from_wire(payload["service"])

    def unadvertise_service(self, name: str, *, agent_id: str | None = None) -> list[ServiceRecord]:
        payload = self._call("unadvertise_service", {"agent_id": agent_id, "name": name})
        return [ServiceRecord.from_wire(item) for item in payload.get("services", [])]

    def lookup_service(
        self,
        name: str,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> ServiceRecord | None:
        payload = self._call(
            "lookup_service",
            {"name": name, "capability": capability, "scope": scope.value},
        )
        service = payload.get("service")
        return ServiceRecord.from_wire(service) if service is not None else None

    def lookup_services(
        self,
        name: str | None = None,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> list[ServiceRecord]:
        payload = self._call(
            "lookup_services",
            {"name": name, "capability": capability, "scope": scope.value},
        )
        return [ServiceRecord.from_wire(item) for item in payload.get("services", [])]

    def lease_service_handle(self, handle, *, ttl: float = 60.0):
        payload = self._call(
            "lease_service_handle",
            {"record": handle.record.to_wire(), "ttl": ttl},
        )
        from paglets.services.resident import ServiceLease

        return ServiceLease(
            handle=handle,
            lease_id=str(payload["lease_id"]),
            host_url=str(payload["host_url"]),
            expires_at=float(payload["expires_at"]),
            client=self.client,
        )

    def health(self) -> dict[str, Any]:
        return dict(self._call("health"))

    def _require_self(self, agent_id: str) -> None:
        if agent_id != self.agent_id:
            raise InvalidAgentError(f"Child paglet cannot manage different paglet {agent_id!r}")

    def _require_agent(self) -> Paglet:
        if self._agent is None:
            raise InvalidAgentError("paglet child is not attached")
        return self._agent


class _ChildManagedStorage:
    def __init__(self, host: _ChildHostFacade, root: str, *, quota_bytes: int | None):
        self._host = host
        self.root = Path(root)
        self.quota_bytes = quota_bytes

    def read_bytes(self, path: Path | str) -> bytes:
        payload = self._host._call("storage_read_bytes", {"path": str(path), "quota_bytes": self.quota_bytes})
        return bytes(payload["data"])

    def write_bytes(self, path: Path | str, data: bytes) -> Path:
        payload = self._host._call(
            "storage_write_bytes",
            {"path": str(path), "data": bytes(data), "quota_bytes": self.quota_bytes},
        )
        return Path(payload["path"])

    def write_text(self, path: Path | str, text: str, *, encoding: str = "utf-8") -> Path:
        return self.write_bytes(path, text.encode(encoding))

    def delete(self, path: Path | str) -> None:
        self._host._call("storage_delete", {"path": str(path), "quota_bytes": self.quota_bytes})

    def clear(self) -> None:
        self._host._call("storage_clear", {"quota_bytes": self.quota_bytes})

    def status(self) -> StorageStatus:
        payload = self._host._call("storage_status", {"quota_bytes": self.quota_bytes})
        return StorageStatus(
            root=str(payload["root"]),
            used_bytes=int(payload["used_bytes"]),
            quota_bytes=payload.get("quota_bytes"),
            available_bytes=payload.get("available_bytes"),
        )
