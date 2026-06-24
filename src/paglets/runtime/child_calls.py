# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from typing import Any

from paglets.core.agent import ACTIVE
from paglets.core.errors import (
    HostError,
)
from paglets.core.runtime_values import (
    EnvelopeKind,
    ServiceScope,
    enum_from_wire,
)
from paglets.persistence.persistency import DeactivationPolicy, DeactivationRequest, InactiveRecord
from paglets.remote.transfer import TransferTicket
from paglets.runtime.process_runtime import ChildProcessController
from paglets.runtime.relay import _is_relay_transport_url
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire, resolve_qualified_name
from paglets.services.contracts import ServiceRecord
from paglets.services.resident import DEFAULT_SERVICE_LEASE_TTL_SECONDS

SHUTDOWN_DEACTIVATE_TIMEOUT_SECONDS = 0.5
MESH_SERVICE_LOOKUP_TIMEOUT_SECONDS = 1.0


class _ChildCallMixin:
    def _handle_child_crash(self, record: ChildProcessController) -> None:
        with self._lock:
            current = self._agents.get(record.agent_id)
            if current is not record:
                return
            self._mailboxes.pop(record.agent_id, None)
        for service in self._services.remove_agent(record.agent_id, keep=self._is_resident_service_record):
            self._emit("service-remove", agent_id=record.agent_id, service_name=service.name)
        self._emit(
            "paglet-crashed",
            agent_id=record.agent_id,
            class_name=record.agent_class_name,
            data={"pid": record.pid, "exitcode": record.exitcode},
            error=record.last_error,
        )

    def _handle_child_host_call(self, agent_id: str, op: str, payload: dict[str, Any]) -> Any:
        if op == "get_proxy":
            proxy = self.get_proxy(str(payload["agent_id"]))
            return {"proxy": proxy.to_wire() if proxy is not None else None}
        if op == "get_proxies":
            proxies = self.get_proxies(int(payload.get("state", ACTIVE)))
            return {"proxies": [proxy.to_wire() for proxy in proxies]}
        if op == "set_process_cpu_affinity":
            record = self._require_agent(str(payload["agent_id"]))
            pid = int(record.pid or 0)
            if pid <= 0:
                raise HostError(f"Paglet {record.agent_id!r} has no running child process")
            cpu_core_ids = [int(cpu_id) for cpu_id in payload.get("cpu_core_ids") or []]
            from paglets.system.compute_slots.affinity import apply_process_cpu_affinity

            return {"affinity": dataclass_to_wire(apply_process_cpu_affinity(pid, cpu_core_ids))}
        if op == "get_property":
            return {"value": self.get_property(str(payload["key"]), payload.get("default"))}
        if op == "set_property":
            self.set_property(str(payload["key"]), payload.get("value"))
            return {"ok": True}
        if op == "create_paglet":
            agent_cls = resolve_qualified_name(str(payload["agent_class_name"]))
            state_cls = resolve_qualified_name(str(payload["state_class_name"]))
            state = dataclass_from_wire(state_cls, payload.get("state") or {})
            host_url = payload.get("host_url")
            if host_url is not None and str(host_url).rstrip("/") != self.address.rstrip("/"):
                proxy = self.create_remote(
                    str(host_url), agent_cls, state, init=payload.get("init"), agent_id=payload.get("agent_id")
                )
            else:
                proxy = self.create(agent_cls, state, init=payload.get("init"), agent_id=payload.get("agent_id"))
            return {"proxy": proxy.to_wire()}
        if op == "preflight_transfer":
            target = self._target_from_child_payload(payload.get("target") or {})
            ticket = self._prepare_ticket(target)
            target_info = self._preflight_transfer(ticket)
            return {"ticket": ticket.to_wire(), "target_info": target_info}
        if op == "complete_dispatch":
            return self._complete_child_dispatch(agent_id, payload)
        if op == "complete_clone":
            return self._complete_child_clone(agent_id, payload)
        if op == "complete_deactivate":
            return self._complete_child_deactivate(agent_id, payload)
        if op == "complete_dispose":
            return self._complete_child_dispose(agent_id, payload)
        if op == "advertise_service":
            record = self.advertise_service(
                str(payload["agent_id"]),
                str(payload["name"]),
                capabilities=payload.get("capabilities"),
                metadata=payload.get("metadata"),
                scope=enum_from_wire(payload.get("scope") or ServiceScope.LOCAL.value, ServiceScope, "scope"),
                ttl=float(payload["ttl"]) if payload.get("ttl") is not None else None,
            )
            return {"service": record.to_wire()}
        if op == "unadvertise_service":
            removed = self.unadvertise_service(str(payload["name"]), agent_id=payload.get("agent_id"))
            return {"services": [record.to_wire() for record in removed]}
        if op == "lookup_service":
            record = self.lookup_service(
                str(payload["name"]),
                capability=payload.get("capability"),
                scope=enum_from_wire(payload.get("scope") or ServiceScope.LOCAL.value, ServiceScope, "scope"),
            )
            return {"service": record.to_wire() if record is not None else None}
        if op == "lookup_services":
            records = self.lookup_services(
                payload.get("name"),
                capability=payload.get("capability"),
                scope=enum_from_wire(payload.get("scope") or ServiceScope.LOCAL.value, ServiceScope, "scope"),
            )
            return {"services": [record.to_wire() for record in records]}
        if op == "lease_service_handle":
            record = ServiceRecord.from_wire(payload["record"])
            host_url = record.host_url or record.proxy.host_url
            if host_url.rstrip("/") == self.address.rstrip("/"):
                lease = self.acquire_resident_service_lease(
                    record.proxy.agent_id, record.name, ttl=float(payload.get("ttl", DEFAULT_SERVICE_LEASE_TTL_SECONDS))
                )
            else:
                lease = self.client.post_json(
                    f"{host_url.rstrip('/')}/services/leases",
                    {
                        "agent_id": record.proxy.agent_id,
                        "service_name": record.name,
                        "ttl": float(payload.get("ttl", DEFAULT_SERVICE_LEASE_TTL_SECONDS)),
                    },
                )
            return {"lease_id": lease["lease_id"], "expires_at": lease["expires_at"], "host_url": host_url}
        if op == "health":
            return self.health()
        if op == "mesh_code_version":
            return self.mesh.code_version
        if op == "available_hosts":
            hosts = self.mesh.hosts(
                online_only=bool(payload.get("online_only", True)),
                include_self=bool(payload.get("include_self", True)),
            )
            return {"hosts": [host.to_wire() for host in hosts]}
        if op == "host_status":
            ref = self.mesh.lookup(str(payload["name_or_url"]))
            return {"host": ref.to_wire() if ref is not None else None}
        if op == "is_host_online":
            return {"online": self.mesh.is_online(str(payload["name_or_url"]))}
        if op == "wait_for_host":
            ref = self.mesh.wait_for_host(
                str(payload["name_or_url"]),
                timeout=float(payload.get("timeout", 10.0)),
                interval=float(payload.get("interval", 0.25)),
            )
            return {"host": ref.to_wire()}
        if op == "resolve_host_url":
            return {"url": self.mesh.resolve_url(str(payload["name_or_url"]))}
        if op == "work_dir":
            return {"path": str(self.work_dir_for(agent_id, create=bool(payload.get("create", True))))}
        if op == "persistent_storage":
            storage = self.persistent_storage_for(agent_id, quota_bytes=payload.get("quota_bytes"))
            return {"root": str(storage.root), "quota_bytes": storage.quota_bytes}
        if op.startswith("storage_"):
            return self._handle_child_storage_call(agent_id, op, payload)
        raise HostError(f"Unknown child host call {op!r}")

    def _target_from_child_payload(self, payload: dict[str, Any]) -> str | TransferTicket:
        if "ticket" in payload:
            return TransferTicket.from_wire(payload["ticket"])
        return str(payload["target"])

    def _complete_child_dispatch(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = self._require_agent(agent_id)
        record.state = dict(payload["state"])
        record.resource_status = dict(payload.get("resources") or {})
        ticket = TransferTicket.from_wire(payload["ticket"])
        target_info = dict(payload["target_info"])
        self._cleanup_agent_work_dir(agent_id)
        envelope = self._make_envelope(record, EnvelopeKind.DISPATCH, target_info, ticket=ticket)
        if not _is_relay_transport_url(str(target_info["address"])):
            self._remove_active_agent(agent_id, record, terminate=False)
            record.set_terminal_proxy_wire({"host_url": target_info["address"], "agent_id": agent_id})
            response = self._post_envelope_with_ticket(ticket, target_info, envelope)
            self._emit("dispatch", agent_id=agent_id, class_name=record.agent_class_name, data={"target": target_info})
            return {"proxy": response["proxy"]}
        record.departing = True
        try:
            response = self._post_envelope_with_ticket(ticket, target_info, envelope)
        except Exception:
            record.departing = False
            raise
        self._remove_active_agent(agent_id, record, terminate=False)
        record.set_terminal_proxy_wire(response["proxy"])
        self._emit("dispatch", agent_id=agent_id, class_name=record.agent_class_name, data={"target": target_info})
        return {"proxy": response["proxy"]}

    def _complete_child_clone(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = self._require_agent(agent_id)
        record.state = dict(payload["state"])
        ticket = TransferTicket.from_wire(payload["ticket"])
        target_info = dict(payload["target_info"])
        clone_id = str(payload["clone_id"])
        envelope = self._make_envelope(
            record,
            EnvelopeKind.CLONE,
            target_info,
            agent_id=clone_id,
            clone_of=agent_id,
            ticket=ticket,
        )
        response = self._post_envelope_with_ticket(ticket, target_info, envelope)
        self._emit(
            "clone",
            agent_id=agent_id,
            class_name=record.agent_class_name,
            data={"clone_agent_id": clone_id, "target": target_info},
        )
        return {"proxy": response["proxy"]}

    def _complete_child_deactivate(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = self._require_agent(agent_id)
        record.state = dict(payload["state"])
        record.resource_status = dict(payload.get("resources") or {})
        request = DeactivationRequest.from_wire(payload.get("request"))
        policy = DeactivationPolicy.from_wire(payload.get("policy"))
        info = {"name": self.name, "address": self.address}
        envelope = self._make_envelope(record, EnvelopeKind.ACTIVATION, info)
        inactive = InactiveRecord(envelope=envelope, policy=policy, request=request)
        self._write_inactive_record(inactive)
        with self._lock:
            self._inactive[agent_id] = inactive
        self._remove_active_agent(agent_id, record, terminate=False)
        self._emit("deactivate", agent_id=agent_id, class_name=record.agent_class_name, data={"reason": request.reason})
        return {"proxy": {"host_url": self.address, "agent_id": agent_id}}

    def _complete_child_dispose(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = self._require_agent(agent_id)
        record.state = dict(payload.get("state") or record.state)
        record.resource_status = dict(payload.get("resources") or {})
        self._cleanup_agent_work_dir(agent_id)
        self._remove_active_agent(agent_id, record, terminate=False)
        self._emit("dispose", agent_id=agent_id, class_name=record.agent_class_name, data={"active": True})
        return {"ok": True}

    def _handle_child_storage_call(self, agent_id: str, op: str, payload: dict[str, Any]) -> Any:
        storage = self.persistent_storage_for(agent_id, quota_bytes=payload.get("quota_bytes"))
        if op == "storage_read_bytes":
            return {"data": storage.read_bytes(str(payload["path"]))}
        if op == "storage_write_bytes":
            path = storage.write_bytes(str(payload["path"]), payload.get("data") or b"")
            return {"path": str(path)}
        if op == "storage_delete":
            storage.delete(str(payload["path"]))
            return {"ok": True}
        if op == "storage_clear":
            storage.clear()
            return {"ok": True}
        if op == "storage_status":
            status = storage.status()
            return {
                "root": status.root,
                "used_bytes": status.used_bytes,
                "quota_bytes": status.quota_bytes,
                "available_bytes": status.available_bytes,
            }
        raise HostError(f"Unknown storage operation {op!r}")
