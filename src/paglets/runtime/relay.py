# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import queue
import threading
import time
import uuid
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

from paglets.artifacts import ArtifactRef
from paglets.core.errors import (
    AuthenticationError,
    ForbiddenError,
    HostError,
    InvalidAgentError,
    NotHandledError,
    PagletCrashedError,
    PagletError,
    PagletInactiveError,
    RemoteHostError,
    ServiceNotFoundError,
    TransferError,
)
from paglets.core.messages import Message
from paglets.core.runtime_values import ServiceScope, enum_from_wire
from paglets.persistence.persistency import DeactivationRequest
from paglets.remote.mesh import HostRef
from paglets.remote.transfer import TransferTicket
from paglets.runtime.envelope import PagletEnvelope
from paglets.services.resident import DEFAULT_SERVICE_LEASE_TTL_SECONDS


@dataclass(slots=True)
class RelayNode:
    name: str
    health: dict[str, Any]
    last_seen: float = field(default_factory=time.time)
    online: bool = True
    active_polls: int = 0
    in_flight: int = 0
    last_poll_started: float = 0.0
    last_poll_finished: float = 0.0
    last_error: str | None = None


@dataclass(slots=True)
class RelayDelivery:
    delivery_id: str
    target: str
    kind: str
    payload: dict[str, Any]
    future: Future[dict[str, Any]]
    created_at: float = field(default_factory=time.time)


class RelayMixin:
    def _connect_relay_url(self) -> str:
        if not self.connect_to:
            return self.public_url or f"http://{self.public_host}:{self.port}"
        return f"{self.connect_to.rstrip('/')}/relay/hosts/{quote(self.name, safe='')}"

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------
    def relay_connect(self, payload: dict[str, Any]) -> dict[str, Any]:
        health = dict(payload.get("health") or payload)
        name = str(health.get("name") or payload.get("name") or "").strip()
        if not name:
            raise HostError("Relay connection requires a host name")
        address = str(health.get("address") or f"{self.address.rstrip('/')}/relay/hosts/{quote(name, safe='')}").rstrip(
            "/"
        )
        health["name"] = name
        health["address"] = address
        health.setdefault("code_version", self.mesh.code_version)
        health.setdefault("active_count", 0)
        health.setdefault("inactive_count", 0)
        now = time.time()
        with self._lock:
            existing = self._relay_nodes.get(name)
            if existing is None:
                existing = RelayNode(name=name, health=health, last_seen=now, online=True)
                self._relay_nodes[name] = existing
            else:
                existing.health = health
                existing.last_seen = now
                existing.online = True
                existing.last_error = None
            self._relay_queues.setdefault(name, queue.Queue(maxsize=self.relay_queue_limit))
        self._register_relay_node_ref(existing)
        return {"hosts": [ref.to_wire() for ref in self.mesh.hosts(include_self=True)]}

    def relay_host_health(self, target: str) -> dict[str, Any]:
        name = unquote(target)
        if name == self.name:
            return self.health()
        with self._lock:
            node = self._relay_nodes.get(name)
            if node is not None:
                self._expire_relay_node_locked(node)
        if node is None or not node.online:
            raise RemoteHostError(self._relay_target_error(name, node))
        return dict(node.health)

    def relay_diagnostics(self) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            for node in self._relay_nodes.values():
                self._expire_relay_node_locked(node, now=now)
            nodes = [
                {
                    "name": node.name,
                    "online": node.online,
                    "last_seen": node.last_seen,
                    "last_seen_age": max(0.0, now - node.last_seen),
                    "active_polls": node.active_polls,
                    "queue_depth": self._relay_queues.get(node.name).qsize() if node.name in self._relay_queues else 0,
                    "in_flight": node.in_flight,
                    "last_error": node.last_error,
                }
                for node in self._relay_nodes.values()
            ]
        return {"nodes": sorted(nodes, key=lambda item: str(item["name"]))}

    def _relay_node_ref(self, node: RelayNode) -> HostRef:
        return HostRef(
            name=node.name,
            url=str(
                node.health.get("address") or f"{self.address.rstrip('/')}/relay/hosts/{quote(node.name, safe='')}"
            ).rstrip("/"),
            code_version=str(node.health.get("code_version") or self.mesh.code_version),
            online=node.online,
            last_seen=node.last_seen,
            active_count=int(node.health.get("active_count", 0)),
            inactive_count=int(node.health.get("inactive_count", 0)),
            tags=tuple(str(item).strip().casefold() for item in node.health.get("tags", []) if str(item).strip()),
            properties={
                str(key).strip(): str(value)
                for key, value in dict(node.health.get("properties") or {}).items()
                if str(key).strip()
            },
            error=node.last_error,
        )

    def _register_relay_node_ref(self, node: RelayNode) -> None:
        self.mesh.register(self._relay_node_ref(node))

    def _expire_relay_node_locked(self, node: RelayNode, *, now: float | None = None) -> None:
        current = time.time() if now is None else now
        age = current - node.last_seen
        if node.online and age > self.relay_offline_after:
            node.online = False
            node.last_error = f"Relay target {node.name!r} is offline/not polling (last seen {age:.3f}s ago)"
            self._register_relay_node_ref(node)
            self._emit(
                "relay-target-offline",
                data={"target": node.name, "stage": "relay-preflight", "last_seen_age": age},
                error=node.last_error,
            )

    def _relay_target_error(self, target: str, node: RelayNode | None) -> str:
        if node is None:
            return f"Relay target {target!r} is not connected"
        if node.last_error:
            return node.last_error
        age = time.time() - node.last_seen
        return f"Relay target {target!r} is offline/not polling (last seen {age:.3f}s ago)"

    def relay_poll(self, node_name: str, *, timeout: float = 25.0) -> dict[str, Any]:
        name = unquote(node_name)
        delivery: RelayDelivery | None = None
        with self._lock:
            node = self._relay_nodes.get(name)
            if node is not None:
                node.last_seen = time.time()
                node.online = True
                node.active_polls += 1
                node.last_poll_started = node.last_seen
                node.last_error = None
                self._register_relay_node_ref(node)
            delivery_queue = self._relay_queues.setdefault(name, queue.Queue(maxsize=self.relay_queue_limit))
        try:
            delivery = delivery_queue.get(timeout=max(0.0, timeout))
        except queue.Empty:
            return {"delivery": None}
        finally:
            with self._lock:
                node = self._relay_nodes.get(name)
                if node is not None:
                    node.active_polls = max(0, node.active_polls - 1)
                    node.last_seen = time.time()
                    node.last_poll_finished = node.last_seen
                    self._register_relay_node_ref(node)
        if delivery is None:
            return {"delivery": None}
        with self._lock:
            node = self._relay_nodes.get(name)
            if node is not None:
                node.in_flight += 1
        self._emit(
            "relay-delivery-dispatched",
            data={"delivery_id": delivery.delivery_id, "target": delivery.target, "kind": delivery.kind},
        )
        return {
            "delivery": {
                "delivery_id": delivery.delivery_id,
                "kind": delivery.kind,
                "payload": delivery.payload,
            }
        }

    def relay_ack(self, delivery_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            delivery = self._relay_pending.pop(delivery_id, None)
        if delivery is None:
            return {"ok": True, "duplicate": True}
        with self._lock:
            node = self._relay_nodes.get(delivery.target)
            if node is not None:
                node.in_flight = max(0, node.in_flight - 1)
        if payload.get("ok", True):
            delivery.future.set_result(dict(payload))
        else:
            delivery.future.set_exception(relay_error_from_ack(payload))
        self._emit(
            "relay-delivery-ack",
            data={
                "delivery_id": delivery.delivery_id,
                "target": delivery.target,
                "kind": delivery.kind,
                "ok": bool(payload.get("ok", True)),
                "error": str(payload.get("error") or ""),
            },
        )
        return {"ok": True}

    def relay_api(
        self,
        target: str,
        method: str,
        path: str,
        payload: dict[str, Any],
        *,
        timeout: float = 10.0,
    ) -> Any:
        response = self._relay_submit(
            unquote(target),
            "api",
            {"method": method, "path": path, "payload": dict(payload)},
            timeout=timeout,
        )
        return response.get("result")

    def relay_receive_envelope(
        self, target: str, envelope: PagletEnvelope, *, timeout: float | None = None
    ) -> dict[str, Any]:
        return self._relay_submit(
            unquote(target),
            "envelope",
            {"envelope": envelope.to_wire()},
            timeout=self.relay_delivery_timeout if timeout is None else timeout,
        )

    def relay_receive_creation(
        self, target: str, payload: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any]:
        return self._relay_submit(
            unquote(target),
            "creation",
            {"creation": dict(payload)},
            timeout=self.relay_delivery_timeout if timeout is None else timeout,
        )

    def relay_deliver_message(
        self,
        target: str,
        agent_id: str,
        message: Message,
        *,
        oneway: bool = False,
        activate_if_inactive: bool = True,
        no_delay: bool = False,
        timeout: float | None = None,
    ) -> Any:
        response = self._relay_submit(
            unquote(target),
            "message",
            {
                "agent_id": agent_id,
                "message": message.to_wire(),
                "oneway": oneway,
                "activate_if_inactive": activate_if_inactive,
                "no_delay": no_delay,
            },
            timeout=self.relay_delivery_timeout if timeout is None else timeout,
        )
        return response.get("result")

    def relay_receive_artifact_upload(
        self,
        target: str,
        headers: Any,
        source: Any,
        query: dict[str, list[str]],
    ) -> ArtifactRef:
        expires_at = time.time() + self.artifact_spool_ttl_seconds
        spool = self.artifacts.create_from_http_request(
            headers,
            source,
            owner_agent_id="",
            name=(query.get("name") or [""])[0],
            compression=(query.get("compression") or [""])[0],
            expires_at=expires_at,
            expected_sha256=(query.get("sha256") or [None])[0],
        )
        try:
            response = self._relay_submit(
                unquote(target),
                "artifact-import",
                {
                    "source_artifact": spool.ref.to_wire(),
                    "owner_agent_id": (query.get("owner_agent_id") or [""])[0],
                    "name": (query.get("name") or [""])[0],
                    "compression": (query.get("compression") or [""])[0],
                    "expires_at": float((query.get("expires_at") or ["0"])[0] or 0.0),
                },
                timeout=self.relay_delivery_timeout,
            )
            return ArtifactRef.from_wire(response["artifact"])
        finally:
            self.artifacts.delete(spool.ref.artifact_id)

    def relay_export_artifact(self, target: str, artifact_id: str) -> ArtifactRef:
        response = self._relay_submit(
            unquote(target),
            "artifact-export",
            {"artifact_id": artifact_id},
            timeout=self.relay_delivery_timeout,
        )
        return ArtifactRef.from_wire(response["artifact"])

    def _relay_submit(self, target: str, kind: str, payload: dict[str, Any], *, timeout: float) -> dict[str, Any]:
        with self._lock:
            node = self._relay_nodes.get(target)
            if node is not None:
                self._expire_relay_node_locked(node)
            delivery_queue = self._relay_queues.setdefault(target, queue.Queue(maxsize=self.relay_queue_limit))
            queue_depth = delivery_queue.qsize()
        if node is None or not node.online:
            message = self._relay_target_error(target, node)
            data = {"target": target, "kind": kind, "stage": "relay-preflight"}
            self._emit("relay-target-offline", data=data, error=message)
            self._emit("transfer-failed", data={"destination": target, **data}, error=message)
            raise RemoteHostError(message)
        if delivery_queue.full():
            message = f"Relay target {target!r} queue is full ({self.relay_queue_limit} deliveries)"
            data = {"target": target, "kind": kind, "stage": "relay-queue", "queue_depth": queue_depth}
            self._emit("relay-target-offline", data=data, error=message)
            self._emit("transfer-failed", data={"destination": target, **data}, error=message)
            raise RemoteHostError(message)
        delivery_id = uuid.uuid4().hex
        future: Future[dict[str, Any]] = Future()
        delivery = RelayDelivery(
            delivery_id=delivery_id,
            target=target,
            kind=kind,
            payload=payload,
            future=future,
        )
        with self._lock:
            self._relay_pending[delivery_id] = delivery
        try:
            delivery_queue.put_nowait(delivery)
        except queue.Full as exc:
            with self._lock:
                self._relay_pending.pop(delivery_id, None)
            message = f"Relay target {target!r} queue is full ({self.relay_queue_limit} deliveries)"
            self._emit(
                "transfer-failed",
                data={"destination": target, "stage": "relay-queue", "queue_depth": self.relay_queue_limit},
                error=message,
            )
            raise RemoteHostError(message) from exc
        self._emit(
            "relay-delivery-enqueued",
            data={
                "delivery_id": delivery_id,
                "target": target,
                "kind": kind,
                "stage": "relay-queue",
                "timeout": max(0.01, timeout),
                "queue_depth": queue_depth + 1,
            },
        )
        try:
            response = future.result(timeout=max(0.01, timeout))
        except FutureTimeoutError as exc:
            with self._lock:
                self._relay_pending.pop(delivery_id, None)
                node = self._relay_nodes.get(target)
                if node is not None:
                    node.in_flight = max(0, node.in_flight - 1)
                    node.last_error = f"delivery {delivery_id} timed out"
            message = f"Relay delivery to {target!r} timed out after {max(0.01, timeout):.3f}s"
            self._emit(
                "relay-delivery-timeout",
                data={
                    "delivery_id": delivery_id,
                    "target": target,
                    "kind": kind,
                    "stage": "relay-ack",
                    "timeout": max(0.01, timeout),
                },
                error=message,
            )
            self._emit(
                "transfer-failed",
                data={"destination": target, "stage": "relay-ack", "timeout": max(0.01, timeout)},
                error=message,
            )
            raise RemoteHostError(message) from exc
        if not response.get("ok", True):
            message = str(response.get("error") or "Relay delivery failed")
            self._emit(
                "transfer-failed",
                data={"destination": target, "stage": "relay-forward", "kind": kind, "delivery_id": delivery_id},
                error=message,
            )
            raise RemoteHostError(message)
        return response

    def _start_relay_client(self) -> None:
        if not self.connect_to:
            return
        self._relay_stop.clear()
        self._relay_client_thread = threading.Thread(
            target=self._relay_client_loop,
            name=f"paglets-relay-client-{self.name}",
            daemon=True,
        )
        self._relay_client_thread.start()

    def _stop_relay_client(self) -> None:
        self._relay_stop.set()
        thread = self._relay_client_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self._relay_client_thread = None

    def _relay_client_loop(self) -> None:
        assert self.connect_to is not None
        while not self._relay_stop.is_set():
            try:
                self._relay_register_once()
                response = self.client.get_json(
                    f"{self.connect_to.rstrip('/')}/relay/poll/{quote(self.name, safe='')}?timeout=25",
                    timeout=35.0,
                )
                delivery = response.get("delivery") if isinstance(response, dict) else None
                if isinstance(delivery, dict):
                    threading.Thread(
                        target=self._handle_relay_delivery,
                        args=(delivery,),
                        name=f"paglets-relay-delivery-{delivery.get('delivery_id', '')}",
                        daemon=True,
                    ).start()
            except Exception as exc:
                self._emit("relay-client-error", error=str(exc))
                self._relay_stop.wait(1.0)

    def _relay_register_once(self) -> None:
        assert self.connect_to is not None
        response = self.client.post_json(
            f"{self.connect_to.rstrip('/')}/relay/connect",
            {"health": self.health()},
            timeout=5.0,
        )
        hosts = response.get("hosts", []) if isinstance(response, dict) else []
        if isinstance(hosts, list):
            for item in hosts:
                if isinstance(item, dict):
                    try:
                        self.mesh.register_wire(item)
                    except (KeyError, TypeError, ValueError):
                        continue

    def _handle_relay_delivery(self, delivery: dict[str, Any]) -> None:
        assert self.connect_to is not None
        delivery_id = str(delivery["delivery_id"])
        payload = dict(delivery.get("payload") or {})
        try:
            kind = str(delivery["kind"])
            if kind == "envelope":
                proxy = self._receive_envelope(PagletEnvelope.from_wire(payload["envelope"]))
                ack = {"ok": True, "proxy": proxy.to_wire()}
            elif kind == "creation":
                proxy = self._receive_creation(dict(payload["creation"]))
                ack = {"ok": True, "proxy": proxy.to_wire()}
            elif kind == "api":
                result = self._relay_local_api(
                    str(payload["method"]),
                    str(payload["path"]),
                    dict(payload.get("payload") or {}),
                )
                ack = {"ok": True, "result": result}
            elif kind == "message":
                result = self.deliver_message(
                    str(payload["agent_id"]),
                    Message.from_wire(payload["message"]),
                    oneway=bool(payload.get("oneway", False)),
                    activate_if_inactive=bool(payload.get("activate_if_inactive", True)),
                    no_delay=bool(payload.get("no_delay", False)),
                )
                ack = {"ok": True, "result": result}
            elif kind == "artifact-import":
                ref = self._relay_import_artifact(dict(payload))
                ack = {"ok": True, "artifact": ref.to_wire()}
            elif kind == "artifact-export":
                ref = self._relay_upload_artifact_export(str(payload["artifact_id"]))
                ack = {"ok": True, "artifact": ref.to_wire()}
            else:
                raise HostError(f"Unknown relay delivery kind {kind!r}")
        except Exception as exc:
            ack = {"ok": False, "error_type": exc.__class__.__name__, "error": str(exc)}
        self.client.post_json(f"{self.connect_to.rstrip('/')}/relay/ack/{delivery_id}", ack, timeout=5.0)

    def _relay_import_artifact(self, payload: dict[str, Any]) -> ArtifactRef:
        source = ArtifactRef.from_wire(payload["source_artifact"])
        temp_path = self._artifact_root / "tmp" / f"{uuid.uuid4().hex}.part"
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.client.download_artifact(source, temp_path, timeout=self.relay_delivery_timeout)
            result = self.artifacts.create_from_path(
                temp_path,
                owner_agent_id=str(payload.get("owner_agent_id") or ""),
                name=str(payload.get("name") or source.name),
                compression=str(payload.get("compression") or source.compression),
                expires_at=float(payload.get("expires_at") or 0.0),
                expected_sha256=source.sha256,
            )
            return result.ref
        finally:
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()

    def _relay_upload_artifact_export(self, artifact_id: str) -> ArtifactRef:
        if not self.connect_to:
            raise HostError("artifact export through relay requires connect_to")
        ref = self.artifacts.ref(artifact_id)
        path = self.artifacts.blob_path(artifact_id)
        return self.client.upload_artifact(
            self.connect_to,
            path,
            owner_agent_id="",
            name=ref.name,
            compression=ref.compression,
            expires_at=time.time() + self.artifact_spool_ttl_seconds,
            expected_sha256=ref.sha256,
            timeout=self.relay_delivery_timeout,
        )

    def _relay_local_api(self, method: str, path: str, payload: dict[str, Any]) -> Any:
        parsed = urlparse(path)
        parts = [part for part in parsed.path.split("/") if part]
        query = parse_qs(parsed.query)

        if method == "GET" and parts == ["health"]:
            return self.health()
        if method == "GET" and parts == ["hosts"]:
            return {"hosts": [ref.to_wire() for ref in self.list_hosts(include_self=True)]}
        if method == "POST" and parts == ["hosts", "join"]:
            return {"hosts": [ref.to_wire() for ref in self.join_mesh(payload)]}
        if method == "GET" and parts == ["events"]:
            since = int((query.get("since") or ["0"])[0])
            limit = int((query.get("limit") or ["100"])[0])
            return {"events": [event.to_wire() for event in self.list_events(since=since, limit=limit)]}
        if method == "GET" and parts == ["artifacts"]:
            owner = (query.get("owner_agent_id") or [None])[0]
            return {"artifacts": [ref.to_wire() for ref in self.artifacts.list(owner_agent_id=owner)]}
        if method == "GET" and len(parts) == 3 and parts[0] == "artifacts" and parts[2] == "metadata":
            return {"artifact": self.artifacts.ref(parts[1]).to_wire()}
        if method == "DELETE" and len(parts) == 2 and parts[0] == "artifacts":
            self.artifacts.delete(parts[1])
            return {"ok": True}
        if method == "GET" and parts == ["services"]:
            name = (query.get("name") or [None])[0]
            capability = (query.get("capability") or [None])[0]
            scope = enum_from_wire((query.get("scope") or [ServiceScope.LOCAL.value])[0], ServiceScope, "scope")
            records = (
                self._services.lookup_all(name, capability, scope=ServiceScope.MESH)
                if scope is ServiceScope.MESH
                else self._services.lookup_all(name, capability)
            )
            return {"services": [record.to_wire() for record in records]}
        if method == "POST" and parts == ["services", "leases"]:
            return self.acquire_resident_service_lease(
                str(payload["agent_id"]),
                str(payload["service_name"]),
                ttl=float(payload.get("ttl", DEFAULT_SERVICE_LEASE_TTL_SECONDS)),
            )
        if method == "POST" and len(parts) == 4 and parts[:2] == ["services", "leases"] and parts[3] == "release":
            return self.release_resident_service_lease(parts[2])
        if method == "GET" and parts == ["agents"]:
            state = (query.get("state") or ["active"])[0]
            include_state = (query.get("include_state") or [""])[0].lower() in {"1", "true", "yes"}
            if state == "all":
                return {"agents": self.list_agents(active=True, inactive=True, include_state=include_state)}
            if state == "inactive":
                return {"agents": self.list_agents(active=False, inactive=True, include_state=include_state)}
            return {"agents": self.list_agents(active=True, inactive=False, include_state=include_state)}
        if method == "POST" and parts == ["agents"]:
            if "envelope" in payload:
                proxy = self._receive_envelope(PagletEnvelope.from_wire(payload["envelope"]))
            else:
                proxy = self._receive_creation(payload)
            return {"proxy": proxy.to_wire()}

        if len(parts) >= 2 and parts[0] == "agents":
            agent_id = parts[1]
            if method == "GET" and len(parts) == 3 and parts[2] == "state":
                return self._state_payload(agent_id)
            if method == "GET" and len(parts) == 2:
                with self._lock:
                    agent = self._agents.get(agent_id)
                    if agent is not None:
                        return self._summary(agent)
                    inactive = self._inactive.get(agent_id)
                    if inactive is not None:
                        return self._inactive_summary(inactive)
                raise InvalidAgentError(f"No paglet {agent_id!r} on {self.name}")
            if method == "POST" and len(parts) == 3:
                action = parts[2]
                if action == "messages":
                    message = Message.from_wire(payload["message"])
                    result = self.deliver_message(
                        agent_id,
                        message,
                        oneway=bool(payload.get("oneway", False)),
                        activate_if_inactive=bool(payload.get("activate_if_inactive", True)),
                        no_delay=bool(payload.get("no_delay", False)),
                    )
                    return {"result": result}
                if action == "dispatch":
                    target = TransferTicket.from_wire(payload["ticket"]) if "ticket" in payload else payload["target"]
                    proxy = self.dispatch(agent_id, target)
                    return {"proxy": proxy.to_wire()}
                if action == "clone":
                    if "ticket" in payload:
                        proxy = self.clone(agent_id, target=TransferTicket.from_wire(payload["ticket"]))
                    else:
                        proxy = self.clone(agent_id, target=payload.get("target"))
                    return {"proxy": proxy.to_wire()}
                if action == "retract":
                    proxy = self._retract_to(agent_id, payload["target"])
                    return {"proxy": proxy.to_wire()}
                if action == "deactivate":
                    proxy = self.deactivate(agent_id, DeactivationRequest.from_wire(payload.get("request")))
                    return {"proxy": proxy.to_wire(), "ok": True}
                if action == "activate":
                    proxy = self.activate(agent_id)
                    return {"proxy": proxy.to_wire()}
                if action == "dispose":
                    self.dispose(agent_id)
                    return {"ok": True}
                if action == "services":
                    record = self.advertise_service(
                        agent_id,
                        str(payload["name"]),
                        capabilities=payload.get("capabilities"),
                        metadata=payload.get("metadata"),
                        scope=enum_from_wire(
                            payload.get("scope") or ServiceScope.LOCAL.value,
                            ServiceScope,
                            "scope",
                        ),
                        ttl=float(payload["ttl"]) if payload.get("ttl") is not None else None,
                    )
                    return {"service": record.to_wire()}
                if action == "unadvertise-service":
                    removed = self.unadvertise_service(str(payload["name"]), agent_id=agent_id)
                    return {"services": [record.to_wire() for record in removed]}

        raise HostError(f"No relay route for {method} {path}")


def _is_relay_transport_url(url: str) -> bool:
    return "/relay/hosts/" in urlparse(url).path


def relay_error_from_ack(payload: dict[str, Any]) -> PagletError:
    message = str(payload.get("error") or "Relay delivery failed")
    error_type = str(payload.get("error_type") or "RemoteHostError")
    if error_type == "TransferError":
        return TransferError(message)
    if error_type == "InvalidAgentError":
        return InvalidAgentError(message)
    if error_type == "PagletInactiveError":
        return PagletInactiveError(message)
    if error_type == "PagletCrashedError":
        return PagletCrashedError(message)
    if error_type == "NotHandledError":
        return NotHandledError(message)
    if error_type == "ForbiddenError":
        return ForbiddenError(message)
    if error_type == "AuthenticationError":
        return AuthenticationError(message)
    if error_type == "ServiceNotFoundError":
        return ServiceNotFoundError(message)
    if error_type == "HostError":
        return HostError(message)
    return RemoteHostError(message)
