# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import queue
import threading
import time
import uuid
from dataclasses import is_dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from paglets.artifacts import (
    ARTIFACT_MOVE,
    ARTIFACT_STATUS_AVAILABLE,
    ArtifactRef,
    PagletFileRef,
    file_sha256,
    safe_target_filename,
)
from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import (
    HostError,
    InvalidAgentError,
    RemoteHostError,
    TransferError,
)
from paglets.core.events import CloneEvent, CreationEvent, MobilityEvent, PersistencyEvent
from paglets.core.runtime_values import (
    ArrivalMode,
    EnvelopeKind,
    enum_from_wire,
)
from paglets.persistence.persistency import DeactivationPolicy, DeactivationRequest, InactiveRecord
from paglets.remote.proxy import PagletProxy
from paglets.remote.transfer import TransferTicket
from paglets.runtime.envelope import PagletEnvelope
from paglets.runtime.process_runtime import ChildProcessController
from paglets.runtime.relay import _is_relay_transport_url
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire, qualified_name, resolve_qualified_name

SHUTDOWN_DEACTIVATE_TIMEOUT_SECONDS = 0.5
MESH_SERVICE_LOOKUP_TIMEOUT_SECONDS = 1.0


class _LifecycleMixin:
    def create(
        self,
        agent_cls: type[Paglet],
        state: PagletState | None = None,
        *,
        init: Any = None,
        agent_id: str | None = None,
    ) -> PagletProxy:
        state_cls = agent_cls.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        if not is_dataclass(state):
            raise HostError(f"{agent_cls.__name__}.State must be a dataclass state object")
        child_id = agent_id or uuid.uuid4().hex
        agent_class_name = qualified_name(agent_cls)
        state_class_name = qualified_name(state_cls)
        record = self._start_child(
            agent_id=child_id,
            agent_class_name=agent_class_name,
            state_class_name=state_class_name,
            state=dataclass_to_wire(state),
        )
        event = CreationEvent(
            agent_id=record.agent_id,
            host_name=self.name,
            host_address=self.address,
            init=init,
        )
        try:
            record.request_lifecycle("creation", dataclass_to_wire(event))
        except Exception:
            self._remove_active_agent(record.agent_id, record, terminate=True)
            raise
        record.ready = True
        self._emit("create", agent_id=record.agent_id, class_name=record.agent_class_name)
        return PagletProxy(self.address, record.agent_id, self.client)

    def create_remote(
        self,
        target: str,
        agent_cls: type[Paglet],
        state: PagletState | None = None,
        *,
        init: Any = None,
        agent_id: str | None = None,
    ) -> PagletProxy:
        state_cls = agent_cls.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        response = self.client.post_pickle(
            f"{target.rstrip('/')}/agents",
            {
                "agent_class_name": qualified_name(agent_cls),
                "state_class_name": qualified_name(state_cls),
                "state": dataclass_to_wire(state),
                "init": init,
                "agent_id": agent_id,
            },
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def dispatch(self, agent_id: str, target: str | TransferTicket) -> PagletProxy:
        ticket = self._prepare_ticket(target)
        record = self._require_agent(agent_id)
        target_info = self._preflight_transfer(ticket)
        event = MobilityEvent(
            agent_id=agent_id,
            host_name=self.name,
            host_address=self.address,
            source_host_name=self.name,
            source_host_address=self.address,
            target_host_name=target_info["name"],
            target_host_address=target_info["address"],
            reason="dispatch",
        )
        record.request_lifecycle("dispatching", dataclass_to_wire(event))
        record.cleanup_resources(reason="dispatch")
        envelope = self._make_envelope(record, EnvelopeKind.DISPATCH, target_info, ticket=ticket)
        staged = self._stage_registered_files(
            agent_id,
            target_info,
            target_agent_id=agent_id,
            kind=EnvelopeKind.DISPATCH,
        )
        if staged:
            envelope.metadata["registered_files"] = staged
        try:
            response = self._post_envelope_with_ticket(ticket, target_info, envelope)
        except Exception:
            self._cleanup_staged_registered_artifacts(staged)
            raise
        self._finalize_registered_file_departure(agent_id, staged, delete_moves=True)
        self._cleanup_agent_work_dir(agent_id)
        self._remove_active_agent(agent_id, record, terminate=True)
        with self._lock:
            self._registered_files.pop(agent_id, None)
        self._emit("dispatch", agent_id=agent_id, class_name=record.agent_class_name, data={"target": target_info})
        return PagletProxy.from_wire(response["proxy"], self.client)

    def clone(self, agent_id: str, *, target: str | TransferTicket | None = None) -> PagletProxy:
        record = self._require_agent(agent_id)
        ticket = self._prepare_ticket(target or self.address)
        target_info = self._preflight_transfer(ticket)
        clone_id = uuid.uuid4().hex
        cloning_event = CloneEvent(
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
        record.request_lifecycle("cloning", dataclass_to_wire(cloning_event))
        envelope = self._make_envelope(
            record,
            EnvelopeKind.CLONE,
            target_info,
            agent_id=clone_id,
            clone_of=agent_id,
            ticket=ticket,
        )
        staged = self._stage_registered_files(agent_id, target_info, target_agent_id=clone_id, kind=EnvelopeKind.CLONE)
        if staged:
            envelope.metadata["registered_files"] = staged
        try:
            response = self._post_envelope_with_ticket(ticket, target_info, envelope)
        except Exception:
            self._cleanup_staged_registered_artifacts(staged)
            raise
        cloned_event = CloneEvent(
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
        record.request_lifecycle("cloned", dataclass_to_wire(cloned_event))
        self._emit(
            "clone",
            agent_id=agent_id,
            class_name=record.agent_class_name,
            data={"clone_agent_id": clone_id, "target": target_info},
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def retract(self, remote_host_url: str, agent_id: str) -> PagletProxy:
        response = self.client.post_json(
            f"{remote_host_url.rstrip('/')}/agents/{agent_id}/retract",
            {"target": self.address},
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def deactivate(
        self,
        agent_id: str,
        request: DeactivationRequest | None = None,
    ) -> PagletProxy:
        record = self._require_agent(agent_id)
        request = request or DeactivationRequest()
        prepared = record.request("deactivate_prepare", {"request": request.to_wire()})
        record._update_from_reply(prepared)
        policy = DeactivationPolicy.from_wire(prepared.get("policy"))
        info = {"name": self.name, "address": self.address}
        envelope = self._make_envelope(record, EnvelopeKind.ACTIVATION, info)
        record = InactiveRecord(envelope=envelope, policy=policy, request=request)
        self._write_inactive_record(record)
        with self._lock:
            self._inactive[agent_id] = record
        self._remove_active_agent(agent_id, expected=None, terminate=True)
        self._emit(
            "deactivate", agent_id=agent_id, class_name=envelope.agent_class_name, data={"reason": request.reason}
        )
        return PagletProxy(self.address, agent_id, self.client)

    def activate(self, agent_id: str) -> PagletProxy:
        with self._lock:
            record = self._inactive.pop(agent_id, None)
        if record is None:
            raise InvalidAgentError(f"No deactivated paglet {agent_id!r} on {self.name}")
        self._delete_inactive_record(agent_id)
        try:
            proxy = self._receive_envelope(record.envelope, inactive_record=record)
        except Exception:
            self._remove_active_agent(agent_id)
            self._write_inactive_record(record)
            with self._lock:
                self._inactive[agent_id] = record
            raise
        self._drain_queued_messages(record)
        self._emit("activate", agent_id=agent_id, data={"queued_message_count": len(record.queued_messages)})
        return proxy

    def dispose(self, agent_id: str) -> None:
        with self._lock:
            record = self._agents.get(agent_id)
            inactive = self._inactive.pop(agent_id, None)
        if record is None:
            if inactive is None:
                raise InvalidAgentError(f"No paglet {agent_id!r} on {self.name}")
            self._delete_inactive_record(agent_id)
            self._cleanup_agent_work_dir(agent_id)
            with self._lock:
                self._registered_files.pop(agent_id, None)
            self._emit(
                "dispose", agent_id=agent_id, class_name=inactive.envelope.agent_class_name, data={"active": False}
            )
            return
        record.request("dispose_prepare", {"reason": "dispose"})
        self._cleanup_agent_work_dir(agent_id)
        with self._lock:
            self._registered_files.pop(agent_id, None)
        self._remove_active_agent(agent_id, record, terminate=True)
        if inactive is not None:
            self._delete_inactive_record(agent_id)
        self._emit("dispose", agent_id=agent_id, class_name=record.agent_class_name, data={"active": True})

    # ------------------------------------------------------------------
    # Message/lifecycle internals
    # ------------------------------------------------------------------

    def _retract_to(self, agent_id: str, target: str) -> PagletProxy:
        record = self._require_agent(agent_id)
        target_info = self._host_info(target)
        event = MobilityEvent(
            agent_id=agent_id,
            host_name=self.name,
            host_address=self.address,
            source_host_name=self.name,
            source_host_address=self.address,
            target_host_name=target_info["name"],
            target_host_address=target_info["address"],
            reason="retract",
        )
        record.request_lifecycle("reverting", dataclass_to_wire(event))
        record.cleanup_resources(reason="retract")
        envelope = self._make_envelope(record, EnvelopeKind.RETRACT, target_info)
        staged = self._stage_registered_files(
            agent_id,
            target_info,
            target_agent_id=agent_id,
            kind=EnvelopeKind.RETRACT,
        )
        if staged:
            envelope.metadata["registered_files"] = staged
        try:
            if self._is_local_transfer_target(target_info):
                response = self._receive_local_envelope_response(envelope)
            else:
                response = self.client.post_pickle(
                    f"{target_info['address'].rstrip('/')}/agents", {"envelope": envelope.to_wire()}
                )
        except Exception:
            self._cleanup_staged_registered_artifacts(staged)
            raise
        self._finalize_registered_file_departure(agent_id, staged, delete_moves=True)
        self._cleanup_agent_work_dir(agent_id)
        self._remove_active_agent(agent_id, record, terminate=True)
        with self._lock:
            self._registered_files.pop(agent_id, None)
        self._emit("retract", agent_id=agent_id, class_name=record.agent_class_name, data={"target": target_info})
        return PagletProxy.from_wire(response["proxy"], self.client)

    def _receive_envelope(
        self,
        envelope: PagletEnvelope,
        *,
        inactive_record: InactiveRecord | None = None,
    ) -> PagletProxy:
        self._import_or_restore_registered_files(envelope)
        if inactive_record is None and self._arrival_mode(envelope) is ArrivalMode.INACTIVE:
            record = self._inactive_arrival_record(envelope)
            self._write_inactive_record(record)
            with self._lock:
                self._inactive[record.agent_id] = record
            self._emit(
                "arrival",
                agent_id=record.agent_id,
                class_name=record.envelope.agent_class_name,
                data={"active": False, "kind": envelope.kind.value},
            )
            return PagletProxy(self.address, record.agent_id, self.client)

        self._validate_agent_classes(envelope.agent_class_name, envelope.state_class_name)
        record = self._start_child(
            agent_id=envelope.agent_id,
            agent_class_name=envelope.agent_class_name,
            state_class_name=envelope.state_class_name,
            state=envelope.state,
        )

        if envelope.kind in (EnvelopeKind.DISPATCH, EnvelopeKind.RETRACT):
            event = MobilityEvent(
                agent_id=record.agent_id,
                host_name=self.name,
                host_address=self.address,
                source_host_name=envelope.source_host_name,
                source_host_address=envelope.source_host_address,
                target_host_name=self.name,
                target_host_address=self.address,
                reason=envelope.kind.value,
            )
            record.request_lifecycle("arrival", dataclass_to_wire(event))
            record.ready = True
            self._emit(
                "arrival",
                agent_id=record.agent_id,
                class_name=record.agent_class_name,
                data={"kind": envelope.kind.value},
            )
            record.wait_for_run_complete_or_departure()
        elif envelope.kind is EnvelopeKind.CLONE:
            event = CloneEvent(
                agent_id=record.agent_id,
                host_name=self.name,
                host_address=self.address,
                source_agent_id=envelope.clone_of or "",
                clone_agent_id=record.agent_id,
                source_host_name=envelope.source_host_name,
                source_host_address=envelope.source_host_address,
                target_host_name=self.name,
                target_host_address=self.address,
            )
            record.request_lifecycle("clone", dataclass_to_wire(event))
            record.ready = True
            self._emit(
                "clone",
                agent_id=record.agent_id,
                class_name=record.agent_class_name,
                data={"source_agent_id": envelope.clone_of},
            )
        elif envelope.kind is EnvelopeKind.ACTIVATION:
            event = PersistencyEvent(
                agent_id=record.agent_id,
                host_name=self.name,
                host_address=self.address,
                reason="activate",
                request=inactive_record.request if inactive_record is not None else None,
                policy=inactive_record.policy if inactive_record is not None else None,
            )
            payload = dataclass_to_wire(event)
            if inactive_record is not None:
                payload["request"] = inactive_record.request.to_wire()
                payload["policy"] = inactive_record.policy.to_wire()
            record.request_lifecycle("activation", payload)
            record.ready = True
        else:  # pragma: no cover - typing already limits this
            raise HostError(f"Unknown envelope kind {envelope.kind!r}")

        return self._current_or_last_proxy(record)

    def _receive_creation(self, payload: dict[str, Any]) -> PagletProxy:
        agent_cls = resolve_qualified_name(payload["agent_class_name"])
        state_cls = resolve_qualified_name(payload["state_class_name"])
        if not issubclass(agent_cls, Paglet):
            raise HostError(f"{payload['agent_class_name']} is not a Paglet subclass")
        if not is_dataclass(state_cls):
            raise HostError(f"{payload['state_class_name']} is not a dataclass state")
        state = dataclass_from_wire(state_cls, payload.get("state") or {})
        return self.create(agent_cls, state, init=payload.get("init"), agent_id=payload.get("agent_id"))

    def _prepare_ticket(self, target: str | TransferTicket) -> TransferTicket:
        ticket = TransferTicket.from_target(target)
        return replace(ticket, destination=self.mesh.resolve_url(ticket.destination).rstrip("/"))

    def _preflight_transfer(self, ticket: TransferTicket) -> dict[str, Any]:
        url = ticket.destination.rstrip("/")
        try:
            info = (
                self.health()
                if url == self.address.rstrip("/")
                else self.client.get_json(
                    f"{url}/health",
                    timeout=ticket.timeout,
                )
            )
        except Exception as exc:
            self._emit("transfer-failed", data={"destination": url, "stage": "preflight"}, error=str(exc))
            raise TransferError(f"Could not preflight transfer target {url}: {exc}") from exc
        code_version = str(info.get("code_version") or "")
        if ticket.expected_code_version is not None and code_version != ticket.expected_code_version:
            message = (
                f"Transfer target {url} has code version {code_version!r}, expected {ticket.expected_code_version!r}"
            )
            self._emit("transfer-failed", data={"destination": url, "stage": "preflight"}, error=message)
            raise TransferError(message)
        capabilities = {str(item) for item in info.get("capabilities", [])}
        missing = [capability for capability in ticket.required_capabilities if capability not in capabilities]
        if missing:
            message = f"Transfer target {url} is missing capabilities: {', '.join(missing)}"
            self._emit("transfer-failed", data={"destination": url, "stage": "preflight"}, error=message)
            raise TransferError(message)
        return {
            "name": str(info.get("name") or urlparse(url).netloc or url),
            "address": str(info.get("address") or url).rstrip("/"),
            "code_version": code_version,
            "capabilities": sorted(capabilities),
        }

    def _post_envelope_with_ticket(
        self,
        ticket: TransferTicket,
        target_info: dict[str, Any],
        envelope: PagletEnvelope,
    ) -> dict[str, Any]:
        if self._is_local_transfer_target(target_info):
            return self._receive_local_envelope_response(envelope)
        url = f"{str(target_info['address']).rstrip('/')}/agents"
        attempts = max(0, ticket.retries) + 1
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                http_timeout = ticket.timeout + 1.0 if _is_relay_transport_url(url) else ticket.timeout
                return self.client.post_pickle(url, {"envelope": envelope.to_wire()}, timeout=http_timeout)
            except Exception as exc:
                last_error = exc
                stage = "relay-forward" if _is_relay_transport_url(url) else "post"
                self._emit(
                    "transfer-failed",
                    agent_id=envelope.agent_id,
                    class_name=envelope.agent_class_name,
                    data={
                        "destination": target_info["address"],
                        "stage": stage,
                        "attempt": attempt + 1,
                        "attempts": attempts,
                    },
                    error=str(exc),
                )
                if attempt + 1 < attempts:
                    time.sleep(max(0.0, ticket.retry_interval))
        target_name = str(target_info.get("name") or target_info["address"])
        if _is_relay_transport_url(str(target_info["address"])):
            raise TransferError(
                f"Transfer to {target_name!r} through relay failed after {attempts} attempt(s): {last_error}"
            )
        raise TransferError(f"Transfer to {target_info['address']} failed after {attempts} attempt(s): {last_error}")

    def _validate_agent_classes(self, agent_class_name: str, state_class_name: str) -> None:
        agent_cls = resolve_qualified_name(agent_class_name)
        state_cls = resolve_qualified_name(state_class_name)
        if not issubclass(agent_cls, Paglet):
            raise HostError(f"{agent_class_name} is not a Paglet subclass")
        if not is_dataclass(state_cls):
            raise HostError(f"{state_class_name} is not a dataclass state")

    def _arrival_mode(self, envelope: PagletEnvelope) -> ArrivalMode:
        ticket = envelope.metadata.get("transfer_ticket")
        if isinstance(ticket, dict):
            return enum_from_wire(
                ticket.get("arrival_mode") or ArrivalMode.ACTIVATE.value,
                ArrivalMode,
                "arrival_mode",
            )
        return ArrivalMode.ACTIVATE

    def _inactive_arrival_record(self, envelope: PagletEnvelope) -> InactiveRecord:
        activation_envelope = PagletEnvelope(
            kind=EnvelopeKind.ACTIVATION,
            agent_id=envelope.agent_id,
            agent_class_name=envelope.agent_class_name,
            state_class_name=envelope.state_class_name,
            state=dict(envelope.state),
            source_host_name=envelope.source_host_name,
            source_host_address=envelope.source_host_address,
            target_host_name=self.name,
            target_host_address=self.address,
            clone_of=envelope.clone_of,
            metadata=dict(envelope.metadata),
        )
        return InactiveRecord(
            envelope=activation_envelope,
            policy=DeactivationPolicy(),
            request=DeactivationRequest(
                reason=f"{envelope.kind.value}-arrival",
                source="transfer",
                metadata={"arrival_mode": ArrivalMode.INACTIVE.value},
            ),
        )

    def _current_or_last_proxy(self, record: ChildProcessController) -> PagletProxy:
        proxy = self.get_proxy(record.agent_id)
        if proxy is not None:
            return proxy
        terminal_proxy = record.terminal_proxy_wire()
        if terminal_proxy is not None:
            return PagletProxy.from_wire(terminal_proxy, self.client)
        raise InvalidAgentError(f"Paglet {record.agent_id!r} moved or disappeared without a proxy")

    def _make_envelope(
        self,
        record: ChildProcessController,
        kind: EnvelopeKind,
        target_info: dict[str, Any],
        *,
        agent_id: str | None = None,
        clone_of: str | None = None,
        ticket: TransferTicket | None = None,
    ) -> PagletEnvelope:
        metadata: dict[str, Any] = {}
        if ticket is not None:
            metadata["transfer_ticket"] = ticket.to_wire()
        registered = self._registered_file_metadata(record.agent_id)
        if registered:
            metadata["registered_files"] = registered
        return PagletEnvelope(
            kind=kind,
            agent_id=agent_id or record.agent_id,
            agent_class_name=record.agent_class_name,
            state_class_name=record.state_class_name,
            state=dict(record.state),
            source_host_name=self.name,
            source_host_address=self.address,
            target_host_name=target_info["name"],
            target_host_address=target_info["address"],
            clone_of=clone_of,
            metadata=metadata,
        )

    def _host_info(self, target: str) -> dict[str, str]:
        url = target.rstrip("/")
        try:
            info = self.client.get_json(f"{url}/health")
            return {"name": str(info["name"]), "address": str(info["address"]).rstrip("/")}
        except RemoteHostError:
            # Keep the runtime usable in tests/dev even if the target does not
            # expose /health yet; the actual POST will still fail if unreachable.
            parsed = urlparse(url)
            return {"name": parsed.netloc or url, "address": url}

    def _is_local_transfer_target(self, target_info: dict[str, Any]) -> bool:
        return str(target_info.get("address") or "").rstrip("/") == self.address.rstrip("/")

    def _receive_local_envelope_response(self, envelope: PagletEnvelope) -> dict[str, Any]:
        results: queue.Queue[dict[str, Any] | BaseException] = queue.Queue(maxsize=1)

        def receive() -> None:
            try:
                results.put({"proxy": self._receive_envelope(envelope).to_wire()})
            except BaseException as exc:
                results.put(exc)

        thread = threading.Thread(
            target=receive,
            name=f"paglets-local-receive-{envelope.agent_id[:8]}",
            daemon=True,
        )
        thread.start()
        result = results.get()
        thread.join(timeout=0.1)
        if isinstance(result, BaseException):
            raise result
        return result

    def _registered_file_metadata(self, agent_id: str) -> list[dict[str, Any]]:
        with self._lock:
            refs = list(self._registered_files.get(agent_id, {}).values())
        return [ref.to_wire() for ref in refs]

    def _stage_registered_files(
        self,
        agent_id: str,
        target_info: dict[str, Any],
        *,
        target_agent_id: str,
        kind: EnvelopeKind,
    ) -> list[dict[str, Any]]:
        with self._lock:
            refs = [PagletFileRef.from_wire(ref.to_wire()) for ref in self._registered_files.get(agent_id, {}).values()]
        staged: list[dict[str, Any]] = []
        try:
            for ref in refs:
                path = Path(ref.current_path or ref.source_path)
                if not path.is_file():
                    raise TransferError(f"registered file {ref.name!r} is missing: {path}")
                stat = path.stat()
                ref.size_bytes = stat.st_size
                ref.sha256 = file_sha256(path)
                ref.current_host_name = self.name
                ref.current_host_url = self.address
                ref.current_path = str(path)
                transfer_mode = "copy" if kind is EnvelopeKind.CLONE else ref.mode
                if self._is_local_transfer_target(target_info):
                    artifact = self.artifacts.create_from_path(
                        path,
                        owner_agent_id=target_agent_id,
                        name=ref.name,
                        expected_sha256=ref.sha256,
                    ).ref
                else:
                    artifact = self.client.upload_artifact(
                        str(target_info["address"]),
                        path,
                        owner_agent_id=target_agent_id,
                        name=ref.name,
                        expected_sha256=ref.sha256,
                    )
                staged.append(
                    {
                        "file": ref.to_wire(),
                        "artifact": artifact.to_wire(),
                        "transfer_mode": transfer_mode,
                        "source_path": str(path),
                    }
                )
        except Exception:
            self._cleanup_staged_registered_artifacts(staged)
            raise
        return staged

    def _cleanup_staged_registered_artifacts(self, staged: list[dict[str, Any]]) -> None:
        for item in staged:
            artifact_payload = item.get("artifact")
            if not isinstance(artifact_payload, dict):
                continue
            with contextlib.suppress(Exception):
                artifact = ArtifactRef.from_wire(artifact_payload)
                self._delete_artifact_ref(artifact)

    def _finalize_registered_file_departure(
        self,
        agent_id: str,
        staged: list[dict[str, Any]],
        *,
        delete_moves: bool,
    ) -> None:
        if delete_moves:
            for item in staged:
                if str(item.get("transfer_mode") or "") != ARTIFACT_MOVE:
                    continue
                source_path = str(item.get("source_path") or "")
                if source_path:
                    with contextlib.suppress(FileNotFoundError):
                        Path(source_path).unlink()
        with self._lock:
            self._registered_files.pop(agent_id, None)

    def _import_or_restore_registered_files(self, envelope: PagletEnvelope) -> None:
        raw_items = envelope.metadata.get("registered_files")
        if not isinstance(raw_items, list):
            return
        if not raw_items:
            with self._lock:
                self._registered_files.pop(envelope.agent_id, None)
            return
        if not any(isinstance(item, dict) and isinstance(item.get("artifact"), dict) for item in raw_items):
            refs = [PagletFileRef.from_wire(dict(item)) for item in raw_items if isinstance(item, dict)]
            with self._lock:
                self._registered_files[envelope.agent_id] = {ref.name: ref for ref in refs}
            return
        imported: list[PagletFileRef] = []
        imported_paths: list[Path] = []
        try:
            for item in raw_items:
                if not isinstance(item, dict):
                    continue
                file_payload = item.get("file") if isinstance(item.get("file"), dict) else item
                artifact_payload = item.get("artifact")
                if not isinstance(file_payload, dict) or not isinstance(artifact_payload, dict):
                    continue
                ref = PagletFileRef.from_wire(file_payload)
                artifact = ArtifactRef.from_wire(artifact_payload)
                work_dir = self._work_path(envelope.agent_id)
                work_dir.mkdir(parents=True, exist_ok=True)
                target_path = work_dir / safe_target_filename(ref.name)
                self._materialize_artifact_ref(artifact, target_path)
                self._delete_artifact_ref(artifact)
                ref.current_host_name = self.name
                ref.current_host_url = self.address
                ref.current_path = str(target_path)
                ref.status = ARTIFACT_STATUS_AVAILABLE
                ref.last_error = ""
                imported.append(ref)
                imported_paths.append(target_path)
        except Exception as exc:
            for path in imported_paths:
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
            self._cleanup_agent_work_dir(envelope.agent_id)
            for item in raw_items:
                if isinstance(item, dict) and isinstance(item.get("artifact"), dict):
                    with contextlib.suppress(Exception):
                        self._delete_artifact_ref(ArtifactRef.from_wire(item["artifact"]))
            raise TransferError(f"Could not import registered files for paglet {envelope.agent_id!r}: {exc}") from exc
        with self._lock:
            self._registered_files[envelope.agent_id] = {ref.name: ref for ref in imported}
        envelope.metadata["registered_files"] = [ref.to_wire() for ref in imported]

    def _materialize_artifact_ref(self, artifact: ArtifactRef, target_path: Path) -> None:
        if artifact.host_url.rstrip("/") == self.address.rstrip("/"):
            self.artifacts.export_to_path(artifact.artifact_id, target_path, expected_sha256=artifact.sha256)
            return
        self.client.download_artifact(artifact, target_path)

    def _delete_artifact_ref(self, artifact: ArtifactRef) -> None:
        if artifact.host_url.rstrip("/") == self.address.rstrip("/"):
            self.artifacts.delete(artifact.artifact_id)
            return
        self.client.delete_artifact(artifact)
