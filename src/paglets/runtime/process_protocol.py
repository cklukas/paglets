# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import Any

from paglets.core.agent import Paglet
from paglets.core.errors import (
    AuthenticationError,
    ForbiddenError,
    HostError,
    InvalidAgentError,
    LifecycleError,
    NotHandledError,
    PagletCrashedError,
    PagletInactiveError,
    RemoteHostError,
    ServiceContractError,
    ServiceNotFoundError,
    TransferError,
)
from paglets.core.events import CloneEvent, MobilityEvent
from paglets.core.wire import WirePayload
from paglets.persistence.storage import StorageQuotaError
from paglets.remote.transfer import TransferTicket
from paglets.remote.transport import (
    receive_local_pickle,
    release_local_pickle_sender,
    start_local_pickle_sender,
)
from paglets.serialization.codec import dataclass_to_wire

_ERROR_TYPES: dict[str, type[Exception]] = {
    "AuthenticationError": AuthenticationError,
    "ForbiddenError": ForbiddenError,
    "HostError": HostError,
    "InvalidAgentError": InvalidAgentError,
    "LifecycleError": LifecycleError,
    "NotHandledError": NotHandledError,
    "PagletCrashedError": PagletCrashedError,
    "PagletInactiveError": PagletInactiveError,
    "RemoteHostError": RemoteHostError,
    "ResourceCleanupError": LifecycleError,
    "ServiceContractError": ServiceContractError,
    "ServiceNotFoundError": ServiceNotFoundError,
    "StorageQuotaError": StorageQuotaError,
    "TransferError": TransferError,
    "ValueError": ValueError,
}


@dataclass(frozen=True, slots=True)
class ChildConfig:
    host_name: str
    host_address: str
    agent_id: str
    agent_class_name: str
    state_class_name: str
    process_title: str
    state: WirePayload | None = None
    state_stream: WirePayload | None = None
    host_api_key: str | None = None


def _stream_state_payload(payload: Any) -> Any:
    if not isinstance(payload, dict) or "state" not in payload:
        return payload
    streamed = dict(payload)
    streamed["state_stream"] = start_local_pickle_sender(streamed.pop("state"))
    return streamed


def _materialize_state_stream(payload: Any) -> Any:
    if not isinstance(payload, dict) or "state_stream" not in payload:
        return payload
    materialized = dict(payload)
    materialized["state"] = receive_local_pickle(dict(materialized.pop("state_stream") or {}))
    return materialized


def _state_stream_token(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    stream = payload.get("state_stream")
    if not isinstance(stream, dict):
        return ""
    return str(stream.get("token") or "")


def _handle_local_pickle_stream_event(message: dict[str, Any]) -> None:
    if message.get("event") != "local_pickle_stream_received":
        return
    with contextlib.suppress(Exception):
        release_local_pickle_sender(str(message.get("token") or ""))


def _mobility_event(agent_id: str, payload: dict[str, Any]) -> MobilityEvent:
    return MobilityEvent(
        agent_id=agent_id,
        host_name=str(payload["host_name"]),
        host_address=str(payload["host_address"]),
        source_host_name=str(payload.get("source_host_name") or ""),
        source_host_address=str(payload.get("source_host_address") or ""),
        target_host_name=str(payload.get("target_host_name") or ""),
        target_host_address=str(payload.get("target_host_address") or ""),
        reason=str(payload.get("reason") or "dispatch"),
    )


def _clone_event(agent_id: str, payload: dict[str, Any]) -> CloneEvent:
    return CloneEvent(
        agent_id=agent_id,
        host_name=str(payload["host_name"]),
        host_address=str(payload["host_address"]),
        source_agent_id=str(payload.get("source_agent_id") or ""),
        clone_agent_id=str(payload.get("clone_agent_id") or ""),
        source_host_name=str(payload.get("source_host_name") or ""),
        source_host_address=str(payload.get("source_host_address") or ""),
        target_host_name=str(payload.get("target_host_name") or ""),
        target_host_address=str(payload.get("target_host_address") or ""),
    )


def _agent_snapshot(agent: Paglet) -> dict[str, Any]:
    with agent.locked_state() as state:
        state_wire = dataclass_to_wire(state)
    return {"state": state_wire, "resources": agent.resources.status()}


def _target_to_wire(target: str | TransferTicket) -> dict[str, Any]:
    if isinstance(target, TransferTicket):
        return {"ticket": target.to_wire()}
    return {"target": str(target)}


def _set_process_title(title: str) -> None:
    try:
        import setproctitle

        setproctitle.setproctitle(title)
    except Exception:
        pass


def _short_class_name(class_name: str) -> str:
    qualname = class_name.split(":", 1)[-1]
    return qualname.rsplit(".", 1)[-1]


def _error_to_wire(exc: Exception) -> dict[str, str]:
    return {"error_type": exc.__class__.__name__, "error": str(exc)}


def _error_from_wire(payload: dict[str, Any]) -> Exception:
    error_type = str(payload.get("error_type") or "HostError")
    message = str(payload.get("error") or error_type)
    cls = _ERROR_TYPES.get(error_type, HostError)
    try:
        return cls(message)
    except TypeError:
        return HostError(message)
