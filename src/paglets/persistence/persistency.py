# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any

from paglets.runtime.envelope import PagletEnvelope
from paglets.core.messages import Message


@dataclass(slots=True)
class DeactivationPolicy:
    """Policy chosen by a paglet for its inactive lifecycle."""

    activate_on_message: bool = True
    queue_messages_when_inactive: bool = True
    activate_on_startup: bool = False
    activate_at: float | None = None

    @classmethod
    def after(cls, seconds: float, **kwargs: Any) -> "DeactivationPolicy":
        return cls(activate_at=time.time() + seconds, **kwargs)

    def to_wire(self) -> dict[str, Any]:
        return {
            "activate_on_message": self.activate_on_message,
            "queue_messages_when_inactive": self.queue_messages_when_inactive,
            "activate_on_startup": self.activate_on_startup,
            "activate_at": self.activate_at,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any] | None) -> "DeactivationPolicy":
        payload = payload or {}
        activate_at = payload.get("activate_at")
        return cls(
            activate_on_message=bool(payload.get("activate_on_message", True)),
            queue_messages_when_inactive=bool(payload.get("queue_messages_when_inactive", True)),
            activate_on_startup=bool(payload.get("activate_on_startup", False)),
            activate_at=float(activate_at) if activate_at is not None else None,
        )


@dataclass(slots=True)
class DeactivationRequest:
    """Context for a deactivation request before the paglet chooses policy."""

    reason: str = "deactivate"
    source: str = "external"
    policy: DeactivationPolicy | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "source": self.source,
            "policy": self.policy.to_wire() if self.policy is not None else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any] | None) -> "DeactivationRequest":
        payload = payload or {}
        policy_payload = payload.get("policy")
        return cls(
            reason=str(payload.get("reason") or "deactivate"),
            source=str(payload.get("source") or "external"),
            policy=DeactivationPolicy.from_wire(policy_payload) if policy_payload is not None else None,
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclass(slots=True)
class QueuedMessage:
    """Message persisted while a paglet is inactive."""

    message: Message
    oneway: bool = False
    queued_at: float = field(default_factory=time.time)

    def to_wire(self) -> dict[str, Any]:
        return {
            "message": self.message.to_wire(),
            "oneway": self.oneway,
            "queued_at": self.queued_at,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "QueuedMessage":
        return cls(
            message=Message.from_wire(payload["message"]),
            oneway=bool(payload.get("oneway", False)),
            queued_at=float(payload.get("queued_at", time.time())),
        )


@dataclass(slots=True)
class InactiveRecord:
    """Durable representation of a deactivated paglet."""

    envelope: PagletEnvelope
    policy: DeactivationPolicy
    request: DeactivationRequest
    deactivated_at: float = field(default_factory=time.time)
    queued_messages: list[QueuedMessage] = field(default_factory=list)

    @property
    def agent_id(self) -> str:
        return self.envelope.agent_id

    def to_wire(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "envelope": self.envelope.to_wire(),
            "policy": self.policy.to_wire(),
            "request": self.request.to_wire(),
            "deactivated_at": self.deactivated_at,
            "queued_messages": [message.to_wire() for message in self.queued_messages],
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "InactiveRecord":
        return cls(
            envelope=PagletEnvelope.from_wire(payload["envelope"]),
            policy=DeactivationPolicy.from_wire(payload.get("policy")),
            request=DeactivationRequest.from_wire(payload.get("request")),
            deactivated_at=float(payload.get("deactivated_at", time.time())),
            queued_messages=[
                QueuedMessage.from_wire(item)
                for item in payload.get("queued_messages", [])
            ],
        )
