# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


EnvelopeKind = Literal["dispatch", "clone", "retract", "activation"]


@dataclass(slots=True)
class PagletEnvelope:
    """Serialized mobile-object envelope transferred between hosts."""

    kind: EnvelopeKind
    agent_id: str
    agent_class_name: str
    state_class_name: str
    state: dict[str, Any]
    source_host_name: str
    source_host_address: str
    target_host_name: str
    target_host_address: str
    clone_of: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "agent_id": self.agent_id,
            "agent_class_name": self.agent_class_name,
            "state_class_name": self.state_class_name,
            "state": self.state,
            "source_host_name": self.source_host_name,
            "source_host_address": self.source_host_address,
            "target_host_name": self.target_host_name,
            "target_host_address": self.target_host_address,
            "clone_of": self.clone_of,
            "metadata": self.metadata,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "PagletEnvelope":
        return cls(
            kind=payload["kind"],
            agent_id=payload["agent_id"],
            agent_class_name=payload["agent_class_name"],
            state_class_name=payload["state_class_name"],
            state=dict(payload["state"]),
            source_host_name=payload["source_host_name"],
            source_host_address=payload["source_host_address"],
            target_host_name=payload["target_host_name"],
            target_host_address=payload["target_host_address"],
            clone_of=payload.get("clone_of"),
            metadata=dict(payload.get("metadata") or {}),
        )
