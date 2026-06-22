# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from paglets.core.runtime_values import ArrivalMode, enum_from_wire, require_enum


@dataclass(frozen=True, slots=True)
class TransferTicket:
    """Options and preflight requirements for dispatching or cloning a paglet."""

    destination: str
    timeout: float = 10.0
    retries: int = 0
    retry_interval: float = 0.25
    required_capabilities: tuple[str, ...] = ()
    expected_code_version: str | None = None
    arrival_mode: ArrivalMode = ArrivalMode.ACTIVATE

    def __post_init__(self) -> None:
        require_enum(self.arrival_mode, ArrivalMode, "arrival_mode")

    @classmethod
    def from_target(cls, target: str | "TransferTicket") -> "TransferTicket":
        if isinstance(target, cls):
            return target
        return cls(destination=target)

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "TransferTicket":
        return cls(
            destination=str(payload["destination"]),
            timeout=float(payload.get("timeout", 10.0)),
            retries=int(payload.get("retries", 0)),
            retry_interval=float(payload.get("retry_interval", 0.25)),
            required_capabilities=tuple(str(item) for item in payload.get("required_capabilities", [])),
            expected_code_version=(
                str(payload["expected_code_version"])
                if payload.get("expected_code_version") is not None
                else None
            ),
            arrival_mode=enum_from_wire(
                payload.get("arrival_mode") or ArrivalMode.ACTIVATE.value,
                ArrivalMode,
                "arrival_mode",
            ),
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "destination": self.destination,
            "timeout": self.timeout,
            "retries": self.retries,
            "retry_interval": self.retry_interval,
            "required_capabilities": list(self.required_capabilities),
            "expected_code_version": self.expected_code_version,
            "arrival_mode": self.arrival_mode.value,
        }
