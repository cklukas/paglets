# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any


@dataclass(frozen=True, slots=True)
class PagletEvent:
    agent_id: str
    host_name: str
    host_address: str
    timestamp: float = field(default_factory=time.time)


@dataclass(frozen=True, slots=True)
class CreationEvent(PagletEvent):
    init: Any = None


@dataclass(frozen=True, slots=True)
class MobilityEvent(PagletEvent):
    source_host_name: str = ""
    source_host_address: str = ""
    target_host_name: str = ""
    target_host_address: str = ""
    reason: str = "dispatch"  # dispatch or retract


@dataclass(frozen=True, slots=True)
class CloneEvent(PagletEvent):
    source_agent_id: str = ""
    clone_agent_id: str = ""
    source_host_name: str = ""
    source_host_address: str = ""
    target_host_name: str = ""
    target_host_address: str = ""


@dataclass(frozen=True, slots=True)
class PersistencyEvent(PagletEvent):
    reason: str = "deactivate"  # deactivate or activate
    request: Any = None
    policy: Any = None
