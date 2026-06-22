# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class ContextEvent:
    """Host-level event emitted by the paglet context."""

    event_id: int
    kind: str
    host_name: str
    host_address: str
    timestamp: float = field(default_factory=time.time)
    agent_id: str | None = None
    class_name: str | None = None
    message_id: str | None = None
    service_name: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_wire(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "kind": self.kind,
            "host_name": self.host_name,
            "host_address": self.host_address,
            "timestamp": self.timestamp,
            "agent_id": self.agent_id,
            "class_name": self.class_name,
            "message_id": self.message_id,
            "service_name": self.service_name,
            "data": self.data,
            "error": self.error,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> ContextEvent:
        return cls(
            event_id=int(payload["event_id"]),
            kind=str(payload["kind"]),
            host_name=str(payload["host_name"]),
            host_address=str(payload["host_address"]),
            timestamp=float(payload.get("timestamp", time.time())),
            agent_id=str(payload["agent_id"]) if payload.get("agent_id") is not None else None,
            class_name=str(payload["class_name"]) if payload.get("class_name") is not None else None,
            message_id=str(payload["message_id"]) if payload.get("message_id") is not None else None,
            service_name=str(payload["service_name"]) if payload.get("service_name") is not None else None,
            data=dict(payload.get("data") or {}),
            error=str(payload["error"]) if payload.get("error") is not None else None,
        )


class ContextListener(Protocol):
    def __call__(self, event: ContextEvent) -> None: ...


class ContextEventLog:
    """Bounded in-memory context event log with best-effort listeners."""

    def __init__(self, *, capacity: int = 1000):
        self.capacity = max(1, int(capacity))
        self._events: deque[ContextEvent] = deque(maxlen=self.capacity)
        self._listeners: list[ContextListener] = []
        self._next_id = 1
        self._lock = threading.RLock()

    def add_listener(self, listener: ContextListener) -> None:
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def remove_listener(self, listener: ContextListener) -> None:
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def emit(
        self,
        *,
        kind: str,
        host_name: str,
        host_address: str,
        agent_id: str | None = None,
        class_name: str | None = None,
        message_id: str | None = None,
        service_name: str | None = None,
        data: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> ContextEvent:
        event = self._append(
            kind=kind,
            host_name=host_name,
            host_address=host_address,
            agent_id=agent_id,
            class_name=class_name,
            message_id=message_id,
            service_name=service_name,
            data=data or {},
            error=error,
        )
        self._notify(event)
        return event

    def events_since(self, since: int = 0, *, limit: int = 100) -> list[ContextEvent]:
        limit = max(0, int(limit))
        with self._lock:
            events = [event for event in self._events if event.event_id > since]
        return events[:limit] if limit else []

    def _append(self, **kwargs: Any) -> ContextEvent:
        with self._lock:
            event = ContextEvent(event_id=self._next_id, **kwargs)
            self._next_id += 1
            self._events.append(event)
            return event

    def _notify(self, event: ContextEvent) -> None:
        with self._lock:
            listeners = list(self._listeners)
        for listener in listeners:
            try:
                listener(event)
            except Exception as exc:
                failure = self._append(
                    kind="event-listener-failed",
                    host_name=event.host_name,
                    host_address=event.host_address,
                    agent_id=event.agent_id,
                    class_name=event.class_name,
                    message_id=event.message_id,
                    service_name=event.service_name,
                    data={"event_id": event.event_id, "listener": repr(listener)},
                    error=str(exc),
                )
                with self._lock:
                    remaining = [item for item in self._listeners if item is not listener]
                for other in remaining:
                    try:
                        other(failure)
                    except Exception:
                        continue
