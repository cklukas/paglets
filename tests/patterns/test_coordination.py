# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from paglets.patterns.coordination import CursorDrainMixin, MeshFanoutMixin, MeshFanoutState


@dataclass
class HelperState(MeshFanoutState):
    events: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: int = 1


class _Client:
    pass


class _Host:
    client = _Client()


class _Context:
    host = _Host()


class Helper(MeshFanoutMixin, CursorDrainMixin):
    def __init__(self):
        self.state = HelperState()
        self.context = _Context()
        self._lock = threading.RLock()
        self.changed = 0

    @contextmanager
    def locked_state(self) -> Iterator[HelperState]:
        with self._lock:
            yield self.state

    def notify_all_state_changed(self) -> None:
        self.changed += 1

    def wait_state(self, ready, *, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if ready(self.state):
                return True
            time.sleep(0.01)
        return ready(self.state)


def test_mesh_fanout_expiry_records_pending_errors():
    helper = Helper()
    helper.state.pending_hosts = ["alpha", "beta"]
    helper.state.deadline = time.monotonic() - 1.0

    helper.fanout_expire_pending("timed out")

    assert helper.state.pending_hosts == []
    assert helper.state.errors == {"alpha": "timed out", "beta": "timed out"}
    assert helper.changed == 1


def test_mesh_fanout_cleanup_records_errors():
    helper = Helper()
    helper.state.child_proxies = {"alpha": {"host_url": "http://127.0.0.1:9", "agent_id": "missing"}}

    helper.fanout_cleanup_children()

    assert "alpha" in helper.state.cleanup_errors


def test_cursor_drain_limits_and_reports_more():
    helper = Helper()
    cursor = helper.cursor_append_events([{"value": "a"}, {"value": "b"}, {"value": "c"}])

    events, last_cursor, more = helper.cursor_drain_events(after_cursor=0, limit=2)

    assert cursor == 3
    assert [event["value"] for event in events] == ["a", "b"]
    assert last_cursor == 2
    assert more is True
