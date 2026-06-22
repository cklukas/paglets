# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import PagletInactiveError
from paglets.core.messages import DEACTIVATE, Message
from paglets.persistence.persistency import DeactivationPolicy
from paglets.runtime.host import Host
from tests.support import free_port


@dataclass
class DurableState(PagletState):
    events: list[str] = field(default_factory=list)
    last_message: str | None = None


class DurableAgent(Paglet[DurableState]):
    State = DurableState

    def on_creation(self, event):
        self.state.events.append(f"created:{event.host_name}")

    def on_deactivating(self, event):
        self.state.events.append(f"deactivating:{event.reason}")

    def on_activation(self, event):
        self.state.events.append(f"activated:{event.reason}")

    def run(self):
        self.state.events.append(f"run:{self.context.name}")

    def handle_message(self, message: Message):
        if message.kind == "remember":
            self.state.last_message = str(message.args["value"])
            self.state.events.append(f"remember:{self.state.last_message}")
            return f"remembered:{self.state.last_message}"
        if message.kind == "self_deactivate":
            proxy = self.deactivate(policy=DeactivationPolicy(activate_on_startup=True))
            return proxy.to_wire()
        return self.not_handled()


def test_deactivation_persists_state_and_activation_restores_it(tmp_path: Path):
    port = free_port()
    persistence_dir = tmp_path / "alpha"
    host = _host("alpha", port, persistence_dir)
    host.start_background()
    try:
        proxy = host.create(DurableAgent, DurableState())
        proxy.send(Message("remember", {"value": "before"}))
        inactive_proxy = proxy.deactivate()
        inactive_file = persistence_dir / "inactive" / f"{proxy.agent_id}.json"

        assert inactive_proxy.agent_id == proxy.agent_id
        assert inactive_file.exists()
        assert host.get_proxy(proxy.agent_id) is None
    finally:
        host.stop()

    restored = _host("alpha", port, persistence_dir)
    restored.start_background()
    try:
        inactive = restored.client.get_json(f"{restored.address}/agents?state=inactive")
        assert [agent["agent_id"] for agent in inactive["agents"]] == [proxy.agent_id]

        restored_proxy = inactive_proxy.activate()
        state = restored.get_state(restored_proxy.agent_id, DurableState)

        assert state.last_message == "before"
        assert state.events[-2:] == ["activated:activate", "run:alpha"]
        assert not inactive_file.exists()
    finally:
        restored.stop()


def test_paglet_can_deactivate_itself_from_message_handler(tmp_path: Path):
    host = _host("alpha", free_port(), tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(DurableAgent, DurableState())

        reply = proxy.send(Message("self_deactivate"))

        assert reply == {"host_url": host.address, "agent_id": proxy.agent_id}
        assert host.get_proxy(proxy.agent_id) is None
        assert host.client.get_json(f"{host.address}/agents/{proxy.agent_id}")["active"] is False
    finally:
        host.stop()


def test_shutdown_deactivation_activates_on_next_startup(tmp_path: Path):
    port = free_port()
    persistence_dir = tmp_path / "alpha"
    host = _host("alpha", port, persistence_dir)
    host.start_background()
    proxy = host.create(DurableAgent, DurableState())

    host.shutdown()

    restarted = _host("alpha", port, persistence_dir)
    restarted.start_background()
    try:
        state = restarted.get_state(proxy.agent_id, DurableState)

        assert "deactivating:shutdown" in state.events
        assert state.events[-2:] == ["activated:activate", "run:alpha"]
    finally:
        restarted.stop()


def test_scheduled_activation_restores_due_inactive_paglet(tmp_path: Path):
    host = _host("alpha", free_port(), tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(DurableAgent, DurableState())
        proxy.deactivate(policy=DeactivationPolicy.after(0.15))

        _wait_until(lambda: host.get_proxy(proxy.agent_id) is not None)

        state = host.get_state(proxy.agent_id, DurableState)
        assert state.events[-2:] == ["activated:activate", "run:alpha"]
    finally:
        host.stop()


def test_message_to_inactive_paglet_activates_and_delivers_by_default(tmp_path: Path):
    host = _host("alpha", free_port(), tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(DurableAgent, DurableState())
        proxy.deactivate()

        result = proxy.send(Message("remember", {"value": "after"}))

        assert result == "remembered:after"
        assert host.get_state(proxy.agent_id, DurableState).last_message == "after"
    finally:
        host.stop()


def test_reserved_deactivate_message_deactivates_paglet(tmp_path: Path):
    host = _host("alpha", free_port(), tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(DurableAgent, DurableState())

        result = proxy.send(Message(DEACTIVATE))

        assert result == {"deactivated": True, "proxy": {"host_url": host.address, "agent_id": proxy.agent_id}}
        assert host.client.get_json(f"{host.address}/agents/{proxy.agent_id}")["active"] is False
    finally:
        host.stop()


def test_inactive_message_queue_and_no_delay_failure(tmp_path: Path):
    host = _host("alpha", free_port(), tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(DurableAgent, DurableState())
        proxy.deactivate(
            policy=DeactivationPolicy(
                activate_on_message=False,
                queue_messages_when_inactive=True,
            )
        )

        queued = proxy.send(Message("remember", {"value": "queued"}))

        assert queued["queued"] is True
        assert isinstance(queued["message_id"], str)
        with pytest.raises(PagletInactiveError):
            proxy.send(Message("remember", {"value": "now"}), no_delay=True)

        proxy.activate()

        state = host.get_state(proxy.agent_id, DurableState)
        assert state.last_message == "queued"
    finally:
        host.stop()


def test_dispose_inactive_paglet_deletes_persisted_record(tmp_path: Path):
    persistence_dir = tmp_path / "alpha"
    host = _host("alpha", free_port(), persistence_dir)
    host.start_background()
    try:
        proxy = host.create(DurableAgent, DurableState())
        proxy.deactivate()
        inactive_file = persistence_dir / "inactive" / f"{proxy.agent_id}.json"

        proxy.dispose()

        assert not inactive_file.exists()
        assert host.client.get_json(f"{host.address}/agents?state=all")["agents"] == []
    finally:
        host.stop()


def _host(name: str, port: int, persistence_dir: Path) -> Host:
    return Host(
        name=name,
        host="127.0.0.1",
        port=port,
        mesh=False,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
    )


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition was not met before timeout")
