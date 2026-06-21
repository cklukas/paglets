# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path

from paglets import Host, Message
from tests.test_paglets_core import TravelAgent, TravelState, free_port


def test_host_http_api_lists_agents_and_reports_health(tmp_path: Path):
    host = Host(name="alpha", host="127.0.0.1", port=free_port(), persistence_dir=tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(TravelAgent, TravelState(), init="seed")
        proxy.send(Message("remember", {"value": "hello"}))
        health = host.client.get_json(f"{host.address}/health")
        agents = host.client.get_json(f"{host.address}/agents")
        active_state = host.client.get_json(f"{host.address}/agents/{proxy.agent_id}/state")
        proxy.deactivate()
        inactive_state = host.client.get_json(f"{host.address}/agents/{proxy.agent_id}/state")
    finally:
        host.stop()

    assert health["name"] == "alpha"
    assert health["address"] == host.address
    assert health["active_count"] == 1
    assert health["inactive_count"] == 0
    assert "agents:state" in health["capabilities"]
    assert len(agents["agents"]) == 1
    agent = agents["agents"][0]
    assert agent["agent_id"] == proxy.agent_id
    assert agent["class_name"] == "tests.test_paglets_core:TravelAgent"
    assert agent["state_class_name"] == "tests.test_paglets_core:TravelState"
    assert agent["host"] == "alpha"
    assert agent["address"] == host.address
    assert agent["active"] is True
    assert isinstance(agent["pid"], int)
    assert agent["crashed"] is False
    assert agent["exitcode"] is None
    assert agent["error"] == ""
    assert agent["mailbox"] == {
        "queued_count": 0,
        "in_flight_count": 0,
        "delivered_count": 1,
        "failed_count": 0,
    }
    assert agent["resources"] == {}
    assert active_state["active"] is True
    assert active_state["state"]["last_message"] == "hello"
    assert inactive_state["active"] is False
    assert inactive_state["state"]["last_message"] == "hello"
