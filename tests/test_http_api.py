from __future__ import annotations

from paglets import Host
from tests.test_paglets_core import TravelAgent, TravelState, free_port


def test_host_http_api_lists_agents_and_reports_health():
    host = Host(name="alpha", host="127.0.0.1", port=free_port())
    host.start_background()
    try:
        proxy = host.create(TravelAgent, TravelState(), init="seed")
        proxy.send_message("remember", {"value": "hello"})
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
    assert agents["agents"] == [
        {
            "agent_id": proxy.agent_id,
            "class_name": "tests.test_paglets_core:TravelAgent",
            "state_class_name": "tests.test_paglets_core:TravelState",
            "host": "alpha",
            "address": host.address,
            "active": True,
        }
    ]
    assert active_state["active"] is True
    assert active_state["state"]["last_message"] == "hello"
    assert inactive_state["active"] is False
    assert inactive_state["state"]["last_message"] == "hello"
