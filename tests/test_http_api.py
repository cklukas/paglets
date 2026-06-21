# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path

import paglets.host as host_module
from paglets import Host, Message
from paglets.cli import _parser as host_cli_parser
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


def test_host_public_bind_resolves_to_lan_host(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(host_module, "_auto_lan_host", lambda: "127.0.0.1")
    port = free_port()
    host = Host(
        name="alpha",
        host="auto",
        port=port,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        health = host.client.get_json(f"http://127.0.0.1:{port}/health")
    finally:
        host.stop()

    assert host.bind_host == "127.0.0.1"
    assert host.address == f"http://127.0.0.1:{port}"
    assert health["address"] == host.address


def test_host_auto_bind_change_rebinds_and_refreshes_mesh(tmp_path: Path, monkeypatch):
    current_ip = {"value": "127.0.0.1"}
    monkeypatch.setattr(host_module, "_auto_lan_host", lambda: current_ip["value"])
    port = free_port()
    host = Host(
        name="alpha",
        host="auto",
        port=port,
        mesh=False,
        bind_watch_interval=0.1,
        persistence_dir=tmp_path / "alpha",
    )
    rebind_calls: list[list[str]] = []

    def fake_rebind(bind_hosts: list[str]) -> bool:
        old_address = host.address
        rebind_calls.append(list(bind_hosts))
        host.bind_hosts = list(bind_hosts)
        host.bind_host = bind_hosts[0]
        host.public_host = bind_hosts[0]
        host.address = f"http://{bind_hosts[0]}:{host.port}"
        host.mesh.local_address_changed(old_address)
        return True

    monkeypatch.setattr(host, "_rebind_http_servers", fake_rebind)
    host.start_background()
    try:
        old_address = host.address
        current_ip["value"] = "127.0.0.2"

        assert host._check_auto_bind_change() is True
        assert rebind_calls == [["127.0.0.2"]]
        assert host.address == f"http://127.0.0.2:{port}"
        assert [ref.url for ref in host.mesh.hosts()] == [host.address]
        assert old_address not in {ref.url for ref in host.mesh.hosts()}
    finally:
        host.stop()


def test_host_cli_bind_public_accepts_auto_and_override():
    parser = host_cli_parser()

    auto = parser.parse_args(["--name", "alpha", "--bind-public"])
    forced = parser.parse_args(["--name", "alpha", "--bind-public", "192.0.2.10"])
    multiple = parser.parse_args(
        ["--name", "alpha", "--bind-public", "192.0.2.10", "--bind-public", "198.51.100.20"]
    )

    assert auto.bind_public == ["auto"]
    assert forced.bind_public == ["192.0.2.10"]
    assert multiple.bind_public == ["192.0.2.10", "198.51.100.20"]
