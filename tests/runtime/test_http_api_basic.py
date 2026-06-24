# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message
from paglets.remote.client import HostClient
from paglets.remote.transfer import TransferTicket
from paglets.runtime.host import Host
from tests.test_paglets_core import TravelAgent, TravelState, free_port


@dataclass
class BinaryTravelState(PagletState):
    payload: bytes = b""
    marker: bytearray = field(default_factory=bytearray)


class BinaryTravelAgent(Paglet[BinaryTravelState]):
    State = BinaryTravelState

    def handle_message(self, message: Message):
        if message.kind == "go":
            return self.dispatch(message.args["target"]).to_wire()
        if message.kind == "clone":
            return self.clone(target=message.args["target"]).to_wire()
        return self.not_handled()


class ExplodingArrivalAgent(Paglet[TravelState]):
    State = TravelState

    def on_dispatching(self, event):
        self.state.events.append(f"dispatching:{event.source_host_name}->{event.target_host_name}")

    def on_arrival(self, event):
        raise RuntimeError("arrival boom")


class SelfDispatchAgent(Paglet[TravelState]):
    State = TravelState

    def handle_message(self, message: Message):
        if message.kind == "go":
            target = message.args["target"]
            if isinstance(target, dict):
                target = TransferTicket.from_wire(target)
            return self.dispatch(target).to_wire()
        return self.not_handled()


class BinaryMessageAgent(Paglet[BinaryTravelState]):
    State = BinaryTravelState

    def handle_message(self, message: Message):
        if message.kind == "echo-binary":
            payload = message.args["payload"]
            self.state.payload = payload
            self.state.marker = message.arg
            return {
                "payload": payload,
                "arg": message.arg,
                "nested": {"payload": payload},
            }
        return self.not_handled()


class RecordingClient(HostClient):
    def __init__(self):
        super().__init__()
        self.pickle_posts: list[str] = []

    def post_pickle(self, url: str, payload: dict, *, timeout: float | None = None):
        self.pickle_posts.append(url)
        return super().post_pickle(url, payload, timeout=timeout)


class FailingPickleClient(HostClient):
    def post_pickle(self, url: str, payload: dict, *, timeout: float | None = None):
        raise AssertionError(f"post_pickle should not be used for same-host movement: {url}")


def _wait_for(predicate, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for condition")


def _relay_hosts(tmp_path: Path, *, relay_offline_after: float = 30.0, relay_delivery_timeout: float | None = None):
    port = free_port()
    public_url = f"http://127.0.0.1:{port}/paglets"
    hub = Host(
        name="A",
        host="127.0.0.1",
        port=port,
        api_key="secret",
        public_url=public_url,
        persistence_dir=tmp_path / "A",
        mesh_multicast=False,
        mesh_lan_discovery=False,
        relay_offline_after=relay_offline_after,
        relay_delivery_timeout=relay_delivery_timeout,
    )
    beta = Host(
        name="B",
        api_key="secret",
        connect_to=public_url,
        persistence_dir=tmp_path / "B",
        mesh_multicast=False,
        mesh_lan_discovery=False,
    )
    laptop = Host(
        name="L",
        api_key="secret",
        connect_to=public_url,
        persistence_dir=tmp_path / "L",
        mesh_multicast=False,
        mesh_lan_discovery=False,
    )
    return hub, beta, laptop, public_url


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


def test_json_message_transport_preserves_binary_args_arg_and_replies(tmp_path: Path):
    host = Host(name="alpha", host="127.0.0.1", port=free_port(), persistence_dir=tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(BinaryMessageAgent, BinaryTravelState())

        reply = proxy.send(
            Message(
                "echo-binary",
                {"payload": b"\x00payload"},
                arg=bytearray(b"\x01argument"),
            )
        )
        state = host.get_state(proxy.agent_id, BinaryTravelState)
    finally:
        host.stop()

    assert reply == {
        "payload": b"\x00payload",
        "arg": bytearray(b"\x01argument"),
        "nested": {"payload": b"\x00payload"},
    }
    assert state.payload == b"\x00payload"
    assert state.marker == bytearray(b"\x01argument")


def test_public_url_path_prefix_and_api_key_are_supported(tmp_path: Path):
    port = free_port()
    public_url = f"http://127.0.0.1:{port}/paglets"
    host = Host(
        name="alpha",
        host="127.0.0.1",
        port=port,
        api_key="secret",
        public_url=public_url,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        health = HostClient(api_key="secret").get_json(f"{public_url}/health")
    finally:
        host.stop()

    assert host.address == public_url
    assert health["address"] == public_url
