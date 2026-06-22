# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import ForbiddenError, RemoteHostError, TransferError
from paglets.core.messages import Message
from paglets.core.runtime_values import ServiceScope
from paglets.remote.client import HostClient
from paglets.remote.proxy import PagletProxy
from paglets.remote.transfer import TransferTicket
from paglets.runtime.host import Host
from paglets.tooling.cli import main as host_cli_main
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


def test_relay_connect_mode_dispatch_and_bidirectional_messages(tmp_path: Path):
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
    hub.start_background()
    beta.start_background()
    laptop.start_background()
    try:
        _wait_for(lambda: hub.mesh.lookup("B") is not None and hub.mesh.lookup("L") is not None)
        _wait_for(lambda: laptop.mesh.lookup("B") is not None)

        local = laptop.create(TravelAgent, TravelState(last_message="from-laptop"), init="seed")
        remote = laptop.dispatch(local.agent_id, "B")

        assert remote.host_url == beta.address
        assert hub.get_proxy(local.agent_id) is None
        assert laptop.get_proxy(local.agent_id) is None
        assert beta.get_state(local.agent_id, TravelState).last_message == "from-laptop"
        assert remote.send(Message("remember", {"value": "on-beta"})) == "remembered:on-beta"
        assert beta.get_state(local.agent_id, TravelState).last_message == "on-beta"

        receiver = laptop.create(TravelAgent, TravelState(), init=None)
        receiver_from_beta = PagletProxy(laptop.address, receiver.agent_id, beta.client)
        assert receiver_from_beta.send(Message("remember", {"value": "from-beta"})) == "remembered:from-beta"
        assert laptop.get_state(receiver.agent_id, TravelState).last_message == "from-beta"

        created = laptop.create_remote(
            beta.address,
            TravelAgent,
            TravelState(last_message="created-through-relay"),
            init="relay-create",
        )
        assert created.host_url == beta.address
        assert created.info()["host"] == "B"
        assert created.send(Message("remember", {"value": "created-on-beta"})) == "remembered:created-on-beta"
        assert beta.get_state(created.agent_id, TravelState).last_message == "created-on-beta"

        beta.advertise_service(created.agent_id, "relay.travel", capabilities=["remember"], scope=ServiceScope.MESH)
        services = laptop.lookup_services("relay.travel", capability="remember", scope=ServiceScope.MESH)
        assert [service.proxy.agent_id for service in services] == [created.agent_id]

        created.dispose()
        assert beta.get_proxy(created.agent_id) is None
    finally:
        laptop.stop()
        beta.stop()
        hub.stop()


def test_relay_dispatch_to_offline_target_fails_and_keeps_source_active(tmp_path: Path):
    hub, _beta, laptop, public_url = _relay_hosts(tmp_path, relay_offline_after=0.1)
    hub.start_background()
    hub.relay_connect(
        {
            "health": {
                "name": "B",
                "address": f"{public_url}/relay/hosts/B",
                "code_version": hub.mesh.code_version,
                "active_count": 0,
                "inactive_count": 0,
            }
        }
    )
    with hub._lock:
        hub._relay_nodes["B"].last_seen = time.time() - 1.0
    laptop.start_background()
    try:
        _wait_for(lambda: laptop.mesh.lookup("B") is not None)
        local = laptop.create(TravelAgent, TravelState(last_message="from-laptop"), init="seed")

        with pytest.raises(TransferError) as error:
            laptop.dispatch(local.agent_id, "B")

        assert "offline/not polling" in str(error.value)
        assert laptop.get_proxy(local.agent_id) is not None
        assert laptop.get_state(local.agent_id, TravelState).last_message == "from-laptop"
        assert any(
            event.kind == "relay-target-offline" and event.data.get("target") == "B" for event in hub.list_events()
        )
    finally:
        laptop.stop()
        hub.stop()


def test_relay_delivery_timeout_fails_and_keeps_source_active(tmp_path: Path):
    hub, _beta, laptop, public_url = _relay_hosts(tmp_path, relay_offline_after=30.0)
    hub.start_background()
    hub.relay_connect(
        {
            "health": {
                "name": "B",
                "address": f"{public_url}/relay/hosts/B",
                "code_version": hub.mesh.code_version,
                "active_count": 0,
                "inactive_count": 0,
            }
        }
    )
    laptop.start_background()
    try:
        _wait_for(lambda: laptop.mesh.lookup("B") is not None)
        local = laptop.create(TravelAgent, TravelState(last_message="waiting"), init="seed")

        with pytest.raises(TransferError) as error:
            laptop.dispatch(local.agent_id, TransferTicket("B", timeout=0.2))

        assert "timed out" in str(error.value)
        assert laptop.get_proxy(local.agent_id) is not None
        state = laptop.get_state(local.agent_id, TravelState)
        assert "dispatching:L->B" in state.events
        diagnostics = HostClient(api_key="secret").get_json(f"{public_url}/relay/diagnostics")
        assert diagnostics["nodes"][0]["name"] == "B"
        assert diagnostics["nodes"][0]["queue_depth"] >= 1
        assert any(
            event.kind == "relay-delivery-enqueued" and event.data.get("target") == "B" for event in hub.list_events()
        )
        assert any(
            event.kind == "relay-delivery-timeout" and event.data.get("stage") == "relay-ack"
            for event in hub.list_events()
        )
    finally:
        laptop.stop()
        hub.stop()


def test_relay_target_arrival_failure_propagates_and_keeps_source_active(tmp_path: Path):
    hub, beta, laptop, _public_url = _relay_hosts(tmp_path)
    hub.start_background()
    beta.start_background()
    laptop.start_background()
    try:
        _wait_for(lambda: laptop.mesh.lookup("B") is not None)
        local = laptop.create(ExplodingArrivalAgent, TravelState(last_message="boom"), init=None)

        with pytest.raises(TransferError) as error:
            laptop.dispatch(local.agent_id, "B")

        assert "arrival boom" in str(error.value)
        assert laptop.get_proxy(local.agent_id) is not None
        assert laptop.get_state(local.agent_id, TravelState).last_message == "boom"
        assert any(event.kind == "relay-delivery-ack" and event.data.get("ok") is False for event in hub.list_events())
        assert not any(event.kind == "arrival" and event.agent_id == local.agent_id for event in hub.list_events())
    finally:
        laptop.stop()
        beta.stop()
        hub.stop()


def test_relay_child_dispatch_failure_keeps_paglet_active(tmp_path: Path):
    hub, _beta, laptop, public_url = _relay_hosts(tmp_path, relay_offline_after=30.0)
    hub.start_background()
    hub.relay_connect(
        {
            "health": {
                "name": "B",
                "address": f"{public_url}/relay/hosts/B",
                "code_version": hub.mesh.code_version,
                "active_count": 0,
                "inactive_count": 0,
            }
        }
    )
    laptop.start_background()
    try:
        _wait_for(lambda: laptop.mesh.lookup("B") is not None)
        local = laptop.create(SelfDispatchAgent, TravelState(last_message="inside"), init=None)

        with pytest.raises(TransferError):
            local.send(Message("go", {"target": TransferTicket("B", timeout=0.2).to_wire()}))

        assert laptop.get_proxy(local.agent_id) is not None
        assert laptop.get_state(local.agent_id, TravelState).last_message == "inside"
    finally:
        laptop.stop()
        hub.stop()


def test_relay_message_to_offline_target_reports_remote_error(tmp_path: Path):
    hub, _beta, laptop, public_url = _relay_hosts(tmp_path, relay_offline_after=0.1)
    hub.start_background()
    hub.relay_connect(
        {
            "health": {
                "name": "B",
                "address": f"{public_url}/relay/hosts/B",
                "code_version": hub.mesh.code_version,
                "active_count": 0,
                "inactive_count": 0,
            }
        }
    )
    with hub._lock:
        hub._relay_nodes["B"].last_seen = time.time() - 1.0
    laptop.start_background()
    try:
        _wait_for(lambda: laptop.mesh.lookup("B") is not None)
        proxy = PagletProxy(f"{public_url}/relay/hosts/B", "missing-agent", laptop.client)

        with pytest.raises(RemoteHostError) as error:
            proxy.send(Message("remember", {"value": "x"}), timeout=1.0)

        assert "offline/not polling" in str(error.value)
    finally:
        laptop.stop()
        hub.stop()


def test_relay_mode_disables_git_update_endpoint_even_with_valid_key(tmp_path: Path):
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
        with pytest.raises(ForbiddenError):
            HostClient(api_key="secret").post_json(f"{public_url}/admin/git-update", {"target_hash": "a" * 40})
    finally:
        host.stop()


def test_cli_rejects_connect_mode_with_auto_update():
    with pytest.raises(SystemExit) as error:
        host_cli_main(["--name", "B", "--connect-to", "http://example.test/paglets", "--auto-update-from-git"])

    assert error.value.code == 2
