# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

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


def test_connect_mode_serve_forever_waits_for_relay_thread(tmp_path: Path, monkeypatch):
    host = Host(
        name="B",
        api_key="secret",
        connect_to="http://127.0.0.1:9/paglets",
        persistence_dir=tmp_path / "B",
        mesh_multicast=False,
        mesh_lan_discovery=False,
    )
    relay_started = threading.Event()
    relay_stop = threading.Event()

    def relay_loop() -> None:
        relay_started.set()
        relay_stop.wait()

    def start_connect_background() -> None:
        if host._relay_client_thread is not None:
            return
        host._relay_client_thread = threading.Thread(target=relay_loop, daemon=True)
        host._relay_client_thread.start()

    monkeypatch.setattr(host, "_start_connect_background", start_connect_background)
    serve_thread = threading.Thread(target=host.serve_forever, daemon=True)
    serve_thread.start()
    try:
        assert relay_started.wait(timeout=1.0)
        time.sleep(0.1)
        assert serve_thread.is_alive()
    finally:
        relay_stop.set()
        with host._server_lock:
            host._relay_client_thread = None
        serve_thread.join(timeout=1.0)
        host.stop()


def test_relay_client_keeps_polling_while_delivery_is_handled(tmp_path: Path, monkeypatch):
    hub, beta, _laptop, public_url = _relay_hosts(tmp_path)
    first_started = threading.Event()
    release_first = threading.Event()
    second_seen = threading.Event()
    handler_calls = 0

    def handle_relay_delivery(delivery: dict) -> None:
        nonlocal handler_calls
        handler_calls += 1
        if handler_calls == 1:
            first_started.set()
            assert release_first.wait(timeout=2.0)
        elif handler_calls == 2:
            second_seen.set()
        beta.client.post_json(
            f"{public_url}/relay/ack/{delivery['delivery_id']}",
            {"ok": True, "result": {"handled": handler_calls}},
            timeout=5.0,
        )

    monkeypatch.setattr(beta, "_handle_relay_delivery", handle_relay_delivery)
    hub.start_background()
    beta.start_background()

    results: list[dict] = []
    errors: list[BaseException] = []

    def call_relay(path: str) -> None:
        try:
            results.append(hub.relay_api("B", "GET", path, {}, timeout=2.0))
        except BaseException as exc:  # pragma: no cover - assertion aid
            errors.append(exc)

    try:
        _wait_for(lambda: "B" in hub._relay_nodes)
        first_call = threading.Thread(target=call_relay, args=("/first",), daemon=True)
        first_call.start()
        assert first_started.wait(timeout=1.0)

        second_call = threading.Thread(target=call_relay, args=("/second",), daemon=True)
        second_call.start()
        assert second_seen.wait(timeout=1.0)

        release_first.set()
        first_call.join(timeout=2.0)
        second_call.join(timeout=2.0)
        assert not first_call.is_alive()
        assert not second_call.is_alive()
        assert not errors
        assert len(results) == 2
    finally:
        release_first.set()
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


def test_cli_rejects_connect_mode_without_api_key(monkeypatch):
    monkeypatch.delenv("PAGLETS_API_KEY", raising=False)

    with pytest.raises(SystemExit) as error:
        host_cli_main(["--name", "B", "--connect-to", "http://example.test/paglets"])

    assert error.value.code == 2


def test_cli_connect_mode_uses_default_api_key_env(tmp_path: Path, monkeypatch):
    hosts: list[dict] = []

    class FakeHost:
        def __init__(self, **kwargs):
            hosts.append(kwargs)
            self.name = kwargs["name"]
            self.address = "http://127.0.0.1:8765"
            self.port = kwargs["port"]
            self.mesh = SimpleNamespace(version_warning="", code_version="test")

        def shutdown(self) -> None:
            pass

        def start_background(self) -> None:
            pass

        def serve_forever(self) -> None:
            pass

    monkeypatch.setenv("PAGLETS_API_KEY", "secret")
    monkeypatch.setattr("paglets.tooling.cli.Host", FakeHost)

    result = host_cli_main(
        [
            "--name",
            "B",
            "--connect-to",
            "http://example.test/paglets",
            "--launch-config",
            str(tmp_path / "launch.toml"),
            "--no-sync-launch-config",
        ]
    )

    assert result == 0
    assert hosts[0]["api_key"] == "secret"
