# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

import paglets.runtime.binding as binding_module
from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message
from paglets.remote.client import HostClient
from paglets.remote.transfer import TransferTicket
from paglets.runtime.host import Host
from paglets.tooling.cli import _parser as host_cli_parser
from tests.test_paglets_core import TravelState, free_port


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


def test_host_public_bind_resolves_to_lan_host(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(binding_module, "_auto_lan_host", lambda: "127.0.0.1")
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
    monkeypatch.setattr(binding_module, "_auto_lan_host", lambda: current_ip["value"])
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
    multiple = parser.parse_args(["--name", "alpha", "--bind-public", "192.0.2.10", "--bind-public", "198.51.100.20"])

    assert auto.bind_public == ["auto"]
    assert forced.bind_public == ["192.0.2.10"]
    assert multiple.bind_public == ["192.0.2.10", "198.51.100.20"]
