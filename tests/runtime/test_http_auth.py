# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import http.client
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

import paglets.remote.client as client_module
import paglets.remote.transport as transport_module
import paglets.runtime.http_api as http_api_module
from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import AuthenticationError
from paglets.core.messages import Message
from paglets.remote.client import HostClient
from paglets.remote.transfer import TransferTicket
from paglets.runtime.host import Host
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


def test_host_api_key_rejects_missing_and_wrong_bearer_tokens(tmp_path: Path):
    host = Host(
        name="alpha",
        host="127.0.0.1",
        port=free_port(),
        api_key="secret",
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        with pytest.raises(AuthenticationError):
            HostClient().get_json(f"{host.address}/health")
        with pytest.raises(AuthenticationError):
            HostClient(api_key="wrong").get_json(f"{host.address}/health")
        assert HostClient(api_key="secret").get_json(f"{host.address}/health")["name"] == "alpha"

        request = Request(f"{host.address}/health", method="GET")
        with pytest.raises(HTTPError) as error:
            urlopen(request, timeout=2)
    finally:
        host.stop()

    assert error.value.code == 401
    assert error.value.headers["WWW-Authenticate"] == "Bearer"
    payload = json.loads(error.value.read().decode("utf-8"))
    assert payload == {"error_type": "AuthenticationError", "error": "Authentication required"}


def test_host_api_key_rejects_pickle_post_before_reading_payload(tmp_path: Path, monkeypatch):
    def fail_load(*_args, **_kwargs):
        raise AssertionError("unauthorized pickle body should not be read")

    monkeypatch.setattr(http_api_module, "load_http_pickle_payload", fail_load)
    host = Host(
        name="alpha",
        host="127.0.0.1",
        port=free_port(),
        api_key="secret",
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    parsed = client_module.urlparse(host.address)
    connection = http.client.HTTPConnection(parsed.netloc, timeout=2.0)
    try:
        connection.putrequest("POST", "/agents")
        connection.putheader("Host", parsed.netloc)
        connection.putheader("Content-Type", transport_module.PICKLE_CONTENT_TYPE)
        connection.putheader("Content-Length", "1048576")
        connection.endheaders()
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        connection.close()
        host.stop()

    assert response.status == 401
    assert payload == {"error_type": "AuthenticationError", "error": "Authentication required"}
