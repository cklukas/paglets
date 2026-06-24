# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import io
import json
import pickle
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

import paglets.remote.client as client_module
import paglets.remote.transport as transport_module
import paglets.runtime.process_protocol as process_protocol
import paglets.runtime.process_runtime as process_runtime
from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message
from paglets.remote.client import HostClient
from paglets.remote.transfer import TransferTicket
from paglets.runtime.host import Host
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire, qualified_name
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


def test_host_accepts_binary_pickle_create_payload(tmp_path: Path):
    host = Host(name="alpha", host="127.0.0.1", port=free_port(), persistence_dir=tmp_path / "alpha")
    large_value = "x" * (1024 * 1024)
    host.start_background()
    try:
        response = host.client.post_pickle(
            f"{host.address}/agents",
            {
                "agent_class_name": qualified_name(TravelAgent),
                "state_class_name": qualified_name(TravelState),
                "state": dataclass_to_wire(TravelState(last_message=large_value)),
                "init": "seed",
                "agent_id": None,
            },
        )
        proxy = host.get_proxy(response["proxy"]["agent_id"])
        assert proxy is not None
        state = host.client.get_json(f"{host.address}/agents/{proxy.agent_id}/state")
    finally:
        host.stop()

    assert state["state"]["last_message"] == large_value


def test_binary_pickle_transport_streams_without_dumps_or_loads(tmp_path: Path, monkeypatch):
    def fail_dumps(*_args, **_kwargs):
        raise AssertionError("post_pickle should stream with pickle.dump, not pickle.dumps")

    def fail_loads(*_args, **_kwargs):
        raise AssertionError("host should stream with pickle.load, not pickle.loads")

    monkeypatch.setattr("paglets.remote.transport.pickle.dumps", fail_dumps)
    monkeypatch.setattr("paglets.remote.transport.pickle.loads", fail_loads)
    host = Host(name="alpha", host="127.0.0.1", port=free_port(), persistence_dir=tmp_path / "alpha")
    host.start_background()
    try:
        response = host.client.post_pickle(
            f"{host.address}/agents",
            {
                "agent_class_name": qualified_name(TravelAgent),
                "state_class_name": qualified_name(TravelState),
                "state": dataclass_to_wire(TravelState(last_message="x" * (1024 * 1024))),
                "init": "seed",
                "agent_id": None,
            },
        )
    finally:
        host.stop()

    assert "proxy" in response


def test_post_pickle_uses_chunked_stream_without_content_length(monkeypatch):
    class FakeResponse:
        status = 200

        def read(self):
            return b'{"ok": true}'

    class FakeConnection:
        def __init__(self):
            self.headers: dict[str, str] = {}
            self.body = bytearray()

        def putrequest(self, method, target):
            self.method = method
            self.target = target

        def putheader(self, name, value):
            self.headers[name] = value

        def endheaders(self):
            return None

        def send(self, data):
            self.body.extend(data)

        def getresponse(self):
            return FakeResponse()

        def close(self):
            return None

    connection = FakeConnection()
    monkeypatch.setattr(client_module, "_connection", lambda *_args, **_kwargs: connection)

    result = HostClient().post_pickle("http://example.test/agents", {"value": "x" * 1024})

    assert result == {"ok": True}
    assert connection.headers["Transfer-Encoding"] == "chunked"
    assert "Content-Length" not in connection.headers
    assert connection.body.endswith(b"0\r\n\r\n")
    payload = pickle.load(transport_module.ChunkedRequestReader(io.BytesIO(connection.body)))
    assert payload == {"value": "x" * 1024}


def test_dispatch_uses_binary_pickle_payload_for_paglet_movement(tmp_path: Path):
    client = RecordingClient()
    alpha = Host(name="alpha", host="127.0.0.1", port=free_port(), client=client, persistence_dir=tmp_path / "alpha")
    beta = Host(name="beta", host="127.0.0.1", port=free_port(), persistence_dir=tmp_path / "beta")
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create(TravelAgent, TravelState(last_message="x" * (1024 * 1024)))
        remote = proxy.dispatch(beta.address)
        state = beta.client.get_json(f"{beta.address}/agents/{remote.agent_id}/state")
    finally:
        beta.stop()
        alpha.stop()

    assert any(url == f"{beta.address}/agents" for url in client.pickle_posts)
    assert state["state"]["last_message"] == "x" * (1024 * 1024)


def test_child_startup_streams_state_outside_process_config():
    state = {"payload": b"x" * (2 * 1024 * 1024)}

    config = process_runtime.make_child_config(
        host_name="alpha",
        host_address="http://127.0.0.1:1",
        agent_id="agent",
        agent_class_name=qualified_name(BinaryTravelAgent),
        state_class_name=qualified_name(BinaryTravelState),
        state=state,
    )

    assert config.state is None
    assert config.state_stream is not None
    assert transport_module.receive_local_pickle(config.state_stream) == state


def test_local_pickle_stream_uses_shared_memory_and_unlinks_after_receive(monkeypatch):
    monkeypatch.setattr(transport_module, "LOCAL_PICKLE_SEGMENT_BYTES", 64)
    payload = {"value": b"x" * 512}

    stream = transport_module.start_local_pickle_sender(payload)

    assert stream["kind"] == "shared_memory_pickle"
    assert len(stream["segments"]) > 1
    assert not hasattr(transport_module, "Listener")
    segment_names = [segment["name"] for segment in stream["segments"]]
    assert transport_module.receive_local_pickle(stream) == payload
    assert stream["token"] not in transport_module._LOCAL_PICKLE_STREAMS
    for name in segment_names:
        with pytest.raises(FileNotFoundError):
            transport_module.shared_memory.SharedMemory(name=name)


def test_child_host_call_state_payload_uses_local_pickle_stream():
    payload = {"state": {"payload": b"x" * (2 * 1024 * 1024)}, "small": True}

    streamed = process_protocol._stream_state_payload(payload)

    assert "state" not in streamed
    assert streamed["small"] is True
    assert "state_stream" in streamed
    assert process_protocol._materialize_state_stream(streamed) == payload


def test_binary_state_json_projection_and_dataclass_restore(tmp_path: Path):
    host = Host(name="alpha", host="127.0.0.1", port=free_port(), persistence_dir=tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(
            BinaryTravelAgent,
            BinaryTravelState(payload=b"\x00payload", marker=bytearray(b"\x01marker")),
        )
        state_payload = host.client.get_json(f"{host.address}/agents/{proxy.agent_id}/state")
        restored = dataclass_from_wire(BinaryTravelState, state_payload["state"])
    finally:
        host.stop()

    assert state_payload["state"]["payload"] == b"\x00payload"
    assert state_payload["state"]["marker"] == bytearray(b"\x01marker")
    assert restored.payload == b"\x00payload"
    assert restored.marker == bytearray(b"\x01marker")


def test_large_binary_state_moves_clones_and_reactivates(tmp_path: Path):
    payload = b"x" * (16 * 1024 * 1024)
    alpha = Host(name="alpha", host="127.0.0.1", port=free_port(), persistence_dir=tmp_path / "alpha")
    beta = Host(name="beta", host="127.0.0.1", port=free_port(), persistence_dir=tmp_path / "beta")
    alpha.start_background()
    beta.start_background()
    try:
        original = alpha.create(BinaryTravelAgent, BinaryTravelState(payload=payload, marker=bytearray(b"m")))

        clone_wire = original.send(Message("clone", {"target": beta.address}))
        clone_state = beta.get_state(clone_wire["agent_id"], BinaryTravelState)

        remote_wire = original.send(Message("go", {"target": beta.address}))
        remote = beta.get_proxy(remote_wire["agent_id"])
        assert remote is not None
        remote.deactivate()
        beta.activate(remote.agent_id)
        moved_state = beta.get_state(remote.agent_id, BinaryTravelState)
    finally:
        beta.stop()
        alpha.stop()

    assert clone_state.payload == payload
    assert clone_state.marker == bytearray(b"m")
    assert moved_state.payload == payload
    assert moved_state.marker == bytearray(b"m")


def test_same_host_dispatch_bypasses_http_pickle(tmp_path: Path):
    host = Host(
        name="alpha",
        host="127.0.0.1",
        port=free_port(),
        client=FailingPickleClient(),
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        proxy = host.create(TravelAgent, TravelState(last_message="local"))
        moved = proxy.dispatch(host.address)
        state = host.get_state(moved.agent_id, TravelState)
    finally:
        host.stop()

    assert moved.host_url == host.address
    assert state.last_message == "local"
    assert "arrived:alpha:from:alpha" in state.events


def test_same_host_clone_bypasses_http_pickle(tmp_path: Path):
    host = Host(
        name="alpha",
        host="127.0.0.1",
        port=free_port(),
        client=FailingPickleClient(),
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        proxy = host.create(BinaryTravelAgent, BinaryTravelState(payload=b"x" * 1024))
        clone = proxy.clone(target=host.address)
        state = host.get_state(clone.agent_id, BinaryTravelState)
    finally:
        host.stop()

    assert clone.host_url == host.address
    assert state.payload == b"x" * 1024


def test_binary_payload_encoding_benchmark_avoids_json_transport_cost():
    wire = {
        "envelope": {
            "kind": "dispatch",
            "agent_id": "agent",
            "agent_class_name": qualified_name(TravelAgent),
            "state_class_name": qualified_name(TravelState),
            "state": dataclass_to_wire(TravelState(last_message="x" * (4 * 1024 * 1024))),
            "source_host_name": "alpha",
            "source_host_address": "http://alpha",
            "target_host_name": "beta",
            "target_host_address": "http://beta",
            "clone_of": None,
            "metadata": {},
        }
    }

    started = time.perf_counter()
    binary = pickle.dumps(wire, protocol=pickle.HIGHEST_PROTOCOL)
    pickle_seconds = time.perf_counter() - started
    started = time.perf_counter()
    json_payload = json.dumps(wire).encode("utf-8")
    json_seconds = time.perf_counter() - started

    assert pickle.loads(binary) == wire
    assert len(binary) < len(json_payload)
    assert pickle_seconds < json_seconds
