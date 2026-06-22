# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
import io
import json
import http.client
from pathlib import Path
import pickle
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

import paglets.client as client_module
import paglets.host as host_module
import paglets.process_runtime as process_runtime
import paglets.transport as transport_module
from paglets import Host, Message, Paglet, PagletState
from paglets import PagletProxy
from paglets.cli import main as host_cli_main
from paglets.cli import _parser as host_cli_parser
from paglets.client import HostClient
from paglets.errors import AuthenticationError, ForbiddenError
from paglets.serde import dataclass_from_wire, dataclass_to_wire, qualified_name
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

    monkeypatch.setattr(host_module, "load_http_pickle_payload", fail_load)
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
    finally:
        laptop.stop()
        beta.stop()
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

    monkeypatch.setattr("paglets.transport.pickle.dumps", fail_dumps)
    monkeypatch.setattr("paglets.transport.pickle.loads", fail_loads)
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

    streamed = process_runtime._stream_state_payload(payload)

    assert "state" not in streamed
    assert streamed["small"] is True
    assert "state_stream" in streamed
    assert process_runtime._materialize_state_stream(streamed) == payload


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

    assert state_payload["state"]["payload"] == {"__paglets_binary__": "bytes", "base64": "AHBheWxvYWQ="}
    assert state_payload["state"]["marker"] == {"__paglets_binary__": "bytearray", "base64": "AW1hcmtlcg=="}
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
