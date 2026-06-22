# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import tempfile
import time

import pytest

from paglets.runtime.host import Host
from paglets.core.messages import Message
from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import HostError
from paglets.remote.mesh import HostRef, decode_mesh_beacon, encode_mesh_beacon
from tests.test_paglets_core import free_port


@dataclass
class MeshState(PagletState):
    events: list[str] = field(default_factory=list)


class MeshAgent(Paglet[MeshState]):
    State = MeshState

    def run(self):
        self.state.events.append(f"run:{self.context.name}")

    def handle_message(self, message: Message):
        if message.kind == "hosts":
            return [host.to_wire() for host in self.context.available_hosts()]
        if message.kind == "wait_host":
            return self.context.wait_for_host(message.args["target"], timeout=0.5).to_wire()
        if message.kind == "is_online":
            return self.context.is_host_online(message.args["target"])
        if message.kind == "wait_missing":
            return self.context.wait_for_host(message.args["target"], timeout=0.1, interval=0.02).to_wire()
        if message.kind == "dispatch_named":
            return self.dispatch_to(message.args["target"]).to_wire()
        if message.kind == "clone_named":
            return self.clone_to(message.args["target"]).to_wire()
        return self.not_handled()


def test_hosts_endpoint_includes_self_and_code_version():
    host = _host("alpha")
    host.start_background()
    try:
        health = host.client.get_json(f"{host.address}/health")
        payload = host.client.get_json(f"{host.address}/hosts")
    finally:
        host.stop()

    assert health["code_version"] == "mesh-test"
    assert "hosts:list" in health["capabilities"]
    assert payload["hosts"] == [
        {
            "name": "alpha",
            "url": host.address,
            "code_version": "mesh-test",
            "online": True,
            "last_seen": pytest.approx(payload["hosts"][0]["last_seen"]),
            "active_count": 0,
            "inactive_count": 0,
        }
    ]


def test_seeded_hosts_join_and_converge_on_same_version_peers():
    alpha = _host("alpha")
    beta = _host("beta", peers=[alpha.address])
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()

        alpha_hosts = {ref.name for ref in alpha.list_hosts()}
        beta_hosts = {ref.name for ref in beta.list_hosts()}
    finally:
        beta.stop()
        alpha.stop()

    assert alpha_hosts == {"alpha", "beta"}
    assert beta_hosts == {"alpha", "beta"}


def test_lan_discovery_registers_dynamic_hosts_and_adds_seed():
    alpha = _host("alpha", lan_discovery=True)
    ref = HostRef(
        name="windows",
        url="http://192.168.86.28:8765",
        code_version="mesh-test",
        online=True,
        last_seen=time.time(),
        active_count=0,
        inactive_count=0,
    )

    alpha.mesh._discover_lan_refs = lambda ports: [ref]  # type: ignore[method-assign]
    registered = alpha.mesh.discover_lan_once(force=True)

    assert registered == [ref]
    assert alpha.mesh.lookup("windows") == ref
    assert "http://192.168.86.28:8765" in alpha.mesh.peer_urls()


def test_offline_peer_is_marked_offline_after_failed_gossip():
    alpha = _host("alpha")
    beta = _host("beta", peers=[alpha.address])
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        beta.stop()
        alpha.mesh.gossip_once()

        beta_status = alpha.mesh.lookup("beta")
    finally:
        alpha.stop()

    assert beta_status is not None
    assert beta_status.online is False
    assert beta_status.error


def test_local_address_change_replaces_mesh_self_record():
    host = _host("alpha")
    host.start_background()
    try:
        old_address = host.address
        host.address = f"http://127.0.0.1:{free_port()}"
        host.mesh.local_address_changed(old_address)

        refs = host.list_hosts()
    finally:
        host.stop()

    assert [ref.url for ref in refs] == [host.address]
    assert old_address not in {ref.url for ref in refs}


def test_version_mismatch_peers_are_ignored():
    alpha = _host("alpha", version="mesh-a")
    beta = _host("beta", version="mesh-b", peers=[alpha.address])
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
    finally:
        beta.stop()
        alpha.stop()

    assert alpha.mesh.lookup("beta") is None
    assert beta.mesh.lookup("alpha") is None


def test_version_mismatch_peers_trigger_git_update_hook(monkeypatch):
    alpha = _host("alpha", version="mesh-a")
    beta = _host("beta", version="mesh-b", peers=[alpha.address])
    calls = []

    def fake_request(url, **kwargs):
        calls.append((url, kwargs))
        return None

    monkeypatch.setattr(beta, "request_peer_git_update", fake_request)
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
    finally:
        beta.stop()
        alpha.stop()

    assert calls
    assert calls[0][0] == alpha.address
    assert calls[0][1]["health"]["code_version"] == "mesh-a"


def test_version_mismatch_beacon_validates_git_update_target(monkeypatch):
    host = _host("alpha", version="mesh-a")
    ref = HostRef(
        name="beta",
        url="http://127.0.0.1:50423",
        code_version="mesh-b",
        online=True,
        last_seen=time.time(),
        active_count=0,
        inactive_count=0,
    )
    calls = []

    def fake_request(url, **kwargs):
        calls.append((url, kwargs))
        return None

    monkeypatch.setattr(host, "request_peer_git_update", fake_request)

    assert host.mesh.register(ref) is None
    assert calls == [
        (
            "http://127.0.0.1:50423",
            {"validate_health": True, "report_unreachable": False},
        )
    ]


def test_context_helpers_resolve_named_hosts_for_dispatch_and_clone():
    alpha = _host("alpha")
    beta = _host("beta", peers=[alpha.address])
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        proxy = alpha.create(MeshAgent, MeshState())

        hosts = [HostRef.from_wire(item) for item in proxy.send(Message("hosts"))]
        beta_ref = HostRef.from_wire(proxy.send(Message("wait_host", {"target": "beta"})))
        clone_proxy = proxy.send(Message("clone_named", {"target": "beta"}))
        moved_proxy = proxy.send(Message("dispatch_named", {"target": "beta"}))
    finally:
        beta.stop()
        alpha.stop()

    assert {ref.name for ref in hosts} == {"alpha", "beta"}
    assert isinstance(beta_ref, HostRef)
    assert clone_proxy["host_url"] == beta.address
    assert moved_proxy == {"host_url": beta.address, "agent_id": proxy.agent_id}


def test_wait_for_host_times_out_for_offline_peer():
    alpha = _host("alpha")
    beta = _host("beta", peers=[alpha.address])
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        proxy = alpha.create(MeshAgent, MeshState())
        beta.stop()
        alpha.mesh.gossip_once()

        assert proxy.send(Message("is_online", {"target": "beta"})) is False
        with pytest.raises(HostError):
            proxy.send(Message("wait_missing", {"target": "beta"}))
    finally:
        alpha.stop()


def test_mesh_beacon_round_trips_and_version_filter_ignores_mismatches():
    ref = HostRef(
        name="beta",
        url="http://127.0.0.1:8766",
        code_version="other",
        online=True,
        last_seen=123.0,
        active_count=1,
        inactive_count=2,
    )
    decoded = decode_mesh_beacon(encode_mesh_beacon(ref))

    host = _host("alpha")
    host.start_background()
    try:
        assert decoded == ref
        assert host.mesh.register(ref) is None
        assert host.mesh.lookup("beta") is None
    finally:
        host.stop()


def test_relay_mode_version_mismatch_does_not_request_git_update(monkeypatch):
    host = Host(
        name="alpha",
        host="127.0.0.1",
        port=free_port(),
        public_url="http://127.0.0.1:8765/paglets",
        mesh_version="mesh-a",
        mesh_multicast=False,
        mesh_lan_discovery=False,
        persistence_dir=Path(tempfile.mkdtemp(prefix="paglets-test-")) / "alpha",
    )
    calls = []
    monkeypatch.setattr(host, "request_peer_git_update", lambda *args, **kwargs: calls.append((args, kwargs)))

    assert host.mesh.register(
        HostRef(
            name="beta",
            url="http://127.0.0.1:8765/paglets/relay/hosts/beta",
            code_version="mesh-b",
            online=True,
            last_seen=time.time(),
            active_count=0,
            inactive_count=0,
        )
    ) is None

    assert calls == []
    assert host.mesh.lookup("beta") is not None
    assert host.mesh.lookup("beta").online is False


def _host(
    name: str,
    *,
    version: str = "mesh-test",
    peers: list[str] | None = None,
    lan_discovery: bool = False,
) -> Host:
    return Host(
        name=name,
        host="127.0.0.1",
        port=free_port(),
        mesh=True,
        peers=peers,
        mesh_multicast=False,
        mesh_lan_discovery=lan_discovery,
        mesh_version=version,
        mesh_gossip_interval=0.05,
        mesh_offline_after=0.2,
        persistence_dir=Path(tempfile.mkdtemp(prefix="paglets-test-")) / name,
    )
