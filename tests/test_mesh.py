# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import tempfile

import pytest

from paglets import Host, Message, Paglet, PagletState
from paglets.errors import HostError
from paglets.mesh import HostRef, decode_mesh_beacon, encode_mesh_beacon
from tests.test_paglets_core import free_port


@dataclass
class MeshState(PagletState):
    events: list[str] = field(default_factory=list)


class MeshAgent(Paglet[MeshState]):
    State = MeshState

    def run(self):
        self.state.events.append(f"run:{self.context.name}")

    def handle_message(self, message: Message):
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


def test_context_helpers_resolve_named_hosts_for_dispatch_and_clone():
    alpha = _host("alpha")
    beta = _host("beta", peers=[alpha.address])
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        proxy = alpha.create(MeshAgent, MeshState())
        agent = alpha._agents[proxy.agent_id]

        hosts = agent.context.available_hosts()
        beta_ref = agent.context.wait_for_host("beta", timeout=0.5)
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
        agent = alpha._agents[proxy.agent_id]
        beta.stop()
        alpha.mesh.gossip_once()

        assert agent.context.is_host_online("beta") is False
        with pytest.raises(HostError):
            agent.context.wait_for_host("beta", timeout=0.1, interval=0.02)
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


def _host(name: str, *, version: str = "mesh-test", peers: list[str] | None = None) -> Host:
    return Host(
        name=name,
        host="127.0.0.1",
        port=free_port(),
        mesh=True,
        peers=peers,
        mesh_multicast=False,
        mesh_version=version,
        mesh_gossip_interval=0.05,
        mesh_offline_after=0.2,
        persistence_dir=Path(tempfile.mkdtemp(prefix="paglets-test-")) / name,
    )
