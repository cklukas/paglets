# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
import time

from paglets.runtime.host import Host
from paglets.core.messages import Message
from paglets.core.agent import Paglet, PagletContext, PagletState
from paglets.core.runtime_values import ServiceScope
from paglets.examples.system_info import SERVER_INFO, GET_SUMMARY
from paglets.examples.mesh_info import MESH_INFO, GET_SNAPSHOT
from paglets.core.runtime_values import LaunchConfigSyncAction, ResidentLifecycle
from paglets.config.startup import load_launch_config, sync_launch_config
from tests.test_paglets_core import free_port


@dataclass
class LeaseClientState(PagletState):
    acquired: bool = False


class LeaseClientAgent(Paglet[LeaseClientState]):
    State = LeaseClientState

    def handle_message(self, message: Message):
        if message.kind == "lease":
            lease = self.lease_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.MESH, ttl=60.0)
            self.state.acquired = True
            return lease.handle.call(GET_SUMMARY).service_agent_id
        return self.not_handled()


def test_launch_config_sync_copies_bundled_config_on_first_start(tmp_path):
    path = tmp_path / "launch.toml"

    result = sync_launch_config(path, interactive=False)
    config = load_launch_config(path)

    assert result.action is LaunchConfigSyncAction.COPIED
    assert path.exists()
    assert config.demo_config_id == "paglets-default-launch"
    assert config.demo_config_version == "4"
    assert len(config.startup_agents) == 0
    assert len(config.resident_services) == 2
    assert config.resident_services[0].class_name == "paglets.examples.system_info.agent:ServerInfoAgent"
    assert config.resident_services[0].lifecycle is ResidentLifecycle.LAZY
    assert config.resident_services[1].class_name == "paglets.examples.mesh_info.agent:MeshInfoAgent"
    assert config.resident_services[1].lifecycle is ResidentLifecycle.EAGER


def test_launch_config_sync_updates_with_backup_when_accepted(tmp_path):
    path = tmp_path / "launch.toml"
    path.write_text(
        """
[launch]
demo_config_id = "paglets-default-launch"
demo_config_version = "0"

[[startup_agents]]
use = "old-service"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = sync_launch_config(path, yes=True, interactive=False)

    assert result.action is LaunchConfigSyncAction.UPDATED
    assert result.backup_path is not None
    assert result.backup_path.exists()
    assert "old-service" in result.backup_path.read_text(encoding="utf-8")
    assert load_launch_config(path).demo_config_version == "4"


def test_launch_config_sync_warns_without_replacing_noninteractive(tmp_path, capsys):
    path = tmp_path / "launch.toml"
    path.write_text(
        """
[launch]
demo_config_id = "paglets-default-launch"
demo_config_version = "0"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = sync_launch_config(path, interactive=False)

    assert result.action is LaunchConfigSyncAction.UPDATE_AVAILABLE
    assert load_launch_config(path).demo_config_version == "0"
    assert "bundled launch config" in capsys.readouterr().err


def test_launch_config_sync_can_be_disabled_from_config(tmp_path):
    path = tmp_path / "launch.toml"
    path.write_text(
        """
[launch]
demo_config_id = "paglets-default-launch"
demo_config_version = "0"
sync_demo_config = false
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = sync_launch_config(path, yes=True, interactive=False)

    assert result.action is LaunchConfigSyncAction.SKIPPED
    assert load_launch_config(path).demo_config_version == "0"


def test_launch_config_declares_lazy_server_info_and_starts_on_first_call(tmp_path):
    path = _server_info_resident_config(tmp_path, lifecycle="lazy", idle_timeout=30.0)
    launch_config = load_launch_config(path)
    persistence_dir = tmp_path / "alpha-persist"

    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
        launch_config=launch_config,
    )
    host.start_background()
    try:
        assert host.get_proxy("service.server-info") is None
        handle = PagletContext(host).require_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.MESH)
        assert handle.record.proxy.agent_id == "service.server-info"
        assert len(host.list_agents()) == 0
        assert handle.call(GET_SUMMARY).service_agent_id == "service.server-info"
        assert host.get_proxy("service.server-info") is not None
        assert len(host.list_agents()) == 1
    finally:
        host.stop(deactivate_active=True)

    restarted = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
        launch_config=launch_config,
    )
    restarted.start_background()
    try:
        assert restarted.get_proxy("service.server-info") is None
        assert len(restarted.list_agents()) == 0
        handle = PagletContext(restarted).require_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.MESH)
        assert handle.call(GET_SUMMARY).service_agent_id == "service.server-info"
    finally:
        restarted.stop()


def test_lazy_resident_service_idle_deactivates_but_stays_discoverable(tmp_path):
    path = _server_info_resident_config(tmp_path, lifecycle="lazy", idle_timeout=0.1)
    launch_config = load_launch_config(path)
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha-persist",
        launch_config=launch_config,
    )
    host.start_background()
    try:
        context = PagletContext(host)
        handle = context.require_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.MESH)
        assert handle.call(GET_SUMMARY).service_agent_id == "service.server-info"
        assert host.get_proxy("service.server-info") is not None

        host._resident_maintenance(time.time() + 1.0)

        assert host.get_proxy("service.server-info") is None
        assert context.require_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.MESH).record.proxy.agent_id == "service.server-info"
    finally:
        host.stop()


def test_service_lease_keeps_lazy_resident_service_active_until_release(tmp_path):
    path = _server_info_resident_config(tmp_path, lifecycle="lazy", idle_timeout=0.1)
    launch_config = load_launch_config(path)
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha-persist",
        launch_config=launch_config,
    )
    host.start_background()
    try:
        context = PagletContext(host)
        lease = context.lease_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.MESH, ttl=60.0)
        with lease as handle:
            assert handle.call(GET_SUMMARY).service_agent_id == "service.server-info"
        assert host.get_proxy("service.server-info") is not None

        active_lease = context.lease_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.MESH, ttl=60.0)
        host._resident_maintenance(time.time() + 1.0)
        assert host.get_proxy("service.server-info") is not None

        active_lease.release()
        host._resident_maintenance(time.time() + 1.0)
        assert host.get_proxy("service.server-info") is None
    finally:
        host.stop()


def test_paglet_owned_service_lease_is_released_by_resource_cleanup(tmp_path):
    path = _server_info_resident_config(tmp_path, lifecycle="lazy", idle_timeout=0.1)
    launch_config = load_launch_config(path)
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha-persist",
        launch_config=launch_config,
    )
    host.start_background()
    try:
        client = host.create(LeaseClientAgent, LeaseClientState())
        assert client.send(Message("lease")) == "service.server-info"
        assert host.get_proxy("service.server-info") is not None

        host._resident_maintenance(time.time() + 1.0)
        assert host.get_proxy("service.server-info") is not None

        client.dispose()
        host._resident_maintenance(time.time() + 1.0)
        assert host.get_proxy("service.server-info") is None
    finally:
        host.stop()


def test_eager_resident_service_starts_immediately(tmp_path):
    path = _server_info_resident_config(tmp_path, lifecycle="eager", idle_timeout=0.1)
    launch_config = load_launch_config(path)
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha-persist",
        launch_config=launch_config,
    )
    host.start_background()
    try:
        assert host.get_proxy("service.server-info") is not None
        assert PagletContext(host).require_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.MESH).call(GET_SUMMARY)
    finally:
        host.stop()


def test_default_launch_config_starts_eager_mesh_info(tmp_path):
    path = tmp_path / "launch.toml"
    sync_launch_config(path, interactive=False)
    launch_config = load_launch_config(path)
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha-persist",
        launch_config=launch_config,
    )
    host.start_background()
    try:
        assert host.get_proxy("service.mesh-info") is not None
        snapshot = PagletContext(host).require_contract(MESH_INFO, operation=GET_SNAPSHOT, scope=ServiceScope.MESH).call(
            GET_SNAPSHOT
        )
        assert snapshot.snapshot is not None
        assert snapshot.snapshot.host_name == "alpha"
    finally:
        host.stop()


def test_concurrent_first_calls_create_one_lazy_resident_service(tmp_path):
    path = _server_info_resident_config(tmp_path, lifecycle="lazy", idle_timeout=30.0)
    launch_config = load_launch_config(path)
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha-persist",
        launch_config=launch_config,
    )
    host.start_background()
    try:
        context = PagletContext(host)

        def call_summary():
            return context.require_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.MESH).call(GET_SUMMARY).service_agent_id

        with ThreadPoolExecutor(max_workers=4) as executor:
            replies = list(executor.map(lambda _: call_summary(), range(4)))

        assert replies == ["service.server-info"] * 4
        assert len(host.list_agents()) == 1
        create_events = [
            event
            for event in host.list_events(limit=100)
            if event.kind == "resident-service-create" and event.agent_id == "service.server-info"
        ]
        assert len(create_events) == 1
    finally:
        host.stop()


def _server_info_resident_config(tmp_path: Path, *, lifecycle: str, idle_timeout: float) -> Path:
    path = tmp_path / f"launch-{lifecycle}-{idle_timeout}.toml"
    path.write_text(
        f"""
[launch]
demo_config_id = "test"
demo_config_version = "1"

[[resident_services]]
class = "paglets.examples.system_info.agent:ServerInfoAgent"
enabled = true
agent_id = "service.server-info"
singleton = true
lifecycle = "{lifecycle}"
scope = "mesh"
idle_timeout = {idle_timeout}
state = {{ service_scope = "mesh" }}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path
