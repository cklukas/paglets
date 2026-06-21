# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path

from paglets import Host, PagletContext
from paglets.examples.system_info import SERVER_INFO, GET_SUMMARY
from paglets.startup import load_launch_config, sync_launch_config
from tests.test_paglets_core import free_port


def test_launch_config_sync_copies_bundled_config_on_first_start(tmp_path):
    path = tmp_path / "launch.toml"

    result = sync_launch_config(path, interactive=False)
    config = load_launch_config(path)

    assert result.action == "copied"
    assert path.exists()
    assert config.demo_config_id == "paglets-default-launch"
    assert config.demo_config_version == "2"
    assert len(config.startup_agents) == 1
    assert config.startup_agents[0].class_name == "paglets.examples.system_info.agent:ServerInfoAgent"


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

    assert result.action == "updated"
    assert result.backup_path is not None
    assert result.backup_path.exists()
    assert "old-service" in result.backup_path.read_text(encoding="utf-8")
    assert load_launch_config(path).demo_config_version == "2"


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

    assert result.action == "update-available"
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

    assert result.action == "skipped"
    assert load_launch_config(path).demo_config_version == "0"


def test_launch_config_autostarts_server_info_singleton(tmp_path):
    path = tmp_path / "launch.toml"
    sync_launch_config(path, interactive=False)
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
        assert host.get_proxy("service.server-info") is not None
        handle = PagletContext(host).require_contract(SERVER_INFO, operation=GET_SUMMARY, scope="mesh")
        assert handle.call(GET_SUMMARY).service_agent_id == "service.server-info"
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
        assert restarted.get_proxy("service.server-info") is not None
        assert len(restarted.list_agents()) == 1
    finally:
        restarted.stop()
