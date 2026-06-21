# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
from pathlib import Path

import psutil

from paglets import Host, PagletContext
from paglets.admin import ServerRef, save_server_config
from paglets.examples.system_info import (
    GET_DISK,
    GET_LOAD,
    LIST_PROCESSES,
    SERVER_INFO,
    DiskRequest,
    LoadRequest,
    ProcessListRequest,
)
from paglets.examples.system_info.cli import main as sysinfo_main
from paglets.startup import load_launch_config, sync_launch_config
from tests.test_paglets_core import free_port


def test_server_info_contract_returns_load_disk_and_process_data(tmp_path):
    launch_config_path = tmp_path / "launch.toml"
    sync_launch_config(launch_config_path, interactive=False)
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
        launch_config=load_launch_config(launch_config_path),
    )
    host.start_background()
    try:
        context = PagletContext(host)
        load = context.require_contract(SERVER_INFO, operation=GET_LOAD, scope="mesh").call(
            GET_LOAD,
            LoadRequest(include_gpu=True),
        )
        assert load.host_name == "alpha"
        assert load.memory_total_bytes > 0
        assert isinstance(load.gpu_available, bool)

        disk = context.require_contract(SERVER_INFO, operation=GET_DISK, scope="mesh").call(
            GET_DISK,
            DiskRequest(paths=[str(tmp_path)], all_volumes=False),
        )
        assert disk.volumes
        assert disk.volumes[0].total_bytes > 0

        process_name = psutil.Process().name()
        processes = context.require_contract(SERVER_INFO, operation=LIST_PROCESSES, scope="mesh").call(
            LIST_PROCESSES,
            ProcessListRequest(query=process_name, limit=10),
        )
        assert any(process.pid == psutil.Process().pid for process in processes.processes)
    finally:
        host.stop()


def test_paglets_sysinfo_df_uses_default_server_config_and_collects_mesh(tmp_path, capsys):
    launch_config_path = tmp_path / "launch.toml"
    sync_launch_config(launch_config_path, interactive=False)
    launch_config = load_launch_config(launch_config_path)
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh_version="sysinfo-cli-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
        launch_config=launch_config,
    )
    beta = Host(
        "beta",
        host="127.0.0.1",
        port=free_port(),
        peers=[alpha.address],
        mesh_version="sysinfo-cli-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "beta",
        launch_config=launch_config,
    )
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        config_path = tmp_path / "servers.json"
        save_server_config([ServerRef("alpha", alpha.address)], config_path)

        result = sysinfo_main(["--config", str(config_path), "--timeout", "3", "--json", "df", str(tmp_path)])

        assert result == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["operation"] == "df"
        assert set(payload["results"]) == {"alpha", "beta"}
        assert payload["errors"] == {}
    finally:
        beta.stop()
        alpha.stop()
