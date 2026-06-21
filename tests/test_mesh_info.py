# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path
import time

from paglets import Host, PagletContext, ServiceScope
from paglets.examples.mesh_info import (
    GET_LANDSCAPE,
    GET_SNAPSHOT,
    MESH_INFO,
    SELECT_TARGETS,
    SYNC_MESH_INFO,
    LandscapeRequest,
    MeshHostSnapshot,
    MeshInfoSyncRequest,
    SnapshotRequest,
    TargetSelectionRequest,
)
from paglets.startup import load_launch_config, sync_launch_config
from tests.test_paglets_core import free_port


def test_mesh_info_eager_service_starts_and_samples_server_info(tmp_path: Path):
    launch_config = _launch_config(tmp_path)
    host = _host("alpha", tmp_path / "alpha", launch_config=launch_config)
    host.start_background()
    try:
        assert host.get_proxy("service.mesh-info") is not None
        context = PagletContext(host)
        handle = context.require_contract(MESH_INFO, operation=GET_SNAPSHOT, scope=ServiceScope.MESH)

        reply = handle.call(GET_SNAPSHOT, SnapshotRequest(force=True))

        assert reply.snapshot is not None
        assert reply.snapshot.host_name == "alpha"
        assert reply.snapshot.memory_total_bytes > 0
        assert reply.snapshot.work_path.endswith("service.mesh-info")
        assert reply.snapshot.work_free_bytes > 0
    finally:
        host.stop()


def test_mesh_info_syncs_landscape_between_hosts(tmp_path: Path):
    launch_config = _launch_config(tmp_path)
    alpha = _host("alpha", tmp_path / "alpha", launch_config=launch_config, mesh_version="mesh-info-test")
    beta = _host(
        "beta",
        tmp_path / "beta",
        launch_config=launch_config,
        mesh_version="mesh-info-test",
        peers=[alpha.address],
    )
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        handle = PagletContext(alpha).require_contract(MESH_INFO, operation=GET_LANDSCAPE, scope=ServiceScope.MESH)

        _wait_until(
            lambda: {"alpha", "beta"}
            <= {snapshot.host_name for snapshot in handle.call(GET_LANDSCAPE, LandscapeRequest()).hosts}
        )
    finally:
        beta.stop()
        alpha.stop()


def test_mesh_info_target_selection_filters_stale_and_overloaded_hosts(tmp_path: Path):
    launch_config = _launch_config(tmp_path)
    host = _host("alpha", tmp_path / "alpha", launch_config=launch_config)
    host.start_background()
    try:
        now = time.time()
        handle = PagletContext(host).require_contract(MESH_INFO, operation=SELECT_TARGETS, scope=ServiceScope.MESH)
        sync = PagletContext(host).require_contract(MESH_INFO, operation=SYNC_MESH_INFO, scope=ServiceScope.MESH)
        sync.call(
            SYNC_MESH_INFO,
            MeshInfoSyncRequest(
                snapshots=[
                    MeshHostSnapshot(
                        host_name="stale",
                        host_url="http://stale",
                        code_version="test",
                        observed_at=now - 1000,
                        cpu_count_logical=4,
                        work_free_bytes=10**9,
                        memory_available_bytes=10**9,
                    ),
                    MeshHostSnapshot(
                        host_name="busy",
                        host_url="http://busy",
                        code_version="test",
                        observed_at=now,
                        cpu_count_logical=4,
                        load_per_cpu=99.0,
                        work_free_bytes=10**9,
                        memory_available_bytes=10**9,
                    ),
                ]
            ),
        )

        reply = handle.call(
            SELECT_TARGETS,
            TargetSelectionRequest(limit=10, max_age_seconds=20.0, max_load_per_cpu=1.0, max_cpu_percent=100.0),
        )

        names = {target.snapshot.host_name for target in reply.targets}
        assert "alpha" in names
        assert "stale" not in names
        assert reply.rejected["busy"].startswith("load per cpu")
    finally:
        host.stop()


def _launch_config(tmp_path: Path):
    path = tmp_path / "launch.toml"
    sync_launch_config(path, interactive=False)
    return load_launch_config(path)


def _host(
    name: str,
    persistence_dir: Path,
    *,
    launch_config,
    mesh_version: str = "mesh-info-test",
    peers: list[str] | None = None,
) -> Host:
    return Host(
        name,
        host="127.0.0.1",
        port=free_port(),
        peers=peers or [],
        mesh_version=mesh_version,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
        launch_config=launch_config,
    )


def _wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError("condition was not met before timeout")
