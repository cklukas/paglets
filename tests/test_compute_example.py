# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
from pathlib import Path

from paglets import Host, Message
from paglets.admin import ServerRef, save_server_config
from paglets.examples.compute import (
    PiBatchRequest,
    PiBatchResult,
    PiComputeCoordinatorAgent,
    PiComputeRequest,
    PiComputeState,
    pi_decimal,
    pi_decimal_digits,
)
from paglets.examples.compute.cli import main as pi_main
from paglets.serde import dataclass_to_wire
from paglets.startup import load_launch_config, sync_launch_config
from tests.test_paglets_core import free_port


def test_pi_decimal_digits_are_deterministic():
    assert pi_decimal(0, 16) == "3.1415926535897932"
    assert pi_decimal_digits(0, 16) == "1415926535897932"


def test_pi_compute_workers_send_results_and_dispose(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha", launch_config=_launch_config(tmp_path))
    host.start_background()
    try:
        proxy = host.create(PiComputeCoordinatorAgent, PiComputeState())
        summary = proxy.send(
            Message(
                "start",
                {
                    "request": dataclass_to_wire(
                        PiComputeRequest(start=0, digits=8, batch_size=1, timeout=5.0, max_cpu_percent=100.0)
                    )
                },
            )
        )

        assert summary["done"] is True
        assert summary["pi"] == "3.14159265"
        assert summary["decimal_digits"] == "14159265"
        assert not [
            agent
            for agent in host.list_agents()
            if agent["class_name"] == "paglets.examples.compute.agent:PiBatchWorkerAgent"
        ]
    finally:
        host.stop()


def test_skipped_batch_results_are_requeued(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(PiComputeCoordinatorAgent, PiComputeState())
        state = host.get_state(proxy.agent_id, PiComputeState)
        batch = PiBatchRequest("terms:0:1", 0, 1)
        state.in_flight[batch.batch_id] = {
            "agent_id": "worker",
            "host_name": "alpha",
            "host_url": host.address,
            "batch": dataclass_to_wire(batch),
        }

        proxy.send(
            Message(
                "batch_result",
                dataclass_to_wire(
                    PiBatchResult(
                        batch_id=batch.batch_id,
                        term_start=batch.term_start,
                        term_count=batch.term_count,
                        host_name="alpha",
                        host_url=host.address,
                        status="skipped",
                        error="host busy",
                    )
                ),
            )
        )

        assert state.skipped_count == 1
        assert state.pending_batches == [dataclass_to_wire(batch)]
        assert state.in_flight == {}
    finally:
        host.stop()


def test_pi_compute_cli_json_output(tmp_path: Path, capsys):
    launch_config = _launch_config(tmp_path)
    host = _host("alpha", tmp_path / "alpha", launch_config=launch_config)
    host.start_background()
    try:
        config_path = tmp_path / "servers.json"
        save_server_config([ServerRef("alpha", host.address)], config_path)

        result = pi_main(
            [
                "--config",
                str(config_path),
                "--timeout",
                "5",
                "--digits",
                "4",
                "--batch-size",
                "1",
                "--max-cpu-percent",
                "100",
                "--json",
            ]
        )

        assert result == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["pi"] == "3.1415"
        assert payload["decimal_digits"] == "1415"
        assert payload["done"] is True
    finally:
        host.stop()


def _launch_config(tmp_path: Path):
    path = tmp_path / "launch.toml"
    sync_launch_config(path, interactive=False)
    return load_launch_config(path)


def _host(name: str, persistence_dir: Path, *, launch_config=None) -> Host:
    return Host(
        name,
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
        launch_config=launch_config,
    )
