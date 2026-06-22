# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
import time
from pathlib import Path

from paglets.config.startup import load_launch_config, sync_launch_config
from paglets.core.messages import Message
from paglets.examples.compute import (
    PiComputeRequest,
)
from paglets.examples.compute.cli import _parser as pi_parser
from paglets.examples.compute.cli import main as pi_main
from paglets.examples.mesh_info import MeshHostSnapshot
from paglets.remote.admin import ServerRef
from paglets.runtime.host import Host
from paglets.serialization.codec import dataclass_to_wire
from tests.support import free_port


def test_pi_compute_cli_has_separate_request_timeout():
    args = pi_parser().parse_args(["--digits", "8", "--stream-chunk-size", "123"])

    assert args.timeout == 0.0
    assert args.request_timeout == 300.0
    assert args.stream_chunk_size == 123


def test_pi_compute_cli_json_output(tmp_path: Path, capsys, monkeypatch):
    launch_config = _launch_config(tmp_path)
    host = _host("alpha", tmp_path / "alpha", launch_config=launch_config)
    host.start_background()
    try:
        monkeypatch.setattr(
            "paglets.examples.compute.cli._select_entry_server",
            lambda *, entry_name, client: ServerRef("alpha", host.address),
        )

        result = pi_main(
            [
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


def test_pi_compute_cli_streams_text_output(tmp_path: Path, capsys, monkeypatch):
    launch_config = _launch_config(tmp_path)
    host = _host("alpha", tmp_path / "alpha", launch_config=launch_config)
    host.start_background()
    try:
        monkeypatch.setattr(
            "paglets.examples.compute.cli._select_entry_server",
            lambda *, entry_name, client: ServerRef("alpha", host.address),
        )

        result = pi_main(
            [
                "--timeout",
                "5",
                "--digits",
                "8",
                "--batch-size",
                "1",
                "--max-cpu-percent",
                "100",
            ]
        )

        assert result == 0
        output = capsys.readouterr().out
        assert output.splitlines()[0] == "3.14159265"
        assert "3.14159265" in output
    finally:
        host.stop()


def test_pi_compute_cli_streams_diagnostics(tmp_path: Path, capsys, monkeypatch):
    launch_config = _launch_config(tmp_path)
    host = _host("alpha", tmp_path / "alpha", launch_config=launch_config)
    host.start_background()
    try:
        monkeypatch.setattr(
            "paglets.examples.compute.cli._select_entry_server",
            lambda *, entry_name, client: ServerRef("alpha", host.address),
        )

        result = pi_main(
            [
                "--timeout",
                "5",
                "--digits",
                "4",
                "--batch-size",
                "1",
                "--max-cpu-percent",
                "100",
            ]
        )

        assert result == 0
        captured = capsys.readouterr()
        assert "pi compute diagnostic: all batches received" in captured.err
        assert "pi compute diagnostic: digits printed=4" in captured.err
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


def _snapshot(host: Host, *, cpu_count: int, load: float, cpu_percent: float = 25.0) -> MeshHostSnapshot:
    return MeshHostSnapshot(
        host_name=host.name,
        host_url=host.address.rstrip("/"),
        code_version="test",
        observed_at=time.time(),
        cpu_count_logical=cpu_count,
        cpu_percent=cpu_percent,
        load_average=[load],
        load_per_cpu=load / cpu_count,
        memory_available_bytes=1024**3,
        work_free_bytes=1024**3,
    )


def _pi_workers(host: Host) -> list[dict]:
    return [
        agent
        for agent in host.list_agents()
        if agent["class_name"] == "paglets.examples.compute.agent:PiBatchWorkerAgent"
    ]


def _run_compute(proxy, request: PiComputeRequest) -> dict:
    proxy.send(Message("start_async", {"request": dataclass_to_wire(request)}))
    summary: dict = {}
    while True:
        reply = proxy.send(Message("drain", {"after_digits": 0, "wait_timeout": 0.5}))
        summary = dict(reply.get("summary") or {})
        if reply.get("done"):
            return summary


def _wait_until(predicate, *, timeout: float = 3.0, interval: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()
