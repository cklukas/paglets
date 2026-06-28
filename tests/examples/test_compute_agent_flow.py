# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

from paglets.config.startup import load_launch_config, sync_launch_config
from paglets.core.messages import Message
from paglets.examples.compute import (
    PiBatchResult,
    PiComputeRequest,
    PiJobPaglet,
    PiJobStartRequest,
    PiJobState,
    chudnovsky_binary_split,
)
from paglets.runtime.host import Host
from paglets.serialization.codec import dataclass_to_wire
from tests.support import free_port


def test_pi_job_workers_send_results_and_write_output(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha", launch_config=_launch_config(tmp_path))
    host.start_background()
    output_path = tmp_path / "pi.txt"
    try:
        proxy = host.create(PiJobPaglet, PiJobState())
        reply = proxy.send(
            Message(
                "pi.start",
                dataclass_to_wire(
                    PiJobStartRequest(
                        request=dataclass_to_wire(PiComputeRequest(start=0, digits=8, batch_size=1)),
                        job_id="pi-test",
                        output_path=str(output_path),
                    )
                ),
            )
        )

        assert reply["accepted"] is True
        assert reply["output_path"] == str(output_path)
        _wait_until(lambda: output_path.read_text(encoding="utf-8") == "3.14159265")
        state = host.get_state(proxy.agent_id, PiJobState)
        assert state.done is True
        assert state.failed is False
        assert state.output_cursor == 8
        _wait_until(lambda: not _pi_workers(host))
    finally:
        host.stop()


def test_pi_job_local_fallback_target_used_when_mesh_info_is_unavailable(monkeypatch):
    agent = PiJobPaglet(PiJobState())
    host = SimpleNamespace(
        list_agents=lambda active=True, inactive=False: [
            {"agent_id": "a", "active": True},
            {"agent_id": "b", "active": False},
        ]
    )
    fake_context = SimpleNamespace(
        name="alpha",
        address="http://127.0.0.1:9999",
        host=host,
        work_dir=lambda create=True: Path("/tmp/paglets-compute-fallback-test"),
    )
    agent._context = fake_context

    def _raise_contract(*_, **__) -> None:
        raise RuntimeError("could not reach mesh-info")

    monkeypatch.setattr(agent, "require_contract", _raise_contract)

    targets = agent._select_targets(PiComputeRequest(max_in_flight=1))

    assert len(targets) == 1
    assert targets[0].snapshot.host_name == "alpha"
    assert targets[0].snapshot.host_url == "http://127.0.0.1:9999"


def test_pi_job_out_of_order_results_emit_only_contiguous_chunks(tmp_path: Path):
    output_path = tmp_path / "pi.txt"
    request = PiComputeRequest(start=0, digits=8, batch_size=1)
    state = PiJobState(
        job_id="pi-test",
        request=dataclass_to_wire(request),
        output_path=str(output_path),
        pending_batches=[],
        in_flight={
            "terms:0:1": {},
            "terms:1:1": {},
        },
    )
    output_path.write_text("3.", encoding="utf-8")
    agent = PiJobPaglet(state)
    agent._context = SimpleNamespace(name="alpha", address="http://127.0.0.1:1")
    term0 = chudnovsky_binary_split(0, 1)
    term1 = chudnovsky_binary_split(1, 2)

    agent.record_batch_result(_result("terms:1:1", 1, term1))

    assert output_path.read_text(encoding="utf-8") == "3."
    assert state.output_cursor == 0

    agent.record_batch_result(_result("terms:0:1", 0, term0))

    assert output_path.read_text(encoding="utf-8") == "3.14159265"
    assert state.output_cursor == 8
    assert state.done is True


def test_pi_job_start_after_decimal_writes_only_requested_range(tmp_path: Path):
    output_path = tmp_path / "pi-range.txt"
    request = PiComputeRequest(start=2, digits=4, batch_size=1)
    state = PiJobState(
        job_id="pi-test",
        request=dataclass_to_wire(request),
        output_path=str(output_path),
        pending_batches=[],
        in_flight={"terms:0:1": {}, "terms:1:1": {}},
    )
    output_path.write_text("", encoding="utf-8")
    agent = PiJobPaglet(state)
    agent._context = SimpleNamespace(name="alpha", address="http://127.0.0.1:1")
    term0 = chudnovsky_binary_split(0, 1)
    term1 = chudnovsky_binary_split(1, 2)

    agent.record_batch_result(_result("terms:0:1", 0, term0))
    agent.record_batch_result(_result("terms:1:1", 1, term1))

    assert output_path.read_text(encoding="utf-8") == "1592"


def test_pi_job_failed_worker_stops_job_and_preserves_partial_file(tmp_path: Path):
    output_path = tmp_path / "pi.txt"
    output_path.write_text("3.1415", encoding="utf-8")
    state = PiJobState(
        job_id="pi-test",
        request=dataclass_to_wire(PiComputeRequest(start=0, digits=8, batch_size=1)),
        output_path=str(output_path),
        pending_batches=[],
        in_flight={"terms:1:1": {}},
    )
    agent = PiJobPaglet(state)
    agent._context = SimpleNamespace(name="alpha", address="http://127.0.0.1:1")

    agent.record_batch_failure(
        PiBatchResult(
            batch_id="terms:1:1",
            term_start=1,
            term_count=1,
            host_name="alpha",
            host_url="http://127.0.0.1:1",
            status="error",
            error="worker crashed",
        )
    )

    assert state.done is True
    assert state.failed is True
    assert state.errors == {"terms:1:1": "worker crashed"}
    assert output_path.read_text(encoding="utf-8") == "3.1415"


def _result(batch_id: str, term_start: int, part: tuple[int, int, int]) -> PiBatchResult:
    return PiBatchResult(
        batch_id=batch_id,
        term_start=term_start,
        term_count=1,
        host_name="alpha",
        host_url="http://127.0.0.1:1",
        status="ok",
        p=hex(part[0]),
        q=hex(part[1]),
        t=hex(part[2]),
    )


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


def _pi_workers(host: Host) -> list[dict]:
    return [
        agent
        for agent in host.list_agents()
        if agent["class_name"] == "paglets.examples.compute.agent:PiBatchWorkerAgent"
    ]


def _wait_until(predicate, *, timeout: float = 3.0, interval: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()
