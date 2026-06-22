# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

from paglets.config.startup import load_launch_config, sync_launch_config
from paglets.core.messages import Message
from paglets.examples.compute import (
    PiBatchRequest,
    PiBatchResult,
    PiComputeCoordinatorAgent,
    PiComputeRequest,
    PiComputeState,
    PiPostProcessSummary,
    PiResultDrainRequest,
    chudnovsky_binary_split,
)
from paglets.examples.mesh_info import MeshHostSnapshot
from paglets.runtime.host import Host
from paglets.serialization.codec import dataclass_to_wire
from tests.support import free_port


def test_pi_compute_workers_send_results_and_dispose(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha", launch_config=_launch_config(tmp_path))
    host.start_background()
    try:
        proxy = host.create(PiComputeCoordinatorAgent, PiComputeState())
        summary = _run_compute(
            proxy,
            PiComputeRequest(start=0, digits=8, batch_size=1, timeout=5.0, max_cpu_percent=100.0),
        )

        assert summary["done"] is True
        assert summary["pi"] == "3.14159265"
        assert summary["decimal_digits"] == "14159265"
        _wait_until(lambda: not _pi_workers(host))
    finally:
        host.stop()


def test_pi_compute_local_fallback_target_used_when_mesh_info_times_out(monkeypatch):
    agent = PiComputeCoordinatorAgent(PiComputeState())
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


def test_pi_compute_launches_despite_existing_error(monkeypatch):
    agent = PiComputeCoordinatorAgent(
        PiComputeState(
            request=dataclass_to_wire(PiComputeRequest(batch_size=1)),
            pending_batches=[dataclass_to_wire(PiBatchRequest("terms:0:1", 0, 1))],
            errors={"mesh-info": "timeout"},
        )
    )
    launches: list[PiComputeRequest] = []

    def _capture(request: PiComputeRequest) -> None:
        launches.append(request)

    monkeypatch.setattr(agent, "_launch_available_batches", _capture)

    agent._launch_from_current_state()

    assert launches


def test_pi_compute_summary_exposes_partial_digits():
    request = PiComputeRequest(start=0, digits=8, batch_size=1, timeout=5.0, max_cpu_percent=100.0)
    state = PiComputeState(
        request=dataclass_to_wire(request),
        pending_batches=[dataclass_to_wire(PiBatchRequest("terms:1:1", 1, 1))],
    )
    p, q, t = chudnovsky_binary_split(0, 1)
    state.results["terms:0:1"] = dataclass_to_wire(
        PiBatchResult(
            batch_id="terms:0:1",
            term_start=0,
            term_count=1,
            host_name="alpha",
            host_url="http://127.0.0.1:1",
            status="ok",
            p=str(p),
            q=str(q),
            t=str(t),
        )
    )

    summary = PiComputeCoordinatorAgent(state).summary()

    assert summary.done is False
    assert summary.completed_terms == 1
    assert summary.available_digits == 4
    assert summary.pi == "3.1415"
    assert summary.decimal_digits == "1415"


def test_pi_compute_stream_drain_returns_only_new_digits():
    request = PiComputeRequest(start=0, digits=8, batch_size=1, timeout=5.0, max_cpu_percent=100.0)
    state = PiComputeState(
        request=dataclass_to_wire(request),
        pending_batches=[dataclass_to_wire(PiBatchRequest("terms:1:1", 1, 1))],
    )
    p, q, t = chudnovsky_binary_split(0, 1)
    state.results["terms:0:1"] = dataclass_to_wire(
        PiBatchResult(
            batch_id="terms:0:1",
            term_start=0,
            term_count=1,
            host_name="alpha",
            host_url="http://127.0.0.1:1",
            status="ok",
            p=str(p),
            q=str(q),
            t=str(t),
        )
    )

    reply = PiComputeCoordinatorAgent(state).drain_stream(after_digits=2, wait_timeout=0.0, max_digits=1)

    assert reply["new_decimal_digits"] == "1"
    assert reply["cursor"] == 3
    assert reply["summary"]["available_digits"] == 4
    assert "results" not in reply["summary"]
    assert "decimal_digits" not in reply["summary"]


def test_pi_compute_stream_drain_waits_until_all_available_digits_are_emitted():
    request = PiComputeRequest(start=0, digits=8, batch_size=1, timeout=5.0, max_cpu_percent=100.0)
    state = PiComputeState(
        request=dataclass_to_wire(request),
        done=True,
    )
    p, q, t = chudnovsky_binary_split(0, 1)
    state.results["terms:0:1"] = dataclass_to_wire(
        PiBatchResult(
            batch_id="terms:0:1",
            term_start=0,
            term_count=1,
            host_name="alpha",
            host_url="http://127.0.0.1:1",
            status="ok",
            p=str(p),
            q=str(q),
            t=str(t),
        )
    )

    agent = PiComputeCoordinatorAgent(state)
    agent._context = SimpleNamespace(name="coordinator", address="http://127.0.0.1:8765")
    first = agent.drain_stream(after_digits=2, wait_timeout=0.0, max_digits=1)
    second = agent.drain_stream(after_digits=3, wait_timeout=0.0, max_digits=1)

    assert first["new_decimal_digits"] == "1"
    assert first["done"] is False
    assert second["new_decimal_digits"] == "5"
    assert second["done"] is True


def test_pi_compute_result_drain_returns_compact_new_results_only():
    request = PiComputeRequest(start=0, digits=8, batch_size=1, timeout=5.0, max_cpu_percent=100.0)
    state = PiComputeState(
        request=dataclass_to_wire(request),
        done=True,
    )
    p, q, t = chudnovsky_binary_split(0, 1)
    result = PiBatchResult(
        batch_id="terms:0:1",
        term_start=0,
        term_count=1,
        host_name="alpha",
        host_url="http://127.0.0.1:1",
        status="ok",
        p=str(p),
        q=str(q),
        t=str(t),
    )
    state.results[result.batch_id] = dataclass_to_wire(result)

    agent = PiComputeCoordinatorAgent(state)
    first = agent.drain_results(PiResultDrainRequest(wait_timeout=0.0))
    second = agent.drain_results(PiResultDrainRequest(known_batch_ids=[result.batch_id], wait_timeout=0.0))

    assert first["results"] == [dataclass_to_wire(result)]
    assert first["summary"]["available_digits"] == 4
    assert "decimal_digits" not in first["summary"]
    assert "pi" not in first["summary"]
    assert second["results"] == []


def test_pi_compute_summary_uses_postprocessor_when_available(monkeypatch):
    request = PiComputeRequest(start=0, digits=8, batch_size=1)
    state = PiComputeState(
        request=dataclass_to_wire(request),
        done=True,
        results={
            "terms:0:1": dataclass_to_wire(
                PiBatchResult(
                    batch_id="terms:0:1",
                    term_start=0,
                    term_count=1,
                    host_name="alpha",
                    host_url="http://127.0.0.1:1",
                    status="ok",
                    p=str(chudnovsky_binary_split(0, 1)[0]),
                    q=str(chudnovsky_binary_split(0, 1)[1]),
                    t=str(chudnovsky_binary_split(0, 1)[2]),
                )
            )
        },
    )
    agent = PiComputeCoordinatorAgent(state)
    agent._context = SimpleNamespace()

    def _pp_summary():
        return PiPostProcessSummary(
            request=dataclass_to_wire(request),
            completed_terms=1,
            available_digits=4,
            done=True,
        )

    def _pp_format(req):
        return {"pi": "3.1415", "decimal_digits": "1415"}

    monkeypatch.setattr(agent, "_postprocessor_summary", lambda: _pp_summary())
    monkeypatch.setattr(agent, "_postprocess_format", lambda req: _pp_format(req))

    summary = agent.summary()
    assert summary.pi == "3.1415"
    assert summary.decimal_digits == "1415"
    assert summary.completed_terms == 1


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
