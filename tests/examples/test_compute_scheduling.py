# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import time
from pathlib import Path
from types import SimpleNamespace

from paglets.config.startup import load_launch_config, sync_launch_config
from paglets.core.errors import InvalidAgentError
from paglets.core.messages import Message
from paglets.examples.compute import (
    PI_BATCH_FAILED,
    PiBatchRequest,
    PiBatchResult,
    PiBatchWorkerAgent,
    PiBatchWorkerState,
    PiComputeRequest,
    PiJobPaglet,
    PiJobState,
)
from paglets.examples.compute.agent import (
    _host_worker_capacity_by_url,
    _host_worker_slots,
    _is_missing_worker_error,
    _normalize_request,
)
from paglets.runtime.host import Host
from paglets.serialization.codec import dataclass_to_wire
from paglets.system.mesh_info import MeshHostSnapshot, TargetCandidate
from tests.support import free_port


def test_pi_compute_timeout_default_is_unbounded():
    assert PiComputeRequest().timeout == 0.0
    assert _normalize_request(PiComputeRequest(timeout=-1.0)).timeout == 0.0


def test_self_disposed_worker_cleanup_errors_are_ignored():
    assert _is_missing_worker_error(InvalidAgentError("No paglet 'pi-worker-1' on alpha"))
    assert not _is_missing_worker_error(InvalidAgentError("Paglet 'pi-worker-1' crashed on alpha"))


def test_pi_worker_self_disposes_when_parent_report_fails(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha", launch_config=_launch_config(tmp_path))
    host.start_background()
    try:
        host.create(
            PiBatchWorkerAgent,
            PiBatchWorkerState(
                batch=dataclass_to_wire(PiBatchRequest("terms:0:1", 0, 1)),
                parent_host_url="http://127.0.0.1:1",
                parent_agent_id="missing-parent",
            ),
        )

        _wait_until(lambda: not _pi_workers(host))
    finally:
        host.stop()


def test_pi_worker_launch_specs_run_in_parallel(monkeypatch):
    agent = PiJobPaglet(PiJobState())
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    active = 0
    max_active = 0

    def fake_post_json(_url, _payload):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            barrier.wait(timeout=1.0)
        finally:
            time.sleep(0.02)
            with lock:
                active -= 1

    agent._context = SimpleNamespace(host=SimpleNamespace(client=SimpleNamespace(post_json=fake_post_json)))
    specs = [
        {
            "host_url": "http://127.0.0.1:1",
            "host_name": "alpha",
            "worker_id": f"worker-{index}",
            "worker_state": PiBatchWorkerState(),
            "batch_id": f"terms:{index}:1",
        }
        for index in range(2)
    ]

    agent._launch_worker_specs(specs)

    assert max_active == 2


def test_pi_compute_counts_free_host_slots(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    host.start_background()
    try:
        request = PiComputeRequest(
            start=0,
            digits=80,
            batch_size=1,
            max_load_per_cpu=1.0,
            max_cpu_percent=100.0,
            max_workers_per_host=3,
        )
        snapshot = _snapshot(host, cpu_count=8, load=3.1)
        assert _host_worker_slots(snapshot, request) == 3
    finally:
        host.stop()


def test_pi_compute_free_slots_are_additional_to_existing_in_flight():
    request = PiComputeRequest(max_workers_per_host=0)

    capacity = _host_worker_capacity_by_url(
        {"http://alpha": (5, 16)},
        {"http://alpha": 7},
        request,
    )

    assert capacity["http://alpha"] == 5


def test_pi_compute_per_host_cap_limits_total_host_capacity():
    request = PiComputeRequest(max_workers_per_host=8)

    capacity = _host_worker_capacity_by_url(
        {"http://alpha": (5, 16)},
        {"http://alpha": 7},
        request,
    )

    assert capacity["http://alpha"] == 1


def test_pi_compute_cpu_count_is_default_host_cap_when_workers_unset():
    request = PiComputeRequest(max_workers_per_host=0)

    capacity = _host_worker_capacity_by_url(
        {"http://alpha": (5, 4)},
        {"http://alpha": 7},
        request,
    )

    assert capacity["http://alpha"] == 0


def test_load_values_do_not_fully_block_slots(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    host.start_background()
    try:
        request = PiComputeRequest(start=0, digits=32, batch_size=1, max_load_per_cpu=0.5, max_cpu_percent=20.0)
        snapshot = _snapshot(host, cpu_count=4, load=4.0, cpu_percent=100.0)
        assert _host_worker_slots(snapshot, request) == 1
    finally:
        host.stop()


def test_pi_compute_slots_are_cpu_capped():
    snapshot = MeshHostSnapshot(
        host_name="alpha",
        host_url="http://alpha:1",
        code_version="test",
        observed_at=time.time(),
        cpu_count_logical=4,
        cpu_percent=10.0,
        load_average=[0.0],
        load_per_cpu=0.0,
        memory_available_bytes=1024**3,
        work_free_bytes=1024**3,
    )
    request = PiComputeRequest(max_load_per_cpu=10.0)

    assert _host_worker_slots(snapshot, request) == 4


def test_pi_compute_max_in_flight_is_capped_by_host_capacity(monkeypatch):
    request = PiComputeRequest(
        batch_size=1,
        max_in_flight=100,
        max_load_per_cpu=1.0,
        max_cpu_percent=100.0,
    )
    state = PiJobState(
        request=dataclass_to_wire(request),
        pending_batches=[dataclass_to_wire(PiBatchRequest(f"terms:{index}:1", index, 1)) for index in range(100)],
        output_path="/tmp/pi.txt",
    )
    agent = PiJobPaglet(state)
    agent._context = SimpleNamespace(
        name="coordinator",
        address="http://127.0.0.1:8765",
        host=SimpleNamespace(client=SimpleNamespace(post_json=lambda _url, _payload: None)),
    )
    targets = [
        TargetCandidate(
            snapshot=MeshHostSnapshot(
                host_name=f"host-{index}",
                host_url=f"http://{index}.local:8765",
                code_version="test",
                observed_at=time.time(),
                cpu_count_logical=4,
                cpu_percent=5.0,
                load_average=[0.0],
                load_per_cpu=0.0,
                memory_available_bytes=1024**3,
                work_free_bytes=1024**3,
            ),
            score=0.0,
            reasons=["eligible"],
        )
        for index in range(2)
    ]

    monkeypatch.setattr(agent, "_select_targets", lambda request: targets)

    agent._launch_available_batches(request)

    with agent.locked_state() as current:
        assert len(current.in_flight) == 8
        assert len(current.pending_batches) == 92


def test_skipped_batch_results_fail_the_job(tmp_path: Path):
    batch = PiBatchRequest("terms:0:1", 0, 1)
    state = PiJobState(
        job_id="pi-test",
        request=dataclass_to_wire(PiComputeRequest()),
        output_path=str(tmp_path / "pi.txt"),
        in_flight={batch.batch_id: {"batch": dataclass_to_wire(batch)}},
    )
    agent = PiJobPaglet(state)
    agent._context = SimpleNamespace(name="alpha", address="http://127.0.0.1:1")

    reply = agent.handle_message(
        Message(
            PI_BATCH_FAILED,
            dataclass_to_wire(
                PiBatchResult(
                    batch_id=batch.batch_id,
                    term_start=batch.term_start,
                    term_count=batch.term_count,
                    host_name="alpha",
                    host_url="http://127.0.0.1:1",
                    status="skipped",
                    error="host busy",
                )
            ),
        )
    )

    assert reply == {"ok": True}
    assert state.done is True
    assert state.failed is True
    assert state.pending_batches == []
    assert state.in_flight == {}
    assert state.errors == {batch.batch_id: "host busy"}


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


def _wait_until(predicate, *, timeout: float = 3.0, interval: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()
