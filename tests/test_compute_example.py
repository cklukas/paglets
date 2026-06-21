# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

from paglets import Host, Message
from paglets.admin import ServerRef
from paglets.errors import InvalidAgentError
from paglets.examples.compute import (
    PiBatchRequest,
    PiBatchResult,
    PiBatchWorkerAgent,
    PiBatchWorkerState,
    PiComputeCoordinatorAgent,
    PiComputeRequest,
    PiComputeState,
    chudnovsky_binary_split,
    pi_decimal,
    pi_decimal_digits,
    pi_decimal_digits_from_results,
    PiResultDrainRequest,
)
from paglets.examples.compute.agent import (
    _decode_bigint,
    _encode_bigint,
    _host_worker_capacity_by_url,
    _host_worker_slots,
    _int_to_decimal_string,
    _is_missing_worker_error,
    _normalize_request,
)
from paglets.examples.compute.cli import _parser as pi_parser
from paglets.examples.compute.cli import main as pi_main
from paglets.examples.mesh_info import MeshHostSnapshot, TargetCandidate
from paglets.serde import dataclass_to_wire
from paglets.startup import load_launch_config, sync_launch_config
from tests.test_paglets_core import free_port


def test_pi_decimal_digits_are_deterministic():
    assert pi_decimal(0, 16) == "3.1415926535897932"
    assert pi_decimal_digits(0, 16) == "1415926535897932"


def test_large_decimal_formatting_avoids_python_int_string_limit():
    text = pi_decimal(0, 4310)

    assert text.startswith("3.1415926535897932")
    assert len(text) == 4312


def test_bigint_wire_helpers_avoid_python_decimal_string_limit():
    decimal_text = "1" + ("0" * 5000)
    value = _decode_bigint(decimal_text)
    encoded = _encode_bigint(-value)

    assert encoded.startswith("-0x")
    assert _decode_bigint(encoded) == -value
    assert _int_to_decimal_string(value) == decimal_text


def test_pi_compute_timeout_default_is_unbounded():
    assert PiComputeRequest().timeout == 0.0
    assert _normalize_request(PiComputeRequest(timeout=-1.0)).timeout == 0.0


def test_pi_compute_cli_has_separate_request_timeout():
    args = pi_parser().parse_args(["--digits", "8", "--stream-chunk-size", "123"])

    assert args.timeout == 0.0
    assert args.request_timeout == 300.0
    assert args.stream_chunk_size == 123


def test_self_disposed_worker_cleanup_errors_are_ignored():
    assert _is_missing_worker_error(InvalidAgentError("No paglet 'pi-worker-1' on alpha"))
    assert not _is_missing_worker_error(InvalidAgentError("Paglet 'pi-worker-1' crashed on alpha"))


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


def test_pi_worker_self_disposes_when_parent_report_fails(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    host.start_background()
    try:
        host.create(
            PiBatchWorkerAgent,
            PiBatchWorkerState(
                batch=dataclass_to_wire(PiBatchRequest("terms:0:1", 0, 1)),
                parent_host_url="http://127.0.0.1:1",
                parent_agent_id="missing-parent",
                ignore_load_limits=True,
            ),
        )

        _wait_until(lambda: not _pi_workers(host))
    finally:
        host.stop()


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


def test_pi_digits_can_be_formatted_from_drained_results_locally():
    request = PiComputeRequest(start=0, digits=8, batch_size=1, timeout=5.0, max_cpu_percent=100.0)
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

    assert pi_decimal_digits_from_results(request, [result], after_digits=0, digits=4) == "1415"


def test_pi_worker_launch_specs_run_in_parallel(monkeypatch):
    agent = PiComputeCoordinatorAgent(PiComputeState())
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    active = 0
    max_active = 0

    def fake_create(_host_url, _worker_state, _worker_id):
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

    monkeypatch.setattr(agent, "_create_worker_paglet", fake_create)
    specs = [
        {
            "host_url": "http://127.0.0.1:1",
            "host_name": "alpha",
            "worker_id": f"worker-{index}",
            "worker_state": PiBatchWorkerState(),
            "batch_id": f"terms:{index}:1",
            "batch_wire": dataclass_to_wire(PiBatchRequest(f"terms:{index}:1", index, 1)),
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
    state = PiComputeState(
        request=dataclass_to_wire(request),
        pending_batches=[dataclass_to_wire(PiBatchRequest(f"terms:{index}:1", index, 1)) for index in range(100)],
    )
    agent = PiComputeCoordinatorAgent(state)
    agent._context = SimpleNamespace(name="coordinator", address="http://127.0.0.1:8765")

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

    launches: list[tuple[str, str]] = []

    def _fake_create(host_url: str, _worker_state: PiBatchWorkerState, worker_id: str) -> None:
        launches.append((host_url, worker_id))

    monkeypatch.setattr(agent, "_select_targets", lambda request: targets)
    monkeypatch.setattr(agent, "_create_worker_paglet", _fake_create)

    agent._launch_available_batches(request)

    with agent.locked_state() as current:
        assert len(current.in_flight) == 8
        assert len(current.pending_batches) == 92
    assert len(launches) == 8


def test_skipped_batch_results_are_requeued(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    host.start_background()
    try:
        batch = PiBatchRequest("terms:0:1", 0, 1)
        initial_state = PiComputeState()
        initial_state.in_flight[batch.batch_id] = {
            "agent_id": "worker",
            "host_name": "alpha",
            "host_url": host.address,
            "batch": dataclass_to_wire(batch),
        }
        proxy = host.create(PiComputeCoordinatorAgent, initial_state)

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

        state = host.get_state(proxy.agent_id, PiComputeState)
        assert state.skipped_count == 1
        assert state.pending_batches == [dataclass_to_wire(batch)]
        assert state.in_flight == {}
    finally:
        host.stop()


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
