# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from pathlib import Path

from paglets.config.startup import load_launch_config, sync_launch_config
from paglets.core.messages import Message
from paglets.examples.compute import (
    PiBatchResult,
    PiComputeRequest,
    PiPostProcessAgent,
    PiPostProcessState,
    PiPostProcessSummary,
    chudnovsky_binary_split,
    pi_decimal,
    pi_decimal_digits,
    pi_decimal_digits_from_results,
)
from paglets.examples.compute.chudnovsky import _decode_bigint, _encode_bigint, _int_to_decimal_string
from paglets.examples.mesh_info import MeshHostSnapshot
from paglets.runtime.host import Host
from paglets.serialization.codec import dataclass_to_wire
from tests.support import free_port


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


def test_post_process_agent_merges_results_in_order():
    state = PiPostProcessState()
    postproc = PiPostProcessAgent(state)
    postproc.handle_message(Message("configure", dataclass_to_wire(PiComputeRequest(digits=8))))
    term0 = chudnovsky_binary_split(0, 1)
    term1 = chudnovsky_binary_split(1, 2)

    postproc.handle_message(
        Message(
            "add_result",
            dataclass_to_wire(
                PiBatchResult(
                    batch_id="terms:0:1",
                    term_start=0,
                    term_count=1,
                    host_name="alpha",
                    host_url="http://127.0.0.1:1",
                    status="ok",
                    p=_encode_bigint(term0[0]),
                    q=_encode_bigint(term0[1]),
                    t=_encode_bigint(term0[2]),
                )
            ),
        )
    )
    postproc.handle_message(
        Message(
            "add_result",
            dataclass_to_wire(
                PiBatchResult(
                    batch_id="terms:1:1",
                    term_start=1,
                    term_count=1,
                    host_name="alpha",
                    host_url="http://127.0.0.1:1",
                    status="ok",
                    p=_encode_bigint(term1[0]),
                    q=_encode_bigint(term1[1]),
                    t=_encode_bigint(term1[2]),
                )
            ),
        )
    )

    summary = postproc.handle_message(Message("pp_summary"))
    summary = PiPostProcessSummary(
        request=summary["request"],
        completed_terms=int(summary["completed_terms"]),
        available_digits=int(summary["available_digits"]),
        done=bool(summary["done"]),
    )
    formatted = postproc.handle_message(Message("format", {"start": 0, "digits": 4}))

    assert summary.done
    assert summary.completed_terms == 2
    assert summary.available_digits >= 8
    assert str(formatted["pi"]).startswith("3.1415")


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
