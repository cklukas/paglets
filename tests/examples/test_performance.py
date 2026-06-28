# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
import multiprocessing as mp
import threading
import time
from collections import namedtuple
from pathlib import Path

from paglets.examples.performance.agent import (
    PERFORMANCE_CHILD_RESULT,
    PERFORMANCE_SUMMARY,
    PerformanceBenchmarkAgent,
    PerformanceBenchmarkState,
    PerformanceChildResultRequest,
)
from paglets.examples.performance.cli import _print_text
from paglets.examples.performance.cli import main as perf_main
from paglets.examples.performance.kernels import (
    _BENCHMARK_THREAD_LOCK,
    HostBenchmarkLock,
    benchmark_cpu,
    benchmark_disk_target,
    benchmark_memory,
    discover_disk_targets,
    parse_size,
    run_host_benchmarks,
)
from paglets.examples.performance.models import (
    BenchmarkRequest,
    DiskBenchmarkResult,
    DiskSkip,
    DiskTarget,
    HostBenchmarkResult,
)
from paglets.patterns.operations import OperationClient
from paglets.remote.admin import ServerRef
from paglets.runtime.host import Host
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from tests.support import free_port


def _daemon_cpu_benchmark_worker(queue):
    before = mp.current_process().daemon
    result = benchmark_cpu(duration_seconds=0.01, workers=1)
    after = mp.current_process().daemon
    queue.put((before, after, [metric.name for metric in result.multi_core], result.errors))


def test_benchmark_dataclasses_round_trip_through_wire():
    request = BenchmarkRequest(
        include_cpu=False,
        duration_seconds=0.25,
        disk_size_bytes=64 * 1024,
        workers=2,
        paths=["/tmp"],
        lock_timeout_seconds=1.5,
    )
    restored_request = dataclass_from_wire(BenchmarkRequest, dataclass_to_wire(request))
    assert restored_request == request

    result = HostBenchmarkResult(
        host_name="alpha",
        host_url="http://127.0.0.1:1",
        platform="test",
        python_version="3.x",
        lock_wait_seconds=0.01,
        errors=["sample"],
    )
    restored_result = dataclass_from_wire(HostBenchmarkResult, dataclass_to_wire(result))
    assert restored_result == result


def test_parse_size_accepts_binary_units():
    assert parse_size("1K") == 1024
    assert parse_size("2M") == 2 * 1024 * 1024
    assert parse_size("1.5G") == int(1.5 * 1024**3)
    assert parse_size("512") == 512
    assert parse_size("512B") == 512


def test_cpu_and_memory_benchmarks_return_positive_rates():
    cpu = benchmark_cpu(duration_seconds=0.01, workers=1)
    assert cpu.single_core
    assert all(metric.operations_per_second > 0 for metric in cpu.single_core)
    assert cpu.multi_core
    assert all(metric.operations_per_second > 0 or metric.bytes_per_second > 0 for metric in cpu.multi_core)

    memory = benchmark_memory(duration_seconds=0.01)
    assert memory.metrics
    assert all(metric.bytes_per_second > 0 for metric in memory.metrics)


def test_cpu_benchmark_runs_multi_core_kernels_from_daemon_process():
    context = mp.get_context("spawn")
    queue = context.Queue()
    process = context.Process(target=_daemon_cpu_benchmark_worker, args=(queue,), daemon=True)

    process.start()
    process.join(30)

    assert process.exitcode == 0
    before, after, metric_names, errors = queue.get_nowait()
    assert before is True
    assert after is True
    assert metric_names == ["integer-multi", "float-multi", "sha256-multi"]
    assert errors == []


def test_disk_discovery_skips_special_readonly_unwritable_and_duplicate_volumes(tmp_path, monkeypatch):
    Partition = namedtuple("Partition", "device mountpoint fstype opts")
    real = tmp_path / "real"
    special = tmp_path / "special"
    readonly = tmp_path / "readonly"
    unwritable = tmp_path / "unwritable"
    for path in (real, special, readonly, unwritable):
        path.mkdir()

    partitions = [
        Partition("/dev/disk1", str(real), "apfs", "rw"),
        Partition("/dev/disk1", str(real), "apfs", "rw"),
        Partition("proc", str(special), "proc", "rw"),
        Partition("/dev/disk2", str(readonly), "apfs", "ro"),
        Partition("/dev/disk3", str(unwritable), "apfs", "rw"),
    ]
    monkeypatch.setattr("paglets.examples.performance.kernels.psutil.disk_partitions", lambda all=False: partitions)
    monkeypatch.setattr("paglets.examples.performance.kernels.os.access", lambda path, mode: Path(path) != unwritable)
    monkeypatch.setattr("paglets.examples.performance.kernels._default_writable_disk_dirs", lambda: [])

    targets, skipped = discover_disk_targets([])

    assert targets == [DiskTarget(str(real), "/dev/disk1", "apfs")]
    reasons = {item.reason for item in skipped}
    assert "duplicate volume" in reasons
    assert "special filesystem" in reasons
    assert "read-only volume" in reasons
    assert "not writable" in reasons


def test_disk_discovery_uses_writable_user_directory_when_mountpoint_is_not_writable(tmp_path, monkeypatch):
    Partition = namedtuple("Partition", "device mountpoint fstype opts")
    mount = tmp_path / "System" / "Volumes" / "Data"
    fallback = mount / "Users" / "klukas" / ".paglets" / "benchmarks"
    fallback.mkdir(parents=True)
    partitions = [Partition("/dev/disk3s5", str(mount), "apfs", "rw")]

    monkeypatch.setattr("paglets.examples.performance.kernels.psutil.disk_partitions", lambda all=False: partitions)
    monkeypatch.setattr("paglets.examples.performance.kernels._default_writable_disk_dirs", lambda: [fallback])
    monkeypatch.setattr("paglets.examples.performance.kernels.os.access", lambda path, mode: Path(path) == fallback)

    targets, skipped = discover_disk_targets([])

    assert targets == [DiskTarget(str(fallback), "/dev/disk3s5", "apfs")]
    assert any(skip.path == str(mount) and skip.reason == "not writable" for skip in skipped)


def test_disk_benchmark_uses_target_temp_directory_and_cleans_up(tmp_path):
    target = DiskTarget(str(tmp_path), "test-device", "testfs")

    result = benchmark_disk_target(target, size_bytes=64 * 1024)

    assert result.path == str(tmp_path)
    assert result.write_bytes_per_second > 0
    assert result.read_bytes_per_second > 0
    assert result.metadata_files_per_second > 0
    assert list(tmp_path.glob("paglets-bench-*")) == []


def test_text_output_hides_skipped_disks_unless_verbose(capsys):
    host_result = HostBenchmarkResult(
        host_name="alpha",
        host_url="http://127.0.0.1:1",
        platform="test",
        python_version="3.x",
        disk=DiskBenchmarkResult(skipped=[DiskSkip("/", "read-only volume")]),
    )
    summary = {
        "results": {"alpha": {"host_url": host_result.host_url, "result": dataclass_to_wire(host_result)}},
        "errors": {},
        "cleanup_errors": {},
    }

    _print_text(summary)
    assert "skipped /" not in capsys.readouterr().out

    _print_text(summary, verbose=True)
    assert "skipped /: read-only volume" in capsys.readouterr().out


def test_host_benchmark_lock_serializes_local_workers():
    intervals: list[tuple[float, float]] = []

    def worker() -> None:
        with HostBenchmarkLock(timeout=2.0):
            started = time.perf_counter()
            time.sleep(0.05)
            intervals.append((started, time.perf_counter()))

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(intervals) == 2
    first, second = sorted(intervals)
    assert second[0] >= first[1] - 0.005


def test_host_benchmark_lock_timeout_returns_host_error_result():
    assert _BENCHMARK_THREAD_LOCK.acquire(timeout=1.0)
    try:
        result = run_host_benchmarks(
            BenchmarkRequest(
                include_cpu=False,
                include_memory=False,
                include_disk=False,
                lock_timeout_seconds=0.01,
            ),
            host_name="alpha",
            host_url="http://127.0.0.1:1",
        )
    finally:
        _BENCHMARK_THREAD_LOCK.release()

    assert result.errors
    assert "benchmark lock busy" in result.errors[0]


def test_paglets_perf_test_json_collects_mesh_with_dynamic_entry(tmp_path, capsys, monkeypatch):
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh_version="perf-cli-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    beta = Host(
        "beta",
        host="127.0.0.1",
        port=free_port(),
        peers=[alpha.address],
        mesh_version="perf-cli-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "beta",
    )
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        monkeypatch.setattr(
            "paglets.examples.performance.cli._select_entry_server",
            lambda *, entry_name, client: ServerRef("alpha", alpha.address),
        )

        result = perf_main(
            [
                "--entry",
                "alpha",
                "--timeout",
                "10",
                "--json",
                "--output",
                str(tmp_path / "perf-summary.json"),
                "--duration",
                "0.01",
                "--disk-size",
                "64K",
                "--workers",
                "1",
                "--path",
                str(tmp_path),
                "--no-cpu",
                "--no-memory",
            ]
        )

        assert result == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["accepted"] is True
        assert payload["output_path"] == str(tmp_path / "perf-summary.json")
        _wait_until(lambda: (tmp_path / "perf-summary.json").read_text(encoding="utf-8").strip())
        payload = json.loads((tmp_path / "perf-summary.json").read_text(encoding="utf-8"))
        assert set(payload["results"]) == {"alpha", "beta"}
        assert payload["errors"] == {}
        for item in payload["results"].values():
            host_result = dataclass_from_wire(HostBenchmarkResult, item["result"])
            assert host_result.disk is not None
            assert host_result.disk.volumes
    finally:
        beta.stop()
        alpha.stop()


def _wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()


def test_benchmark_child_failure_records_error_for_one_host(tmp_path):
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh_version="perf-error-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    beta = Host(
        "beta",
        host="127.0.0.1",
        port=free_port(),
        peers=[alpha.address],
        mesh_version="perf-error-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "beta",
    )

    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        state = PerformanceBenchmarkState(pending_hosts=["alpha", "beta"])
        proxy = alpha.create(PerformanceBenchmarkAgent, state)
        operations = OperationClient(proxy)
        operations.call(
            PERFORMANCE_CHILD_RESULT,
            PerformanceChildResultRequest(
                host_name="alpha",
                host_url=alpha.address,
                result=dataclass_to_wire(
                    HostBenchmarkResult(
                        host_name="alpha",
                        host_url=alpha.address,
                        platform="test",
                        python_version="3.x",
                    )
                ),
            ),
        )
        operations.call(
            PERFORMANCE_CHILD_RESULT,
            PerformanceChildResultRequest(
                host_name="beta",
                host_url=beta.address,
                error="forced benchmark failure",
            ),
        )
        summary = operations.call(PERFORMANCE_SUMMARY)

        assert set(summary.results) == {"alpha"}
        assert summary.errors == {"beta": "forced benchmark failure"}
    finally:
        beta.stop()
        alpha.stop()
