# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pandas as pd

from paglets.examples.analysis_jobs.agent import AnalysisJobPaglet, AnalysisJobState
from paglets.examples.analysis_jobs.locking import CrossProcessFileLock
from paglets.examples.analysis_jobs.workload import (
    append_frames_to_sqlite,
    download_data,
    frames_to_payloads,
    process_data_to_frames,
)


def test_analysis_sqlite_append_uses_serial_file_lock(tmp_path: Path):
    db_path = tmp_path / "results.sqlite"
    payloads = frames_to_payloads(
        {
            "job_summary": pd.DataFrame([{"job_id": "a", "host_name": "alpha", "rows": 1}]),
            "feature_summary": pd.DataFrame([{"job_id": "a", "feature": "x", "mean": 1.0}]),
            "prediction_summary": pd.DataFrame([{"job_id": "a", "target": 0, "prediction": 0, "count": 1}]),
        }
    )

    threads = [
        threading.Thread(target=append_frames_to_sqlite, args=(db_path, payloads), kwargs={"lock_timeout_seconds": 5.0})
        for _ in range(4)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with sqlite3.connect(db_path) as connection:
        [(count,)] = connection.execute("select count(*) from job_summary").fetchall()
    assert count == 4


def test_cross_process_file_lock_serializes_threads(tmp_path: Path):
    lock_path = tmp_path / "demo.lock"
    order: list[int] = []

    def worker(index: int) -> None:
        with CrossProcessFileLock(lock_path, timeout=5.0):
            order.append(index)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(order) == [0, 1, 2]


def test_analysis_workload_records_cpu_affinity_metadata():
    data = download_data(job_id="job-0", seed=42, row_count=120, feature_count=6)

    frames = process_data_to_frames(
        data,
        job_id="job-0",
        host_name="alpha",
        seed=42,
        target_runtime_seconds=0.0,
        estimator_trees=1,
        cpu_core_ids=[1, 2],
        cpu_affinity_supported=True,
        cpu_affinity_enforced=True,
        cpu_affinity_error="",
    )

    row = frames["job_summary"].iloc[0]
    assert row["cpu_core_ids"] == "1,2"
    assert bool(row["cpu_affinity_supported"]) is True
    assert bool(row["cpu_affinity_enforced"]) is True


def test_analysis_job_uses_base_scheduler_api_without_internal_overrides():
    assert AnalysisJobPaglet.__dict__.keys().isdisjoint(
        {
            "_place_or_request_compute_slot",
            "_dispatch_to_compute_candidate",
            "_request_local_compute_slot",
            "_compute_slot_request_locked",
            "_record_compute_slot_grant_locked",
            "_record_compute_slot_redirect_locked",
        }
    )

    state = AnalysisJobState()
    assert state.estimated_runtime_seconds > 0.0
    assert state.memory_bytes > 0
