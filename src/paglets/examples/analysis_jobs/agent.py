# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message
from paglets.persistence.persistency import DeactivationPolicy
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from paglets.system.compute_slots import (
    COMPUTE_STATUS_RUNNING,
    ComputeJobPaglet,
    ComputeJobState,
)

from .workload import append_frames_to_sqlite, download_data, frames_to_payloads, process_data_to_frames

DEFAULT_TASK_COUNT = 20
DEFAULT_ROW_COUNT = 80_000
DEFAULT_FEATURE_COUNT = 32
DEFAULT_ESTIMATOR_TREES = 80
DEFAULT_TARGET_RUNTIME_SECONDS = 150.0
DEFAULT_HOME_CHECK_SECONDS = 300.0
DEFAULT_DB_LOCK_TIMEOUT_SECONDS = 60.0
DEFAULT_CPU_CORES = 1
DEFAULT_MEMORY_BYTES = 512 * 1024 * 1024
DEFAULT_TEMP_STORAGE_BYTES = 256 * 1024 * 1024
PERSISTENT_STORAGE_QUOTA_BYTES = 256 * 1024 * 1024

STATUS_NEW = "NEW"
STATUS_RUNNING = COMPUTE_STATUS_RUNNING
STATUS_WAITING_FOR_HOME = "WAITING_FOR_HOME"
STATUS_RETURNING_HOME = "RETURNING_HOME"
STATUS_COMMITTING = "COMMITTING"
STATUS_COMMITTED = "COMMITTED"
STATUS_FAILED_FINAL = "FAILED_FINAL"


@dataclass(frozen=True, slots=True)
class AnalysisCampaignRequest:
    task_count: int = DEFAULT_TASK_COUNT
    db_path: str = "paglets-analysis-results.sqlite"
    row_count: int = DEFAULT_ROW_COUNT
    feature_count: int = DEFAULT_FEATURE_COUNT
    estimator_trees: int = DEFAULT_ESTIMATOR_TREES
    target_runtime_seconds: float = DEFAULT_TARGET_RUNTIME_SECONDS
    cpu_cores: int = DEFAULT_CPU_CORES
    memory_bytes: int = DEFAULT_MEMORY_BYTES
    temp_storage_bytes: int = DEFAULT_TEMP_STORAGE_BYTES
    db_lock_timeout_seconds: float = DEFAULT_DB_LOCK_TIMEOUT_SECONDS


@dataclass
class CampaignSeederState(PagletState):
    request: dict[str, Any] = field(default_factory=dict)
    created_jobs: list[dict[str, str]] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)
    done: bool = False


# --8<-- [start:analysis-job-state]
@dataclass
class AnalysisJobState(ComputeJobState):
    campaign_id: str = ""
    job_id: str = ""
    status: str = STATUS_NEW
    seed: int = 0
    db_path: str = ""
    row_count: int = DEFAULT_ROW_COUNT
    feature_count: int = DEFAULT_FEATURE_COUNT
    estimator_trees: int = DEFAULT_ESTIMATOR_TREES
    target_runtime_seconds: float = DEFAULT_TARGET_RUNTIME_SECONDS
    estimated_runtime_seconds: float = DEFAULT_TARGET_RUNTIME_SECONDS
    home_check_seconds: float = DEFAULT_HOME_CHECK_SECONDS
    db_lock_timeout_seconds: float = DEFAULT_DB_LOCK_TIMEOUT_SECONDS
    cpu_cores: int = DEFAULT_CPU_CORES
    memory_bytes: int = DEFAULT_MEMORY_BYTES
    temp_storage_bytes: int = DEFAULT_TEMP_STORAGE_BYTES
    result_paths: dict[str, str] = field(default_factory=dict)
    result_payloads: dict[str, bytes] = field(default_factory=dict)
    started_at: float = 0.0
    completed_at: float = 0.0
    error: str = ""


# --8<-- [end:analysis-job-state]


class CampaignSeederPaglet(Paglet[CampaignSeederState]):
    """Create a batch of synthetic analysis job paglets on the home host."""

    State = CampaignSeederState

    def __init__(self, state: CampaignSeederState | None = None, *, agent_id: str | None = None):
        super().__init__(state=state, agent_id=agent_id)
        self._thread: threading.Thread | None = None

    def handle_message(self, message: Message):
        if message.kind == "start":
            request = dataclass_from_wire(AnalysisCampaignRequest, dict(message.args.get("request") or message.args))
            return self.start(request)
        if message.kind == "summary":
            return self.summary()
        return self.not_handled()

    def start(self, request: AnalysisCampaignRequest) -> dict[str, Any]:
        with self.locked():
            if self._thread is not None and self._thread.is_alive():
                return self.summary()
            self.state.request = dataclass_to_wire(request)
            self.state.created_jobs = []
            self.state.errors = {}
            self.state.done = False
            self._thread = threading.Thread(target=self._seed_jobs, args=(request,), daemon=True)
            self._thread.start()
        return self.summary()

    def summary(self) -> dict[str, Any]:
        with self.locked_state() as state:
            return {
                "request": dict(state.request),
                "created_jobs": list(state.created_jobs),
                "errors": dict(state.errors),
                "done": bool(state.done),
            }

    # --8<-- [start:seed-jobs]
    def _seed_jobs(self, request: AnalysisCampaignRequest) -> None:
        campaign_id = f"analysis-{uuid.uuid4().hex}"
        task_count = max(1, int(request.task_count))
        for index in range(task_count):
            job_id = f"{campaign_id}-{index:04d}"
            state = AnalysisJobState(
                campaign_id=campaign_id,
                job_id=job_id,
                seed=index + 10_000,
                db_path=request.db_path,
                row_count=max(100, int(request.row_count)),
                feature_count=max(4, int(request.feature_count)),
                estimator_trees=max(1, int(request.estimator_trees)),
                target_runtime_seconds=max(0.0, float(request.target_runtime_seconds)),
                estimated_runtime_seconds=max(0.0, float(request.target_runtime_seconds)),
                db_lock_timeout_seconds=max(0.0, float(request.db_lock_timeout_seconds)),
                cpu_cores=max(1, int(request.cpu_cores)),
                memory_bytes=max(0, int(request.memory_bytes)),
                temp_storage_bytes=max(0, int(request.temp_storage_bytes)),
            )
            try:
                proxy = self.context.create_paglet(AnalysisJobPaglet, state)
                with self.locked_state() as current:
                    current.created_jobs.append({"job_id": job_id, "agent_id": proxy.agent_id})
            except Exception as exc:
                with self.locked_state() as current:
                    current.errors[job_id] = str(exc)
        with self.locked_state() as current:
            current.done = True
        self.notify_all_state_changed()

    # --8<-- [end:seed-jobs]


class AnalysisJobPaglet(ComputeJobPaglet[AnalysisJobState]):
    """Synthetic dataframe analysis job that uses the built-in compute scheduler."""

    State = AnalysisJobState

    def handle_compute_job_message(self, message: Message):
        if message.kind == "status":
            with self.locked_state() as state:
                return dataclass_to_wire(state)
        return None

    # --8<-- [start:run-compute-job]
    def run_compute_job(self) -> None:
        with self.locked_state() as state:
            state.status = STATUS_RUNNING
            state.started_at = time.time()
            job_id = state.job_id
            seed = state.seed
            row_count = state.row_count
            feature_count = state.feature_count
            estimator_trees = state.estimator_trees
            target_runtime = state.target_runtime_seconds
        with self.locked_state() as state:
            cpu_core_ids = list(state.cpu_core_ids)
            cpu_affinity_supported = state.cpu_affinity_supported
            cpu_affinity_enforced = state.cpu_affinity_enforced
            cpu_affinity_error = state.cpu_affinity_error
        data = download_data(job_id=job_id, seed=seed, row_count=row_count, feature_count=feature_count)
        frames = process_data_to_frames(
            data,
            job_id=job_id,
            host_name=self.context.name,
            seed=seed,
            target_runtime_seconds=target_runtime,
            estimator_trees=estimator_trees,
            cpu_core_ids=cpu_core_ids,
            cpu_affinity_supported=cpu_affinity_supported,
            cpu_affinity_enforced=cpu_affinity_enforced,
            cpu_affinity_error=cpu_affinity_error,
        )
        payloads = frames_to_payloads(frames)
        result_paths = self._save_payloads(payloads)
        with self.locked_state() as state:
            state.result_paths = result_paths
            state.result_payloads = {}
            state.completed_at = time.time()
            state.status = STATUS_WAITING_FOR_HOME

    # --8<-- [end:run-compute-job]

    # --8<-- [start:continue-after-success]
    def continue_after_compute_success(self) -> None:
        with self.locked_state() as state:
            status = state.status
        if status == STATUS_RETURNING_HOME:
            self._commit_at_home()
            return
        self._try_return_home()

    # --8<-- [end:continue-after-success]

    # --8<-- [start:try-return-home]
    def _try_return_home(self) -> None:
        if self.is_compute_home():
            with self.locked_state() as state:
                state.result_payloads = self._load_payloads(state.result_paths)
                state.status = STATUS_RETURNING_HOME
            self._commit_at_home()
            return
        with self.locked_state() as state:
            home = state.home_host_url or state.home_host_name
            interval = max(1.0, float(state.home_check_seconds))
        if not home or not self.context.is_host_online(home):
            self.deactivate(
                policy=DeactivationPolicy.after(
                    interval,
                    activate_on_message=True,
                    queue_messages_when_inactive=True,
                    activate_on_startup=False,
                )
            )
            return
        with self.locked_state() as state:
            state.result_payloads = self._load_payloads(state.result_paths)
            state.status = STATUS_RETURNING_HOME
            target = state.home_host_url or state.home_host_name
        self.dispatch(target)

    # --8<-- [end:try-return-home]

    # --8<-- [start:commit-at-home]
    def _commit_at_home(self) -> None:
        with self.locked_state() as state:
            state.status = STATUS_COMMITTING
            payloads = dict(state.result_payloads)
            db_path = state.db_path
            timeout = state.db_lock_timeout_seconds
        append_frames_to_sqlite(db_path, payloads, lock_timeout_seconds=timeout)
        with self.locked_state() as state:
            state.status = STATUS_COMMITTED
        self.notify_user(
            "info",
            "Analysis result saved",
            f"Saved {self.state.job_id} to {db_path}",
            job_id=self.state.job_id,
        )
        self.context.host.dispose(self.agent_id)

    # --8<-- [end:commit-at-home]

    def _save_payloads(self, payloads: dict[str, bytes]) -> dict[str, str]:
        store = self.persistent_storage(quota_bytes=PERSISTENT_STORAGE_QUOTA_BYTES)
        paths: dict[str, str] = {}
        for name, payload in payloads.items():
            path = f"analysis_jobs/{self.agent_id}/{name}.pkl"
            store.write_bytes(path, payload)
            paths[name] = path
        return paths

    def _load_payloads(self, paths: dict[str, str]) -> dict[str, bytes]:
        store = self.persistent_storage(quota_bytes=PERSISTENT_STORAGE_QUOTA_BYTES)
        return {name: store.read_bytes(path) for name, path in paths.items()}

    # --8<-- [start:after-compute-failure]
    def after_compute_failure(self, message: str) -> None:
        with self.locked_state() as state:
            state.status = STATUS_FAILED_FINAL
            state.error = message
        self.notify_user("error", "Analysis job failed", message, job_id=self.state.job_id)

    # --8<-- [end:after-compute-failure]


def default_result_db() -> str:
    return str(Path.home() / "paglets-analysis-results.sqlite")
