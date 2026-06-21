# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
import os
import math
import threading
import shutil
import time
from typing import Any
import uuid

from ...agent import Paglet, PagletState, state_locked
from ...errors import InvalidAgentError
from ...messages import Message
from ...serde import dataclass_from_wire, dataclass_to_wire, qualified_name
from ...runtime_values import ServiceScope
from ..mesh_info import (
    GET_SNAPSHOT,
    MESH_INFO,
    SELECT_TARGETS,
    MeshHostSnapshot,
    SnapshotRequest,
    TargetCandidate,
    TargetSelectionRequest,
)


CHUDNOVSKY_DIGITS_PER_TERM = 14
CHUDNOVSKY_GUARD_DIGITS = 10
CHUDNOVSKY_A = 13591409
CHUDNOVSKY_B = 545140134
CHUDNOVSKY_C = 640320
CHUDNOVSKY_C3_OVER_24 = CHUDNOVSKY_C**3 // 24
DECIMAL_CHUNK_DIGITS = 9
DECIMAL_CHUNK_BASE = 10**DECIMAL_CHUNK_DIGITS
DEFAULT_STREAM_CHUNK_DIGITS = 8192
DEFAULT_RESULT_DRAIN_BATCH_SIZE = 128
MAX_PARALLEL_WORKER_LAUNCHES = 32
TARGET_SELECTION_TIMEOUT_SECONDS = 1.0
WORKER_BUSY_REJECTION_TIMEOUT_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class PiComputeRequest:
    start: int = 0
    digits: int = 16
    batch_size: int = 1
    max_in_flight: int = 0
    max_workers_per_host: int = 0
    timeout: float = 0.0
    max_load_per_cpu: float = 1.0
    max_cpu_percent: float = 90.0
    min_memory_available_bytes: int = 0
    min_work_free_bytes: int = 0


@dataclass(frozen=True, slots=True)
class PiBatchRequest:
    batch_id: str
    term_start: int
    term_count: int


@dataclass(frozen=True, slots=True)
class PiBatchResult:
    batch_id: str
    term_start: int
    term_count: int
    host_name: str
    host_url: str
    status: str
    worker_agent_id: str = ""
    p: str = ""
    q: str = ""
    t: str = ""
    error: str = ""
    duration_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class PiResultDrainRequest:
    known_batch_ids: list[str] = field(default_factory=list)
    wait_timeout: float = 0.5
    max_results: int = DEFAULT_RESULT_DRAIN_BATCH_SIZE


@dataclass(frozen=True, slots=True)
class PiComputeSummary:
    start: int
    digits: int
    decimal_digits: str
    pi: str
    terms: int
    completed_terms: int
    available_digits: int
    done: bool
    pending: int
    in_flight: int
    skipped_count: int
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _PiComputeProgress:
    request: PiComputeRequest
    pieces: list[PiBatchResult]
    total_terms: int
    completed_terms: int
    available_digits: int
    done: bool
    pending: int
    in_flight: int
    skipped_count: int
    errors: dict[str, str]
    cleanup_errors: dict[str, str]


@dataclass
class PiComputeState(PagletState):
    request: dict[str, Any] = field(default_factory=dict)
    pending_batches: list[dict[str, Any]] = field(default_factory=list)
    in_flight: dict[str, dict[str, Any]] = field(default_factory=dict)
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)
    cached_targets: list[dict[str, Any]] = field(default_factory=list)
    skipped_count: int = 0
    done: bool = False
    started_at: float = 0.0


@dataclass
class PiBatchWorkerState(PagletState):
    batch: dict[str, Any] = field(default_factory=dict)
    parent_host_url: str = ""
    parent_agent_id: str = ""
    max_load_per_cpu: float = 1.0
    max_cpu_percent: float = 90.0
    min_memory_available_bytes: int = 0
    min_work_free_bytes: int = 0
    ignore_load_limits: bool = False


class PiComputeCoordinatorAgent(Paglet[PiComputeState]):
    """Coordinate short-lived Chudnovsky Pi workers across the mesh."""

    State = PiComputeState

    def __init__(self, state: PiComputeState | None = None, *, agent_id: str | None = None):
        super().__init__(state=state, agent_id=agent_id)
        self._job_thread: threading.Thread | None = None
        self._launch_lock = threading.Lock()

    def handle_message(self, message: Message):
        if message.kind == "start":
            request_wire = dict(message.args.get("request") or message.args)
            return self.start_job(dataclass_from_wire(PiComputeRequest, request_wire))
        if message.kind == "start_async":
            request_wire = dict(message.args.get("request") or message.args)
            return self.start_async(dataclass_from_wire(PiComputeRequest, request_wire))
        if message.kind == "drain":
            return self.drain(
                after_digits=int(message.args.get("after_digits", 0)),
                wait_timeout=float(message.args.get("wait_timeout", 0.5)),
            )
        if message.kind == "drain_stream":
            return self.drain_stream(
                after_digits=int(message.args.get("after_digits", 0)),
                wait_timeout=float(message.args.get("wait_timeout", 0.5)),
                max_digits=int(message.args.get("max_digits", DEFAULT_STREAM_CHUNK_DIGITS)),
            )
        if message.kind == "drain_results":
            request_wire = dict(message.args.get("request") or message.args)
            request = dataclass_from_wire(PiResultDrainRequest, request_wire)
            return self.drain_results(request)
        if message.kind == "batch_result":
            return self.record_batch_result(message.args)
        if message.kind == "summary":
            return dataclass_to_wire(self.summary())
        if message.kind == "cleanup":
            return dataclass_to_wire(self.cleanup_workers())
        return self.not_handled()

    def start_job(self, request: PiComputeRequest) -> dict[str, Any]:
        reply = self.start_async(request)
        return reply["summary"]

    def start_async(self, request: PiComputeRequest) -> dict[str, Any]:
        request = _normalize_request(request)
        with self.locked():
            if self._job_thread is not None and self._job_thread.is_alive():
                return {"started": False, "summary": dataclass_to_wire(self.summary())}
            self._prepare_job(request)
            self._job_thread = threading.Thread(
                target=self._run_job,
                args=(request,),
                name=f"paglets-pi-coordinator-{self.context.name}",
                daemon=True,
            )
            self._job_thread.start()
        return {"started": True, "summary": dataclass_to_wire(self.summary())}

    def drain(self, *, after_digits: int, wait_timeout: float) -> dict[str, Any]:
        after_digits = max(0, int(after_digits))
        timeout = max(0.0, float(wait_timeout))

        def ready(state: PiComputeState) -> bool:
            progress = self._progress_from_state(state)
            return progress.done or bool(progress.errors) or progress.available_digits > after_digits

        self.wait_state(ready, timeout=timeout)
        self._launch_from_current_state()
        summary = self.summary()
        return {
            "summary": dataclass_to_wire(summary),
            "done": summary.done or bool(summary.errors),
        }

    def drain_stream(
        self,
        *,
        after_digits: int,
        wait_timeout: float,
        max_digits: int = DEFAULT_STREAM_CHUNK_DIGITS,
    ) -> dict[str, Any]:
        after_digits = max(0, int(after_digits))
        timeout = max(0.0, float(wait_timeout))
        max_digits = max(0, int(max_digits))

        def ready(state: PiComputeState) -> bool:
            progress = self._progress_from_state(state)
            return progress.done or bool(progress.errors) or progress.available_digits > after_digits

        self.wait_state(ready, timeout=timeout)
        self._launch_from_current_state()
        with self.locked_state() as state:
            progress = self._progress_from_state(state)
        decimal_digits = ""
        if progress.completed_terms > 0 and progress.available_digits > after_digits:
            available = progress.available_digits - after_digits
            chunk_digits = available if max_digits <= 0 else min(available, max_digits)
            decimal_digits = self._decimal_digits_from_progress(progress, after_digits=after_digits, digits=chunk_digits)
        cursor = after_digits + len(decimal_digits)
        return {
            "new_decimal_digits": decimal_digits,
            "cursor": cursor,
            "summary": self._compact_summary_from_progress(progress),
            "done": bool(progress.errors) or (progress.done and cursor >= progress.available_digits),
        }

    def drain_results(self, request: PiResultDrainRequest) -> dict[str, Any]:
        known_batch_ids = {str(batch_id) for batch_id in request.known_batch_ids}
        wait_timeout = max(0.0, float(request.wait_timeout))

        def ready(state: PiComputeState) -> bool:
            return bool(state.errors) or state.done or any(batch_id not in known_batch_ids for batch_id in state.results)

        self.wait_state(ready, timeout=wait_timeout)
        self._launch_from_current_state()
        with self.locked_state() as state:
            summary = self._compact_summary_from_state(state)
            result_items = [
                dict(result)
                for batch_id, result in state.results.items()
                if batch_id not in known_batch_ids
            ]
        result_items.sort(key=lambda item: (int(item.get("term_start") or 0), str(item.get("batch_id") or "")))
        max_results = max(0, int(request.max_results))
        if max_results > 0:
            result_items = result_items[:max_results]
        return {
            "results": result_items,
            "summary": summary,
            "done": bool(summary["errors"]) or bool(summary["done"]),
        }

    def _prepare_job(self, request: PiComputeRequest) -> None:
        batches = _make_batches(request)
        with self.locked_state() as state:
            state.request = dataclass_to_wire(request)
            state.pending_batches = [dataclass_to_wire(batch) for batch in batches]
            state.in_flight = {}
            state.results = {}
            state.errors = {}
            state.cleanup_errors = {}
            state.skipped_count = 0
            state.done = False
            state.started_at = time.time()
        self.notify_all_state_changed()

    def _run_job(self, request: PiComputeRequest) -> None:
        deadline = time.monotonic() + request.timeout if request.timeout > 0 else None
        while True:
            with self.locked_state() as state:
                complete = not state.pending_batches and not state.in_flight
            if complete:
                break
            if _deadline_expired(deadline):
                self._timeout_remaining()
                break
            self._launch_available_batches(request)
            self.wait_state(
                lambda state: (not state.pending_batches and not state.in_flight) or _deadline_expired(deadline),
                timeout=0.1,
            )
        with self.locked_state() as state:
            state.done = not state.pending_batches and not state.in_flight and not state.errors
        self.notify_all_state_changed()

    def _launch_available_batches(self, request: PiComputeRequest) -> None:
        with self._launch_lock:
            self._launch_available_batches_locked(request)

    def _launch_available_batches_locked(self, request: PiComputeRequest) -> None:
        targets = self._select_targets(request)
        with self.locked_state() as state:
            in_flight_count = len(state.in_flight)
            has_in_flight = in_flight_count > 0
        fallback_minimum = False
        slots_by_host = _slots_by_host(targets, request)
        if (not targets or sum((entry[0] for entry in slots_by_host.values())) <= 0) and not has_in_flight:
            targets = self._select_targets(request, ignore_load_limits=True, limit=1)
            slots_by_host = {
                target.snapshot.host_url.rstrip("/"): (1, _host_cpu_count(target.snapshot))
                for target in targets[:1]
            }
            fallback_minimum = bool(targets)
        if not targets:
            return

        available_slots = max(1, sum((entry[0] if isinstance(entry, tuple) else entry) for entry in slots_by_host.values()))
        if request.max_in_flight > 0:
            target_limit = min(max(1, int(request.max_in_flight)), available_slots)
        else:
            target_limit = available_slots
        if fallback_minimum:
            target_limit = min(target_limit, 1)
        with self.locked_state() as state:
            in_flight_by_host = _in_flight_by_host(state.in_flight)
            host_capacity_by_url = _host_worker_capacity_by_url(
                slots_by_host,
                in_flight_by_host,
                request,
            )
            additional_capacity = sum(host_capacity_by_url.values())
            if request.max_in_flight <= 0:
                target_limit = max(1, len(state.in_flight) + additional_capacity)
            host_base_capacity_by_url: dict[str, int] = {}
            for host_url, capacity in host_capacity_by_url.items():
                host_base_capacity_by_url[host_url] = in_flight_by_host.get(host_url, 0) + max(0, capacity)
            capacity = max(0, target_limit - len(state.in_flight))
        if capacity <= 0:
            return

        launches: list[dict[str, Any]] = []
        for target in targets:
            snapshot = target.snapshot
            host_url = snapshot.host_url.rstrip("/")
            host_capacity = host_capacity_by_url.get(host_url, 0)
            host_base_capacity = host_base_capacity_by_url.get(host_url, in_flight_by_host.get(host_url, 0) + host_capacity)
            while capacity > 0 and in_flight_by_host.get(host_url, 0) < host_base_capacity:
                with self.locked_state() as state:
                    if not state.pending_batches or len(state.in_flight) >= target_limit:
                        self._launch_worker_specs(launches)
                        return
                    batch_wire = state.pending_batches.pop(0)
                    batch = dataclass_from_wire(PiBatchRequest, batch_wire)
                    worker_id = f"pi-worker-{uuid.uuid4().hex}"
                    state.in_flight[batch.batch_id] = {
                        "agent_id": worker_id,
                        "host_name": snapshot.host_name,
                        "host_url": host_url,
                        "started_at": time.time(),
                        "batch": batch_wire,
                        "ignore_load_limits": fallback_minimum,
                    }
                worker_state = PiBatchWorkerState(
                    batch=dataclass_to_wire(batch),
                    parent_host_url=self.context.address,
                    parent_agent_id=self.agent_id,
                    max_load_per_cpu=request.max_load_per_cpu,
                    max_cpu_percent=request.max_cpu_percent,
                    min_memory_available_bytes=request.min_memory_available_bytes,
                    min_work_free_bytes=request.min_work_free_bytes,
                    ignore_load_limits=fallback_minimum,
                )
                launches.append(
                    {
                        "host_url": host_url,
                        "host_name": snapshot.host_name,
                        "worker_id": worker_id,
                        "worker_state": worker_state,
                        "batch_id": batch.batch_id,
                        "batch_wire": batch_wire,
                    }
                )
                in_flight_by_host[host_url] = in_flight_by_host.get(host_url, 0) + 1
                capacity -= 1
                if fallback_minimum or capacity <= 0:
                    self._launch_worker_specs(launches)
                    return
        self._launch_worker_specs(launches)

    def _launch_worker_specs(self, launches: list[dict[str, Any]]) -> None:
        if not launches:
            return
        max_workers = min(len(launches), MAX_PARALLEL_WORKER_LAUNCHES)
        if max_workers <= 1:
            for spec in launches:
                self._launch_worker_spec(spec)
            return
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="paglets-pi-launch") as executor:
            futures = {executor.submit(self._launch_worker_spec, spec): spec for spec in launches}
            for future in as_completed(futures):
                future.result()

    def _launch_worker_spec(self, spec: dict[str, Any]) -> None:
        try:
            self._create_worker_paglet(
                str(spec["host_url"]),
                spec["worker_state"],
                str(spec["worker_id"]),
            )
        except Exception as exc:
            with self.locked_state() as state:
                state.in_flight.pop(str(spec["batch_id"]), None)
                state.pending_batches.insert(0, dict(spec["batch_wire"]))
                state.errors[str(spec.get("host_name") or spec["host_url"])] = str(exc)
            self.notify_all_state_changed()

    def _create_worker_paglet(self, host_url: str, worker_state: PiBatchWorkerState, worker_id: str) -> None:
        self.context.host.client.post_json(
            f"{host_url.rstrip('/')}/agents",
            {
                "agent_class_name": qualified_name(PiBatchWorkerAgent),
                "state_class_name": qualified_name(PiBatchWorkerState),
                "state": dataclass_to_wire(worker_state),
                "agent_id": worker_id,
            },
        )

    def _select_targets(self, request: PiComputeRequest, *, ignore_load_limits: bool = False, limit: int | None = None):
        request_wire = TargetSelectionRequest(
            limit=max(1, limit if limit is not None else request.max_in_flight or 64),
            max_load_per_cpu=0.0 if ignore_load_limits else request.max_load_per_cpu,
            max_cpu_percent=-1.0 if ignore_load_limits else request.max_cpu_percent,
            min_memory_available_bytes=request.min_memory_available_bytes,
            min_work_free_bytes=request.min_work_free_bytes,
            include_self=True,
        )
        requested_limit = request_wire.limit
        try:
            service = self.require_contract(MESH_INFO, operation=SELECT_TARGETS, scope=ServiceScope.LOCAL)
            reply = service.call(
                SELECT_TARGETS,
                request_wire,
                no_delay=True,
                timeout=TARGET_SELECTION_TIMEOUT_SECONDS,
            )
            with self.locked_state() as state:
                state.cached_targets = [dataclass_to_wire(target) for target in reply.targets]
                state.errors.pop("mesh-info", None)
            return reply.targets
        except Exception as exc:
            message = str(exc).lower()
            is_transient_error = _is_mesh_info_transient_error(message)
            fallback_targets = self._get_cached_targets(requested_limit)
            if not fallback_targets:
                fallback_targets = self._local_fallback_targets(request_wire)
            if fallback_targets:
                with self.locked_state() as state:
                    state.cached_targets = [dataclass_to_wire(target) for target in fallback_targets]
                    if is_transient_error:
                        state.errors.pop("mesh-info", None)
                    else:
                        state.errors["mesh-info"] = str(exc)
                return fallback_targets
            with self.locked_state() as state:
                state.errors["mesh-info"] = str(exc)
            return []

    def _get_cached_targets(self, limit: int | None = None) -> list[TargetCandidate]:
        requested = max(1, limit or 1)
        with self.locked_state() as state:
            cached_wire = list(state.cached_targets)
        targets: list[TargetCandidate] = []
        for wire in cached_wire:
            try:
                targets.append(dataclass_from_wire(TargetCandidate, wire))
            except Exception:
                continue
        return targets[:requested]

    def _local_fallback_targets(self, request_wire: TargetSelectionRequest) -> list[TargetCandidate]:
        if self._context is None:
            return []
        if not request_wire.include_self:
            return []
        snapshot = self._current_host_snapshot()
        if snapshot is None:
            return []
        return [TargetCandidate(snapshot=snapshot, score=0.0, reasons=["fallback", "eligible"])]

    def _current_host_snapshot(self) -> MeshHostSnapshot | None:
        try:
            work_path = str(self.context.work_dir(create=True))
        except Exception:
            work_path = str(self.context.work_dir())
        cpu_count = max(1, int(os.cpu_count() or 1))
        try:
            load_average = list(os.getloadavg())
        except Exception:
            load_average = []
        try:
            work_usage = shutil.disk_usage(work_path)
            work_total_bytes = work_usage.total
            work_free_bytes = work_usage.free
        except Exception:
            work_total_bytes = 0
            work_free_bytes = 0
        work_percent_used = 0.0
        if work_total_bytes > 0:
            work_percent_used = (1.0 - float(work_free_bytes) / float(work_total_bytes)) * 100.0
        agent_count = self.context.host.list_agents()
        active_count = len([item for item in agent_count if item.get("active")])
        inactive_count = len([item for item in agent_count if not item.get("active")])
        load_average_value = load_average[0] if load_average else 0.0
        return MeshHostSnapshot(
            host_name=self.context.name,
            host_url=self.context.address.rstrip("/"),
            code_version="",
            observed_at=time.time(),
            platform="",
            cpu_count_logical=cpu_count,
            cpu_percent=0.0,
            load_average=load_average,
            load_per_cpu=max(0.0, float(load_average_value)) / max(1.0, float(cpu_count)),
            memory_total_bytes=1,
            memory_available_bytes=1,
            memory_percent=0.0,
            swap_percent=0.0,
            work_path=work_path,
            work_total_bytes=work_total_bytes,
            work_free_bytes=work_free_bytes,
            work_percent_used=work_percent_used,
            active_count=active_count,
            inactive_count=inactive_count,
            errors=[],
        )

    def _launch_from_current_state(self) -> None:
        with self.locked_state() as state:
            if state.done or not state.pending_batches:
                return
            request_wire = dict(state.request)
        if not request_wire:
            return
        self._launch_available_batches(_normalize_request(dataclass_from_wire(PiComputeRequest, request_wire)))

    def record_batch_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = dataclass_from_wire(PiBatchResult, payload)
        with self.locked_state() as state:
            in_flight = state.in_flight.pop(result.batch_id, None)
            if result.status == "skipped":
                state.skipped_count += 1
                batch_wire = (
                    dict(in_flight.get("batch") or {})
                    if in_flight is not None
                    else dataclass_to_wire(PiBatchRequest(result.batch_id, result.term_start, result.term_count))
                )
                state.pending_batches.append(batch_wire)
            elif result.status == "ok":
                state.results[result.batch_id] = dataclass_to_wire(result)
            else:
                state.errors[result.batch_id] = result.error or result.status
        self.notify_all_state_changed()
        return {"ok": True}

    def _timeout_remaining(self) -> None:
        cleanup: list[dict[str, Any]] = []
        with self.locked_state() as state:
            for batch_wire in state.pending_batches:
                batch = dataclass_from_wire(PiBatchRequest, batch_wire)
                state.errors[batch.batch_id] = "timed out before launch"
            state.pending_batches = []
            for batch_id, item in list(state.in_flight.items()):
                state.errors[batch_id] = "timed out waiting for worker result"
                cleanup.append(dict(item))
            state.in_flight = {}
        self._cleanup_worker_records(cleanup)
        self.notify_all_state_changed()

    @state_locked
    def summary(self) -> PiComputeSummary:
        return self._summary_from_state(self.state)

    @staticmethod
    def _summary_from_state(state: PiComputeState) -> PiComputeSummary:
        progress = PiComputeCoordinatorAgent._progress_from_state(state)
        decimal_digits = ""
        pi_text = ""
        if progress.completed_terms > 0 and progress.available_digits > 0:
            p, q, t = _combine_result_parts(progress.pieces)
            pi_text, decimal_digits = _format_pi_decimal(
                p,
                q,
                t,
                start=progress.request.start,
                digits=progress.available_digits,
                precision_digits=max(
                    CHUDNOVSKY_GUARD_DIGITS + 1,
                    progress.request.start + progress.available_digits + CHUDNOVSKY_GUARD_DIGITS,
                ),
            )
        return PiComputeSummary(
            start=progress.request.start,
            digits=progress.request.digits,
            decimal_digits=decimal_digits,
            pi=pi_text,
            terms=progress.total_terms,
            completed_terms=progress.completed_terms,
            available_digits=progress.available_digits,
            done=progress.done,
            pending=progress.pending,
            in_flight=progress.in_flight,
            skipped_count=progress.skipped_count,
            results=dict(state.results),
            errors=progress.errors,
            cleanup_errors=progress.cleanup_errors,
        )

    @staticmethod
    def _progress_from_state(state: PiComputeState) -> _PiComputeProgress:
        request = dataclass_from_wire(PiComputeRequest, state.request) if state.request else PiComputeRequest()
        results = [dataclass_from_wire(PiBatchResult, wire) for wire in state.results.values()]
        pieces = [result for result in sorted(results, key=lambda item: item.term_start) if result.status == "ok"]
        contiguous_pieces = _contiguous_result_pieces(pieces)
        total_terms = _terms_for_request(request)
        completed_terms = sum(piece.term_count for piece in contiguous_pieces)
        available_digits = _available_decimal_digits(request, completed_terms)
        return _PiComputeProgress(
            request=request,
            pieces=contiguous_pieces,
            total_terms=total_terms,
            completed_terms=completed_terms,
            available_digits=available_digits,
            done=bool(state.done),
            pending=len(state.pending_batches),
            in_flight=len(state.in_flight),
            skipped_count=state.skipped_count,
            errors=dict(state.errors),
            cleanup_errors=dict(state.cleanup_errors),
        )

    @staticmethod
    def _decimal_digits_from_progress(
        progress: _PiComputeProgress,
        *,
        after_digits: int = 0,
        digits: int | None = None,
    ) -> str:
        after_digits = max(0, int(after_digits))
        available = max(0, progress.available_digits - after_digits)
        digit_count = available if digits is None else min(available, max(0, int(digits)))
        if progress.completed_terms <= 0 or digit_count <= 0:
            return ""
        return pi_decimal_digits_from_results(
            progress.request,
            progress.pieces,
            after_digits=after_digits,
            digits=digit_count,
        )

    @staticmethod
    def _compact_summary_from_progress(progress: _PiComputeProgress) -> dict[str, Any]:
        return {
            "start": progress.request.start,
            "digits": progress.request.digits,
            "terms": progress.total_terms,
            "completed_terms": progress.completed_terms,
            "available_digits": progress.available_digits,
            "done": progress.done,
            "pending": progress.pending,
            "in_flight": progress.in_flight,
            "skipped_count": progress.skipped_count,
            "errors": progress.errors,
            "cleanup_errors": progress.cleanup_errors,
        }

    @staticmethod
    def _compact_summary_from_state(state: PiComputeState) -> dict[str, Any]:
        request = dataclass_from_wire(PiComputeRequest, state.request) if state.request else PiComputeRequest()
        completed_terms = _contiguous_completed_terms_from_wires(state.results.values())
        return {
            "start": request.start,
            "digits": request.digits,
            "terms": _terms_for_request(request),
            "completed_terms": completed_terms,
            "available_digits": _available_decimal_digits(request, completed_terms),
            "done": bool(state.done),
            "pending": len(state.pending_batches),
            "in_flight": len(state.in_flight),
            "skipped_count": state.skipped_count,
            "errors": dict(state.errors),
            "cleanup_errors": dict(state.cleanup_errors),
        }

    def cleanup_workers(self) -> PiComputeSummary:
        with self.locked_state() as state:
            cleanup = [dict(item) for item in state.in_flight.values()]
            state.in_flight = {}
        self._cleanup_worker_records(cleanup)
        return self.summary()

    def _cleanup_worker_records(self, records: list[dict[str, Any]]) -> None:
        for item in records:
            agent_id = str(item.get("agent_id") or "")
            host_url = str(item.get("host_url") or "")
            host_name = str(item.get("host_name") or host_url)
            if not agent_id or not host_url:
                continue
            try:
                proxy = self.context.get_proxy(agent_id, host_url)
                if proxy is not None:
                    proxy.dispose()
            except Exception as exc:
                if _is_missing_worker_error(exc):
                    continue
                with self.locked_state() as state:
                    state.cleanup_errors[host_name] = str(exc)


def _is_mesh_info_transient_error(message: str) -> bool:
    if not message:
        return False
    return "timeout" in message or "timed out" in message or "connection" in message

class PiBatchWorkerAgent(Paglet[PiBatchWorkerState]):
    """Compute one Chudnovsky term range and report it to a coordinator."""

    State = PiBatchWorkerState

    def __init__(self, state: PiBatchWorkerState | None = None, *, agent_id: str | None = None):
        super().__init__(state=state, agent_id=agent_id)
        self._worker_thread: threading.Thread | None = None

    def run(self) -> None:
        if self._worker_thread is not None and self._worker_thread.is_alive():
            return
        thread = threading.Thread(
            target=self._run_batch,
            name=f"paglets-pi-worker-{self.context.name}",
            daemon=True,
        )
        self._worker_thread = thread
        self.resources.register(
            "pi-worker-thread",
            lambda thread=thread: _join_worker_thread(thread),
            suppress=True,
        )
        thread.start()

    def _run_batch(self) -> None:
        started = time.perf_counter()
        try:
            batch = dataclass_from_wire(PiBatchRequest, self.state.batch)
            rejection = self._busy_rejection()
            if rejection:
                result = PiBatchResult(
                    batch_id=batch.batch_id,
                    term_start=batch.term_start,
                    term_count=batch.term_count,
                    host_name=self.context.name,
                    host_url=self.context.address,
                    status="skipped",
                    worker_agent_id=self.agent_id,
                    error=rejection,
                    duration_seconds=time.perf_counter() - started,
                )
            else:
                p, q, t = chudnovsky_binary_split(batch.term_start, batch.term_start + batch.term_count)
                result = PiBatchResult(
                    batch_id=batch.batch_id,
                    term_start=batch.term_start,
                    term_count=batch.term_count,
                    host_name=self.context.name,
                    host_url=self.context.address,
                    status="ok",
                    worker_agent_id=self.agent_id,
                    p=_encode_bigint(p),
                    q=_encode_bigint(q),
                    t=_encode_bigint(t),
                    duration_seconds=time.perf_counter() - started,
                )
        except Exception as exc:
            result = PiBatchResult(
                batch_id=str(self.state.batch.get("batch_id") or ""),
                term_start=int(self.state.batch.get("term_start") or 0),
                term_count=int(self.state.batch.get("term_count") or 0),
                host_name=self.context.name,
                host_url=self.context.address,
                status="error",
                worker_agent_id=self.agent_id,
                error=str(exc),
                duration_seconds=time.perf_counter() - started,
            )

        try:
            parent = self.context.get_proxy(self.state.parent_agent_id, self.state.parent_host_url)
            if parent is not None:
                parent.send_oneway(Message("batch_result", dataclass_to_wire(result)), no_delay=True)
        except Exception:
            pass
        finally:
            try:
                self.context.host.dispose(self.agent_id)
            except Exception:
                pass

    def _busy_rejection(self) -> str:
        try:
            service = self.require_contract(MESH_INFO, operation=GET_SNAPSHOT, scope=ServiceScope.LOCAL)
            reply = service.call(
                GET_SNAPSHOT,
                SnapshotRequest(force=True),
                no_delay=True,
                timeout=WORKER_BUSY_REJECTION_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            if _is_mesh_info_transient_error(str(exc).lower()):
                return ""
            return f"mesh-info unavailable: {exc}"
        snapshot = reply.snapshot
        if snapshot is None:
            return "mesh-info returned no snapshot"
        if (
            not self.state.ignore_load_limits
            and self.state.max_load_per_cpu > 0
            and snapshot.load_per_cpu > self.state.max_load_per_cpu
        ):
            return f"load per cpu {snapshot.load_per_cpu:.2f} > {self.state.max_load_per_cpu:.2f}"
        if (
            not self.state.ignore_load_limits
            and self.state.max_cpu_percent >= 0
            and snapshot.cpu_percent > self.state.max_cpu_percent
        ):
            return f"cpu {snapshot.cpu_percent:.1f}% > {self.state.max_cpu_percent:.1f}%"
        if self.state.min_memory_available_bytes > 0 and snapshot.memory_available_bytes < self.state.min_memory_available_bytes:
            return "available memory below minimum"
        if self.state.min_work_free_bytes > 0 and snapshot.work_free_bytes < self.state.min_work_free_bytes:
            return "work storage below minimum"
        if snapshot.errors:
            return "; ".join(snapshot.errors)
        return ""


def pi_decimal_digits(start: int, digits: int) -> str:
    if start < 0:
        raise ValueError("start must be non-negative")
    if digits < 0:
        raise ValueError("digits must be non-negative")
    request = PiComputeRequest(start=start, digits=digits)
    p, q, t = chudnovsky_binary_split(0, _terms_for_request(request))
    return _format_pi_decimal(p, q, t, start=start, digits=digits, precision_digits=_precision_digits(request))[1]


def pi_decimal(start: int, digits: int) -> str:
    request = PiComputeRequest(start=max(0, start), digits=max(0, digits))
    p, q, t = chudnovsky_binary_split(0, _terms_for_request(request))
    return _format_pi_decimal(p, q, t, start=request.start, digits=request.digits, precision_digits=_precision_digits(request))[0]


def pi_decimal_digits_from_results(
    request: PiComputeRequest,
    results: list[PiBatchResult],
    *,
    after_digits: int = 0,
    digits: int | None = None,
) -> str:
    after_digits = max(0, int(after_digits))
    contiguous = _contiguous_result_pieces(results)
    completed_terms = sum(piece.term_count for piece in contiguous)
    available = max(0, _available_decimal_digits(request, completed_terms) - after_digits)
    digit_count = available if digits is None else min(available, max(0, int(digits)))
    if digit_count <= 0:
        return ""
    pieces = _pieces_needed_for_digits(request, contiguous, after_digits + digit_count)
    p, q, t = _combine_result_parts(pieces)
    absolute_start = request.start + after_digits
    return _format_pi_decimal(
        p,
        q,
        t,
        start=absolute_start,
        digits=digit_count,
        precision_digits=max(
            CHUDNOVSKY_GUARD_DIGITS + 1,
            absolute_start + digit_count + CHUDNOVSKY_GUARD_DIGITS,
        ),
    )[1]


def chudnovsky_binary_split(a: int, b: int) -> tuple[int, int, int]:
    if b <= a:
        raise ValueError("term range cannot be empty")
    if b - a == 1:
        if a == 0:
            p = 1
            q = 1
        else:
            p = (6 * a - 5) * (2 * a - 1) * (6 * a - 1)
            q = a * a * a * CHUDNOVSKY_C3_OVER_24
        t = p * (CHUDNOVSKY_A + CHUDNOVSKY_B * a)
        if a % 2:
            t = -t
        return p, q, t
    middle = (a + b) // 2
    left = chudnovsky_binary_split(a, middle)
    right = chudnovsky_binary_split(middle, b)
    return _combine_parts(left, right)


def _combine_parts(left: tuple[int, int, int], right: tuple[int, int, int]) -> tuple[int, int, int]:
    p1, q1, t1 = left
    p2, q2, t2 = right
    return p1 * p2, q1 * q2, t1 * q2 + p1 * t2


def _combine_result_parts(results: list[PiBatchResult]) -> tuple[int, int, int]:
    combined: tuple[int, int, int] | None = None
    for result in sorted(results, key=lambda item: item.term_start):
        part = (_decode_bigint(result.p), _decode_bigint(result.q), _decode_bigint(result.t))
        combined = part if combined is None else _combine_parts(combined, part)
    if combined is None:
        raise ValueError("no Pi term results to combine")
    return combined


def _encode_bigint(value: int) -> str:
    return hex(value)


def _decode_bigint(value: str) -> int:
    text = value.strip()
    if text.lower().startswith(("0x", "+0x", "-0x")):
        return int(text, 16)
    return _parse_decimal_bigint(text)


def _parse_decimal_bigint(value: str) -> int:
    if not value:
        raise ValueError("empty integer")
    sign = 1
    digits = value
    if value[0] in "+-":
        sign = -1 if value[0] == "-" else 1
        digits = value[1:]
    if not digits or not digits.isdecimal():
        raise ValueError(f"invalid integer: {value!r}")
    number = 0
    for index in range(0, len(digits), DECIMAL_CHUNK_DIGITS):
        chunk = digits[index : index + DECIMAL_CHUNK_DIGITS]
        number = number * (10 ** len(chunk)) + int(chunk)
    return sign * number


def _int_to_decimal_string(value: int) -> str:
    if value == 0:
        return "0"
    sign = "-" if value < 0 else ""
    value = abs(value)
    chunks: list[int] = []
    while value:
        value, chunk = divmod(value, DECIMAL_CHUNK_BASE)
        chunks.append(chunk)
    head = str(chunks[-1])
    tail = "".join(f"{chunk:0{DECIMAL_CHUNK_DIGITS}d}" for chunk in reversed(chunks[:-1]))
    return f"{sign}{head}{tail}"


def _contiguous_result_pieces(results: list[PiBatchResult]) -> list[PiBatchResult]:
    contiguous: list[PiBatchResult] = []
    next_term = 0
    for result in sorted(results, key=lambda item: item.term_start):
        if result.term_start != next_term:
            break
        contiguous.append(result)
        next_term += result.term_count
    return contiguous


def _available_decimal_digits(request: PiComputeRequest, completed_terms: int) -> int:
    reliable_digits = completed_terms * CHUDNOVSKY_DIGITS_PER_TERM - CHUDNOVSKY_GUARD_DIGITS
    available_after_start = max(0, reliable_digits - request.start)
    return min(request.digits, available_after_start)


def _contiguous_completed_terms_from_wires(result_wires: Any) -> int:
    terms: list[tuple[int, int]] = []
    for wire in result_wires:
        if not isinstance(wire, dict) or wire.get("status") != "ok":
            continue
        terms.append((int(wire.get("term_start") or 0), int(wire.get("term_count") or 0)))
    completed_terms = 0
    for term_start, term_count in sorted(terms):
        if term_start != completed_terms:
            break
        completed_terms += term_count
    return completed_terms


def _pieces_needed_for_digits(
    request: PiComputeRequest,
    pieces: list[PiBatchResult],
    digit_end: int,
) -> list[PiBatchResult]:
    absolute_digit_end = request.start + max(0, int(digit_end))
    required_terms = max(1, math.ceil((absolute_digit_end + CHUDNOVSKY_GUARD_DIGITS) / CHUDNOVSKY_DIGITS_PER_TERM) + 1)
    selected: list[PiBatchResult] = []
    completed_terms = 0
    for piece in sorted(pieces, key=lambda item: item.term_start):
        if piece.term_start != completed_terms:
            break
        selected.append(piece)
        completed_terms += piece.term_count
        if completed_terms >= required_terms:
            break
    return selected


def _slots_by_host(targets: list[Any], request: PiComputeRequest) -> dict[str, tuple[int, int]]:
    slots: dict[str, tuple[int, int]] = {}
    for target in targets:
        host_url = target.snapshot.host_url.rstrip("/")
        slots[host_url] = (_host_worker_slots(target.snapshot, request), _host_cpu_count(target.snapshot))
    return slots


def _host_worker_capacity_by_url(
    additional_slots_by_url: dict[str, tuple[int, int]],
    in_flight_by_url: dict[str, int],
    request: PiComputeRequest,
) -> dict[str, int]:
    capacity: dict[str, int] = {}
    for host_url, (additional_slots, cpu_count) in additional_slots_by_url.items():
        current = max(0, int(in_flight_by_url.get(host_url, 0)))
        available_slots = max(0, int(additional_slots))
        host_limit = max(0, int(cpu_count))
        if request.max_workers_per_host > 0:
            host_limit = int(request.max_workers_per_host)
        capacity[host_url] = max(0, min(available_slots, host_limit - current))
    return capacity


def _host_cpu_count(snapshot: Any) -> int:
    return max(1, int(getattr(snapshot, "cpu_count_logical", 0) or 0))


def _host_worker_slots(snapshot: Any, request: PiComputeRequest) -> int:
    cpu_count = max(1, int(snapshot.cpu_count_logical or 0))
    if request.max_load_per_cpu <= 0:
        slots = cpu_count
    else:
        target_load = max(0.0, float(request.max_load_per_cpu)) * cpu_count
        free_load = target_load - _snapshot_load_value(snapshot, cpu_count)
        slots = 0 if free_load <= 0 else max(1, int(math.floor(free_load)))
    slots = min(slots, cpu_count)
    if request.max_workers_per_host > 0:
        slots = min(slots, request.max_workers_per_host)
    return max(0, slots)


def _snapshot_load_value(snapshot: Any, cpu_count: int) -> float:
    load_average = list(getattr(snapshot, "load_average", []) or [])
    if load_average:
        return max(0.0, float(load_average[0]))
    load_per_cpu = float(getattr(snapshot, "load_per_cpu", 0.0) or 0.0)
    if load_per_cpu > 0:
        return max(0.0, load_per_cpu * cpu_count)
    cpu_percent = float(getattr(snapshot, "cpu_percent", 0.0) or 0.0)
    return max(0.0, (cpu_percent / 100.0) * cpu_count)


def _in_flight_by_host(in_flight: dict[str, dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in in_flight.values():
        host_url = str(item.get("host_url") or "").rstrip("/")
        if not host_url:
            continue
        counts[host_url] = counts.get(host_url, 0) + 1
    return counts


def _deadline_expired(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def _is_missing_worker_error(exc: Exception) -> bool:
    return isinstance(exc, InvalidAgentError) and str(exc).startswith("No paglet ")


def _join_worker_thread(thread: threading.Thread) -> None:
    if thread is threading.current_thread():
        return
    if thread.is_alive():
        thread.join(timeout=1.0)


def _format_pi_decimal(
    p: int,
    q: int,
    t: int,
    *,
    start: int,
    digits: int,
    precision_digits: int,
) -> tuple[str, str]:
    scale = 10**precision_digits
    sqrt_value = math.isqrt(10005 * 10 ** (2 * precision_digits))
    pi_scaled = (q * 426880 * sqrt_value) // t
    integer_part = pi_scaled // scale
    fractional = _int_to_decimal_string(pi_scaled % scale).zfill(precision_digits)
    requested = fractional[start : start + digits]
    displayed_fractional = fractional[: start + digits]
    return f"{integer_part}.{displayed_fractional}", requested


def _normalize_request(request: PiComputeRequest) -> PiComputeRequest:
    start = max(0, int(request.start))
    digits = max(0, int(request.digits))
    return PiComputeRequest(
        start=start,
        digits=digits,
        batch_size=max(1, int(request.batch_size)),
        max_in_flight=max(0, int(request.max_in_flight)),
        max_workers_per_host=max(0, int(request.max_workers_per_host)),
        timeout=max(0.0, float(request.timeout)),
        max_load_per_cpu=float(request.max_load_per_cpu),
        max_cpu_percent=float(request.max_cpu_percent),
        min_memory_available_bytes=max(0, int(request.min_memory_available_bytes)),
        min_work_free_bytes=max(0, int(request.min_work_free_bytes)),
    )


def _make_batches(request: PiComputeRequest) -> list[PiBatchRequest]:
    batches: list[PiBatchRequest] = []
    total_terms = _terms_for_request(request)
    term_start = 0
    while term_start < total_terms:
        term_count = min(request.batch_size, total_terms - term_start)
        batches.append(PiBatchRequest(batch_id=f"terms:{term_start}:{term_count}", term_start=term_start, term_count=term_count))
        term_start += term_count
    return batches


def _precision_digits(request: PiComputeRequest) -> int:
    return max(1, request.start + request.digits + CHUDNOVSKY_GUARD_DIGITS)


def _terms_for_request(request: PiComputeRequest) -> int:
    return max(1, _precision_digits(request) // CHUDNOVSKY_DIGITS_PER_TERM + 1)
