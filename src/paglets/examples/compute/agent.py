# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import math
import os
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import InvalidAgentError
from paglets.core.messages import Message
from paglets.core.runtime_values import ServiceScope
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire, qualified_name
from paglets.services.contracts import ServiceOperation
from paglets.system.mesh_info import (
    MESH_INFO,
    SELECT_TARGETS,
    MeshHostSnapshot,
    TargetCandidate,
    TargetSelectionRequest,
)
from paglets.system.user_info import (
    NOTIFY_USER,
    PI_DONE_USER,
    PI_FAILED_USER,
    PI_OUTPUT_USER,
    PI_PROGRESS_USER,
    STREAM_USER,
    USER_INFO,
    UserInfoRequest,
    UserInfoStreamRequest,
)

from .chudnovsky import (
    _available_decimal_digits,
    _combine_parts,
    _decode_bigint,
    _encode_bigint,
    _format_pi_decimal,
    _terms_for_request,
    chudnovsky_binary_split,
)
from .models import (
    CHUDNOVSKY_GUARD_DIGITS,
    DEFAULT_OUTPUT_CHUNK_DIGITS,
    MAX_PARALLEL_WORKER_LAUNCHES,
    TARGET_SELECTION_TIMEOUT_SECONDS,
    PiBatchRequest,
    PiBatchResult,
    PiComputeRequest,
    PiComputeSummary,
    PiJobStartReply,
    PiJobStartRequest,
)

PI_START = ServiceOperation("pi.start", PiJobStartRequest, PiJobStartReply)
PI_BATCH_RESULT = "pi.batch_result"
PI_BATCH_FAILED = "pi.batch_failed"


@dataclass
class PiJobState(PagletState):
    job_id: str = ""
    request: dict[str, Any] = field(default_factory=dict)
    output_path: str = ""
    output_chunk_digits: int = DEFAULT_OUTPUT_CHUNK_DIGITS
    output_cursor: int = 0
    pending_batches: list[dict[str, Any]] = field(default_factory=list)
    in_flight: dict[str, dict[str, Any]] = field(default_factory=dict)
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    by_term: dict[str, str] = field(default_factory=dict)
    combined_term_count: int = 0
    combined_p: str = ""
    combined_q: str = ""
    combined_t: str = ""
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)
    cached_targets: list[dict[str, Any]] = field(default_factory=list)
    done: bool = False
    failed: bool = False
    started_at: float = 0.0
    completed_at: float = 0.0


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


class PiJobPaglet(Paglet[PiJobState]):
    """Message-driven Pi job that writes output on the entry host."""

    State = PiJobState

    def __init__(self, state: PiJobState | None = None, *, agent_id: str | None = None):
        super().__init__(state=state, agent_id=agent_id)
        self._launch_lock = threading.Lock()
        self._launcher_thread: threading.Thread | None = None

    def handle_message(self, message: Message):
        if message.kind == "pi.start":
            return dataclass_to_wire(self.start(dataclass_from_wire(PiJobStartRequest, dict(message.args))))
        if message.kind == PI_BATCH_RESULT:
            return self.record_batch_result(dataclass_from_wire(PiBatchResult, dict(message.args)))
        if message.kind == PI_BATCH_FAILED:
            return self.record_batch_failure(dataclass_from_wire(PiBatchResult, dict(message.args)))
        if message.kind == "summary":
            return dataclass_to_wire(self.summary())
        return self.not_handled()

    def start(self, start_request: PiJobStartRequest) -> PiJobStartReply:
        request = _normalize_request(dataclass_from_wire(PiComputeRequest, start_request.request))
        job_id = start_request.job_id or f"pi-{uuid.uuid4().hex}"
        output_path = Path(start_request.output_path).expanduser()
        if not output_path.is_absolute():
            raise ValueError("pi output_path must be absolute")
        output_chunk_digits = max(1, int(start_request.output_chunk_digits or DEFAULT_OUTPUT_CHUNK_DIGITS))
        batches = _make_batches(request)
        self._initialize_output_file(output_path, request)
        with self.locked_state() as state:
            state.job_id = job_id
            state.request = dataclass_to_wire(request)
            state.output_path = str(output_path)
            state.output_chunk_digits = output_chunk_digits
            state.output_cursor = 0
            state.pending_batches = [dataclass_to_wire(batch) for batch in batches]
            state.in_flight = {}
            state.results = {}
            state.by_term = {}
            state.combined_term_count = 0
            state.combined_p = ""
            state.combined_q = ""
            state.combined_t = ""
            state.errors = {}
            state.cleanup_errors = {}
            state.done = False
            state.failed = False
            state.started_at = time.time()
            state.completed_at = 0.0
        if request.start == 0:
            self._user_output("3.", target="stdout", operation=PI_OUTPUT_USER)
        self._start_launcher_thread(request)
        return PiJobStartReply(
            accepted=True,
            job_id=job_id,
            agent_id=self.agent_id,
            host_url=self.context.address,
            output_path=str(output_path),
        )

    def _start_launcher_thread(self, request: PiComputeRequest) -> None:
        if self._launcher_thread is not None and self._launcher_thread.is_alive():
            return
        thread = threading.Thread(
            target=self._launch_available_batches,
            args=(request,),
            name=f"paglets-pi-job-{self.context.name}",
            daemon=True,
        )
        self._launcher_thread = thread
        thread.start()

    def record_batch_result(self, result: PiBatchResult) -> dict[str, Any]:
        if result.status != "ok":
            return self.record_batch_failure(result)
        with self.locked_state() as state:
            if state.done or state.failed:
                return {"ok": True}
            state.in_flight.pop(result.batch_id, None)
            state.results[result.batch_id] = dataclass_to_wire(result)
            state.by_term[str(int(result.term_start))] = result.batch_id
            self._merge_contiguous_results_locked(state)
            request = dataclass_from_wire(PiComputeRequest, state.request)
            chunks = self._take_output_chunks_locked(state, request)
            should_finish = not state.pending_batches and not state.in_flight and state.output_cursor >= request.digits
            if should_finish:
                state.done = True
                state.completed_at = time.time()
        for chunk in chunks:
            self._append_output_chunk(chunk)
            self._user_output(chunk, target="stdout", operation=PI_OUTPUT_USER)
        self._emit_progress()
        if should_finish:
            self._user_notify("info", "pi.done", f"Pi job complete; output: {self.state.output_path}", PI_DONE_USER)
        else:
            self._launch_available_batches(request)
        self.notify_all_state_changed()
        return {"ok": True}

    def record_batch_failure(self, result: PiBatchResult) -> dict[str, Any]:
        message = result.error or result.status or "worker failed"
        with self.locked_state() as state:
            state.in_flight.pop(result.batch_id, None)
            state.errors[result.batch_id or f"worker:{result.worker_agent_id}"] = message
            state.failed = True
            state.done = True
            state.completed_at = time.time()
            output_path = state.output_path
        self._user_notify("error", "pi.failed", f"{message}; partial output: {output_path}", PI_FAILED_USER)
        self.notify_all_state_changed()
        return {"ok": True}

    def _initialize_output_file(self, path: Path, request: PiComputeRequest) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        initial = "3." if request.start == 0 else ""
        with path.open("w", encoding="utf-8") as handle:
            handle.write(initial)
            handle.flush()

    def _append_output_chunk(self, chunk: str) -> None:
        if not chunk:
            return
        path = Path(self.state.output_path)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(chunk)
            handle.flush()

    def _user_output(self, text: str, *, target: str, operation=STREAM_USER) -> None:
        if not text:
            return
        with contextlib.suppress(Exception):
            handle = self.require_contract(USER_INFO, operation=operation, scope=ServiceScope.LOCAL)
            handle.send_oneway(
                operation,
                UserInfoStreamRequest(stream_id=self.state.job_id, text=text, target=target, flush=True),
                no_delay=True,
            )

    def _user_notify(self, severity: str, title: str, message: str, operation=NOTIFY_USER) -> None:
        with contextlib.suppress(Exception):
            handle = self.require_contract(USER_INFO, operation=operation, scope=ServiceScope.LOCAL)
            handle.send_oneway(
                operation,
                UserInfoRequest(
                    severity=severity,
                    title=title,
                    message=message,
                    source_agent_id=self.agent_id,
                    job_id=self.state.job_id,
                    timestamp=time.time(),
                ),
                no_delay=True,
            )

    def _emit_progress(self) -> None:
        with self.locked_state() as state:
            request = dataclass_from_wire(PiComputeRequest, state.request) if state.request else PiComputeRequest()
            text = (
                f"pi.progress job={state.job_id} digits={state.output_cursor}/{request.digits} "
                f"pending={len(state.pending_batches)} in_flight={len(state.in_flight)} "
                f"output={state.output_path}\n"
            )
        self._user_output(text, target="stderr", operation=PI_PROGRESS_USER)

    def _combined_cached_values_locked(self, state: PiJobState) -> tuple[int, int, int]:
        if not state.combined_p or not state.combined_q or not state.combined_t:
            return chudnovsky_binary_split(0, 1)
        return _decode_bigint(state.combined_p), _decode_bigint(state.combined_q), _decode_bigint(state.combined_t)

    def _merge_contiguous_results_locked(self, state: PiJobState) -> None:
        while True:
            batch_id = state.by_term.get(str(state.combined_term_count), "")
            if not batch_id:
                break
            result_wire = state.results.get(batch_id)
            if result_wire is None:
                break
            result = dataclass_from_wire(PiBatchResult, result_wire)
            if result.term_start != state.combined_term_count:
                break
            part = (_decode_bigint(result.p), _decode_bigint(result.q), _decode_bigint(result.t))
            if state.combined_term_count == 0 and not state.combined_p:
                state.combined_p = result.p
                state.combined_q = result.q
                state.combined_t = result.t
            else:
                merged = _combine_parts(self._combined_cached_values_locked(state), part)
                state.combined_p = _encode_bigint(merged[0])
                state.combined_q = _encode_bigint(merged[1])
                state.combined_t = _encode_bigint(merged[2])
            state.combined_term_count += result.term_count

    def _take_output_chunks_locked(self, state: PiJobState, request: PiComputeRequest) -> list[str]:
        available = _available_decimal_digits(request, state.combined_term_count)
        if available <= state.output_cursor:
            return []
        chunks: list[str] = []
        while state.output_cursor < available:
            digit_count = min(state.output_chunk_digits, available - state.output_cursor)
            chunks.append(self._format_digits_locked(state, request, state.output_cursor, digit_count))
            state.output_cursor += digit_count
        return chunks

    def _format_digits_locked(
        self,
        state: PiJobState,
        request: PiComputeRequest,
        after_digits: int,
        digit_count: int,
    ) -> str:
        p, q, t = self._combined_cached_values_locked(state)
        absolute_start = request.start + after_digits
        return _format_pi_decimal(
            p,
            q,
            t,
            start=absolute_start,
            digits=digit_count,
            precision_digits=max(CHUDNOVSKY_GUARD_DIGITS + 1, absolute_start + digit_count + CHUDNOVSKY_GUARD_DIGITS),
        )[1]

    def _launch_available_batches(self, request: PiComputeRequest) -> None:
        with self._launch_lock:
            self._launch_available_batches_locked(request)

    def _launch_available_batches_locked(self, request: PiComputeRequest) -> None:
        with self.locked_state() as state:
            if state.done or state.failed or not state.pending_batches:
                return
            has_in_flight = bool(state.in_flight)
        targets = self._select_targets(request)
        slots_by_host = _slots_by_host(targets, request)
        if (not targets or sum(entry[0] for entry in slots_by_host.values()) <= 0) and not has_in_flight:
            targets = self._select_targets(request, ignore_load_limits=True, limit=1)
            slots_by_host = {
                target.snapshot.host_url.rstrip("/"): (1, _host_cpu_count(target.snapshot)) for target in targets[:1]
            }
        if not targets:
            self._fail_job("placement", "no eligible Pi worker target")
            return

        available_slots = max(1, sum(entry[0] for entry in slots_by_host.values()))
        target_limit = (
            min(max(1, request.max_in_flight), available_slots) if request.max_in_flight > 0 else available_slots
        )
        with self.locked_state() as state:
            in_flight_by_host = _in_flight_by_host(state.in_flight)
            capacity_by_url = _host_worker_capacity_by_url(slots_by_host, in_flight_by_host, request)
            if request.max_in_flight <= 0:
                target_limit = max(1, len(state.in_flight) + sum(capacity_by_url.values()))
            capacity = max(0, target_limit - len(state.in_flight))
        if capacity <= 0:
            return

        launches: list[dict[str, Any]] = []
        for target in targets:
            snapshot = target.snapshot
            host_url = snapshot.host_url.rstrip("/")
            host_capacity = capacity_by_url.get(host_url, 0)
            while capacity > 0 and in_flight_by_host.get(host_url, 0) < host_capacity:
                with self.locked_state() as state:
                    if state.done or state.failed or not state.pending_batches or len(state.in_flight) >= target_limit:
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
                    }
                launches.append(
                    {
                        "host_url": host_url,
                        "host_name": snapshot.host_name,
                        "worker_id": worker_id,
                        "batch_id": batch.batch_id,
                        "worker_state": PiBatchWorkerState(
                            batch=dataclass_to_wire(batch),
                            parent_host_url=self.context.address,
                            parent_agent_id=self.agent_id,
                            max_load_per_cpu=request.max_load_per_cpu,
                            max_cpu_percent=request.max_cpu_percent,
                            min_memory_available_bytes=request.min_memory_available_bytes,
                            min_work_free_bytes=request.min_work_free_bytes,
                        ),
                    }
                )
                in_flight_by_host[host_url] = in_flight_by_host.get(host_url, 0) + 1
                capacity -= 1
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
            self.context.host.client.post_json(
                f"{str(spec['host_url']).rstrip('/')}/agents",
                {
                    "agent_class_name": qualified_name(PiBatchWorkerAgent),
                    "state_class_name": qualified_name(PiBatchWorkerState),
                    "state": dataclass_to_wire(spec["worker_state"]),
                    "agent_id": str(spec["worker_id"]),
                },
            )
        except Exception as exc:
            self._fail_job(str(spec.get("batch_id") or "launch"), f"launch:{spec.get('host_name')}: {exc}")

    def _fail_job(self, key: str, message: str) -> None:
        with self.locked_state() as state:
            if state.done:
                return
            state.errors[key] = message
            state.failed = True
            state.done = True
            state.completed_at = time.time()
            output_path = state.output_path
        self._user_notify("error", "pi.failed", f"{message}; partial output: {output_path}", PI_FAILED_USER)
        self.notify_all_state_changed()

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
            fallback_targets = self._get_cached_targets(requested_limit) or self._local_fallback_targets(request_wire)
            if fallback_targets:
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
            with contextlib.suppress(Exception):
                targets.append(dataclass_from_wire(TargetCandidate, wire))
        return targets[:requested]

    def _local_fallback_targets(self, request_wire: TargetSelectionRequest) -> list[TargetCandidate]:
        if self._context is None or not request_wire.include_self:
            return []
        snapshot = self._current_host_snapshot()
        return [TargetCandidate(snapshot=snapshot, score=0.0, reasons=["fallback", "eligible"])] if snapshot else []

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
            usage = shutil.disk_usage(work_path)
            work_total_bytes = usage.total
            work_free_bytes = usage.free
        except Exception:
            work_total_bytes = 0
            work_free_bytes = 0
        agent_count = self.context.host.list_agents()
        active_count = len([item for item in agent_count if item.get("active")])
        inactive_count = len([item for item in agent_count if not item.get("active")])
        load_value = load_average[0] if load_average else 0.0
        return MeshHostSnapshot(
            host_name=self.context.name,
            host_url=self.context.address.rstrip("/"),
            code_version="",
            observed_at=time.time(),
            platform="",
            cpu_count_logical=cpu_count,
            cpu_percent=0.0,
            load_average=load_average,
            load_per_cpu=max(0.0, float(load_value)) / max(1.0, float(cpu_count)),
            memory_total_bytes=1,
            memory_available_bytes=1,
            memory_percent=0.0,
            swap_percent=0.0,
            work_path=work_path,
            work_total_bytes=work_total_bytes,
            work_free_bytes=work_free_bytes,
            work_percent_used=0.0,
            active_count=active_count,
            inactive_count=inactive_count,
            errors=[],
        )

    def summary(self) -> PiComputeSummary:
        with self.locked_state() as state:
            request = dataclass_from_wire(PiComputeRequest, state.request) if state.request else PiComputeRequest()
            return PiComputeSummary(
                start=request.start,
                digits=request.digits,
                decimal_digits="",
                pi="",
                terms=_terms_for_request(request),
                completed_terms=state.combined_term_count,
                available_digits=_available_decimal_digits(request, state.combined_term_count),
                done=state.done,
                pending=len(state.pending_batches),
                in_flight=len(state.in_flight),
                skipped_count=0,
                results=dict(state.results),
                errors=dict(state.errors),
                cleanup_errors=dict(state.cleanup_errors),
                job_id=state.job_id,
                output_path=state.output_path,
            )


class PiBatchWorkerAgent(Paglet[PiBatchWorkerState]):
    """Compute one Chudnovsky term range and report it to a Pi job paglet."""

    State = PiBatchWorkerState

    def __init__(self, state: PiBatchWorkerState | None = None, *, agent_id: str | None = None):
        super().__init__(state=state, agent_id=agent_id)
        self._worker_thread: threading.Thread | None = None

    def run(self) -> None:
        if self._worker_thread is not None and self._worker_thread.is_alive():
            return
        thread = threading.Thread(target=self._run_batch, name=f"paglets-pi-worker-{self.context.name}", daemon=True)
        self._worker_thread = thread
        self.resources.register("pi-worker-thread", lambda thread=thread: _join_worker_thread(thread), suppress=True)
        thread.start()

    def _run_batch(self) -> None:
        result = self._compute_result()
        kind = PI_BATCH_RESULT if result.status == "ok" else PI_BATCH_FAILED
        try:
            parent = self.context.get_proxy(self.state.parent_agent_id, self.state.parent_host_url)
            if parent is None:
                raise RuntimeError("Pi parent paglet not found")
            parent.send_oneway(Message(kind, dataclass_to_wire(result)), no_delay=True)
        except Exception as exc:
            self._notify_delivery_failure(str(exc), result)
            raise
        finally:
            with contextlib.suppress(Exception):
                self.context.host.dispose(self.agent_id)

    def _compute_result(self) -> PiBatchResult:
        started = time.perf_counter()
        try:
            batch = dataclass_from_wire(PiBatchRequest, self.state.batch)
            rejection = self._busy_rejection()
            if rejection:
                raise RuntimeError(rejection)
            p, q, t = chudnovsky_binary_split(batch.term_start, batch.term_start + batch.term_count)
            return PiBatchResult(
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
            return PiBatchResult(
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

    def _notify_delivery_failure(self, message: str, result: PiBatchResult) -> None:
        with contextlib.suppress(Exception):
            handle = self.require_contract(USER_INFO, operation=NOTIFY_USER, scope=ServiceScope.LOCAL)
            handle.send_oneway(
                NOTIFY_USER,
                UserInfoRequest(
                    severity="error",
                    title="pi.batch_failed",
                    message=f"Could not deliver {result.batch_id}: {message}",
                    source_agent_id=self.agent_id,
                    timestamp=time.time(),
                ),
                no_delay=True,
            )

    def _busy_rejection(self) -> str:
        return ""


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
    slots = cpu_count
    if request.max_load_per_cpu > 0:
        target_load = max(0.0, float(request.max_load_per_cpu)) * cpu_count
        free_load = target_load - _snapshot_load_value(snapshot, cpu_count)
        slots = 1 if free_load <= 0 else max(1, math.ceil(free_load))
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
        if host_url:
            counts[host_url] = counts.get(host_url, 0) + 1
    return counts


def _is_missing_worker_error(exc: Exception) -> bool:
    return isinstance(exc, InvalidAgentError) and str(exc).startswith("No paglet ")


def _join_worker_thread(thread: threading.Thread) -> None:
    if thread is threading.current_thread():
        return
    if thread.is_alive():
        thread.join(timeout=1.0)


def _normalize_request(request: PiComputeRequest) -> PiComputeRequest:
    return PiComputeRequest(
        start=max(0, int(request.start)),
        digits=max(0, int(request.digits)),
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
        batches.append(
            PiBatchRequest(batch_id=f"terms:{term_start}:{term_count}", term_start=term_start, term_count=term_count)
        )
        term_start += term_count
    return batches
