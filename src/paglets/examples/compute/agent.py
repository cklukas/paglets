# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
import math
import threading
import time
from typing import Any
import uuid

from ...agent import Paglet, PagletState, state_locked
from ...messages import Message
from ...serde import dataclass_from_wire, dataclass_to_wire
from ...runtime_values import ServiceScope
from ..mesh_info import GET_SNAPSHOT, MESH_INFO, SELECT_TARGETS, SnapshotRequest, TargetSelectionRequest


CHUDNOVSKY_DIGITS_PER_TERM = 14
CHUDNOVSKY_GUARD_DIGITS = 10
CHUDNOVSKY_A = 13591409
CHUDNOVSKY_B = 545140134
CHUDNOVSKY_C = 640320
CHUDNOVSKY_C3_OVER_24 = CHUDNOVSKY_C**3 // 24


@dataclass(frozen=True, slots=True)
class PiComputeRequest:
    start: int = 0
    digits: int = 16
    batch_size: int = 1
    max_in_flight: int = 0
    timeout: float = 60.0
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
class PiComputeSummary:
    start: int
    digits: int
    decimal_digits: str
    pi: str
    terms: int
    done: bool
    pending: int
    in_flight: int
    skipped_count: int
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)


@dataclass
class PiComputeState(PagletState):
    request: dict[str, Any] = field(default_factory=dict)
    pending_batches: list[dict[str, Any]] = field(default_factory=list)
    in_flight: dict[str, dict[str, Any]] = field(default_factory=dict)
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)
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


class PiComputeCoordinatorAgent(Paglet[PiComputeState]):
    """Coordinate short-lived Chudnovsky Pi workers across the mesh."""

    State = PiComputeState

    def handle_message(self, message: Message):
        if message.kind == "start":
            request_wire = dict(message.args.get("request") or message.args)
            return self.start_job(dataclass_from_wire(PiComputeRequest, request_wire))
        if message.kind == "batch_result":
            return self.record_batch_result(message.args)
        if message.kind == "summary":
            return dataclass_to_wire(self.summary())
        if message.kind == "cleanup":
            return dataclass_to_wire(self.cleanup_workers())
        return self.not_handled()

    def start_job(self, request: PiComputeRequest) -> dict[str, Any]:
        request = _normalize_request(request)
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

        deadline = time.monotonic() + request.timeout
        while True:
            with self.locked_state() as state:
                complete = not state.pending_batches and not state.in_flight
            if complete:
                break
            if time.monotonic() >= deadline:
                self._timeout_remaining()
                break
            self._launch_available_batches(request)
            self.wait_state(
                lambda state: (not state.pending_batches and not state.in_flight) or time.monotonic() >= deadline,
                timeout=0.1,
            )
        with self.locked_state() as state:
            state.done = not state.pending_batches and not state.in_flight and not state.errors
        return dataclass_to_wire(self.summary())

    def _launch_available_batches(self, request: PiComputeRequest) -> None:
        targets = self._select_targets(request)
        if not targets:
            return
        target_limit = request.max_in_flight if request.max_in_flight > 0 else len(targets)
        with self.locked_state() as state:
            busy_hosts = {str(item.get("host_url") or "") for item in state.in_flight.values()}
            capacity = max(0, target_limit - len(state.in_flight))
        if capacity <= 0:
            return

        for target in targets:
            snapshot = target.snapshot
            if snapshot.host_url in busy_hosts:
                continue
            with self.locked_state() as state:
                if not state.pending_batches or len(state.in_flight) >= target_limit:
                    return
                batch_wire = state.pending_batches.pop(0)
                batch = dataclass_from_wire(PiBatchRequest, batch_wire)
                worker_id = f"pi-worker-{uuid.uuid4().hex}"
                state.in_flight[batch.batch_id] = {
                    "agent_id": worker_id,
                    "host_name": snapshot.host_name,
                    "host_url": snapshot.host_url,
                    "started_at": time.time(),
                    "batch": batch_wire,
                }
            worker_state = PiBatchWorkerState(
                batch=dataclass_to_wire(batch),
                parent_host_url=self.context.address,
                parent_agent_id=self.agent_id,
                max_load_per_cpu=request.max_load_per_cpu,
                max_cpu_percent=request.max_cpu_percent,
                min_memory_available_bytes=request.min_memory_available_bytes,
                min_work_free_bytes=request.min_work_free_bytes,
            )
            try:
                self.context.host.create_remote(
                    snapshot.host_url,
                    PiBatchWorkerAgent,
                    worker_state,
                    agent_id=worker_id,
                )
            except Exception as exc:
                with self.locked_state() as state:
                    state.in_flight.pop(batch.batch_id, None)
                    state.pending_batches.insert(0, batch_wire)
                    state.errors[snapshot.host_name or snapshot.host_url] = str(exc)
                self.notify_all_state_changed()
                return
            busy_hosts.add(snapshot.host_url)
            capacity -= 1
            if capacity <= 0:
                return

    def _select_targets(self, request: PiComputeRequest):
        try:
            service = self.require_contract(MESH_INFO, operation=SELECT_TARGETS, scope=ServiceScope.LOCAL)
            reply = service.call(
                SELECT_TARGETS,
                TargetSelectionRequest(
                    limit=max(1, request.max_in_flight or 64),
                    max_load_per_cpu=request.max_load_per_cpu,
                    max_cpu_percent=request.max_cpu_percent,
                    min_memory_available_bytes=request.min_memory_available_bytes,
                    min_work_free_bytes=request.min_work_free_bytes,
                    include_self=True,
                ),
                no_delay=True,
            )
            return reply.targets
        except Exception as exc:
            with self.locked_state() as state:
                state.errors["mesh-info"] = str(exc)
            return []

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
        self._cleanup_completed_worker(result)
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
        request = dataclass_from_wire(PiComputeRequest, self.state.request) if self.state.request else PiComputeRequest()
        results = {
            batch_id: dataclass_from_wire(PiBatchResult, wire)
            for batch_id, wire in self.state.results.items()
        }
        pieces = [result for result in sorted(results.values(), key=lambda item: item.term_start) if result.status == "ok"]
        decimal_digits = ""
        pi_text = ""
        if pieces and not self.state.pending_batches and not self.state.in_flight and not self.state.errors:
            precision_digits = _precision_digits(request)
            p, q, t = _combine_result_parts(pieces)
            pi_text, decimal_digits = _format_pi_decimal(
                p,
                q,
                t,
                start=request.start,
                digits=request.digits,
                precision_digits=precision_digits,
            )
        return PiComputeSummary(
            start=request.start,
            digits=request.digits,
            decimal_digits=decimal_digits,
            pi=pi_text,
            terms=_terms_for_request(request),
            done=bool(self.state.done),
            pending=len(self.state.pending_batches),
            in_flight=len(self.state.in_flight),
            skipped_count=self.state.skipped_count,
            results=dict(self.state.results),
            errors=dict(self.state.errors),
            cleanup_errors=dict(self.state.cleanup_errors),
        )

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
                with self.locked_state() as state:
                    state.cleanup_errors[host_name] = str(exc)

    def _cleanup_completed_worker(self, result: PiBatchResult) -> None:
        if not result.worker_agent_id:
            return
        try:
            proxy = self.context.get_proxy(result.worker_agent_id, result.host_url)
            if proxy is not None:
                proxy.dispose()
        except Exception as exc:
            with self.locked_state() as state:
                state.cleanup_errors[result.host_name or result.host_url] = str(exc)


class PiBatchWorkerAgent(Paglet[PiBatchWorkerState]):
    """Compute one Chudnovsky term range and report it to a coordinator."""

    State = PiBatchWorkerState

    def run(self) -> None:
        thread = threading.Thread(
            target=self._run_batch,
            name=f"paglets-pi-worker-{self.context.name}",
            daemon=True,
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
                    p=str(p),
                    q=str(q),
                    t=str(t),
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
                parent.send(Message("batch_result", dataclass_to_wire(result)))
        finally:
            try:
                self.context.host.dispose(self.agent_id)
            except Exception:
                pass

    def _busy_rejection(self) -> str:
        try:
            service = self.require_contract(MESH_INFO, operation=GET_SNAPSHOT, scope=ServiceScope.LOCAL)
            reply = service.call(GET_SNAPSHOT, SnapshotRequest(force=True), no_delay=True)
        except Exception as exc:
            return f"mesh-info unavailable: {exc}"
        snapshot = reply.snapshot
        if snapshot is None:
            return "mesh-info returned no snapshot"
        if self.state.max_load_per_cpu > 0 and snapshot.load_per_cpu > self.state.max_load_per_cpu:
            return f"load per cpu {snapshot.load_per_cpu:.2f} > {self.state.max_load_per_cpu:.2f}"
        if self.state.max_cpu_percent >= 0 and snapshot.cpu_percent > self.state.max_cpu_percent:
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
        part = (int(result.p), int(result.q), int(result.t))
        combined = part if combined is None else _combine_parts(combined, part)
    if combined is None:
        raise ValueError("no Pi term results to combine")
    return combined


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
    fractional = str(pi_scaled % scale).zfill(precision_digits)
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
        timeout=max(0.1, float(request.timeout)),
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
