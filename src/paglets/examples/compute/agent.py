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
from typing import Any

from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import InvalidAgentError
from paglets.core.messages import Message
from paglets.core.runtime_values import ServiceScope
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire, qualified_name
from paglets.services.contracts import EmptyPayload, ServiceOperation
from paglets.system.mesh_info import (
    MESH_INFO,
    SELECT_TARGETS,
    MeshHostSnapshot,
    TargetCandidate,
    TargetSelectionRequest,
)

from .chudnovsky import (
    _available_decimal_digits,
    _combine_parts,
    _combine_result_parts,
    _contiguous_completed_terms_from_wires,
    _contiguous_result_pieces,
    _decode_bigint,
    _encode_bigint,
    _format_pi_decimal,
    _terms_for_request,
    chudnovsky_binary_split,
    pi_decimal_digits_from_results,
)
from .models import (
    CHUDNOVSKY_GUARD_DIGITS,
    DEFAULT_STREAM_CHUNK_DIGITS,
    MAX_PARALLEL_WORKER_LAUNCHES,
    POSTPROCESSOR_STREAM_CHUNK_DIGITS,
    TARGET_SELECTION_TIMEOUT_SECONDS,
    PiBatchRequest,
    PiBatchResult,
    PiComputeRequest,
    PiComputeSummary,
    PiPostProcessStreamRequest,
    PiPostProcessSummary,
    PiResultDrainRequest,
    _PiComputeProgress,
)


@dataclass(frozen=True, slots=True)
class PiStartRequest:
    request: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PiStartReply:
    started: bool = False
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PiDrainRequest:
    after_digits: int = 0
    wait_timeout: float = 0.5


@dataclass(frozen=True, slots=True)
class PiDrainReply:
    summary: dict[str, Any] = field(default_factory=dict)
    done: bool = False


@dataclass(frozen=True, slots=True)
class PiDrainStreamRequest:
    after_digits: int = 0
    wait_timeout: float = 0.5
    max_digits: int = DEFAULT_STREAM_CHUNK_DIGITS


@dataclass(frozen=True, slots=True)
class PiDrainStreamReply:
    new_decimal_digits: str = ""
    cursor: int = 0
    summary: dict[str, Any] = field(default_factory=dict)
    done: bool = False


PI_START_ASYNC = ServiceOperation("start_async", PiStartRequest, PiStartReply)
PI_DRAIN = ServiceOperation("drain", PiDrainRequest, PiDrainReply)
PI_DRAIN_STREAM = ServiceOperation("drain_stream", PiDrainStreamRequest, PiDrainStreamReply)
PI_CLEANUP = ServiceOperation("cleanup", EmptyPayload, PiComputeSummary)


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
    postprocessor_agent_id: str = ""


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


@dataclass
class PiPostProcessState(PagletState):
    request: dict[str, Any] = field(default_factory=dict)
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    by_term: dict[str, str] = field(default_factory=dict)
    combined_term_count: int = 0
    combined_p: str = ""
    combined_q: str = ""
    combined_t: str = ""


class PiPostProcessAgent(Paglet[PiPostProcessState]):
    """Combine batch term segments and format decimal digits for the coordinator."""

    State = PiPostProcessState

    def handle_message(self, message: Message):
        if message.kind == "configure":
            request_wire = dict(message.args)
            return self._configure(dataclass_from_wire(PiComputeRequest, request_wire))
        if message.kind == "add_result":
            result_wire = dict(message.args.get("result") or message.args)
            return self._add_result(dataclass_from_wire(PiBatchResult, result_wire))
        if message.kind == "pp_summary":
            return dataclass_to_wire(self._summary())
        if message.kind == "drain":
            request_wire = dict(message.args)
            request = PiPostProcessStreamRequest(
                after_digits=request_wire.get("after_digits", 0),
                max_digits=request_wire.get("max_digits", POSTPROCESSOR_STREAM_CHUNK_DIGITS),
            )
            return self._drain(request)
        if message.kind == "format":
            request_wire = dict(message.args)
            request = PiPostProcessStreamRequest(
                after_digits=request_wire.get("start", 0),
                max_digits=request_wire.get("digits", 0),
            )
            return self._format_range(request.after_digits, request.max_digits)
        if message.kind == "done":
            return self._mark_done()
        return self.not_handled()

    def _configure(self, request: PiComputeRequest) -> dict[str, Any]:
        with self.locked():
            self.state.request = dataclass_to_wire(request)
            self.state.results = {}
            self.state.by_term = {}
            self.state.combined_term_count = 0
            self.state.combined_p = ""
            self.state.combined_q = ""
            self.state.combined_t = ""
        return {"ok": True}

    def _mark_done(self) -> dict[str, Any]:
        return {"ok": True}

    def _add_result(self, result: PiBatchResult) -> dict[str, Any]:
        if result.status != "ok":
            return {"ok": True}
        batch_id = str(result.batch_id)
        with self.locked():
            if batch_id in self.state.results:
                return {"ok": True}
            if result.term_start < 0 or result.term_count <= 0:
                return {"ok": True}
            wire = dataclass_to_wire(result)
            self.state.results[batch_id] = wire
            self.state.by_term[str(int(result.term_start))] = batch_id
            self._merge_contiguous_results()
        return {"ok": True}

    def _summary(self) -> PiPostProcessSummary:
        with self.locked():
            request = (
                dataclass_from_wire(PiComputeRequest, self.state.request) if self.state.request else PiComputeRequest()
            )
            return PiPostProcessSummary(
                request=self.state.request,
                completed_terms=int(self.state.combined_term_count),
                available_digits=_available_decimal_digits(request, self.state.combined_term_count),
                done=_available_decimal_digits(request, self.state.combined_term_count) >= request.digits,
            )

    def _drain(self, request: PiPostProcessStreamRequest) -> dict[str, Any]:
        with self.locked():
            current_request = (
                dataclass_from_wire(PiComputeRequest, self.state.request) if self.state.request else PiComputeRequest()
            )
            available_digits = _available_decimal_digits(current_request, self.state.combined_term_count)
            after_digits = max(0, int(request.after_digits))
            if available_digits <= after_digits:
                return {
                    "new_decimal_digits": "",
                    "cursor": after_digits,
                    "available_digits": available_digits,
                }
            max_digits = int(request.max_digits)
            needed = available_digits - after_digits
            if max_digits > 0:
                needed = min(needed, max_digits)
            digits = self._format_digits(current_request, after_digits, needed)
            cursor = after_digits + len(digits)
            return {"new_decimal_digits": digits, "cursor": cursor, "available_digits": available_digits}

    def _format_range(self, start: int, digits: int) -> dict[str, Any]:
        with self.locked():
            current_request = (
                dataclass_from_wire(PiComputeRequest, self.state.request) if self.state.request else PiComputeRequest()
            )
            available_digits = _available_decimal_digits(current_request, self.state.combined_term_count)
            if available_digits <= 0:
                return {"pi": "3.", "decimal_digits": ""}
            start = max(0, int(start))
            digits = max(0, int(digits))
            p, q, t = self._combined_cached_values()
            precision_digits = max(
                CHUDNOVSKY_GUARD_DIGITS + 1,
                start + digits + CHUDNOVSKY_GUARD_DIGITS,
            )
            pi_text, decimal_digits = _format_pi_decimal(
                p,
                q,
                t,
                start=start,
                digits=min(digits, available_digits),
                precision_digits=precision_digits,
            )
            return {
                "pi": pi_text,
                "decimal_digits": decimal_digits,
                "available_digits": available_digits,
            }

    def _format_digits(self, request: PiComputeRequest, after_digits: int, max_digits: int) -> str:
        if max_digits <= 0:
            return ""
        p, q, t = self._combined_cached_values()
        start = request.start + after_digits
        if request.digits <= 0:
            return ""
        precision_digits = max(
            CHUDNOVSKY_GUARD_DIGITS + 1,
            start + max_digits + CHUDNOVSKY_GUARD_DIGITS,
        )
        return _format_pi_decimal(
            p,
            q,
            t,
            start=start,
            digits=max_digits,
            precision_digits=precision_digits,
        )[1]

    def _combined_cached_values(self) -> tuple[int, int, int]:
        if not self.state.combined_p or not self.state.combined_q or not self.state.combined_t:
            p, q, t = chudnovsky_binary_split(0, 1)
        else:
            p, q, t = (
                _decode_bigint(self.state.combined_p),
                _decode_bigint(self.state.combined_q),
                _decode_bigint(self.state.combined_t),
            )
        return p, q, t

    def _merge_contiguous_results(self) -> None:
        request = (
            dataclass_from_wire(PiComputeRequest, self.state.request) if self.state.request else PiComputeRequest()
        )
        _ = request
        while True:
            next_wire = self.state.results.get(self.state.by_term.get(str(self.state.combined_term_count), ""))
            if next_wire is None:
                break
            result = dataclass_from_wire(PiBatchResult, next_wire)
            next_wire_id = str(result.batch_id)
            self.state.results.pop(next_wire_id, None)
            self.state.by_term.pop(str(self.state.combined_term_count), None)
            if result.term_start != self.state.combined_term_count:
                self.state.results[next_wire_id] = dataclass_to_wire(result)
                self.state.by_term[str(result.term_start)] = next_wire_id
                break
            part = (_decode_bigint(result.p), _decode_bigint(result.q), _decode_bigint(result.t))
            if self.state.combined_term_count == 0 and not self.state.combined_p:
                self.state.combined_p, self.state.combined_q, self.state.combined_t = (
                    result.p,
                    result.q,
                    result.t,
                )
            else:
                existing = (
                    _decode_bigint(self.state.combined_p),
                    _decode_bigint(self.state.combined_q),
                    _decode_bigint(self.state.combined_t),
                )
                merged = _combine_parts(existing, part)
                self.state.combined_p = _encode_bigint(merged[0])
                self.state.combined_q = _encode_bigint(merged[1])
                self.state.combined_t = _encode_bigint(merged[2])
            self.state.combined_term_count += result.term_count


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
        reply = self._postprocessor_drain(after_digits, max_digits)
        if reply is not None:
            decimal_digits = str(reply.get("new_decimal_digits") or "")
            cursor = int(reply.get("cursor") or after_digits + len(decimal_digits))
            summary = self._compact_summary_from_progress(progress)
            if "available_digits" in reply:
                summary["available_digits"] = int(reply.get("available_digits") or summary["available_digits"])
            done = bool(progress.errors) or bool(progress.done and summary["available_digits"] <= cursor)
            return {
                "new_decimal_digits": decimal_digits,
                "cursor": cursor,
                "summary": summary,
                "done": done,
            }

        decimal_digits = ""
        if progress.completed_terms > 0 and progress.available_digits > after_digits:
            available = progress.available_digits - after_digits
            chunk_digits = available if max_digits <= 0 else min(available, max_digits)
            decimal_digits = self._decimal_digits_from_progress(
                progress, after_digits=after_digits, digits=chunk_digits
            )
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
            return (
                bool(state.errors) or state.done or any(batch_id not in known_batch_ids for batch_id in state.results)
            )

        self.wait_state(ready, timeout=wait_timeout)
        self._launch_from_current_state()
        with self.locked_state() as state:
            summary = self._compact_summary_from_state(state)
            result_items = [
                dict(result) for batch_id, result in state.results.items() if batch_id not in known_batch_ids
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
            self._cleanup_postprocessor_locked(state)
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
        self._ensure_postprocessor(request)
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
            if not state.pending_batches and not state.in_flight:
                _clear_launch_retry_errors(state.errors)
            state.done = not state.pending_batches and not state.in_flight and not state.errors
        self.notify_all_state_changed()

    def _ensure_postprocessor(self, request: PiComputeRequest) -> None:
        if self._context is None:
            return
        proxy = self._postprocessor_proxy()
        if proxy is not None:
            try:
                proxy.send(Message("configure", dataclass_to_wire(request)), no_delay=True)
                return
            except Exception:
                self._cleanup_postprocessor_locked()

        self._create_postprocessor(request)

    def _create_postprocessor(self, request: PiComputeRequest) -> None:
        if self._context is None:
            return
        worker_id = f"pi-pp-{uuid.uuid4().hex}"
        proxy = None
        try:
            proxy = self.context.create_paglet(PiPostProcessAgent, PiPostProcessState(), agent_id=worker_id)
            with self.locked_state() as state:
                state.postprocessor_agent_id = worker_id
            proxy.send(Message("configure", dataclass_to_wire(request)), no_delay=True)
        except Exception:
            self._cleanup_postprocessor_locked()
            if proxy is not None:
                with contextlib.suppress(Exception):
                    proxy.dispose()

    def _postprocessor_proxy(self) -> Any | None:
        if self._context is None:
            return None
        with self.locked_state() as state:
            agent_id = state.postprocessor_agent_id
        if not agent_id:
            return None
        try:
            return self.context.get_proxy(agent_id, self.context.address)
        except Exception:
            self._cleanup_postprocessor_locked()
            return None

    def _postprocessor_summary(self) -> PiPostProcessSummary | None:
        proxy = self._postprocessor_proxy()
        if proxy is None:
            return None
        try:
            reply = proxy.send(Message("pp_summary"))
            return dataclass_from_wire(PiPostProcessSummary, reply)
        except Exception:
            self._cleanup_postprocessor_locked()
            return None

    def _postprocessor_drain(self, after_digits: int, max_digits: int) -> dict[str, Any] | None:
        proxy = self._postprocessor_proxy()
        if proxy is None:
            return None
        try:
            return dict(
                proxy.send(
                    Message(
                        "drain",
                        {"after_digits": after_digits, "max_digits": max_digits},
                    )
                )
            )
        except Exception:
            self._cleanup_postprocessor_locked()
            return None

    def _postprocess_send_result(self, result: PiBatchResult) -> None:
        proxy = self._postprocessor_proxy()
        if proxy is None:
            return
        try:
            proxy.send_oneway(Message("add_result", dataclass_to_wire(result)), no_delay=True)
        except Exception:
            self._cleanup_postprocessor_locked()

    def _postprocess_format(self, request: PiComputeRequest) -> dict[str, Any] | None:
        if request.digits <= 0:
            return {"pi": "", "decimal_digits": ""}
        proxy = self._postprocessor_proxy()
        if proxy is None:
            return None
        try:
            return dict(
                proxy.send(
                    Message(
                        "format",
                        {
                            "start": request.start,
                            "digits": request.digits,
                        },
                    )
                )
            )
        except Exception:
            self._cleanup_postprocessor_locked()
            return None

    def _cleanup_postprocessor_locked(self, state: PiComputeState | None = None) -> None:
        if self._context is None:
            old_agent_id = None
            if state is not None:
                state.postprocessor_agent_id = ""
            return
        if state is None:
            with self.locked_state() as current_state:
                old_agent_id = str(current_state.postprocessor_agent_id)
                current_state.postprocessor_agent_id = ""
        else:
            old_agent_id = str(state.postprocessor_agent_id)
            state.postprocessor_agent_id = ""

        if not old_agent_id:
            return
        try:
            proxy = self.context.get_proxy(old_agent_id, self.context.address)
            if proxy is not None:
                proxy.dispose()
        except Exception:
            pass

    def _cleanup_postprocessor(self) -> None:
        self._cleanup_postprocessor_locked()

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
        if (not targets or sum(entry[0] for entry in slots_by_host.values()) <= 0) and not has_in_flight:
            targets = self._select_targets(request, ignore_load_limits=True, limit=1)
            slots_by_host = {
                target.snapshot.host_url.rstrip("/"): (1, _host_cpu_count(target.snapshot)) for target in targets[:1]
            }
            fallback_minimum = bool(targets)
        if not targets:
            return

        available_slots = max(
            1, sum((entry[0] if isinstance(entry, tuple) else entry) for entry in slots_by_host.values())
        )
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
            host_base_capacity = host_base_capacity_by_url.get(
                host_url, in_flight_by_host.get(host_url, 0) + host_capacity
            )
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
        host_name = str(spec.get("host_name") or spec["host_url"])
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
                state.errors[_launch_retry_error_key(host_name)] = str(exc)
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
        postprocess_result = False
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
                postprocess_result = True
            else:
                state.errors[result.batch_id] = result.error or result.status
        self.notify_all_state_changed()
        if postprocess_result:
            self._postprocess_send_result(result)
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

    def summary(self) -> PiComputeSummary:
        with self.locked_state() as state:
            state_request_wire = dict(state.request)
            state_results = dict(state.results)
            state_errors = dict(state.errors)
            state_cleanup_errors = dict(state.cleanup_errors)
            state_done = bool(state.done)
            state_skipped_count = int(state.skipped_count)
            progress = PiComputeCoordinatorAgent._progress_from_state(state)

        return self._summary_from_state(
            request_wire=state_request_wire,
            progress=progress,
            results=state_results,
            done=state_done,
            skipped_count=state_skipped_count,
            errors=state_errors,
            cleanup_errors=state_cleanup_errors,
        )

    def _summary_from_state(
        self,
        *,
        request_wire: dict[str, Any],
        progress: _PiComputeProgress,
        results: dict[str, dict[str, Any]],
        done: bool,
        skipped_count: int,
        errors: dict[str, str],
        cleanup_errors: dict[str, str],
    ) -> PiComputeSummary:
        request = dataclass_from_wire(PiComputeRequest, request_wire) if request_wire else PiComputeRequest()
        pp_summary = self._postprocessor_summary()
        if pp_summary is not None:
            request = (
                dataclass_from_wire(PiComputeRequest, pp_summary.request) if pp_summary.request else progress.request
            )
            available_digits = int(pp_summary.available_digits)
            decimal_digits = ""
            pi_text = ""
            if request.digits > 0 and progress.done and available_digits > 0:
                reply = self._postprocess_format(request)
                if reply is not None:
                    pi_text = str(reply.get("pi") or "")
                    decimal_digits = str(reply.get("decimal_digits") or "")
            return PiComputeSummary(
                start=request.start,
                digits=request.digits,
                decimal_digits=decimal_digits,
                pi=pi_text,
                terms=progress.total_terms,
                completed_terms=int(pp_summary.completed_terms),
                available_digits=available_digits,
                done=done,
                pending=progress.pending,
                in_flight=progress.in_flight,
                skipped_count=skipped_count,
                results=results,
                errors=errors,
                cleanup_errors=cleanup_errors,
            )
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
            done=done,
            pending=progress.pending,
            in_flight=progress.in_flight,
            skipped_count=skipped_count,
            results=results,
            errors=errors,
            cleanup_errors=cleanup_errors,
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
        self._cleanup_postprocessor()
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


def _launch_retry_error_key(host_name: str) -> str:
    return f"launch:{host_name}"


def _is_launch_retry_error_key(key: str) -> bool:
    return key.startswith("launch:")


def _clear_launch_retry_errors(errors: dict[str, str]) -> None:
    for key in [key for key in errors if _is_launch_retry_error_key(str(key))]:
        errors.pop(key, None)


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
            with contextlib.suppress(Exception):
                self.context.host.dispose(self.agent_id)

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
        batches.append(
            PiBatchRequest(batch_id=f"terms:{term_start}:{term_count}", term_start=term_start, term_count=term_count)
        )
        term_start += term_count
    return batches
