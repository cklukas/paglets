# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import threading
from dataclasses import dataclass, field
from typing import Any

from paglets.core.agent import state_locked
from paglets.patterns.coordination import MeshFanoutMixin, MeshFanoutState
from paglets.patterns.operations import OperationClient, OperationPaglet
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from paglets.services.contracts import EmptyPayload, ServiceOperation

from .kernels import run_host_benchmarks
from .models import BenchmarkRequest


@dataclass
class PerformanceBenchmarkState(MeshFanoutState):
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = 120.0
    results: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PerformanceCollectRequest:
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = 120.0


@dataclass(frozen=True, slots=True)
class PerformanceDrainRequest:
    wait_timeout: float = 0.5


@dataclass(frozen=True, slots=True)
class PerformanceSummaryReply:
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)
    pending_hosts: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class PerformanceDrainReply:
    done: bool = False
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PerformanceChildResultRequest:
    host_name: str = ""
    host_url: str = ""
    result: dict[str, Any] = field(default_factory=dict)
    error: str = ""


PERFORMANCE_COLLECT = ServiceOperation("collect", PerformanceCollectRequest, PerformanceSummaryReply)
PERFORMANCE_DRAIN = ServiceOperation("drain", PerformanceDrainRequest, PerformanceDrainReply)
PERFORMANCE_CHILD_RESULT = ServiceOperation("child_result", PerformanceChildResultRequest, EmptyPayload)
PERFORMANCE_SUMMARY = ServiceOperation("summary", EmptyPayload, PerformanceSummaryReply)
PERFORMANCE_CLEANUP = ServiceOperation("cleanup", EmptyPayload, PerformanceSummaryReply)


class PerformanceBenchmarkAgent(MeshFanoutMixin, OperationPaglet[PerformanceBenchmarkState]):
    """Clone across the mesh and run local host performance benchmarks."""

    State = PerformanceBenchmarkState
    Operations = (
        PERFORMANCE_COLLECT,
        PERFORMANCE_DRAIN,
        PERFORMANCE_CHILD_RESULT,
        PERFORMANCE_SUMMARY,
        PERFORMANCE_CLEANUP,
    )

    def operation_handlers(self):
        return {
            PERFORMANCE_COLLECT: self.collect,
            PERFORMANCE_DRAIN: self.drain,
            PERFORMANCE_CHILD_RESULT: self.record_child_result,
            PERFORMANCE_SUMMARY: self.summary,
            PERFORMANCE_CLEANUP: self.cleanup_children,
        }

    def run(self) -> None:
        with self.locked_state() as state:
            is_child = state.role == "child"
        if is_child:
            thread = threading.Thread(
                target=self._run_child,
                name=f"paglets-benchmark-{self.context.name}",
                daemon=True,
            )
            thread.start()

    def collect(self, request: PerformanceCollectRequest) -> PerformanceSummaryReply:
        self.fanout_reset(timeout=request.timeout)
        with self.locked_state() as state:
            state.request = dict(request.request)
            state.timeout = float(request.timeout)
            state.results = {}
        hosts = self.fanout_available_hosts(include_self=True)

        for host in hosts:
            self.fanout_prepare_clone(host)
            try:
                child = self.clone_to(host.name)
                self.fanout_record_child_proxy(host.name, child)
            except Exception as exc:
                self.fanout_record_error(host.name, str(exc))
            finally:
                self.fanout_finish_clone_prepare()

        return self.summary()

    def drain(self, request: PerformanceDrainRequest) -> PerformanceDrainReply:
        self._expire_timed_out_hosts()

        def ready(state: PerformanceBenchmarkState) -> bool:
            return not state.pending_hosts

        self.fanout_wait_for(ready, wait_timeout=request.wait_timeout)
        self._expire_timed_out_hosts()
        summary = self.summary()
        return PerformanceDrainReply(done=not summary.pending_hosts, summary=dataclass_to_wire(summary))

    def _run_child(self) -> None:
        with self.locked_state() as state:
            request_wire = dict(state.request)
            target_host_name = state.target_host_name
            target_host_url = state.target_host_url
            parent_agent_id = state.parent_agent_id
            parent_host_url = state.parent_host_url
        try:
            request = dataclass_from_wire(BenchmarkRequest, request_wire)
            result = run_host_benchmarks(
                request,
                host_name=self.context.name,
                host_url=self.context.address,
            )
            payload = {
                "host_name": target_host_name or self.context.name,
                "host_url": target_host_url or self.context.address,
                "result": dataclass_to_wire(result),
                "error": "",
            }
        except Exception as exc:
            payload = {
                "host_name": target_host_name or self.context.name,
                "host_url": target_host_url or self.context.address,
                "result": {},
                "error": str(exc),
            }

        parent = self.context.get_proxy(parent_agent_id, parent_host_url)
        try:
            if parent is not None:
                OperationClient(parent).call(PERFORMANCE_CHILD_RESULT, PerformanceChildResultRequest(**payload))
        finally:
            with contextlib.suppress(Exception):
                self.context.host.dispose(self.agent_id)

    def record_child_result(self, request: PerformanceChildResultRequest) -> EmptyPayload:
        host_name = str(request.host_name)
        with self.locked_state() as state:
            state.pending_hosts = [name for name in state.pending_hosts if name != host_name]
            if host_name and host_name not in state.done_hosts:
                state.done_hosts.append(host_name)
            if request.error:
                state.errors[host_name] = request.error
            else:
                state.results[host_name] = {
                    "host_url": request.host_url,
                    "result": dict(request.result),
                }
        self.notify_all_state_changed()
        return EmptyPayload()

    @state_locked
    def summary(self, request: EmptyPayload | None = None) -> PerformanceSummaryReply:
        _ = request
        return PerformanceSummaryReply(
            results=dict(self.state.results),
            errors=dict(self.state.errors),
            cleanup_errors=dict(self.state.cleanup_errors),
            pending_hosts=list(self.state.pending_hosts),
        )

    def cleanup_children(self, request: EmptyPayload | None = None) -> PerformanceSummaryReply:
        _ = request
        self.fanout_cleanup_children()
        return self.summary()

    def _expire_timed_out_hosts(self) -> None:
        self.fanout_expire_pending("timed out waiting for benchmark result")
