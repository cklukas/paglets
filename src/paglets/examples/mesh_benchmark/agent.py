# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import base64
from dataclasses import dataclass, field
import json
import os
import statistics
import threading
import time
from typing import Any
import uuid

from ...agent import Paglet, PagletState, state_locked
from ...mesh import HostRef
from ...messages import Message
from ...serde import dataclass_from_wire, dataclass_to_wire
from ...transfer import TransferTicket


DEFAULT_CLOCK_PROBES = 5
DEFAULT_DIGITS = 1
DEFAULT_TIMEOUT_SECONDS = 600.0
CONTINUE_DELAY_SECONDS = 0.1
MESH_BENCHMARK_STORAGE_DIR = "mesh-benchmark"


@dataclass(frozen=True, slots=True)
class MeshBenchmarkRequest:
    repeats: int = 1
    payload_size_bytes: int = 0
    include_self: bool = True
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    digits: int = DEFAULT_DIGITS
    clock_probes: int = DEFAULT_CLOCK_PROBES


@dataclass(frozen=True, slots=True)
class MeshBenchmarkHost:
    name: str
    url: str


@dataclass(frozen=True, slots=True)
class MeshRouteEdge:
    source_name: str
    source_url: str
    target_name: str
    target_url: str
    repeat: int
    sequence: int


@dataclass(frozen=True, slots=True)
class ClockOffsetSample:
    host_name: str
    host_url: str
    entry_host_name: str
    entry_host_url: str
    offset_seconds: float
    rtt_seconds: float
    sampled_at: float


@dataclass(frozen=True, slots=True)
class ClockOffsetSummary:
    host_name: str
    host_url: str
    entry_host_name: str
    entry_host_url: str
    sample_count: int
    median_offset_seconds: float
    mean_offset_seconds: float
    best_rtt_offset_seconds: float
    best_rtt_seconds: float


@dataclass(frozen=True, slots=True)
class MessageTimingSummary:
    host_name: str
    host_url: str
    entry_host_name: str
    entry_host_url: str
    sample_count: int
    median_rtt_seconds: float
    mean_rtt_seconds: float
    best_rtt_seconds: float
    worst_rtt_seconds: float


@dataclass(frozen=True, slots=True)
class MeshTravelRecord:
    run_id: str
    sequence: int
    repeat: int
    source_name: str
    source_url: str
    target_name: str
    target_url: str
    source_wall_start: float
    source_wall_end: float
    elapsed_seconds: float
    payload_size_bytes: int
    clock_offset: ClockOffsetSummary | None = None


@dataclass(frozen=True, slots=True)
class MeshBenchmarkSummary:
    run_id: str
    entry_host_name: str
    entry_host_url: str
    hosts: list[MeshBenchmarkHost] = field(default_factory=list)
    records: list[MeshTravelRecord] = field(default_factory=list)
    matrix_seconds: dict[str, dict[str, float]] = field(default_factory=dict)
    clock_offsets: list[ClockOffsetSummary] = field(default_factory=list)
    clock_samples: list[ClockOffsetSample] = field(default_factory=list)
    message_timings: list[MessageTimingSummary] = field(default_factory=list)
    movement_count: int = 0
    measured_round_trip_seconds: float = 0.0
    setup_seconds: float = 0.0
    total_elapsed_seconds: float = 0.0
    measured_overhead_seconds: float = 0.0
    overall_benchmark_seconds: float = 0.0
    average_elapsed_seconds: float = 0.0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass
class MeshBenchmarkCoordinatorState(PagletState):
    request: dict[str, Any] = field(default_factory=dict)
    run_id: str = ""
    started_at: float = 0.0
    deadline: float = 0.0
    done: bool = False
    summary: dict[str, Any] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    traveler_proxy: dict[str, str] = field(default_factory=dict)


@dataclass
class MeshBenchmarkTravelerState(PagletState):
    run_id: str = ""
    request: dict[str, Any] = field(default_factory=dict)
    hosts: list[dict[str, str]] = field(default_factory=list)
    route_edges: list[dict[str, Any]] = field(default_factory=list)
    route_index: int = 0
    phase: str = "measure"
    payload: str = ""
    pending_edge: dict[str, Any] = field(default_factory=dict)
    awaiting_timing: bool = False
    arrival_local_time: float = 0.0
    overall_started_at: float = 0.0
    measured_started_at: float = 0.0
    measured_finished_at: float = 0.0
    coordinator_agent_id: str = ""
    coordinator_host_url: str = ""
    collection_targets: list[str] = field(default_factory=list)
    collection_index: int = 0
    collected_records: list[dict[str, Any]] = field(default_factory=list)
    clock_samples: list[dict[str, Any]] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


class MeshBenchmarkCoordinatorAgent(Paglet[MeshBenchmarkCoordinatorState]):
    """Entry-host coordinator for the directional mesh movement benchmark."""

    State = MeshBenchmarkCoordinatorState

    def handle_message(self, message: Message):
        if message.kind == "start":
            return self.start(message.args)
        if message.kind == "drain":
            return self.drain(wait_timeout=float(message.args.get("wait_timeout", 0.5)))
        if message.kind == "clock_probe":
            received_at = time.time()
            return {"received_at": received_at, "sent_at": time.time()}
        if message.kind == "traveler_done":
            return self.record_summary(dict(message.args.get("summary") or {}))
        if message.kind == "traveler_error":
            return self.record_error(str(message.args.get("host") or "traveler"), str(message.args.get("error") or ""))
        if message.kind == "summary":
            return self.summary()
        return self.not_handled()

    def start(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = dataclass_from_wire(MeshBenchmarkRequest, dict(payload.get("request") or {}))
        request = normalize_request(request)
        hosts = _ordered_hosts(
            self.context.available_hosts(online_only=True, include_self=True),
            entry_name=self.context.name,
            entry_url=self.context.address,
        )
        if not hosts:
            raise ValueError("no online mesh hosts are available")
        route_edges = build_route_edges(hosts, repeats=request.repeats, include_self=request.include_self)
        run_id = uuid.uuid4().hex
        started_at = time.time()
        state = MeshBenchmarkTravelerState(
            run_id=run_id,
            request=dataclass_to_wire(request),
            hosts=[dataclass_to_wire(host) for host in hosts],
            route_edges=[dataclass_to_wire(edge) for edge in route_edges],
            payload=random_ascii(request.payload_size_bytes),
            overall_started_at=started_at,
            coordinator_agent_id=self.agent_id,
            coordinator_host_url=self.context.address,
        )
        with self.locked_state() as coordinator_state:
            coordinator_state.request = dataclass_to_wire(request)
            coordinator_state.run_id = run_id
            coordinator_state.started_at = started_at
            coordinator_state.deadline = time.monotonic() + max(0.0, request.timeout_seconds)
            coordinator_state.done = False
            coordinator_state.summary = {}
            coordinator_state.errors = {}
            coordinator_state.traveler_proxy = {}
        traveler = self.context.create_paglet(MeshBenchmarkTravelerAgent, state)
        with self.locked_state() as coordinator_state:
            coordinator_state.traveler_proxy = traveler.to_wire()
        traveler.send_oneway(Message("continue"))
        return self.summary()

    def drain(self, *, wait_timeout: float) -> dict[str, Any]:
        self._expire_if_needed()
        return self.summary()

    @state_locked
    def record_summary(self, summary: dict[str, Any]) -> dict[str, Any]:
        self.state.summary = summary
        self.state.done = True
        self.notify_all_state_changed()
        return {"ok": True}

    @state_locked
    def record_error(self, host: str, error: str) -> dict[str, Any]:
        self.state.errors[host] = error
        return {"ok": True}

    @state_locked
    def summary(self) -> dict[str, Any]:
        summary = dict(self.state.summary)
        if self.state.errors:
            existing = dict(summary.get("errors") or {})
            existing.update(self.state.errors)
            summary["errors"] = existing
        return {
            "done": self.state.done,
            "run_id": self.state.run_id,
            "summary": summary,
            "errors": dict(self.state.errors),
        }

    def _expire_if_needed(self) -> None:
        with self.locked_state() as state:
            if state.done or state.deadline <= 0 or time.monotonic() < state.deadline:
                return
            state.done = True
            state.errors["timeout"] = "timed out waiting for mesh benchmark traveler"
            traveler_proxy = dict(state.traveler_proxy)
        self.notify_all_state_changed()
        self._dispose_traveler(traveler_proxy)

    def _dispose_traveler(self, proxy_wire: dict[str, str]) -> None:
        if not proxy_wire:
            return
        try:
            proxy = self.context.get_proxy(str(proxy_wire["agent_id"]), str(proxy_wire["host_url"]))
            if proxy is not None:
                proxy.dispose()
        except Exception:
            pass


class MeshBenchmarkTravelerAgent(Paglet[MeshBenchmarkTravelerState]):
    """Mobile traveler that measures directed movement edges across the mesh."""

    State = MeshBenchmarkTravelerState

    def run(self) -> None:
        arrival_local_time = time.time()
        with self.locked_state() as state:
            if state.awaiting_timing:
                state.arrival_local_time = arrival_local_time
                self._schedule_complete_arrival()
            elif state.phase == "collect":
                self._schedule_continue()

    def handle_message(self, message: Message):
        if message.kind == "hop_timing":
            self._record_hop_timing(dict(message.args))
            self._schedule_continue()
            return {"ok": True}
        if message.kind == "continue":
            self._continue()
            return {"ok": True}
        return self.not_handled()

    def _schedule_complete_arrival(self) -> None:
        timer = threading.Timer(CONTINUE_DELAY_SECONDS, self._complete_arrival)
        timer.name = f"paglets-mesh-benchmark-arrival-{self.context.name}"
        timer.daemon = True
        timer.start()

    def _schedule_continue(self) -> None:
        with self.locked_state() as state:
            if state.awaiting_timing:
                return
        timer = threading.Timer(CONTINUE_DELAY_SECONDS, self._continue)
        timer.name = f"paglets-mesh-benchmark-{self.context.name}"
        timer.daemon = True
        timer.start()

    def _continue(self) -> None:
        try:
            with self.locked_state() as state:
                phase = state.phase
                awaiting = state.awaiting_timing
            if awaiting:
                return
            if phase == "measure":
                self._continue_measuring()
            elif phase == "collect":
                self._continue_collecting()
        except Exception as exc:
            self._record_local_error(str(exc))
            self._notify_coordinator_error(str(exc))

    def _continue_measuring(self) -> None:
        with self.locked_state() as state:
            if state.route_index >= len(state.route_edges):
                state.measured_finished_at = time.time()
                state.phase = "collect"
                state.collection_targets = [str(host["url"]) for host in state.hosts]
                state.collection_index = 0
                state.awaiting_timing = False
                state.pending_edge = {}
                state.payload = ""
                phase_changed = True
            else:
                edge_wire = dict(state.route_edges[state.route_index])
                if state.route_index == 0:
                    state.measured_started_at = time.time()
                state.pending_edge = edge_wire
                state.route_index += 1
                state.awaiting_timing = True
                phase_changed = False
        if phase_changed:
            self._continue_collecting()
            return

        edge = dataclass_from_wire(MeshRouteEdge, edge_wire)
        request = dataclass_from_wire(MeshBenchmarkRequest, dict(self.state.request))
        source_wall_start, start_samples = self._probe_entry_time(count=request.clock_probes)
        with self.locked_state() as state:
            state.pending_edge = dict(edge_wire)
            state.pending_edge["source_wall_start"] = source_wall_start
            state.clock_samples.extend(dataclass_to_wire(sample) for sample in start_samples)
        self.dispatch(benchmark_transfer_ticket(edge.target_url, request))

    def _complete_arrival(self) -> None:
        with self.locked_state() as state:
            if not state.awaiting_timing:
                return
            edge_wire = dict(state.pending_edge)
            request = dataclass_from_wire(MeshBenchmarkRequest, dict(state.request))
            arrival_local_time = state.arrival_local_time or time.time()
        edge = dataclass_from_wire(MeshRouteEdge, edge_wire)
        source_wall_start = float(edge_wire.get("source_wall_start", 0.0))
        entry_time, arrival_samples = self._probe_entry_time(
            count=request.clock_probes,
            local_reference=arrival_local_time,
        )
        elapsed = max(0.0, entry_time - source_wall_start)
        self._record_arrival_timing(
            edge,
            source_wall_start=source_wall_start,
            source_wall_end=entry_time,
            elapsed_seconds=elapsed,
            clock_samples=arrival_samples,
            request=request,
        )
        self._continue()

    def _continue_collecting(self) -> None:
        self._collect_local_records()
        with self.locked_state() as state:
            while state.collection_index < len(state.collection_targets):
                target = state.collection_targets[state.collection_index]
                state.collection_index += 1
                if target.rstrip("/") == self.context.address.rstrip("/"):
                    continue
                break
            else:
                target = ""
        if target:
            request = dataclass_from_wire(MeshBenchmarkRequest, dict(self.state.request))
            self.dispatch(benchmark_transfer_ticket(target, request))
            return
        self._finish()

    def _record_hop_timing(self, payload: dict[str, Any]) -> None:
        with self.locked_state() as state:
            edge_wire = dict(state.pending_edge)
            request = dataclass_from_wire(MeshBenchmarkRequest, dict(state.request))
            state.awaiting_timing = False
            state.pending_edge = {}
        edge = dataclass_from_wire(MeshRouteEdge, edge_wire)
        _entry_time, clock_samples = self._probe_entry_time(count=request.clock_probes)
        clock_summary = summarize_clock_samples(clock_samples)
        record = MeshTravelRecord(
            run_id=self.state.run_id,
            sequence=edge.sequence,
            repeat=edge.repeat,
            source_name=edge.source_name,
            source_url=edge.source_url,
            target_name=edge.target_name,
            target_url=edge.target_url,
            source_wall_start=float(payload.get("source_wall_start", 0.0)),
            source_wall_end=float(payload.get("source_wall_end", 0.0)),
            elapsed_seconds=float(payload.get("elapsed_seconds", 0.0)),
            payload_size_bytes=request.payload_size_bytes,
            clock_offset=clock_summary,
        )
        self._append_local_record(record)
        with self.locked_state() as state:
            state.clock_samples.extend(dataclass_to_wire(sample) for sample in clock_samples)

    def _record_arrival_timing(
        self,
        edge: MeshRouteEdge,
        *,
        source_wall_start: float,
        source_wall_end: float,
        elapsed_seconds: float,
        clock_samples: list[ClockOffsetSample],
        request: MeshBenchmarkRequest,
    ) -> None:
        clock_summary = summarize_clock_samples(clock_samples)
        record = MeshTravelRecord(
            run_id=self.state.run_id,
            sequence=edge.sequence,
            repeat=edge.repeat,
            source_name=edge.source_name,
            source_url=edge.source_url,
            target_name=edge.target_name,
            target_url=edge.target_url,
            source_wall_start=source_wall_start,
            source_wall_end=source_wall_end,
            elapsed_seconds=elapsed_seconds,
            payload_size_bytes=request.payload_size_bytes,
            clock_offset=clock_summary,
        )
        self._append_local_record(record)
        with self.locked_state() as state:
            state.awaiting_timing = False
            state.pending_edge = {}
            state.arrival_local_time = 0.0
            state.clock_samples.extend(dataclass_to_wire(sample) for sample in clock_samples)

    def _probe_entry_time(
        self,
        *,
        count: int,
        local_reference: float | None = None,
    ) -> tuple[float, list[ClockOffsetSample]]:
        with self.locked_state() as state:
            coordinator_agent_id = state.coordinator_agent_id
            coordinator_host_url = state.coordinator_host_url
        proxy = self.context.get_proxy(coordinator_agent_id, coordinator_host_url)
        if proxy is None:
            reference = time.time() if local_reference is None else local_reference
            return reference, []
        samples: list[ClockOffsetSample] = []
        entry_host = self._entry_host()
        for _ in range(max(1, int(count))):
            local_send = time.time()
            reply = proxy.send(Message("clock_probe", {"client_sent_at": local_send}))
            local_receive = time.time()
            entry_receive = float(reply.get("received_at", 0.0))
            entry_send = float(reply.get("sent_at", entry_receive))
            rtt = max(0.0, (local_receive - local_send) - (entry_send - entry_receive))
            samples.append(
                ClockOffsetSample(
                    host_name=self.context.name,
                    host_url=self.context.address,
                    entry_host_name=entry_host.name,
                    entry_host_url=coordinator_host_url,
                    offset_seconds=local_minus_entry_offset(local_send, local_receive, entry_receive, entry_send),
                    rtt_seconds=rtt,
                    sampled_at=local_receive,
                )
            )
        reference = time.time() if local_reference is None else local_reference
        return entry_time_for_local_reference(reference, samples), samples

    def _entry_host(self) -> MeshBenchmarkHost:
        with self.locked_state() as state:
            entry_url = state.coordinator_host_url.rstrip("/")
            hosts = [dataclass_from_wire(MeshBenchmarkHost, dict(host)) for host in state.hosts]
        for host in hosts:
            if host.url.rstrip("/") == entry_url:
                return host
        return MeshBenchmarkHost("entry", entry_url)

    def _append_local_record(self, record: MeshTravelRecord) -> None:
        path = _storage_path(record.run_id)
        storage = self.persistent_storage()
        records = _read_record_list(storage, path)
        records.append(dataclass_to_wire(record))
        storage.write_text(path, json.dumps(records, sort_keys=True))

    def _collect_local_records(self) -> None:
        with self.locked_state() as state:
            run_id = state.run_id
        path = _storage_path(run_id)
        storage = self.persistent_storage()
        records = _read_record_list(storage, path)
        if records:
            with self.locked_state() as state:
                state.collected_records.extend(dict(record) for record in records if isinstance(record, dict))
        storage.delete(path)

    def _finish(self) -> None:
        with self.locked_state() as state:
            hosts = [dataclass_from_wire(MeshBenchmarkHost, dict(host)) for host in state.hosts]
            records = [dataclass_from_wire(MeshTravelRecord, dict(item)) for item in state.collected_records]
            samples = [dataclass_from_wire(ClockOffsetSample, dict(item)) for item in state.clock_samples]
            errors = dict(state.errors)
            run_id = state.run_id
            setup_seconds = max(0.0, state.measured_started_at - state.overall_started_at)
            measured_round_trip_seconds = (
                max(0.0, state.measured_finished_at - state.measured_started_at)
                if state.measured_started_at > 0
                else 0.0
            )
            overall_benchmark_seconds = max(0.0, time.time() - state.overall_started_at)
        summary = build_summary(
            run_id=run_id,
            entry_host=hosts[0],
            hosts=hosts,
            records=records,
            clock_samples=samples,
            measured_round_trip_seconds=measured_round_trip_seconds,
            setup_seconds=setup_seconds,
            overall_benchmark_seconds=overall_benchmark_seconds,
            errors=errors,
        )
        coordinator = self.context.get_proxy(self.state.coordinator_agent_id, self.state.coordinator_host_url)
        if coordinator is not None:
            coordinator.send(Message("traveler_done", {"summary": dataclass_to_wire(summary)}))
        try:
            self.context.host.dispose(self.agent_id)
        except Exception:
            pass

    def _record_local_error(self, error: str) -> None:
        with self.locked_state() as state:
            state.errors[self.context.name] = error

    def _notify_coordinator_error(self, error: str) -> None:
        with self.locked_state() as state:
            coordinator_agent_id = state.coordinator_agent_id
            coordinator_host_url = state.coordinator_host_url
        try:
            coordinator = self.context.get_proxy(coordinator_agent_id, coordinator_host_url)
            if coordinator is not None:
                coordinator.send(Message("traveler_error", {"host": self.context.name, "error": error}))
        except Exception:
            pass


def normalize_request(request: MeshBenchmarkRequest) -> MeshBenchmarkRequest:
    return MeshBenchmarkRequest(
        repeats=max(1, int(request.repeats)),
        payload_size_bytes=max(0, int(request.payload_size_bytes)),
        include_self=bool(request.include_self),
        timeout_seconds=max(0.1, float(request.timeout_seconds)),
        digits=max(0, int(request.digits)),
        clock_probes=max(1, int(request.clock_probes)),
    )


def benchmark_transfer_ticket(target_url: str, request: MeshBenchmarkRequest) -> TransferTicket:
    return TransferTicket(destination=target_url, timeout=max(0.1, float(request.timeout_seconds)))


def build_route_edges(
    hosts: list[MeshBenchmarkHost],
    *,
    repeats: int,
    include_self: bool,
) -> list[MeshRouteEdge]:
    if not hosts:
        return []
    route = _eulerian_vertex_route([host.name for host in hosts], start=hosts[0].name, include_self=include_self)
    by_name = {host.name: host for host in hosts}
    edges: list[MeshRouteEdge] = []
    sequence = 0
    for repeat in range(max(1, repeats)):
        for source_name, target_name in zip(route, route[1:]):
            if not include_self and source_name == target_name:
                continue
            source = by_name[source_name]
            target = by_name[target_name]
            edges.append(
                MeshRouteEdge(
                    source_name=source.name,
                    source_url=source.url,
                    target_name=target.name,
                    target_url=target.url,
                    repeat=repeat,
                    sequence=sequence,
                )
            )
            sequence += 1
    return edges


def build_summary(
    *,
    run_id: str,
    entry_host: MeshBenchmarkHost,
    hosts: list[MeshBenchmarkHost],
    records: list[MeshTravelRecord],
    clock_samples: list[ClockOffsetSample],
    measured_round_trip_seconds: float,
    setup_seconds: float = 0.0,
    overall_benchmark_seconds: float | None = None,
    errors: dict[str, str] | None = None,
) -> MeshBenchmarkSummary:
    matrix = aggregate_matrix(records, hosts)
    offsets = aggregate_clock_offsets(clock_samples)
    message_timings = aggregate_message_timings(clock_samples)
    movement_count = len(records)
    total_elapsed = sum(record.elapsed_seconds for record in records)
    average = statistics.fmean(record.elapsed_seconds for record in records) if records else 0.0
    return MeshBenchmarkSummary(
        run_id=run_id,
        entry_host_name=entry_host.name,
        entry_host_url=entry_host.url,
        hosts=hosts,
        records=sorted(records, key=lambda record: record.sequence),
        matrix_seconds=matrix,
        clock_offsets=offsets,
        clock_samples=clock_samples,
        message_timings=message_timings,
        movement_count=movement_count,
        measured_round_trip_seconds=measured_round_trip_seconds,
        setup_seconds=setup_seconds,
        total_elapsed_seconds=total_elapsed,
        measured_overhead_seconds=max(0.0, measured_round_trip_seconds - total_elapsed),
        overall_benchmark_seconds=(
            measured_round_trip_seconds if overall_benchmark_seconds is None else overall_benchmark_seconds
        ),
        average_elapsed_seconds=average,
        errors=errors or {},
    )


def aggregate_matrix(records: list[MeshTravelRecord], hosts: list[MeshBenchmarkHost]) -> dict[str, dict[str, float]]:
    values: dict[tuple[str, str], list[float]] = {}
    for record in records:
        values.setdefault((record.source_name, record.target_name), []).append(record.elapsed_seconds)
    matrix: dict[str, dict[str, float]] = {host.name: {} for host in hosts}
    for source in hosts:
        row = matrix[source.name]
        for target in hosts:
            samples = values.get((source.name, target.name), [])
            if samples:
                row[target.name] = statistics.fmean(samples)
    return matrix


def aggregate_clock_offsets(samples: list[ClockOffsetSample]) -> list[ClockOffsetSummary]:
    grouped: dict[str, list[ClockOffsetSample]] = {}
    for sample in samples:
        grouped.setdefault(sample.host_name, []).append(sample)
    summaries: list[ClockOffsetSummary] = []
    for host_name, host_samples in sorted(grouped.items()):
        best = min(host_samples, key=lambda sample: sample.rtt_seconds)
        summaries.append(
            ClockOffsetSummary(
                host_name=host_name,
                host_url=best.host_url,
                entry_host_name=best.entry_host_name,
                entry_host_url=best.entry_host_url,
                sample_count=len(host_samples),
                median_offset_seconds=statistics.median(sample.offset_seconds for sample in host_samples),
                mean_offset_seconds=statistics.fmean(sample.offset_seconds for sample in host_samples),
                best_rtt_offset_seconds=best.offset_seconds,
                best_rtt_seconds=best.rtt_seconds,
            )
        )
    return summaries


def aggregate_message_timings(samples: list[ClockOffsetSample]) -> list[MessageTimingSummary]:
    grouped: dict[str, list[ClockOffsetSample]] = {}
    for sample in samples:
        grouped.setdefault(sample.host_name, []).append(sample)
    summaries: list[MessageTimingSummary] = []
    for host_name, host_samples in sorted(grouped.items()):
        best = min(host_samples, key=lambda sample: sample.rtt_seconds)
        worst = max(host_samples, key=lambda sample: sample.rtt_seconds)
        summaries.append(
            MessageTimingSummary(
                host_name=host_name,
                host_url=best.host_url,
                entry_host_name=best.entry_host_name,
                entry_host_url=best.entry_host_url,
                sample_count=len(host_samples),
                median_rtt_seconds=statistics.median(sample.rtt_seconds for sample in host_samples),
                mean_rtt_seconds=statistics.fmean(sample.rtt_seconds for sample in host_samples),
                best_rtt_seconds=best.rtt_seconds,
                worst_rtt_seconds=worst.rtt_seconds,
            )
        )
    return summaries


def summarize_clock_samples(samples: list[ClockOffsetSample]) -> ClockOffsetSummary | None:
    return aggregate_clock_offsets(samples)[0] if samples else None


def entry_time_for_local_reference(local_reference: float, samples: list[ClockOffsetSample]) -> float:
    if not samples:
        return local_reference
    best = min(samples, key=lambda sample: sample.rtt_seconds)
    return local_reference - best.offset_seconds


def local_minus_entry_offset(
    local_send: float,
    local_receive: float,
    entry_receive: float,
    entry_send: float,
) -> float:
    return ((local_send - entry_receive) + (local_receive - entry_send)) / 2.0


def random_ascii(size: int) -> str:
    if size <= 0:
        return ""
    random_bytes = os.urandom((size * 3 + 3) // 4)
    return base64.b64encode(random_bytes).decode("ascii")[:size]


def parse_size(value: str) -> int:
    text = value.strip()
    if not text:
        raise ValueError("size cannot be empty")
    unit = text[-1].upper()
    if unit in {"K", "M", "G"}:
        number = text[:-1]
        multiplier = {"K": 1024, "M": 1024**2, "G": 1024**3}[unit]
    else:
        number = text[:-1] if unit == "B" else text
        multiplier = 1
    try:
        amount = float(number)
    except ValueError as exc:
        raise ValueError(f"invalid size {value!r}") from exc
    if amount < 0:
        raise ValueError("size must be non-negative")
    return int(amount * multiplier)


def _eulerian_vertex_route(host_names: list[str], *, start: str, include_self: bool) -> list[str]:
    adjacency: dict[str, list[str]] = {}
    for source in sorted(host_names, reverse=True):
        targets = [target for target in sorted(host_names, reverse=True) if include_self or target != source]
        adjacency[source] = targets
    stack = [start]
    circuit: list[str] = []
    while stack:
        vertex = stack[-1]
        targets = adjacency.get(vertex, [])
        if targets:
            stack.append(targets.pop())
        else:
            circuit.append(stack.pop())
    return list(reversed(circuit))


def _ordered_hosts(hosts: list[HostRef], *, entry_name: str, entry_url: str) -> list[MeshBenchmarkHost]:
    seen: set[str] = set()
    ordered: list[MeshBenchmarkHost] = [MeshBenchmarkHost(entry_name, entry_url.rstrip("/"))]
    seen.add(entry_url.rstrip("/"))
    for host in sorted(hosts, key=lambda item: item.name):
        url = host.url.rstrip("/")
        if url in seen:
            continue
        ordered.append(MeshBenchmarkHost(host.name, url))
        seen.add(url)
    return ordered


def _storage_path(run_id: str) -> str:
    return f"{MESH_BENCHMARK_STORAGE_DIR}/{run_id}.json"


def _read_record_list(storage: Any, path: str) -> list[Any]:
    try:
        raw = storage.read_bytes(path).decode("utf-8")
        records = json.loads(raw)
        return records if isinstance(records, list) else []
    except FileNotFoundError:
        return []
    except Exception as exc:
        if "No such file or directory" in str(exc):
            return []
        raise
