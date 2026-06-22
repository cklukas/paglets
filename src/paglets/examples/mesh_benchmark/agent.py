# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from paglets.core.agent import Paglet, PagletState, state_locked
from paglets.core.messages import Message
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire

from .analysis import (
    _ordered_hosts,
    _read_record_list,
    _storage_path,
    benchmark_transfer_ticket,
    build_route_edges,
    build_summary,
    entry_time_for_local_reference,
    local_minus_entry_offset,
    normalize_request,
    random_ascii,
    summarize_clock_samples,
)
from .models import (
    CONTINUE_DELAY_SECONDS,
    ClockOffsetSample,
    MeshBenchmarkHost,
    MeshBenchmarkRequest,
    MeshRouteEdge,
    MeshTravelRecord,
)


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
        with contextlib.suppress(Exception):
            self.context.host.dispose(self.agent_id)

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
