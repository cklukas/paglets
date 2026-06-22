# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import base64
import itertools
import json
import os
import statistics
from typing import Any

from paglets.remote.mesh import HostRef
from paglets.remote.transfer import TransferTicket

from .models import (
    MESH_BENCHMARK_STORAGE_DIR,
    ClockOffsetSample,
    ClockOffsetSummary,
    MeshBenchmarkHost,
    MeshBenchmarkRequest,
    MeshBenchmarkSummary,
    MeshRouteEdge,
    MeshTravelRecord,
    MessageTimingSummary,
    PayloadTransferSpeedSummary,
)


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
        for source_name, target_name in itertools.pairwise(route):
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
    payload_transfer_speeds = aggregate_payload_transfer_speeds(records, hosts)
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
        payload_transfer_speeds=payload_transfer_speeds,
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


def aggregate_payload_transfer_speeds(
    records: list[MeshTravelRecord],
    hosts: list[MeshBenchmarkHost],
) -> list[PayloadTransferSpeedSummary]:
    grouped: dict[tuple[str, str], list[MeshTravelRecord]] = {}
    for record in records:
        if record.payload_size_bytes <= 0 or record.elapsed_seconds <= 0:
            continue
        relation = "self" if record.source_url.rstrip("/") == record.target_url.rstrip("/") else "other"
        grouped.setdefault((record.target_name, relation), []).append(record)

    by_name = {host.name: host for host in hosts}
    summaries: list[PayloadTransferSpeedSummary] = []
    for (host_name, relation), host_records in sorted(grouped.items(), key=lambda item: (item[0][1], item[0][0])):
        payload_bytes = sum(max(0, int(record.payload_size_bytes)) for record in host_records)
        elapsed_seconds = sum(max(0.0, float(record.elapsed_seconds)) for record in host_records)
        if elapsed_seconds <= 0:
            continue
        host = by_name.get(host_name, MeshBenchmarkHost(host_name, host_records[0].target_url))
        summaries.append(
            PayloadTransferSpeedSummary(
                host_name=host.name,
                host_url=host.url,
                relation=relation,
                sample_count=len(host_records),
                payload_bytes=payload_bytes,
                elapsed_seconds=elapsed_seconds,
                bytes_per_second=payload_bytes / elapsed_seconds,
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
