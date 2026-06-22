# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

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
class PayloadTransferSpeedSummary:
    host_name: str
    host_url: str
    relation: str
    sample_count: int
    payload_bytes: int
    elapsed_seconds: float
    bytes_per_second: float


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
    payload_transfer_speeds: list[PayloadTransferSpeedSummary] = field(default_factory=list)
    movement_count: int = 0
    measured_round_trip_seconds: float = 0.0
    setup_seconds: float = 0.0
    total_elapsed_seconds: float = 0.0
    measured_overhead_seconds: float = 0.0
    overall_benchmark_seconds: float = 0.0
    average_elapsed_seconds: float = 0.0
    errors: dict[str, str] = field(default_factory=dict)
