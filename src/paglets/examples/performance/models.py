# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

DEFAULT_BENCHMARK_DURATION_SECONDS = 1.0
DEFAULT_DISK_SIZE_BYTES = 128 * 1024 * 1024
DEFAULT_LOCK_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class BenchmarkRequest:
    include_cpu: bool = True
    include_memory: bool = True
    include_disk: bool = True
    duration_seconds: float = DEFAULT_BENCHMARK_DURATION_SECONDS
    disk_size_bytes: int = DEFAULT_DISK_SIZE_BYTES
    workers: int = 0
    paths: list[str] = field(default_factory=list)
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS


@dataclass(frozen=True, slots=True)
class BenchmarkMetric:
    name: str
    duration_seconds: float
    operations: int = 0
    operations_per_second: float = 0.0
    bytes_processed: int = 0
    bytes_per_second: float = 0.0


@dataclass(frozen=True, slots=True)
class CpuBenchmarkResult:
    workers: int
    logical_cpus: int
    physical_cpus: int | None
    single_core: list[BenchmarkMetric] = field(default_factory=list)
    multi_core: list[BenchmarkMetric] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkResult:
    metrics: list[BenchmarkMetric] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class DiskTarget:
    path: str
    device: str = ""
    filesystem: str = ""


@dataclass(frozen=True, slots=True)
class DiskSkip:
    path: str
    reason: str
    device: str = ""
    filesystem: str = ""


@dataclass(frozen=True, slots=True)
class DiskVolumeBenchmark:
    path: str
    device: str
    filesystem: str
    total_bytes: int
    free_bytes_before: int
    benchmark_size_bytes: int
    write_seconds: float
    write_bytes_per_second: float
    fsync_seconds: float
    read_seconds: float
    read_bytes_per_second: float
    metadata_files_per_second: float


@dataclass(frozen=True, slots=True)
class DiskBenchmarkResult:
    volumes: list[DiskVolumeBenchmark] = field(default_factory=list)
    skipped: list[DiskSkip] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class HostBenchmarkResult:
    host_name: str
    host_url: str
    platform: str
    python_version: str
    lock_wait_seconds: float = 0.0
    cpu: CpuBenchmarkResult | None = None
    memory: MemoryBenchmarkResult | None = None
    disk: DiskBenchmarkResult | None = None
    errors: list[str] = field(default_factory=list)
