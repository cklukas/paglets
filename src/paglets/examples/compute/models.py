# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

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
POSTPROCESSOR_STREAM_CHUNK_DIGITS = 8192
MAX_PARALLEL_WORKER_LAUNCHES = 32
TARGET_SELECTION_TIMEOUT_SECONDS = 1.0


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
class PiPostProcessStreamRequest:
    after_digits: int = 0
    max_digits: int = POSTPROCESSOR_STREAM_CHUNK_DIGITS


@dataclass(frozen=True, slots=True)
class PiPostProcessSummary:
    request: dict[str, Any]
    completed_terms: int
    available_digits: int
    done: bool


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
