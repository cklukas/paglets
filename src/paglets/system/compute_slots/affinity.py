# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import os
import platform
from collections.abc import Sequence
from dataclasses import dataclass, field

import psutil


@dataclass(frozen=True, slots=True)
class CpuAffinityResult:
    requested_cpu_ids: list[int] = field(default_factory=list)
    supported: bool = False
    enforced: bool = False
    error: str = ""


def apply_current_process_cpu_affinity(cpu_core_ids: Sequence[int]) -> CpuAffinityResult:
    """Best-effort pin the current process to the granted CPU IDs."""

    return apply_process_cpu_affinity(0, cpu_core_ids)


def apply_process_cpu_affinity(pid: int, cpu_core_ids: Sequence[int]) -> CpuAffinityResult:
    """Best-effort pin a process to the requested CPU IDs."""

    requested = _normalize_cpu_ids(cpu_core_ids)
    if not requested:
        return CpuAffinityResult(requested_cpu_ids=[])

    system = platform.system()
    try:
        if system == "Linux" and hasattr(os, "sched_setaffinity"):
            os.sched_setaffinity(int(pid), set(requested))
            return CpuAffinityResult(requested_cpu_ids=requested, supported=True, enforced=True)
        if system in {"Windows", "FreeBSD"}:
            _set_psutil_cpu_affinity(int(pid), requested)
            return CpuAffinityResult(requested_cpu_ids=requested, supported=True, enforced=True)
    except Exception as exc:
        return CpuAffinityResult(requested_cpu_ids=requested, supported=True, enforced=False, error=str(exc))

    return CpuAffinityResult(
        requested_cpu_ids=requested,
        supported=False,
        enforced=False,
        error=f"CPU affinity is not supported on {system or 'this platform'}",
    )


def _set_psutil_cpu_affinity(pid: int, cpu_core_ids: Sequence[int]) -> None:
    psutil.Process(pid if pid else None).cpu_affinity(list(cpu_core_ids))


def _normalize_cpu_ids(cpu_core_ids: Sequence[int]) -> list[int]:
    normalized: set[int] = set()
    for cpu_id in cpu_core_ids:
        value = int(cpu_id)
        if value >= 0:
            normalized.add(value)
    return sorted(normalized)
