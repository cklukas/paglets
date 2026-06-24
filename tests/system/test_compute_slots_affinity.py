# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import paglets.system.compute_slots.affinity as affinity
from paglets.system.compute_slots import apply_current_process_cpu_affinity


def test_apply_cpu_affinity_linux_uses_sched_setaffinity(monkeypatch):
    calls: list[tuple[int, set[int]]] = []

    monkeypatch.setattr(affinity.platform, "system", lambda: "Linux")
    monkeypatch.setattr(affinity.os, "sched_setaffinity", lambda pid, ids: calls.append((pid, set(ids))), raising=False)

    result = apply_current_process_cpu_affinity([2, 1, 1])

    assert result.supported is True
    assert result.enforced is True
    assert result.requested_cpu_ids == [1, 2]
    assert calls == [(0, {1, 2})]


def test_apply_cpu_affinity_windows_uses_psutil(monkeypatch):
    calls: list[list[int]] = []

    monkeypatch.setattr(affinity.platform, "system", lambda: "Windows")
    monkeypatch.setattr(affinity, "_set_psutil_cpu_affinity", lambda pid, ids: calls.append([pid, *ids]))

    result = apply_current_process_cpu_affinity([3])

    assert result.supported is True
    assert result.enforced is True
    assert calls == [[0, 3]]


def test_apply_cpu_affinity_macos_reports_unsupported(monkeypatch):
    monkeypatch.setattr(affinity.platform, "system", lambda: "Darwin")

    result = apply_current_process_cpu_affinity([0])

    assert result.supported is False
    assert result.enforced is False
    assert "Darwin" in result.error
