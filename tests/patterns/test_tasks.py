# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from paglets.patterns.tasks import TaskClient, TaskPaglet, TaskState, TaskStatus
from paglets.runtime.host import Host
from tests.support import free_port


@dataclass(frozen=True, slots=True)
class AddRequest:
    value: int = 0


@dataclass(frozen=True, slots=True)
class AddResult:
    value: int = 0


@dataclass
class AddState(TaskState):
    pass


class AddTaskPaglet(TaskPaglet[AddRequest, AddResult, AddState]):
    State = AddState
    Request = AddRequest
    Result = AddResult

    def run_task(self, request: AddRequest) -> AddResult:
        return AddResult(request.value + 1)


class FailingTaskPaglet(AddTaskPaglet):
    def run_task(self, request: AddRequest) -> AddResult:
        _ = request
        raise RuntimeError("forced failure")


def test_task_paglet_start_status_and_wait_are_typed(tmp_path: Path):
    host = _host(tmp_path)
    host.start_background()
    try:
        proxy = host.create(AddTaskPaglet, AddState())
        task = TaskClient.for_paglet(proxy, AddTaskPaglet)

        initial = task.status()
        summary = task.start_and_wait(AddRequest(4), wait_timeout=0.0)
        later = task.wait(wait_timeout=0.0)

        assert initial.status is TaskStatus.NEW
        assert summary.status is TaskStatus.COMPLETED
        assert isinstance(summary.result, AddResult)
        assert summary.result.value == 5
        assert later.result == summary.result
    finally:
        host.stop()


def test_task_paglet_failure_becomes_failed_state(tmp_path: Path):
    host = _host(tmp_path)
    host.start_background()
    try:
        proxy = host.create(FailingTaskPaglet, AddState())
        task = TaskClient.for_paglet(proxy, FailingTaskPaglet)

        summary = task.start_and_wait(AddRequest(4), wait_timeout=0.0)

        assert summary.status is TaskStatus.FAILED
        assert summary.done is True
        assert summary.result is None
        assert "forced failure" in summary.error
    finally:
        host.stop()


def _host(tmp_path: Path) -> Host:
    return Host(
        name="alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
