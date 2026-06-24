# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from dataclasses import dataclass, field, is_dataclass
from enum import Enum
from typing import Any, Generic, TypeVar

from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import HostError
from paglets.core.messages import Message
from paglets.remote.proxy import PagletProxy
from paglets.remote.references import PagletProxyRef
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from paglets.services.contracts import EmptyPayload, ServiceOperation


class TaskStatus(Enum):
    NEW = "NEW"
    RUNNING = "RUNNING"
    WAITING_FOR_ARRIVAL = "WAITING_FOR_ARRIVAL"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class TaskState(PagletState):
    status: TaskStatus = TaskStatus.NEW
    done: bool = False
    request: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    started_at: float = 0.0
    completed_at: float = 0.0


@dataclass(frozen=True, slots=True)
class TaskStartRequest:
    request: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TaskWaitRequest:
    wait_timeout: float = 0.5


@dataclass(frozen=True, slots=True)
class TaskStatusReply:
    status: TaskStatus = TaskStatus.NEW
    done: bool = False
    request: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    started_at: float = 0.0
    completed_at: float = 0.0
    agent_id: str = ""
    host_name: str = ""
    host_url: str = ""


TASK_START = ServiceOperation("start", TaskStartRequest, TaskStatusReply)
TASK_STATUS = ServiceOperation("status", EmptyPayload, TaskStatusReply)
TASK_WAIT = ServiceOperation("wait", TaskWaitRequest, TaskStatusReply)

RequestT = TypeVar("RequestT")
ResultT = TypeVar("ResultT")
StateT = TypeVar("StateT", bound=TaskState)


@dataclass(frozen=True, slots=True)
class TaskSnapshot(Generic[ResultT]):
    status: TaskStatus
    done: bool
    result: ResultT | None = None
    error: str = ""
    started_at: float = 0.0
    completed_at: float = 0.0
    agent_id: str = ""
    host_name: str = ""
    host_url: str = ""

    def to_wire(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "done": self.done,
            "result": dataclass_to_wire(self.result) if self.result is not None and is_dataclass(self.result) else {},
            "error": self.error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "agent_id": self.agent_id,
            "host_name": self.host_name,
            "host_url": self.host_url,
        }


class TaskPaglet(Paglet[StateT], Generic[RequestT, ResultT, StateT]):
    """Base class for simple typed paglets with start/status/wait operations."""

    Request: type[Any]
    Result: type[Any]

    def handle_message(self, message: Message):
        if message.kind == TASK_START.name:
            request = TASK_START.decode_request(message)
            return self._start_task(request)
        if message.kind == TASK_STATUS.name:
            return dataclass_to_wire(self.task_status_reply())
        if message.kind == TASK_WAIT.name:
            request = TASK_WAIT.decode_request(message)
            self.wait_state(lambda state: state.done, timeout=max(0.0, float(request.wait_timeout)))
            return dataclass_to_wire(self.task_status_reply())
        custom = self.handle_task_message(message)
        if custom is not None:
            return custom
        return self.not_handled()

    def handle_task_message(self, message: Message) -> Any | None:
        _ = message
        return None

    def run_task(self, request: RequestT) -> ResultT | PagletProxy | PagletProxyRef | None:
        _ = request
        raise NotImplementedError

    def task_status_reply(self) -> TaskStatusReply:
        with self.locked_state() as state:
            return TaskStatusReply(
                status=state.status,
                done=state.done,
                request=dict(state.request),
                result=dict(state.result),
                error=state.error,
                started_at=state.started_at,
                completed_at=state.completed_at,
                agent_id=self.agent_id,
                host_name=self.context.name,
                host_url=self.context.address,
            )

    def complete_task(self, result: ResultT, *, status: TaskStatus = TaskStatus.COMPLETED) -> None:
        if not is_dataclass(result) or isinstance(result, type):
            raise HostError("Task results must be dataclass instances")
        with self.locked_state() as state:
            state.status = status
            state.done = True
            state.result = dataclass_to_wire(result)
            state.error = ""
            state.completed_at = time.time()
        self.notify_all_state_changed()

    def fail_task(self, error: str | BaseException) -> None:
        message = str(error)
        with self.locked_state() as state:
            state.status = TaskStatus.FAILED
            state.done = True
            state.error = message
            state.completed_at = time.time()
        self.notify_all_state_changed()

    def set_task_status(self, status: TaskStatus, *, done: bool | None = None) -> None:
        with self.locked_state() as state:
            state.status = status
            if done is not None:
                state.done = bool(done)
        self.notify_all_state_changed()

    def _start_task(self, request: TaskStartRequest) -> dict[str, Any]:
        typed_request = dataclass_from_wire(self.request_class(), request.request)
        with self.locked_state() as state:
            state.status = TaskStatus.RUNNING
            state.done = False
            state.request = dict(request.request)
            state.result = {}
            state.error = ""
            state.started_at = time.time()
            state.completed_at = 0.0
        self.notify_all_state_changed()
        try:
            outcome = self.run_task(typed_request)
        except Exception as exc:
            self.fail_task(exc)
            return dataclass_to_wire(self.task_status_reply())
        if isinstance(outcome, PagletProxy):
            return outcome.to_wire()
        if isinstance(outcome, PagletProxyRef):
            return outcome.to_wire()
        if outcome is not None:
            self.complete_task(outcome)
        return dataclass_to_wire(self.task_status_reply())

    @classmethod
    def request_class(cls) -> type[RequestT]:
        request_cls = getattr(cls, "Request", None)
        if request_cls is None:
            raise HostError(f"{cls.__name__} must define Request")
        return request_cls

    @classmethod
    def result_class(cls) -> type[ResultT]:
        result_cls = getattr(cls, "Result", None)
        if result_cls is None:
            raise HostError(f"{cls.__name__} must define Result")
        return result_cls


@dataclass(slots=True)
class TaskClient(Generic[RequestT, ResultT]):
    proxy: PagletProxy
    request_type: type[RequestT]
    result_type: type[ResultT]

    @classmethod
    def for_paglet(
        cls,
        proxy: PagletProxy,
        paglet_cls: type[TaskPaglet[RequestT, ResultT, Any]],
    ) -> TaskClient[RequestT, ResultT]:
        return cls(proxy=proxy, request_type=paglet_cls.request_class(), result_type=paglet_cls.result_class())

    def start(self, request: RequestT, *, timeout: float | None = None) -> TaskSnapshot[ResultT]:
        message = TASK_START.to_message(TaskStartRequest(dataclass_to_wire(request)))
        reply = self.proxy.send(message, timeout=timeout)
        if _is_proxy_reply(reply):
            self.proxy = PagletProxy.from_wire(reply, self.proxy.client)
            return self.status(timeout=timeout)
        return self._snapshot_from_reply(reply)

    def status(self, *, timeout: float | None = None) -> TaskSnapshot[ResultT]:
        return self._snapshot_from_reply(self.proxy.send(TASK_STATUS.to_message(), timeout=timeout))

    def wait(self, *, wait_timeout: float = 0.5, timeout: float | None = None) -> TaskSnapshot[ResultT]:
        message = TASK_WAIT.to_message(TaskWaitRequest(wait_timeout=max(0.0, float(wait_timeout))))
        return self._snapshot_from_reply(self.proxy.send(message, timeout=timeout))

    def start_and_wait(
        self,
        request: RequestT,
        *,
        wait_timeout: float = 0.5,
        timeout: float | None = None,
    ) -> TaskSnapshot[ResultT]:
        snapshot = self.start(request, timeout=timeout)
        if snapshot.done:
            return snapshot
        return self.wait(wait_timeout=wait_timeout, timeout=timeout)

    def _snapshot_from_reply(self, reply: Any) -> TaskSnapshot[ResultT]:
        status = dataclass_from_wire(TaskStatusReply, dict(reply or {}))
        result = dataclass_from_wire(self.result_type, status.result) if status.result else None
        return TaskSnapshot(
            status=status.status,
            done=status.done,
            result=result,
            error=status.error,
            started_at=status.started_at,
            completed_at=status.completed_at,
            agent_id=status.agent_id,
            host_name=status.host_name,
            host_url=status.host_url,
        )


def _is_proxy_reply(reply: Any) -> bool:
    return isinstance(reply, dict) and set(reply) == {"host_url", "agent_id"}
