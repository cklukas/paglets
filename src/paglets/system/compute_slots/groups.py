# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from paglets.core.agent import Paglet, PagletContext, PagletState
from paglets.core.messages import Message
from paglets.persistence.persistency import DeactivationPolicy
from paglets.remote.proxy import PagletProxy
from paglets.remote.references import PagletProxyRef
from paglets.serialization.codec import dataclass_to_wire

from .job import ComputeJobPaglet, ComputeJobState

GROUP_STATUS_ACTIVE = "ACTIVE"
GROUP_STATUS_COMPLETE = "COMPLETE"
GROUP_STATUS_WAITING_FOR_HOME = "WAITING_FOR_HOME"
GROUP_STATUS_RETURNING_HOME = "RETURNING_HOME"


@dataclass
class ResultCollectorState(PagletState):
    group_id: str = ""
    home_host_name: str = ""
    home_host_url: str = ""
    status: str = GROUP_STATUS_ACTIVE
    return_home_when_complete: bool = False
    home_check_seconds: float = 300.0
    expected_jobs: dict[str, dict[str, Any]] = field(default_factory=dict)
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    failures: dict[str, dict[str, Any]] = field(default_factory=dict)
    duplicate_reports: list[dict[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    completed_at: float = 0.0
    last_report_at: float = 0.0


@dataclass
class CollectingComputeJobState(ComputeJobState):
    group_id: str = ""
    job_key: str = ""
    collector_agent_id: str = ""
    collector_host_url: str = ""
    report_timeout_seconds: float = 5.0
    dispose_after_report: bool = True
    report_sent: bool = False
    report_error: str = ""


@dataclass(frozen=True, slots=True)
class ComputeJobGroupSubmission:
    group_id: str
    collector: PagletProxy
    jobs: list[PagletProxy] = field(default_factory=list)
    creation_errors: dict[str, str] = field(default_factory=dict)


class ResultCollectorPaglet(Paglet[ResultCollectorState]):
    """Collect JSON-sized success/failure reports for a group of compute jobs."""

    State = ResultCollectorState

    def on_creation(self, event) -> None:
        with self.locked_state() as state:
            if not state.home_host_name:
                state.home_host_name = self.context.name
            if not state.home_host_url:
                state.home_host_url = self.context.address.rstrip("/")
            if not state.group_id:
                state.group_id = f"group-{uuid.uuid4().hex}"

    def on_activation(self, event) -> None:
        self.run()

    def run(self) -> None:
        with self.locked_state() as state:
            should_return = state.status in {GROUP_STATUS_WAITING_FOR_HOME, GROUP_STATUS_RETURNING_HOME}
        if should_return:
            self._try_return_home()

    def handle_message(self, message: Message):
        if message.kind == "register_jobs":
            return self.register_jobs(list(message.args.get("jobs") or []))
        if message.kind == "job_result":
            return self.record_job_result(dict(message.args))
        if message.kind == "job_failure":
            return self.record_job_failure(dict(message.args))
        if message.kind == "summary":
            return self.summary()
        if message.kind == "drain":
            return self.drain(wait_timeout=float(message.args.get("wait_timeout", 0.5)))
        if message.kind == "return_home":
            self._try_return_home()
            return self.summary()
        return self.not_handled()

    def register_jobs(self, jobs: list[dict[str, Any]]) -> dict[str, Any]:
        with self.locked_state() as state:
            if not state.group_id:
                state.group_id = f"group-{uuid.uuid4().hex}"
            for job in jobs:
                job_key = str(job.get("job_key") or job.get("job_id") or "").strip()
                if not job_key:
                    continue
                current = dict(state.expected_jobs.get(job_key) or {})
                current.update(dict(job))
                current["job_key"] = job_key
                state.expected_jobs[job_key] = current
        self.notify_all_state_changed()
        return self.summary()

    def record_job_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._record_report(payload, success=True)

    def record_job_failure(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._record_report(payload, success=False)

    def drain(self, *, wait_timeout: float = 0.5) -> dict[str, Any]:
        self.wait_state(lambda state: self._is_complete_locked(state), timeout=max(0.0, wait_timeout))
        return {"done": self._is_complete(), "summary": self.summary()}

    def summary(self) -> dict[str, Any]:
        with self.locked_state() as state:
            expected = set(state.expected_jobs)
            completed = set(state.results)
            failed = set(state.failures)
            pending = sorted(expected - completed - failed)
            return {
                "group_id": state.group_id,
                "status": state.status,
                "collector": {
                    "host_name": self.context.name,
                    "host_url": self.context.address,
                    "agent_id": self.agent_id,
                },
                "home": {"host_name": state.home_host_name, "host_url": state.home_host_url},
                "return_home_when_complete": state.return_home_when_complete,
                "waiting_for_home": state.status == GROUP_STATUS_WAITING_FOR_HOME,
                "expected_count": len(expected),
                "completed_count": len(completed),
                "failed_count": len(failed),
                "pending_count": len(pending),
                "pending_jobs": pending,
                "expected_jobs": dict(state.expected_jobs),
                "results": dict(state.results),
                "failures": dict(state.failures),
                "duplicate_reports": list(state.duplicate_reports),
                "created_at": state.created_at,
                "completed_at": state.completed_at,
                "last_report_at": state.last_report_at,
            }

    def _record_report(self, payload: dict[str, Any], *, success: bool) -> dict[str, Any]:
        job_key = str(payload.get("job_key") or payload.get("job_id") or "").strip()
        if not job_key:
            return {"ok": False, "error": "job report missing job_key"}
        with self.locked_state() as state:
            if job_key not in state.expected_jobs:
                state.expected_jobs[job_key] = {"job_key": job_key}
            if job_key in state.results or job_key in state.failures:
                duplicate = dict(payload)
                duplicate["success"] = success
                state.duplicate_reports.append(duplicate)
                return {"ok": True, "duplicate": True, "summary": self.summary()}
            record = dict(payload)
            record["job_key"] = job_key
            record["reported_at"] = time.time()
            if success:
                state.results[job_key] = record
            else:
                state.failures[job_key] = record
            state.last_report_at = record["reported_at"]
            if self._is_complete_locked(state) and state.completed_at <= 0:
                state.completed_at = time.time()
        self.notify_all_state_changed()
        if self._is_complete():
            self._on_group_complete()
        return {"ok": True, "summary": self.summary()}

    def _on_group_complete(self) -> None:
        with self.locked_state() as state:
            if not state.return_home_when_complete:
                state.status = GROUP_STATUS_COMPLETE
                return
        self._try_return_home()

    def _try_return_home(self) -> None:
        with self.locked_state() as state:
            home = state.home_host_url or state.home_host_name
            interval = max(1.0, float(state.home_check_seconds))
        if not home or self._is_home():
            with self.locked_state() as state:
                state.status = GROUP_STATUS_COMPLETE
            self.notify_all_state_changed()
            return
        if not self.context.is_host_online(home):
            with self.locked_state() as state:
                state.status = GROUP_STATUS_WAITING_FOR_HOME
            self.notify_all_state_changed()
            self.deactivate(
                policy=DeactivationPolicy.after(
                    interval,
                    activate_on_message=True,
                    queue_messages_when_inactive=True,
                    activate_on_startup=True,
                )
            )
            return
        with self.locked_state() as state:
            state.status = GROUP_STATUS_RETURNING_HOME
        self.notify_all_state_changed()
        self.dispatch(home)

    def _is_home(self) -> bool:
        with self.locked_state() as state:
            return self.context.name == state.home_host_name or self.context.address.rstrip(
                "/"
            ) == state.home_host_url.rstrip("/")

    def _is_complete(self) -> bool:
        with self.locked_state() as state:
            return self._is_complete_locked(state)

    @staticmethod
    def _is_complete_locked(state: ResultCollectorState) -> bool:
        expected = set(state.expected_jobs)
        return bool(expected) and expected <= (set(state.results) | set(state.failures))


class CollectingComputeJobPaglet(ComputeJobPaglet[CollectingComputeJobState]):
    """Compute-job base that reports success or failure to a result collector."""

    State = CollectingComputeJobState

    def handle_compute_job_message(self, message: Message) -> Any | None:
        if message.kind == "status":
            with self.locked_state() as state:
                return dataclass_to_wire(state)
        return None

    def after_compute_success(self) -> None:
        with self.locked_state() as state:
            already_reported = bool(state.report_sent)
        if not already_reported:
            self.report_compute_success(self.build_result_payload())
        with self.locked_state() as state:
            dispose = bool(state.dispose_after_report)
        if dispose:
            self.context.host.dispose(self.agent_id)

    def after_compute_failure(self, message: str) -> None:
        self.report_compute_failure(message)

    def build_result_payload(self) -> dict[str, Any]:
        return {}

    def report_compute_success(self, result: dict[str, Any] | None = None) -> bool:
        return self._report_to_collector("job_result", {"result": dict(result or {})})

    def report_compute_artifact(
        self,
        path: str | Path,
        *,
        result: dict[str, Any] | None = None,
        move: bool = False,
        name: str | None = None,
    ) -> bool:
        with self.locked_state() as state:
            collector_host_url = state.collector_host_url
            collector_agent_id = state.collector_agent_id
        artifact = self.context.upload_artifact(
            path,
            host_url=collector_host_url,
            owner_agent_id=collector_agent_id,
            name=name,
        )
        payload = dict(result or {})
        payload["artifact"] = artifact.to_wire()
        ok = self.report_compute_success(payload)
        if ok and move:
            with contextlib.suppress(FileNotFoundError):
                Path(path).unlink()
        return ok

    def report_compute_failure(self, error: str, *, details: dict[str, Any] | None = None) -> bool:
        payload = {"error": error}
        if details:
            payload["details"] = dict(details)
        return self._report_to_collector("job_failure", payload)

    def _report_to_collector(self, kind: str, payload: dict[str, Any]) -> bool:
        with self.locked_state() as state:
            report = {
                "group_id": state.group_id,
                "job_key": state.job_key or self.compute_job_id(),
                "agent_id": self.agent_id,
                "host_name": self.context.name,
                "host_url": self.context.address,
                **payload,
            }
            collector_agent_id = state.collector_agent_id
            collector_host_url = state.collector_host_url
            timeout = max(0.0, float(state.report_timeout_seconds))
        if not collector_agent_id or not collector_host_url:
            with self.locked_state() as state:
                state.report_error = "collector is not configured"
            return False
        try:
            collector = self.context.get_proxy(collector_agent_id, collector_host_url)
            if collector is None:
                raise RuntimeError("collector proxy is unavailable")
            collector.send(Message(kind, report), timeout=timeout)
        except Exception as exc:
            with self.locked_state() as state:
                state.report_error = str(exc)
            return False
        with self.locked_state() as state:
            state.report_sent = True
            state.report_error = ""
        return True


def submit_compute_job_group(
    context_or_host: Any,
    job_cls: type[ComputeJobPaglet],
    job_states: list[CollectingComputeJobState],
    *,
    collector_cls: type[ResultCollectorPaglet] = ResultCollectorPaglet,
    collector_state: ResultCollectorState | None = None,
    collector_host_url: str | None = None,
    group_id: str | None = None,
    return_home_when_complete: bool = False,
    job_metadata: list[dict[str, Any]] | None = None,
) -> ComputeJobGroupSubmission:
    context = _context_from(context_or_host)
    group_id = group_id or f"group-{uuid.uuid4().hex}"
    if collector_state is None:
        collector_state = collector_cls.state_class()()
    collector_state.group_id = group_id
    collector_state.home_host_name = collector_state.home_host_name or context.name
    collector_state.home_host_url = collector_state.home_host_url or context.address.rstrip("/")
    collector_state.return_home_when_complete = return_home_when_complete
    collector = _create_paglet(context, collector_cls, collector_state, host_url=collector_host_url)
    collector_ref = PagletProxyRef.from_proxy(collector)

    expected: list[dict[str, Any]] = []
    metadata_items = job_metadata or [{} for _ in job_states]
    for index, state in enumerate(job_states):
        state.group_id = group_id
        if not state.job_key:
            state.job_key = f"{group_id}-{index:04d}"
        state.collector_agent_id = collector.agent_id
        state.collector_host_url = collector.host_url.rstrip("/")
        item = {"job_key": state.job_key, "collector": collector_ref.to_wire()}
        if index < len(metadata_items):
            item.update(dict(metadata_items[index]))
        expected.append(item)
    collector.send(Message("register_jobs", {"jobs": expected}))

    jobs: list[PagletProxy] = []
    creation_errors: dict[str, str] = {}
    registered_updates: list[dict[str, Any]] = []
    for state in job_states:
        try:
            proxy = _create_paglet(context, job_cls, state)
        except Exception as exc:
            creation_errors[state.job_key] = str(exc)
            collector.send(
                Message(
                    "job_failure",
                    {
                        "group_id": group_id,
                        "job_key": state.job_key,
                        "error": str(exc),
                        "stage": "create",
                    },
                )
            )
            continue
        jobs.append(proxy)
        registered_updates.append({"job_key": state.job_key, "agent_id": proxy.agent_id, "host_url": proxy.host_url})
    if registered_updates:
        collector.send(Message("register_jobs", {"jobs": registered_updates}))
    return ComputeJobGroupSubmission(group_id=group_id, collector=collector, jobs=jobs, creation_errors=creation_errors)


def _context_from(context_or_host: Any) -> Any:
    if isinstance(context_or_host, PagletContext):
        return context_or_host
    if hasattr(context_or_host, "create") and hasattr(context_or_host, "address") and hasattr(context_or_host, "name"):
        return _HostSubmitContext(context_or_host)
    return context_or_host


def _create_paglet(
    context: Any, agent_cls: type[Paglet], state: PagletState, *, host_url: str | None = None
) -> PagletProxy:
    if hasattr(context, "create_paglet"):
        return context.create_paglet(agent_cls, state, host_url=host_url)
    if host_url is not None and host_url.rstrip("/") != context.address.rstrip("/"):
        return context.create_remote(host_url, agent_cls, state)
    return context.create(agent_cls, state)


class _HostSubmitContext:
    def __init__(self, host: Any):
        self._host = host
        self.name = host.name
        self.address = host.address

    def create_paglet(self, agent_cls: type[Paglet], state: PagletState, *, host_url: str | None = None) -> PagletProxy:
        if host_url is not None and host_url.rstrip("/") != self.address.rstrip("/"):
            return self._host.create_remote(host_url, agent_cls, state)
        return self._host.create(agent_cls, state)


__all__ = [
    "GROUP_STATUS_ACTIVE",
    "GROUP_STATUS_COMPLETE",
    "GROUP_STATUS_RETURNING_HOME",
    "GROUP_STATUS_WAITING_FOR_HOME",
    "CollectingComputeJobPaglet",
    "CollectingComputeJobState",
    "ComputeJobGroupSubmission",
    "ResultCollectorPaglet",
    "ResultCollectorState",
    "submit_compute_job_group",
]
