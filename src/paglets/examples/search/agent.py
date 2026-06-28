# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from paglets.core.agent import state_locked
from paglets.core.runtime_values import ServiceScope
from paglets.patterns.coordination import MeshFanoutMixin, MeshFanoutState
from paglets.patterns.operations import OperationPaglet
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from paglets.services.contracts import EmptyPayload, ServiceOperation
from paglets.system.user_info import NOTIFY_USER, STREAM_USER, USER_INFO, UserInfoRequest, UserInfoStreamRequest

from .local_search import run_local_search
from .models import DEFAULT_SEARCH_TIMEOUT_SECONDS, SearchEvent, SearchRequest


@dataclass
class MeshSearchState(MeshFanoutState):
    job_id: str = ""
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = DEFAULT_SEARCH_TIMEOUT_SECONDS
    output_path: str = ""
    requested_targets: list[str] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: int = 1
    summaries: dict[str, dict[str, Any]] = field(default_factory=dict)
    started: bool = False


@dataclass(frozen=True, slots=True)
class SearchStartRequest:
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = DEFAULT_SEARCH_TIMEOUT_SECONDS
    targets: list[str] = field(default_factory=list)
    job_id: str = ""
    output_path: str = ""


@dataclass(frozen=True, slots=True)
class SearchSummaryReply:
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)
    pending_hosts: list[str] = field(default_factory=list)
    done_hosts: list[str] = field(default_factory=list)
    event_count: int = 0


@dataclass(frozen=True, slots=True)
class SearchStartReply:
    accepted: bool = True
    job_id: str = ""
    agent_id: str = ""
    host_url: str = ""
    output_path: str = ""
    targets: list[dict[str, str]] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchChildEventsRequest:
    host_name: str = ""
    host_url: str = ""
    events: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SearchChildEventsReply:
    ok: bool = True
    cursor: int = 0


@dataclass(frozen=True, slots=True)
class SearchChildDoneRequest:
    host_name: str = ""
    host_url: str = ""
    summary: dict[str, Any] = field(default_factory=dict)
    error: str = ""


SEARCH_START = ServiceOperation("start", SearchStartRequest, SearchStartReply)
SEARCH_CHILD_EVENTS = ServiceOperation("child_events", SearchChildEventsRequest, SearchChildEventsReply)
SEARCH_CHILD_DONE = ServiceOperation("child_done", SearchChildDoneRequest, EmptyPayload)
SEARCH_SUMMARY = ServiceOperation("summary", EmptyPayload, SearchSummaryReply)
SEARCH_CLEANUP = ServiceOperation("cleanup", EmptyPayload, SearchSummaryReply)


class MeshSearchAgent(MeshFanoutMixin, OperationPaglet[MeshSearchState]):
    """Clone across the mesh and stream local filesystem search hits."""

    State = MeshSearchState
    Operations = (SEARCH_START, SEARCH_CHILD_EVENTS, SEARCH_CHILD_DONE, SEARCH_SUMMARY, SEARCH_CLEANUP)

    def operation_handlers(self):
        return {
            SEARCH_START: self.start,
            SEARCH_CHILD_EVENTS: self.record_child_events,
            SEARCH_CHILD_DONE: self.record_child_done,
            SEARCH_SUMMARY: self.summary,
            SEARCH_CLEANUP: self.cleanup_children,
        }

    def run(self) -> None:
        with self.locked_state() as state:
            is_child = state.role == "child"
        if is_child:
            thread = threading.Thread(
                target=self._run_child,
                name=f"paglets-search-{self.context.name}",
                daemon=True,
            )
            thread.start()

    def start(self, request: SearchStartRequest) -> SearchStartReply:
        self.fanout_reset(timeout=request.timeout)
        job_id = request.job_id or f"search-{uuid.uuid4().hex}"
        output_path = Path(request.output_path).expanduser()
        if not output_path.is_absolute():
            raise ValueError("search output_path must be absolute")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("", encoding="utf-8")
        with self.locked_state() as state:
            state.job_id = job_id
            state.request = dict(request.request)
            state.timeout = float(request.timeout)
            state.output_path = str(output_path)
            state.requested_targets = list(request.targets)
            state.events = []
            state.next_cursor = 1
            state.summaries = {}
            state.started = True
            requested_targets = list(state.requested_targets)
        hosts = self._target_hosts(requested_targets)
        for host in hosts:
            self.fanout_prepare_clone(host)
            try:
                child = self.clone_to(host.name)
                self.fanout_record_child_proxy(host.name, child)
            except Exception as exc:
                self.fanout_record_error(host.name, str(exc))
                self.notify_all_state_changed()
            finally:
                self.fanout_finish_clone_prepare()
        if not hosts:
            with self.locked_state() as state:
                state.errors["mesh"] = "no online target hosts found"
            self.notify_all_state_changed()
        return SearchStartReply(
            accepted=True,
            job_id=job_id,
            agent_id=self.agent_id,
            host_url=self.context.address,
            output_path=str(output_path),
            targets=[{"name": host.name, "url": host.url} for host in hosts],
            summary=dataclass_to_wire(self.summary()),
        )

    @state_locked
    def summary(self, request: EmptyPayload | None = None) -> SearchSummaryReply:
        _ = request
        return self._summary_from_state(self.state)

    def cleanup_children(self, request: EmptyPayload | None = None) -> SearchSummaryReply:
        _ = request
        self.fanout_cleanup_children()
        return self.summary()

    def _target_hosts(self, requested_targets: list[str]):
        return self.fanout_select_hosts(requested_targets, include_self=True)

    def _run_child(self) -> None:
        with self.locked_state() as state:
            request_wire = dict(state.request)
            target_host_name = state.target_host_name or self.context.name
            target_host_url = state.target_host_url or self.context.address
            parent_agent_id = state.parent_agent_id
            parent_host_url = state.parent_host_url
        parent = self.context.get_proxy(parent_agent_id, parent_host_url)
        buffer: list[dict[str, Any]] = []

        def flush() -> None:
            if parent is None or not buffer:
                buffer.clear()
                return
            payload = {
                "host_name": target_host_name,
                "host_url": target_host_url,
                "events": list(buffer),
            }
            buffer.clear()
            parent.send_oneway(SEARCH_CHILD_EVENTS.to_message(SearchChildEventsRequest(**payload)), no_delay=True)

        try:
            request = dataclass_from_wire(SearchRequest, request_wire)
            batch_size = max(1, int(request.batch_size))

            def emit(events: list[SearchEvent]) -> None:
                for event in events:
                    buffer.append(dataclass_to_wire(event))
                    if len(buffer) >= batch_size:
                        flush()

            summary = run_local_search(
                request,
                host_name=target_host_name,
                host_url=target_host_url,
                emit=emit,
            )
            flush()
            payload = {
                "host_name": target_host_name,
                "host_url": target_host_url,
                "summary": dataclass_to_wire(summary),
                "error": "",
            }
        except Exception as exc:
            flush()
            payload = {
                "host_name": target_host_name,
                "host_url": target_host_url,
                "summary": {},
                "error": str(exc),
            }
        if parent is not None:
            parent.send_oneway(SEARCH_CHILD_DONE.to_message(SearchChildDoneRequest(**payload)), no_delay=True)

    def record_child_events(self, request: SearchChildEventsRequest) -> SearchChildEventsReply:
        records = [dict(event) for event in request.events]
        with self.locked_state() as state:
            for event in records:
                event["cursor"] = state.next_cursor
                state.next_cursor += 1
                state.events.append(event)
            cursor = state.next_cursor - 1
            output_path = state.output_path
        self._append_events(output_path, records)
        for event in records:
            self._user_output(_format_event_line(event))
        self.notify_all_state_changed()
        return SearchChildEventsReply(ok=True, cursor=cursor)

    def record_child_done(self, request: SearchChildDoneRequest) -> EmptyPayload:
        with self.locked_state() as state:
            host_name = str(request.host_name or "")
            if host_name:
                state.pending_hosts = [name for name in state.pending_hosts if name != host_name]
                if host_name not in state.done_hosts:
                    state.done_hosts.append(host_name)
            if request.error:
                state.errors[host_name or "unknown"] = request.error
            elif request.summary:
                state.summaries[host_name] = dict(request.summary)
            done = not state.pending_hosts
            output_path = state.output_path
        self.notify_all_state_changed()
        if done:
            self._user_notify("info", "search.done", f"Search job complete; output: {output_path}")
        return EmptyPayload()

    def _expire_timed_out_hosts(self) -> None:
        self.fanout_expire_pending("timed out waiting for search result")

    def _append_events(self, output_path: str, events: list[dict[str, Any]]) -> None:
        if not output_path or not events:
            return
        import json

        with Path(output_path).open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, sort_keys=True))
                handle.write("\n")
            handle.flush()

    def _user_output(self, text: str) -> None:
        if not text:
            return
        with contextlib.suppress(Exception):
            service = self.require_contract(USER_INFO, operation=STREAM_USER, scope=ServiceScope.LOCAL)
            service.send_oneway(
                STREAM_USER,
                UserInfoStreamRequest(stream_id=self.state.job_id, text=f"{text}\n", target="stdout"),
                no_delay=True,
            )

    def _user_notify(self, severity: str, title: str, message: str) -> None:
        with contextlib.suppress(Exception):
            service = self.require_contract(USER_INFO, operation=NOTIFY_USER, scope=ServiceScope.LOCAL)
            service.send_oneway(
                NOTIFY_USER,
                UserInfoRequest(
                    severity=severity,
                    title=title,
                    message=message,
                    source_agent_id=self.agent_id,
                    job_id=self.state.job_id,
                    timestamp=time.time(),
                ),
                no_delay=True,
            )

    @staticmethod
    def _summary_from_state(state: MeshSearchState) -> SearchSummaryReply:
        return SearchSummaryReply(
            results=dict(state.summaries),
            errors=dict(state.errors),
            cleanup_errors=dict(state.cleanup_errors),
            pending_hosts=list(state.pending_hosts),
            done_hosts=list(state.done_hosts),
            event_count=len(state.events),
        )


def _format_event_line(event: dict[str, Any]) -> str:
    kind = str(event.get("event") or "")
    host_name = str(event.get("host_name") or "")
    path = str(event.get("path") or "")
    if kind == "file":
        return f"{host_name}:{path}"
    if kind == "count":
        return f"{host_name}:{path}:{event.get('count', 0)}"
    if kind == "error":
        return f"{host_name}: error: {event.get('message', '')}"
    if kind not in {"match", "context"}:
        return ""
    sep = "-" if kind == "context" else ":"
    parts = [host_name, path]
    if event.get("line_number"):
        parts.append(str(event["line_number"]))
    if event.get("column"):
        parts.append(str(event["column"]))
    return sep.join(parts) + sep + str(event.get("text") or "")
