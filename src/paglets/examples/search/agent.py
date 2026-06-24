# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from paglets.core.agent import state_locked
from paglets.patterns.coordination import CursorDrainMixin, MeshFanoutMixin, MeshFanoutState
from paglets.patterns.operations import OperationClient, OperationPaglet
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from paglets.services.contracts import EmptyPayload, ServiceOperation

from .local_search import run_local_search
from .models import (
    DEFAULT_DRAIN_WAIT_SECONDS,
    DEFAULT_SEARCH_TIMEOUT_SECONDS,
    SearchEvent,
    SearchRequest,
)


@dataclass
class MeshSearchState(MeshFanoutState):
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = DEFAULT_SEARCH_TIMEOUT_SECONDS
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
    targets: list[dict[str, str]] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchDrainRequest:
    after_cursor: int = 0
    wait_timeout: float = DEFAULT_DRAIN_WAIT_SECONDS
    limit: int = 200


@dataclass(frozen=True, slots=True)
class SearchDrainReply:
    events: list[dict[str, Any]] = field(default_factory=list)
    cursor: int = 0
    done: bool = False
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
SEARCH_DRAIN = ServiceOperation("drain", SearchDrainRequest, SearchDrainReply)
SEARCH_CHILD_EVENTS = ServiceOperation("child_events", SearchChildEventsRequest, SearchChildEventsReply)
SEARCH_CHILD_DONE = ServiceOperation("child_done", SearchChildDoneRequest, EmptyPayload)
SEARCH_SUMMARY = ServiceOperation("summary", EmptyPayload, SearchSummaryReply)
SEARCH_CLEANUP = ServiceOperation("cleanup", EmptyPayload, SearchSummaryReply)


class MeshSearchAgent(CursorDrainMixin, MeshFanoutMixin, OperationPaglet[MeshSearchState]):
    """Clone across the mesh and stream local filesystem search hits."""

    State = MeshSearchState
    Operations = (SEARCH_START, SEARCH_DRAIN, SEARCH_CHILD_EVENTS, SEARCH_CHILD_DONE, SEARCH_SUMMARY, SEARCH_CLEANUP)

    def operation_handlers(self):
        return {
            SEARCH_START: self.start,
            SEARCH_DRAIN: self.drain,
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
        with self.locked_state() as state:
            state.request = dict(request.request)
            state.timeout = float(request.timeout)
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
            targets=[{"name": host.name, "url": host.url} for host in hosts],
            summary=dataclass_to_wire(self.summary()),
        )

    def drain(self, request: SearchDrainRequest) -> SearchDrainReply:
        self._expire_timed_out_hosts()

        def ready(state: MeshSearchState) -> bool:
            return state.next_cursor > request.after_cursor + 1 or not state.pending_hosts or bool(state.errors)

        self.fanout_wait_for(ready, wait_timeout=request.wait_timeout)
        self._expire_timed_out_hosts()

        with self.locked_state() as state:
            pending = bool(state.pending_hosts)
        events, cursor, more_events = self.cursor_drain_events(
            after_cursor=max(0, int(request.after_cursor)),
            limit=max(1, int(request.limit)),
        )
        return SearchDrainReply(
            events=events,
            cursor=cursor,
            done=not pending and not more_events,
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
            OperationClient(parent).call(SEARCH_CHILD_EVENTS, SearchChildEventsRequest(**payload))

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
            OperationClient(parent).call(SEARCH_CHILD_DONE, SearchChildDoneRequest(**payload))

    def record_child_events(self, request: SearchChildEventsRequest) -> SearchChildEventsReply:
        cursor = self.cursor_append_events([dict(event) for event in request.events])
        return SearchChildEventsReply(ok=True, cursor=cursor)

    @state_locked
    def record_child_done(self, request: SearchChildDoneRequest) -> EmptyPayload:
        host_name = str(request.host_name or "")
        if host_name:
            self.state.pending_hosts = [name for name in self.state.pending_hosts if name != host_name]
            if host_name not in self.state.done_hosts:
                self.state.done_hosts.append(host_name)
        if request.error:
            self.state.errors[host_name or "unknown"] = request.error
        elif request.summary:
            self.state.summaries[host_name] = dict(request.summary)
        self.notify_all_state_changed()
        return EmptyPayload()

    def _expire_timed_out_hosts(self) -> None:
        self.fanout_expire_pending("timed out waiting for search result")

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
