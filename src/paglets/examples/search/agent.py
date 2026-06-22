# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

from paglets.core.agent import Paglet, PagletState, state_locked
from paglets.core.messages import Message
from paglets.remote.proxy import PagletProxy
from paglets.serialization.serde import dataclass_from_wire, dataclass_to_wire

from .local_search import run_local_search
from .models import (
    DEFAULT_DRAIN_WAIT_SECONDS,
    DEFAULT_SEARCH_TIMEOUT_SECONDS,
    SearchEvent,
    SearchRequest,
)


@dataclass
class MeshSearchState(PagletState):
    role: str = "parent"
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = DEFAULT_SEARCH_TIMEOUT_SECONDS
    deadline: float = 0.0
    parent_host_url: str = ""
    parent_agent_id: str = ""
    target_host_name: str = ""
    target_host_url: str = ""
    requested_targets: list[str] = field(default_factory=list)
    pending_hosts: list[str] = field(default_factory=list)
    done_hosts: list[str] = field(default_factory=list)
    children: dict[str, dict[str, str]] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: int = 1
    summaries: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)
    started: bool = False


class MeshSearchAgent(Paglet[MeshSearchState]):
    """Clone across the mesh and stream local filesystem search hits."""

    State = MeshSearchState

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

    def handle_message(self, message: Message):
        if message.kind == "start":
            with self.locked_state() as state:
                state.request = dict(message.args.get("request") or {})
                state.timeout = float(message.args.get("timeout", DEFAULT_SEARCH_TIMEOUT_SECONDS))
                state.requested_targets = [str(item) for item in message.args.get("targets") or []]
            return self.start()
        if message.kind == "child_events":
            return self.record_child_events(message.args)
        if message.kind == "child_done":
            return self.record_child_done(message.args)
        if message.kind == "drain":
            return self.drain(
                after_cursor=int(message.args.get("after_cursor", 0)),
                wait_timeout=float(message.args.get("wait_timeout", DEFAULT_DRAIN_WAIT_SECONDS)),
                limit=int(message.args.get("limit", 200)),
            )
        if message.kind == "summary":
            return self.summary()
        if message.kind == "cleanup":
            return self.cleanup_children()
        return self.not_handled()

    def start(self) -> dict[str, Any]:
        with self.locked_state() as state:
            state.role = "parent"
            state.parent_host_url = self.context.address
            state.parent_agent_id = self.agent_id
            state.pending_hosts = []
            state.done_hosts = []
            state.children = {}
            state.events = []
            state.next_cursor = 1
            state.summaries = {}
            state.errors = {}
            state.cleanup_errors = {}
            state.started = True
            state.deadline = time.monotonic() + max(0.0, state.timeout)
            requested_targets = list(state.requested_targets)
        hosts = self._target_hosts(requested_targets)
        for host in hosts:
            with self.locked_state() as state:
                state.pending_hosts.append(host.name)
                state.role = "child"
                state.target_host_name = host.name
                state.target_host_url = host.url
            try:
                child = self.clone_to(host.name)
                with self.locked_state() as state:
                    state.children[host.name] = child.to_wire()
            except Exception as exc:
                with self.locked_state() as state:
                    state.pending_hosts = [name for name in state.pending_hosts if name != host.name]
                    state.errors[host.name] = str(exc)
                self.notify_all_state_changed()
            finally:
                with self.locked_state() as state:
                    state.role = "parent"
                    state.target_host_name = ""
                    state.target_host_url = ""
        if not hosts:
            with self.locked_state() as state:
                state.errors["mesh"] = "no online target hosts found"
            self.notify_all_state_changed()
        return {
            "targets": [{"name": host.name, "url": host.url} for host in hosts],
            "summary": self.summary(),
        }

    def drain(self, *, after_cursor: int, wait_timeout: float, limit: int) -> dict[str, Any]:
        limit = max(1, limit)
        self._expire_timed_out_hosts()

        def ready(state: MeshSearchState) -> bool:
            return state.next_cursor > after_cursor + 1 or not state.pending_hosts or bool(state.errors)

        timeout = max(0.0, wait_timeout)
        with self.locked_state() as state:
            if state.deadline > 0:
                timeout = min(timeout, max(0.0, state.deadline - time.monotonic()))
        self.wait_state(ready, timeout=timeout)
        self._expire_timed_out_hosts()

        with self.locked_state() as state:
            matching = [event for event in state.events if int(event.get("cursor", 0)) > after_cursor]
            events = matching[:limit]
            last_cursor = after_cursor
            if events:
                last_cursor = max(int(event.get("cursor", 0)) for event in events)
            more_events = len(matching) > len(events)
            done = not state.pending_hosts and not more_events
            summary = self._summary_from_state(state)
        return {
            "events": events,
            "cursor": last_cursor,
            "done": done,
            "summary": summary,
        }

    @state_locked
    def summary(self) -> dict[str, Any]:
        return self._summary_from_state(self.state)

    def cleanup_children(self) -> dict[str, Any]:
        with self.locked_state() as state:
            children = {name: dict(proxy) for name, proxy in state.children.items()}
        for host_name, proxy_wire in children.items():
            try:
                PagletProxy.from_wire(proxy_wire, self.context.host.client).dispose()
            except Exception as exc:
                with self.locked_state() as state:
                    state.cleanup_errors[host_name] = str(exc)
        return self.summary()

    def _target_hosts(self, requested_targets: list[str]):
        hosts = self.context.available_hosts(online_only=True, include_self=True)
        if not requested_targets:
            return hosts
        selected = []
        for target in requested_targets:
            ref = self.context.host_status(target)
            if ref is None or not ref.online:
                with self.locked_state() as state:
                    state.errors[target] = "target host is not online or not visible in the mesh"
                continue
            selected.append(ref)
        return selected

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
            parent.send(Message("child_events", payload))

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
            }
        except Exception as exc:
            flush()
            payload = {
                "host_name": target_host_name,
                "host_url": target_host_url,
                "error": str(exc),
            }
        if parent is not None:
            parent.send(Message("child_done", payload))

    @state_locked
    def record_child_events(self, payload: dict[str, Any]) -> dict[str, Any]:
        for event in payload.get("events") or []:
            item = dict(event)
            item["cursor"] = self.state.next_cursor
            self.state.next_cursor += 1
            self.state.events.append(item)
        self.notify_all_state_changed()
        return {"ok": True, "cursor": self.state.next_cursor - 1}

    @state_locked
    def record_child_done(self, payload: dict[str, Any]) -> dict[str, Any]:
        host_name = str(payload.get("host_name") or "")
        if host_name:
            self.state.pending_hosts = [name for name in self.state.pending_hosts if name != host_name]
            if host_name not in self.state.done_hosts:
                self.state.done_hosts.append(host_name)
        if payload.get("error"):
            self.state.errors[host_name or "unknown"] = str(payload["error"])
        elif payload.get("summary"):
            self.state.summaries[host_name] = dict(payload["summary"])
        self.notify_all_state_changed()
        return {"ok": True}

    def _expire_timed_out_hosts(self) -> None:
        with self.locked_state() as state:
            if not state.pending_hosts or state.deadline <= 0 or time.monotonic() < state.deadline:
                return
            timed_out = list(state.pending_hosts)
            for host_name in timed_out:
                state.errors[host_name] = "timed out waiting for search result"
            state.pending_hosts = []
        self.notify_all_state_changed()

    @staticmethod
    def _summary_from_state(state: MeshSearchState) -> dict[str, Any]:
        return {
            "results": dict(state.summaries),
            "errors": dict(state.errors),
            "cleanup_errors": dict(state.cleanup_errors),
            "pending_hosts": list(state.pending_hosts),
            "done_hosts": list(state.done_hosts),
            "event_count": len(state.events),
        }
