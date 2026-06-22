# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from paglets.core.agent import Paglet, PagletState, state_locked
from paglets.core.messages import Message
from paglets.serialization.serde import dataclass_from_wire, dataclass_to_wire

from .kernels import run_host_benchmarks
from .models import BenchmarkRequest


@dataclass
class PerformanceBenchmarkState(PagletState):
    role: str = "parent"
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = 120.0
    parent_host_url: str = ""
    parent_agent_id: str = ""
    target_host_name: str = ""
    target_host_url: str = ""
    deadline: float = 0.0
    pending_hosts: list[str] = field(default_factory=list)
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)
    child_proxies: dict[str, dict[str, str]] = field(default_factory=dict)


class PerformanceBenchmarkAgent(Paglet[PerformanceBenchmarkState]):
    """Clone across the mesh and run local host performance benchmarks."""

    State = PerformanceBenchmarkState

    def run(self) -> None:
        with self.locked_state() as state:
            is_child = state.role == "child"
        if is_child:
            thread = threading.Thread(
                target=self._run_child,
                name=f"paglets-benchmark-{self.context.name}",
                daemon=True,
            )
            thread.start()

    def handle_message(self, message: Message):
        if message.kind == "collect":
            with self.locked_state() as state:
                state.request = dict(message.args.get("request") or {})
                state.timeout = float(message.args.get("timeout", 120.0))
            return self.collect()
        if message.kind == "drain":
            return self.drain(wait_timeout=float(message.args.get("wait_timeout", 0.5)))
        if message.kind == "child_result":
            return self.record_child_result(message.args)
        if message.kind == "summary":
            self._expire_timed_out_hosts()
            return self.summary()
        if message.kind == "cleanup":
            return self.cleanup_children()
        return self.not_handled()

    def collect(self) -> dict[str, Any]:
        with self.locked_state() as state:
            state.role = "parent"
            state.parent_host_url = self.context.address
            state.parent_agent_id = self.agent_id
            state.pending_hosts = []
            state.results = {}
            state.errors = {}
            state.cleanup_errors = {}
            state.child_proxies = {}
            timeout = state.timeout
            state.deadline = time.monotonic() + max(0.0, timeout)
        hosts = self.context.available_hosts(online_only=True, include_self=True)

        for host in hosts:
            with self.locked_state() as state:
                state.pending_hosts.append(host.name)
                state.role = "child"
                state.target_host_name = host.name
                state.target_host_url = host.url
            try:
                child = self.clone_to(host.name)
                with self.locked_state() as state:
                    state.child_proxies[host.name] = child.to_wire()
            except Exception as exc:
                with self.locked_state() as state:
                    state.pending_hosts = [name for name in state.pending_hosts if name != host.name]
                    state.errors[host.name] = str(exc)
            finally:
                with self.locked_state() as state:
                    state.role = "parent"
                    state.target_host_name = ""
                    state.target_host_url = ""

        return self.summary()

    def drain(self, *, wait_timeout: float) -> dict[str, Any]:
        self._expire_timed_out_hosts()

        def ready(state: PerformanceBenchmarkState) -> bool:
            return not state.pending_hosts

        timeout = max(0.0, wait_timeout)
        with self.locked_state() as state:
            if state.deadline > 0:
                timeout = min(timeout, max(0.0, state.deadline - time.monotonic()))
        self.wait_state(ready, timeout=timeout)
        self._expire_timed_out_hosts()
        summary = self.summary()
        return {"done": not summary["pending_hosts"], "summary": summary}

    def _run_child(self) -> None:
        with self.locked_state() as state:
            request_wire = dict(state.request)
            target_host_name = state.target_host_name
            target_host_url = state.target_host_url
            parent_agent_id = state.parent_agent_id
            parent_host_url = state.parent_host_url
        try:
            request = dataclass_from_wire(BenchmarkRequest, request_wire)
            result = run_host_benchmarks(
                request,
                host_name=self.context.name,
                host_url=self.context.address,
            )
            payload = {
                "host_name": target_host_name or self.context.name,
                "host_url": target_host_url or self.context.address,
                "result": dataclass_to_wire(result),
            }
        except Exception as exc:
            payload = {
                "host_name": target_host_name or self.context.name,
                "host_url": target_host_url or self.context.address,
                "error": str(exc),
            }

        parent = self.context.get_proxy(parent_agent_id, parent_host_url)
        try:
            if parent is not None:
                parent.send(Message("child_result", payload))
        finally:
            with contextlib.suppress(Exception):
                self.context.host.dispose(self.agent_id)

    @state_locked
    def record_child_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        host_name = str(payload["host_name"])
        self.state.pending_hosts = [name for name in self.state.pending_hosts if name != host_name]
        if payload.get("error"):
            self.state.errors[host_name] = str(payload["error"])
        else:
            self.state.results[host_name] = {
                "host_url": str(payload.get("host_url") or ""),
                "result": dict(payload.get("result") or {}),
            }
        return {"ok": True}

    @state_locked
    def summary(self) -> dict[str, Any]:
        return {
            "results": dict(self.state.results),
            "errors": dict(self.state.errors),
            "cleanup_errors": dict(self.state.cleanup_errors),
            "pending_hosts": list(self.state.pending_hosts),
        }

    def cleanup_children(self) -> dict[str, Any]:
        with self.locked_state() as state:
            children = {host_name: dict(proxy) for host_name, proxy in state.child_proxies.items()}
        for host_name, proxy_wire in children.items():
            try:
                from paglets.remote.proxy import PagletProxy

                PagletProxy.from_wire(proxy_wire, self.context.host.client).dispose()
            except Exception as exc:
                with self.locked_state() as state:
                    state.cleanup_errors[host_name] = str(exc)
        return self.summary()

    def _expire_timed_out_hosts(self) -> None:
        with self.locked_state() as state:
            if not state.pending_hosts or state.deadline <= 0 or time.monotonic() < state.deadline:
                return
            for host_name in list(state.pending_hosts):
                state.errors[host_name] = "timed out waiting for benchmark result"
            state.pending_hosts = []
        self.notify_all_state_changed()
