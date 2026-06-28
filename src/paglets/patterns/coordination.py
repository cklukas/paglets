# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, cast

from paglets.core.agent import PagletState
from paglets.remote.mesh import HostRef
from paglets.remote.proxy import PagletProxy


@dataclass
class ChildRoleState(PagletState):
    role: str = "parent"
    parent_host_url: str = ""
    parent_agent_id: str = ""
    target_host_name: str = ""
    target_host_url: str = ""


@dataclass
class MeshFanoutState(ChildRoleState):
    deadline: float = 0.0
    pending_hosts: list[str] = field(default_factory=list)
    done_hosts: list[str] = field(default_factory=list)
    child_proxies: dict[str, dict[str, str]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)


class MeshFanoutMixin:
    """Common parent/child clone fanout helpers."""

    def fanout_reset(self, *, timeout: float) -> None:
        with cast(Any, self).locked_state() as state:
            state.role = "parent"
            state.parent_host_url = cast(Any, self).context.address
            state.parent_agent_id = cast(Any, self).agent_id
            state.target_host_name = ""
            state.target_host_url = ""
            state.pending_hosts = []
            state.done_hosts = []
            state.child_proxies = {}
            state.errors = {}
            state.cleanup_errors = {}
            state.deadline = time.monotonic() + max(0.0, float(timeout))
        cast(Any, self).notify_all_state_changed()

    def fanout_available_hosts(self, *, include_self: bool = True) -> list[HostRef]:
        return list(cast(Any, self).context.available_hosts(online_only=True, include_self=include_self))

    def fanout_select_hosts(self, targets: list[str] | tuple[str, ...], *, include_self: bool = True) -> list[HostRef]:
        if not targets:
            return self.fanout_available_hosts(include_self=include_self)
        selected: list[HostRef] = []
        for target in targets:
            ref = cast(Any, self).context.host_status(target)
            if ref is None or not ref.online:
                self.fanout_record_error(target, "target host is not online or not visible in the mesh")
                continue
            selected.append(ref)
        return selected

    def fanout_prepare_clone(self, host: HostRef) -> None:
        with cast(Any, self).locked_state() as state:
            state.pending_hosts.append(host.name)
            state.role = "child"
            state.target_host_name = host.name
            state.target_host_url = host.url

    def fanout_finish_clone_prepare(self) -> None:
        with cast(Any, self).locked_state() as state:
            state.role = "parent"
            state.target_host_name = ""
            state.target_host_url = ""

    def fanout_record_child_proxy(self, host_name: str, proxy: PagletProxy) -> None:
        with cast(Any, self).locked_state() as state:
            state.child_proxies[host_name] = proxy.to_wire()

    def fanout_record_error(self, host_name: str, error: str) -> None:
        with cast(Any, self).locked_state() as state:
            state.pending_hosts = [name for name in state.pending_hosts if name != host_name]
            state.errors[host_name] = error
        cast(Any, self).notify_all_state_changed()

    def fanout_record_done(self, host_name: str) -> None:
        if not host_name:
            return
        with cast(Any, self).locked_state() as state:
            state.pending_hosts = [name for name in state.pending_hosts if name != host_name]
            if host_name not in state.done_hosts:
                state.done_hosts.append(host_name)
        cast(Any, self).notify_all_state_changed()

    def fanout_expire_pending(self, error: str) -> None:
        with cast(Any, self).locked_state() as state:
            if not state.pending_hosts or state.deadline <= 0 or time.monotonic() < state.deadline:
                return
            pending = list(state.pending_hosts)
            for host_name in pending:
                state.errors[host_name] = error
            state.pending_hosts = []
        cast(Any, self).notify_all_state_changed()

    def fanout_cleanup_children(self) -> None:
        with cast(Any, self).locked_state() as state:
            children = {host_name: dict(proxy) for host_name, proxy in state.child_proxies.items()}
        for host_name, proxy_wire in children.items():
            try:
                PagletProxy.from_wire(proxy_wire, cast(Any, self).context.host.client).dispose()
            except Exception as exc:
                with cast(Any, self).locked_state() as state:
                    state.cleanup_errors[host_name] = str(exc)
        cast(Any, self).notify_all_state_changed()
