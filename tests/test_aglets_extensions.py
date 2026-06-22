# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time

import pytest

from paglets.persistence.persistency import DeactivationPolicy
from paglets.runtime.host import Host
from paglets.core.messages import Message
from paglets.core.agent import Paglet, PagletState, state_locked
from paglets.remote.references import PagletProxyRef
from paglets.core.runtime_values import ServiceScope, ArrivalMode
from paglets.core.errors import TransferError
from paglets.remote.transfer import TransferTicket
from paglets.core.errors import LifecycleError
from paglets.runtime.mailbox import MessageMailbox
from tests.test_paglets_core import free_port


@dataclass
class SyncState(PagletState):
    events: list[str] = field(default_factory=list)


class SyncAgent(Paglet[SyncState]):
    State = SyncState

    def handle_message(self, message: Message):
        if message.kind == "wait":
            self.state.events.append(f"wait-start:{message.get_arg()}")
            released = self.wait_message(timeout=float(message.args.get("timeout", 1.0)))
            self.state.events.append(f"wait-done:{message.get_arg()}:{released}")
            return released
        if message.kind == "notify":
            self.state.events.append("notify")
            if message.args.get("all"):
                self.notify_all_messages()
            else:
                self.notify_message()
            return "notified"
        return self.not_handled()


@dataclass
class ServiceState(PagletState):
    advertised: bool = False


class ServiceAgent(Paglet[ServiceState]):
    State = ServiceState

    def on_creation(self, event):
        self.advertise_service("quotes", capabilities=("quote", "price"), metadata={"version": 1}, scope=ServiceScope.MESH)
        self.state.advertised = True

    def handle_message(self, message: Message):
        if message.kind == "ping":
            return "pong"
        return self.not_handled()


@dataclass
class BasicState(PagletState):
    events: list[str] = field(default_factory=list)


class BasicAgent(Paglet[BasicState]):
    State = BasicState

    def run(self):
        self.state.events.append(f"run:{self.context.name}")

    def handle_message(self, message: Message):
        if message.kind == "ping":
            return "pong"
        return self.not_handled()


@dataclass
class LockingState(PagletState):
    count: int = 0
    active: int = 0
    max_active: int = 0
    events: list[str] = field(default_factory=list)


class LockingAgent(Paglet[LockingState]):
    State = LockingState

    def increment_many(self, amount: int) -> None:
        for _ in range(amount):
            with self.locked_state() as state:
                current = state.count
                time.sleep(0)
                state.count = current + 1

    @state_locked
    def decorated_increment(self, label: str) -> int:
        self.state.active += 1
        self.state.max_active = max(self.state.max_active, self.state.active)
        current = self.state.count
        time.sleep(0.02)
        self.state.count = current + 1
        self.state.events.append(label)
        self.state.active -= 1
        return self.state.count

    @state_locked
    def decorated_failure(self) -> None:
        raise RuntimeError("boom")


@dataclass
class SingleWorkerState(PagletState):
    started: list[str] = field(default_factory=list)
    finished: list[str] = field(default_factory=list)


class SingleWorkerAgent(Paglet[SingleWorkerState]):
    State = SingleWorkerState
    MAILBOX_WORKERS = 1

    def handle_message(self, message: Message):
        if message.kind == "hold":
            label = str(message.get_arg())
            with self.locked_state() as state:
                state.started.append(label)
            time.sleep(float(message.args.get("delay", 0.0)))
            with self.locked_state() as state:
                state.finished.append(label)
            return label
        return self.not_handled()


@dataclass
class WaitingCollectorState(PagletState):
    pending: bool = False
    result: str = ""


class WaitingCollectorAgent(Paglet[WaitingCollectorState]):
    State = WaitingCollectorState

    def handle_message(self, message: Message):
        if message.kind == "collect":
            with self.locked_state() as state:
                state.pending = True
                state.result = ""
            if self.wait_state(lambda state: not state.pending, timeout=1.0):
                with self.locked_state() as state:
                    return {"result": state.result}
            return {"error": "timeout"}
        if message.kind == "child_result":
            with self.locked_state() as state:
                state.result = str(message.args["value"])
                state.pending = False
            self.notify_all_state_changed()
            return {"ok": True}
        return self.not_handled()


@dataclass
class RefState(PagletState):
    ref: PagletProxyRef | None = None


class RefAgent(Paglet[RefState]):
    State = RefState


@dataclass
class ResourceState(PagletState):
    cleaned: list[str] = field(default_factory=list)


class ResourceAgent(Paglet[ResourceState]):
    State = ResourceState

    def handle_message(self, message: Message):
        if message.kind == "register":
            self.resources.register("first", lambda: self.state.cleaned.append("first"))
            self.resources.register("second", lambda: self.state.cleaned.append("second"))
            return "registered"
        if message.kind == "bad":
            def fail() -> None:
                self.state.cleaned.append("bad")
                raise RuntimeError("boom")

            self.resources.register("bad", fail)
            return "registered"
        if message.kind == "suppressed":
            def fail() -> None:
                self.state.cleaned.append("suppressed")
                raise RuntimeError("ignored")

            self.resources.register("suppressed", fail, suppress=True)
            return "registered"
        return self.not_handled()


def test_mailbox_priority_and_wait_notify_synchronization():
    started = threading.Event()
    release = threading.Event()
    handled: list[str] = []

    def handler(message: Message, oneway: bool):
        handled.append(f"start:{message.kind}")
        started.set()
        if message.kind.startswith("low"):
            release.wait(1)
        handled.append(f"done:{message.kind}")
        return message.kind

    mailbox = MessageMailbox("agent", handler, max_workers=1)
    try:
        low = mailbox.submit(Message("low", priority=1))
        started.wait(1)
        high = mailbox.submit(Message("high", priority=10))
        queued_low = mailbox.submit(Message("low-queued", priority=1))
        release.set()

        assert high.result(timeout=1) == "high"
        assert queued_low.result(timeout=1) == "low-queued"
        assert handled.index("start:high") < handled.index("start:low-queued")
        assert low.result(timeout=1) == "low"
    finally:
        mailbox.close()


def test_paglet_wait_message_notify_and_notify_all(tmp_path):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        proxy = host.create(SyncAgent, SyncState())
        one = proxy.send_future(Message("wait", {"timeout": 1.0}, arg="one"))
        two = proxy.send_future(Message("wait", {"timeout": 1.0}, arg="two"))
        _wait_until(lambda: host.get_state(proxy.agent_id, SyncState).events.count("wait-start:one") == 1)
        _wait_until(lambda: "wait-start:two" in host.get_state(proxy.agent_id, SyncState).events)

        assert proxy.send(Message("notify", {"all": True})) == "notified"
        assert one.get_reply(timeout=2) is False
        assert two.get_reply(timeout=2) is False

        timed_out = proxy.send(Message("wait", {"timeout": 0.05}, arg="timeout"))
        assert timed_out is False
    finally:
        host.stop()


def test_locked_state_serializes_concurrent_dataclass_access():
    agent = LockingAgent(LockingState())
    threads = [threading.Thread(target=agent.increment_many, args=(100,)) for _ in range(8)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert agent.state.count == 800


def test_state_locked_decorator_serializes_methods_and_preserves_exceptions():
    agent = LockingAgent(LockingState())
    results: list[int] = []
    threads = [
        threading.Thread(target=lambda label=label: results.append(agent.decorated_increment(label)))
        for label in ("one", "two")
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert sorted(results) == [1, 2]
    assert agent.state.count == 2
    assert agent.state.max_active == 1
    assert sorted(agent.state.events) == ["one", "two"]
    with pytest.raises(RuntimeError, match="boom"):
        agent.decorated_failure()


def test_wait_state_returns_immediately_when_predicate_already_true():
    agent = LockingAgent(LockingState(count=1))

    assert agent.wait_state(lambda state: state.count == 1, timeout=0.01) is True
    assert agent.wait_state(lambda state: state.count == 2, timeout=0.01) is False


def test_paglet_mailbox_workers_class_setting_limits_concurrent_handlers(tmp_path):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        proxy = host.create(SingleWorkerAgent, SingleWorkerState())
        first = proxy.send_future(Message("hold", {"delay": 0.3}, arg="one"))
        _wait_until(lambda: host.get_state(proxy.agent_id, SingleWorkerState).started == ["one"])
        second = proxy.send_future(Message("hold", {"delay": 0.0}, arg="two"))
        time.sleep(0.05)

        assert host.get_state(proxy.agent_id, SingleWorkerState).started == ["one"]
        assert first.get_reply(timeout=1) == "one"
        assert second.get_reply(timeout=1) == "two"
        assert host.get_state(proxy.agent_id, SingleWorkerState).started == ["one", "two"]
        assert host.get_state(proxy.agent_id, SingleWorkerState).finished == ["one", "two"]
    finally:
        host.stop()


def test_default_concurrent_mailbox_allows_collector_replies_while_waiting(tmp_path):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        proxy = host.create(WaitingCollectorAgent, WaitingCollectorState())
        collect = proxy.send_future(Message("collect"))
        _wait_until(lambda: host.get_state(proxy.agent_id, WaitingCollectorState).pending)

        assert collect.get_reply(timeout=2) == {"error": "timeout"}
        assert proxy.send(Message("child_result", {"value": "done"})) == {"ok": True}
        assert host.get_state(proxy.agent_id, WaitingCollectorState).result == "done"
    finally:
        host.stop()


def test_service_registry_local_ttl_capability_and_mesh_lookup(tmp_path):
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh_version="services-test",
        persistence_dir=tmp_path / "alpha",
    )
    beta = Host(
        "beta",
        host="127.0.0.1",
        port=free_port(),
        peers=[alpha.address],
        mesh_version="services-test",
        persistence_dir=tmp_path / "beta",
    )
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        proxy = beta.create(ServiceAgent, ServiceState())

        local = beta.lookup_service("quotes", capability="quote")
        assert local is not None
        assert local.proxy.agent_id == proxy.agent_id

        mesh = alpha.lookup_service("quotes", capability="price", scope=ServiceScope.MESH)
        assert mesh is not None
        assert mesh.proxy.resolve(alpha.client).send(Message("ping")) == "pong"

        beta.advertise_service(proxy.agent_id, "temporary", capabilities=("short",), ttl=0.05)
        assert beta.lookup_service("temporary", capability="short") is not None
        time.sleep(0.08)
        assert beta.lookup_service("temporary", capability="short") is None

        beta.unadvertise_service("quotes", agent_id=proxy.agent_id)
        assert beta.lookup_service("quotes") is None
    finally:
        beta.stop()
        alpha.stop()


def test_transfer_ticket_preflight_failure_preserves_source_and_inactive_arrival(tmp_path):
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    beta = Host(
        "beta",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "beta",
    )
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create(BasicAgent, BasicState())
        with pytest.raises(TransferError):
            proxy.dispatch(TransferTicket(beta.address, required_capabilities=("missing",)))
        assert alpha.get_proxy(proxy.agent_id) is not None

        inactive_proxy = proxy.dispatch(TransferTicket(beta.address, arrival_mode=ArrivalMode.INACTIVE))
        assert inactive_proxy.agent_id == proxy.agent_id
        assert alpha.get_proxy(proxy.agent_id) is None
        assert beta.get_proxy(proxy.agent_id) is None
        assert beta.client.get_json(f"{beta.address}/agents/{proxy.agent_id}")["active"] is False

        inactive_proxy.activate()
        assert beta.get_state(proxy.agent_id, BasicState).events[-1] == "run:beta"
    finally:
        beta.stop()
        alpha.stop()


def test_context_events_capture_listener_failures_services_and_message_failures(tmp_path):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    seen: list[str] = []

    def listener(event):
        seen.append(event.kind)
        if event.kind == "service-advertise":
            raise RuntimeError("listener failed")

    host.add_listener(listener)
    host.start_background()
    try:
        proxy = host.create(ServiceAgent, ServiceState())
        with pytest.raises(Exception):
            proxy.send(Message("missing"))

        events = host.list_events(limit=50)
        kinds = [event.kind for event in events]
        assert "context-start" in kinds
        assert "service-advertise" in kinds
        assert "event-listener-failed" in kinds
        assert "message-failed" in kinds
        assert "service-advertise" in seen
        assert host.client.get_json(f"{host.address}/events?since=0&limit=2")["events"][0]["event_id"] == 1
    finally:
        host.stop()


def test_proxy_ref_round_trips_through_dataclass_and_inactive_persistence(tmp_path):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        target = host.create(BasicAgent, BasicState())
        holder = host.create(RefAgent, RefState(ref=target.ref()))
        holder.deactivate(policy=DeactivationPolicy(activate_on_message=False))
        state = host.client.get_json(f"{host.address}/agents/{holder.agent_id}/state")
        assert state["state"]["ref"] == target.ref().to_wire()

        holder.activate()
        restored = host.get_state(holder.agent_id, RefState)
        assert restored.ref == target.ref()
        assert restored.ref.resolve(host.client).send(Message("ping")) == "pong"
    finally:
        host.stop()


def test_resource_cleanup_runs_in_reverse_order_and_failures_cancel_lifecycle(tmp_path):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        proxy = host.create(ResourceAgent, ResourceState())
        assert proxy.send(Message("register")) == "registered"
        proxy.deactivate()
        inactive = host.client.get_json(f"{host.address}/agents/{proxy.agent_id}/state")
        assert inactive["state"]["cleaned"] == ["second", "first"]

        active = proxy.activate()
        assert active.send(Message("bad")) == "registered"
        with pytest.raises(LifecycleError):
            active.deactivate()
        assert host.get_proxy(active.agent_id) is not None

        host.resources_for(active.agent_id).remove("bad")
        assert active.send(Message("suppressed")) == "registered"
        host.dispose(active.agent_id)
        assert host.client.get_json(f"{host.address}/agents?state=all")["agents"] == []
    finally:
        host.stop()


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition was not met before timeout")
