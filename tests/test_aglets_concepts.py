# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

from paglets import (
    EXECUTE_ON_ARRIVAL,
    EXECUTE_ON_DEFAULT,
    EXECUTE_ON_DISPATCH,
    ACTIVE,
    INACTIVE,
    Host,
    ItineraryAgentMixin,
    ItineraryPlan,
    ItineraryTask,
    Message,
    Paglet,
    PagletState,
    ReplySet,
    TaskItineraryPlan,
)
from tests.test_paglets_core import free_port


@dataclass
class EchoState(PagletState):
    label: str = ""
    seen: list[str] = field(default_factory=list)
    init: str | None = None


class EchoAgent(Paglet[EchoState]):
    State = EchoState

    def on_creation(self, event):
        self.state.init = event.init

    def handle_message(self, message: Message):
        if message.kind == "echo":
            value = str(message.get_arg("value"))
            self.state.seen.append(value)
            return f"{self.context.name}:{self.state.label}:{value}"
        if message.kind == "single-arg":
            return message.get_arg()
        return self.not_handled()


@dataclass
class RouteState(PagletState):
    itinerary: ItineraryPlan = field(default_factory=ItineraryPlan)
    running: bool = False
    events: list[str] = field(default_factory=list)


class RouteAgent(ItineraryAgentMixin, Paglet[RouteState]):
    State = RouteState

    def on_creation(self, event):
        self.state.events.append(f"created@{event.host_name}")

    def on_dispatching(self, event):
        self.state.events.append(f"leaving:{event.source_host_name}->{event.target_host_name}")

    def on_arrival(self, event):
        self.state.events.append(f"arrived@{event.host_name}")

    def run(self):
        if self.state.running:
            self._continue()

    def handle_message(self, message: Message):
        if message.kind == "start":
            self.state.running = True
            proxy = self._continue()
            return proxy.to_wire() if proxy is not None else {"done": True}
        return self.not_handled()

    def _continue(self):
        proxy = self.go_to_next_destination()
        if proxy is None:
            self.state.running = False
            self.state.events.append(f"done@{self.context.name}")
        return proxy


def test_future_reply_and_reply_set_collect_async_message_results():
    host = Host(name="alpha", host="127.0.0.1", port=free_port())
    host.start_background()
    try:
        first = host.create(EchoAgent, EchoState(label="first"))
        second = host.create(EchoAgent, EchoState(label="second"))

        future = first.send_future_message("echo", {"value": "one"})
        assert future.get_reply(timeout=2) == "alpha:first:one"

        replies = ReplySet(
            [
                first.send_future_message("echo", {"value": "two"}),
                second.send_future_message("echo", {"value": "two"}),
            ]
        )
        assert replies.wait_for_all_replies(timeout=2)
        assert sorted(reply.get_reply() for reply in replies) == [
            "alpha:first:two",
            "alpha:second:two",
        ]
    finally:
        host.stop()


def test_context_multicast_returns_reply_set_for_active_local_agents():
    host = Host(name="alpha", host="127.0.0.1", port=free_port())
    host.start_background()
    try:
        host.create(EchoAgent, EchoState(label="first"))
        host.create(EchoAgent, EchoState(label="second"))

        replies = host.multicast_message("echo", {"value": "broadcast"})

        assert replies.wait_for_all_replies(timeout=2)
        assert sorted(reply.get_reply() for reply in replies) == [
            "alpha:first:broadcast",
            "alpha:second:broadcast",
        ]
    finally:
        host.stop()


def test_remote_create_and_inactive_proxy_status_are_exposed_over_http():
    alpha = Host(name="alpha", host="127.0.0.1", port=free_port())
    beta = Host(name="beta", host="127.0.0.1", port=free_port())
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create_remote(beta.address, EchoAgent, EchoState(label="remote"), init="seed")

        assert proxy.host_url == beta.address
        assert beta.get_state(proxy.agent_id, EchoState).init == "seed"
        assert proxy.is_valid()
        assert proxy.is_active()
        assert proxy.is_state(ACTIVE)
        assert [p.agent_id for p in beta.get_proxies(ACTIVE)] == [proxy.agent_id]

        proxy.deactivate()
        assert proxy.is_valid()
        assert not proxy.is_active()
        assert proxy.is_state(INACTIVE)
        assert [p.agent_id for p in beta.get_proxies(INACTIVE)] == [proxy.agent_id]
        assert beta.client.get_json(f"{beta.address}/agents?state=all")["agents"][0]["active"] is False

        proxy.activate()
        assert proxy.is_active()
    finally:
        beta.stop()
        alpha.stop()


def test_itinerary_plan_moves_agent_and_preserves_route_progress():
    alpha = Host(name="alpha", host="127.0.0.1", port=free_port())
    beta = Host(name="beta", host="127.0.0.1", port=free_port())
    alpha.start_background()
    beta.start_background()
    try:
        plan = ItineraryPlan(destinations=[beta.address, alpha.address, beta.address])
        proxy = alpha.create(RouteAgent, RouteState(itinerary=plan))

        proxy.send_message("start")

        final_state = beta.get_state(proxy.agent_id, RouteState)
        assert final_state.itinerary.completed is True
        assert final_state.itinerary.current_index == 3
        assert final_state.itinerary.visited_destinations == [beta.address, alpha.address, beta.address]
        assert final_state.events == [
            "created@alpha",
            "leaving:alpha->beta",
            "arrived@beta",
            "leaving:beta->alpha",
            "arrived@alpha",
            "leaving:alpha->beta",
            "arrived@beta",
            "done@beta",
        ]
    finally:
        beta.stop()
        alpha.stop()


def test_task_itinerary_keeps_default_and_destination_task_policy_serializable():
    plan = TaskItineraryPlan(destinations=["alpha", "beta"])
    plan.current_location = "alpha"
    default = ItineraryTask("log-every-arrival", EXECUTE_ON_DEFAULT)
    arrival = ItineraryTask("collect-local-info", EXECUTE_ON_ARRIVAL, {"key": "os.name"})
    dispatch = ItineraryTask("before-leaving", EXECUTE_ON_DISPATCH)

    assert plan.add_default_task(default)
    assert plan.add_task_for_destination("alpha", arrival)
    assert plan.add_task_for_destination("alpha", dispatch)

    assert [task.name for task in plan.tasks_for_phase("alpha", EXECUTE_ON_ARRIVAL)] == [
        "log-every-arrival",
        "collect-local-info",
    ]
    assert [task.name for task in plan.tasks_for_phase("alpha", EXECUTE_ON_DISPATCH)] == [
        "before-leaving"
    ]
