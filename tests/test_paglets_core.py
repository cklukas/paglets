# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
import socket

import pytest

from paglets import Host, Message, Paglet, PagletState
from paglets.errors import NotHandledError


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class TravelState(PagletState):
    events: list[str] = field(default_factory=list)
    last_message: str | None = None


class TravelAgent(Paglet[TravelState]):
    State = TravelState

    def on_creation(self, event):
        self.state.events.append(f"created:{event.host_name}:{event.init}")

    def on_dispatching(self, event):
        self.state.events.append(f"dispatching:{event.source_host_name}->{event.target_host_name}")

    def on_arrival(self, event):
        self.state.events.append(f"arrived:{event.host_name}:from:{event.source_host_name}")

    def on_reverting(self, event):
        self.state.events.append(f"reverting:{event.host_name}->target:{event.target_host_name}")

    def run(self):
        self.state.events.append(f"run:{self.context.name}")

    def handle_message(self, message: Message):
        if message.kind == "remember":
            self.state.last_message = message.args["value"]
            return f"remembered:{self.state.last_message}"
        if message.kind == "go":
            return self.dispatch(message.args["target"]).to_wire()
        return self.not_handled()


@dataclass
class CloneState(PagletState):
    label: str = "original"
    events: list[str] = field(default_factory=list)


class CloneAgent(Paglet[CloneState]):
    State = CloneState

    def on_creation(self, event):
        self.state.events.append(f"created:{event.host_name}")

    def on_cloning(self, event):
        self.state.events.append(f"cloning:{event.host_name}->target:{event.target_host_name}")

    def on_clone(self, event):
        self.state.label = "clone"
        self.state.events.append(f"clone:{event.host_name}:from:{event.source_agent_id}")

    def on_cloned(self, event):
        self.state.events.append(f"cloned:{event.host_name}:clone:{event.clone_agent_id}")

    def run(self):
        self.state.events.append(f"run:{self.context.name}")


@pytest.fixture
def two_hosts():
    alpha = Host(name="alpha", host="127.0.0.1", port=free_port())
    beta = Host(name="beta", host="127.0.0.1", port=free_port())
    alpha.start_background()
    beta.start_background()
    try:
        yield alpha, beta
    finally:
        beta.stop()
        alpha.stop()


def test_create_and_synchronous_messages_use_dataclass_state(two_hosts):
    alpha, _ = two_hosts

    proxy = alpha.create(TravelAgent, TravelState(), init="seed")

    assert proxy.send(Message("remember", {"value": "hello"})) == "remembered:hello"
    state = alpha.get_state(proxy.agent_id, TravelState)
    assert state.last_message == "hello"
    assert state.events == ["created:alpha:seed", "run:alpha"]

    with pytest.raises(NotHandledError):
        proxy.send(Message("unknown"))


def test_dispatch_moves_agent_state_between_two_hosts(two_hosts):
    alpha, beta = two_hosts
    proxy = alpha.create(TravelAgent, TravelState(), init="seed")

    remote_proxy_wire = proxy.send(Message("go", {"target": beta.address}))

    assert remote_proxy_wire["agent_id"] == proxy.agent_id
    assert alpha.get_proxy(proxy.agent_id) is None
    remote_state = beta.get_state(proxy.agent_id, TravelState)
    assert remote_state.events == [
        "created:alpha:seed",
        "run:alpha",
        "dispatching:alpha->beta",
        "arrived:beta:from:alpha",
        "run:beta",
    ]


def test_clone_copies_dataclass_state_and_fires_original_and_clone_events(two_hosts):
    alpha, beta = two_hosts
    proxy = alpha.create(CloneAgent, CloneState(), init=None)

    clone_proxy = proxy.clone(target=beta.address)

    original_state = alpha.get_state(proxy.agent_id, CloneState)
    clone_state = beta.get_state(clone_proxy.agent_id, CloneState)

    assert proxy.agent_id != clone_proxy.agent_id
    assert original_state.label == "original"
    assert original_state.events == [
        "created:alpha",
        "run:alpha",
        "cloning:alpha->target:beta",
        f"cloned:alpha:clone:{clone_proxy.agent_id}",
    ]
    assert clone_state.label == "clone"
    assert clone_state.events == [
        "created:alpha",
        "run:alpha",
        "cloning:alpha->target:beta",
        f"clone:beta:from:{proxy.agent_id}",
        "run:beta",
    ]


def test_retract_pulls_a_remote_agent_back_to_the_requesting_host(two_hosts):
    alpha, beta = two_hosts
    proxy = alpha.create(TravelAgent, TravelState(), init="seed")
    proxy.send(Message("go", {"target": beta.address}))

    returned_proxy = alpha.retract(beta.address, proxy.agent_id)

    assert returned_proxy.agent_id == proxy.agent_id
    assert beta.get_proxy(proxy.agent_id) is None
    returned_state = alpha.get_state(proxy.agent_id, TravelState)
    assert returned_state.events[-3:] == [
        "reverting:beta->target:alpha",
        "arrived:alpha:from:beta",
        "run:alpha",
    ]
