# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

from paglets import Host, Message, Paglet, PagletState
from tests.test_paglets_core import free_port


@dataclass
class ChainState(PagletState):
    itinerary: list[str] = field(default_factory=list)
    visits: list[str] = field(default_factory=list)
    running: bool = False


class ChainAgent(Paglet[ChainState]):
    State = ChainState

    def on_creation(self, event):
        self.state.visits.append(f"created@{event.host_name}")

    def on_dispatching(self, event):
        self.state.visits.append(f"leaving:{event.source_host_name}->{event.target_host_name}")

    def on_arrival(self, event):
        self.state.visits.append(f"arrived@{event.host_name}:from:{event.source_host_name}")

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
        if not self.state.itinerary:
            self.state.running = False
            self.state.visits.append(f"done@{self.context.name}")
            return None
        return self.dispatch(self.state.itinerary.pop(0))


def test_agent_can_self_dispatch_through_chain_that_returns_to_same_host():
    alpha = Host(name="alpha", host="127.0.0.1", port=free_port())
    beta = Host(name="beta", host="127.0.0.1", port=free_port())
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create(
            ChainAgent,
            ChainState(itinerary=[beta.address, alpha.address, beta.address]),
        )
        returned = proxy.send(Message("start"))

        assert returned == {"host_url": beta.address, "agent_id": proxy.agent_id}
        assert alpha.get_proxy(proxy.agent_id) is None
        final_state = beta.get_state(proxy.agent_id, ChainState)
        assert final_state.visits == [
            "created@alpha",
            "leaving:alpha->beta",
            "arrived@beta:from:alpha",
            "leaving:beta->alpha",
            "arrived@alpha:from:beta",
            "leaving:alpha->beta",
            "arrived@beta:from:alpha",
            "done@beta",
        ]
    finally:
        beta.stop()
        alpha.stop()
