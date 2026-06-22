# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message

try:
    from .support import local_hosts, run_importable_main
except ImportError:  # pragma: no cover - direct script execution
    from support import local_hosts, run_importable_main


@dataclass
class MobilityState(PagletState):
    history: list[str] = field(default_factory=list)
    hop_count: int = 0


class MobilityEventsPaglet(Paglet[MobilityState]):
    """Python conversion of examples/events/MobilityEvents.java."""

    State = MobilityState

    def on_creation(self, event):
        self.state.history.append("[History of MobilityEvents]")

    def on_dispatching(self, event):
        self.state.history.append(f"on_dispatching:{event.source_host_name}->{event.target_host_name}")

    def on_arrival(self, event):
        self.state.hop_count += 1
        self.state.history.append(f"on_arrival:{event.host_name}:from:{event.source_host_name}")

    def on_reverting(self, event):
        self.state.history.append(f"on_reverting:{event.host_name}->{event.target_host_name}")

    def run(self):
        self.state.history.append(f"run:{self.context.name}:hops={self.state.hop_count}")

    def handle_message(self, message: Message):
        if message.kind == "history":
            return "\n".join(self.state.history)
        if message.kind == "clear":
            self.state.history.clear()
            self.state.hop_count = 0
            return {"ok": True}
        if message.kind == "go":
            return self.dispatch(message.args["target"]).to_wire()
        return self.not_handled()


def main() -> None:
    with local_hosts("alpha", "beta") as hosts:
        alpha, beta = hosts
        proxy = alpha.create(MobilityEventsPaglet, MobilityState())
        remote = proxy.dispatch(beta.address)
        returned = alpha.retract(beta.address, remote.agent_id)

        print(returned.send(Message("history")))


if __name__ == "__main__":
    run_importable_main("examples.mobility_events_demo")
