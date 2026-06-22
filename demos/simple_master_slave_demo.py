# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

from paglets.core.agent import Paglet, PagletState
from paglets.core.itinerary import ItineraryAgentMixin, ItineraryPlan
from paglets.core.messages import Message

try:
    from .support import local_hosts, run_importable_main
except ImportError:  # pragma: no cover - direct script execution
    from support import local_hosts, run_importable_main


@dataclass
class SlaveState(PagletState):
    master_host_url: str = ""
    master_agent_id: str = ""
    itinerary: ItineraryPlan = field(default_factory=ItineraryPlan)
    observations: list[str] = field(default_factory=list)


class SlaveAgent(ItineraryAgentMixin, Paglet[SlaveState]):
    """Python conversion of the Aglets simple master/slave travel pattern."""

    State = SlaveState

    def on_creation(self, event):
        self.state.observations.append(f"created@{event.host_name}")

    def on_arrival(self, event):
        self.state.observations.append(f"arrived@{event.host_name}")

    def run(self):
        self.state.observations.append(f"sample:{self.context.name}:{self.context.address}")
        proxy = self.go_to_next_destination()
        if proxy is None:
            master = self.context.get_proxy(self.state.master_agent_id, self.state.master_host_url)
            if master is not None:
                master.send(Message("result", {"slave": self.agent_id, "observations": list(self.state.observations)}))


@dataclass
class MasterState(PagletState):
    results: dict[str, list[str]] = field(default_factory=dict)
    launches: list[str] = field(default_factory=list)


class MasterAgent(Paglet[MasterState]):
    State = MasterState

    def handle_message(self, message: Message):
        if message.kind == "go":
            destinations = list(message.args["destinations"])
            if not destinations:
                return {"error": "no destinations"}
            first, remaining = destinations[0], destinations[1:]
            slave = self.context.create_paglet(
                SlaveAgent,
                SlaveState(
                    master_host_url=self.context.address,
                    master_agent_id=self.agent_id,
                    itinerary=ItineraryPlan(destinations=remaining),
                ),
                host_url=first,
            )
            self.state.launches.append(slave.agent_id)
            return slave.to_wire()
        if message.kind == "result":
            self.state.results[str(message.get_arg("slave"))] = list(message.get_arg("observations"))
            return {"ok": True}
        if message.kind == "results":
            return dict(self.state.results)
        return self.not_handled()


def main() -> None:
    with local_hosts("alpha", "beta", "gamma") as hosts:
        alpha, beta, gamma = hosts
        master = alpha.create(MasterAgent, MasterState())

        slave = master.send(Message("go", {"destinations": [beta.address, gamma.address, alpha.address]}))
        print(f"slave completed at {slave['host_url']} with id {slave['agent_id']}")
        for slave_id, observations in master.send(Message("results")).items():
            print(f"{slave_id}:")
            for line in observations:
                print(f"  - {line}")


if __name__ == "__main__":
    run_importable_main("demos.simple_master_slave_demo")
