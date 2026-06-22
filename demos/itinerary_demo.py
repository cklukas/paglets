# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

from paglets.core.agent import ACTIVE, Paglet, PagletState
from paglets.core.itinerary import (
    EXECUTE_ON_ARRIVAL,
    EXECUTE_ON_DEFAULT,
    EXECUTE_ON_DISPATCH,
    ItineraryAgentMixin,
    ItineraryTask,
    TaskItineraryPlan,
)
from paglets.core.messages import Message
from paglets.runtime.host import Host

try:
    from .support import local_hosts, run_importable_main
except ImportError:  # pragma: no cover - direct script execution
    from support import local_hosts, run_importable_main


@dataclass
class CirculateState(PagletState):
    itinerary: TaskItineraryPlan = field(default_factory=TaskItineraryPlan)
    running: bool = False
    report: list[str] = field(default_factory=list)


class CirculateAgent(ItineraryAgentMixin, Paglet[CirculateState]):
    """Python conversion of the Aglets CirculateAglet/SeqPlanItinerary idea."""

    State = CirculateState

    def on_creation(self, event):
        self.state.report.append(f"created@{event.host_name}")

    def on_dispatching(self, event):
        self.execute_itinerary_tasks(EXECUTE_ON_DISPATCH, event)
        self.state.report.append(f"leaving:{event.source_host_name}->{event.target_host_name}")

    def on_arrival(self, event):
        self.state.report.append(f"arrived@{event.host_name}:from:{event.source_host_name}")
        self.execute_itinerary_tasks(EXECUTE_ON_ARRIVAL, event)

    def run(self):
        if self.state.running:
            self._continue()

    def handle_message(self, message: Message):
        if message.kind == "start":
            self.state.running = True
            proxy = self._continue()
            return proxy.to_wire() if proxy is not None else {"done": True}
        if message.kind == "report":
            return {
                "host": self.context.name,
                "report": list(self.state.report),
                "visited": list(self.state.itinerary.visited_destinations),
            }
        return self.not_handled()

    def execute_itinerary_task(self, task: ItineraryTask, phase: str, event=None):
        if task.name == "record-host":
            self.state.report.append(f"task:{phase}:record-host:{self.context.name}:{self.context.address}")
        elif task.name == "count-proxies":
            count = len(self.context.get_proxies(ACTIVE))
            self.state.report.append(f"task:{phase}:active-proxies:{self.context.name}:{count}")
        elif task.name == "note":
            self.state.report.append(f"task:{phase}:note:{task.args['text']}")
        else:
            raise ValueError(f"unknown task {task.name!r}")

    def _continue(self):
        proxy = self.go_to_next_destination()
        if proxy is None:
            self.state.running = False
            self.state.report.append(f"done@{self.context.name}")
        return proxy


def find_state(hosts: list[Host], agent_id: str) -> tuple[str, CirculateState] | None:
    for host in hosts:
        try:
            return host.name, host.get_state(agent_id, CirculateState)  # type: ignore[return-value]
        except Exception:
            pass
    return None


def build_plan(alpha: Host, beta: Host) -> TaskItineraryPlan:
    plan = TaskItineraryPlan(destinations=[beta.address, alpha.address, beta.address])
    plan.add_default_task(ItineraryTask("record-host", EXECUTE_ON_DEFAULT))
    plan.add_task_for_destination(beta.address, ItineraryTask("count-proxies", EXECUTE_ON_ARRIVAL))
    plan.add_task_for_destination(alpha.address, ItineraryTask("note", EXECUTE_ON_ARRIVAL, {"text": "back home"}))
    plan.add_task_for_destination(beta.address, ItineraryTask("note", EXECUTE_ON_DISPATCH, {"text": "next stop beta"}))
    plan.set_immutable()
    return plan


def main() -> None:
    with local_hosts("alpha", "beta") as hosts:
        alpha, beta = hosts
        proxy = alpha.create(CirculateAgent, CirculateState(itinerary=build_plan(alpha, beta)))
        proxy.send(Message("start"))

        found = find_state(hosts, proxy.agent_id)
        if found is None:
            raise RuntimeError("agent disappeared")

        host_name, state = found
        print(f"final host: {host_name}")
        print("route:")
        for destination in state.itinerary.visited_destinations:
            print(f"  - {destination}")
        print("report:")
        for line in state.report:
            print(f"  - {line}")


if __name__ == "__main__":
    run_importable_main("demos.itinerary_demo")
