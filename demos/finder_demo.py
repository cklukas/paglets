# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message
from paglets.core.runtime_values import ServiceScope
from paglets.runtime.host import Host

try:
    from .support import local_hosts, run_importable_main
except ImportError:  # pragma: no cover - direct script execution
    from support import local_hosts, run_importable_main


@dataclass
class TravellerState(PagletState):
    name: str = "Traveller"
    registrations: list[str] = field(default_factory=list)


class TravellerPaglet(Paglet[TravellerState]):
    """Service-registry version of the classic Aglets finder/traveller pattern."""

    State = TravellerState

    def on_creation(self, event):
        self._advertise()

    def on_arrival(self, event):
        self._advertise()

    def handle_message(self, message: Message):
        if message.kind == "go":
            return self.dispatch(message.args["target"]).to_wire()
        if message.kind == "registrations":
            return list(self.state.registrations)
        return self.not_handled()

    def _advertise(self) -> None:
        self.advertise_service(
            self.state.name,
            capabilities=("travel",),
            metadata={"host": self.context.name},
            scope=ServiceScope.MESH,
        )
        self.state.registrations.append(f"{self.state.name}@{self.context.name}")


def lookup_traveller(host: Host, name: str = "Traveller") -> dict[str, str] | None:
    record = host.lookup_service(name, capability="travel", scope=ServiceScope.MESH)
    return record.proxy.to_wire() if record is not None else None


def main() -> None:
    with local_hosts("alpha", "beta", mesh=True) as hosts:
        alpha, beta = hosts
        traveller = alpha.create(TravellerPaglet, TravellerState())

        print("initial lookup:", lookup_traveller(alpha))
        traveller.send(Message("go", {"target": beta.address}))
        print("after dispatch:", lookup_traveller(alpha))
        beta.get_proxy(traveller.agent_id).dispose()  # type: ignore[union-attr]
        print("after dispose:", lookup_traveller(alpha))


if __name__ == "__main__":
    run_importable_main("demos.finder_demo")
