# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

from paglets import Message, Paglet, PagletState

try:
    from .support import local_hosts
except ImportError:  # pragma: no cover - direct script execution
    from support import local_hosts


@dataclass
class FinderState(PagletState):
    database: dict[str, dict[str, str]] = field(default_factory=dict)


class FinderPaglet(Paglet[FinderState]):
    """Python conversion of examples/finder/Finder.java."""

    State = FinderState

    def on_creation(self, event):
        self.context.set_property("finder", self.context.get_proxy(self.agent_id))

    def handle_message(self, message: Message):
        if message.kind == "Lookup":
            name = message.get_arg("NAME") or message.get_arg()
            return self.state.database.get(name)
        if message.kind == "Register":
            self.state.database[str(message.get_arg("NAME"))] = dict(message.get_arg("PROXY"))
            return {"ok": True}
        if message.kind == "Unregister":
            self.state.database.pop(str(message.get_arg("NAME")), None)
            return {"ok": True}
        if message.kind == "Dump":
            return dict(self.state.database)
        return self.not_handled()


@dataclass
class TravellerState(PagletState):
    name: str = "Traveller"
    finder_host_url: str = ""
    finder_agent_id: str = ""
    registrations: list[str] = field(default_factory=list)


class TravellerPaglet(Paglet[TravellerState]):
    """Python conversion of Traveller.java plus Register.java."""

    State = TravellerState

    def on_creation(self, event):
        self._register()

    def on_arrival(self, event):
        self._register()

    def on_disposing(self, event):
        finder = self.context.get_proxy(self.state.finder_agent_id, self.state.finder_host_url)
        if finder is not None:
            finder.send_oneway_message("Unregister", {"NAME": self.state.name})

    def handle_message(self, message: Message):
        if message.kind == "go":
            return self.dispatch(message.args["target"]).to_wire()
        if message.kind == "registrations":
            return list(self.state.registrations)
        return self.not_handled()

    def _register(self) -> None:
        finder = self.context.get_proxy(self.state.finder_agent_id, self.state.finder_host_url)
        local_proxy = self.context.get_proxy(self.agent_id)
        if finder is None or local_proxy is None:
            return
        finder.send_oneway_message(
            "Register",
            {"NAME": self.state.name, "PROXY": local_proxy.to_wire()},
        )
        self.state.registrations.append(f"{self.state.name}@{self.context.name}")


def main() -> None:
    with local_hosts("alpha", "beta") as hosts:
        alpha, beta = hosts
        finder = alpha.create(FinderPaglet, FinderState())
        traveller = alpha.create(
            TravellerPaglet,
            TravellerState(finder_host_url=finder.host_url, finder_agent_id=finder.agent_id),
        )

        print("initial lookup:", finder.send_message("Lookup", {"NAME": "Traveller"}))
        traveller.send_message("go", {"target": beta.address})
        print("after dispatch:", finder.send_message("Lookup", {"NAME": "Traveller"}))
        beta.get_proxy(traveller.agent_id).dispose()  # type: ignore[union-attr]
        print("after dispose:", finder.send_message("Dump"))


if __name__ == "__main__":
    main()
