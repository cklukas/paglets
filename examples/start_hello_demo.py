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
class FirstState(PagletState):
    log: list[str] = field(default_factory=list)


class FirstPaglet(Paglet[FirstState]):
    """Python conversion of examples/start/FirstAglet.java."""

    State = FirstState

    def run(self):
        self.state.log.append(f"Hello, I'm running on {self.context.name}")

    def handle_message(self, message: Message):
        if message.kind == "log":
            return list(self.state.log)
        return self.not_handled()


@dataclass
class VanillaState(PagletState):
    created_at: str | None = None


class VanillaPaglet(Paglet[VanillaState]):
    """Python conversion of examples/simple/VanillaAglet.java."""

    State = VanillaState

    def on_creation(self, event):
        self.state.created_at = event.host_name


def main() -> None:
    with local_hosts("alpha") as hosts:
        (alpha,) = hosts
        first = alpha.create(FirstPaglet, FirstState())
        vanilla = alpha.create(VanillaPaglet, VanillaState())

        print(first.send(Message("log"))[0])
        print(f"vanilla exists with id {vanilla.agent_id} on {vanilla.host_url}")


if __name__ == "__main__":
    run_importable_main("examples.start_hello_demo")
