# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
import socket
import time

from paglets import Host, Message, Paglet, PagletState


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class SumState(PagletState):
    numbers: list[int] = field(default_factory=list)
    branch: str = "root"
    result: int | None = None
    events: list[str] = field(default_factory=list)


class SumWorkerAgent(Paglet[SumState]):
    State = SumState

    def on_creation(self, event):
        self.state.events.append(f"created@{event.host_name}")

    def on_cloning(self, event):
        self.state.events.append(f"cloning:{event.host_name}->{event.target_host_name}")

    def on_clone(self, event):
        self.state.events.append(f"clone@{event.host_name}:from:{event.source_agent_id}")
        self._compute()

    def on_cloned(self, event):
        self.state.events.append(f"cloned:{event.host_name}:clone:{event.clone_agent_id}")

    def handle_message(self, message: Message):
        if message.kind == "split":
            target = message.args["target"]
            mid = len(self.state.numbers) // 2
            left = self.state.numbers[:mid]
            right = self.state.numbers[mid:]

            # The clone receives the current dataclass state. Set up the clone's
            # half first, clone it, then make the original compute the other half.
            self.state.branch = "left-clone"
            self.state.numbers = left
            clone_proxy = self.clone(target=target)

            self.state.branch = "right-original"
            self.state.numbers = right
            self._compute()
            return {"original": self.context.get_proxy(self.agent_id).to_wire(), "clone": clone_proxy.to_wire()}
        return self.not_handled()

    def _compute(self) -> None:
        self.state.result = sum(self.state.numbers)
        self.state.events.append(f"computed:{self.state.branch}@{self.context.name}={self.state.result}")


def main() -> None:
    alpha = Host("alpha", port=free_port())
    beta = Host("beta", port=free_port())
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create(SumWorkerAgent, SumState(numbers=list(range(1, 11))))
        reply = proxy.send_message("split", {"target": beta.address})
        time.sleep(0.2)

        original_state = alpha.get_state(reply["original"]["agent_id"], SumState)
        clone_state = beta.get_state(reply["clone"]["agent_id"], SumState)
        print(f"original on alpha: {original_state.branch} -> {original_state.result}")
        print(f"clone on beta:     {clone_state.branch} -> {clone_state.result}")
        print(f"combined result:   {original_state.result + clone_state.result}")
    finally:
        beta.stop()
        alpha.stop()


if __name__ == "__main__":
    main()
