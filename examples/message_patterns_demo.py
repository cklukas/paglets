# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

from paglets import Message, Paglet, PagletState, ReplySet

try:
    from .support import local_hosts, run_importable_main
except ImportError:  # pragma: no cover - direct script execution
    from support import local_hosts, run_importable_main


@dataclass
class EchoState(PagletState):
    name: str = ""
    received: list[str] = field(default_factory=list)


class EchoAgent(Paglet[EchoState]):
    State = EchoState

    def handle_message(self, message: Message):
        if message.kind == "echo":
            value = message.get_arg("value") or message.get_arg()
            self.state.received.append(str(value))
            return f"{self.state.name}:{value}"
        if message.kind == "received":
            return list(self.state.received)
        return self.not_handled()


def main() -> None:
    with local_hosts("alpha") as hosts:
        (alpha,) = hosts
        proxies = [
            alpha.create(EchoAgent, EchoState(name="one")),
            alpha.create(EchoAgent, EchoState(name="two")),
            alpha.create(EchoAgent, EchoState(name="three")),
        ]

        future = proxies[0].send_future(Message("echo", arg="future"))
        print("future:", future.get_reply(timeout=2))

        reply_set = ReplySet(
            [
                proxy.send_future(Message("echo", {"value": "reply-set"}))
                for proxy in proxies
            ]
        )
        print("reply set:")
        for reply in reply_set:
            print(f"  - {reply.get_reply()}")

        multicast = alpha.multicast_message("echo", {"value": "broadcast"})
        print("multicast:")
        for reply in multicast:
            print(f"  - {reply.get_reply()}")


if __name__ == "__main__":
    run_importable_main("examples.message_patterns_demo")
