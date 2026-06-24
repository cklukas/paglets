# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from paglets.core.agent import PagletState
from paglets.core.messages import Message
from paglets.patterns.operations import OperationClient, OperationPaglet
from paglets.runtime.host import Host
from paglets.services.contracts import EmptyPayload, ServiceOperation
from tests.support import free_port


@dataclass(frozen=True, slots=True)
class EchoRequest:
    text: str = ""


@dataclass(frozen=True, slots=True)
class EchoReply:
    text: str = ""
    count: int = 0


@dataclass
class EchoState(PagletState):
    count: int = 0
    fallback_seen: str = ""


ECHO = ServiceOperation("echo", EchoRequest, EchoReply)
RECORD = ServiceOperation("record", EchoRequest, EmptyPayload)


class EchoPaglet(OperationPaglet[EchoState]):
    State = EchoState
    Operations = (ECHO, RECORD)

    def operation_handlers(self):
        return {
            ECHO: self.echo,
            RECORD: self.record,
        }

    def echo(self, request: EchoRequest) -> EchoReply:
        with self.locked_state() as state:
            state.count += 1
            count = state.count
        return EchoReply(text=request.text.upper(), count=count)

    def record(self, request: EchoRequest) -> EmptyPayload:
        with self.locked_state() as state:
            state.fallback_seen = request.text
        return EmptyPayload()

    def handle_operation_message(self, message: Message):
        if message.kind == "fallback":
            with self.locked_state() as state:
                state.fallback_seen = str(message.args.get("value") or "")
            return {"ok": True}
        return None


def test_operation_paglet_routes_typed_request_and_reply():
    paglet = EchoPaglet(EchoState())

    reply = paglet.handle_message(ECHO.to_message(EchoRequest("hello")))

    assert ECHO.decode_reply(reply) == EchoReply(text="HELLO", count=1)


def test_operation_paglet_keeps_fallback_escape_hatch():
    paglet = EchoPaglet(EchoState())

    reply = paglet.handle_message(Message("fallback", {"value": "seen"}))

    assert reply == {"ok": True}
    assert paglet.state.fallback_seen == "seen"


def test_operation_client_call_and_oneway(tmp_path: Path):
    host = Host(
        name="alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        proxy = host.create(EchoPaglet, EchoState())
        client = OperationClient(proxy)

        reply = client.call(ECHO, EchoRequest("client"))
        client.send_oneway(RECORD, EchoRequest("oneway"))

        deadline = time.monotonic() + 2.0
        while host.get_state(proxy.agent_id, EchoState).fallback_seen != "oneway" and time.monotonic() < deadline:
            time.sleep(0.02)

        assert reply == EchoReply(text="CLIENT", count=1)
        assert host.get_state(proxy.agent_id, EchoState).fallback_seen == "oneway"
    finally:
        host.stop()
