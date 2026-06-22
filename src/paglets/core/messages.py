# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any

from paglets.core.wire import WirePayload

SYNCHRONOUS = 0
FUTURE = 1
ONEWAY = 2

MIN_PRIORITY = 1
NORMAL_PRIORITY = 5
MAX_PRIORITY = 10
REENTRANT_PRIORITY = 0xFFFFFFFF
SYSTEM_PRIORITY = REENTRANT_PRIORITY - 1
REQUEST_PRIORITY = SYSTEM_PRIORITY - 1
UNQUEUED_PRIORITY = 0

CLONE = "_clone"
DISPATCH = "_dispatch"
DISPOSE = "_dispose"
DEACTIVATE = "_deactivate"
REVERT = "_revert"


@dataclass(slots=True)
class Message:
    """A message delivered to a paglet.

    Mirrors the useful part of Aglets' ``Message``: a kind, named arguments,
    optional single argument, priority, sender metadata, and a timestamp.
    Replies are represented by the return value of ``Paglet.handle_message``.
    """

    kind: str
    args: WirePayload = field(default_factory=dict)
    arg: Any = None
    sender: str | None = None
    reply_to: str | None = None
    priority: int = NORMAL_PRIORITY
    message_type: int = SYNCHRONOUS
    timestamp: float = field(default_factory=time.time)
    message_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def get_arg(self, name: str | None = None, default: Any = None) -> Any:
        if name is None:
            return self.arg
        return self.args.get(name, default)

    def set_arg(self, name: str, value: Any) -> Message:
        self.args[name] = value
        return self

    def same_kind(self, kind: str | Message) -> bool:
        return self.kind == (kind.kind if isinstance(kind, Message) else kind)

    def increase_priority(self) -> None:
        if self.priority < MAX_PRIORITY:
            self.priority += 1

    def decrease_priority(self) -> None:
        if self.priority > MIN_PRIORITY:
            self.priority -= 1

    def to_wire(self) -> WirePayload:
        return {
            "kind": self.kind,
            "args": self.args,
            "arg": self.arg,
            "sender": self.sender,
            "reply_to": self.reply_to,
            "priority": self.priority,
            "message_type": self.message_type,
            "timestamp": self.timestamp,
            "message_id": self.message_id,
        }

    @classmethod
    def from_wire(cls, payload: WirePayload) -> Message:
        return cls(
            kind=payload["kind"],
            args=dict(payload.get("args") or {}),
            arg=payload.get("arg"),
            sender=payload.get("sender"),
            reply_to=payload.get("reply_to"),
            priority=int(payload.get("priority", NORMAL_PRIORITY)),
            message_type=int(payload.get("message_type", SYNCHRONOUS)),
            timestamp=float(payload.get("timestamp", time.time())),
            message_id=str(payload.get("message_id") or uuid.uuid4().hex),
        )


class FutureReply:
    """Result handle for an asynchronous paglet message.

    This is the Python analogue of Aglets' ``FutureReply``. It wraps a
    ``concurrent.futures.Future`` and exposes Aglets-style names while still
    preserving the original exception behavior of ``Future.result()``.
    """

    def __init__(self, future: Future[Any]):
        self._future = future
        self._reply_sets: list[ReplySet] = []
        self._future.add_done_callback(lambda _: self._notify_reply_sets())

    def added_to(self, reply_set: ReplySet) -> None:
        self._reply_sets.append(reply_set)
        if self.is_available():
            reply_set.done(self)

    def is_available(self) -> bool:
        return self._future.done()

    def wait_for_reply(self, timeout: float | None = None) -> bool:
        try:
            self._future.result(timeout=timeout)
            return True
        except TimeoutError:
            return False

    def get_reply(self, timeout: float | None = None) -> Any:
        return self._future.result(timeout=timeout)

    def get_boolean_reply(self, timeout: float | None = None) -> bool:
        return bool(self.get_reply(timeout))

    def get_int_reply(self, timeout: float | None = None) -> int:
        return int(self.get_reply(timeout))

    def get_float_reply(self, timeout: float | None = None) -> float:
        return float(self.get_reply(timeout))

    def get_string_reply(self, timeout: float | None = None) -> str:
        return str(self.get_reply(timeout))

    def _notify_reply_sets(self) -> None:
        for reply_set in list(self._reply_sets):
            reply_set.done(self)


class ReplySet:
    """Container that yields ``FutureReply`` objects as replies arrive."""

    def __init__(self, replies: list[FutureReply] | None = None):
        self._done: list[FutureReply] = []
        self._unavailable: list[FutureReply] = []
        self._condition = threading.Condition()
        for reply in replies or []:
            self.add_future_reply(reply)

    def add_future_reply(self, reply: FutureReply) -> None:
        with self._condition:
            if reply.is_available():
                self._done.append(reply)
            else:
                self._unavailable.append(reply)
            reply.added_to(self)
            self._condition.notify_all()

    def done(self, reply: FutureReply) -> None:
        with self._condition:
            if reply in self._done:
                return
            if reply in self._unavailable:
                self._unavailable.remove(reply)
            self._done.append(reply)
            self._condition.notify_all()

    def are_all_available(self) -> bool:
        with self._condition:
            return not self._unavailable

    def count_available(self) -> int:
        with self._condition:
            return len(self._done)

    def count_unavailable(self) -> int:
        with self._condition:
            return len(self._unavailable)

    def is_any_available(self) -> bool:
        with self._condition:
            return bool(self._done)

    def has_more_future_replies(self) -> bool:
        with self._condition:
            return bool(self._done or self._unavailable)

    def wait_for_all_replies(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._unavailable:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def wait_for_next_future_reply(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._done:
                if not self._unavailable:
                    return False
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def get_next_future_reply(self, timeout: float | None = None) -> FutureReply | None:
        if not self.wait_for_next_future_reply(timeout):
            return None
        with self._condition:
            return self._done.pop(0)

    def __iter__(self) -> Iterator[FutureReply]:
        while self.has_more_future_replies():
            reply = self.get_next_future_reply()
            if reply is not None:
                yield reply
