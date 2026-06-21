# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import heapq
import itertools
import threading
from typing import Any, Callable

from .messages import Message


@dataclass(frozen=True, slots=True)
class MailboxStatus:
    queued_count: int
    in_flight_count: int
    delivered_count: int
    failed_count: int

    def to_wire(self) -> dict[str, int]:
        return {
            "queued_count": self.queued_count,
            "in_flight_count": self.in_flight_count,
            "delivered_count": self.delivered_count,
            "failed_count": self.failed_count,
        }


class MessageMailbox:
    """Priority mailbox for one paglet."""

    def __init__(self, agent_id: str, handler: Callable[[Message, bool], Any], *, max_workers: int = 4):
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self.agent_id = agent_id
        self._handler = handler
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers + 4,
            thread_name_prefix=f"paglets-mailbox-{agent_id[:8]}",
        )
        self._condition = threading.Condition()
        self._queue: list[tuple[int, int, Message, bool, Future[Any]]] = []
        self._sequence = itertools.count()
        self._closed = False
        self._running = 0
        self._in_flight = 0
        self._delivered = 0
        self._failed = 0

    def submit(self, message: Message, *, oneway: bool = False) -> Future[Any]:
        future: Future[Any] = Future()
        with self._condition:
            if self._closed:
                future.set_exception(RuntimeError(f"Mailbox for {self.agent_id} is closed"))
                return future
            heapq.heappush(self._queue, (-message.priority, next(self._sequence), message, oneway, future))
            self._condition.notify_all()
        self._schedule()
        return future

    def submit_unqueued(self, message: Message, *, oneway: bool = False) -> Future[Any]:
        return self._executor.submit(self._run_message, message, oneway)

    def wait_message(self, timeout: float | None = None) -> bool:
        with self._condition:
            return self._condition.wait(timeout)

    def notify_message(self) -> None:
        with self._condition:
            self._condition.notify(1)

    def notify_all_messages(self) -> None:
        with self._condition:
            self._condition.notify_all()

    def status(self) -> MailboxStatus:
        with self._condition:
            return MailboxStatus(
                queued_count=len(self._queue),
                in_flight_count=self._in_flight,
                delivered_count=self._delivered,
                failed_count=self._failed,
            )

    def close(self) -> None:
        with self._condition:
            self._closed = True
            while self._queue:
                _, _, _, _, future = heapq.heappop(self._queue)
                if not future.done():
                    future.set_exception(RuntimeError(f"Mailbox for {self.agent_id} is closed"))
            self._condition.notify_all()
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _schedule(self) -> None:
        submissions = 0
        with self._condition:
            while self._queue and self._running + submissions < self._max_workers:
                submissions += 1
        for _ in range(submissions):
            self._executor.submit(self._run_next)

    def _run_next(self) -> None:
        with self._condition:
            if not self._queue:
                return
            self._running += 1
            _, _, message, oneway, future = heapq.heappop(self._queue)
            self._in_flight += 1
        try:
            result = self._handler(message, oneway)
        except Exception as exc:
            with self._condition:
                self._failed += 1
                self._in_flight -= 1
                self._running -= 1
                self._condition.notify_all()
            future.set_exception(exc)
        else:
            with self._condition:
                self._delivered += 1
                self._in_flight -= 1
                self._running -= 1
                self._condition.notify_all()
            future.set_result(result)
        self._schedule()

    def _run_message(self, message: Message, oneway: bool) -> Any:
        with self._condition:
            self._in_flight += 1
        try:
            result = self._handler(message, oneway)
        except Exception:
            with self._condition:
                self._failed += 1
                self._in_flight -= 1
                self._condition.notify_all()
            raise
        with self._condition:
            self._delivered += 1
            self._in_flight -= 1
            self._condition.notify_all()
        return result
