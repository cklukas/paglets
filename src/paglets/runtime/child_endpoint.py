# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import queue
import threading
import uuid
from multiprocessing.connection import Connection
from typing import TYPE_CHECKING, Any

from paglets.core.agent import Paglet
from paglets.core.errors import (
    InvalidAgentError,
)
from paglets.runtime.process_protocol import (
    _agent_snapshot,
    _error_from_wire,
    _error_to_wire,
    _handle_local_pickle_stream_event,
    _materialize_state_stream,
    _state_stream_token,
    _stream_state_payload,
)

if TYPE_CHECKING:
    from paglets.runtime.child_facade import _ChildHostFacade


class _ChildEndpoint:
    def __init__(self, conn: Connection):
        self._conn = conn
        self._send_lock = threading.Lock()
        self._pending: dict[str, queue.Queue[dict[str, Any]]] = {}
        self._pending_lock = threading.Lock()
        self._requests: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._closed = threading.Event()
        self.agent: Paglet | None = None
        self.facade: _ChildHostFacade | None = None

    def start_reader(self) -> threading.Thread:
        thread = threading.Thread(target=self._reader_loop, name="paglets-child-ipc", daemon=True)
        thread.start()
        return thread

    def next_request(self) -> dict[str, Any] | None:
        return self._requests.get()

    def reply_ok(self, request_id: str, payload: Any) -> None:
        self._send({"type": "reply", "id": request_id, "ok": True, "payload": _stream_state_payload(payload)})

    def reply_error(self, request_id: str, exc: Exception) -> None:
        self._send({"type": "reply", "id": request_id, "ok": False, "error": _error_to_wire(exc)})

    def host_call(self, op: str, payload: dict[str, Any] | None = None) -> Any:
        request_id = uuid.uuid4().hex
        inbox: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[request_id] = inbox
        self._send({"type": "host_call", "id": request_id, "op": op, "payload": payload or {}})
        reply = inbox.get()
        if reply.get("ok", False):
            token = _state_stream_token(reply.get("payload"))
            payload = _materialize_state_stream(reply.get("payload"))
            if token:
                self._send({"type": "event", "event": "local_pickle_stream_received", "token": token})
            return payload
        raise _error_from_wire(reply.get("error") or {})

    def close(self) -> None:
        self._closed.set()
        with contextlib.suppress(Exception):
            self._conn.close()

    def request_exit(self) -> None:
        self._requests.put(None)

    def _send(self, message: dict[str, Any]) -> None:
        with self._send_lock:
            self._conn.send(message)

    def _reader_loop(self) -> None:
        try:
            while not self._closed.is_set():
                try:
                    message = self._conn.recv()
                except EOFError:
                    break
                if not isinstance(message, dict):
                    continue
                if message.get("type") == "reply":
                    request_id = str(message.get("id") or "")
                    with self._pending_lock:
                        inbox = self._pending.pop(request_id, None)
                    if inbox is not None:
                        inbox.put(message)
                elif message.get("type") == "event":
                    _handle_local_pickle_stream_event(message)
                elif message.get("type") == "request":
                    if message.get("op") in {"state", "resource_status"}:
                        self._handle_control_request(message)
                    else:
                        self._requests.put(message)
        except OSError:
            pass
        finally:
            self._closed.set()
            self._requests.put(None)

    def _handle_control_request(self, message: dict[str, Any]) -> None:
        request_id = str(message.get("id") or "")
        try:
            if self.agent is None:
                raise InvalidAgentError("paglet child is not initialized")
            if message.get("op") == "state":
                payload = _agent_snapshot(self.agent)
            else:
                payload = {"resources": self.agent.resources.status()}
            self.reply_ok(request_id, payload)
        except Exception as exc:
            self.reply_error(request_id, exc)
