# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import multiprocessing as mp
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any

from paglets.core.errors import (
    HostError,
    PagletCrashedError,
)
from paglets.core.messages import Message
from paglets.remote.transport import (
    start_local_pickle_sender,
)
from paglets.runtime.child_bootstrap import _child_main
from paglets.runtime.process_protocol import (
    ChildConfig,
    _error_from_wire,
    _error_to_wire,
    _handle_local_pickle_stream_event,
    _materialize_state_stream,
    _short_class_name,
    _state_stream_token,
)


class ChildProcessController:
    """Parent-side controller for one isolated paglet child process."""

    def __init__(
        self,
        config: ChildConfig,
        *,
        host_call_handler: Callable[[str, dict[str, Any]], Any],
        crash_handler: Callable[[ChildProcessController], None],
    ):
        self.config = config
        self.agent_id = config.agent_id
        self.agent_class_name = config.agent_class_name
        self.state_class_name = config.state_class_name
        self.state: dict[str, Any] = dict(config.state or {})
        self.resource_status: dict[str, bool] = {}
        self.ready = False
        self.crashed = False
        self.exitcode: int | None = None
        self.last_error = ""
        self.departing = False
        self._host_call_handler = host_call_handler
        self._crash_handler = crash_handler
        self._pending: dict[str, Future[Any]] = {}
        self._pending_ops: dict[str, str] = {}
        self._pending_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._closed = threading.Event()
        self._process_closed = False
        self._terminal_message_result: Any = None
        self._has_terminal_message_result = False
        self._run_complete = threading.Event()
        self._run_complete.set()
        context = mp.get_context("spawn")
        parent_conn, child_conn = context.Pipe(duplex=True)
        self._conn = parent_conn
        self.process = context.Process(
            target=_child_main,
            args=(config, child_conn),
            name=config.process_title,
            daemon=True,
        )
        self.process.start()
        self._pid = self.process.pid
        child_conn.close()
        self._reader = threading.Thread(
            target=self._reader_loop,
            name=f"paglets-child-reader-{self.agent_id[:8]}",
            daemon=True,
        )
        self._reader.start()

    @property
    def pid(self) -> int | None:
        return self._pid

    def terminal_proxy_wire(self) -> dict[str, str] | None:
        if not self._has_terminal_message_result or not isinstance(self._terminal_message_result, dict):
            return None
        if "host_url" not in self._terminal_message_result or "agent_id" not in self._terminal_message_result:
            return None
        return {
            "host_url": str(self._terminal_message_result["host_url"]),
            "agent_id": str(self._terminal_message_result["agent_id"]),
        }

    def set_terminal_proxy_wire(self, proxy: dict[str, Any]) -> None:
        self._has_terminal_message_result = True
        self._terminal_message_result = {
            "host_url": str(proxy["host_url"]),
            "agent_id": str(proxy["agent_id"]),
        }

    def request(self, op: str, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> Any:
        if self._closed.is_set():
            raise PagletCrashedError(f"Paglet {self.agent_id!r} is not running")
        request_id = uuid.uuid4().hex
        future: Future[Any] = Future()
        with self._pending_lock:
            self._pending[request_id] = future
            self._pending_ops[request_id] = op
        try:
            self._send({"type": "request", "id": request_id, "op": op, "payload": payload or {}})
        except Exception:
            with self._pending_lock:
                self._pending.pop(request_id, None)
                self._pending_ops.pop(request_id, None)
            raise
        return future.result(timeout=timeout)

    def request_lifecycle(self, name: str, event: dict[str, Any]) -> dict[str, Any]:
        if name in {"arrival"}:
            self._run_complete.clear()
        reply = self.request("lifecycle", {"name": name, "event": event})
        self._update_from_reply(reply)
        return dict(reply)

    def wait_for_run_complete_or_departure(self, *, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + max(0.0, timeout)
        while not self.departing and not self._closed.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            if self._run_complete.wait(min(0.05, remaining)):
                return

    def request_message(self, message: Message, *, oneway: bool = False) -> Any:
        reply = self.request("message", {"message": message.to_wire(), "oneway": oneway})
        self._update_from_reply(reply)
        return None if oneway else reply.get("result")

    def fetch_state(self, *, timeout: float | None = None) -> dict[str, Any]:
        reply = self.request("state", timeout=timeout)
        self._update_from_reply(reply)
        return dict(self.state)

    def cleanup_resources(self, *, reason: str) -> dict[str, Any]:
        reply = self.request("cleanup_resources", {"reason": reason})
        self._update_from_reply(reply)
        return dict(reply)

    def resource_status_snapshot(self) -> dict[str, bool]:
        try:
            reply = self.request("resource_status", timeout=2.0)
            self._update_from_reply(reply)
        except Exception:
            pass
        return dict(self.resource_status)

    def shutdown(self, *, graceful: bool = True, timeout: float = 2.0) -> None:
        if self._closed.is_set():
            return
        if graceful:
            with contextlib.suppress(Exception):
                self.request("shutdown", timeout=timeout)
        self._closed.set()
        with contextlib.suppress(Exception):
            self._conn.close()

    def terminate(self, *, timeout: float = 2.0, kill_timeout: float = 1.0) -> None:
        self.shutdown(graceful=True, timeout=timeout)
        if self._process_closed:
            self._closed.set()
            return
        if self._is_process_alive():
            self.process.terminate()
            self.process.join(timeout=timeout)
        if self._is_process_alive():
            self.process.kill()
            self.process.join(timeout=kill_timeout)
        self.exitcode = self._safe_exitcode()
        self._close_process_handle()
        self._closed.set()

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
                kind = message.get("type")
                if kind == "reply":
                    self._complete_reply(message)
                elif kind == "host_call":
                    threading.Thread(
                        target=self._handle_host_call,
                        args=(message,),
                        name=f"paglets-host-call-{self.agent_id[:8]}",
                        daemon=True,
                    ).start()
                elif kind == "event":
                    if message.get("event") == "run_complete":
                        self._run_complete.set()
                    _handle_local_pickle_stream_event(message)
                    continue
        except OSError:
            pass
        finally:
            with contextlib.suppress(Exception):
                self.process.join(timeout=0.5)
            self.exitcode = self._safe_exitcode()
            self._close_process_handle()
            if not self.departing and self.exitcode not in (0, None):
                self._mark_crashed(f"process exited with code {self.exitcode}")
            self._fail_pending(PagletCrashedError(f"Paglet {self.agent_id!r} process exited"))
            self._closed.set()
            if self.crashed:
                self._crash_handler(self)

    def _close_process_handle(self) -> None:
        return

    def _is_process_alive(self) -> bool:
        if self._process_closed:
            return False
        try:
            return self.process.is_alive()
        except ValueError:
            self._process_closed = True
            return False

    def _safe_exitcode(self) -> int | None:
        if self._process_closed:
            return self.exitcode
        try:
            return self.process.exitcode
        except ValueError:
            self._process_closed = True
            return self.exitcode

    def _complete_reply(self, message: dict[str, Any]) -> None:
        request_id = str(message.get("id") or "")
        with self._pending_lock:
            future = self._pending.pop(request_id, None)
            self._pending_ops.pop(request_id, None)
        if future is None:
            return
        if message.get("ok", False):
            token = _state_stream_token(message.get("payload"))
            payload = _materialize_state_stream(message.get("payload"))
            if token:
                self._send({"type": "event", "event": "local_pickle_stream_received", "token": token})
            future.set_result(payload)
            return
        future.set_exception(_error_from_wire(message.get("error") or {}))

    def _handle_host_call(self, message: dict[str, Any]) -> None:
        request_id = str(message.get("id") or "")
        op = str(message.get("op") or "")
        try:
            raw_payload = dict(message.get("payload") or {})
            token = _state_stream_token(raw_payload)
            request_payload = _materialize_state_stream(raw_payload)
            if token:
                self._send({"type": "event", "event": "local_pickle_stream_received", "token": token})
            payload = self._host_call_handler(op, request_payload)
            if op in {"complete_dispatch", "complete_deactivate", "complete_dispose"}:
                self._has_terminal_message_result = True
                self._terminal_message_result = payload.get("proxy") if isinstance(payload, dict) else None
        except Exception as exc:
            reply = {"type": "reply", "id": request_id, "ok": False, "error": _error_to_wire(exc)}
        else:
            reply = {"type": "reply", "id": request_id, "ok": True, "payload": payload}
        try:
            self._send(reply)
        except Exception:
            self._mark_crashed("could not reply to child host call")

    def _update_from_reply(self, reply: dict[str, Any] | None) -> None:
        if not isinstance(reply, dict):
            return
        if "state" in reply and isinstance(reply["state"], dict):
            self.state = dict(reply["state"])
        if "resources" in reply and isinstance(reply["resources"], dict):
            self.resource_status = {str(key): bool(value) for key, value in reply["resources"].items()}

    def _mark_crashed(self, error: str) -> None:
        self.crashed = True
        self.last_error = error

    def _fail_pending(self, exc: Exception) -> None:
        with self._pending_lock:
            pending = list(self._pending.items())
            pending_ops = {request_id: self._pending_ops.get(request_id, "") for request_id, _ in pending}
            self._pending.clear()
            self._pending_ops.clear()
        for request_id, future in pending:
            if not future.done():
                if self.departing and self._has_terminal_message_result and pending_ops.get(request_id) == "message":
                    future.set_result(
                        {
                            "state": dict(self.state),
                            "resources": dict(self.resource_status),
                            "result": self._terminal_message_result,
                        }
                    )
                else:
                    future.set_exception(exc)


def make_child_config(
    *,
    host_name: str,
    host_address: str,
    agent_id: str,
    agent_class_name: str,
    state_class_name: str,
    state: dict[str, Any],
    host_api_key: str | None = None,
) -> ChildConfig:
    if agent_class_name.startswith("__main__:") or state_class_name.startswith("__main__:"):
        raise HostError("Process-isolated paglets must be importable by module path, not __main__")
    title = f"paglet:{host_name}:{_short_class_name(agent_class_name)}:{agent_id}"
    return ChildConfig(
        host_name=host_name,
        host_address=host_address,
        host_api_key=host_api_key,
        agent_id=agent_id,
        agent_class_name=agent_class_name,
        state_class_name=state_class_name,
        process_title=title,
        state_stream=start_local_pickle_sender(dict(state)),
    )
