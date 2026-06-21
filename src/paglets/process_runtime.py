# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, is_dataclass
import multiprocessing as mp
from multiprocessing.connection import Connection
from pathlib import Path
import os
import queue
import signal
import threading
import time
from typing import Any, Callable
import uuid

from .agent import NOT_HANDLED, Paglet, PagletContext
from .client import HostClient
from .envelope import PagletEnvelope
from .errors import (
    HostError,
    InvalidAgentError,
    LifecycleError,
    NotHandledError,
    PagletCrashedError,
    PagletError,
    PagletInactiveError,
    RemoteHostError,
    ServiceContractError,
    ServiceNotFoundError,
    TransferError,
)
from .events import CloneEvent, CreationEvent, MobilityEvent, PersistencyEvent
from .messages import Message
from .persistency import DeactivationPolicy, DeactivationRequest
from .proxy import PagletProxy
from .references import PagletProxyRef
from .resources import ResourceCleanupError
from .runtime_values import ServiceScope, enum_from_wire
from .serde import dataclass_from_wire, dataclass_to_wire, qualified_name, resolve_qualified_name
from .services import ServiceRecord
from .storage import ManagedStorage, StorageQuotaError, StorageStatus
from .transfer import TransferTicket


_ERROR_TYPES: dict[str, type[Exception]] = {
    "HostError": HostError,
    "InvalidAgentError": InvalidAgentError,
    "LifecycleError": LifecycleError,
    "NotHandledError": NotHandledError,
    "PagletCrashedError": PagletCrashedError,
    "PagletInactiveError": PagletInactiveError,
    "RemoteHostError": RemoteHostError,
    "ResourceCleanupError": LifecycleError,
    "ServiceContractError": ServiceContractError,
    "ServiceNotFoundError": ServiceNotFoundError,
    "StorageQuotaError": StorageQuotaError,
    "TransferError": TransferError,
    "ValueError": ValueError,
}


@dataclass(frozen=True, slots=True)
class ChildConfig:
    host_name: str
    host_address: str
    agent_id: str
    agent_class_name: str
    state_class_name: str
    state: dict[str, Any]
    process_title: str


class ChildProcessController:
    """Parent-side controller for one isolated paglet child process."""

    def __init__(
        self,
        config: ChildConfig,
        *,
        host_call_handler: Callable[[str, dict[str, Any]], Any],
        crash_handler: Callable[["ChildProcessController"], None],
    ):
        self.config = config
        self.agent_id = config.agent_id
        self.agent_class_name = config.agent_class_name
        self.state_class_name = config.state_class_name
        self.state: dict[str, Any] = dict(config.state)
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
        reply = self.request("lifecycle", {"name": name, "event": event})
        self._update_from_reply(reply)
        return dict(reply)

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
            try:
                self.request("shutdown", timeout=timeout)
            except Exception:
                pass
        self._closed.set()
        try:
            self._conn.close()
        except Exception:
            pass

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
                    self._handle_host_call(message)
                elif kind == "event":
                    continue
        except OSError:
            pass
        finally:
            try:
                self.process.join(timeout=0.5)
            except Exception:
                pass
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
            future.set_result(message.get("payload"))
            return
        future.set_exception(_error_from_wire(message.get("error") or {}))

    def _handle_host_call(self, message: dict[str, Any]) -> None:
        request_id = str(message.get("id") or "")
        op = str(message.get("op") or "")
        try:
            payload = self._host_call_handler(op, dict(message.get("payload") or {}))
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
        self._send({"type": "reply", "id": request_id, "ok": True, "payload": payload})

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
            return reply.get("payload")
        raise _error_from_wire(reply.get("error") or {})

    def close(self) -> None:
        self._closed.set()
        try:
            self._conn.close()
        except Exception:
            pass

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
                payload = {"resources": self.agent.resources.status(), "state": dataclass_to_wire(self.agent.state)}
            self.reply_ok(request_id, payload)
        except Exception as exc:
            self.reply_error(request_id, exc)


class _ChildMeshFacade:
    def __init__(self, host: "_ChildHostFacade"):
        self._host = host

    @property
    def code_version(self) -> str:
        return str(self._host._call("mesh_code_version") or "")

    def hosts(self, *, online_only: bool = True, include_self: bool = True):
        from .mesh import HostRef

        payload = self._host._call(
            "available_hosts",
            {"online_only": online_only, "include_self": include_self},
        )
        return [HostRef.from_wire(item) for item in payload.get("hosts", [])]

    def lookup(self, name_or_url: str):
        from .mesh import HostRef

        payload = self._host._call("host_status", {"name_or_url": name_or_url})
        ref = payload.get("host")
        return HostRef.from_wire(ref) if ref is not None else None

    def is_online(self, name_or_url: str) -> bool:
        return bool(self._host._call("is_host_online", {"name_or_url": name_or_url}).get("online"))

    def wait_for_host(self, name_or_url: str, *, timeout: float = 10.0, interval: float = 0.25):
        from .mesh import HostRef

        payload = self._host._call(
            "wait_for_host",
            {"name_or_url": name_or_url, "timeout": timeout, "interval": interval},
        )
        return HostRef.from_wire(payload["host"])

    def resolve_url(self, name_or_url: str) -> str:
        return str(self._host._call("resolve_host_url", {"name_or_url": name_or_url})["url"])


class _ChildHostFacade:
    def __init__(self, endpoint: _ChildEndpoint, config: ChildConfig):
        self._endpoint = endpoint
        self.name = config.host_name
        self.address = config.host_address
        self.agent_id = config.agent_id
        self.client = HostClient()
        self.mesh = _ChildMeshFacade(self)
        self._message_condition = threading.Condition()
        self._agent: Paglet | None = None
        self._terminal = False

    def attach_agent(self, agent: Paglet) -> None:
        self._agent = agent

    @property
    def terminal(self) -> bool:
        return self._terminal

    def _call(self, op: str, payload: dict[str, Any] | None = None) -> Any:
        return self._endpoint.host_call(op, payload or {})

    def get_proxy(self, agent_id: str, host_url: str | None = None) -> PagletProxy | None:
        if host_url is not None and host_url.rstrip("/") != self.address.rstrip("/"):
            return PagletProxy(host_url.rstrip("/"), agent_id, self.client)
        payload = self._call("get_proxy", {"agent_id": agent_id})
        proxy = payload.get("proxy")
        return PagletProxy.from_wire(proxy, self.client) if proxy is not None else None

    def get_proxies(self, state: int = 1) -> list[PagletProxy]:
        payload = self._call("get_proxies", {"state": state})
        return [PagletProxy.from_wire(item, self.client) for item in payload.get("proxies", [])]

    def get_property(self, key: str, default: Any = None) -> Any:
        return self._call("get_property", {"key": key, "default": default}).get("value")

    def set_property(self, key: str, value: Any) -> None:
        self._call("set_property", {"key": key, "value": value})

    def create(
        self,
        agent_cls: type[Paglet],
        state: Any = None,
        *,
        init: Any = None,
        agent_id: str | None = None,
    ) -> PagletProxy:
        state_cls = agent_cls.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        payload = self._call(
            "create_paglet",
            {
                "agent_class_name": qualified_name(agent_cls),
                "state_class_name": qualified_name(state_cls),
                "state": dataclass_to_wire(state),
                "init": init,
                "agent_id": agent_id,
                "host_url": None,
            },
        )
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def create_remote(
        self,
        target: str,
        agent_cls: type[Paglet],
        state: Any = None,
        *,
        init: Any = None,
        agent_id: str | None = None,
    ) -> PagletProxy:
        state_cls = agent_cls.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        payload = self._call(
            "create_paglet",
            {
                "agent_class_name": qualified_name(agent_cls),
                "state_class_name": qualified_name(state_cls),
                "state": dataclass_to_wire(state),
                "init": init,
                "agent_id": agent_id,
                "host_url": target,
            },
        )
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def dispatch(self, agent_id: str, target: str | TransferTicket) -> PagletProxy:
        self._require_self(agent_id)
        preflight = self._call("preflight_transfer", {"target": _target_to_wire(target)})
        target_info = dict(preflight["target_info"])
        ticket = TransferTicket.from_wire(preflight["ticket"])
        agent = self._require_agent()
        agent.on_dispatching(
            MobilityEvent(
                agent_id=agent_id,
                host_name=self.name,
                host_address=self.address,
                source_host_name=self.name,
                source_host_address=self.address,
                target_host_name=target_info["name"],
                target_host_address=target_info["address"],
                reason="dispatch",
            )
        )
        agent.resources.cleanup(reason="dispatch")
        payload = self._call(
            "complete_dispatch",
            {
                "ticket": ticket.to_wire(),
                "target_info": target_info,
                "state": dataclass_to_wire(agent.state),
                "resources": agent.resources.status(),
            },
        )
        self._terminal = True
        self._endpoint.request_exit()
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def clone(self, agent_id: str, *, target: str | TransferTicket | None = None) -> PagletProxy:
        self._require_self(agent_id)
        preflight = self._call("preflight_transfer", {"target": _target_to_wire(target or self.address)})
        target_info = dict(preflight["target_info"])
        ticket = TransferTicket.from_wire(preflight["ticket"])
        clone_id = uuid.uuid4().hex
        agent = self._require_agent()
        event = CloneEvent(
            agent_id=agent_id,
            host_name=self.name,
            host_address=self.address,
            source_agent_id=agent_id,
            clone_agent_id=clone_id,
            source_host_name=self.name,
            source_host_address=self.address,
            target_host_name=target_info["name"],
            target_host_address=target_info["address"],
        )
        agent.on_cloning(event)
        payload = self._call(
            "complete_clone",
            {
                "ticket": ticket.to_wire(),
                "target_info": target_info,
                "clone_id": clone_id,
                "state": dataclass_to_wire(agent.state),
            },
        )
        agent.on_cloned(event)
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def deactivate(self, agent_id: str, request: DeactivationRequest | None = None) -> PagletProxy:
        self._require_self(agent_id)
        agent = self._require_agent()
        request = request or DeactivationRequest()
        policy = agent.deactivation_policy(request)
        if not isinstance(policy, DeactivationPolicy):
            raise HostError(f"{agent.__class__.__name__}.deactivation_policy() must return DeactivationPolicy")
        agent.on_deactivating(
            PersistencyEvent(
                agent_id=agent_id,
                host_name=self.name,
                host_address=self.address,
                reason=request.reason,
                request=request,
                policy=policy,
            )
        )
        agent.resources.cleanup(reason="deactivate")
        payload = self._call(
            "complete_deactivate",
            {
                "request": request.to_wire(),
                "policy": policy.to_wire(),
                "state": dataclass_to_wire(agent.state),
                "resources": agent.resources.status(),
            },
        )
        self._terminal = True
        self._endpoint.request_exit()
        return PagletProxy.from_wire(payload["proxy"], self.client)

    def dispose(self, agent_id: str) -> None:
        self._require_self(agent_id)
        agent = self._require_agent()
        agent.on_disposing(PersistencyEvent(agent_id=agent_id, host_name=self.name, host_address=self.address, reason="dispose"))
        agent.resources.cleanup(reason="dispose")
        self._call(
            "complete_dispose",
            {"state": dataclass_to_wire(agent.state), "resources": agent.resources.status()},
        )
        self._terminal = True
        self._endpoint.request_exit()

    def wait_message(self, agent_id: str, *, timeout: float | None = None) -> bool:
        self._require_self(agent_id)
        with self._message_condition:
            return self._message_condition.wait(timeout)

    def notify_message(self, agent_id: str) -> None:
        self._require_self(agent_id)
        with self._message_condition:
            self._message_condition.notify(1)

    def notify_all_messages(self, agent_id: str) -> None:
        self._require_self(agent_id)
        with self._message_condition:
            self._message_condition.notify_all()

    def resources_for(self, agent_id: str):
        self._require_self(agent_id)
        return self._require_agent().resources

    def work_dir_for(self, agent_id: str, *, create: bool = True) -> Path:
        self._require_self(agent_id)
        payload = self._call("work_dir", {"agent_id": agent_id, "create": create})
        return Path(payload["path"])

    def persistent_storage_for(self, agent_id: str, *, quota_bytes: int | None = None):
        self._require_self(agent_id)
        payload = self._call("persistent_storage", {"agent_id": agent_id, "quota_bytes": quota_bytes})
        return _ChildManagedStorage(self, payload["root"], quota_bytes=payload.get("quota_bytes"))

    def advertise_service(
        self,
        agent_id: str,
        name: str,
        *,
        capabilities: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
        ttl: float | None = None,
    ) -> ServiceRecord:
        payload = self._call(
            "advertise_service",
            {
                "agent_id": agent_id,
                "name": name,
                "capabilities": list(capabilities or []),
                "metadata": metadata or {},
                "scope": scope.value,
                "ttl": ttl,
            },
        )
        return ServiceRecord.from_wire(payload["service"])

    def unadvertise_service(self, name: str, *, agent_id: str | None = None) -> list[ServiceRecord]:
        payload = self._call("unadvertise_service", {"agent_id": agent_id, "name": name})
        return [ServiceRecord.from_wire(item) for item in payload.get("services", [])]

    def lookup_service(
        self,
        name: str,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> ServiceRecord | None:
        payload = self._call(
            "lookup_service",
            {"name": name, "capability": capability, "scope": scope.value},
        )
        service = payload.get("service")
        return ServiceRecord.from_wire(service) if service is not None else None

    def lookup_services(
        self,
        name: str | None = None,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> list[ServiceRecord]:
        payload = self._call(
            "lookup_services",
            {"name": name, "capability": capability, "scope": scope.value},
        )
        return [ServiceRecord.from_wire(item) for item in payload.get("services", [])]

    def lease_service_handle(self, handle, *, ttl: float = 60.0):
        payload = self._call(
            "lease_service_handle",
            {"record": handle.record.to_wire(), "ttl": ttl},
        )
        from .resident import ServiceLease

        return ServiceLease(
            handle=handle,
            lease_id=str(payload["lease_id"]),
            host_url=str(payload["host_url"]),
            expires_at=float(payload["expires_at"]),
            client=self.client,
        )

    def health(self) -> dict[str, Any]:
        return dict(self._call("health"))

    def _require_self(self, agent_id: str) -> None:
        if agent_id != self.agent_id:
            raise InvalidAgentError(f"Child paglet cannot manage different paglet {agent_id!r}")

    def _require_agent(self) -> Paglet:
        if self._agent is None:
            raise InvalidAgentError("paglet child is not attached")
        return self._agent


class _ChildManagedStorage:
    def __init__(self, host: _ChildHostFacade, root: str, *, quota_bytes: int | None):
        self._host = host
        self.root = Path(root)
        self.quota_bytes = quota_bytes

    def read_bytes(self, path: Path | str) -> bytes:
        payload = self._host._call("storage_read_bytes", {"path": str(path), "quota_bytes": self.quota_bytes})
        return bytes(payload["data"])

    def write_bytes(self, path: Path | str, data: bytes) -> Path:
        payload = self._host._call(
            "storage_write_bytes",
            {"path": str(path), "data": bytes(data), "quota_bytes": self.quota_bytes},
        )
        return Path(payload["path"])

    def write_text(self, path: Path | str, text: str, *, encoding: str = "utf-8") -> Path:
        return self.write_bytes(path, text.encode(encoding))

    def delete(self, path: Path | str) -> None:
        self._host._call("storage_delete", {"path": str(path), "quota_bytes": self.quota_bytes})

    def clear(self) -> None:
        self._host._call("storage_clear", {"quota_bytes": self.quota_bytes})

    def status(self) -> StorageStatus:
        payload = self._host._call("storage_status", {"quota_bytes": self.quota_bytes})
        return StorageStatus(
            root=str(payload["root"]),
            used_bytes=int(payload["used_bytes"]),
            quota_bytes=payload.get("quota_bytes"),
            available_bytes=payload.get("available_bytes"),
        )


def make_child_config(
    *,
    host_name: str,
    host_address: str,
    agent_id: str,
    agent_class_name: str,
    state_class_name: str,
    state: dict[str, Any],
) -> ChildConfig:
    if agent_class_name.startswith("__main__:") or state_class_name.startswith("__main__:"):
        raise HostError("Process-isolated paglets must be importable by module path, not __main__")
    title = f"paglet:{host_name}:{_short_class_name(agent_class_name)}:{agent_id}"
    return ChildConfig(
        host_name=host_name,
        host_address=host_address,
        agent_id=agent_id,
        agent_class_name=agent_class_name,
        state_class_name=state_class_name,
        state=dict(state),
        process_title=title,
    )


def _child_main(config: ChildConfig, conn: Connection) -> None:
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except Exception:
        pass
    _set_process_title(config.process_title)
    endpoint = _ChildEndpoint(conn)
    reader = endpoint.start_reader()
    agent_cls = resolve_qualified_name(config.agent_class_name)
    state_cls = resolve_qualified_name(config.state_class_name)
    if not issubclass(agent_cls, Paglet):
        raise HostError(f"{config.agent_class_name} is not a Paglet subclass")
    if not is_dataclass(state_cls):
        raise HostError(f"{config.state_class_name} is not a dataclass state")
    state = dataclass_from_wire(state_cls, config.state)
    agent = agent_cls(state=state, agent_id=config.agent_id)
    facade = _ChildHostFacade(endpoint, config)
    facade.attach_agent(agent)
    endpoint.agent = agent
    endpoint.facade = facade
    agent._attach(PagletContext(facade, agent.agent_id))

    try:
        while True:
            request = endpoint.next_request()
            if request is None:
                break
            request_id = str(request.get("id") or "")
            op = str(request.get("op") or "")
            payload = dict(request.get("payload") or {})
            try:
                result = _handle_child_request(agent, facade, op, payload)
            except Exception as exc:
                endpoint.reply_error(request_id, exc)
            else:
                endpoint.reply_ok(request_id, result)
                if facade.terminal:
                    break
    finally:
        endpoint.close()
        reader.join(timeout=0.2)


def _handle_child_request(agent: Paglet, facade: _ChildHostFacade, op: str, payload: dict[str, Any]) -> dict[str, Any]:
    if op == "lifecycle":
        _run_lifecycle(agent, str(payload["name"]), dict(payload.get("event") or {}))
        return _agent_snapshot(agent)
    if op == "message":
        message = Message.from_wire(payload["message"])
        result = agent.handle_message(message)
        if result is NOT_HANDLED:
            raise NotHandledError(f"{agent.__class__.__name__} did not handle {message.kind!r}")
        snapshot = _agent_snapshot(agent)
        snapshot["result"] = None if payload.get("oneway") else result
        return snapshot
    if op == "cleanup_resources":
        agent.resources.cleanup(reason=str(payload.get("reason") or "lifecycle"))
        return _agent_snapshot(agent)
    if op == "resource_remove":
        agent.resources.remove(str(payload["name"]))
        return _agent_snapshot(agent)
    if op == "deactivate_prepare":
        request = DeactivationRequest.from_wire(payload.get("request"))
        policy = agent.deactivation_policy(request)
        if not isinstance(policy, DeactivationPolicy):
            raise HostError(f"{agent.__class__.__name__}.deactivation_policy() must return DeactivationPolicy")
        agent.on_deactivating(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=facade.name,
                host_address=facade.address,
                reason=request.reason,
                request=request,
                policy=policy,
            )
        )
        agent.resources.cleanup(reason="deactivate")
        snapshot = _agent_snapshot(agent)
        snapshot["policy"] = policy.to_wire()
        return snapshot
    if op == "dispose_prepare":
        agent.on_disposing(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=facade.name,
                host_address=facade.address,
                reason=str(payload.get("reason") or "dispose"),
            )
        )
        agent.resources.cleanup(reason="dispose")
        return _agent_snapshot(agent)
    if op == "shutdown":
        facade._terminal = True
        return _agent_snapshot(agent)
    raise HostError(f"Unknown child operation {op!r}")


def _run_lifecycle(agent: Paglet, name: str, payload: dict[str, Any]) -> None:
    if name == "creation":
        agent.on_creation(
            CreationEvent(
                agent_id=agent.agent_id,
                host_name=str(payload["host_name"]),
                host_address=str(payload["host_address"]),
                init=payload.get("init"),
            )
        )
        agent.run()
        return
    if name == "arrival":
        agent.on_arrival(_mobility_event(agent.agent_id, payload))
        agent.run()
        return
    if name == "clone":
        agent.on_clone(_clone_event(agent.agent_id, payload))
        agent.run()
        return
    if name == "activation":
        request = DeactivationRequest.from_wire(payload.get("request"))
        policy = DeactivationPolicy.from_wire(payload.get("policy"))
        agent.on_activation(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=str(payload["host_name"]),
                host_address=str(payload["host_address"]),
                reason=str(payload.get("reason") or "activate"),
                request=request,
                policy=policy,
            )
        )
        agent.run()
        return
    if name == "dispatching":
        agent.on_dispatching(_mobility_event(agent.agent_id, payload))
        return
    if name == "reverting":
        agent.on_reverting(_mobility_event(agent.agent_id, payload))
        return
    if name == "cloning":
        agent.on_cloning(_clone_event(agent.agent_id, payload))
        return
    if name == "cloned":
        agent.on_cloned(_clone_event(agent.agent_id, payload))
        return
    if name == "deactivating":
        request = DeactivationRequest.from_wire(payload.get("request"))
        policy = DeactivationPolicy.from_wire(payload.get("policy"))
        agent.on_deactivating(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=str(payload["host_name"]),
                host_address=str(payload["host_address"]),
                reason=str(payload.get("reason") or request.reason),
                request=request,
                policy=policy,
            )
        )
        return
    if name == "disposing":
        agent.on_disposing(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=str(payload["host_name"]),
                host_address=str(payload["host_address"]),
                reason=str(payload.get("reason") or "dispose"),
            )
        )
        return
    raise HostError(f"Unknown lifecycle {name!r}")


def _agent_snapshot(agent: Paglet) -> dict[str, Any]:
    with agent.locked_state() as state:
        state_wire = dataclass_to_wire(state)
    return {"state": state_wire, "resources": agent.resources.status()}


def _mobility_event(agent_id: str, payload: dict[str, Any]) -> MobilityEvent:
    return MobilityEvent(
        agent_id=agent_id,
        host_name=str(payload["host_name"]),
        host_address=str(payload["host_address"]),
        source_host_name=str(payload.get("source_host_name") or ""),
        source_host_address=str(payload.get("source_host_address") or ""),
        target_host_name=str(payload.get("target_host_name") or ""),
        target_host_address=str(payload.get("target_host_address") or ""),
        reason=str(payload.get("reason") or "dispatch"),
    )


def _clone_event(agent_id: str, payload: dict[str, Any]) -> CloneEvent:
    return CloneEvent(
        agent_id=agent_id,
        host_name=str(payload["host_name"]),
        host_address=str(payload["host_address"]),
        source_agent_id=str(payload.get("source_agent_id") or ""),
        clone_agent_id=str(payload.get("clone_agent_id") or ""),
        source_host_name=str(payload.get("source_host_name") or ""),
        source_host_address=str(payload.get("source_host_address") or ""),
        target_host_name=str(payload.get("target_host_name") or ""),
        target_host_address=str(payload.get("target_host_address") or ""),
    )


def _target_to_wire(target: str | TransferTicket) -> dict[str, Any]:
    if isinstance(target, TransferTicket):
        return {"ticket": target.to_wire()}
    return {"target": str(target)}


def _set_process_title(title: str) -> None:
    try:
        import setproctitle

        setproctitle.setproctitle(title)
    except Exception:
        pass


def _short_class_name(class_name: str) -> str:
    qualname = class_name.split(":", 1)[-1]
    return qualname.rsplit(".", 1)[-1]


def _error_to_wire(exc: Exception) -> dict[str, str]:
    return {"error_type": exc.__class__.__name__, "error": str(exc)}


def _error_from_wire(payload: dict[str, Any]) -> Exception:
    error_type = str(payload.get("error_type") or "HostError")
    message = str(payload.get("error") or error_type)
    cls = _ERROR_TYPES.get(error_type, HostError)
    try:
        return cls(message)
    except TypeError:
        return HostError(message)
