# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import json
import os
import queue
import shutil
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, is_dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import paglets.tooling.git_update as git_update
from paglets.config.startup import (
    LaunchConfig,
    LaunchConfigSyncResult,
    ResolvedResidentService,
    resolve_resident_service,
    resolve_startup_agent,
)
from paglets.core.agent import ACTIVE, INACTIVE, Paglet, PagletState
from paglets.core.context_events import ContextEvent, ContextEventLog, ContextListener
from paglets.core.errors import (
    HostError,
    InvalidAgentError,
    PagletCrashedError,
    PagletError,
    PagletInactiveError,
    RemoteHostError,
    ServiceNotFoundError,
    TransferError,
)
from paglets.core.events import CloneEvent, CreationEvent, MobilityEvent, PersistencyEvent
from paglets.core.messages import DEACTIVATE, UNQUEUED_PRIORITY, Message, ReplySet
from paglets.core.runtime_values import (
    ArrivalMode,
    EnvelopeKind,
    LaunchConfigSyncAction,
    ResidentLifecycle,
    ServiceScope,
    enum_from_wire,
    require_enum,
)
from paglets.persistence.persistency import DeactivationPolicy, DeactivationRequest, InactiveRecord, QueuedMessage
from paglets.persistence.storage import DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES, ManagedStorage
from paglets.remote.client import HostClient
from paglets.remote.mesh import HostRef, MeshRegistry
from paglets.remote.proxy import PagletProxy
from paglets.remote.references import PagletProxyRef
from paglets.remote.transfer import TransferTicket
from paglets.remote.transport import json_safe
from paglets.runtime.binding import _bind_host_specs, _resolve_bind_hosts, _resolve_public_host
from paglets.runtime.envelope import PagletEnvelope
from paglets.runtime.http_api import PagletHTTPServer as _PagletHTTPServer
from paglets.runtime.http_api import RequestHandler as _RequestHandler
from paglets.runtime.mailbox import MessageMailbox
from paglets.runtime.process_runtime import ChildProcessController, make_child_config
from paglets.runtime.relay import RelayDelivery as _RelayDelivery
from paglets.runtime.relay import RelayMixin, _is_relay_transport_url
from paglets.runtime.relay import RelayNode as _RelayNode
from paglets.serialization.serde import dataclass_from_wire, dataclass_to_wire, qualified_name, resolve_qualified_name
from paglets.services.contracts import ServiceContract, ServiceHandle, ServiceRecord, ServiceRegistry
from paglets.services.resident import (
    DEFAULT_SERVICE_LEASE_TTL_SECONDS,
    RESIDENT_SERVICE_METADATA_KEY,
    ServiceLease,
)

HOST_CAPABILITIES = [
    "agents:list",
    "agents:state",
    "agents:create",
    "agents:message",
    "agents:dispatch",
    "agents:clone",
    "agents:retract",
    "agents:activate",
    "agents:deactivate",
    "agents:dispose",
    "events:list",
    "hosts:list",
    "hosts:join",
    "messages:mailbox",
    "services:list",
    "services:mesh",
    "transfer:tickets",
    "admin:git-update",
]

SHUTDOWN_DEACTIVATE_TIMEOUT_SECONDS = 0.5
AUTO_UPDATE_REQUEST_INTERVAL_SECONDS = 10.0
AUTO_UPDATE_REQUEST_TIMEOUT_SECONDS = 10.0
AUTO_UPDATE_RESTART_DELAY_SECONDS = 0.2
NETWORK_BIND_WATCH_INTERVAL_SECONDS = 5.0
MESH_SERVICE_LOOKUP_TIMEOUT_SECONDS = 1.0
RELAY_OFFLINE_AFTER_SECONDS = 30.0
RELAY_QUEUE_LIMIT = 1024


DEFAULT_PERSISTENCE_ROOT = Path.home() / ".paglets" / "hosts"


@dataclass(slots=True)
class _ManagedResidentService:
    agent_cls: type[Paglet]
    state_class: type[PagletState]
    state_wire: dict[str, Any]
    agent_id: str
    contract: ServiceContract
    scope: ServiceScope
    lifecycle: ResidentLifecycle
    idle_timeout: float
    singleton: bool = True
    init: Any = None
    in_flight: int = 0
    leases: dict[str, float] = field(default_factory=dict)
    last_used: float = field(default_factory=time.time)


class Host(RelayMixin):
    """A paglet host/context served over a small JSON HTTP API.

    One process can run one host. For development, one Python process can also
    start multiple hosts on different ports. Migration always uses the same
    envelope model: class path + dataclass state + lifecycle metadata.
    """

    def __init__(
        self,
        name: str,
        host: str | Sequence[str] = "127.0.0.1",
        port: int = 0,
        *,
        client: HostClient | None = None,
        api_key: str | None = None,
        public_url: str | None = None,
        connect_to: str | None = None,
        mesh: bool = True,
        peers: list[str] | None = None,
        mesh_multicast: bool = True,
        mesh_lan_discovery: bool = True,
        mesh_version: str | None = None,
        mesh_gossip_interval: float = 1.0,
        mesh_offline_after: float = 10.0,
        persistence_dir: str | Path | None = None,
        persistent_storage_quota_bytes: int | None = DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES,
        launch_config: LaunchConfig | None = None,
        launch_config_sync_result: LaunchConfigSyncResult | None = None,
        auto_update_from_git: bool = False,
        git_repo_root: str | Path | None = None,
        git_process_start_head: str | None = None,
        auto_update_restart_callback: Callable[[], None] | None = None,
        auto_update_reporter: Callable[[str], None] | None = None,
        auto_update_restart_delay: float = AUTO_UPDATE_RESTART_DELAY_SECONDS,
        bind_watch_interval: float = NETWORK_BIND_WATCH_INTERVAL_SECONDS,
        relay_offline_after: float = RELAY_OFFLINE_AFTER_SECONDS,
        relay_delivery_timeout: float | None = None,
        relay_queue_limit: int = RELAY_QUEUE_LIMIT,
    ):
        self.name = name
        self.api_key = api_key
        self.public_url = public_url.strip().rstrip("/") if public_url else None
        self.connect_to = connect_to.strip().rstrip("/") if connect_to else None
        self.relay_mode = bool(self.public_url or self.connect_to)
        self._bind_host_specs = _bind_host_specs(host)
        self._auto_bind_enabled = any(value.casefold() == "auto" for value in self._bind_host_specs)
        self._bind_watch_interval = max(0.1, float(bind_watch_interval))
        self._bind_watch_stop = threading.Event()
        self._bind_watch_thread: threading.Thread | None = None
        self._server_lock = threading.RLock()
        self.bind_hosts = _resolve_bind_hosts(self._bind_host_specs)
        self.bind_host = self.bind_hosts[0]
        self.public_host = _resolve_public_host(self.bind_host)
        self.port = int(port)
        self.address = (
            self._connect_relay_url() if self.connect_to else (self.public_url or f"http://{self.public_host}:{port}")
        )
        self.client = client or HostClient(api_key=api_key)
        if api_key and getattr(self.client, "api_key", None) is None:
            self.client.api_key = api_key
        self._agents: dict[str, ChildProcessController] = {}
        self._mailboxes: dict[str, MessageMailbox] = {}
        self.persistence_dir = (
            Path(persistence_dir).expanduser()
            if persistence_dir is not None
            else DEFAULT_PERSISTENCE_ROOT / self._safe_host_name(name)
        )
        self._inactive_dir = self.persistence_dir / "inactive"
        self._work_root = self.persistence_dir / "work"
        self._storage_root = self.persistence_dir / "storage"
        self.persistent_storage_quota_bytes = persistent_storage_quota_bytes
        self._inactive: dict[str, InactiveRecord] = {}
        self._services = ServiceRegistry()
        self._resident_services: dict[str, _ManagedResidentService] = {}
        self._resident_activation_locks: dict[str, threading.Lock] = {}
        self._events = ContextEventLog()
        self._properties: dict[str, Any] = {}
        self.launch_config = launch_config
        self.launch_config_sync_result = launch_config_sync_result
        self._lock = threading.RLock()
        self._server: _PagletHTTPServer | None = None
        self._servers: list[_PagletHTTPServer] = []
        self._thread: threading.Thread | None = None
        self._threads: list[threading.Thread] = []
        self._activation_stop = threading.Event()
        self._activation_thread: threading.Thread | None = None
        self._relay_stop = threading.Event()
        self._relay_client_thread: threading.Thread | None = None
        self._relay_nodes: dict[str, _RelayNode] = {}
        self._relay_queues: dict[str, queue.Queue[_RelayDelivery]] = {}
        self._relay_pending: dict[str, _RelayDelivery] = {}
        self.relay_offline_after = max(0.1, float(relay_offline_after))
        self.relay_delivery_timeout = (
            10.0 if relay_delivery_timeout is None else max(0.01, float(relay_delivery_timeout))
        )
        self.relay_queue_limit = max(1, int(relay_queue_limit))
        self.auto_update_from_git = bool(auto_update_from_git)
        self.git_repo_root = Path(git_repo_root).resolve() if git_repo_root is not None else None
        self.git_process_start_head = git_process_start_head or ""
        self._git_update_status: dict[str, Any] | None = None
        self._auto_update_restart_callback = auto_update_restart_callback
        self._auto_update_reporter = auto_update_reporter
        self._auto_update_restart_delay = max(0.0, float(auto_update_restart_delay))
        self._auto_update_restart_scheduled = False
        self._auto_update_request_times: dict[str, float] = {}
        if self.auto_update_from_git:
            if self.git_repo_root is None:
                self.git_repo_root = git_update.find_repo_root(Path.cwd())
            if not self.git_process_start_head:
                self.git_process_start_head = git_update.current_head(self.git_repo_root)
        self.mesh = MeshRegistry(
            self,
            enabled=mesh,
            peers=peers,
            code_version=mesh_version,
            multicast=mesh_multicast,
            lan_discovery=mesh_lan_discovery,
            gossip_interval=mesh_gossip_interval,
            offline_after=mesh_offline_after,
        )
        self._load_inactive_records()

    def start_background(self) -> None:
        if self.connect_to:
            self._start_connect_background()
            return
        with self._server_lock:
            if self._server is not None:
                return
            self._clear_work_root()
            servers = self._open_http_servers(self.bind_hosts, self.port)
            self._install_http_servers(servers, self.bind_hosts)
        self._activation_stop.clear()
        self._emit_launch_config_sync_result()
        self._activate_startup_records()
        self._start_resident_services()
        self._start_launch_agents()
        self._start_activation_scheduler()
        self.mesh.start()
        self._start_bind_watcher()
        self._emit("context-start")

    def serve_forever(self) -> None:
        self.start_background()
        try:
            while True:
                with self._server_lock:
                    if self._server is None:
                        return
                    threads = list(self._threads)
                if not threads:
                    return
                for thread in threads:
                    thread.join(timeout=0.5)
        except KeyboardInterrupt:  # pragma: no cover - CLI convenience
            self.shutdown()

    def shutdown(self) -> None:
        self.stop(deactivate_active=True)

    def stop(self, *, deactivate_active: bool = False) -> None:
        self._stop_bind_watcher()
        with self._server_lock:
            server = self._server
            if server is None and self._relay_client_thread is None:
                return
            servers = list(self._servers or ([server] if server is not None else []))
            threads = list(self._threads)
            if not threads and self._thread is not None:
                threads = [self._thread]
        self._stop_relay_client()
        self._stop_activation_scheduler()
        if deactivate_active:
            self._deactivate_active_for_shutdown()
        self._terminate_active_children()
        self.mesh.stop()
        self._emit("context-shutdown")
        with self._server_lock:
            self._clear_http_servers()
        if servers:
            self._shutdown_http_servers(servers, threads)

    def _start_connect_background(self) -> None:
        with self._server_lock:
            if self._relay_client_thread is not None:
                return
            self._clear_work_root()
        self._activation_stop.clear()
        self._emit_launch_config_sync_result()
        self._activate_startup_records()
        self._start_resident_services()
        self._start_launch_agents()
        self._start_activation_scheduler()
        self.mesh.refresh_self()
        self._start_relay_client()
        self._emit("context-start")

    def _open_http_servers(self, bind_hosts: list[str], port: int) -> list[_PagletHTTPServer]:
        servers: list[_PagletHTTPServer] = []
        try:
            for index, bind_host in enumerate(bind_hosts):
                bind_port = port if index == 0 or port != 0 else int(servers[0].server_address[1])
                servers.append(_PagletHTTPServer((bind_host, bind_port), _RequestHandler, self))
        except Exception:
            for server in servers:
                server.server_close()
            raise
        return servers

    def _install_http_servers(self, servers: list[_PagletHTTPServer], bind_hosts: list[str]) -> None:
        _actual_host, actual_port = servers[0].server_address[:2]
        self.bind_hosts = list(bind_hosts)
        self.bind_host = self.bind_hosts[0]
        self.public_host = _resolve_public_host(self.bind_host)
        self.port = int(actual_port)
        self.address = self.public_url or f"http://{self.public_host}:{actual_port}"
        self._servers = servers
        self._server = servers[0]
        self._threads = [
            threading.Thread(target=server.serve_forever, name=f"paglets-{self.name}-{index}", daemon=True)
            for index, server in enumerate(servers)
        ]
        self._thread = self._threads[0]
        for thread in self._threads:
            thread.start()

    def _clear_http_servers(self) -> None:
        self._servers = []
        self._server = None
        self._threads = []
        self._thread = None

    def _shutdown_http_servers(
        self,
        servers: list[_PagletHTTPServer],
        threads: list[threading.Thread],
    ) -> None:
        for running_server in servers:
            running_server.shutdown()
        for running_server in servers:
            running_server.server_close()
        current_thread = threading.current_thread()
        for thread in threads:
            if thread is not current_thread and thread.is_alive():
                thread.join(timeout=2)

    def _start_bind_watcher(self) -> None:
        if not self._auto_bind_enabled:
            return
        if self._bind_watch_thread is not None and self._bind_watch_thread.is_alive():
            return
        self._bind_watch_stop.clear()
        self._bind_watch_thread = threading.Thread(
            target=self._bind_watch_loop,
            name=f"paglets-bind-watch-{self.name}",
            daemon=True,
        )
        self._bind_watch_thread.start()

    def _stop_bind_watcher(self) -> None:
        self._bind_watch_stop.set()
        thread = self._bind_watch_thread
        self._bind_watch_thread = None
        if thread is not None and thread is not threading.current_thread() and thread.is_alive():
            thread.join(timeout=2)

    def _bind_watch_loop(self) -> None:
        while not self._bind_watch_stop.wait(self._bind_watch_interval):
            try:
                self._check_auto_bind_change()
            except Exception as exc:  # pragma: no cover - defensive background boundary
                self.mesh._debug(f"auto bind refresh failed: {exc}")

    def _check_auto_bind_change(self) -> bool:
        if not self._auto_bind_enabled:
            return False
        with self._server_lock:
            if self._server is None:
                return False
            current_bind_hosts = list(self.bind_hosts)
        next_bind_hosts = _resolve_bind_hosts(self._bind_host_specs)
        if next_bind_hosts == current_bind_hosts:
            return False
        return self._rebind_http_servers(next_bind_hosts)

    def _rebind_http_servers(self, next_bind_hosts: list[str]) -> bool:
        with self._server_lock:
            server = self._server
            if server is None:
                return False
            current_bind_hosts = list(self.bind_hosts)
            if next_bind_hosts == current_bind_hosts:
                return False
            old_address = self.address
            old_port = self.port
            old_servers = list(self._servers or [server])
            old_threads = list(self._threads)
            if not old_threads and self._thread is not None:
                old_threads = [self._thread]
            try:
                self._shutdown_http_servers(old_servers, old_threads)
                new_servers = self._open_http_servers(next_bind_hosts, old_port)
            except Exception as exc:
                try:
                    restored_servers = self._open_http_servers(current_bind_hosts, old_port)
                except Exception as restore_exc:
                    self._clear_http_servers()
                    self.mesh._debug(f"auto bind refresh failed and restore failed: {exc}; restore: {restore_exc}")
                    raise
                self._install_http_servers(restored_servers, current_bind_hosts)
                self.mesh._debug(f"auto bind refresh failed; restored previous bind hosts: {exc}")
                return False
            self._install_http_servers(new_servers, next_bind_hosts)
            new_address = self.address
        self.mesh.local_address_changed(old_address)
        self._emit(
            "context-rebind",
            data={
                "old_address": old_address,
                "new_address": new_address,
                "bind_hosts": list(next_bind_hosts),
            },
        )
        return True

    # ------------------------------------------------------------------
    # Local management API
    # ------------------------------------------------------------------
    def create(
        self,
        agent_cls: type[Paglet],
        state: PagletState | None = None,
        *,
        init: Any = None,
        agent_id: str | None = None,
    ) -> PagletProxy:
        state_cls = agent_cls.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        if not is_dataclass(state):
            raise HostError(f"{agent_cls.__name__}.State must be a dataclass state object")
        child_id = agent_id or uuid.uuid4().hex
        agent_class_name = qualified_name(agent_cls)
        state_class_name = qualified_name(state_cls)
        record = self._start_child(
            agent_id=child_id,
            agent_class_name=agent_class_name,
            state_class_name=state_class_name,
            state=dataclass_to_wire(state),
        )
        event = CreationEvent(
            agent_id=record.agent_id,
            host_name=self.name,
            host_address=self.address,
            init=init,
        )
        try:
            record.request_lifecycle("creation", dataclass_to_wire(event))
        except Exception:
            self._remove_active_agent(record.agent_id, record, terminate=True)
            raise
        record.ready = True
        self._emit("create", agent_id=record.agent_id, class_name=record.agent_class_name)
        return self._current_or_last_proxy(record)

    def get_proxy(self, agent_id: str) -> PagletProxy | None:
        with self._lock:
            record = self._agents.get(agent_id)
            if record is None or not record.ready or record.crashed:
                return None
        return PagletProxy(self.address, agent_id, self.client)

    def get_proxies(self, state: int = ACTIVE) -> list[PagletProxy]:
        proxies: list[PagletProxy] = []
        with self._lock:
            if state & ACTIVE:
                proxies.extend(
                    PagletProxy(self.address, agent_id, self.client)
                    for agent_id, record in self._agents.items()
                    if record.ready and not record.crashed
                )
            if state & INACTIVE:
                proxies.extend(PagletProxy(self.address, agent_id, self.client) for agent_id in self._inactive)
        return proxies

    def get_property(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._properties.get(key, default)

    def set_property(self, key: str, value: Any) -> None:
        with self._lock:
            if value is None:
                self._properties.pop(key, None)
            else:
                self._properties[key] = value

    def get_state(self, agent_id: str, state_cls: type[PagletState]) -> PagletState:
        record = self._require_agent(agent_id)
        state_payload = record.fetch_state()
        state = dataclass_from_wire(state_cls, state_payload)
        if not isinstance(state, state_cls):
            raise HostError(f"Paglet {agent_id!r} state is not {state_cls!r}")
        return state

    def resources_for(self, agent_id: str):
        return _RemoteResourceRegistry(self, agent_id)

    def work_dir_for(self, agent_id: str, *, create: bool = True) -> Path:
        self._require_agent(agent_id)
        path = self._work_path(agent_id)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def persistent_storage_for(self, agent_id: str, *, quota_bytes: int | None = None) -> ManagedStorage:
        record = self._require_agent(agent_id)
        quota = self.persistent_storage_quota_bytes if quota_bytes is None else quota_bytes
        return ManagedStorage(
            self._storage_root / self._storage_class_key(record.agent_class_name),
            quota_bytes=quota,
        )

    def list_agents(self, *, active: bool = True, inactive: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            agents = [self._summary(agent) for agent in self._agents.values()] if active else []
            if inactive:
                agents.extend(self._inactive_summary(record) for record in self._inactive.values())
            return agents

    def health(self) -> dict[str, Any]:
        with self._lock:
            active_count = sum(1 for record in self._agents.values() if record.ready and not record.crashed)
            inactive_count = len(self._inactive)
        capabilities = list(HOST_CAPABILITIES)
        if self.relay_mode and "admin:git-update" in capabilities:
            capabilities.remove("admin:git-update")
        if not self.connect_to:
            capabilities.extend(["relay:connect", "relay:poll"])
        payload = {
            "name": self.name,
            "address": self.address,
            "active_count": active_count,
            "inactive_count": inactive_count,
            "code_version": self.mesh.code_version,
            "capabilities": capabilities,
        }
        if self._relay_nodes:
            payload["relay_nodes"] = self.relay_diagnostics()["nodes"]
        payload.update(self._git_update_health())
        return payload

    def list_hosts(self, *, online_only: bool = False, include_self: bool = True) -> list[HostRef]:
        return self.mesh.hosts(online_only=online_only, include_self=include_self)

    def join_mesh(self, payload: dict[str, Any]) -> list[HostRef]:
        self.mesh.register_wire(payload)
        return self.mesh.hosts(include_self=True)

    def handle_git_update_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.auto_update_from_git or self.git_repo_root is None:
            status = {
                "ok": False,
                "status": "disabled",
                "error": "git auto-update is disabled for this host",
                "target_hash": str(payload.get("target_hash") or ""),
            }
            self._store_git_update_status(status)
            return status

        target_hash = str(payload.get("target_hash") or "").strip()
        source_name = str(payload.get("source_name") or "")
        source_url = str(payload.get("source_url") or "")
        result = git_update.update_checkout(
            self.git_repo_root,
            process_start_head=self.git_process_start_head,
            target_hash=target_hash,
            sync_dependencies=os.name != "nt",
        )
        status = result.to_wire()
        status.update(
            {
                "source_name": source_name,
                "source_url": source_url,
                "restart_scheduled": False,
            }
        )
        self._store_git_update_status(status)
        if result.restart_required:
            status["restart_scheduled"] = self._schedule_auto_update_restart()
            self._store_git_update_status(status)
        return status

    def broadcast_git_update(
        self,
        targets: list[str] | None = None,
        *,
        validate_targets: bool = False,
        report_unreachable: bool = True,
    ) -> list[dict[str, Any]]:
        if not self.auto_update_from_git:
            return []
        urls = set(targets or [])
        urls.update(self.mesh.peer_urls(include_known=True))
        responses: list[dict[str, Any]] = []
        for url in sorted(urls):
            response = self.request_peer_git_update(
                url,
                validate_health=validate_targets,
                report_unreachable=report_unreachable,
            )
            if response is not None:
                responses.append(response)
        return responses

    def request_peer_git_update(
        self,
        url: str,
        *,
        target_hash: str | None = None,
        health: dict[str, Any] | None = None,
        throttle: bool = True,
        validate_health: bool = False,
        report_unreachable: bool = True,
    ) -> dict[str, Any] | None:
        if not self.auto_update_from_git:
            return None
        normalized = url.rstrip("/")
        if validate_health and health is None:
            try:
                probed = self.client.get_json(
                    f"{normalized}/health",
                    timeout=AUTO_UPDATE_REQUEST_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                if report_unreachable:
                    failure = {"ok": False, "status": "unreachable", "error": str(exc), "url": normalized}
                    self._report_git_update_failure(normalized, failure)
                    return failure
                return None
            if not isinstance(probed, dict):
                failure = {
                    "ok": False,
                    "status": "invalid-health",
                    "error": f"unexpected health {probed!r}",
                    "url": normalized,
                }
                self._report_git_update_failure(normalized, failure)
                return failure
            health = probed
        if health is not None and health.get("auto_update_from_git") is False:
            return None
        try:
            normalized = HostRef.from_wire(
                {
                    "name": health.get("name", url) if health else url,
                    "url": health.get("address", url) if health else url,
                    "code_version": health.get("code_version", self.mesh.code_version)
                    if health
                    else self.mesh.code_version,
                    "online": True,
                    "last_seen": time.time(),
                    "active_count": health.get("active_count", 0) if health else 0,
                    "inactive_count": health.get("inactive_count", 0) if health else 0,
                }
            ).url
        except Exception:
            normalized = url.rstrip("/")
        if normalized.rstrip("/") == self.address.rstrip("/"):
            return None
        if throttle and not self._reserve_git_update_request(normalized):
            return None

        target = (target_hash or self._current_git_head()).strip()
        if not target:
            return None
        try:
            response = self.client.post_json(
                f"{normalized.rstrip('/')}/admin/git-update",
                {
                    "target_hash": target,
                    "source_name": self.name,
                    "source_url": self.address,
                },
                timeout=AUTO_UPDATE_REQUEST_TIMEOUT_SECONDS,
            )
            if isinstance(response, dict):
                response.setdefault("url", normalized)
                if not response.get("ok"):
                    self._report_git_update_failure(normalized, response)
                return response
            failure = {
                "ok": False,
                "status": "invalid-response",
                "error": f"unexpected response {response!r}",
                "url": normalized,
            }
            self._report_git_update_failure(normalized, failure)
            return failure
        except Exception as exc:
            failure = {
                "ok": False,
                "status": "request-failed",
                "error": str(exc),
                "target_hash": target,
                "url": normalized,
            }
            self._report_git_update_failure(normalized, failure)
            return failure

    def _git_update_health(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "auto_update_from_git": self.auto_update_from_git,
            "auto_update_restart_scheduled": self._auto_update_restart_scheduled,
        }
        if self.git_repo_root is not None:
            payload["git_repo_root"] = str(self.git_repo_root)
            payload["git_head"] = self._current_git_head()
            payload["git_process_start_head"] = self.git_process_start_head
        status = self._git_update_status
        if status is not None:
            payload["git_update"] = dict(status)
        return payload

    def _current_git_head(self) -> str:
        if self.git_repo_root is None:
            return ""
        try:
            return git_update.current_head(self.git_repo_root)
        except git_update.GitUpdateError:
            return self.git_process_start_head

    def _store_git_update_status(self, status: dict[str, Any]) -> None:
        with self._lock:
            self._git_update_status = dict(status)

    def _reserve_git_update_request(self, url: str) -> bool:
        now = time.monotonic()
        with self._lock:
            last = self._auto_update_request_times.get(url, 0.0)
            if now - last < AUTO_UPDATE_REQUEST_INTERVAL_SECONDS:
                return False
            self._auto_update_request_times[url] = now
            return True

    def _schedule_auto_update_restart(self) -> bool:
        if self._auto_update_restart_callback is None:
            self._report_auto_update("restart required, but no restart callback is configured")
            return False
        with self._lock:
            if self._auto_update_restart_scheduled:
                return True
            self._auto_update_restart_scheduled = True
        thread = threading.Thread(
            target=self._run_auto_update_restart,
            name=f"paglets-auto-update-restart-{self.name}",
            daemon=True,
        )
        thread.start()
        return True

    def _run_auto_update_restart(self) -> None:
        time.sleep(self._auto_update_restart_delay)
        callback = self._auto_update_restart_callback
        try:
            self._report_auto_update("restart scheduled; shutting down host for re-exec")
            self.shutdown()
        finally:
            if callback is not None:
                callback()

    def _report_git_update_failure(self, url: str, response: dict[str, Any]) -> None:
        status = str(response.get("status") or "failed")
        target = str(response.get("target_hash") or "")
        error = str(response.get("error") or "")
        pieces = [f"{url}: git auto-update {status}"]
        if target:
            pieces.append(f"target {target}")
        if error:
            pieces.append(error)
        if status == "target-missing":
            pieces.append(
                "The commit may not have been pushed yet; run git push and restart this host to broadcast again."
            )
        stderr = _trim_git_output(str(response.get("stderr") or ""))
        stdout = _trim_git_output(str(response.get("stdout") or ""))
        if stderr:
            pieces.append(f"stderr: {stderr}")
        if stdout:
            pieces.append(f"stdout: {stdout}")
        self._report_auto_update("; ".join(pieces))

    def _report_auto_update(self, message: str) -> None:
        reporter = self._auto_update_reporter
        if reporter is not None:
            reporter(message)

    def add_listener(self, listener: ContextListener) -> None:
        self._events.add_listener(listener)

    def remove_listener(self, listener: ContextListener) -> None:
        self._events.remove_listener(listener)

    def list_events(self, *, since: int = 0, limit: int = 100) -> list[ContextEvent]:
        return self._events.events_since(since, limit=limit)

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
        require_enum(scope, ServiceScope, "scope")
        self._require_agent(agent_id)
        existing = self._services.record(name, agent_id)
        merged_metadata = dict(metadata or {})
        if existing is not None and RESIDENT_SERVICE_METADATA_KEY in existing.metadata:
            merged_metadata[RESIDENT_SERVICE_METADATA_KEY] = existing.metadata[RESIDENT_SERVICE_METADATA_KEY]
        record = self._services.advertise(
            host_name=self.name,
            host_url=self.address,
            name=name,
            proxy=PagletProxyRef(self.address, agent_id),
            capabilities=capabilities,
            metadata=merged_metadata,
            scope=scope,
            ttl=ttl,
        )
        self._emit("service-advertise", agent_id=agent_id, service_name=name, data=record.to_wire())
        return record

    def unadvertise_service(self, name: str, *, agent_id: str | None = None) -> list[ServiceRecord]:
        removed = self._services.unadvertise(name, agent_id=agent_id)
        for record in removed:
            self._emit("service-remove", agent_id=record.proxy.agent_id, service_name=record.name)
        return removed

    def lookup_service(
        self,
        name: str,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> ServiceRecord | None:
        require_enum(scope, ServiceScope, "scope")
        matches = self.lookup_services(name, capability=capability, scope=scope)
        return matches[0] if matches else None

    def lookup_services(
        self,
        name: str | None = None,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> list[ServiceRecord]:
        require_enum(scope, ServiceScope, "scope")
        records = self._services.lookup_all(name, capability)
        if scope is ServiceScope.MESH:
            records.extend(self._lookup_mesh_services(name=name, capability=capability))
        return records

    def _lookup_mesh_services(self, *, name: str | None = None, capability: str | None = None) -> list[ServiceRecord]:
        records: list[ServiceRecord] = []
        for host_ref in self.mesh.hosts(online_only=True, include_self=False):
            query: dict[str, str] = {}
            if name is not None:
                query["name"] = name
            if capability is not None:
                query["capability"] = capability
            suffix = f"?{urlencode(query)}" if query else ""
            try:
                separator = "&" if suffix else "?"
                payload = self.client.get_json(
                    f"{host_ref.url.rstrip('/')}/services{suffix}{separator}scope=mesh",
                    timeout=MESH_SERVICE_LOOKUP_TIMEOUT_SECONDS,
                )
            except PagletError:
                continue
            for item in payload.get("services", []):
                if isinstance(item, dict):
                    records.append(ServiceRecord.from_wire(item))
        return records

    def create_remote(
        self,
        target: str,
        agent_cls: type[Paglet],
        state: PagletState | None = None,
        *,
        init: Any = None,
        agent_id: str | None = None,
    ) -> PagletProxy:
        state_cls = agent_cls.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        response = self.client.post_pickle(
            f"{target.rstrip('/')}/agents",
            {
                "agent_class_name": qualified_name(agent_cls),
                "state_class_name": qualified_name(state_cls),
                "state": dataclass_to_wire(state),
                "init": init,
                "agent_id": agent_id,
            },
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def dispatch(self, agent_id: str, target: str | TransferTicket) -> PagletProxy:
        ticket = self._prepare_ticket(target)
        record = self._require_agent(agent_id)
        target_info = self._preflight_transfer(ticket)
        event = MobilityEvent(
            agent_id=agent_id,
            host_name=self.name,
            host_address=self.address,
            source_host_name=self.name,
            source_host_address=self.address,
            target_host_name=target_info["name"],
            target_host_address=target_info["address"],
            reason="dispatch",
        )
        record.request_lifecycle("dispatching", dataclass_to_wire(event))
        record.cleanup_resources(reason="dispatch")
        self._cleanup_agent_work_dir(agent_id)
        envelope = self._make_envelope(record, EnvelopeKind.DISPATCH, target_info, ticket=ticket)
        response = self._post_envelope_with_ticket(ticket, target_info, envelope)
        self._remove_active_agent(agent_id, record, terminate=True)
        self._emit("dispatch", agent_id=agent_id, class_name=record.agent_class_name, data={"target": target_info})
        return PagletProxy.from_wire(response["proxy"], self.client)

    def clone(self, agent_id: str, *, target: str | TransferTicket | None = None) -> PagletProxy:
        record = self._require_agent(agent_id)
        ticket = self._prepare_ticket(target or self.address)
        target_info = self._preflight_transfer(ticket)
        clone_id = uuid.uuid4().hex
        cloning_event = CloneEvent(
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
        record.request_lifecycle("cloning", dataclass_to_wire(cloning_event))
        envelope = self._make_envelope(
            record,
            EnvelopeKind.CLONE,
            target_info,
            agent_id=clone_id,
            clone_of=agent_id,
            ticket=ticket,
        )
        response = self._post_envelope_with_ticket(ticket, target_info, envelope)
        cloned_event = CloneEvent(
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
        record.request_lifecycle("cloned", dataclass_to_wire(cloned_event))
        self._emit(
            "clone",
            agent_id=agent_id,
            class_name=record.agent_class_name,
            data={"clone_agent_id": clone_id, "target": target_info},
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def retract(self, remote_host_url: str, agent_id: str) -> PagletProxy:
        response = self.client.post_json(
            f"{remote_host_url.rstrip('/')}/agents/{agent_id}/retract",
            {"target": self.address},
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def deactivate(
        self,
        agent_id: str,
        request: DeactivationRequest | None = None,
    ) -> PagletProxy:
        record = self._require_agent(agent_id)
        request = request or DeactivationRequest()
        prepared = record.request("deactivate_prepare", {"request": request.to_wire()})
        record._update_from_reply(prepared)
        policy = DeactivationPolicy.from_wire(prepared.get("policy"))
        info = {"name": self.name, "address": self.address}
        envelope = self._make_envelope(record, EnvelopeKind.ACTIVATION, info)
        record = InactiveRecord(envelope=envelope, policy=policy, request=request)
        self._write_inactive_record(record)
        with self._lock:
            self._inactive[agent_id] = record
        self._remove_active_agent(agent_id, expected=None, terminate=True)
        self._emit(
            "deactivate", agent_id=agent_id, class_name=envelope.agent_class_name, data={"reason": request.reason}
        )
        return PagletProxy(self.address, agent_id, self.client)

    def activate(self, agent_id: str) -> PagletProxy:
        with self._lock:
            record = self._inactive.pop(agent_id, None)
        if record is None:
            raise InvalidAgentError(f"No deactivated paglet {agent_id!r} on {self.name}")
        self._delete_inactive_record(agent_id)
        try:
            proxy = self._receive_envelope(record.envelope, inactive_record=record)
        except Exception:
            self._remove_active_agent(agent_id)
            self._write_inactive_record(record)
            with self._lock:
                self._inactive[agent_id] = record
            raise
        self._drain_queued_messages(record)
        self._emit("activate", agent_id=agent_id, data={"queued_message_count": len(record.queued_messages)})
        return proxy

    def dispose(self, agent_id: str) -> None:
        with self._lock:
            record = self._agents.get(agent_id)
            inactive = self._inactive.pop(agent_id, None)
        if record is None:
            if inactive is None:
                raise InvalidAgentError(f"No paglet {agent_id!r} on {self.name}")
            self._delete_inactive_record(agent_id)
            self._cleanup_agent_work_dir(agent_id)
            self._emit(
                "dispose", agent_id=agent_id, class_name=inactive.envelope.agent_class_name, data={"active": False}
            )
            return
        record.request("dispose_prepare", {"reason": "dispose"})
        self._cleanup_agent_work_dir(agent_id)
        self._remove_active_agent(agent_id, record, terminate=True)
        if inactive is not None:
            self._delete_inactive_record(agent_id)
        self._emit("dispose", agent_id=agent_id, class_name=record.agent_class_name, data={"active": True})

    # ------------------------------------------------------------------
    # Message/lifecycle internals
    # ------------------------------------------------------------------
    def deliver_message(
        self,
        agent_id: str,
        message: Message,
        *,
        oneway: bool = False,
        activate_if_inactive: bool = True,
        no_delay: bool = False,
    ) -> Any:
        if message.kind == DEACTIVATE:
            proxy = self.deactivate(
                agent_id,
                DeactivationRequest.from_wire(message.args.get("request")),
            )
            return None if oneway else {"deactivated": True, "proxy": proxy.to_wire()}
        with self._lock:
            agent = self._agents.get(agent_id)
            inactive = self._inactive.get(agent_id)
            is_resident_service = agent_id in self._resident_services
        if agent is None:
            if is_resident_service and activate_if_inactive:
                self._ensure_resident_service_active(agent_id)
            else:
                if inactive is None:
                    raise InvalidAgentError(f"No active paglet {agent_id!r} on {self.name}")
                if activate_if_inactive and inactive.policy.activate_on_message:
                    self.activate(agent_id)
                elif no_delay or not inactive.policy.queue_messages_when_inactive:
                    raise PagletInactiveError(f"Paglet {agent_id!r} is inactive on {self.name}")
                else:
                    inactive.queued_messages.append(QueuedMessage(message=message, oneway=oneway))
                    self._write_inactive_record(inactive)
                    self._emit("message-queued", agent_id=agent_id, message_id=message.message_id)
                    return None if oneway else {"queued": True, "message_id": message.message_id}
        with self._lock:
            mailbox = self._mailboxes.get(agent_id)
        if mailbox is None:
            raise InvalidAgentError(f"No active paglet {agent_id!r} on {self.name}")
        if message.priority == UNQUEUED_PRIORITY:
            future = mailbox.submit_unqueued(message, oneway=oneway)
        else:
            future = mailbox.submit(message, oneway=oneway)
            self._emit("message-queued", agent_id=agent_id, message_id=message.message_id)
        return None if oneway else future.result()

    def _deliver_active_message(self, agent_id: str, message: Message, *, oneway: bool = False) -> Any:
        with self._lock:
            record = self._agents.get(agent_id)
        if record is None:
            error = InvalidAgentError(f"No active paglet {agent_id!r} on {self.name}")
            self._emit("message-failed", agent_id=agent_id, message_id=message.message_id, error=str(error))
            raise error
        self._begin_resident_service_call(agent_id)
        try:
            try:
                result = record.request_message(message, oneway=oneway)
            except Exception as exc:
                self._emit("message-failed", agent_id=agent_id, message_id=message.message_id, error=str(exc))
                raise
            self._emit("message-delivered", agent_id=agent_id, message_id=message.message_id)
            return None if oneway else result
        finally:
            self._end_resident_service_call(agent_id)

    def multicast_message(
        self,
        kind: str | Message,
        args: dict[str, Any] | None = None,
        *,
        exclude: set[str] | None = None,
    ) -> ReplySet:
        exclude = exclude or set()
        reply_set = ReplySet()
        for proxy in self.get_proxies(ACTIVE):
            if proxy.agent_id in exclude:
                continue
            message = (
                Message.from_wire(kind.to_wire())
                if isinstance(kind, Message)
                else Message(kind=kind, args=args or {}, sender=self.address)
            )
            if message.sender is None:
                message.sender = self.address
            reply_set.add_future_reply(proxy.send_future(message))
        return reply_set

    def wait_message(self, agent_id: str, *, timeout: float | None = None) -> bool:
        return self._require_mailbox(agent_id).wait_message(timeout)

    def notify_message(self, agent_id: str) -> None:
        self._require_mailbox(agent_id).notify_message()

    def notify_all_messages(self, agent_id: str) -> None:
        self._require_mailbox(agent_id).notify_all_messages()

    def mailbox_status(self, agent_id: str) -> dict[str, int]:
        return self._require_mailbox(agent_id).status().to_wire()

    def _require_mailbox(self, agent_id: str) -> MessageMailbox:
        with self._lock:
            mailbox = self._mailboxes.get(agent_id)
        if mailbox is None:
            raise InvalidAgentError(f"No active paglet {agent_id!r} on {self.name}")
        return mailbox

    def _retract_to(self, agent_id: str, target: str) -> PagletProxy:
        record = self._require_agent(agent_id)
        target_info = self._host_info(target)
        event = MobilityEvent(
            agent_id=agent_id,
            host_name=self.name,
            host_address=self.address,
            source_host_name=self.name,
            source_host_address=self.address,
            target_host_name=target_info["name"],
            target_host_address=target_info["address"],
            reason="retract",
        )
        record.request_lifecycle("reverting", dataclass_to_wire(event))
        record.cleanup_resources(reason="retract")
        self._cleanup_agent_work_dir(agent_id)
        envelope = self._make_envelope(record, EnvelopeKind.RETRACT, target_info)
        if self._is_local_transfer_target(target_info):
            response = self._receive_local_envelope_response(envelope)
        else:
            response = self.client.post_pickle(
                f"{target_info['address'].rstrip('/')}/agents", {"envelope": envelope.to_wire()}
            )
        self._remove_active_agent(agent_id, record, terminate=True)
        self._emit("retract", agent_id=agent_id, class_name=record.agent_class_name, data={"target": target_info})
        return PagletProxy.from_wire(response["proxy"], self.client)

    def _receive_envelope(
        self,
        envelope: PagletEnvelope,
        *,
        inactive_record: InactiveRecord | None = None,
    ) -> PagletProxy:
        if inactive_record is None and self._arrival_mode(envelope) is ArrivalMode.INACTIVE:
            record = self._inactive_arrival_record(envelope)
            self._write_inactive_record(record)
            with self._lock:
                self._inactive[record.agent_id] = record
            self._emit(
                "arrival",
                agent_id=record.agent_id,
                class_name=record.envelope.agent_class_name,
                data={"active": False, "kind": envelope.kind.value},
            )
            return PagletProxy(self.address, record.agent_id, self.client)

        self._validate_agent_classes(envelope.agent_class_name, envelope.state_class_name)
        record = self._start_child(
            agent_id=envelope.agent_id,
            agent_class_name=envelope.agent_class_name,
            state_class_name=envelope.state_class_name,
            state=envelope.state,
        )

        if envelope.kind in (EnvelopeKind.DISPATCH, EnvelopeKind.RETRACT):
            event = MobilityEvent(
                agent_id=record.agent_id,
                host_name=self.name,
                host_address=self.address,
                source_host_name=envelope.source_host_name,
                source_host_address=envelope.source_host_address,
                target_host_name=self.name,
                target_host_address=self.address,
                reason=envelope.kind.value,
            )
            record.request_lifecycle("arrival", dataclass_to_wire(event))
            record.ready = True
            self._emit(
                "arrival",
                agent_id=record.agent_id,
                class_name=record.agent_class_name,
                data={"kind": envelope.kind.value},
            )
            record.wait_for_run_complete_or_departure()
        elif envelope.kind is EnvelopeKind.CLONE:
            event = CloneEvent(
                agent_id=record.agent_id,
                host_name=self.name,
                host_address=self.address,
                source_agent_id=envelope.clone_of or "",
                clone_agent_id=record.agent_id,
                source_host_name=envelope.source_host_name,
                source_host_address=envelope.source_host_address,
                target_host_name=self.name,
                target_host_address=self.address,
            )
            record.request_lifecycle("clone", dataclass_to_wire(event))
            record.ready = True
            self._emit(
                "clone",
                agent_id=record.agent_id,
                class_name=record.agent_class_name,
                data={"source_agent_id": envelope.clone_of},
            )
        elif envelope.kind is EnvelopeKind.ACTIVATION:
            event = PersistencyEvent(
                agent_id=record.agent_id,
                host_name=self.name,
                host_address=self.address,
                reason="activate",
                request=inactive_record.request if inactive_record is not None else None,
                policy=inactive_record.policy if inactive_record is not None else None,
            )
            payload = dataclass_to_wire(event)
            if inactive_record is not None:
                payload["request"] = inactive_record.request.to_wire()
                payload["policy"] = inactive_record.policy.to_wire()
            record.request_lifecycle("activation", payload)
            record.ready = True
        else:  # pragma: no cover - typing already limits this
            raise HostError(f"Unknown envelope kind {envelope.kind!r}")

        return self._current_or_last_proxy(record)

    def _receive_creation(self, payload: dict[str, Any]) -> PagletProxy:
        agent_cls = resolve_qualified_name(payload["agent_class_name"])
        state_cls = resolve_qualified_name(payload["state_class_name"])
        if not issubclass(agent_cls, Paglet):
            raise HostError(f"{payload['agent_class_name']} is not a Paglet subclass")
        if not is_dataclass(state_cls):
            raise HostError(f"{payload['state_class_name']} is not a dataclass state")
        state = dataclass_from_wire(state_cls, payload.get("state") or {})
        return self.create(agent_cls, state, init=payload.get("init"), agent_id=payload.get("agent_id"))

    def _start_resident_services(self) -> None:
        config = self.launch_config
        if config is None:
            return
        for resident_service in config.resident_services:
            if not resident_service.enabled:
                self._emit(
                    "resident-service-skip",
                    agent_id=resident_service.agent_id,
                    data={"reason": "disabled", "use": resident_service.use, "class": resident_service.class_name},
                )
                continue
            try:
                resolved = resolve_resident_service(resident_service)
                self._declare_resident_service(resolved)
            except Exception as exc:
                self._emit(
                    "resident-service-failed",
                    agent_id=resident_service.agent_id,
                    data={
                        "use": resident_service.use,
                        "class": resident_service.class_name,
                        "error": str(exc),
                    },
                )

    def _declare_resident_service(self, resolved: ResolvedResidentService) -> None:
        contract = resolved.spec.contract
        managed = _ManagedResidentService(
            agent_cls=resolved.agent_cls,
            state_class=resolved.agent_cls.state_class(),
            state_wire=dataclass_to_wire(resolved.state),
            agent_id=resolved.agent_id,
            contract=contract,
            scope=resolved.scope,
            lifecycle=resolved.lifecycle,
            idle_timeout=resolved.idle_timeout,
            singleton=resolved.singleton,
            init=resolved.init,
        )
        with self._lock:
            self._resident_services[resolved.agent_id] = managed
        record = self._services.advertise(
            host_name=self.name,
            host_url=self.address,
            name=contract.name,
            proxy=PagletProxyRef(self.address, resolved.agent_id),
            capabilities=contract.capabilities,
            metadata=self._resident_service_metadata(managed),
            scope=resolved.scope,
        )
        self._emit(
            "resident-service-declare",
            agent_id=resolved.agent_id,
            class_name=qualified_name(resolved.agent_cls),
            service_name=contract.name,
            data=record.to_wire(),
        )
        self._emit("service-advertise", agent_id=resolved.agent_id, service_name=contract.name, data=record.to_wire())
        if resolved.lifecycle is ResidentLifecycle.EAGER:
            self._ensure_resident_service_active(resolved.agent_id)

    def _ensure_resident_service_active(self, agent_id: str) -> PagletProxy:
        lock = self._resident_activation_lock(agent_id)
        with lock:
            with self._lock:
                if agent_id in self._agents:
                    return PagletProxy(self.address, agent_id, self.client)
                managed = self._resident_services.get(agent_id)
                inactive = self._inactive.get(agent_id)
            if managed is None:
                raise InvalidAgentError(f"No managed resident service {agent_id!r} on {self.name}")
            if inactive is not None:
                proxy = self.activate(agent_id)
                self._mark_resident_service_used(agent_id)
                self._emit(
                    "resident-service-activate",
                    agent_id=agent_id,
                    class_name=qualified_name(managed.agent_cls),
                    service_name=managed.contract.name,
                    data={"lifecycle": managed.lifecycle.value},
                )
                return proxy

            state = dataclass_from_wire(managed.state_class, managed.state_wire)
            proxy = self.create(managed.agent_cls, state, init=managed.init, agent_id=agent_id)
            self._mark_resident_service_used(agent_id)
            self._emit(
                "resident-service-create",
                agent_id=agent_id,
                class_name=qualified_name(managed.agent_cls),
                service_name=managed.contract.name,
                data={"lifecycle": managed.lifecycle.value},
            )
            return proxy

    def lease_service_handle(
        self,
        handle: ServiceHandle,
        *,
        ttl: float = DEFAULT_SERVICE_LEASE_TTL_SECONDS,
    ) -> ServiceLease:
        record = handle.record
        host_url = record.host_url or record.proxy.host_url
        if host_url.rstrip("/") == self.address.rstrip("/"):
            payload = self.acquire_resident_service_lease(record.proxy.agent_id, record.name, ttl=ttl)
        else:
            payload = self.client.post_json(
                f"{host_url.rstrip('/')}/services/leases",
                {
                    "agent_id": record.proxy.agent_id,
                    "service_name": record.name,
                    "ttl": ttl,
                },
            )
        return ServiceLease(
            handle=handle,
            lease_id=str(payload["lease_id"]),
            host_url=host_url,
            expires_at=float(payload["expires_at"]),
            client=self.client,
        )

    def acquire_resident_service_lease(self, agent_id: str, service_name: str, *, ttl: float) -> dict[str, Any]:
        ttl = DEFAULT_SERVICE_LEASE_TTL_SECONDS if ttl is None else float(ttl)
        if ttl <= 0:
            raise HostError("service lease ttl must be positive")
        lease_id = uuid.uuid4().hex
        now = time.time()
        expires_at = now + ttl
        with self._lock:
            managed = self._resident_services.get(agent_id)
            if managed is None or managed.contract.name != service_name:
                raise ServiceNotFoundError(f"No managed resident service {service_name!r} for agent {agent_id!r}")
            managed.leases[lease_id] = expires_at
            managed.last_used = now
        try:
            self._ensure_resident_service_active(agent_id)
        except Exception:
            with self._lock:
                managed = self._resident_services.get(agent_id)
                if managed is not None:
                    managed.leases.pop(lease_id, None)
            raise
        self._emit(
            "service-lease-acquire",
            agent_id=agent_id,
            service_name=service_name,
            data={"lease_id": lease_id, "expires_at": expires_at, "ttl": ttl},
        )
        return {"lease_id": lease_id, "expires_at": expires_at}

    def release_resident_service_lease(self, lease_id: str) -> dict[str, bool]:
        released = False
        agent_id = None
        service_name = None
        with self._lock:
            for managed in self._resident_services.values():
                if lease_id in managed.leases:
                    managed.leases.pop(lease_id, None)
                    managed.last_used = time.time()
                    agent_id = managed.agent_id
                    service_name = managed.contract.name
                    released = True
                    break
        if released:
            self._emit(
                "service-lease-release",
                agent_id=agent_id,
                service_name=service_name,
                data={"lease_id": lease_id},
            )
        return {"released": released}

    def _resident_activation_lock(self, agent_id: str) -> threading.Lock:
        with self._lock:
            lock = self._resident_activation_locks.get(agent_id)
            if lock is None:
                lock = threading.Lock()
                self._resident_activation_locks[agent_id] = lock
            return lock

    def _resident_service_metadata(self, managed: _ManagedResidentService) -> dict[str, Any]:
        metadata = managed.contract.advertise_metadata()
        metadata[RESIDENT_SERVICE_METADATA_KEY] = {
            "agent_id": managed.agent_id,
            "agent_class_name": qualified_name(managed.agent_cls),
            "lifecycle": managed.lifecycle.value,
            "idle_timeout": managed.idle_timeout,
        }
        return metadata

    def _is_resident_service_record(self, record: ServiceRecord) -> bool:
        return RESIDENT_SERVICE_METADATA_KEY in record.metadata

    def _begin_resident_service_call(self, agent_id: str) -> None:
        with self._lock:
            managed = self._resident_services.get(agent_id)
            if managed is not None:
                managed.in_flight += 1

    def _end_resident_service_call(self, agent_id: str) -> None:
        with self._lock:
            managed = self._resident_services.get(agent_id)
            if managed is not None:
                managed.in_flight = max(0, managed.in_flight - 1)
                managed.last_used = time.time()

    def _mark_resident_service_used(self, agent_id: str) -> None:
        with self._lock:
            managed = self._resident_services.get(agent_id)
            if managed is not None:
                managed.last_used = time.time()

    def _resident_service_shutdown_policy(self, agent_id: str) -> DeactivationPolicy:
        with self._lock:
            managed = self._resident_services.get(agent_id)
        return DeactivationPolicy(activate_on_startup=managed is None or managed.lifecycle is ResidentLifecycle.EAGER)

    def _resident_maintenance(self, now: float) -> None:
        expired_leases: list[tuple[str, str, str]] = []
        due_agent_ids: list[str] = []
        with self._lock:
            for managed in self._resident_services.values():
                for lease_id, expires_at in list(managed.leases.items()):
                    if expires_at <= now:
                        managed.leases.pop(lease_id, None)
                        expired_leases.append((managed.agent_id, managed.contract.name, lease_id))
                if (
                    managed.lifecycle is ResidentLifecycle.LAZY
                    and managed.agent_id in self._agents
                    and managed.in_flight == 0
                    and not managed.leases
                    and now - managed.last_used >= managed.idle_timeout
                ):
                    due_agent_ids.append(managed.agent_id)

        for agent_id, service_name, lease_id in expired_leases:
            self._emit(
                "service-lease-expire",
                agent_id=agent_id,
                service_name=service_name,
                data={"lease_id": lease_id},
            )

        for agent_id in due_agent_ids:
            with self._lock:
                managed = self._resident_services.get(agent_id)
                still_due = (
                    managed is not None
                    and managed.lifecycle is ResidentLifecycle.LAZY
                    and agent_id in self._agents
                    and managed.in_flight == 0
                    and not managed.leases
                    and now - managed.last_used >= managed.idle_timeout
                )
            if not still_due or managed is None:
                continue
            try:
                self.deactivate(
                    agent_id,
                    DeactivationRequest(
                        reason="resident-service-idle",
                        source="resident-service-manager",
                        policy=DeactivationPolicy(
                            activate_on_message=True,
                            queue_messages_when_inactive=True,
                            activate_on_startup=False,
                        ),
                    ),
                )
                self._emit(
                    "resident-service-idle-deactivate",
                    agent_id=agent_id,
                    class_name=qualified_name(managed.agent_cls),
                    service_name=managed.contract.name,
                    data={"idle_timeout": managed.idle_timeout},
                )
            except PagletError as exc:
                self._emit(
                    "resident-service-failed",
                    agent_id=agent_id,
                    class_name=qualified_name(managed.agent_cls),
                    service_name=managed.contract.name,
                    error=str(exc),
                )

    def _prepare_ticket(self, target: str | TransferTicket) -> TransferTicket:
        ticket = TransferTicket.from_target(target)
        return replace(ticket, destination=self.mesh.resolve_url(ticket.destination).rstrip("/"))

    def _preflight_transfer(self, ticket: TransferTicket) -> dict[str, Any]:
        url = ticket.destination.rstrip("/")
        try:
            info = (
                self.health()
                if url == self.address.rstrip("/")
                else self.client.get_json(
                    f"{url}/health",
                    timeout=ticket.timeout,
                )
            )
        except Exception as exc:
            self._emit("transfer-failed", data={"destination": url, "stage": "preflight"}, error=str(exc))
            raise TransferError(f"Could not preflight transfer target {url}: {exc}") from exc
        code_version = str(info.get("code_version") or "")
        if ticket.expected_code_version is not None and code_version != ticket.expected_code_version:
            message = (
                f"Transfer target {url} has code version {code_version!r}, expected {ticket.expected_code_version!r}"
            )
            self._emit("transfer-failed", data={"destination": url, "stage": "preflight"}, error=message)
            raise TransferError(message)
        capabilities = {str(item) for item in info.get("capabilities", [])}
        missing = [capability for capability in ticket.required_capabilities if capability not in capabilities]
        if missing:
            message = f"Transfer target {url} is missing capabilities: {', '.join(missing)}"
            self._emit("transfer-failed", data={"destination": url, "stage": "preflight"}, error=message)
            raise TransferError(message)
        return {
            "name": str(info.get("name") or urlparse(url).netloc or url),
            "address": str(info.get("address") or url).rstrip("/"),
            "code_version": code_version,
            "capabilities": sorted(capabilities),
        }

    def _post_envelope_with_ticket(
        self,
        ticket: TransferTicket,
        target_info: dict[str, Any],
        envelope: PagletEnvelope,
    ) -> dict[str, Any]:
        if self._is_local_transfer_target(target_info):
            return self._receive_local_envelope_response(envelope)
        url = f"{str(target_info['address']).rstrip('/')}/agents"
        attempts = max(0, ticket.retries) + 1
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                http_timeout = ticket.timeout + 1.0 if _is_relay_transport_url(url) else ticket.timeout
                return self.client.post_pickle(url, {"envelope": envelope.to_wire()}, timeout=http_timeout)
            except Exception as exc:
                last_error = exc
                stage = "relay-forward" if _is_relay_transport_url(url) else "post"
                self._emit(
                    "transfer-failed",
                    agent_id=envelope.agent_id,
                    class_name=envelope.agent_class_name,
                    data={
                        "destination": target_info["address"],
                        "stage": stage,
                        "attempt": attempt + 1,
                        "attempts": attempts,
                    },
                    error=str(exc),
                )
                if attempt + 1 < attempts:
                    time.sleep(max(0.0, ticket.retry_interval))
        target_name = str(target_info.get("name") or target_info["address"])
        if _is_relay_transport_url(str(target_info["address"])):
            raise TransferError(
                f"Transfer to {target_name!r} through relay failed after {attempts} attempt(s): {last_error}"
            )
        raise TransferError(f"Transfer to {target_info['address']} failed after {attempts} attempt(s): {last_error}")

    def _validate_agent_classes(self, agent_class_name: str, state_class_name: str) -> None:
        agent_cls = resolve_qualified_name(agent_class_name)
        state_cls = resolve_qualified_name(state_class_name)
        if not issubclass(agent_cls, Paglet):
            raise HostError(f"{agent_class_name} is not a Paglet subclass")
        if not is_dataclass(state_cls):
            raise HostError(f"{state_class_name} is not a dataclass state")

    def _handle_child_crash(self, record: ChildProcessController) -> None:
        with self._lock:
            current = self._agents.get(record.agent_id)
            if current is not record:
                return
            self._mailboxes.pop(record.agent_id, None)
        for service in self._services.remove_agent(record.agent_id, keep=self._is_resident_service_record):
            self._emit("service-remove", agent_id=record.agent_id, service_name=service.name)
        self._emit(
            "paglet-crashed",
            agent_id=record.agent_id,
            class_name=record.agent_class_name,
            data={"pid": record.pid, "exitcode": record.exitcode},
            error=record.last_error,
        )

    def _handle_child_host_call(self, agent_id: str, op: str, payload: dict[str, Any]) -> Any:
        if op == "get_proxy":
            proxy = self.get_proxy(str(payload["agent_id"]))
            return {"proxy": proxy.to_wire() if proxy is not None else None}
        if op == "get_proxies":
            proxies = self.get_proxies(int(payload.get("state", ACTIVE)))
            return {"proxies": [proxy.to_wire() for proxy in proxies]}
        if op == "get_property":
            return {"value": self.get_property(str(payload["key"]), payload.get("default"))}
        if op == "set_property":
            self.set_property(str(payload["key"]), payload.get("value"))
            return {"ok": True}
        if op == "create_paglet":
            agent_cls = resolve_qualified_name(str(payload["agent_class_name"]))
            state_cls = resolve_qualified_name(str(payload["state_class_name"]))
            state = dataclass_from_wire(state_cls, payload.get("state") or {})
            host_url = payload.get("host_url")
            if host_url is not None and str(host_url).rstrip("/") != self.address.rstrip("/"):
                proxy = self.create_remote(
                    str(host_url), agent_cls, state, init=payload.get("init"), agent_id=payload.get("agent_id")
                )
            else:
                proxy = self.create(agent_cls, state, init=payload.get("init"), agent_id=payload.get("agent_id"))
            return {"proxy": proxy.to_wire()}
        if op == "preflight_transfer":
            target = self._target_from_child_payload(payload.get("target") or {})
            ticket = self._prepare_ticket(target)
            target_info = self._preflight_transfer(ticket)
            return {"ticket": ticket.to_wire(), "target_info": target_info}
        if op == "complete_dispatch":
            return self._complete_child_dispatch(agent_id, payload)
        if op == "complete_clone":
            return self._complete_child_clone(agent_id, payload)
        if op == "complete_deactivate":
            return self._complete_child_deactivate(agent_id, payload)
        if op == "complete_dispose":
            return self._complete_child_dispose(agent_id, payload)
        if op == "advertise_service":
            record = self.advertise_service(
                str(payload["agent_id"]),
                str(payload["name"]),
                capabilities=payload.get("capabilities"),
                metadata=payload.get("metadata"),
                scope=enum_from_wire(payload.get("scope") or ServiceScope.LOCAL.value, ServiceScope, "scope"),
                ttl=float(payload["ttl"]) if payload.get("ttl") is not None else None,
            )
            return {"service": record.to_wire()}
        if op == "unadvertise_service":
            removed = self.unadvertise_service(str(payload["name"]), agent_id=payload.get("agent_id"))
            return {"services": [record.to_wire() for record in removed]}
        if op == "lookup_service":
            record = self.lookup_service(
                str(payload["name"]),
                capability=payload.get("capability"),
                scope=enum_from_wire(payload.get("scope") or ServiceScope.LOCAL.value, ServiceScope, "scope"),
            )
            return {"service": record.to_wire() if record is not None else None}
        if op == "lookup_services":
            records = self.lookup_services(
                payload.get("name"),
                capability=payload.get("capability"),
                scope=enum_from_wire(payload.get("scope") or ServiceScope.LOCAL.value, ServiceScope, "scope"),
            )
            return {"services": [record.to_wire() for record in records]}
        if op == "lease_service_handle":
            record = ServiceRecord.from_wire(payload["record"])
            host_url = record.host_url or record.proxy.host_url
            if host_url.rstrip("/") == self.address.rstrip("/"):
                lease = self.acquire_resident_service_lease(
                    record.proxy.agent_id, record.name, ttl=float(payload.get("ttl", DEFAULT_SERVICE_LEASE_TTL_SECONDS))
                )
            else:
                lease = self.client.post_json(
                    f"{host_url.rstrip('/')}/services/leases",
                    {
                        "agent_id": record.proxy.agent_id,
                        "service_name": record.name,
                        "ttl": float(payload.get("ttl", DEFAULT_SERVICE_LEASE_TTL_SECONDS)),
                    },
                )
            return {"lease_id": lease["lease_id"], "expires_at": lease["expires_at"], "host_url": host_url}
        if op == "health":
            return self.health()
        if op == "mesh_code_version":
            return self.mesh.code_version
        if op == "available_hosts":
            hosts = self.mesh.hosts(
                online_only=bool(payload.get("online_only", True)),
                include_self=bool(payload.get("include_self", True)),
            )
            return {"hosts": [host.to_wire() for host in hosts]}
        if op == "host_status":
            ref = self.mesh.lookup(str(payload["name_or_url"]))
            return {"host": ref.to_wire() if ref is not None else None}
        if op == "is_host_online":
            return {"online": self.mesh.is_online(str(payload["name_or_url"]))}
        if op == "wait_for_host":
            ref = self.mesh.wait_for_host(
                str(payload["name_or_url"]),
                timeout=float(payload.get("timeout", 10.0)),
                interval=float(payload.get("interval", 0.25)),
            )
            return {"host": ref.to_wire()}
        if op == "resolve_host_url":
            return {"url": self.mesh.resolve_url(str(payload["name_or_url"]))}
        if op == "work_dir":
            return {"path": str(self.work_dir_for(agent_id, create=bool(payload.get("create", True))))}
        if op == "persistent_storage":
            storage = self.persistent_storage_for(agent_id, quota_bytes=payload.get("quota_bytes"))
            return {"root": str(storage.root), "quota_bytes": storage.quota_bytes}
        if op.startswith("storage_"):
            return self._handle_child_storage_call(agent_id, op, payload)
        raise HostError(f"Unknown child host call {op!r}")

    def _target_from_child_payload(self, payload: dict[str, Any]) -> str | TransferTicket:
        if "ticket" in payload:
            return TransferTicket.from_wire(payload["ticket"])
        return str(payload["target"])

    def _complete_child_dispatch(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = self._require_agent(agent_id)
        record.state = dict(payload["state"])
        record.resource_status = dict(payload.get("resources") or {})
        ticket = TransferTicket.from_wire(payload["ticket"])
        target_info = dict(payload["target_info"])
        self._cleanup_agent_work_dir(agent_id)
        envelope = self._make_envelope(record, EnvelopeKind.DISPATCH, target_info, ticket=ticket)
        if not _is_relay_transport_url(str(target_info["address"])):
            self._remove_active_agent(agent_id, record, terminate=False)
            record.set_terminal_proxy_wire({"host_url": target_info["address"], "agent_id": agent_id})
            response = self._post_envelope_with_ticket(ticket, target_info, envelope)
            self._emit("dispatch", agent_id=agent_id, class_name=record.agent_class_name, data={"target": target_info})
            return {"proxy": response["proxy"]}
        record.departing = True
        try:
            response = self._post_envelope_with_ticket(ticket, target_info, envelope)
        except Exception:
            record.departing = False
            raise
        self._remove_active_agent(agent_id, record, terminate=False)
        record.set_terminal_proxy_wire(response["proxy"])
        self._emit("dispatch", agent_id=agent_id, class_name=record.agent_class_name, data={"target": target_info})
        return {"proxy": response["proxy"]}

    def _complete_child_clone(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = self._require_agent(agent_id)
        record.state = dict(payload["state"])
        ticket = TransferTicket.from_wire(payload["ticket"])
        target_info = dict(payload["target_info"])
        clone_id = str(payload["clone_id"])
        envelope = self._make_envelope(
            record,
            EnvelopeKind.CLONE,
            target_info,
            agent_id=clone_id,
            clone_of=agent_id,
            ticket=ticket,
        )
        response = self._post_envelope_with_ticket(ticket, target_info, envelope)
        self._emit(
            "clone",
            agent_id=agent_id,
            class_name=record.agent_class_name,
            data={"clone_agent_id": clone_id, "target": target_info},
        )
        return {"proxy": response["proxy"]}

    def _complete_child_deactivate(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = self._require_agent(agent_id)
        record.state = dict(payload["state"])
        record.resource_status = dict(payload.get("resources") or {})
        request = DeactivationRequest.from_wire(payload.get("request"))
        policy = DeactivationPolicy.from_wire(payload.get("policy"))
        info = {"name": self.name, "address": self.address}
        envelope = self._make_envelope(record, EnvelopeKind.ACTIVATION, info)
        inactive = InactiveRecord(envelope=envelope, policy=policy, request=request)
        self._write_inactive_record(inactive)
        with self._lock:
            self._inactive[agent_id] = inactive
        self._remove_active_agent(agent_id, record, terminate=False)
        self._emit("deactivate", agent_id=agent_id, class_name=record.agent_class_name, data={"reason": request.reason})
        return {"proxy": {"host_url": self.address, "agent_id": agent_id}}

    def _complete_child_dispose(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = self._require_agent(agent_id)
        record.state = dict(payload.get("state") or record.state)
        record.resource_status = dict(payload.get("resources") or {})
        self._cleanup_agent_work_dir(agent_id)
        self._remove_active_agent(agent_id, record, terminate=False)
        self._emit("dispose", agent_id=agent_id, class_name=record.agent_class_name, data={"active": True})
        return {"ok": True}

    def _handle_child_storage_call(self, agent_id: str, op: str, payload: dict[str, Any]) -> Any:
        storage = self.persistent_storage_for(agent_id, quota_bytes=payload.get("quota_bytes"))
        if op == "storage_read_bytes":
            return {"data": storage.read_bytes(str(payload["path"]))}
        if op == "storage_write_bytes":
            path = storage.write_bytes(str(payload["path"]), payload.get("data") or b"")
            return {"path": str(path)}
        if op == "storage_delete":
            storage.delete(str(payload["path"]))
            return {"ok": True}
        if op == "storage_clear":
            storage.clear()
            return {"ok": True}
        if op == "storage_status":
            status = storage.status()
            return {
                "root": status.root,
                "used_bytes": status.used_bytes,
                "quota_bytes": status.quota_bytes,
                "available_bytes": status.available_bytes,
            }
        raise HostError(f"Unknown storage operation {op!r}")

    def _clear_work_root(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(self._work_root)
        self._work_root.mkdir(parents=True, exist_ok=True)

    def _cleanup_agent_work_dir(self, agent_id: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(self._work_path(agent_id))

    def _work_path(self, agent_id: str) -> Path:
        return self._work_root / self._safe_storage_name(agent_id)

    @classmethod
    def _storage_class_key(cls, class_name: str) -> str:
        return cls._safe_storage_name(class_name.replace(":", "."))

    @staticmethod
    def _safe_storage_name(value: str) -> str:
        return "".join(char if char.isalnum() or char in "._-" else "_" for char in value) or "storage"

    def _arrival_mode(self, envelope: PagletEnvelope) -> ArrivalMode:
        ticket = envelope.metadata.get("transfer_ticket")
        if isinstance(ticket, dict):
            return enum_from_wire(
                ticket.get("arrival_mode") or ArrivalMode.ACTIVATE.value,
                ArrivalMode,
                "arrival_mode",
            )
        return ArrivalMode.ACTIVATE

    def _inactive_arrival_record(self, envelope: PagletEnvelope) -> InactiveRecord:
        activation_envelope = PagletEnvelope(
            kind=EnvelopeKind.ACTIVATION,
            agent_id=envelope.agent_id,
            agent_class_name=envelope.agent_class_name,
            state_class_name=envelope.state_class_name,
            state=dict(envelope.state),
            source_host_name=envelope.source_host_name,
            source_host_address=envelope.source_host_address,
            target_host_name=self.name,
            target_host_address=self.address,
            clone_of=envelope.clone_of,
            metadata=dict(envelope.metadata),
        )
        return InactiveRecord(
            envelope=activation_envelope,
            policy=DeactivationPolicy(),
            request=DeactivationRequest(
                reason=f"{envelope.kind.value}-arrival",
                source="transfer",
                metadata={"arrival_mode": ArrivalMode.INACTIVE.value},
            ),
        )

    def _current_or_last_proxy(self, record: ChildProcessController) -> PagletProxy:
        proxy = self.get_proxy(record.agent_id)
        if proxy is not None:
            return proxy
        terminal_proxy = record.terminal_proxy_wire()
        if terminal_proxy is not None:
            return PagletProxy.from_wire(terminal_proxy, self.client)
        raise InvalidAgentError(f"Paglet {record.agent_id!r} moved or disappeared without a proxy")

    def _start_child(
        self,
        *,
        agent_id: str,
        agent_class_name: str,
        state_class_name: str,
        state: dict[str, Any],
    ) -> ChildProcessController:
        self._validate_agent_classes(agent_class_name, state_class_name)
        config = make_child_config(
            host_name=self.name,
            host_address=self.address,
            host_api_key=self.api_key,
            agent_id=agent_id,
            agent_class_name=agent_class_name,
            state_class_name=state_class_name,
            state=state,
        )
        record = ChildProcessController(
            config,
            host_call_handler=lambda op, payload, child_id=agent_id: self._handle_child_host_call(
                child_id, op, payload
            ),
            crash_handler=self._handle_child_crash,
        )
        mailbox = MessageMailbox(
            agent_id,
            lambda message, oneway, child_id=agent_id: self._deliver_active_message(child_id, message, oneway=oneway),
            max_workers=1,
        )
        with self._lock:
            old_record = self._agents.pop(agent_id, None)
            old_mailbox = self._mailboxes.pop(agent_id, None)
            self._agents[agent_id] = record
            self._mailboxes[agent_id] = mailbox
        if old_mailbox is not None:
            old_mailbox.close()
        if old_record is not None and not old_record.departing:
            old_record.terminate(timeout=0.5, kill_timeout=0.5)
        return record

    def _remove_active_agent(
        self,
        agent_id: str,
        expected: ChildProcessController | None = None,
        *,
        terminate: bool = False,
    ) -> None:
        with self._lock:
            current = self._agents.get(agent_id)
            if expected is not None and current is not expected:
                return
            if current is not None:
                current.departing = True
            self._agents.pop(agent_id, None)
            mailbox = self._mailboxes.pop(agent_id, None)
        if mailbox is not None:
            mailbox.close()
        for record in self._services.remove_agent(agent_id, keep=self._is_resident_service_record):
            self._emit("service-remove", agent_id=agent_id, service_name=record.name)
        if current is not None and terminate:
            current.terminate(timeout=0.5, kill_timeout=0.5)

    def _require_agent(self, agent_id: str) -> ChildProcessController:
        with self._lock:
            record = self._agents.get(agent_id)
        if record is None:
            raise InvalidAgentError(f"No active paglet {agent_id!r} on {self.name}")
        if record.crashed:
            raise PagletCrashedError(f"Paglet {agent_id!r} crashed: {record.last_error}")
        return record

    def _make_envelope(
        self,
        record: ChildProcessController,
        kind: EnvelopeKind,
        target_info: dict[str, Any],
        *,
        agent_id: str | None = None,
        clone_of: str | None = None,
        ticket: TransferTicket | None = None,
    ) -> PagletEnvelope:
        metadata: dict[str, Any] = {}
        if ticket is not None:
            metadata["transfer_ticket"] = ticket.to_wire()
        return PagletEnvelope(
            kind=kind,
            agent_id=agent_id or record.agent_id,
            agent_class_name=record.agent_class_name,
            state_class_name=record.state_class_name,
            state=dict(record.state),
            source_host_name=self.name,
            source_host_address=self.address,
            target_host_name=target_info["name"],
            target_host_address=target_info["address"],
            clone_of=clone_of,
            metadata=metadata,
        )

    def _host_info(self, target: str) -> dict[str, str]:
        url = target.rstrip("/")
        try:
            info = self.client.get_json(f"{url}/health")
            return {"name": str(info["name"]), "address": str(info["address"]).rstrip("/")}
        except RemoteHostError:
            # Keep the runtime usable in tests/dev even if the target does not
            # expose /health yet; the actual POST will still fail if unreachable.
            parsed = urlparse(url)
            return {"name": parsed.netloc or url, "address": url}

    def _is_local_transfer_target(self, target_info: dict[str, Any]) -> bool:
        return str(target_info.get("address") or "").rstrip("/") == self.address.rstrip("/")

    def _receive_local_envelope_response(self, envelope: PagletEnvelope) -> dict[str, Any]:
        results: queue.Queue[dict[str, Any] | BaseException] = queue.Queue(maxsize=1)

        def receive() -> None:
            try:
                results.put({"proxy": self._receive_envelope(envelope).to_wire()})
            except BaseException as exc:
                results.put(exc)

        thread = threading.Thread(
            target=receive,
            name=f"paglets-local-receive-{envelope.agent_id[:8]}",
            daemon=True,
        )
        thread.start()
        result = results.get()
        thread.join(timeout=0.1)
        if isinstance(result, BaseException):
            raise result
        return result

    def _summary(self, record: ChildProcessController) -> dict[str, Any]:
        mailbox = self._mailboxes.get(record.agent_id)
        return {
            "agent_id": record.agent_id,
            "class_name": record.agent_class_name,
            "state_class_name": record.state_class_name,
            "host": self.name,
            "address": self.address,
            "active": not record.crashed,
            "pid": record.pid,
            "crashed": record.crashed,
            "exitcode": record.exitcode,
            "error": record.last_error,
            "mailbox": mailbox.status().to_wire() if mailbox is not None else None,
            "resources": record.resource_status_snapshot(),
        }

    def _inactive_summary(self, record: InactiveRecord) -> dict[str, Any]:
        return {
            "agent_id": record.envelope.agent_id,
            "class_name": record.envelope.agent_class_name,
            "state_class_name": record.envelope.state_class_name,
            "host": self.name,
            "address": self.address,
            "active": False,
            "deactivated_at": record.deactivated_at,
        }

    def _state_payload(self, agent_id: str) -> dict[str, Any]:
        with self._lock:
            record = self._agents.get(agent_id)
            mailbox = self._mailboxes.get(agent_id)
            inactive = self._inactive.get(agent_id)
        if record is not None:
            try:
                state_payload = record.fetch_state(timeout=2.0)
            except Exception:
                state_payload = dict(record.state)
            return {
                "agent_id": record.agent_id,
                "class_name": record.agent_class_name,
                "state_class_name": record.state_class_name,
                "host": self.name,
                "address": self.address,
                "active": not record.crashed,
                "pid": record.pid,
                "crashed": record.crashed,
                "exitcode": record.exitcode,
                "error": record.last_error,
                "state": state_payload,
                "mailbox": mailbox.status().to_wire() if mailbox is not None else None,
                "resources": record.resource_status_snapshot(),
            }
        if inactive is not None:
            return {
                "agent_id": inactive.envelope.agent_id,
                "class_name": inactive.envelope.agent_class_name,
                "state_class_name": inactive.envelope.state_class_name,
                "host": self.name,
                "address": self.address,
                "active": False,
                "state": inactive.envelope.state,
                "deactivation_policy": inactive.policy.to_wire(),
                "queued_message_count": len(inactive.queued_messages),
            }
        raise InvalidAgentError(f"No paglet {agent_id!r} on {self.name}")

    def _load_inactive_records(self) -> None:
        if not self._inactive_dir.exists():
            return
        records: dict[str, InactiveRecord] = {}
        for path in sorted(self._inactive_dir.glob("*.json")):
            try:
                record = InactiveRecord.from_wire(json.loads(path.read_text(encoding="utf-8")))
            except Exception:
                continue
            records[record.agent_id] = record
        with self._lock:
            self._inactive.update(records)

    def _write_inactive_record(self, record: InactiveRecord) -> None:
        self._inactive_dir.mkdir(parents=True, exist_ok=True)
        path = self._inactive_path(record.agent_id)
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(json_safe(record.to_wire()), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp_path.replace(path)

    def _delete_inactive_record(self, agent_id: str) -> None:
        path = self._inactive_path(agent_id)
        try:
            path.unlink()
        except FileNotFoundError:
            return

    def _inactive_path(self, agent_id: str) -> Path:
        return self._inactive_dir / f"{agent_id}.json"

    def _activate_startup_records(self) -> None:
        with self._lock:
            startup_ids = [agent_id for agent_id, record in self._inactive.items() if record.policy.activate_on_startup]
        for agent_id in startup_ids:
            try:
                self.activate(agent_id)
            except PagletError:
                continue

    def _emit_launch_config_sync_result(self) -> None:
        result = self.launch_config_sync_result
        if result is None:
            return
        if result.action is LaunchConfigSyncAction.COPIED:
            self._emit("launch-config-copy", data={"path": str(result.path), "message": result.message})
        elif result.action is LaunchConfigSyncAction.UPDATED:
            self._emit(
                "launch-config-update",
                data={
                    "path": str(result.path),
                    "backup_path": str(result.backup_path) if result.backup_path is not None else None,
                    "message": result.message,
                },
            )

    def _start_launch_agents(self) -> None:
        config = self.launch_config
        if config is None:
            return
        for startup_agent in config.startup_agents:
            if not startup_agent.enabled:
                self._emit(
                    "startup-agent-skip",
                    data={"reason": "disabled", "use": startup_agent.use, "class": startup_agent.class_name},
                )
                continue
            try:
                resolved = resolve_startup_agent(startup_agent)
                class_name = qualified_name(resolved.agent_cls)
                if resolved.singleton and resolved.agent_id:
                    with self._lock:
                        active = resolved.agent_id in self._agents
                        inactive = resolved.agent_id in self._inactive
                    if active:
                        self._emit(
                            "startup-agent-skip",
                            agent_id=resolved.agent_id,
                            class_name=class_name,
                            data={"reason": "already-active"},
                        )
                        continue
                    if inactive:
                        self.activate(resolved.agent_id)
                        self._emit(
                            "startup-agent-activate",
                            agent_id=resolved.agent_id,
                            class_name=class_name,
                            data={"source": "launch-config"},
                        )
                        continue

                proxy = self.create(
                    resolved.agent_cls,
                    resolved.state,
                    init=resolved.init,
                    agent_id=resolved.agent_id,
                )
                self._emit(
                    "startup-agent-create",
                    agent_id=proxy.agent_id,
                    class_name=class_name,
                    data={"source": "launch-config"},
                )
            except Exception as exc:
                self._emit(
                    "startup-agent-failed",
                    agent_id=startup_agent.agent_id,
                    data={
                        "use": startup_agent.use,
                        "class": startup_agent.class_name,
                        "error": str(exc),
                    },
                )

    def _start_activation_scheduler(self) -> None:
        if self._activation_thread is not None and self._activation_thread.is_alive():
            return
        self._activation_thread = threading.Thread(
            target=self._activation_scheduler_loop,
            name=f"paglets-activation-{self.name}",
            daemon=True,
        )
        self._activation_thread.start()

    def _stop_activation_scheduler(self) -> None:
        self._activation_stop.set()
        thread = self._activation_thread
        self._activation_thread = None
        if thread is not None and thread.is_alive():
            thread.join(timeout=2)

    def _activation_scheduler_loop(self) -> None:
        while not self._activation_stop.wait(self._next_activation_delay()):
            now = time.time()
            self._resident_maintenance(now)
            with self._lock:
                due_ids = [
                    agent_id
                    for agent_id, record in self._inactive.items()
                    if record.policy.activate_at is not None and record.policy.activate_at <= now
                ]
            for agent_id in due_ids:
                try:
                    self.activate(agent_id)
                except PagletError:
                    continue

    def _next_activation_delay(self) -> float:
        with self._lock:
            activate_at_values = [
                record.policy.activate_at for record in self._inactive.values() if record.policy.activate_at is not None
            ]
        if not activate_at_values:
            return 1.0
        return max(0.05, min(1.0, min(activate_at_values) - time.time()))

    def _deactivate_active_for_shutdown(self) -> None:
        with self._lock:
            records = list(self._agents.items())
        for agent_id, record in records:
            with self._lock:
                if self._agents.get(agent_id) is not record:
                    continue
            request = DeactivationRequest(
                reason="shutdown",
                source="host",
                policy=self._resident_service_shutdown_policy(agent_id),
            )
            try:
                prepared = record.request(
                    "deactivate_prepare",
                    {"request": request.to_wire()},
                    timeout=SHUTDOWN_DEACTIVATE_TIMEOUT_SECONDS,
                )
                record._update_from_reply(prepared)
                policy = DeactivationPolicy.from_wire(prepared.get("policy"))
                info = {"name": self.name, "address": self.address}
                envelope = self._make_envelope(record, EnvelopeKind.ACTIVATION, info)
                inactive = InactiveRecord(envelope=envelope, policy=policy, request=request)
                self._write_inactive_record(inactive)
                with self._lock:
                    if self._agents.get(agent_id) is record:
                        self._inactive[agent_id] = inactive
                self._remove_active_agent(agent_id, expected=record, terminate=True)
                self._emit(
                    "deactivate",
                    agent_id=agent_id,
                    class_name=envelope.agent_class_name,
                    data={"reason": request.reason},
                )
            except Exception:
                continue

    def _terminate_active_children(self) -> None:
        with self._lock:
            records = list(self._agents.items())
            self._agents.clear()
            mailboxes = list(self._mailboxes.values())
            self._mailboxes.clear()
        for mailbox in mailboxes:
            mailbox.close()
        for agent_id, record in records:
            record.departing = True
            record.terminate(timeout=0.5, kill_timeout=0.5)
            for service in self._services.remove_agent(agent_id, keep=self._is_resident_service_record):
                self._emit("service-remove", agent_id=agent_id, service_name=service.name)

    def _drain_queued_messages(self, record: InactiveRecord) -> None:
        for index, queued in enumerate(record.queued_messages):
            if self.get_proxy(record.agent_id) is None:
                self._requeue_messages(record.agent_id, record.queued_messages[index:])
                return
            try:
                self.deliver_message(
                    record.agent_id,
                    queued.message,
                    oneway=queued.oneway,
                    activate_if_inactive=False,
                    no_delay=True,
                )
            except PagletError:
                continue

    def _requeue_messages(self, agent_id: str, messages: list[QueuedMessage]) -> None:
        if not messages:
            return
        with self._lock:
            record = self._inactive.get(agent_id)
        if record is None:
            return
        record.queued_messages.extend(messages)
        self._write_inactive_record(record)

    def _emit(
        self,
        kind: str,
        *,
        agent_id: str | None = None,
        class_name: str | None = None,
        message_id: str | None = None,
        service_name: str | None = None,
        data: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> ContextEvent:
        return self._events.emit(
            kind=kind,
            host_name=self.name,
            host_address=self.address,
            agent_id=agent_id,
            class_name=class_name,
            message_id=message_id,
            service_name=service_name,
            data=data or {},
            error=error,
        )

    @staticmethod
    def _safe_host_name(name: str) -> str:
        return "".join(char if char.isalnum() or char in "._-" else "_" for char in name) or "host"


class _RemoteResourceRegistry:
    def __init__(self, host: Host, agent_id: str):
        self._host = host
        self._agent_id = agent_id

    def status(self) -> dict[str, bool]:
        return self._host._require_agent(self._agent_id).resource_status_snapshot()

    def remove(self, name: str) -> None:
        self._host._require_agent(self._agent_id).request("resource_remove", {"name": name})

    def cleanup(self, *, reason: str = "lifecycle") -> None:
        self._host._require_agent(self._agent_id).cleanup_resources(reason=reason)


def _trim_git_output(value: str, *, limit: int = 500) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."
