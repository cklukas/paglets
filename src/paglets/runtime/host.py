# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import os
import queue
import shutil
import threading
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import paglets.tooling.git_update as git_update
from paglets.artifacts import (
    DEFAULT_ARTIFACT_MAX_BYTES,
    DEFAULT_ARTIFACT_SPOOL_TTL_SECONDS,
    DEFAULT_ARTIFACT_STORAGE_QUOTA_BYTES,
    ArtifactStore,
    PagletFileRef,
    paglet_file_ref_from_path,
)
from paglets.config.startup import (
    LaunchConfig,
    LaunchConfigSyncResult,
    resolve_startup_agent,
)
from paglets.core.agent import ACTIVE, INACTIVE, PagletState
from paglets.core.context_events import ContextEvent, ContextEventLog, ContextListener
from paglets.core.errors import (
    HostError,
    InvalidAgentError,
    PagletCrashedError,
    PagletInactiveError,
)
from paglets.core.messages import DEACTIVATE, UNQUEUED_PRIORITY, Message, ReplySet
from paglets.core.runtime_values import (
    LaunchConfigSyncAction,
)
from paglets.persistence.persistency import DeactivationRequest, InactiveRecord, QueuedMessage
from paglets.persistence.storage import DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES, ManagedStorage
from paglets.remote.client import HostClient
from paglets.remote.mesh import HostRef, MeshRegistry
from paglets.remote.proxy import PagletProxy
from paglets.runtime.binding import _bind_host_specs, _resolve_bind_hosts, _resolve_public_host
from paglets.runtime.child_calls import _ChildCallMixin
from paglets.runtime.http_api import PagletHTTPServer as _PagletHTTPServer
from paglets.runtime.http_api import RequestHandler as _RequestHandler
from paglets.runtime.inactive_records import _InactiveRecordsMixin
from paglets.runtime.lifecycle import _LifecycleMixin
from paglets.runtime.mailbox import MessageMailbox
from paglets.runtime.process_runtime import ChildProcessController, make_child_config
from paglets.runtime.relay import RelayDelivery as _RelayDelivery
from paglets.runtime.relay import RelayMixin
from paglets.runtime.relay import RelayNode as _RelayNode
from paglets.runtime.resident_services import _ManagedResidentService, _ResidentServicesMixin
from paglets.serialization.codec import dataclass_from_wire, qualified_name
from paglets.services.contracts import ServiceRegistry

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
    "artifacts:upload",
    "artifacts:download",
    "artifacts:list",
    "artifacts:delete",
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


class Host(_LifecycleMixin, _ResidentServicesMixin, _ChildCallMixin, _InactiveRecordsMixin, RelayMixin):
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
        artifact_max_bytes: int | None = DEFAULT_ARTIFACT_MAX_BYTES,
        artifact_storage_quota_bytes: int | None = DEFAULT_ARTIFACT_STORAGE_QUOTA_BYTES,
        artifact_spool_ttl_seconds: float = DEFAULT_ARTIFACT_SPOOL_TTL_SECONDS,
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
        tags: Sequence[str] | None = None,
        properties: dict[str, str] | None = None,
    ):
        self.name = name
        self.tags = _normalize_host_tags(tags or ())
        self.host_properties = _normalize_host_properties(properties or {})
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
        self._artifact_root = self.persistence_dir / "artifacts"
        self.persistent_storage_quota_bytes = persistent_storage_quota_bytes
        self.artifact_max_bytes = 0 if artifact_max_bytes is None else max(0, int(artifact_max_bytes))
        self.artifact_storage_quota_bytes = (
            None if artifact_storage_quota_bytes is None else max(0, int(artifact_storage_quota_bytes))
        )
        self.artifact_spool_ttl_seconds = max(1.0, float(artifact_spool_ttl_seconds))
        self.artifacts = ArtifactStore(
            self._artifact_root,
            host_url=self.address,
            max_artifact_bytes=self.artifact_max_bytes,
            quota_bytes=self.artifact_storage_quota_bytes,
            spool_ttl_seconds=self.artifact_spool_ttl_seconds,
        )
        self._registered_files: dict[str, dict[str, PagletFileRef]] = {}
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
        self._artifact_cleanup_stop = threading.Event()
        self._artifact_cleanup_thread: threading.Thread | None = None
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
        self._start_artifact_cleanup()
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
        self._stop_artifact_cleanup()
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
            self.artifacts.set_host_url(self.address)
            self.artifacts.cleanup_temporary()
        self._activation_stop.clear()
        self._emit_launch_config_sync_result()
        self._activate_startup_records()
        self._start_resident_services()
        self._start_launch_agents()
        self._start_activation_scheduler()
        self.mesh.refresh_self()
        self._start_relay_client()
        self._start_artifact_cleanup()
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
        self.artifacts.set_host_url(self.address)
        self.artifacts.cleanup_temporary()
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

    def _start_artifact_cleanup(self) -> None:
        if self._artifact_cleanup_thread is not None and self._artifact_cleanup_thread.is_alive():
            return
        self._artifact_cleanup_stop.clear()
        self._artifact_cleanup_thread = threading.Thread(
            target=self._artifact_cleanup_loop,
            name=f"paglets-artifacts-cleanup-{self.name}",
            daemon=True,
        )
        self._artifact_cleanup_thread.start()

    def _stop_artifact_cleanup(self) -> None:
        self._artifact_cleanup_stop.set()
        thread = self._artifact_cleanup_thread
        self._artifact_cleanup_thread = None
        if thread is not None and thread is not threading.current_thread() and thread.is_alive():
            thread.join(timeout=2)

    def _artifact_cleanup_loop(self) -> None:
        interval = max(1.0, min(float(self.artifact_spool_ttl_seconds), 300.0))
        while not self._artifact_cleanup_stop.wait(interval):
            with contextlib.suppress(Exception):
                self.artifacts.cleanup_temporary()

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

    def register_file_for(
        self,
        agent_id: str,
        path: str | Path,
        *,
        name: str | None = None,
        mode: str = "copy",
    ) -> PagletFileRef:
        self._require_agent(agent_id)
        ref = paglet_file_ref_from_path(
            path,
            name=name,
            mode=mode,
            host_name=self.name,
            host_url=self.address,
        )
        with self._lock:
            files = self._registered_files.setdefault(agent_id, {})
            files[ref.name] = ref
        return ref

    def registered_files_for(self, agent_id: str) -> list[PagletFileRef]:
        with self._lock:
            return [PagletFileRef.from_wire(ref.to_wire()) for ref in self._registered_files.get(agent_id, {}).values()]

    def unregister_file_for(self, agent_id: str, name_or_ref: str | PagletFileRef) -> None:
        name = name_or_ref.name if isinstance(name_or_ref, PagletFileRef) else str(name_or_ref)
        with self._lock:
            files = self._registered_files.get(agent_id)
            if files is not None:
                files.pop(name, None)
                if not files:
                    self._registered_files.pop(agent_id, None)

    def registered_file_path_for(self, agent_id: str, name_or_ref: str | PagletFileRef) -> Path:
        name = name_or_ref.name if isinstance(name_or_ref, PagletFileRef) else str(name_or_ref)
        with self._lock:
            ref = self._registered_files.get(agent_id, {}).get(name)
        if ref is None:
            raise HostError(f"No registered file {name!r} for paglet {agent_id!r}")
        return Path(ref.current_path)

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
            "tags": list(self.tags),
            "properties": dict(self.host_properties),
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


def _normalize_host_tags(tags: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({str(tag).strip().casefold() for tag in tags if str(tag).strip()}))


def _normalize_host_properties(properties: dict[str, str]) -> dict[str, str]:
    return {str(key).strip(): str(value) for key, value in properties.items() if str(key).strip()}


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
