# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field, is_dataclass, replace
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import shutil
import threading
import time
import uuid
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from .agent import ACTIVE, INACTIVE, NOT_HANDLED, Paglet, PagletContext, PagletState
from .client import HostClient
from .context_events import ContextEvent, ContextEventLog, ContextListener
from .envelope import PagletEnvelope
from .errors import HostError, InvalidAgentError, NotHandledError, PagletError, PagletInactiveError, RemoteHostError, ServiceNotFoundError, TransferError
from .events import CloneEvent, CreationEvent, MobilityEvent, PersistencyEvent
from .mailbox import MessageMailbox
from .mesh import HostRef, MeshRegistry
from .messages import DEACTIVATE, UNQUEUED_PRIORITY, Message, ReplySet
from .persistency import DeactivationPolicy, DeactivationRequest, InactiveRecord, QueuedMessage
from .proxy import PagletProxy
from .references import PagletProxyRef
from .resident import (
    DEFAULT_SERVICE_LEASE_TTL_SECONDS,
    RESIDENT_SERVICE_METADATA_KEY,
    ServiceLease,
)
from .resources import ResourceRegistry
from .runtime_values import (
    ArrivalMode,
    EnvelopeKind,
    LaunchConfigSyncAction,
    ResidentLifecycle,
    ServiceScope,
    enum_from_wire,
    require_enum,
)
from .serde import dataclass_from_wire, dataclass_to_wire, qualified_name, resolve_qualified_name
from .services import ServiceContract, ServiceHandle, ServiceRecord, ServiceRegistry
from .startup import LaunchConfig, LaunchConfigSyncResult, ResolvedResidentService, resolve_resident_service, resolve_startup_agent
from .storage import DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES, ManagedStorage
from .transfer import TransferTicket


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
]


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


class Host:
    """A paglet host/context served over a small JSON HTTP API.

    One process can run one host. For development, one Python process can also
    start multiple hosts on different ports. Migration always uses the same
    envelope model: class path + dataclass state + lifecycle metadata.
    """

    def __init__(
        self,
        name: str,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        client: HostClient | None = None,
        mesh: bool = True,
        peers: list[str] | None = None,
        mesh_multicast: bool = True,
        mesh_version: str | None = None,
        mesh_gossip_interval: float = 1.0,
        mesh_offline_after: float = 10.0,
        persistence_dir: str | Path | None = None,
        persistent_storage_quota_bytes: int | None = DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES,
        launch_config: LaunchConfig | None = None,
        launch_config_sync_result: LaunchConfigSyncResult | None = None,
    ):
        self.name = name
        self.bind_host = host
        self.port = int(port)
        self.address = f"http://{host}:{port}"
        self.client = client or HostClient()
        self._agents: dict[str, Paglet] = {}
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
        self._thread: threading.Thread | None = None
        self._activation_stop = threading.Event()
        self._activation_thread: threading.Thread | None = None
        self.mesh = MeshRegistry(
            self,
            enabled=mesh,
            peers=peers,
            code_version=mesh_version,
            multicast=mesh_multicast,
            gossip_interval=mesh_gossip_interval,
            offline_after=mesh_offline_after,
        )
        self._load_inactive_records()

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------
    def start_background(self) -> None:
        if self._server is not None:
            return
        self._clear_work_root()
        server = _PagletHTTPServer((self.bind_host, self.port), _RequestHandler, self)
        actual_host, actual_port = server.server_address[:2]
        public_host = self.bind_host if self.bind_host not in ("0.0.0.0", "") else "127.0.0.1"
        if self.port == 0:
            self.port = int(actual_port)
        self.address = f"http://{public_host}:{actual_port}"
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, name=f"paglets-{self.name}", daemon=True)
        self._thread.start()
        self._activation_stop.clear()
        self._emit_launch_config_sync_result()
        self._activate_startup_records()
        self._start_resident_services()
        self._start_launch_agents()
        self._start_activation_scheduler()
        self.mesh.start()
        self._emit("context-start")

    def serve_forever(self) -> None:
        self.start_background()
        assert self._thread is not None
        try:
            self._thread.join()
        except KeyboardInterrupt:  # pragma: no cover - CLI convenience
            self.shutdown()

    def shutdown(self) -> None:
        self.stop(deactivate_active=True)

    def stop(self, *, deactivate_active: bool = False) -> None:
        server = self._server
        if server is None:
            return
        self._stop_activation_scheduler()
        if deactivate_active:
            self._deactivate_active_for_shutdown()
        self.mesh.stop()
        self._emit("context-shutdown")
        server.shutdown()
        server.server_close()
        self._server = None
        thread = self._thread
        self._thread = None
        if thread is not None and thread.is_alive():
            thread.join(timeout=2)

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
        agent = agent_cls(state=state, agent_id=agent_id)
        self._register(agent)
        event = CreationEvent(
            agent_id=agent.agent_id,
            host_name=self.name,
            host_address=self.address,
            init=init,
        )
        agent.on_creation(event)
        agent.run()
        self._emit("create", agent_id=agent.agent_id, class_name=qualified_name(agent.__class__))
        return self._current_or_last_proxy(agent)

    def get_proxy(self, agent_id: str) -> PagletProxy | None:
        with self._lock:
            if agent_id not in self._agents:
                return None
        return PagletProxy(self.address, agent_id, self.client)

    def get_proxies(self, state: int = ACTIVE) -> list[PagletProxy]:
        proxies: list[PagletProxy] = []
        with self._lock:
            if state & ACTIVE:
                proxies.extend(PagletProxy(self.address, agent_id, self.client) for agent_id in self._agents)
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
        with self._lock:
            agent = self._agents.get(agent_id)
            if agent is None:
                raise InvalidAgentError(f"No active paglet {agent_id!r} on {self.name}")
            state = agent.state
        if not isinstance(state, state_cls):
            raise HostError(f"Paglet {agent_id!r} state is not {state_cls!r}")
        return state

    def resources_for(self, agent_id: str) -> ResourceRegistry:
        return self._require_agent(agent_id).resources

    def work_dir_for(self, agent_id: str, *, create: bool = True) -> Path:
        self._require_agent(agent_id)
        path = self._work_path(agent_id)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def persistent_storage_for(self, agent_id: str, *, quota_bytes: int | None = None) -> ManagedStorage:
        agent = self._require_agent(agent_id)
        quota = self.persistent_storage_quota_bytes if quota_bytes is None else quota_bytes
        return ManagedStorage(
            self._storage_root / self._storage_class_key(qualified_name(agent.__class__)),
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
            active_count = len(self._agents)
            inactive_count = len(self._inactive)
        return {
            "name": self.name,
            "address": self.address,
            "active_count": active_count,
            "inactive_count": inactive_count,
            "code_version": self.mesh.code_version,
            "capabilities": list(HOST_CAPABILITIES),
        }

    def list_hosts(self, *, online_only: bool = False, include_self: bool = True) -> list[HostRef]:
        return self.mesh.hosts(online_only=online_only, include_self=include_self)

    def join_mesh(self, payload: dict[str, Any]) -> list[HostRef]:
        self.mesh.register_wire(payload)
        return self.mesh.hosts(include_self=True)

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
                payload = self.client.get_json(f"{host_ref.url.rstrip('/')}/services{suffix}{separator}scope=mesh", timeout=2.0)
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
        response = self.client.post_json(
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
        agent = self._require_agent(agent_id)
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
        agent.on_dispatching(event)
        self._cleanup_agent_resources(agent, reason="dispatch")
        self._cleanup_agent_work_dir(agent_id)
        envelope = self._make_envelope(agent, EnvelopeKind.DISPATCH, target_info, ticket=ticket)
        response = self._post_envelope_with_ticket(ticket, target_info, envelope)
        self._remove_active_agent(agent_id, agent)
        self._emit("dispatch", agent_id=agent_id, class_name=qualified_name(agent.__class__), data={"target": target_info})
        return PagletProxy.from_wire(response["proxy"], self.client)

    def clone(self, agent_id: str, *, target: str | TransferTicket | None = None) -> PagletProxy:
        agent = self._require_agent(agent_id)
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
        agent.on_cloning(cloning_event)
        envelope = self._make_envelope(
            agent,
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
        agent.on_cloned(cloned_event)
        self._emit(
            "clone",
            agent_id=agent_id,
            class_name=qualified_name(agent.__class__),
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
        agent = self._require_agent(agent_id)
        request = request or DeactivationRequest()
        policy = agent.deactivation_policy(request)
        if not isinstance(policy, DeactivationPolicy):
            raise HostError(f"{agent.__class__.__name__}.deactivation_policy() must return DeactivationPolicy")
        event = PersistencyEvent(
            agent_id=agent_id,
            host_name=self.name,
            host_address=self.address,
            reason=request.reason,
            request=request,
            policy=policy,
        )
        agent.on_deactivating(event)
        self._cleanup_agent_resources(agent, reason="deactivate")
        info = {"name": self.name, "address": self.address}
        envelope = self._make_envelope(agent, EnvelopeKind.ACTIVATION, info)
        record = InactiveRecord(envelope=envelope, policy=policy, request=request)
        self._write_inactive_record(record)
        with self._lock:
            self._inactive[agent_id] = record
        self._remove_active_agent(agent_id, agent)
        self._emit("deactivate", agent_id=agent_id, class_name=qualified_name(agent.__class__), data={"reason": request.reason})
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
            agent = self._agents.get(agent_id)
            inactive = self._inactive.pop(agent_id, None)
        if agent is None:
            if inactive is None:
                raise InvalidAgentError(f"No paglet {agent_id!r} on {self.name}")
            self._delete_inactive_record(agent_id)
            self._cleanup_agent_work_dir(agent_id)
            self._emit("dispose", agent_id=agent_id, class_name=inactive.envelope.agent_class_name, data={"active": False})
            return
        event = PersistencyEvent(agent_id=agent_id, host_name=self.name, host_address=self.address, reason="dispose")
        agent.on_disposing(event)
        self._cleanup_agent_resources(agent, reason="dispose")
        self._cleanup_agent_work_dir(agent_id)
        self._remove_active_agent(agent_id, agent)
        if inactive is not None:
            self._delete_inactive_record(agent_id)
        self._emit("dispose", agent_id=agent_id, class_name=qualified_name(agent.__class__), data={"active": True})

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
            agent = self._agents.get(agent_id)
        if agent is None:
            error = InvalidAgentError(f"No active paglet {agent_id!r} on {self.name}")
            self._emit("message-failed", agent_id=agent_id, message_id=message.message_id, error=str(error))
            raise error
        self._begin_resident_service_call(agent_id)
        try:
            try:
                result = agent.handle_message(message)
            except Exception as exc:
                self._emit("message-failed", agent_id=agent_id, message_id=message.message_id, error=str(exc))
                raise
            if result is NOT_HANDLED:
                error = NotHandledError(f"{agent.__class__.__name__} did not handle {message.kind!r}")
                self._emit("message-failed", agent_id=agent_id, message_id=message.message_id, error=str(error))
                raise error
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
            message = Message.from_wire(kind.to_wire()) if isinstance(kind, Message) else Message(kind=kind, args=args or {}, sender=self.address)
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
        agent = self._require_agent(agent_id)
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
        agent.on_reverting(event)
        self._cleanup_agent_resources(agent, reason="retract")
        self._cleanup_agent_work_dir(agent_id)
        envelope = self._make_envelope(agent, EnvelopeKind.RETRACT, target_info)
        response = self.client.post_json(f"{target_info['address'].rstrip('/')}/agents", {"envelope": envelope.to_wire()})
        self._remove_active_agent(agent_id, agent)
        self._emit("retract", agent_id=agent_id, class_name=qualified_name(agent.__class__), data={"target": target_info})
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

        agent_cls = resolve_qualified_name(envelope.agent_class_name)
        state_cls = resolve_qualified_name(envelope.state_class_name)
        if not issubclass(agent_cls, Paglet):
            raise HostError(f"{envelope.agent_class_name} is not a Paglet subclass")
        if not is_dataclass(state_cls):
            raise HostError(f"{envelope.state_class_name} is not a dataclass state")
        state = dataclass_from_wire(state_cls, envelope.state)
        agent = agent_cls(state=state, agent_id=envelope.agent_id)
        self._register(agent)

        if envelope.kind in (EnvelopeKind.DISPATCH, EnvelopeKind.RETRACT):
            event = MobilityEvent(
                agent_id=agent.agent_id,
                host_name=self.name,
                host_address=self.address,
                source_host_name=envelope.source_host_name,
                source_host_address=envelope.source_host_address,
                target_host_name=self.name,
                target_host_address=self.address,
                reason=envelope.kind.value,
            )
            agent.on_arrival(event)
            agent.run()
            self._emit("arrival", agent_id=agent.agent_id, class_name=qualified_name(agent.__class__), data={"kind": envelope.kind.value})
        elif envelope.kind is EnvelopeKind.CLONE:
            event = CloneEvent(
                agent_id=agent.agent_id,
                host_name=self.name,
                host_address=self.address,
                source_agent_id=envelope.clone_of or "",
                clone_agent_id=agent.agent_id,
                source_host_name=envelope.source_host_name,
                source_host_address=envelope.source_host_address,
                target_host_name=self.name,
                target_host_address=self.address,
            )
            agent.on_clone(event)
            agent.run()
            self._emit("clone", agent_id=agent.agent_id, class_name=qualified_name(agent.__class__), data={"source_agent_id": envelope.clone_of})
        elif envelope.kind is EnvelopeKind.ACTIVATION:
            event = PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=self.name,
                host_address=self.address,
                reason="activate",
                request=inactive_record.request if inactive_record is not None else None,
                policy=inactive_record.policy if inactive_record is not None else None,
            )
            agent.on_activation(event)
            agent.run()
        else:  # pragma: no cover - typing already limits this
            raise HostError(f"Unknown envelope kind {envelope.kind!r}")

        return self._current_or_last_proxy(agent)

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
            info = self.health() if url == self.address.rstrip("/") else self.client.get_json(
                f"{url}/health",
                timeout=ticket.timeout,
            )
        except Exception as exc:
            self._emit("transfer-failed", data={"destination": url, "stage": "preflight"}, error=str(exc))
            raise TransferError(f"Could not preflight transfer target {url}: {exc}") from exc
        code_version = str(info.get("code_version") or "")
        if ticket.expected_code_version is not None and code_version != ticket.expected_code_version:
            message = (
                f"Transfer target {url} has code version {code_version!r}, "
                f"expected {ticket.expected_code_version!r}"
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
        url = f"{str(target_info['address']).rstrip('/')}/agents"
        attempts = max(0, ticket.retries) + 1
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                return self.client.post_json(url, {"envelope": envelope.to_wire()}, timeout=ticket.timeout)
            except Exception as exc:
                last_error = exc
                self._emit(
                    "transfer-failed",
                    agent_id=envelope.agent_id,
                    class_name=envelope.agent_class_name,
                    data={"destination": target_info["address"], "attempt": attempt + 1, "attempts": attempts},
                    error=str(exc),
                )
                if attempt + 1 < attempts:
                    time.sleep(max(0.0, ticket.retry_interval))
        raise TransferError(f"Transfer to {target_info['address']} failed after {attempts} attempt(s): {last_error}")

    def _cleanup_agent_resources(self, agent: Paglet, *, reason: str) -> None:
        agent.resources.cleanup(reason=reason)

    def _clear_work_root(self) -> None:
        try:
            shutil.rmtree(self._work_root)
        except FileNotFoundError:
            pass
        self._work_root.mkdir(parents=True, exist_ok=True)

    def _cleanup_agent_work_dir(self, agent_id: str) -> None:
        try:
            shutil.rmtree(self._work_path(agent_id))
        except FileNotFoundError:
            pass

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

    def _current_or_last_proxy(self, agent: Paglet) -> PagletProxy:
        proxy = self.get_proxy(agent.agent_id)
        if proxy is not None:
            return proxy
        last_proxy = getattr(agent, "_last_proxy", None)
        if last_proxy is not None:
            return last_proxy
        raise InvalidAgentError(f"Paglet {agent.agent_id!r} moved or disappeared without a proxy")

    def _register(self, agent: Paglet) -> None:
        agent._attach(PagletContext(self, agent.agent_id))
        try:
            mailbox_workers = int(getattr(agent, "MAILBOX_WORKERS", 4))
        except (TypeError, ValueError) as exc:
            raise HostError(f"{agent.__class__.__name__}.MAILBOX_WORKERS must be an integer") from exc
        if mailbox_workers < 1:
            raise HostError(f"{agent.__class__.__name__}.MAILBOX_WORKERS must be at least 1")
        mailbox = MessageMailbox(
            agent.agent_id,
            lambda message, oneway, agent_id=agent.agent_id: self._deliver_active_message(agent_id, message, oneway=oneway),
            max_workers=mailbox_workers,
        )
        with self._lock:
            old_mailbox = self._mailboxes.pop(agent.agent_id, None)
            self._agents[agent.agent_id] = agent
            self._mailboxes[agent.agent_id] = mailbox
        if old_mailbox is not None:
            old_mailbox.close()

    def _remove_active_agent(self, agent_id: str, expected: Paglet | None = None) -> None:
        with self._lock:
            current = self._agents.get(agent_id)
            if expected is not None and current is not expected:
                return
            self._agents.pop(agent_id, None)
            mailbox = self._mailboxes.pop(agent_id, None)
        if mailbox is not None:
            mailbox.close()
        for record in self._services.remove_agent(agent_id, keep=self._is_resident_service_record):
            self._emit("service-remove", agent_id=agent_id, service_name=record.name)

    def _require_agent(self, agent_id: str) -> Paglet:
        with self._lock:
            agent = self._agents.get(agent_id)
        if agent is None:
            raise InvalidAgentError(f"No active paglet {agent_id!r} on {self.name}")
        return agent

    def _make_envelope(
        self,
        agent: Paglet,
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
        with agent.locked_state() as state:
            state_payload = dataclass_to_wire(state)
        return PagletEnvelope(
            kind=kind,
            agent_id=agent_id or agent.agent_id,
            agent_class_name=qualified_name(agent.__class__),
            state_class_name=qualified_name(agent.state_class()),
            state=state_payload,
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

    def _summary(self, agent: Paglet) -> dict[str, Any]:
        mailbox = self._mailboxes.get(agent.agent_id)
        return {
            "agent_id": agent.agent_id,
            "class_name": qualified_name(agent.__class__),
            "state_class_name": qualified_name(agent.state_class()),
            "host": self.name,
            "address": self.address,
            "active": True,
            "mailbox": mailbox.status().to_wire() if mailbox is not None else None,
            "resources": agent.resources.status(),
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
            agent = self._agents.get(agent_id)
            mailbox = self._mailboxes.get(agent_id)
            inactive = self._inactive.get(agent_id)
        if agent is not None:
            with agent.locked_state() as state:
                state_payload = dataclass_to_wire(state)
            return {
                "agent_id": agent.agent_id,
                "class_name": qualified_name(agent.__class__),
                "state_class_name": qualified_name(agent.state_class()),
                "host": self.name,
                "address": self.address,
                "active": True,
                "state": state_payload,
                "mailbox": mailbox.status().to_wire() if mailbox is not None else None,
                "resources": agent.resources.status(),
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
        tmp_path.write_text(json.dumps(record.to_wire(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
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
            startup_ids = [
                agent_id
                for agent_id, record in self._inactive.items()
                if record.policy.activate_on_startup
            ]
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
                self._emit("startup-agent-skip", data={"reason": "disabled", "use": startup_agent.use, "class": startup_agent.class_name})
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
                record.policy.activate_at
                for record in self._inactive.values()
                if record.policy.activate_at is not None
            ]
        if not activate_at_values:
            return 1.0
        return max(0.05, min(1.0, min(activate_at_values) - time.time()))

    def _deactivate_active_for_shutdown(self) -> None:
        with self._lock:
            agent_ids = list(self._agents)
        for agent_id in agent_ids:
            with self._lock:
                if agent_id not in self._agents:
                    continue
            try:
                self.deactivate(
                    agent_id,
                    DeactivationRequest(
                        reason="shutdown",
                        source="host",
                        policy=self._resident_service_shutdown_policy(agent_id),
                    ),
                )
            except PagletError:
                continue

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


class _PagletHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], handler_cls: type[BaseHTTPRequestHandler], host_runtime: Host):
        super().__init__(server_address, handler_cls)
        self.host_runtime = host_runtime


class _RequestHandler(BaseHTTPRequestHandler):
    server: _PagletHTTPServer

    def log_message(self, format: str, *args: Any) -> None:  # keep examples/tests quiet
        return

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler name
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler name
        self._handle("POST")

    def _handle(self, method: str) -> None:
        try:
            payload = self._read_json() if method == "POST" else {}
            result = self._route(method, self.path, payload)
            self._write_json(200, result)
        except NotHandledError as exc:
            self._write_error(422, exc)
        except InvalidAgentError as exc:
            self._write_error(404, exc)
        except PagletError as exc:
            self._write_error(400, exc)
        except Exception as exc:  # pragma: no cover - defensive server boundary
            self._write_error(500, RemoteHostError(str(exc)))

    def _route(self, method: str, path: str, payload: dict[str, Any]) -> Any:
        host = self.server.host_runtime
        parsed = urlparse(path)
        parts = [part for part in parsed.path.split("/") if part]
        query = parse_qs(parsed.query)

        if method == "GET" and parts == ["health"]:
            return host.health()
        if method == "GET" and parts == ["hosts"]:
            return {"hosts": [ref.to_wire() for ref in host.list_hosts(include_self=True)]}
        if method == "POST" and parts == ["hosts", "join"]:
            return {"hosts": [ref.to_wire() for ref in host.join_mesh(payload)]}
        if method == "GET" and parts == ["events"]:
            since = int((query.get("since") or ["0"])[0])
            limit = int((query.get("limit") or ["100"])[0])
            return {"events": [event.to_wire() for event in host.list_events(since=since, limit=limit)]}
        if method == "GET" and parts == ["services"]:
            name = (query.get("name") or [None])[0]
            capability = (query.get("capability") or [None])[0]
            scope = enum_from_wire((query.get("scope") or [ServiceScope.LOCAL.value])[0], ServiceScope, "scope")
            if scope is ServiceScope.MESH:
                records = host._services.lookup_all(name, capability, scope=ServiceScope.MESH)
            else:
                records = host._services.lookup_all(name, capability)
            return {
                "services": [
                    record.to_wire()
                    for record in records
                ]
            }
        if method == "POST" and parts == ["services", "leases"]:
            return host.acquire_resident_service_lease(
                str(payload["agent_id"]),
                str(payload["service_name"]),
                ttl=float(payload.get("ttl", DEFAULT_SERVICE_LEASE_TTL_SECONDS)),
            )
        if method == "POST" and len(parts) == 4 and parts[:2] == ["services", "leases"] and parts[3] == "release":
            return host.release_resident_service_lease(parts[2])
        if method == "GET" and parts == ["agents"]:
            state = (query.get("state") or ["active"])[0]
            if state == "all":
                return {"agents": host.list_agents(active=True, inactive=True)}
            if state == "inactive":
                return {"agents": host.list_agents(active=False, inactive=True)}
            return {"agents": host.list_agents(active=True, inactive=False)}
        if method == "POST" and parts == ["agents"]:
            if "envelope" in payload:
                proxy = host._receive_envelope(PagletEnvelope.from_wire(payload["envelope"]))
            else:
                proxy = host._receive_creation(payload)
            return {"proxy": proxy.to_wire()}

        if len(parts) >= 2 and parts[0] == "agents":
            agent_id = parts[1]
            if method == "GET" and len(parts) == 3 and parts[2] == "state":
                return host._state_payload(agent_id)
            if method == "GET" and len(parts) == 2:
                with host._lock:
                    agent = host._agents.get(agent_id)
                    if agent is not None:
                        return host._summary(agent)
                    inactive = host._inactive.get(agent_id)
                    if inactive is not None:
                        return host._inactive_summary(inactive)
                raise InvalidAgentError(f"No paglet {agent_id!r} on {host.name}")
            if method == "POST" and len(parts) == 3:
                action = parts[2]
                if action == "messages":
                    message = Message.from_wire(payload["message"])
                    result = host.deliver_message(
                        agent_id,
                        message,
                        oneway=bool(payload.get("oneway", False)),
                        activate_if_inactive=bool(payload.get("activate_if_inactive", True)),
                        no_delay=bool(payload.get("no_delay", False)),
                    )
                    return {"result": result}
                if action == "dispatch":
                    target = TransferTicket.from_wire(payload["ticket"]) if "ticket" in payload else payload["target"]
                    proxy = host.dispatch(agent_id, target)
                    return {"proxy": proxy.to_wire()}
                if action == "clone":
                    if "ticket" in payload:
                        proxy = host.clone(agent_id, target=TransferTicket.from_wire(payload["ticket"]))
                    else:
                        proxy = host.clone(agent_id, target=payload.get("target"))
                    return {"proxy": proxy.to_wire()}
                if action == "retract":
                    proxy = host._retract_to(agent_id, payload["target"])
                    return {"proxy": proxy.to_wire()}
                if action == "deactivate":
                    proxy = host.deactivate(agent_id, DeactivationRequest.from_wire(payload.get("request")))
                    return {"proxy": proxy.to_wire(), "ok": True}
                if action == "activate":
                    proxy = host.activate(agent_id)
                    return {"proxy": proxy.to_wire()}
                if action == "dispose":
                    host.dispose(agent_id)
                    return {"ok": True}
                if action == "services":
                    record = host.advertise_service(
                        agent_id,
                        str(payload["name"]),
                        capabilities=payload.get("capabilities"),
                        metadata=payload.get("metadata"),
                        scope=enum_from_wire(
                            payload.get("scope") or ServiceScope.LOCAL.value,
                            ServiceScope,
                            "scope",
                        ),
                        ttl=float(payload["ttl"]) if payload.get("ttl") is not None else None,
                    )
                    return {"service": record.to_wire()}
                if action == "unadvertise-service":
                    removed = host.unadvertise_service(str(payload["name"]), agent_id=agent_id)
                    return {"services": [record.to_wire() for record in removed]}

        raise HostError(f"No route for {method} {path}")

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        return json.loads(raw)

    def _write_json(self, status: int, payload: Any) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _write_error(self, status: int, exc: Exception) -> None:
        self._write_json(status, {"error_type": exc.__class__.__name__, "error": str(exc)})
