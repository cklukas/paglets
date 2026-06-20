from __future__ import annotations

from dataclasses import is_dataclass
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import uuid
from typing import Any
from urllib.parse import parse_qs, urlparse

from .agent import ACTIVE, INACTIVE, NOT_HANDLED, Paglet, PagletContext, PagletState
from .client import HostClient
from .envelope import PagletEnvelope
from .errors import HostError, InvalidAgentError, NotHandledError, PagletError, RemoteHostError
from .events import CloneEvent, CreationEvent, MobilityEvent, PersistencyEvent
from .mesh import HostRef, MeshRegistry
from .messages import Message, ReplySet
from .proxy import PagletProxy
from .serde import dataclass_from_wire, dataclass_to_wire, qualified_name, resolve_qualified_name


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
    "hosts:list",
    "hosts:join",
]


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
    ):
        self.name = name
        self.bind_host = host
        self.port = int(port)
        self.address = f"http://{host}:{port}"
        self.client = client or HostClient()
        self._agents: dict[str, Paglet] = {}
        self._inactive: dict[str, PagletEnvelope] = {}
        self._properties: dict[str, Any] = {}
        self._lock = threading.RLock()
        self._server: _PagletHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.mesh = MeshRegistry(
            self,
            enabled=mesh,
            peers=peers,
            code_version=mesh_version,
            multicast=mesh_multicast,
            gossip_interval=mesh_gossip_interval,
            offline_after=mesh_offline_after,
        )

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------
    def start_background(self) -> None:
        if self._server is not None:
            return
        server = _PagletHTTPServer((self.bind_host, self.port), _RequestHandler, self)
        actual_host, actual_port = server.server_address[:2]
        public_host = self.bind_host if self.bind_host not in ("0.0.0.0", "") else "127.0.0.1"
        if self.port == 0:
            self.port = int(actual_port)
        self.address = f"http://{public_host}:{actual_port}"
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, name=f"paglets-{self.name}", daemon=True)
        self._thread.start()
        self.mesh.start()

    def serve_forever(self) -> None:
        self.start_background()
        assert self._thread is not None
        try:
            self._thread.join()
        except KeyboardInterrupt:  # pragma: no cover - CLI convenience
            self.stop()

    def stop(self) -> None:
        server = self._server
        if server is None:
            return
        self.mesh.stop()
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

    def list_agents(self, *, active: bool = True, inactive: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            agents = [self._summary(agent) for agent in self._agents.values()] if active else []
            if inactive:
                agents.extend(self._inactive_summary(envelope) for envelope in self._inactive.values())
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

    def dispatch(self, agent_id: str, target: str) -> PagletProxy:
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
            reason="dispatch",
        )
        agent.on_dispatching(event)
        envelope = self._make_envelope(agent, "dispatch", target_info)
        response = self.client.post_json(f"{target_info['address'].rstrip('/')}/agents", {"envelope": envelope.to_wire()})
        with self._lock:
            if self._agents.get(agent_id) is agent:
                self._agents.pop(agent_id, None)
        return PagletProxy.from_wire(response["proxy"], self.client)

    def clone(self, agent_id: str, *, target: str | None = None) -> PagletProxy:
        agent = self._require_agent(agent_id)
        target = target or self.address
        target_info = self._host_info(target)
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
        envelope = self._make_envelope(agent, "clone", target_info, agent_id=clone_id, clone_of=agent_id)
        response = self.client.post_json(f"{target_info['address'].rstrip('/')}/agents", {"envelope": envelope.to_wire()})
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
        return PagletProxy.from_wire(response["proxy"], self.client)

    def retract(self, remote_host_url: str, agent_id: str) -> PagletProxy:
        response = self.client.post_json(
            f"{remote_host_url.rstrip('/')}/agents/{agent_id}/retract",
            {"target": self.address},
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def deactivate(self, agent_id: str) -> None:
        agent = self._require_agent(agent_id)
        event = PersistencyEvent(agent_id=agent_id, host_name=self.name, host_address=self.address, reason="deactivate")
        agent.on_deactivating(event)
        info = {"name": self.name, "address": self.address}
        envelope = self._make_envelope(agent, "activation", info)
        with self._lock:
            self._inactive[agent_id] = envelope
            self._agents.pop(agent_id, None)

    def activate(self, agent_id: str) -> PagletProxy:
        with self._lock:
            envelope = self._inactive.pop(agent_id, None)
        if envelope is None:
            raise InvalidAgentError(f"No deactivated paglet {agent_id!r} on {self.name}")
        return self._receive_envelope(envelope)

    def dispose(self, agent_id: str) -> None:
        agent = self._require_agent(agent_id)
        event = PersistencyEvent(agent_id=agent_id, host_name=self.name, host_address=self.address, reason="dispose")
        agent.on_disposing(event)
        with self._lock:
            if self._agents.get(agent_id) is agent:
                self._agents.pop(agent_id, None)
            self._inactive.pop(agent_id, None)

    # ------------------------------------------------------------------
    # Message/lifecycle internals
    # ------------------------------------------------------------------
    def deliver_message(self, agent_id: str, message: Message, *, oneway: bool = False) -> Any:
        agent = self._require_agent(agent_id)
        result = agent.handle_message(message)
        if result is NOT_HANDLED:
            raise NotHandledError(f"{agent.__class__.__name__} did not handle {message.kind!r}")
        return None if oneway else result

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
            reply_set.add_future_reply(proxy.send_future_message(kind, args or {}, sender=self.address))
        return reply_set

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
        envelope = self._make_envelope(agent, "retract", target_info)
        response = self.client.post_json(f"{target_info['address'].rstrip('/')}/agents", {"envelope": envelope.to_wire()})
        with self._lock:
            if self._agents.get(agent_id) is agent:
                self._agents.pop(agent_id, None)
        return PagletProxy.from_wire(response["proxy"], self.client)

    def _receive_envelope(self, envelope: PagletEnvelope) -> PagletProxy:
        agent_cls = resolve_qualified_name(envelope.agent_class_name)
        state_cls = resolve_qualified_name(envelope.state_class_name)
        if not issubclass(agent_cls, Paglet):
            raise HostError(f"{envelope.agent_class_name} is not a Paglet subclass")
        if not is_dataclass(state_cls):
            raise HostError(f"{envelope.state_class_name} is not a dataclass state")
        state = dataclass_from_wire(state_cls, envelope.state)
        agent = agent_cls(state=state, agent_id=envelope.agent_id)
        self._register(agent)

        if envelope.kind in ("dispatch", "retract"):
            event = MobilityEvent(
                agent_id=agent.agent_id,
                host_name=self.name,
                host_address=self.address,
                source_host_name=envelope.source_host_name,
                source_host_address=envelope.source_host_address,
                target_host_name=self.name,
                target_host_address=self.address,
                reason=envelope.kind,
            )
            agent.on_arrival(event)
            agent.run()
        elif envelope.kind == "clone":
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
        elif envelope.kind == "activation":
            event = PersistencyEvent(agent_id=agent.agent_id, host_name=self.name, host_address=self.address, reason="activate")
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

    def _current_or_last_proxy(self, agent: Paglet) -> PagletProxy:
        proxy = self.get_proxy(agent.agent_id)
        if proxy is not None:
            return proxy
        last_proxy = getattr(agent, "_last_proxy", None)
        if last_proxy is not None:
            return last_proxy
        raise InvalidAgentError(f"Paglet {agent.agent_id!r} moved or disappeared without a proxy")

    def _register(self, agent: Paglet) -> None:
        agent._attach(PagletContext(self))
        with self._lock:
            self._agents[agent.agent_id] = agent

    def _require_agent(self, agent_id: str) -> Paglet:
        with self._lock:
            agent = self._agents.get(agent_id)
        if agent is None:
            raise InvalidAgentError(f"No active paglet {agent_id!r} on {self.name}")
        return agent

    def _make_envelope(
        self,
        agent: Paglet,
        kind: str,
        target_info: dict[str, str],
        *,
        agent_id: str | None = None,
        clone_of: str | None = None,
    ) -> PagletEnvelope:
        return PagletEnvelope(
            kind=kind,  # type: ignore[arg-type]
            agent_id=agent_id or agent.agent_id,
            agent_class_name=qualified_name(agent.__class__),
            state_class_name=qualified_name(agent.state_class()),
            state=dataclass_to_wire(agent.state),
            source_host_name=self.name,
            source_host_address=self.address,
            target_host_name=target_info["name"],
            target_host_address=target_info["address"],
            clone_of=clone_of,
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
        return {
            "agent_id": agent.agent_id,
            "class_name": qualified_name(agent.__class__),
            "state_class_name": qualified_name(agent.state_class()),
            "host": self.name,
            "address": self.address,
            "active": True,
        }

    def _inactive_summary(self, envelope: PagletEnvelope) -> dict[str, Any]:
        return {
            "agent_id": envelope.agent_id,
            "class_name": envelope.agent_class_name,
            "state_class_name": envelope.state_class_name,
            "host": self.name,
            "address": self.address,
            "active": False,
        }

    def _state_payload(self, agent_id: str) -> dict[str, Any]:
        with self._lock:
            agent = self._agents.get(agent_id)
            if agent is not None:
                return {
                    "agent_id": agent.agent_id,
                    "class_name": qualified_name(agent.__class__),
                    "state_class_name": qualified_name(agent.state_class()),
                    "host": self.name,
                    "address": self.address,
                    "active": True,
                    "state": dataclass_to_wire(agent.state),
                }
            inactive = self._inactive.get(agent_id)
            if inactive is not None:
                return {
                    "agent_id": inactive.agent_id,
                    "class_name": inactive.agent_class_name,
                    "state_class_name": inactive.state_class_name,
                    "host": self.name,
                    "address": self.address,
                    "active": False,
                    "state": inactive.state,
                }
        raise InvalidAgentError(f"No paglet {agent_id!r} on {self.name}")


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
                    result = host.deliver_message(agent_id, message, oneway=bool(payload.get("oneway", False)))
                    return {"result": result}
                if action == "dispatch":
                    proxy = host.dispatch(agent_id, payload["target"])
                    return {"proxy": proxy.to_wire()}
                if action == "clone":
                    proxy = host.clone(agent_id, target=payload.get("target"))
                    return {"proxy": proxy.to_wire()}
                if action == "retract":
                    proxy = host._retract_to(agent_id, payload["target"])
                    return {"proxy": proxy.to_wire()}
                if action == "deactivate":
                    host.deactivate(agent_id)
                    return {"ok": True}
                if action == "activate":
                    proxy = host.activate(agent_id)
                    return {"proxy": proxy.to_wire()}
                if action == "dispose":
                    host.dispose(agent_id)
                    return {"ok": True}

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
