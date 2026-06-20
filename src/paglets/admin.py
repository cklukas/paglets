# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
from time import perf_counter
from typing import Any
from urllib.parse import urlencode, urlparse

from .client import HostClient
from .errors import RemoteHostError
from .mesh import HostRef
from .messages import Message


DEFAULT_CONFIG_PATH = Path.home() / ".paglets" / "servers.json"


@dataclass(frozen=True, slots=True)
class ServerRef:
    name: str
    url: str
    enabled: bool = True
    local_start: bool = False


@dataclass(slots=True)
class AgentDiscoveryConfig:
    paths: list[str]
    modules: list[str]


@dataclass(slots=True)
class TuiConfig:
    servers: list[ServerRef]
    agent_discovery: AgentDiscoveryConfig


@dataclass(frozen=True, slots=True)
class ServerStatus:
    name: str
    url: str
    reachable: bool
    latency_ms: float
    active_count: int
    inactive_count: int
    error: str | None = None
    code_version: str | None = None


@dataclass(frozen=True, slots=True)
class AgentRecord:
    server_name: str
    host_url: str
    agent_id: str
    class_name: str
    state_class_name: str
    active: bool


@dataclass(frozen=True, slots=True)
class ServiceSummary:
    server_name: str
    name: str
    host_url: str
    agent_id: str
    capabilities: tuple[str, ...]
    scope: str


def normalize_server_url(url: str) -> str:
    normalized = url.strip().rstrip("/")
    if not normalized:
        raise ValueError("Server URL cannot be empty")
    if "://" not in normalized:
        normalized = f"http://{normalized}"
    return normalized


def normalize_discovery_path(path: str, *, base_path: Path | str | None = None) -> str:
    value = path.strip()
    if not value:
        raise ValueError("Discovery path cannot be empty")
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        base = Path.cwd() if base_path is None else Path(base_path)
        candidate = base / candidate
    return str(candidate.resolve(strict=False))


def default_agent_discovery_config() -> AgentDiscoveryConfig:
    repo_root = Path(__file__).resolve().parents[2]
    examples_path = repo_root / "examples"
    paths = [str(examples_path)] if examples_path.exists() else []
    return AgentDiscoveryConfig(paths=paths, modules=[])


def is_local_server_url(url: str) -> bool:
    parsed = urlparse(normalize_server_url(url))
    host = (parsed.hostname or "").lower()
    return host in {"localhost", "127.0.0.1", "::1"}


def can_start_local_server(server: ServerRef) -> bool:
    return is_local_server_url(server.url)


def local_server_command(server: ServerRef, *, peers: list[str] | None = None) -> list[str]:
    parsed = urlparse(normalize_server_url(server.url))
    if not is_local_server_url(server.url):
        raise ValueError(f"Server {server.name!r} is not a local server URL")
    if parsed.port is None:
        raise ValueError(f"Server {server.name!r} URL must include a port to start locally")
    host = parsed.hostname or "127.0.0.1"
    if host == "localhost":
        host = "127.0.0.1"
    command = [
        sys.executable,
        "-m",
        "paglets.cli",
        "--name",
        server.name,
        "--host",
        host,
        "--port",
        str(parsed.port),
    ]
    for peer in peers or []:
        normalized_peer = normalize_server_url(peer)
        if normalized_peer != normalize_server_url(server.url):
            command.extend(["--peer", normalized_peer])
    return command


def start_local_server(server: ServerRef, *, peers: list[str] | None = None) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        local_server_command(server, peers=peers),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def parse_server_arg(value: str) -> ServerRef:
    if "=" not in value:
        raise ValueError("Expected server in NAME=URL format")
    name, url = value.split("=", 1)
    normalized_url = normalize_server_url(url)
    return ServerRef(
        name=name.strip(),
        url=normalized_url,
        enabled=True,
        local_start=is_local_server_url(normalized_url),
    )


def load_server_config(path: Path | str = DEFAULT_CONFIG_PATH) -> list[ServerRef]:
    config_path = Path(path)
    if not config_path.exists():
        return []
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return _server_refs_from_payload(payload, config_path)


def load_tui_config(
    path: Path | str = DEFAULT_CONFIG_PATH,
    *,
    base_path: Path | str | None = None,
) -> TuiConfig:
    config_path = Path(path)
    if not config_path.exists():
        return TuiConfig(servers=[], agent_discovery=default_agent_discovery_config())
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return TuiConfig(
        servers=_server_refs_from_payload(payload, config_path),
        agent_discovery=_discovery_config_from_payload(payload, base_path=base_path),
    )


def _server_refs_from_payload(payload: dict[str, Any], config_path: Path) -> list[ServerRef]:
    servers = payload.get("servers", [])
    if not isinstance(servers, list):
        raise ValueError(f"{config_path} must contain a 'servers' list")
    refs: list[ServerRef] = []
    for item in servers:
        if not isinstance(item, dict):
            raise ValueError(f"Invalid server entry in {config_path}: {item!r}")
        url = normalize_server_url(str(item["url"]))
        refs.append(
            ServerRef(
                name=str(item["name"]),
                url=url,
                enabled=bool(item.get("enabled", True)),
                local_start=bool(item.get("local_start", is_local_server_url(url)))
                and is_local_server_url(url),
            )
        )
    return refs


def _discovery_config_from_payload(
    payload: dict[str, Any],
    *,
    base_path: Path | str | None = None,
) -> AgentDiscoveryConfig:
    discovery = payload.get("agent_discovery")
    if discovery is None:
        return default_agent_discovery_config()
    if not isinstance(discovery, dict):
        raise ValueError("'agent_discovery' must be an object")
    raw_paths = discovery.get("paths", [])
    raw_modules = discovery.get("modules", [])
    if not isinstance(raw_paths, list) or not isinstance(raw_modules, list):
        raise ValueError("'agent_discovery.paths' and 'agent_discovery.modules' must be lists")
    return AgentDiscoveryConfig(
        paths=[normalize_discovery_path(str(item), base_path=base_path) for item in raw_paths],
        modules=[str(item).strip() for item in raw_modules if str(item).strip()],
    )


def save_server_config(servers: list[ServerRef], path: Path | str = DEFAULT_CONFIG_PATH) -> None:
    save_tui_config(
        TuiConfig(servers=servers, agent_discovery=AgentDiscoveryConfig(paths=[], modules=[])),
        path,
        include_agent_discovery=False,
    )


def save_tui_config(
    config: TuiConfig,
    path: Path | str = DEFAULT_CONFIG_PATH,
    *,
    include_agent_discovery: bool = True,
) -> None:
    config_path = Path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "servers": [
            {
                "name": server.name,
                "url": normalize_server_url(server.url),
                "enabled": server.enabled,
                "local_start": (
                    bool(server.local_start or is_local_server_url(server.url))
                    and is_local_server_url(server.url)
                ),
            }
            for server in config.servers
        ]
    }
    if include_agent_discovery:
        payload["agent_discovery"] = {
            "paths": list(config.agent_discovery.paths),
            "modules": list(config.agent_discovery.modules),
        }
    config_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def add_server_ref(servers: list[ServerRef], server: ServerRef) -> list[ServerRef]:
    if any(existing.name == server.name for existing in servers):
        raise ValueError(f"Duplicate server name {server.name!r}")
    return [*servers, server]


def upsert_server_ref(servers: list[ServerRef], server: ServerRef) -> list[ServerRef]:
    replaced = False
    updated: list[ServerRef] = []
    for existing in servers:
        if existing.name == server.name:
            updated.append(server)
            replaced = True
        else:
            updated.append(existing)
    if not replaced:
        updated.append(server)
    return updated


def remove_server_ref(servers: list[ServerRef], name: str) -> list[ServerRef]:
    return [server for server in servers if server.name != name]


class PagletsAdminClient:
    def __init__(self, servers: list[ServerRef], *, client: HostClient | None = None):
        self.servers = list(servers)
        self.client = client or HostClient(timeout=2.0)

    def enabled_servers(self) -> list[ServerRef]:
        return [server for server in self.servers if server.enabled]

    def set_servers(self, servers: list[ServerRef]) -> None:
        self.servers = list(servers)

    def get_server(self, name_or_url: str) -> ServerRef:
        target = name_or_url.strip().rstrip("/")
        for server in self.servers:
            if server.name == target or server.url.rstrip("/") == target:
                return server
        return ServerRef(name=target, url=normalize_server_url(target), enabled=True)

    def health_all(self) -> list[ServerStatus]:
        return [self.health(server) for server in self.enabled_servers()]

    def health(self, server: ServerRef) -> ServerStatus:
        started = perf_counter()
        try:
            payload = self.client.get_json(f"{server.url}/health")
            latency_ms = (perf_counter() - started) * 1000
            return ServerStatus(
                name=server.name,
                url=str(payload.get("address") or server.url),
                reachable=True,
                latency_ms=latency_ms,
                active_count=int(payload.get("active_count", 0)),
                inactive_count=int(payload.get("inactive_count", 0)),
                code_version=str(payload["code_version"]) if payload.get("code_version") else None,
            )
        except Exception as exc:
            latency_ms = (perf_counter() - started) * 1000
            return ServerStatus(
                name=server.name,
                url=server.url,
                reachable=False,
                latency_ms=latency_ms,
                active_count=0,
                inactive_count=0,
                error=str(exc),
            )

    def list_agents_all(self) -> list[AgentRecord]:
        agents: list[AgentRecord] = []
        for server in self.enabled_servers():
            try:
                agents.extend(self.list_agents(server))
            except RemoteHostError:
                continue
        return agents

    def list_agents(self, server: ServerRef) -> list[AgentRecord]:
        payload = self.client.get_json(f"{server.url}/agents?state=all")
        records: list[AgentRecord] = []
        for item in payload.get("agents", []):
            records.append(
                AgentRecord(
                    server_name=server.name,
                    host_url=str(item.get("address") or server.url),
                    agent_id=str(item["agent_id"]),
                    class_name=str(item["class_name"]),
                    state_class_name=str(item["state_class_name"]),
                    active=bool(item["active"]),
                )
            )
        return records

    def list_hosts(self, server: ServerRef) -> list[HostRef]:
        payload = self.client.get_json(f"{server.url}/hosts")
        hosts: list[HostRef] = []
        for item in payload.get("hosts", []):
            if isinstance(item, dict):
                hosts.append(HostRef.from_wire(item))
        return hosts

    def send(
        self,
        agent: AgentRecord,
        message: Message,
        *,
        oneway: bool = False,
        activate_if_inactive: bool = True,
        no_delay: bool = False,
    ) -> Any:
        payload = {
            "message": message.to_wire(),
            "oneway": oneway,
            "activate_if_inactive": activate_if_inactive,
            "no_delay": no_delay,
        }
        response = self.client.post_json(
            f"{agent.host_url.rstrip('/')}/agents/{agent.agent_id}/messages",
            payload,
        )
        return response.get("result")

    def list_services(
        self,
        server: ServerRef,
        *,
        name: str | None = None,
        capability: str | None = None,
        scope: str = "local",
    ) -> list[ServiceSummary]:
        query: dict[str, str] = {}
        if name:
            query["name"] = name
        if capability:
            query["capability"] = capability
        if scope:
            query["scope"] = scope
        suffix = f"?{urlencode(query)}" if query else ""
        payload = self.client.get_json(f"{server.url}/services{suffix}")
        services: list[ServiceSummary] = []
        for item in payload.get("services", []):
            proxy = item.get("proxy") or {}
            services.append(
                ServiceSummary(
                    server_name=server.name,
                    name=str(item["name"]),
                    host_url=str(proxy.get("host_url") or item.get("host_url") or server.url),
                    agent_id=str(proxy.get("agent_id") or ""),
                    capabilities=tuple(str(value) for value in item.get("capabilities", [])),
                    scope=str(item.get("scope") or "local"),
                )
            )
        return services

    def list_events(self, server: ServerRef, *, since: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        payload = self.client.get_json(f"{server.url}/events?{urlencode({'since': since, 'limit': limit})}")
        return [dict(item) for item in payload.get("events", [])]

    def create_agent(
        self,
        server: ServerRef,
        agent_class_name: str,
        state_class_name: str,
        state: dict[str, Any] | None = None,
        *,
        init: Any = None,
    ) -> dict[str, str]:
        response = self.client.post_json(
            f"{server.url}/agents",
            {
                "agent_class_name": agent_class_name,
                "state_class_name": state_class_name,
                "state": state or {},
                "init": init,
            },
        )
        return dict(response["proxy"])

    def dispatch(self, agent: AgentRecord, target_url: str) -> dict[str, str]:
        return self._agent_action(agent, "dispatch", {"target": normalize_server_url(target_url)})

    def clone(self, agent: AgentRecord, target_url: str | None = None) -> dict[str, str]:
        payload = {"target": normalize_server_url(target_url)} if target_url else {"target": None}
        return self._agent_action(agent, "clone", payload)

    def retract(self, agent: AgentRecord, target_url: str) -> dict[str, str]:
        return self._agent_action(agent, "retract", {"target": normalize_server_url(target_url)})

    def activate(self, agent: AgentRecord) -> dict[str, str]:
        return self._agent_action(agent, "activate", {})

    def deactivate(self, agent: AgentRecord) -> None:
        self._agent_action(agent, "deactivate", {})

    def dispose(self, agent: AgentRecord) -> None:
        self._agent_action(agent, "dispose", {})

    def get_agent_info(self, agent: AgentRecord) -> dict[str, Any]:
        return self.client.get_json(f"{agent.host_url.rstrip('/')}/agents/{agent.agent_id}")

    def get_agent_state(self, agent: AgentRecord) -> dict[str, Any]:
        return self.client.get_json(f"{agent.host_url.rstrip('/')}/agents/{agent.agent_id}/state")

    def _agent_action(self, agent: AgentRecord, action: str, payload: dict[str, Any]) -> dict[str, str]:
        response = self.client.post_json(
            f"{agent.host_url.rstrip('/')}/agents/{agent.agent_id}/{action}",
            payload,
        )
        proxy = response.get("proxy")
        return dict(proxy) if proxy else {}
