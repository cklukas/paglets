# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import ipaddress
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import urlencode

from paglets.config.env import DEFAULT_API_KEY_ENV
from paglets.core.errors import AuthenticationError, RemoteHostError
from paglets.core.messages import Message
from paglets.core.runtime_values import ServiceScope, enum_from_wire, require_enum
from paglets.remote.client import HostClient
from paglets.remote.mesh import MESH_MULTICAST_GROUP, MESH_MULTICAST_PORT, HostRef, decode_mesh_beacon

DEFAULT_LAN_DISCOVERY_TIMEOUT_SECONDS = 0.25
DEFAULT_LAN_DISCOVERY_WORKERS = 64


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
    scope: ServiceScope


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
    repo_root = Path(__file__).resolve().parents[3]
    demos_path = repo_root / "demos"
    paths = [str(demos_path)] if demos_path.exists() else []
    return AgentDiscoveryConfig(paths=paths, modules=[])


def detect_lan_host() -> str:
    """Return the local IPv4 address used for default-route traffic."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            address = sock.getsockname()[0]
            if address and not address.startswith("127."):
                return address
    except OSError:
        pass

    try:
        hostname = socket.gethostname()
        for family, _type, _proto, _canonname, sockaddr in socket.getaddrinfo(hostname, None, socket.AF_INET):
            if family != socket.AF_INET:
                continue
            address = sockaddr[0]
            if address and not address.startswith("127."):
                return address
    except OSError:
        pass
    return "127.0.0.1"


def select_reachable_entry_server(
    *,
    entry_name: str | None,
    client: HostClient,
    timeout: float = 1.0,
) -> ServerRef:
    if entry_name is not None and "://" in entry_name:
        url = normalize_server_url(entry_name)
        try:
            health = client.get_json(f"{url.rstrip('/')}/health", timeout=timeout)
        except AuthenticationError as exc:
            raise ValueError(
                f"Entry server requires authentication; set {DEFAULT_API_KEY_ENV} or pass --api-key-env with an "
                "environment variable containing a Paglets bearer API key"
            ) from exc
        return ServerRef(
            name=str(health.get("name") or url),
            url=normalize_server_url(str(health.get("address") or url)),
            enabled=True,
            local_start=False,
        )
    entry_candidates = _ambient_entry_candidates()
    tried: list[str] = []
    auth_errors: list[str] = []
    for candidate in _dedupe_servers(entry_candidates):
        tried.append(candidate.url)
        try:
            health = client.get_json(f"{candidate.url.rstrip('/')}/health", timeout=timeout)
        except AuthenticationError as exc:
            auth_errors.append(str(exc))
            continue
        except Exception:
            continue
        selected = ServerRef(
            name=str(health.get("name") or candidate.name),
            url=normalize_server_url(str(health.get("address") or candidate.url)),
            enabled=True,
            local_start=False,
        )
        if entry_name is not None and selected.name != entry_name:
            continue
        return selected
    tried_text = ", ".join(tried) if tried else "none"
    if auth_errors:
        raise ValueError(
            f"Entry server requires authentication; set {DEFAULT_API_KEY_ENV} or pass --api-key-env with an "
            "environment variable containing a Paglets bearer API key"
        )
    if entry_name is not None:
        raise ValueError(f"No reachable entry server named {entry_name!r} found; tried {tried_text}")
    raise ValueError(f"No reachable entry server found; tried {tried_text}")


def _ambient_entry_candidates() -> list[ServerRef]:
    ports = {8765}
    candidates: list[ServerRef] = []
    lan_host = detect_lan_host()
    for port in sorted(ports):
        candidates.append(ServerRef("local", f"http://127.0.0.1:{port}", enabled=True, local_start=True))
        if not lan_host.startswith("127."):
            candidates.append(ServerRef("local-lan", f"http://{lan_host}:{port}", enabled=True, local_start=False))
    candidates.extend(discover_mesh_entry_servers(timeout=0.75))
    candidates.extend(discover_lan_entry_servers(ports=ports))
    return candidates


def discover_mesh_entry_servers(*, timeout: float = 0.75) -> list[ServerRef]:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", MESH_MULTICAST_PORT))
        group = socket.inet_aton(MESH_MULTICAST_GROUP)
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            group + socket.inet_aton("0.0.0.0"),
        )
        sock.settimeout(0.1)
    except OSError:
        return []

    discovered: list[ServerRef] = []
    deadline = perf_counter() + max(0.0, timeout)
    with sock:
        while perf_counter() < deadline:
            try:
                data, _addr = sock.recvfrom(65535)
            except TimeoutError:
                continue
            except OSError:
                break
            ref = decode_mesh_beacon(data)
            if ref is None or not ref.online:
                continue
            discovered.append(ServerRef(ref.name, ref.url, enabled=True, local_start=False))
    return _dedupe_servers(discovered)


def discover_lan_entry_servers(
    *,
    ports: set[int] | list[int] | tuple[int, ...] | None = None,
    timeout: float = DEFAULT_LAN_DISCOVERY_TIMEOUT_SECONDS,
    workers: int = DEFAULT_LAN_DISCOVERY_WORKERS,
) -> list[ServerRef]:
    lan_host = detect_lan_host()
    try:
        address = ipaddress.ip_address(lan_host)
    except ValueError:
        return []
    if not isinstance(address, ipaddress.IPv4Address) or address.is_loopback:
        return []

    network = ipaddress.ip_network(f"{lan_host}/24", strict=False)
    probe_ports = sorted({int(port) for port in (ports or {8765}) if int(port) > 0})
    if not probe_ports:
        return []

    targets = [(str(candidate), port) for candidate in network.hosts() if candidate != address for port in probe_ports]
    if not targets:
        return []

    client = HostClient(timeout=timeout)
    discovered: list[ServerRef] = []
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        futures = [executor.submit(_probe_entry_server, client, host, port, timeout) for host, port in targets]
        for future in as_completed(futures):
            ref = future.result()
            if ref is not None:
                discovered.append(ref)
    return _dedupe_servers(discovered)


def _probe_entry_server(client: HostClient, host: str, port: int, timeout: float) -> ServerRef | None:
    url = f"http://{host}:{port}"
    try:
        health = client.get_json(f"{url}/health", timeout=timeout)
    except Exception:
        return None
    if not isinstance(health, dict):
        return None
    address = normalize_server_url(str(health.get("address") or url))
    return ServerRef(
        name=str(health.get("name") or host),
        url=address,
        enabled=True,
        local_start=False,
    )


def _dedupe_servers(servers: list[ServerRef]) -> list[ServerRef]:
    result: list[ServerRef] = []
    seen: set[str] = set()
    for server in servers:
        try:
            key = normalize_server_url(server.url).rstrip("/")
        except ValueError:
            continue
        if key in seen:
            continue
        seen.add(key)
        result.append(
            ServerRef(
                name=server.name,
                url=key,
                enabled=server.enabled,
                local_start=server.local_start,
            )
        )
    return result


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
        payloads = self.list_agent_payloads(server)
        records: list[AgentRecord] = []
        for item in payloads:
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

    def list_agent_payloads(self, server: ServerRef, *, include_state: bool = False) -> list[dict[str, Any]]:
        query = "state=all"
        if include_state:
            query += "&include_state=true"
        payload = self.client.get_json(f"{server.url}/agents?{query}")
        agents: list[dict[str, Any]] = []
        for item in payload.get("agents", []):
            if not isinstance(item, dict):
                continue
            enriched = dict(item)
            enriched["server_name"] = server.name
            enriched["host_url"] = str(item.get("address") or server.url)
            agents.append(enriched)
        return agents

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
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> list[ServiceSummary]:
        require_enum(scope, ServiceScope, "scope")
        query: dict[str, str] = {}
        if name:
            query["name"] = name
        if capability:
            query["capability"] = capability
        if scope:
            query["scope"] = scope.value
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
                    scope=enum_from_wire(item.get("scope") or ServiceScope.LOCAL.value, ServiceScope, "scope"),
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
