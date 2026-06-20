# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
import importlib.metadata
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from .errors import HostError

if TYPE_CHECKING:  # pragma: no cover
    from .host import Host


MESH_BEACON_KIND = "paglets.mesh.v1"
MESH_MULTICAST_GROUP = "239.42.74.53"
MESH_MULTICAST_PORT = 48765


@dataclass(frozen=True, slots=True)
class HostRef:
    name: str
    url: str
    code_version: str
    online: bool
    last_seen: float
    active_count: int
    inactive_count: int
    error: str | None = None

    def to_wire(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "url": self.url.rstrip("/"),
            "code_version": self.code_version,
            "online": self.online,
            "last_seen": self.last_seen,
            "active_count": self.active_count,
            "inactive_count": self.inactive_count,
        }
        if self.error:
            payload["error"] = self.error
        return payload

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "HostRef":
        return cls(
            name=str(payload["name"]),
            url=normalize_host_url(str(payload.get("url") or payload.get("address") or "")),
            code_version=str(payload["code_version"]),
            online=bool(payload.get("online", True)),
            last_seen=float(payload.get("last_seen", time.time())),
            active_count=int(payload.get("active_count", 0)),
            inactive_count=int(payload.get("inactive_count", 0)),
            error=str(payload["error"]) if payload.get("error") else None,
        )


def normalize_host_url(url: str) -> str:
    value = url.strip().rstrip("/")
    if not value:
        raise ValueError("Host URL cannot be empty")
    if "://" not in value:
        value = f"http://{value}"
    return value


def encode_mesh_beacon(ref: HostRef) -> bytes:
    payload = {"kind": MESH_BEACON_KIND, "host": ref.to_wire()}
    return json.dumps(payload, sort_keys=True).encode("utf-8")


def decode_mesh_beacon(data: bytes) -> HostRef | None:
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or payload.get("kind") != MESH_BEACON_KIND:
        return None
    host_payload = payload.get("host")
    if not isinstance(host_payload, dict):
        return None
    try:
        return HostRef.from_wire(host_payload)
    except (KeyError, TypeError, ValueError):
        return None


def resolve_code_version(override: str | None = None) -> tuple[str, str | None]:
    if override:
        return override, None
    env_value = os.environ.get("PAGLETS_MESH_VERSION")
    if env_value:
        return env_value, None

    repo_root = Path(__file__).resolve().parents[2]
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=1,
        )
        version = result.stdout.strip()
        if version:
            return version, None
    except (OSError, subprocess.SubprocessError):
        pass

    try:
        package_version = importlib.metadata.version("paglets")
    except importlib.metadata.PackageNotFoundError:
        package_version = "0.0.0"
    version = f"paglets-{package_version}-no-git"
    return version, f"mesh code version fell back to {version!r}; use --mesh-version for deterministic meshes"


class MeshRegistry:
    """Version-gated host registry owned by a paglets host."""

    def __init__(
        self,
        host_runtime: "Host",
        *,
        enabled: bool = True,
        peers: list[str] | None = None,
        code_version: str | None = None,
        multicast: bool = True,
        gossip_interval: float = 1.0,
        offline_after: float = 10.0,
    ):
        self._host = host_runtime
        resolved_version, warning = resolve_code_version(code_version)
        self.code_version = resolved_version
        self.version_warning = warning
        self.enabled = enabled
        self.multicast = multicast
        self.gossip_interval = max(0.05, float(gossip_interval))
        self.offline_after = max(0.1, float(offline_after))
        self._seeds = {normalize_host_url(peer) for peer in peers or []}
        self._hosts: dict[str, HostRef] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        self._stop.clear()
        self.refresh_self()
        if not self.enabled:
            return
        self.gossip_once()
        self._threads = [
            threading.Thread(target=self._gossip_loop, name=f"paglets-mesh-{self._host.name}", daemon=True)
        ]
        if self.multicast:
            self._threads.extend(
                [
                    threading.Thread(
                        target=self._multicast_send_loop,
                        name=f"paglets-mesh-beacon-{self._host.name}",
                        daemon=True,
                    ),
                    threading.Thread(
                        target=self._multicast_receive_loop,
                        name=f"paglets-mesh-listen-{self._host.name}",
                        daemon=True,
                    ),
                ]
            )
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=1)
        self._threads = []

    def add_seed(self, url: str) -> None:
        normalized = normalize_host_url(url)
        if normalized != self._host.address.rstrip("/"):
            self._seeds.add(normalized)

    def hosts(self, *, online_only: bool = False, include_self: bool = True) -> list[HostRef]:
        self.refresh_self()
        self._expire_stale_hosts()
        with self._lock:
            refs = list(self._hosts.values())
        if not include_self:
            self_url = self._host.address.rstrip("/")
            refs = [ref for ref in refs if ref.url.rstrip("/") != self_url]
        if online_only:
            refs = [ref for ref in refs if ref.online]
        return sorted(refs, key=lambda ref: (ref.name, ref.url))

    def lookup(self, name_or_url: str) -> HostRef | None:
        target = name_or_url.strip().rstrip("/")
        if not target:
            return None
        self.refresh_self()
        self._expire_stale_hosts()
        with self._lock:
            for ref in self._hosts.values():
                if ref.name == target or ref.url.rstrip("/") == target:
                    return ref
                try:
                    if ref.url.rstrip("/") == normalize_host_url(target):
                        return ref
                except ValueError:
                    continue
        return None

    def resolve_url(self, name_or_url: str) -> str:
        ref = self.lookup(name_or_url)
        if ref is not None:
            return ref.url
        return normalize_host_url(name_or_url)

    def is_online(self, name_or_url: str) -> bool:
        ref = self.lookup(name_or_url)
        return bool(ref and ref.online)

    def wait_for_host(self, name_or_url: str, *, timeout: float = 10.0, interval: float = 0.25) -> HostRef:
        deadline = time.monotonic() + timeout
        while True:
            ref = self.lookup(name_or_url)
            if ref is not None and ref.online:
                return ref
            if time.monotonic() >= deadline:
                raise HostError(f"Timed out waiting for host {name_or_url!r}")
            if self.enabled:
                self.gossip_once()
            time.sleep(max(0.01, interval))

    def register_wire(self, payload: dict[str, Any]) -> HostRef | None:
        return self.register(HostRef.from_wire(payload))

    def register(self, ref: HostRef) -> HostRef | None:
        if ref.code_version != self.code_version:
            self._debug(
                f"ignoring mesh peer {ref.name} at {ref.url}: "
                f"version {ref.code_version!r} != {self.code_version!r}"
            )
            return None
        if ref.url.rstrip("/") == self._host.address.rstrip("/"):
            return self.refresh_self()
        normalized = HostRef(
            name=ref.name,
            url=ref.url.rstrip("/"),
            code_version=ref.code_version,
            online=ref.online,
            last_seen=ref.last_seen or time.time(),
            active_count=ref.active_count,
            inactive_count=ref.inactive_count,
            error=ref.error,
        )
        with self._lock:
            existing = self._hosts.get(normalized.url)
            if existing is None or existing.last_seen <= normalized.last_seen or normalized.online:
                self._hosts[normalized.url] = normalized
        return normalized

    def join(self, url: str) -> list[HostRef]:
        if not self.enabled:
            return self.hosts(include_self=True)
        normalized = normalize_host_url(url)
        if normalized == self._host.address.rstrip("/"):
            return self.hosts(include_self=True)
        try:
            health = self._host.client.get_json(f"{normalized}/health")
            health_version = str(health.get("code_version") or "")
            if health_version != self.code_version:
                self._debug(
                    f"ignoring mesh peer {health.get('name', normalized)} at {normalized}: "
                    f"version {health_version!r} != {self.code_version!r}"
                )
                return self.hosts(include_self=True)
            remote_ref = HostRef(
                name=str(health.get("name") or _name_from_url(normalized)),
                url=str(health.get("address") or normalized).rstrip("/"),
                code_version=health_version,
                online=True,
                last_seen=time.time(),
                active_count=int(health.get("active_count", 0)),
                inactive_count=int(health.get("inactive_count", 0)),
            )
            self.register(remote_ref)
            response = self._host.client.post_json(f"{remote_ref.url}/hosts/join", self.refresh_self().to_wire())
            hosts = response.get("hosts", [])
            if isinstance(hosts, list):
                for item in hosts:
                    if isinstance(item, dict):
                        try:
                            self.register_wire(item)
                        except (KeyError, TypeError, ValueError):
                            continue
            return self.hosts(include_self=True)
        except Exception as exc:
            self.mark_offline(normalized, str(exc))
            return self.hosts(include_self=True)

    def gossip_once(self) -> None:
        if not self.enabled:
            return
        self.refresh_self()
        targets = set(self._seeds)
        with self._lock:
            targets.update(url for url in self._hosts if url != self._host.address.rstrip("/"))
        for url in sorted(targets):
            self.join(url)

    def mark_offline(self, url: str, error: str) -> None:
        normalized = normalize_host_url(url)
        if normalized == self._host.address.rstrip("/"):
            return
        with self._lock:
            existing = self._hosts.get(normalized)
            if existing is None:
                existing = HostRef(
                    name=_name_from_url(normalized),
                    url=normalized,
                    code_version=self.code_version,
                    online=False,
                    last_seen=time.time(),
                    active_count=0,
                    inactive_count=0,
                    error=error,
                )
            else:
                existing = HostRef(
                    name=existing.name,
                    url=existing.url,
                    code_version=existing.code_version,
                    online=False,
                    last_seen=existing.last_seen,
                    active_count=existing.active_count,
                    inactive_count=existing.inactive_count,
                    error=error,
                )
            self._hosts[normalized] = existing

    def refresh_self(self) -> HostRef:
        with self._host._lock:
            active_count = len(self._host._agents)
            inactive_count = len(self._host._inactive)
        ref = HostRef(
            name=self._host.name,
            url=self._host.address.rstrip("/"),
            code_version=self.code_version,
            online=True,
            last_seen=time.time(),
            active_count=active_count,
            inactive_count=inactive_count,
        )
        with self._lock:
            self._hosts[ref.url] = ref
        return ref

    def _gossip_loop(self) -> None:
        while not self._stop.wait(self.gossip_interval):
            try:
                self.gossip_once()
            except Exception as exc:  # pragma: no cover - defensive background boundary
                self._debug(f"mesh gossip failed: {exc}")

    def _multicast_send_loop(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        except OSError as exc:
            self._debug(f"mesh multicast sender disabled: {exc}")
            return
        with sock:
            while not self._stop.wait(self.gossip_interval):
                try:
                    sock.sendto(encode_mesh_beacon(self.refresh_self()), (MESH_MULTICAST_GROUP, MESH_MULTICAST_PORT))
                except OSError as exc:
                    self._debug(f"mesh multicast send failed: {exc}")

    def _multicast_receive_loop(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", MESH_MULTICAST_PORT))
            group = socket.inet_aton(MESH_MULTICAST_GROUP)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group + socket.inet_aton("0.0.0.0"))
            sock.settimeout(0.25)
        except OSError as exc:
            self._debug(f"mesh multicast listener disabled: {exc}")
            return
        with sock:
            while not self._stop.is_set():
                try:
                    data, _addr = sock.recvfrom(65535)
                except TimeoutError:
                    continue
                except OSError:
                    if self._stop.is_set():
                        return
                    continue
                ref = decode_mesh_beacon(data)
                if ref is None:
                    continue
                registered = self.register(ref)
                if registered is not None and registered.url != self._host.address.rstrip("/"):
                    self.add_seed(registered.url)

    def _expire_stale_hosts(self) -> None:
        if self.offline_after <= 0:
            return
        now = time.time()
        self_url = self._host.address.rstrip("/")
        with self._lock:
            for url, ref in list(self._hosts.items()):
                if url == self_url or not ref.online:
                    continue
                if now - ref.last_seen <= self.offline_after:
                    continue
                self._hosts[url] = HostRef(
                    name=ref.name,
                    url=ref.url,
                    code_version=ref.code_version,
                    online=False,
                    last_seen=ref.last_seen,
                    active_count=ref.active_count,
                    inactive_count=ref.inactive_count,
                    error="stale mesh peer",
                )

    def _debug(self, message: str) -> None:
        if os.environ.get("PAGLETS_MESH_DEBUG"):
            print(f"[paglets mesh] {message}", flush=True)


def _name_from_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc or url
