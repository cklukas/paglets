# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import os
import platform
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psutil

from paglets.core.agent import Paglet, PagletState, state_locked
from paglets.core.messages import Message
from paglets.core.runtime_values import ResidentLifecycle, ServiceScope
from paglets.serialization.serde import dataclass_from_wire
from paglets.services.contracts import EmptyPayload, ServiceContract, ServiceOperation
from paglets.services.resident import ResidentServiceSpec


@dataclass(frozen=True, slots=True)
class GpuInfo:
    index: int
    name: str
    utilization_percent: float | None = None
    memory_used_mb: float | None = None
    memory_total_mb: float | None = None


@dataclass(frozen=True, slots=True)
class LoadRequest:
    interval: float = 0.0
    include_gpu: bool = True


@dataclass(frozen=True, slots=True)
class LoadReply:
    host_name: str
    host_url: str
    cpu_percent: float
    load_average: list[float] = field(default_factory=list)
    memory_total_bytes: int = 0
    memory_used_bytes: int = 0
    memory_available_bytes: int = 0
    memory_percent: float = 0.0
    swap_total_bytes: int = 0
    swap_used_bytes: int = 0
    swap_percent: float = 0.0
    gpu_available: bool = False
    gpu_error: str | None = None
    gpus: list[GpuInfo] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class DiskRequest:
    paths: list[str] = field(default_factory=list)
    all_volumes: bool = True


@dataclass(frozen=True, slots=True)
class DiskUsageInfo:
    path: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    percent_used: float


@dataclass(frozen=True, slots=True)
class DiskReply:
    host_name: str
    host_url: str
    volumes: list[DiskUsageInfo] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProcessListRequest:
    query: str = ""
    limit: int = 25
    include_args: bool = False


@dataclass(frozen=True, slots=True)
class ProcessInfo:
    pid: int
    name: str
    status: str
    username: str
    cpu_percent: float
    memory_rss_bytes: int
    memory_percent: float
    cmdline: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ProcessListReply:
    host_name: str
    host_url: str
    query: str
    processes: list[ProcessInfo] = field(default_factory=list)
    truncated: bool = False


@dataclass(frozen=True, slots=True)
class SummaryReply:
    host_name: str
    host_url: str
    platform: str
    python_version: str
    boot_time: float
    cpu_count_logical: int
    cpu_count_physical: int | None
    memory_total_bytes: int
    service_agent_id: str


GET_LOAD = ServiceOperation("load", LoadRequest, LoadReply)
GET_DISK = ServiceOperation("df", DiskRequest, DiskReply)
LIST_PROCESSES = ServiceOperation("plist", ProcessListRequest, ProcessListReply)
GET_SUMMARY = ServiceOperation("summary", EmptyPayload, SummaryReply)

SERVER_INFO = ServiceContract(
    "server-info",
    operations=(GET_LOAD, GET_DISK, LIST_PROCESSES, GET_SUMMARY),
    version="1",
)


@dataclass
class ServerInfoState(PagletState):
    service_scope: ServiceScope = ServiceScope.MESH


class ServerInfoAgent(Paglet[ServerInfoState]):
    """Resident service agent that reports local host system information."""

    State = ServerInfoState
    RESIDENT_SERVICES = (
        ResidentServiceSpec(
            contract=SERVER_INFO,
            scope=ServiceScope.MESH,
            lifecycle=ResidentLifecycle.LAZY,
            idle_timeout=30.0,
            agent_id="service.server-info",
            singleton=True,
            state={"service_scope": ServiceScope.MESH.value},
        ),
    )

    def on_creation(self, event):
        self.advertise_contract(SERVER_INFO, scope=self.state.service_scope)

    def handle_message(self, message: Message):
        return SERVER_INFO.route(
            message,
            {
                GET_LOAD: self.get_load,
                GET_DISK: self.get_disk,
                LIST_PROCESSES: self.list_processes,
                GET_SUMMARY: self.get_summary,
            },
            default=self.not_handled(),
        )

    def get_load(self, request: LoadRequest) -> LoadReply:
        virtual = psutil.virtual_memory()
        swap = psutil.swap_memory()
        gpus, gpu_error = collect_gpu_info() if request.include_gpu else ([], None)
        return LoadReply(
            host_name=self.context.name,
            host_url=self.context.address,
            cpu_percent=float(psutil.cpu_percent(interval=max(0.0, request.interval))),
            load_average=_load_average(),
            memory_total_bytes=int(virtual.total),
            memory_used_bytes=int(virtual.used),
            memory_available_bytes=int(virtual.available),
            memory_percent=float(virtual.percent),
            swap_total_bytes=int(swap.total),
            swap_used_bytes=int(swap.used),
            swap_percent=float(swap.percent),
            gpu_available=bool(gpus),
            gpu_error=gpu_error,
            gpus=gpus,
        )

    def get_disk(self, request: DiskRequest) -> DiskReply:
        volumes: list[DiskUsageInfo] = []
        errors: dict[str, str] = {}
        for path in _disk_paths(request):
            try:
                usage = shutil.disk_usage(path)
            except OSError as exc:
                errors[str(path)] = str(exc)
                continue
            percent = (usage.used / usage.total * 100.0) if usage.total else 0.0
            volumes.append(
                DiskUsageInfo(
                    path=str(path),
                    total_bytes=int(usage.total),
                    used_bytes=int(usage.used),
                    free_bytes=int(usage.free),
                    percent_used=round(percent, 2),
                )
            )
        return DiskReply(
            host_name=self.context.name,
            host_url=self.context.address,
            volumes=volumes,
            errors=errors,
        )

    def list_processes(self, request: ProcessListRequest) -> ProcessListReply:
        query = request.query.casefold()
        limit = max(1, request.limit)
        matches: list[ProcessInfo] = []
        for process in psutil.process_iter(["pid", "name", "status", "username", "cmdline", "memory_percent"]):
            try:
                info = process.info
                name = str(info.get("name") or "")
                cmdline = [str(item) for item in info.get("cmdline") or []]
                haystack = " ".join([name, *cmdline]).casefold()
                if query and query not in haystack:
                    continue
                memory = process.memory_info()
                matches.append(
                    ProcessInfo(
                        pid=int(info.get("pid") or process.pid),
                        name=name,
                        status=str(info.get("status") or ""),
                        username=str(info.get("username") or ""),
                        cpu_percent=float(process.cpu_percent(interval=None)),
                        memory_rss_bytes=int(memory.rss),
                        memory_percent=float(info.get("memory_percent") or 0.0),
                        cmdline=cmdline if request.include_args else [],
                    )
                )
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
        matches.sort(key=lambda item: item.memory_rss_bytes, reverse=True)
        return ProcessListReply(
            host_name=self.context.name,
            host_url=self.context.address,
            query=request.query,
            processes=matches[:limit],
            truncated=len(matches) > limit,
        )

    def get_summary(self, request: EmptyPayload) -> SummaryReply:
        virtual = psutil.virtual_memory()
        return SummaryReply(
            host_name=self.context.name,
            host_url=self.context.address,
            platform=platform.platform(),
            python_version=platform.python_version(),
            boot_time=float(psutil.boot_time()),
            cpu_count_logical=int(psutil.cpu_count(logical=True) or 0),
            cpu_count_physical=psutil.cpu_count(logical=False),
            memory_total_bytes=int(virtual.total),
            service_agent_id=self.agent_id,
        )


@dataclass
class SystemInfoCollectorState(PagletState):
    role: str = "parent"
    operation: str = ""
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = 5.0
    parent_host_url: str = ""
    parent_agent_id: str = ""
    target_host_name: str = ""
    target_host_url: str = ""
    deadline: float = 0.0
    pending_hosts: list[str] = field(default_factory=list)
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


class SystemInfoCollectorAgent(Paglet[SystemInfoCollectorState]):
    """Clone across the mesh and collect server-info replies."""

    State = SystemInfoCollectorState

    def run(self):
        with self.locked_state() as state:
            is_child = state.role == "child"
        if is_child:
            thread = threading.Thread(
                target=self._run_child,
                name=f"paglets-sysinfo-{self.context.name}",
                daemon=True,
            )
            thread.start()

    def handle_message(self, message: Message):
        if message.kind == "collect":
            with self.locked_state() as state:
                state.operation = str(message.args["operation"])
                state.request = dict(message.args.get("request") or {})
                state.timeout = float(message.args.get("timeout", 5.0))
            return self.collect()
        if message.kind == "drain":
            return self.drain(wait_timeout=float(message.args.get("wait_timeout", 0.5)))
        if message.kind == "child_result":
            return self.record_child_result(message.args)
        if message.kind == "summary":
            self._expire_timed_out_hosts()
            return self.summary()
        return self.not_handled()

    def collect(self) -> dict[str, Any]:
        with self.locked_state() as state:
            state.role = "parent"
            state.parent_host_url = self.context.address
            state.parent_agent_id = self.agent_id
            state.pending_hosts = []
            state.results = {}
            state.errors = {}
            timeout = state.timeout
            state.deadline = time.monotonic() + max(0.0, timeout)
        hosts = self.context.available_hosts(online_only=True, include_self=True)

        for host in hosts:
            with self.locked_state() as state:
                state.pending_hosts.append(host.name)
                state.role = "child"
                state.target_host_name = host.name
                state.target_host_url = host.url
            try:
                self.clone_to(host.name)
            except Exception as exc:
                with self.locked_state() as state:
                    state.pending_hosts = [name for name in state.pending_hosts if name != host.name]
                    state.errors[host.name] = str(exc)
            finally:
                with self.locked_state() as state:
                    state.role = "parent"
                    state.target_host_name = ""
                    state.target_host_url = ""

        return self.summary()

    def drain(self, *, wait_timeout: float) -> dict[str, Any]:
        self._expire_timed_out_hosts()

        def ready(state: SystemInfoCollectorState) -> bool:
            return not state.pending_hosts

        timeout = max(0.0, wait_timeout)
        with self.locked_state() as state:
            if state.deadline > 0:
                timeout = min(timeout, max(0.0, state.deadline - time.monotonic()))
        self.wait_state(ready, timeout=timeout)
        self._expire_timed_out_hosts()
        summary = self.summary()
        return {"done": not summary["pending_hosts"], "summary": summary}

    def _run_child(self) -> None:
        with self.locked_state() as state:
            operation_name = state.operation
            request_wire = dict(state.request)
            target_host_name = state.target_host_name
            target_host_url = state.target_host_url
            parent_agent_id = state.parent_agent_id
            parent_host_url = state.parent_host_url
        operation = _operation_for_name(operation_name)
        try:
            request = dataclass_from_wire(operation.request_type, request_wire)
            service = self.require_contract(SERVER_INFO, operation=operation, scope=ServiceScope.LOCAL)
            reply = service.call(operation, request)
            payload = {
                "host_name": target_host_name,
                "host_url": target_host_url,
                "result": operation.encode_reply(reply),
            }
        except Exception as exc:
            payload = {
                "host_name": target_host_name,
                "host_url": target_host_url,
                "error": str(exc),
            }

        parent = self.context.get_proxy(parent_agent_id, parent_host_url)
        try:
            if parent is not None:
                parent.send(Message("child_result", payload))
        finally:
            with contextlib.suppress(Exception):
                self.context.host.dispose(self.agent_id)

    @state_locked
    def record_child_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        host_name = str(payload["host_name"])
        self.state.pending_hosts = [name for name in self.state.pending_hosts if name != host_name]
        if payload.get("error"):
            self.state.errors[host_name] = str(payload["error"])
        else:
            self.state.results[host_name] = {
                "host_url": str(payload.get("host_url") or ""),
                "result": dict(payload.get("result") or {}),
            }
        self.notify_all_state_changed()
        return {"ok": True}

    @state_locked
    def summary(self) -> dict[str, Any]:
        return {
            "operation": self.state.operation,
            "results": dict(self.state.results),
            "errors": dict(self.state.errors),
            "pending_hosts": list(self.state.pending_hosts),
        }

    def _expire_timed_out_hosts(self) -> None:
        with self.locked_state() as state:
            if not state.pending_hosts or state.deadline <= 0 or time.monotonic() < state.deadline:
                return
            for host_name in list(state.pending_hosts):
                state.errors[host_name] = "timed out waiting for server-info result"
            state.pending_hosts = []
        self.notify_all_state_changed()


def collect_gpu_info() -> tuple[list[GpuInfo], str | None]:
    query = "index,name,utilization.gpu,memory.used,memory.total"
    try:
        completed = subprocess.run(
            ["nvidia-smi", f"--query-gpu={query}", "--format=csv,noheader,nounits"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except FileNotFoundError:
        return [], "nvidia-smi not found"
    except Exception as exc:
        return [], str(exc)
    if completed.returncode != 0:
        return [], completed.stderr.strip() or "nvidia-smi failed"

    gpus: list[GpuInfo] = []
    for line in completed.stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 5:
            continue
        gpus.append(
            GpuInfo(
                index=_parse_int(parts[0], default=len(gpus)),
                name=parts[1],
                utilization_percent=_parse_float(parts[2]),
                memory_used_mb=_parse_float(parts[3]),
                memory_total_mb=_parse_float(parts[4]),
            )
        )
    return gpus, None if gpus else "no GPUs reported by nvidia-smi"


def _load_average() -> list[float]:
    if hasattr(os, "getloadavg"):
        return [float(value) for value in os.getloadavg()]
    return []


def _disk_paths(request: DiskRequest) -> list[Path]:
    if request.paths:
        return [Path(path) for path in request.paths]
    if not request.all_volumes:
        return [Path("/")]
    paths = {Path("/")}
    try:
        for partition in psutil.disk_partitions(all=False):
            if partition.mountpoint:
                paths.add(Path(partition.mountpoint))
    except Exception:
        pass
    return sorted(paths, key=lambda path: str(path))


def _operation_for_name(name: str) -> ServiceOperation[Any, Any]:
    operation = SERVER_INFO.operation_for(name)
    if operation is None:
        raise ValueError(f"Unknown server-info operation {name!r}")
    return operation


def _parse_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def _parse_int(value: str, *, default: int) -> int:
    try:
        return int(value)
    except ValueError:
        return default
