# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
import hashlib
import math
import os
from pathlib import Path
import platform
import shutil
import tempfile
import threading
import time
from typing import Any

import psutil

from ...agent import Paglet, PagletState, state_locked
from ...messages import Message
from ...serde import dataclass_from_wire, dataclass_to_wire


DEFAULT_BENCHMARK_DURATION_SECONDS = 1.0
DEFAULT_DISK_SIZE_BYTES = 128 * 1024 * 1024
DEFAULT_LOCK_TIMEOUT_SECONDS = 60.0

_BENCHMARK_THREAD_LOCK = threading.Lock()
_PSEUDO_FILESYSTEMS = {
    "",
    "autofs",
    "binfmt_misc",
    "bpf",
    "cgroup",
    "cgroup2",
    "configfs",
    "debugfs",
    "devfs",
    "devpts",
    "devtmpfs",
    "fdesc",
    "fusectl",
    "hugetlbfs",
    "mqueue",
    "nsfs",
    "overlay",
    "proc",
    "pstore",
    "ramfs",
    "rpc_pipefs",
    "securityfs",
    "squashfs",
    "sysfs",
    "tmpfs",
    "tracefs",
}


@dataclass(frozen=True, slots=True)
class BenchmarkRequest:
    include_cpu: bool = True
    include_memory: bool = True
    include_disk: bool = True
    duration_seconds: float = DEFAULT_BENCHMARK_DURATION_SECONDS
    disk_size_bytes: int = DEFAULT_DISK_SIZE_BYTES
    workers: int = 0
    paths: list[str] = field(default_factory=list)
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS


@dataclass(frozen=True, slots=True)
class BenchmarkMetric:
    name: str
    duration_seconds: float
    operations: int = 0
    operations_per_second: float = 0.0
    bytes_processed: int = 0
    bytes_per_second: float = 0.0


@dataclass(frozen=True, slots=True)
class CpuBenchmarkResult:
    workers: int
    logical_cpus: int
    physical_cpus: int | None
    single_core: list[BenchmarkMetric] = field(default_factory=list)
    multi_core: list[BenchmarkMetric] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkResult:
    metrics: list[BenchmarkMetric] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class DiskTarget:
    path: str
    device: str = ""
    filesystem: str = ""


@dataclass(frozen=True, slots=True)
class DiskSkip:
    path: str
    reason: str
    device: str = ""
    filesystem: str = ""


@dataclass(frozen=True, slots=True)
class DiskVolumeBenchmark:
    path: str
    device: str
    filesystem: str
    total_bytes: int
    free_bytes_before: int
    benchmark_size_bytes: int
    write_seconds: float
    write_bytes_per_second: float
    fsync_seconds: float
    read_seconds: float
    read_bytes_per_second: float
    metadata_files_per_second: float


@dataclass(frozen=True, slots=True)
class DiskBenchmarkResult:
    volumes: list[DiskVolumeBenchmark] = field(default_factory=list)
    skipped: list[DiskSkip] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class HostBenchmarkResult:
    host_name: str
    host_url: str
    platform: str
    python_version: str
    lock_wait_seconds: float = 0.0
    cpu: CpuBenchmarkResult | None = None
    memory: MemoryBenchmarkResult | None = None
    disk: DiskBenchmarkResult | None = None
    errors: list[str] = field(default_factory=list)


@dataclass
class PerformanceBenchmarkState(PagletState):
    role: str = "parent"
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = 120.0
    parent_host_url: str = ""
    parent_agent_id: str = ""
    target_host_name: str = ""
    target_host_url: str = ""
    deadline: float = 0.0
    pending_hosts: list[str] = field(default_factory=list)
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)


class BenchmarkLockTimeout(TimeoutError):
    """Raised when another local benchmark keeps the host lock too long."""


class HostBenchmarkLock:
    """Process-local plus best-effort OS lock for one physical host/user."""

    def __init__(self, timeout: float):
        self.timeout = max(0.0, float(timeout))
        self.wait_seconds = 0.0
        self._file = None
        self._thread_acquired = False
        self._file_acquired = False

    def __enter__(self) -> "HostBenchmarkLock":
        started = time.perf_counter()
        deadline = started + self.timeout
        if not _BENCHMARK_THREAD_LOCK.acquire(timeout=self.timeout):
            self.wait_seconds = time.perf_counter() - started
            raise BenchmarkLockTimeout(f"benchmark lock busy after {self.wait_seconds:.3f}s")
        self._thread_acquired = True

        path = Path(tempfile.gettempdir()) / "paglets-benchmark.lock"
        self._file = path.open("a+b")
        self._file.seek(0, os.SEEK_END)
        if self._file.tell() == 0:
            self._file.write(b"\0")
            self._file.flush()
        while True:
            if _try_lock_file(self._file):
                self._file_acquired = True
                self.wait_seconds = time.perf_counter() - started
                return self
            if time.perf_counter() >= deadline:
                self.wait_seconds = time.perf_counter() - started
                self.__exit__(None, None, None)
                raise BenchmarkLockTimeout(f"benchmark file lock busy after {self.wait_seconds:.3f}s")
            time.sleep(0.05)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is not None:
            if self._file_acquired:
                _unlock_file(self._file)
                self._file_acquired = False
            self._file.close()
            self._file = None
        if self._thread_acquired:
            _BENCHMARK_THREAD_LOCK.release()
            self._thread_acquired = False


class PerformanceBenchmarkAgent(Paglet[PerformanceBenchmarkState]):
    """Clone across the mesh and run local host performance benchmarks."""

    State = PerformanceBenchmarkState

    def run(self) -> None:
        with self.locked_state() as state:
            is_child = state.role == "child"
        if is_child:
            thread = threading.Thread(
                target=self._run_child,
                name=f"paglets-benchmark-{self.context.name}",
                daemon=True,
            )
            thread.start()

    def handle_message(self, message: Message):
        if message.kind == "collect":
            with self.locked_state() as state:
                state.request = dict(message.args.get("request") or {})
                state.timeout = float(message.args.get("timeout", 120.0))
            return self.collect()
        if message.kind == "drain":
            return self.drain(wait_timeout=float(message.args.get("wait_timeout", 0.5)))
        if message.kind == "child_result":
            return self.record_child_result(message.args)
        if message.kind == "summary":
            self._expire_timed_out_hosts()
            return self.summary()
        if message.kind == "cleanup":
            return self.cleanup_children()
        return self.not_handled()

    def collect(self) -> dict[str, Any]:
        with self.locked_state() as state:
            state.role = "parent"
            state.parent_host_url = self.context.address
            state.parent_agent_id = self.agent_id
            state.pending_hosts = []
            state.results = {}
            state.errors = {}
            state.cleanup_errors = {}
            state.child_proxies = {}
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
                child = self.clone_to(host.name)
                with self.locked_state() as state:
                    state.child_proxies[host.name] = child.to_wire()
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

        def ready(state: PerformanceBenchmarkState) -> bool:
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
            request_wire = dict(state.request)
            target_host_name = state.target_host_name
            target_host_url = state.target_host_url
            parent_agent_id = state.parent_agent_id
            parent_host_url = state.parent_host_url
        try:
            request = dataclass_from_wire(BenchmarkRequest, request_wire)
            result = run_host_benchmarks(
                request,
                host_name=self.context.name,
                host_url=self.context.address,
            )
            payload = {
                "host_name": target_host_name or self.context.name,
                "host_url": target_host_url or self.context.address,
                "result": dataclass_to_wire(result),
            }
        except Exception as exc:
            payload = {
                "host_name": target_host_name or self.context.name,
                "host_url": target_host_url or self.context.address,
                "error": str(exc),
            }

        parent = self.context.get_proxy(parent_agent_id, parent_host_url)
        try:
            if parent is not None:
                parent.send(Message("child_result", payload))
        finally:
            try:
                self.context.host.dispose(self.agent_id)
            except Exception:
                pass

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
        return {"ok": True}

    @state_locked
    def summary(self) -> dict[str, Any]:
        return {
            "results": dict(self.state.results),
            "errors": dict(self.state.errors),
            "cleanup_errors": dict(self.state.cleanup_errors),
            "pending_hosts": list(self.state.pending_hosts),
        }

    def cleanup_children(self) -> dict[str, Any]:
        with self.locked_state() as state:
            children = {host_name: dict(proxy) for host_name, proxy in state.child_proxies.items()}
        for host_name, proxy_wire in children.items():
            try:
                from ...proxy import PagletProxy

                PagletProxy.from_wire(proxy_wire, self.context.host.client).dispose()
            except Exception as exc:
                with self.locked_state() as state:
                    state.cleanup_errors[host_name] = str(exc)
        return self.summary()

    def _expire_timed_out_hosts(self) -> None:
        with self.locked_state() as state:
            if not state.pending_hosts or state.deadline <= 0 or time.monotonic() < state.deadline:
                return
            for host_name in list(state.pending_hosts):
                state.errors[host_name] = "timed out waiting for benchmark result"
            state.pending_hosts = []
        self.notify_all_state_changed()


def parse_size(value: str) -> int:
    text = value.strip()
    if not text:
        raise ValueError("size cannot be empty")
    unit = text[-1].upper()
    if unit in {"K", "M", "G"}:
        number = text[:-1]
        multiplier = {"K": 1024, "M": 1024**2, "G": 1024**3}[unit]
    else:
        number = text[:-1] if unit == "B" else text
        multiplier = 1
    try:
        amount = float(number)
    except ValueError as exc:
        raise ValueError(f"invalid size {value!r}") from exc
    if amount <= 0:
        raise ValueError("size must be positive")
    return int(amount * multiplier)


def run_host_benchmarks(request: BenchmarkRequest, *, host_name: str, host_url: str) -> HostBenchmarkResult:
    errors: list[str] = []
    lock_wait = 0.0
    cpu: CpuBenchmarkResult | None = None
    memory: MemoryBenchmarkResult | None = None
    disk: DiskBenchmarkResult | None = None

    try:
        with HostBenchmarkLock(request.lock_timeout_seconds) as lock:
            lock_wait = lock.wait_seconds
            duration = max(0.01, float(request.duration_seconds))
            workers = request.workers if request.workers > 0 else int(psutil.cpu_count(logical=True) or 1)
            workers = max(1, workers)
            if request.include_cpu:
                cpu = benchmark_cpu(duration_seconds=duration, workers=workers)
            if request.include_memory:
                memory = benchmark_memory(duration_seconds=duration)
            if request.include_disk:
                disk = benchmark_disk(paths=request.paths, size_bytes=max(1, int(request.disk_size_bytes)))
    except BenchmarkLockTimeout as exc:
        errors.append(str(exc))
    except Exception as exc:
        errors.append(str(exc))

    return HostBenchmarkResult(
        host_name=host_name,
        host_url=host_url,
        platform=platform.platform(),
        python_version=platform.python_version(),
        lock_wait_seconds=lock_wait,
        cpu=cpu,
        memory=memory,
        disk=disk,
        errors=errors,
    )


def benchmark_cpu(*, duration_seconds: float, workers: int) -> CpuBenchmarkResult:
    errors: list[str] = []
    single = [
        _cpu_integer_kernel(duration_seconds),
        _cpu_float_kernel(duration_seconds),
        _cpu_sha256_kernel(duration_seconds),
    ]
    multi: list[BenchmarkMetric] = []
    for name in ("integer", "float", "sha256"):
        try:
            multi.append(_run_multi_core_kernel(name, duration_seconds, workers))
        except Exception as exc:
            errors.append(f"{name} multi-core benchmark failed: {exc}")
    return CpuBenchmarkResult(
        workers=workers,
        logical_cpus=int(psutil.cpu_count(logical=True) or 0),
        physical_cpus=psutil.cpu_count(logical=False),
        single_core=single,
        multi_core=multi,
        errors=errors,
    )


def benchmark_memory(*, duration_seconds: float) -> MemoryBenchmarkResult:
    errors: list[str] = []
    metrics: list[BenchmarkMetric] = []
    try:
        metrics.append(_memory_copy_kernel(duration_seconds))
    except Exception as exc:
        errors.append(f"memory copy benchmark failed: {exc}")
    try:
        metrics.append(_memory_scan_kernel(duration_seconds))
    except Exception as exc:
        errors.append(f"memory scan benchmark failed: {exc}")
    return MemoryBenchmarkResult(metrics=metrics, errors=errors)


def benchmark_disk(*, paths: list[str], size_bytes: int) -> DiskBenchmarkResult:
    volumes: list[DiskVolumeBenchmark] = []
    skipped: list[DiskSkip] = []
    errors: list[str] = []
    targets, discovered_skips = discover_disk_targets(paths)
    skipped.extend(discovered_skips)
    for target in targets:
        try:
            usage = shutil.disk_usage(target.path)
        except OSError as exc:
            skipped.append(DiskSkip(target.path, f"cannot read disk usage: {exc}", target.device, target.filesystem))
            continue
        if usage.free < size_bytes * 2:
            skipped.append(
                DiskSkip(
                    target.path,
                    f"free space below required {size_bytes * 2} bytes",
                    target.device,
                    target.filesystem,
                )
            )
            continue
        try:
            volumes.append(benchmark_disk_target(target, size_bytes=size_bytes))
        except Exception as exc:
            errors.append(f"{target.path}: {exc}")
    return DiskBenchmarkResult(volumes=volumes, skipped=skipped, errors=errors)


def discover_disk_targets(paths: list[str] | None = None) -> tuple[list[DiskTarget], list[DiskSkip]]:
    targets: list[DiskTarget] = []
    skipped: list[DiskSkip] = []
    seen: set[str] = set()
    partitions = _partitions_by_mountpoint()

    if paths:
        for raw_path in paths:
            path = Path(raw_path).expanduser()
            target = _target_for_explicit_path(path, partitions)
            _append_target(target, targets, skipped, seen, explicit=True)
        return targets, skipped

    for partition in psutil.disk_partitions(all=False):
        target = DiskTarget(
            path=str(partition.mountpoint),
            device=str(partition.device or ""),
            filesystem=str(partition.fstype or ""),
        )
        _append_target(target, targets, skipped, seen, explicit=False, opts=str(partition.opts or ""))
    for path in _default_writable_disk_dirs():
        target = _target_for_explicit_path(path, partitions)
        _append_target(target, targets, skipped, seen, explicit=True)
    return targets, skipped


def benchmark_disk_target(target: DiskTarget, *, size_bytes: int) -> DiskVolumeBenchmark:
    temp_dir = Path(tempfile.mkdtemp(prefix="paglets-bench-", dir=target.path))
    file_path = temp_dir / "sequential.bin"
    chunk = b"\0" * min(1024 * 1024, max(1, size_bytes))
    written = 0
    fsync_seconds = 0.0
    try:
        write_started = time.perf_counter()
        with file_path.open("wb") as handle:
            while written < size_bytes:
                piece = chunk[: min(len(chunk), size_bytes - written)]
                handle.write(piece)
                written += len(piece)
            handle.flush()
            fsync_started = time.perf_counter()
            os.fsync(handle.fileno())
            fsync_seconds = time.perf_counter() - fsync_started
        write_seconds = max(time.perf_counter() - write_started - fsync_seconds, 1e-9)

        read_bytes = 0
        read_started = time.perf_counter()
        with file_path.open("rb") as handle:
            while True:
                data = handle.read(len(chunk))
                if not data:
                    break
                read_bytes += len(data)
        read_seconds = max(time.perf_counter() - read_started, 1e-9)

        metadata_count = 100
        metadata_started = time.perf_counter()
        for index in range(metadata_count):
            metadata_path = temp_dir / f"meta-{index:04d}.tmp"
            metadata_path.write_bytes(b"x")
        for index in range(metadata_count):
            (temp_dir / f"meta-{index:04d}.tmp").unlink()
        metadata_seconds = max(time.perf_counter() - metadata_started, 1e-9)

        usage = shutil.disk_usage(target.path)
        return DiskVolumeBenchmark(
            path=target.path,
            device=target.device,
            filesystem=target.filesystem,
            total_bytes=int(usage.total),
            free_bytes_before=int(usage.free),
            benchmark_size_bytes=size_bytes,
            write_seconds=write_seconds,
            write_bytes_per_second=written / write_seconds,
            fsync_seconds=fsync_seconds,
            read_seconds=read_seconds,
            read_bytes_per_second=read_bytes / read_seconds,
            metadata_files_per_second=metadata_count / metadata_seconds,
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _append_target(
    target: DiskTarget,
    targets: list[DiskTarget],
    skipped: list[DiskSkip],
    seen: set[str],
    *,
    explicit: bool,
    opts: str = "",
) -> None:
    path = Path(target.path)
    options = {part.strip().lower() for part in opts.split(",") if part.strip()}
    if not path.exists() or not path.is_dir():
        skipped.append(DiskSkip(target.path, "path is not an existing directory", target.device, target.filesystem))
        return
    if "ro" in options:
        skipped.append(DiskSkip(target.path, "read-only volume", target.device, target.filesystem))
        return
    if not explicit and target.filesystem.lower() in _PSEUDO_FILESYSTEMS:
        skipped.append(DiskSkip(target.path, "special filesystem", target.device, target.filesystem))
        return
    if not os.access(path, os.W_OK | os.X_OK):
        skipped.append(DiskSkip(target.path, "not writable", target.device, target.filesystem))
        return
    key = _disk_target_key(path)
    if key in seen:
        skipped.append(DiskSkip(target.path, "duplicate volume", target.device, target.filesystem))
        return
    seen.add(key)
    targets.append(target)


def _default_writable_disk_dirs() -> list[Path]:
    candidates = [
        Path.home() / ".paglets" / "benchmarks",
        Path(tempfile.gettempdir()),
    ]
    result: list[Path] = []
    for path in candidates:
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError:
            continue
        if path.is_dir() and os.access(path, os.W_OK | os.X_OK):
            result.append(path)
    return result


def _disk_target_key(path: Path) -> str:
    try:
        return f"dev:{path.stat().st_dev}"
    except OSError:
        return f"path:{path.resolve() if path.exists() else path}"


def _partitions_by_mountpoint() -> list[DiskTarget]:
    partitions = []
    for partition in psutil.disk_partitions(all=False):
        partitions.append(
            DiskTarget(
                path=str(partition.mountpoint),
                device=str(partition.device or ""),
                filesystem=str(partition.fstype or ""),
            )
        )
    return sorted(partitions, key=lambda item: len(item.path), reverse=True)


def _target_for_explicit_path(path: Path, partitions: list[DiskTarget]) -> DiskTarget:
    resolved = str(path.resolve()) if path.exists() else str(path)
    for partition in partitions:
        mount = Path(partition.path)
        try:
            if Path(resolved).is_relative_to(mount.resolve()):
                return DiskTarget(str(path), partition.device, partition.filesystem)
        except (OSError, ValueError):
            continue
    return DiskTarget(str(path), "", "")


def _run_multi_core_kernel(name: str, duration_seconds: float, workers: int) -> BenchmarkMetric:
    with ProcessPoolExecutor(max_workers=workers) as executor:
        metrics = list(executor.map(_cpu_kernel_worker, [(name, duration_seconds)] * workers))
    duration = max(max(metric.duration_seconds for metric in metrics), 1e-9)
    operations = sum(metric.operations for metric in metrics)
    bytes_processed = sum(metric.bytes_processed for metric in metrics)
    return BenchmarkMetric(
        name=f"{name}-multi",
        duration_seconds=duration,
        operations=operations,
        operations_per_second=operations / duration,
        bytes_processed=bytes_processed,
        bytes_per_second=bytes_processed / duration,
    )


def _cpu_kernel_worker(args: tuple[str, float]) -> BenchmarkMetric:
    name, duration_seconds = args
    if name == "integer":
        return _cpu_integer_kernel(duration_seconds)
    if name == "float":
        return _cpu_float_kernel(duration_seconds)
    if name == "sha256":
        return _cpu_sha256_kernel(duration_seconds)
    raise ValueError(f"unknown CPU kernel {name!r}")


def _cpu_integer_kernel(duration_seconds: float) -> BenchmarkMetric:
    started = time.perf_counter()
    deadline = started + duration_seconds
    operations = 0
    value = 0x12345678
    while time.perf_counter() < deadline:
        for _ in range(10_000):
            value = (value * 1_664_525 + 1_013_904_223) & 0xFFFFFFFF
        operations += 10_000
    elapsed = max(time.perf_counter() - started, 1e-9)
    return BenchmarkMetric("integer", elapsed, operations, operations / elapsed)


def _cpu_float_kernel(duration_seconds: float) -> BenchmarkMetric:
    started = time.perf_counter()
    deadline = started + duration_seconds
    operations = 0
    value = 1.000001
    while time.perf_counter() < deadline:
        for _ in range(10_000):
            value = math.sin(value) + value * 1.0000001 + 0.0000003
            if value > 1000.0:
                value = 1.000001
        operations += 10_000
    elapsed = max(time.perf_counter() - started, 1e-9)
    return BenchmarkMetric("float", elapsed, operations, operations / elapsed)


def _cpu_sha256_kernel(duration_seconds: float) -> BenchmarkMetric:
    started = time.perf_counter()
    deadline = started + duration_seconds
    block = b"paglets-benchmark" * 4096
    operations = 0
    bytes_processed = 0
    digest = b""
    while time.perf_counter() < deadline:
        digest = hashlib.sha256(block + digest[:8]).digest()
        operations += 1
        bytes_processed += len(block)
    elapsed = max(time.perf_counter() - started, 1e-9)
    return BenchmarkMetric(
        "sha256",
        elapsed,
        operations,
        operations / elapsed,
        bytes_processed,
        bytes_processed / elapsed,
    )


def _memory_copy_kernel(duration_seconds: float) -> BenchmarkMetric:
    started = time.perf_counter()
    deadline = started + duration_seconds
    source = bytearray(b"\x55" * (8 * 1024 * 1024))
    destination = bytearray(len(source))
    bytes_processed = 0
    operations = 0
    while time.perf_counter() < deadline:
        destination[:] = source
        bytes_processed += len(source)
        operations += 1
    elapsed = max(time.perf_counter() - started, 1e-9)
    return BenchmarkMetric("memory-copy", elapsed, operations, operations / elapsed, bytes_processed, bytes_processed / elapsed)


def _memory_scan_kernel(duration_seconds: float) -> BenchmarkMetric:
    started = time.perf_counter()
    deadline = started + duration_seconds
    source = bytearray(b"\x33" * (8 * 1024 * 1024))
    bytes_processed = 0
    operations = 0
    checksum = 0
    while time.perf_counter() < deadline:
        checksum ^= sum(source) & 0xFFFFFFFF
        bytes_processed += len(source)
        operations += 1
    elapsed = max(time.perf_counter() - started, 1e-9)
    return BenchmarkMetric("memory-scan", elapsed, operations, operations / elapsed, bytes_processed, bytes_processed / elapsed)


def _try_lock_file(handle) -> bool:
    if os.name == "nt":  # pragma: no cover - exercised on Windows only
        try:
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    try:
        import fcntl
    except ImportError:
        return True

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        return False


def _unlock_file(handle) -> None:
    if os.name == "nt":  # pragma: no cover - exercised on Windows only
        try:
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        return
    try:
        import fcntl
    except ImportError:
        return

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
