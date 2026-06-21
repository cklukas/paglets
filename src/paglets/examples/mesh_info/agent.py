# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time
from typing import Any

from ...agent import Paglet, PagletState, state_locked
from ...messages import Message
from ...resident import ResidentServiceSpec
from ...runtime_values import ResidentLifecycle, ServiceScope
from ...serde import dataclass_from_wire, dataclass_to_wire
from ...services import ServiceContract, ServiceOperation
from ..system_info import GET_DISK, GET_LOAD, GET_SUMMARY, SERVER_INFO, DiskRequest, LoadRequest


DEFAULT_SAMPLE_INTERVAL_SECONDS = 5.0
DEFAULT_GOSSIP_INTERVAL_SECONDS = 2.0
DEFAULT_SAMPLE_TTL_SECONDS = 20.0
DEFAULT_PEER_BATCH_SIZE = 8
DEFAULT_SYNC_BATCH_SIZE = 64


@dataclass(frozen=True, slots=True)
class MeshHostSnapshot:
    host_name: str
    host_url: str
    code_version: str
    observed_at: float
    platform: str = ""
    cpu_count_logical: int = 0
    cpu_percent: float = 0.0
    load_average: list[float] = field(default_factory=list)
    load_per_cpu: float = 0.0
    memory_total_bytes: int = 0
    memory_available_bytes: int = 0
    memory_percent: float = 0.0
    swap_percent: float = 0.0
    work_path: str = ""
    work_total_bytes: int = 0
    work_free_bytes: int = 0
    work_percent_used: float = 0.0
    active_count: int = 0
    inactive_count: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SnapshotRequest:
    force: bool = False


@dataclass(frozen=True, slots=True)
class SnapshotReply:
    snapshot: MeshHostSnapshot | None = None
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class LandscapeRequest:
    fresh_only: bool = True
    max_age_seconds: float = 0.0
    limit: int = 0


@dataclass(frozen=True, slots=True)
class LandscapeReply:
    generated_at: float
    hosts: list[MeshHostSnapshot] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MeshInfoSyncRequest:
    snapshots: list[MeshHostSnapshot] = field(default_factory=list)
    max_age_seconds: float = 0.0
    limit: int = DEFAULT_SYNC_BATCH_SIZE


@dataclass(frozen=True, slots=True)
class MeshInfoSyncReply:
    generated_at: float
    accepted: int = 0
    snapshots: list[MeshHostSnapshot] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class TargetSelectionRequest:
    limit: int = 1
    max_age_seconds: float = 0.0
    max_load_per_cpu: float = 1.0
    max_cpu_percent: float = 100.0
    min_memory_available_bytes: int = 0
    min_work_free_bytes: int = 0
    include_self: bool = True


@dataclass(frozen=True, slots=True)
class TargetCandidate:
    snapshot: MeshHostSnapshot
    score: float
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class TargetSelectionReply:
    generated_at: float
    targets: list[TargetCandidate] = field(default_factory=list)
    rejected: dict[str, str] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


GET_SNAPSHOT = ServiceOperation("snapshot", SnapshotRequest, SnapshotReply)
GET_LANDSCAPE = ServiceOperation("landscape", LandscapeRequest, LandscapeReply)
SYNC_MESH_INFO = ServiceOperation("sync", MeshInfoSyncRequest, MeshInfoSyncReply)
SELECT_TARGETS = ServiceOperation("select", TargetSelectionRequest, TargetSelectionReply)

MESH_INFO = ServiceContract(
    "mesh-info",
    operations=(GET_SNAPSHOT, GET_LANDSCAPE, SYNC_MESH_INFO, SELECT_TARGETS),
    version="1",
)


@dataclass
class MeshInfoState(PagletState):
    service_scope: ServiceScope = ServiceScope.MESH
    sample_interval: float = DEFAULT_SAMPLE_INTERVAL_SECONDS
    gossip_interval: float = DEFAULT_GOSSIP_INTERVAL_SECONDS
    sample_ttl: float = DEFAULT_SAMPLE_TTL_SECONDS
    peer_batch_size: int = DEFAULT_PEER_BATCH_SIZE
    sync_batch_size: int = DEFAULT_SYNC_BATCH_SIZE
    include_gpu: bool = False
    last_sample_at: float = 0.0
    snapshots: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


class MeshInfoAgent(Paglet[MeshInfoState]):
    """Resident service that keeps a mesh-wide host resource landscape."""

    State = MeshInfoState
    RESIDENT_SERVICES = (
        ResidentServiceSpec(
            contract=MESH_INFO,
            scope=ServiceScope.MESH,
            lifecycle=ResidentLifecycle.EAGER,
            idle_timeout=0.0,
            agent_id="service.mesh-info",
            singleton=True,
            state={"service_scope": ServiceScope.MESH.value},
        ),
    )

    def __init__(self, state: MeshInfoState | None = None, *, agent_id: str | None = None):
        super().__init__(state=state, agent_id=agent_id)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def on_creation(self, event):
        self.advertise_contract(MESH_INFO, scope=self.state.service_scope)

    def on_activation(self, event):
        self.advertise_contract(MESH_INFO, scope=self.state.service_scope)

    def run(self) -> None:
        self._refresh_local_snapshot(force=True)
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self.resources.register("mesh-info-loop", self._stop.set, suppress=True)
        self._thread = threading.Thread(
            target=self._loop,
            name=f"paglets-mesh-info-{self.context.name}",
            daemon=True,
        )
        self._thread.start()

    def handle_message(self, message: Message):
        return MESH_INFO.route(
            message,
            {
                GET_SNAPSHOT: self.get_snapshot,
                GET_LANDSCAPE: self.get_landscape,
                SYNC_MESH_INFO: self.sync,
                SELECT_TARGETS: self.select_targets,
            },
            default=self.not_handled(),
        )

    def get_snapshot(self, request: SnapshotRequest) -> SnapshotReply:
        snapshot = self._refresh_local_snapshot(force=request.force)
        return SnapshotReply(snapshot=snapshot, errors=list(snapshot.errors))

    def get_landscape(self, request: LandscapeRequest) -> LandscapeReply:
        self._refresh_local_snapshot(force=False)
        max_age = request.max_age_seconds if request.max_age_seconds > 0 else self.state.sample_ttl
        hosts = self._snapshots(max_age=max_age, fresh_only=request.fresh_only)
        if request.limit > 0:
            hosts = hosts[: request.limit]
        with self.locked_state() as state:
            errors = dict(state.errors)
        return LandscapeReply(generated_at=time.time(), hosts=hosts, errors=errors)

    def sync(self, request: MeshInfoSyncRequest) -> MeshInfoSyncReply:
        accepted = 0
        for snapshot in request.snapshots:
            if self._merge_snapshot(snapshot):
                accepted += 1
        self._refresh_local_snapshot(force=False)
        max_age = request.max_age_seconds if request.max_age_seconds > 0 else self.state.sample_ttl
        limit = max(1, request.limit)
        return MeshInfoSyncReply(
            generated_at=time.time(),
            accepted=accepted,
            snapshots=self._snapshots(max_age=max_age, fresh_only=True)[:limit],
        )

    def select_targets(self, request: TargetSelectionRequest) -> TargetSelectionReply:
        self._refresh_local_snapshot(force=False)
        max_age = request.max_age_seconds if request.max_age_seconds > 0 else self.state.sample_ttl
        hosts = self._snapshots(max_age=max_age, fresh_only=True)
        limit = max(1, request.limit)
        targets: list[TargetCandidate] = []
        rejected: dict[str, str] = {}
        for snapshot in hosts:
            key = snapshot.host_name or snapshot.host_url
            if not request.include_self and snapshot.host_url.rstrip("/") == self.context.address.rstrip("/"):
                rejected[key] = "self excluded"
                continue
            rejection = _target_rejection(snapshot, request)
            if rejection:
                rejected[key] = rejection
                continue
            score = _target_score(snapshot)
            targets.append(TargetCandidate(snapshot=snapshot, score=score, reasons=["fresh", "eligible"]))
        targets.sort(key=lambda item: (item.score, item.snapshot.host_name, item.snapshot.host_url))
        with self.locked_state() as state:
            errors = dict(state.errors)
        return TargetSelectionReply(
            generated_at=time.time(),
            targets=targets[:limit],
            rejected=rejected,
            errors=errors,
        )

    def _loop(self) -> None:
        while not self._stop.wait(max(0.1, float(self.state.gossip_interval))):
            try:
                self._gossip_once()
            except Exception as exc:  # pragma: no cover - background diagnostics
                with self.locked_state() as state:
                    state.errors["mesh-info-loop"] = str(exc)

    def _gossip_once(self) -> None:
        self._refresh_local_snapshot(force=False)
        handles = self.lookup_contracts(MESH_INFO, operation=SYNC_MESH_INFO, scope=ServiceScope.MESH)
        with self.locked_state() as state:
            peer_batch_size = max(1, int(state.peer_batch_size))
            sync_batch_size = max(1, int(state.sync_batch_size))
            sample_ttl = float(state.sample_ttl)
        outgoing = self._snapshots(max_age=sample_ttl, fresh_only=True)[:sync_batch_size]
        peers = [
            handle
            for handle in handles
            if handle.record.proxy.agent_id != self.agent_id
            or handle.record.host_url.rstrip("/") != self.context.address.rstrip("/")
        ][:peer_batch_size]
        for handle in peers:
            try:
                reply = handle.call(
                    SYNC_MESH_INFO,
                    MeshInfoSyncRequest(snapshots=outgoing, max_age_seconds=sample_ttl, limit=sync_batch_size),
                    no_delay=True,
                )
            except Exception as exc:
                with self.locked_state() as state:
                    state.errors[handle.record.host_name or handle.record.host_url] = str(exc)
                continue
            for snapshot in reply.snapshots:
                self._merge_snapshot(snapshot)
        self._prune_snapshots()

    def _refresh_local_snapshot(self, *, force: bool) -> MeshHostSnapshot:
        with self.locked_state() as state:
            existing_wire = state.snapshots.get(self.context.address.rstrip("/"))
            if (
                not force
                and existing_wire is not None
                and time.time() - state.last_sample_at < max(0.0, float(state.sample_interval))
            ):
                return dataclass_from_wire(MeshHostSnapshot, existing_wire)
            include_gpu = bool(state.include_gpu)

        errors: list[str] = []
        now = time.time()
        load = None
        summary = None
        disk = None
        try:
            load = self.require_contract(SERVER_INFO, operation=GET_LOAD, scope=ServiceScope.LOCAL).call(
                GET_LOAD,
                LoadRequest(interval=0.0, include_gpu=include_gpu),
            )
        except Exception as exc:
            errors.append(f"load: {exc}")
        try:
            summary = self.require_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.LOCAL).call(GET_SUMMARY)
        except Exception as exc:
            errors.append(f"summary: {exc}")
        work_path = self.work_dir()
        try:
            disk = self.require_contract(SERVER_INFO, operation=GET_DISK, scope=ServiceScope.LOCAL).call(
                GET_DISK,
                DiskRequest(paths=[str(work_path)], all_volumes=False),
            )
        except Exception as exc:
            errors.append(f"work-disk: {exc}")

        volume = disk.volumes[0] if disk is not None and disk.volumes else None
        if disk is not None:
            errors.extend(f"{path}: {error}" for path, error in disk.errors.items())
        cpu_count = int(summary.cpu_count_logical if summary is not None else 0)
        load_average = list(load.load_average if load is not None else [])
        load_per_cpu = (load_average[0] / cpu_count) if load_average and cpu_count > 0 else 0.0
        health = self.context.host.health()
        snapshot = MeshHostSnapshot(
            host_name=self.context.name,
            host_url=self.context.address.rstrip("/"),
            code_version=self.context.host.mesh.code_version,
            observed_at=now,
            platform=summary.platform if summary is not None else "",
            cpu_count_logical=cpu_count,
            cpu_percent=float(load.cpu_percent if load is not None else 0.0),
            load_average=load_average,
            load_per_cpu=round(load_per_cpu, 4),
            memory_total_bytes=int(load.memory_total_bytes if load is not None else 0),
            memory_available_bytes=int(load.memory_available_bytes if load is not None else 0),
            memory_percent=float(load.memory_percent if load is not None else 0.0),
            swap_percent=float(load.swap_percent if load is not None else 0.0),
            work_path=str(work_path),
            work_total_bytes=int(volume.total_bytes if volume is not None else 0),
            work_free_bytes=int(volume.free_bytes if volume is not None else 0),
            work_percent_used=float(volume.percent_used if volume is not None else 0.0),
            active_count=int(health.get("active_count", 0)),
            inactive_count=int(health.get("inactive_count", 0)),
            errors=errors,
        )
        with self.locked_state() as state:
            state.last_sample_at = now
            state.snapshots[snapshot.host_url] = dataclass_to_wire(snapshot)
            if errors:
                state.errors[self.context.name] = "; ".join(errors)
            else:
                state.errors.pop(self.context.name, None)
        return snapshot

    @state_locked
    def _merge_snapshot(self, snapshot: MeshHostSnapshot) -> bool:
        key = snapshot.host_url.rstrip("/")
        current = self.state.snapshots.get(key)
        if current is not None:
            existing = dataclass_from_wire(MeshHostSnapshot, current)
            if existing.observed_at >= snapshot.observed_at:
                return False
        self.state.snapshots[key] = dataclass_to_wire(snapshot)
        return True

    def _snapshots(self, *, max_age: float, fresh_only: bool) -> list[MeshHostSnapshot]:
        now = time.time()
        with self.locked_state() as state:
            snapshots = [dataclass_from_wire(MeshHostSnapshot, item) for item in state.snapshots.values()]
        if fresh_only:
            snapshots = [snapshot for snapshot in snapshots if now - snapshot.observed_at <= max_age]
        return sorted(snapshots, key=lambda item: (item.host_name, item.host_url))

    def _prune_snapshots(self) -> None:
        now = time.time()
        with self.locked_state() as state:
            max_age = max(float(state.sample_ttl) * 3.0, float(state.sample_ttl) + 1.0)
            for key, item in list(state.snapshots.items()):
                snapshot = dataclass_from_wire(MeshHostSnapshot, item)
                if snapshot.host_url.rstrip("/") == self.context.address.rstrip("/"):
                    continue
                if now - snapshot.observed_at > max_age:
                    state.snapshots.pop(key, None)


def _target_rejection(snapshot: MeshHostSnapshot, request: TargetSelectionRequest) -> str:
    if request.max_load_per_cpu > 0 and snapshot.load_per_cpu > request.max_load_per_cpu:
        return f"load per cpu {snapshot.load_per_cpu:.2f} > {request.max_load_per_cpu:.2f}"
    if request.max_cpu_percent >= 0 and snapshot.cpu_percent > request.max_cpu_percent:
        return f"cpu {snapshot.cpu_percent:.1f}% > {request.max_cpu_percent:.1f}%"
    if request.min_memory_available_bytes > 0 and snapshot.memory_available_bytes < request.min_memory_available_bytes:
        return "available memory below minimum"
    if request.min_work_free_bytes > 0 and snapshot.work_free_bytes < request.min_work_free_bytes:
        return "work storage below minimum"
    if snapshot.errors:
        return "; ".join(snapshot.errors)
    return ""


def _target_score(snapshot: MeshHostSnapshot) -> float:
    load_score = max(0.0, snapshot.load_per_cpu)
    cpu_score = max(0.0, snapshot.cpu_percent / 100.0)
    memory_score = snapshot.memory_percent / 100.0 if snapshot.memory_total_bytes else 1.0
    disk_score = snapshot.work_percent_used / 100.0 if snapshot.work_total_bytes else 1.0
    return round(load_score + cpu_score + memory_score + disk_score, 6)
