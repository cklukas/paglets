# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import hashlib
import math
import os
import platform
import threading
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import psutil

from paglets.core.agent import Paglet, PagletState, state_locked
from paglets.core.messages import Message
from paglets.core.runtime_values import ResidentLifecycle, ServiceScope
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from paglets.services.contracts import ServiceContract, ServiceOperation
from paglets.services.resident import ResidentServiceSpec

from ..server_info import GET_DISK, GET_LOAD, GET_SUMMARY, SERVER_INFO, DiskRequest, LoadRequest
from .affinity import CpuAffinityResult, apply_process_cpu_affinity

DEFAULT_LOOP_INTERVAL_SECONDS = 5.0
DEFAULT_GOSSIP_INTERVAL_SECONDS = 5.0
DEFAULT_GRANT_INTERVAL_SECONDS = 15.0
DEFAULT_LOAD_PER_CPU_LIMIT = 1.0
DEFAULT_MAX_STATUS_AGE_SECONDS = 20.0
DEFAULT_MAX_GRANTS_PER_TICK = 4
DEFAULT_MAX_REDIRECTS_PER_TICK = 4
DEFAULT_MAX_REDIRECT_FRACTION = 0.5
DEFAULT_REDIRECT_COOLDOWN_SECONDS = 60.0
DEFAULT_BURST_LOAD_PER_CPU_LIMIT = 0.5
DEFAULT_BURST_RESOURCE_HEADROOM_FACTOR = 2.0
DEFAULT_PLACEMENT_SAMPLE_SIZE = 3
DEFAULT_ACTIVE_EXPIRED_LEASE_EXTENSION_SECONDS = 300.0
COMPUTE_SLOTS_LOOP_ERROR_KEY = "compute-slots-loop"
COMPUTE_SLOTS_EXPIRED_ACTIVE_LEASE_ERROR_KEY = "compute-slots-expired-active-lease"
COMPUTE_SLOTS_SYNC_TIMEOUT_SECONDS = 1.0

DECISION_RUN_NOW = "run_now"
DECISION_SLEEP = "sleep"
DECISION_REDIRECT = "redirect"
DECISION_REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class ComputeSlotRequest:
    request_id: str = ""
    agent_id: str = ""
    agent_host_url: str = ""
    job_id: str = ""
    cpu_cores: int = 1
    memory_bytes: int = 0
    temp_storage_bytes: int = 0
    estimated_runtime_seconds: float = 0.0
    requires_gpu: bool = False
    gpu_memory_mb: int = 0
    required_host_tags: tuple[str, ...] = ()
    excluded_host_tags: tuple[str, ...] = ()
    preferred_host_tags: tuple[str, ...] = ()
    excluded_host_names: tuple[str, ...] = ()
    excluded_host_urls: tuple[str, ...] = ()
    submitted_at: float = 0.0
    redirect_count: int = 0
    last_redirect_at: float = 0.0
    last_redirect_from_host_url: str = ""


@dataclass(frozen=True, slots=True)
class CandidateHostsRequest:
    slot: ComputeSlotRequest = field(default_factory=ComputeSlotRequest)
    limit: int = 0
    include_self: bool = True
    max_age_seconds: float = DEFAULT_MAX_STATUS_AGE_SECONDS
    placement_sample_size: int = DEFAULT_PLACEMENT_SAMPLE_SIZE


@dataclass(frozen=True, slots=True)
class SchedulerHostStatus:
    host_name: str
    host_url: str
    observed_at: float
    supports_cpu_jobs: bool = True
    supports_gpu_jobs: bool = False
    cpu_count_logical: int = 0
    cpu_affinity_supported: bool = False
    eligible_cpu_ids: list[int] = field(default_factory=list)
    cpu_percent: float = 0.0
    load_average: list[float] = field(default_factory=list)
    load_per_cpu: float = 0.0
    memory_total_bytes: int = 0
    memory_available_bytes: int = 0
    work_dir_base: str = ""
    work_total_bytes: int = 0
    work_free_bytes: int = 0
    queue_length: int = 0
    active_leases: int = 0
    host_tags: tuple[str, ...] = ()
    host_properties: dict[str, str] = field(default_factory=dict)
    reserved_cpu_cores: int = 0
    reserved_memory_bytes: int = 0
    reserved_temp_storage_bytes: int = 0
    free_cpu_cores: int = 0
    free_memory_bytes: int = 0
    free_temp_storage_bytes: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class CandidateHost:
    status: SchedulerHostStatus
    score: float
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class CandidateHostsReply:
    generated_at: float
    candidates: list[CandidateHost] = field(default_factory=list)
    selected: CandidateHost | None = None
    rejected: dict[str, str] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SlotLease:
    lease_id: str
    request: ComputeSlotRequest
    host_name: str
    host_url: str
    work_dir_base: str
    granted_at: float
    expires_at: float
    cpu_core_ids: list[int] = field(default_factory=list)
    reserved_cpu_core_ids: list[int] = field(default_factory=list)
    cpu_affinity_supported: bool = False
    cpu_affinity_enforced: bool = False
    cpu_affinity_error: str = ""


@dataclass(frozen=True, slots=True)
class SlotDecisionReply:
    decision: str
    request_id: str = ""
    lease_id: str = ""
    host_name: str = ""
    host_url: str = ""
    work_dir_base: str = ""
    message: str = ""
    redirect_count: int = 0
    redirected_from_host_url: str = ""
    cpu_core_ids: list[int] = field(default_factory=list)
    cpu_affinity_supported: bool = False
    cpu_affinity_enforced: bool = False
    cpu_affinity_error: str = ""


@dataclass(frozen=True, slots=True)
class SlotReleaseRequest:
    lease_id: str = ""
    agent_id: str = ""


@dataclass(frozen=True, slots=True)
class SlotReleaseReply:
    ok: bool = True


@dataclass(frozen=True, slots=True)
class ComputeJobRuntimeInfo:
    lease_id: str
    request_id: str
    job_id: str
    agent_id: str
    active: bool = False
    pid: int = 0
    declared_cpu_cores: int = 0
    reserved_cpu_core_ids: list[int] = field(default_factory=list)
    assigned_cpu_core_ids: list[int] = field(default_factory=list)
    cpu_affinity_supported: bool = False
    cpu_affinity_enforced: bool = False
    cpu_affinity_error: str = ""
    declared_memory_bytes: int = 0
    declared_temp_storage_bytes: int = 0
    current_cpu_percent: float = 0.0
    current_memory_rss_bytes: int = 0
    current_memory_percent: float = 0.0
    process_status: str = ""
    error: str = ""


@dataclass(frozen=True, slots=True)
class SchedulerStatusRequest:
    include_queue: bool = False
    include_jobs: bool = False


@dataclass(frozen=True, slots=True)
class SchedulerStatusReply:
    status: SchedulerHostStatus
    queued_requests: list[ComputeSlotRequest] = field(default_factory=list)
    leases: list[SlotLease] = field(default_factory=list)
    active_jobs: list[ComputeJobRuntimeInfo] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SchedulerStatusSyncRequest:
    statuses: list[SchedulerHostStatus] = field(default_factory=list)
    max_age_seconds: float = DEFAULT_MAX_STATUS_AGE_SECONDS


@dataclass(frozen=True, slots=True)
class SchedulerStatusSyncReply:
    generated_at: float
    accepted: int = 0
    statuses: list[SchedulerHostStatus] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class CancelSlotRequestsRequest:
    request_ids: tuple[str, ...] = ()
    agent_ids: tuple[str, ...] = ()
    job_ids: tuple[str, ...] = ()
    all: bool = False
    include_leases: bool = False


@dataclass(frozen=True, slots=True)
class CancelSlotRequestsReply:
    cancelled_requests: int = 0
    cancelled_leases: int = 0


CANDIDATE_HOSTS = ServiceOperation("candidate_hosts", CandidateHostsRequest, CandidateHostsReply)
REQUEST_SLOT = ServiceOperation("request_slot", ComputeSlotRequest, SlotDecisionReply)
RELEASE_SLOT = ServiceOperation("release_slot", SlotReleaseRequest, SlotReleaseReply)
SCHEDULER_STATUS = ServiceOperation("scheduler_status", SchedulerStatusRequest, SchedulerStatusReply)
SYNC_SCHEDULER_STATUS = ServiceOperation("sync_scheduler_status", SchedulerStatusSyncRequest, SchedulerStatusSyncReply)
CANCEL_SLOT_REQUESTS = ServiceOperation("cancel_slot_requests", CancelSlotRequestsRequest, CancelSlotRequestsReply)

COMPUTE_SLOTS = ServiceContract(
    "compute-slots",
    operations=(
        CANDIDATE_HOSTS,
        REQUEST_SLOT,
        RELEASE_SLOT,
        SCHEDULER_STATUS,
        SYNC_SCHEDULER_STATUS,
        CANCEL_SLOT_REQUESTS,
    ),
    version="1",
)


@dataclass
class ComputeSlotsState(PagletState):
    service_scope: ServiceScope = ServiceScope.MESH
    loop_interval: float = DEFAULT_LOOP_INTERVAL_SECONDS
    gossip_interval: float = DEFAULT_GOSSIP_INTERVAL_SECONDS
    grant_interval: float = DEFAULT_GRANT_INTERVAL_SECONDS
    max_load_per_cpu: float = DEFAULT_LOAD_PER_CPU_LIMIT
    max_active_leases: int = 0
    max_grants_per_tick: int = DEFAULT_MAX_GRANTS_PER_TICK
    max_redirects_per_tick: int = DEFAULT_MAX_REDIRECTS_PER_TICK
    max_redirect_fraction: float = DEFAULT_MAX_REDIRECT_FRACTION
    redirect_cooldown_seconds: float = DEFAULT_REDIRECT_COOLDOWN_SECONDS
    burst_load_per_cpu: float = DEFAULT_BURST_LOAD_PER_CPU_LIMIT
    burst_resource_headroom_factor: float = DEFAULT_BURST_RESOURCE_HEADROOM_FACTOR
    supports_gpu_jobs: bool = False
    queued_requests: list[dict[str, Any]] = field(default_factory=list)
    leases: dict[str, dict[str, Any]] = field(default_factory=dict)
    peer_statuses: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    last_grant_at: float = 0.0
    last_gossip_at: float = 0.0


class ComputeSlotsAgent(Paglet[ComputeSlotsState]):
    """Resident mesh compute-slot scheduler with local admission control."""

    State = ComputeSlotsState
    RESIDENT_SERVICES = (
        ResidentServiceSpec(
            contract=COMPUTE_SLOTS,
            scope=ServiceScope.MESH,
            lifecycle=ResidentLifecycle.EAGER,
            idle_timeout=0.0,
            agent_id="service.compute-slots",
            singleton=True,
            state={"service_scope": ServiceScope.MESH.value},
        ),
    )

    def __init__(self, state: ComputeSlotsState | None = None, *, agent_id: str | None = None):
        super().__init__(state=state, agent_id=agent_id)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def on_creation(self, event):
        self.advertise_contract(COMPUTE_SLOTS, scope=self.state.service_scope)

    def on_activation(self, event):
        self.advertise_contract(COMPUTE_SLOTS, scope=self.state.service_scope)

    def run(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self.resources.register("compute-slots-loop", self._stop.set, suppress=True)
        self._thread = threading.Thread(
            target=self._loop,
            name=f"paglets-compute-slots-{self.context.name}",
            daemon=True,
        )
        self._thread.start()

    def handle_message(self, message: Message):
        return COMPUTE_SLOTS.route(
            message,
            {
                CANDIDATE_HOSTS: self.candidate_hosts,
                REQUEST_SLOT: self.request_slot,
                RELEASE_SLOT: self.release_slot,
                SCHEDULER_STATUS: self.scheduler_status,
                SYNC_SCHEDULER_STATUS: self.sync_scheduler_status,
                CANCEL_SLOT_REQUESTS: self.cancel_slot_requests,
            },
            default=self.not_handled(),
        )

    def candidate_hosts(self, request: CandidateHostsRequest) -> CandidateHostsReply:
        statuses = self._collect_statuses(request.max_age_seconds)
        candidates: list[CandidateHost] = []
        rejected: dict[str, str] = {}
        for status in statuses.values():
            if not request.include_self and status.host_url.rstrip("/") == self.context.address.rstrip("/"):
                rejected[status.host_name or status.host_url] = "self excluded"
                continue
            rejection = _can_ever_satisfy(status, request.slot)
            key = status.host_name or status.host_url
            if rejection:
                rejected[key] = rejection
                continue
            health = _current_health_rejection(status)
            if health:
                rejected[key] = health
                continue
            candidates.append(
                CandidateHost(status=status, score=_candidate_score(status, request.slot), reasons=["suitable"])
            )
        candidates.sort(key=lambda item: (item.score, item.status.host_name, item.status.host_url))
        limit = max(0, int(request.limit))
        if limit:
            candidates = candidates[:limit]
        selected = _select_placement_candidate(
            candidates,
            request.slot,
            sample_size=max(1, int(request.placement_sample_size)),
        )
        with self.locked_state() as state:
            errors = dict(state.errors)
        return CandidateHostsReply(
            generated_at=time.time(),
            candidates=candidates,
            selected=selected,
            rejected=rejected,
            errors=errors,
        )

    def request_slot(self, request: ComputeSlotRequest) -> SlotDecisionReply:
        request = _normalize_request(request, default_host_url=self.context.address)
        local = self._local_status()
        rejection = _can_ever_satisfy(local, request)
        if rejection:
            redirect = self._redirect_target(request)
            if redirect is not None:
                return self._redirect_reply(request, redirect, message="local host cannot satisfy request")
            return SlotDecisionReply(decision=DECISION_REJECTED, request_id=request.request_id, message=rejection)
        if self._can_grant_now(request, local, allow_burst=True):
            return self._grant_now(request, local, send_message=False)
        redirect = self._redirect_target(request)
        if redirect is not None and _can_run_now(redirect, request):
            return self._redirect_reply(request, redirect, message="peer host currently has capacity")
        self._queue_request(request)
        return SlotDecisionReply(
            decision=DECISION_SLEEP,
            request_id=request.request_id,
            host_name=local.host_name,
            host_url=local.host_url,
            work_dir_base=local.work_dir_base,
            message="queued for compute slot",
        )

    @state_locked
    def release_slot(self, request: SlotReleaseRequest) -> SlotReleaseReply:
        lease = self.state.leases.get(request.lease_id)
        if lease is not None:
            stored = dataclass_from_wire(SlotLease, lease)
            if not request.agent_id or request.agent_id == stored.request.agent_id:
                self.state.leases.pop(request.lease_id, None)
        return SlotReleaseReply(ok=True)

    def scheduler_status(self, request: SchedulerStatusRequest) -> SchedulerStatusReply:
        status = self._local_status()
        queued: list[ComputeSlotRequest] = []
        leases: list[SlotLease] = []
        if request.include_queue or request.include_jobs:
            with self.locked_state() as state:
                queued = [dataclass_from_wire(ComputeSlotRequest, item) for item in state.queued_requests]
                leases = [dataclass_from_wire(SlotLease, item) for item in state.leases.values()]
        active_jobs = self._runtime_info_for_leases(leases) if request.include_jobs else []
        if not request.include_queue:
            queued = []
            leases = []
        return SchedulerStatusReply(status=status, queued_requests=queued, leases=leases, active_jobs=active_jobs)

    @state_locked
    def cancel_slot_requests(self, request: CancelSlotRequestsRequest) -> CancelSlotRequestsReply:
        if not _has_cancel_filter(request):
            return CancelSlotRequestsReply()

        remaining_requests: list[dict[str, Any]] = []
        cancelled_requests = 0
        for item in self.state.queued_requests:
            queued_request = dataclass_from_wire(ComputeSlotRequest, item)
            if _matches_cancel_filter(queued_request, request):
                cancelled_requests += 1
            else:
                remaining_requests.append(item)
        self.state.queued_requests = remaining_requests

        cancelled_leases = 0
        if request.include_leases:
            for lease_id, wire in list(self.state.leases.items()):
                lease = dataclass_from_wire(SlotLease, wire)
                if _matches_cancel_filter(lease.request, request):
                    self.state.leases.pop(lease_id, None)
                    cancelled_leases += 1

        return CancelSlotRequestsReply(
            cancelled_requests=cancelled_requests,
            cancelled_leases=cancelled_leases,
        )

    def sync_scheduler_status(self, request: SchedulerStatusSyncRequest) -> SchedulerStatusSyncReply:
        accepted = 0
        max_age = max(0.0, request.max_age_seconds)
        now = time.time()
        with self.locked_state() as state:
            for status in request.statuses:
                if max_age and now - status.observed_at > max_age:
                    continue
                key = status.host_url.rstrip("/")
                if key == self.context.address.rstrip("/"):
                    continue
                current = state.peer_statuses.get(key)
                if current is not None:
                    existing = dataclass_from_wire(SchedulerHostStatus, current)
                    if existing.observed_at >= status.observed_at:
                        continue
                state.peer_statuses[key] = dataclass_to_wire(status)
                accepted += 1
        statuses = list(self._fresh_statuses(max_age or DEFAULT_MAX_STATUS_AGE_SECONDS).values())
        statuses.append(self._local_status())
        return SchedulerStatusSyncReply(generated_at=now, accepted=accepted, statuses=statuses)

    def _loop(self) -> None:
        while not self._stop.wait(max(0.1, float(self.state.loop_interval))):
            try:
                self._expire_leases()
                self._cleanup_inactive_leases()
                self._maybe_gossip()
                self._grant_queued_requests()
                self._rebalance_elastic_affinity()
            except Exception as exc:  # pragma: no cover - background diagnostics
                with self.locked_state() as state:
                    state.errors[COMPUTE_SLOTS_LOOP_ERROR_KEY] = str(exc)
            else:
                with self.locked_state() as state:
                    state.errors.pop(COMPUTE_SLOTS_LOOP_ERROR_KEY, None)

    def _maybe_gossip(self) -> None:
        now = time.time()
        with self.locked_state() as state:
            if now - state.last_gossip_at < max(0.1, float(state.gossip_interval)):
                return
            state.last_gossip_at = now
        local = self._local_status()
        handles = self.lookup_contracts(COMPUTE_SLOTS, operation=SYNC_SCHEDULER_STATUS, scope=ServiceScope.MESH)
        for handle in handles:
            if handle.record.proxy.agent_id == self.agent_id and handle.record.host_url.rstrip(
                "/"
            ) == self.context.address.rstrip("/"):
                continue
            try:
                reply = handle.call(
                    SYNC_SCHEDULER_STATUS,
                    SchedulerStatusSyncRequest(statuses=[local], max_age_seconds=DEFAULT_MAX_STATUS_AGE_SECONDS),
                    no_delay=True,
                    timeout=COMPUTE_SLOTS_SYNC_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                self._record_error(handle.record.host_name or handle.record.host_url, str(exc))
                continue
            self._clear_error(handle.record.host_name or handle.record.host_url)
            with self.locked_state() as state:
                for status in reply.statuses:
                    if status.host_url.rstrip("/") != self.context.address.rstrip("/"):
                        state.peer_statuses[status.host_url.rstrip("/")] = dataclass_to_wire(status)

    def _grant_queued_requests(self) -> None:
        grants_this_tick = 0
        with self.locked_state() as state:
            max_grants_per_tick = max(1, int(state.max_grants_per_tick))
        while True:
            request = self._next_grantable_request(allow_burst=grants_this_tick > 0)
            if request is None:
                break
            local = self._local_status()
            reply = self._grant_now(request, local, send_message=True)
            if reply.decision != DECISION_RUN_NOW:
                break
            self._remove_queued_request(request.request_id)
            grants_this_tick += 1
            if grants_this_tick >= max_grants_per_tick:
                break
        self._redirect_queued_requests()

    def _redirect_queued_requests(self) -> None:
        local = self._local_status()
        with self.locked_state() as state:
            queued = [dataclass_from_wire(ComputeSlotRequest, item) for item in state.queued_requests]
            redirect_budget = _redirect_budget(
                queue_length=len(queued),
                max_fraction=float(state.max_redirect_fraction),
                max_per_tick=int(state.max_redirects_per_tick),
            )
            cooldown_seconds = max(0.0, float(state.redirect_cooldown_seconds))
        if not queued:
            return
        if redirect_budget <= 0:
            return
        projected = self._fresh_statuses(DEFAULT_MAX_STATUS_AGE_SECONDS)
        redirected = 0
        now = time.time()
        for request in queued:
            if redirected >= redirect_budget:
                return
            if _can_run_now(local, request):
                continue
            if not _request_redirect_ready(request, now=now, cooldown_seconds=cooldown_seconds):
                continue
            target = _best_projected_redirect_target(
                request,
                projected.values(),
                local_host_url=self.context.address.rstrip("/"),
            )
            if target is None or not _can_run_now(target, request):
                continue
            reply = self._redirect_reply(request, target, message="peer scheduler has free capacity")
            if self._send_redirect(request, reply):
                self._remove_queued_request(request.request_id)
                projected[target.host_url.rstrip("/")] = _reserve_projected_capacity(target, request)
                redirected += 1

    def _next_grantable_request(self, *, allow_burst: bool) -> ComputeSlotRequest | None:
        local = self._local_status()
        with self.locked_state() as state:
            queued = [dataclass_from_wire(ComputeSlotRequest, item) for item in state.queued_requests]
        for request in queued:
            if self._can_grant_now(request, local, allow_burst=allow_burst):
                return request
        return None

    def _can_grant_now(self, request: ComputeSlotRequest, status: SchedulerHostStatus, *, allow_burst: bool) -> bool:
        if not _can_run_now(status, request):
            return False
        with self.locked_state() as state:
            if time.time() - state.last_grant_at >= max(0.0, float(state.grant_interval)):
                return True
        with self.locked_state() as state:
            burst_load_per_cpu = max(0.0, float(state.burst_load_per_cpu))
            resource_headroom_factor = max(1.0, float(state.burst_resource_headroom_factor))
        return allow_burst and _has_burst_headroom(
            status,
            request,
            load_per_cpu_limit=burst_load_per_cpu,
            resource_headroom_factor=resource_headroom_factor,
        )

    def _grant_now(
        self,
        request: ComputeSlotRequest,
        status: SchedulerHostStatus,
        *,
        send_message: bool,
    ) -> SlotDecisionReply:
        cpu_core_ids = self._allocate_cpu_core_ids(status, request)
        if status.cpu_affinity_supported and len(cpu_core_ids) < max(1, int(request.cpu_cores)):
            return SlotDecisionReply(
                decision=DECISION_REJECTED,
                request_id=request.request_id,
                message="no CPU core IDs available for affinity lease",
            )
        lease_id = f"slot-{uuid.uuid4().hex}"
        lease = SlotLease(
            lease_id=lease_id,
            request=request,
            host_name=status.host_name,
            host_url=status.host_url,
            work_dir_base=status.work_dir_base,
            granted_at=time.time(),
            expires_at=time.time() + max(60.0, request.estimated_runtime_seconds + 120.0),
            cpu_core_ids=cpu_core_ids,
            reserved_cpu_core_ids=cpu_core_ids,
            cpu_affinity_supported=status.cpu_affinity_supported,
            cpu_affinity_enforced=False,
        )
        if status.cpu_affinity_supported and cpu_core_ids:
            affinity = self._set_agent_cpu_affinity(request.agent_id, cpu_core_ids)
            lease = replace(
                lease,
                cpu_affinity_enforced=affinity.enforced,
                cpu_affinity_error=affinity.error,
            )
        with self.locked_state() as state:
            previous_last_grant_at = state.last_grant_at
            state.leases[lease_id] = dataclass_to_wire(lease)
            state.last_grant_at = time.time()
        reply = SlotDecisionReply(
            decision=DECISION_RUN_NOW,
            request_id=request.request_id,
            lease_id=lease_id,
            host_name=status.host_name,
            host_url=status.host_url,
            work_dir_base=status.work_dir_base,
            message="compute slot granted",
            cpu_core_ids=cpu_core_ids,
            cpu_affinity_supported=status.cpu_affinity_supported,
            cpu_affinity_enforced=lease.cpu_affinity_enforced,
            cpu_affinity_error=lease.cpu_affinity_error,
        )
        if send_message and not self._send_grant(request, reply):
            with self.locked_state() as state:
                state.leases.pop(lease_id, None)
                state.last_grant_at = previous_last_grant_at
            return SlotDecisionReply(
                decision=DECISION_REJECTED,
                request_id=request.request_id,
                message="grant delivery failed",
            )
        if send_message and status.cpu_affinity_supported and cpu_core_ids and not lease.cpu_affinity_enforced:
            affinity = self._set_agent_cpu_affinity(request.agent_id, cpu_core_ids)
            if affinity.enforced or affinity.error != lease.cpu_affinity_error:
                lease = replace(
                    lease,
                    cpu_affinity_enforced=affinity.enforced,
                    cpu_affinity_error=affinity.error,
                )
                with self.locked_state() as state:
                    if lease_id in state.leases:
                        state.leases[lease_id] = dataclass_to_wire(lease)
        return reply

    def _send_grant(self, request: ComputeSlotRequest, reply: SlotDecisionReply) -> bool:
        try:
            proxy = self.context.get_proxy(request.agent_id, request.agent_host_url or self.context.address)
            if proxy is None:
                return False
            proxy.send_oneway(Message("compute_slot_granted", dataclass_to_wire(reply)), activate_if_inactive=True)
            return True
        except Exception:
            return False

    def _send_redirect(self, request: ComputeSlotRequest, reply: SlotDecisionReply) -> bool:
        try:
            proxy = self.context.get_proxy(request.agent_id, request.agent_host_url or self.context.address)
            if proxy is None:
                return False
            proxy.send_oneway(Message("compute_slot_redirect", dataclass_to_wire(reply)), activate_if_inactive=True)
            return True
        except Exception:
            return False

    def _redirect_reply(
        self,
        request: ComputeSlotRequest,
        target: SchedulerHostStatus,
        *,
        message: str,
    ) -> SlotDecisionReply:
        target_url = target.host_url.rstrip("/")
        source_url = self.context.address.rstrip("/")
        return SlotDecisionReply(
            decision=DECISION_REDIRECT,
            request_id=request.request_id,
            host_name=target.host_name,
            host_url=target_url,
            message=message,
            redirect_count=max(0, int(request.redirect_count)) + 1,
            redirected_from_host_url=source_url,
        )

    def _redirect_target(self, request: ComputeSlotRequest) -> SchedulerHostStatus | None:
        if not _request_redirect_ready(
            request,
            now=time.time(),
            cooldown_seconds=max(0.0, float(self.state.redirect_cooldown_seconds)),
        ):
            return None
        try:
            local_host_url = self.context.address.rstrip("/")
        except Exception:
            local_host_url = ""
        return _best_projected_redirect_target(
            request,
            self._fresh_statuses(DEFAULT_MAX_STATUS_AGE_SECONDS).values(),
            local_host_url=local_host_url,
        )

    def _local_status(self) -> SchedulerHostStatus:
        self._expire_leases()
        self._cleanup_inactive_leases()
        errors: list[str] = []
        load = None
        summary = None
        disk = None
        work_dir = self.work_dir()
        try:
            load = self.require_contract(SERVER_INFO, operation=GET_LOAD, scope=ServiceScope.LOCAL).call(
                GET_LOAD,
                LoadRequest(interval=0.0, include_gpu=bool(self.state.supports_gpu_jobs)),
                timeout=1.0,
            )
        except Exception as exc:
            errors.append(f"load: {exc}")
        try:
            summary = self.require_contract(SERVER_INFO, operation=GET_SUMMARY, scope=ServiceScope.LOCAL).call(
                GET_SUMMARY,
                timeout=1.0,
            )
        except Exception as exc:
            errors.append(f"summary: {exc}")
        try:
            disk = self.require_contract(SERVER_INFO, operation=GET_DISK, scope=ServiceScope.LOCAL).call(
                GET_DISK,
                DiskRequest(paths=[str(work_dir)], all_volumes=False),
                timeout=1.0,
            )
        except Exception as exc:
            errors.append(f"work-disk: {exc}")
        volume = disk.volumes[0] if disk is not None and disk.volumes else None
        if disk is not None:
            errors.extend(f"{path}: {error}" for path, error in disk.errors.items())
        try:
            health = self.context.host.health()
        except Exception:
            health = {}
        cpu_count = int(summary.cpu_count_logical if summary is not None else 0)
        load_average = list(load.load_average if load is not None else [])
        load_value = load_average[0] if load_average else 0.0
        load_per_cpu = (float(load_value) / cpu_count) if cpu_count > 0 else 0.0
        reserved = self._reserved_resources()
        affinity_supported, eligible_cpu_ids = _local_cpu_affinity_capability(cpu_count)
        memory_total = int(load.memory_total_bytes if load is not None else 0)
        memory_available = int(load.memory_available_bytes if load is not None else 0)
        work_total = int(volume.total_bytes if volume is not None else 0)
        work_free = int(volume.free_bytes if volume is not None else 0)
        free_cpu = _free_cpu_cores(eligible_cpu_ids=eligible_cpu_ids, reserved_cpu=reserved["cpu"])
        free_memory = max(0, memory_available - reserved["memory"])
        free_storage = max(0, work_free - reserved["storage"])
        with self.locked_state() as state:
            queue_length = len(state.queued_requests)
            active_leases = len(state.leases)
            if state.max_active_leases > 0 and active_leases >= state.max_active_leases:
                free_cpu = 0
        return SchedulerHostStatus(
            host_name=self.context.name,
            host_url=self.context.address.rstrip("/"),
            observed_at=time.time(),
            supports_cpu_jobs=True,
            supports_gpu_jobs=bool(self.state.supports_gpu_jobs),
            cpu_count_logical=cpu_count,
            cpu_affinity_supported=affinity_supported,
            eligible_cpu_ids=eligible_cpu_ids,
            cpu_percent=float(load.cpu_percent if load is not None else 0.0),
            load_average=load_average,
            load_per_cpu=round(load_per_cpu, 4),
            memory_total_bytes=memory_total,
            memory_available_bytes=memory_available,
            work_dir_base=str(Path(work_dir).parent),
            work_total_bytes=work_total,
            work_free_bytes=work_free,
            queue_length=queue_length,
            active_leases=active_leases,
            host_tags=tuple(str(item) for item in health.get("tags", [])),
            host_properties={str(key): str(value) for key, value in dict(health.get("properties") or {}).items()},
            reserved_cpu_cores=reserved["cpu"],
            reserved_memory_bytes=reserved["memory"],
            reserved_temp_storage_bytes=reserved["storage"],
            free_cpu_cores=free_cpu,
            free_memory_bytes=free_memory,
            free_temp_storage_bytes=free_storage,
            errors=errors,
        )

    def _reserved_resources(self) -> dict[str, int]:
        total = {"cpu": 0, "memory": 0, "storage": 0}
        with self.locked_state() as state:
            leases = [dataclass_from_wire(SlotLease, item) for item in state.leases.values()]
        for lease in leases:
            total["cpu"] += max(1, int(lease.request.cpu_cores))
            total["memory"] += max(0, int(lease.request.memory_bytes))
            total["storage"] += max(0, int(lease.request.temp_storage_bytes))
        return total

    def _reserved_cpu_core_ids(self) -> set[int]:
        reserved: set[int] = set()
        with self.locked_state() as state:
            leases = [dataclass_from_wire(SlotLease, item) for item in state.leases.values()]
        for lease in leases:
            reserved.update(_lease_reserved_cpu_core_ids(lease))
        return reserved

    def _allocate_cpu_core_ids(self, status: SchedulerHostStatus, request: ComputeSlotRequest) -> list[int]:
        if not status.cpu_affinity_supported:
            return []
        reserved = self._reserved_cpu_core_ids()
        eligible = _eligible_cpu_ids(status)
        available = [cpu_id for cpu_id in eligible if cpu_id not in reserved]
        return available[: max(1, int(request.cpu_cores))]

    def _cleanup_inactive_leases(self) -> None:
        stale: list[str] = []
        with self.locked_state() as state:
            leases = [dataclass_from_wire(SlotLease, item) for item in state.leases.values()]
        for lease in leases:
            if not self._is_agent_active(lease.request.agent_id):
                stale.append(lease.lease_id)
        if not stale:
            return
        with self.locked_state() as state:
            for lease_id in stale:
                state.leases.pop(lease_id, None)

    def _rebalance_elastic_affinity(self) -> None:
        status = self._local_status()
        if not status.cpu_affinity_supported:
            return
        eligible = _eligible_cpu_ids(status)
        if not eligible:
            return
        with self.locked_state() as state:
            leases = [dataclass_from_wire(SlotLease, item) for item in state.leases.values()]
        active = [
            lease
            for lease in leases
            if lease.cpu_affinity_supported
            and lease.request.agent_id
            and _lease_reserved_cpu_core_ids(lease)
            and self._is_agent_active(lease.request.agent_id)
        ]
        if not active:
            return
        active.sort(key=lambda lease: (lease.granted_at, lease.lease_id))
        assignments = _elastic_cpu_assignments(eligible, active)
        updates: dict[str, SlotLease] = {}
        for lease in active:
            assigned = assignments.get(lease.lease_id, _lease_reserved_cpu_core_ids(lease))
            if assigned == lease.cpu_core_ids and lease.cpu_affinity_enforced:
                continue
            affinity = self._set_agent_cpu_affinity(lease.request.agent_id, assigned)
            updates[lease.lease_id] = replace(
                lease,
                cpu_core_ids=assigned,
                cpu_affinity_enforced=affinity.enforced,
                cpu_affinity_error=affinity.error,
            )
        if not updates:
            return
        with self.locked_state() as state:
            for lease_id, lease in updates.items():
                if lease_id in state.leases:
                    state.leases[lease_id] = dataclass_to_wire(lease)

    def _is_agent_active(self, agent_id: str) -> bool:
        if not agent_id:
            return False
        try:
            proxy = self.context.get_proxy(agent_id, self.context.address)
            return proxy is not None
        except Exception:
            return True

    def _set_agent_cpu_affinity(self, agent_id: str, cpu_core_ids: list[int]) -> CpuAffinityResult:
        host = getattr(self.context, "host", None)
        setter = getattr(host, "set_process_cpu_affinity", None)
        if callable(setter):
            try:
                payload = setter(agent_id, cpu_core_ids)
                return dataclass_from_wire(CpuAffinityResult, dict(payload.get("affinity") or payload))
            except Exception as exc:
                return CpuAffinityResult(
                    requested_cpu_ids=list(cpu_core_ids),
                    supported=True,
                    enforced=False,
                    error=str(exc),
                )
        try:
            proxy = self.context.get_proxy(agent_id, self.context.address)
            if proxy is None:
                return CpuAffinityResult(requested_cpu_ids=list(cpu_core_ids), supported=True, error="agent not found")
            pid = int(proxy.info().get("pid") or 0)
            if pid <= 0:
                return CpuAffinityResult(requested_cpu_ids=list(cpu_core_ids), supported=True, error="agent has no pid")
            return apply_process_cpu_affinity(pid, cpu_core_ids)
        except Exception as exc:
            return CpuAffinityResult(
                requested_cpu_ids=list(cpu_core_ids),
                supported=True,
                enforced=False,
                error=str(exc),
            )

    def _runtime_info_for_leases(self, leases: list[SlotLease]) -> list[ComputeJobRuntimeInfo]:
        info: list[ComputeJobRuntimeInfo] = []
        for lease in leases:
            active = False
            pid = 0
            process_status = ""
            cpu_percent = 0.0
            memory_rss = 0
            memory_percent = 0.0
            error = ""
            try:
                proxy = self.context.get_proxy(lease.request.agent_id, self.context.address)
                agent_info = proxy.info() if proxy is not None else {}
                active = bool(agent_info.get("active"))
                pid = int(agent_info.get("pid") or 0)
                if pid > 0:
                    process = psutil.Process(pid)
                    with process.oneshot():
                        process_status = process.status()
                        memory = process.memory_info()
                        memory_rss = int(memory.rss)
                        memory_percent = float(process.memory_percent())
                    cpu_percent = float(process.cpu_percent(interval=None))
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess) as exc:
                error = str(exc)
            except Exception as exc:
                error = str(exc)
            info.append(
                ComputeJobRuntimeInfo(
                    lease_id=lease.lease_id,
                    request_id=lease.request.request_id,
                    job_id=lease.request.job_id,
                    agent_id=lease.request.agent_id,
                    active=active,
                    pid=pid,
                    declared_cpu_cores=max(1, int(lease.request.cpu_cores)),
                    reserved_cpu_core_ids=_lease_reserved_cpu_core_ids(lease),
                    assigned_cpu_core_ids=list(lease.cpu_core_ids),
                    cpu_affinity_supported=lease.cpu_affinity_supported,
                    cpu_affinity_enforced=lease.cpu_affinity_enforced,
                    cpu_affinity_error=lease.cpu_affinity_error,
                    declared_memory_bytes=max(0, int(lease.request.memory_bytes)),
                    declared_temp_storage_bytes=max(0, int(lease.request.temp_storage_bytes)),
                    current_cpu_percent=round(cpu_percent, 2),
                    current_memory_rss_bytes=memory_rss,
                    current_memory_percent=round(memory_percent, 4),
                    process_status=process_status,
                    error=error,
                )
            )
        info.sort(key=lambda item: (item.job_id, item.agent_id, item.lease_id))
        return info

    def _fresh_statuses(self, max_age: float) -> dict[str, SchedulerHostStatus]:
        now = time.time()
        max_age = max(0.0, float(max_age))
        with self.locked_state() as state:
            items = list(state.peer_statuses.items())
        statuses: dict[str, SchedulerHostStatus] = {}
        for key, wire in items:
            status = dataclass_from_wire(SchedulerHostStatus, wire)
            if max_age and now - status.observed_at > max_age:
                continue
            statuses[key] = status
        return statuses

    def _collect_statuses(self, max_age: float) -> dict[str, SchedulerHostStatus]:
        statuses = self._fresh_statuses(max_age)
        local = self._local_status()
        statuses[local.host_url.rstrip("/")] = local
        handles = self.lookup_contracts(COMPUTE_SLOTS, operation=SCHEDULER_STATUS, scope=ServiceScope.MESH)
        for handle in handles:
            host_url = handle.record.host_url.rstrip("/")
            if host_url == self.context.address.rstrip("/") and handle.record.proxy.agent_id == self.agent_id:
                continue
            try:
                reply = handle.call(
                    SCHEDULER_STATUS,
                    SchedulerStatusRequest(include_queue=False),
                    no_delay=True,
                    timeout=COMPUTE_SLOTS_SYNC_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                self._record_error(handle.record.host_name or handle.record.host_url, str(exc))
                continue
            status = reply.status
            statuses[status.host_url.rstrip("/")] = status
            with self.locked_state() as state:
                state.peer_statuses[status.host_url.rstrip("/")] = dataclass_to_wire(status)
            self._clear_error(handle.record.host_name or handle.record.host_url)
        return statuses

    @state_locked
    def _queue_request(self, request: ComputeSlotRequest) -> None:
        self.state.queued_requests = [
            item
            for item in self.state.queued_requests
            if dataclass_from_wire(ComputeSlotRequest, item).request_id != request.request_id
        ]
        self.state.queued_requests.append(dataclass_to_wire(request))

    @state_locked
    def _remove_queued_request(self, request_id: str) -> None:
        self.state.queued_requests = [
            item
            for item in self.state.queued_requests
            if dataclass_from_wire(ComputeSlotRequest, item).request_id != request_id
        ]

    @state_locked
    def _expire_leases(self) -> None:
        now = time.time()
        retained_expired = 0
        for lease_id, wire in list(self.state.leases.items()):
            lease = dataclass_from_wire(SlotLease, wire)
            if lease.expires_at <= now:
                if self._is_agent_active(lease.request.agent_id):
                    retained_expired += 1
                    self.state.leases[lease_id] = dataclass_to_wire(
                        replace(lease, expires_at=now + DEFAULT_ACTIVE_EXPIRED_LEASE_EXTENSION_SECONDS)
                    )
                else:
                    self.state.leases.pop(lease_id, None)
        if retained_expired:
            self.state.errors[COMPUTE_SLOTS_EXPIRED_ACTIVE_LEASE_ERROR_KEY] = (
                f"retained {retained_expired} expired active lease(s)"
            )
        else:
            self.state.errors.pop(COMPUTE_SLOTS_EXPIRED_ACTIVE_LEASE_ERROR_KEY, None)

    def _record_error(self, key: str, error: str) -> None:
        with self.locked_state() as state:
            state.errors[key] = error

    def _clear_error(self, key: str) -> None:
        with self.locked_state() as state:
            state.errors.pop(key, None)


def _normalize_request(request: ComputeSlotRequest, *, default_host_url: str) -> ComputeSlotRequest:
    request_id = request.request_id or f"slot-request-{uuid.uuid4().hex}"
    submitted_at = request.submitted_at or time.time()
    return ComputeSlotRequest(
        request_id=request_id,
        agent_id=request.agent_id,
        agent_host_url=(request.agent_host_url or default_host_url).rstrip("/"),
        job_id=request.job_id,
        cpu_cores=max(1, int(request.cpu_cores)),
        memory_bytes=max(0, int(request.memory_bytes)),
        temp_storage_bytes=max(0, int(request.temp_storage_bytes)),
        estimated_runtime_seconds=max(0.0, float(request.estimated_runtime_seconds)),
        requires_gpu=bool(request.requires_gpu),
        gpu_memory_mb=max(0, int(request.gpu_memory_mb)),
        required_host_tags=_normalize_tag_tuple(request.required_host_tags),
        excluded_host_tags=_normalize_tag_tuple(request.excluded_host_tags),
        preferred_host_tags=_normalize_tag_tuple(request.preferred_host_tags),
        excluded_host_names=tuple(
            sorted({str(name).strip().casefold() for name in request.excluded_host_names if str(name).strip()})
        ),
        excluded_host_urls=tuple(
            sorted({str(url).strip().rstrip("/") for url in request.excluded_host_urls if str(url).strip()})
        ),
        submitted_at=submitted_at,
        redirect_count=max(0, int(request.redirect_count)),
        last_redirect_at=max(0.0, float(request.last_redirect_at)),
        last_redirect_from_host_url=request.last_redirect_from_host_url.rstrip("/"),
    )


def _has_cancel_filter(request: CancelSlotRequestsRequest) -> bool:
    return bool(request.all or request.request_ids or request.agent_ids or request.job_ids)


def _matches_cancel_filter(slot_request: ComputeSlotRequest, request: CancelSlotRequestsRequest) -> bool:
    if request.all:
        return True
    if slot_request.request_id and slot_request.request_id in request.request_ids:
        return True
    if slot_request.agent_id and slot_request.agent_id in request.agent_ids:
        return True
    return bool(slot_request.job_id and slot_request.job_id in request.job_ids)


def _can_ever_satisfy(status: SchedulerHostStatus, request: ComputeSlotRequest) -> str:
    if not status.supports_cpu_jobs:
        return "CPU jobs unsupported"
    if request.requires_gpu and not status.supports_gpu_jobs:
        return "GPU jobs unsupported"
    host_rejection = _host_policy_rejection(status, request)
    if host_rejection:
        return host_rejection
    eligible_cpu_count = len(_eligible_cpu_ids(status))
    if eligible_cpu_count > 0 and request.cpu_cores > eligible_cpu_count:
        return "requested CPU cores exceed host eligible CPU count"
    if status.memory_total_bytes > 0 and request.memory_bytes > status.memory_total_bytes:
        return "requested memory exceeds host RAM"
    if status.work_total_bytes > 0 and request.temp_storage_bytes > status.work_total_bytes:
        return "requested temp storage exceeds host work storage"
    return ""


def _current_health_rejection(status: SchedulerHostStatus) -> str:
    if status.errors:
        return "host status unavailable: " + "; ".join(status.errors)
    return ""


def _can_run_now(status: SchedulerHostStatus, request: ComputeSlotRequest) -> bool:
    if _can_ever_satisfy(status, request):
        return False
    if _current_health_rejection(status):
        return False
    if request.cpu_cores > max(0, status.free_cpu_cores):
        return False
    if request.memory_bytes > max(0, status.free_memory_bytes):
        return False
    return request.temp_storage_bytes <= max(0, status.free_temp_storage_bytes)


def _redirect_budget(*, queue_length: int, max_fraction: float, max_per_tick: int) -> int:
    queue_length = max(0, int(queue_length))
    if queue_length == 0 or max_per_tick <= 0 or max_fraction <= 0:
        return 0
    local_anchor_limit = 1 if queue_length == 1 else queue_length - 1
    fraction_limit = max(1, math.ceil(queue_length * max_fraction))
    return max(0, min(local_anchor_limit, fraction_limit, int(max_per_tick)))


def _request_redirect_ready(request: ComputeSlotRequest, *, now: float, cooldown_seconds: float) -> bool:
    last_redirect_at = max(0.0, float(request.last_redirect_at))
    return not last_redirect_at or now - last_redirect_at >= max(0.0, float(cooldown_seconds))


def _best_projected_redirect_target(
    request: ComputeSlotRequest,
    statuses: Iterable[SchedulerHostStatus],
    *,
    local_host_url: str,
) -> SchedulerHostStatus | None:
    candidates = [
        status
        for status in statuses
        if status.host_url.rstrip("/") != local_host_url.rstrip("/")
        and _can_run_now(status, request)
        and status.host_url.rstrip("/") != request.last_redirect_from_host_url.rstrip("/")
    ]
    if not candidates:
        candidates = [
            status
            for status in statuses
            if status.host_url.rstrip("/") != local_host_url.rstrip("/") and _can_run_now(status, request)
        ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda status: (
            status.queue_length,
            -status.free_cpu_cores,
            _candidate_score(status, request),
            status.host_name,
            status.host_url,
        )
    )
    return candidates[0]


def _reserve_projected_capacity(status: SchedulerHostStatus, request: ComputeSlotRequest) -> SchedulerHostStatus:
    return SchedulerHostStatus(
        host_name=status.host_name,
        host_url=status.host_url,
        observed_at=status.observed_at,
        supports_cpu_jobs=status.supports_cpu_jobs,
        supports_gpu_jobs=status.supports_gpu_jobs,
        cpu_count_logical=status.cpu_count_logical,
        cpu_affinity_supported=status.cpu_affinity_supported,
        eligible_cpu_ids=list(status.eligible_cpu_ids),
        cpu_percent=status.cpu_percent,
        load_average=list(status.load_average),
        load_per_cpu=status.load_per_cpu,
        memory_total_bytes=status.memory_total_bytes,
        memory_available_bytes=status.memory_available_bytes,
        work_dir_base=status.work_dir_base,
        work_total_bytes=status.work_total_bytes,
        work_free_bytes=status.work_free_bytes,
        queue_length=status.queue_length,
        active_leases=status.active_leases + 1,
        host_tags=status.host_tags,
        host_properties=dict(status.host_properties),
        reserved_cpu_cores=status.reserved_cpu_cores + max(1, int(request.cpu_cores)),
        reserved_memory_bytes=status.reserved_memory_bytes + max(0, int(request.memory_bytes)),
        reserved_temp_storage_bytes=status.reserved_temp_storage_bytes + max(0, int(request.temp_storage_bytes)),
        free_cpu_cores=max(0, status.free_cpu_cores - max(1, int(request.cpu_cores))),
        free_memory_bytes=max(0, status.free_memory_bytes - max(0, int(request.memory_bytes))),
        free_temp_storage_bytes=max(0, status.free_temp_storage_bytes - max(0, int(request.temp_storage_bytes))),
        errors=list(status.errors),
    )


def _select_placement_candidate(
    candidates: list[CandidateHost],
    request: ComputeSlotRequest,
    *,
    sample_size: int,
) -> CandidateHost | None:
    if not candidates:
        return None
    window = candidates[: max(1, min(int(sample_size), len(candidates)))]
    seed = request.job_id or request.request_id or request.agent_id
    if not seed:
        return window[0]
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    index = int.from_bytes(digest[:8], "big") % len(window)
    return window[index]


def _has_burst_headroom(
    status: SchedulerHostStatus,
    request: ComputeSlotRequest,
    *,
    load_per_cpu_limit: float,
    resource_headroom_factor: float,
) -> bool:
    factor = max(1.0, float(resource_headroom_factor))
    return (
        status.free_cpu_cores >= math.ceil(request.cpu_cores * factor)
        and status.free_memory_bytes >= math.ceil(request.memory_bytes * factor)
        and status.free_temp_storage_bytes >= math.ceil(request.temp_storage_bytes * factor)
        and status.load_per_cpu < max(0.0, float(load_per_cpu_limit))
    )


def _free_cpu_cores(*, eligible_cpu_ids: list[int], reserved_cpu: int) -> int:
    return max(0, len(eligible_cpu_ids) - max(0, int(reserved_cpu)))


def _eligible_cpu_ids(status: SchedulerHostStatus) -> list[int]:
    if status.eligible_cpu_ids:
        return sorted({int(cpu_id) for cpu_id in status.eligible_cpu_ids if int(cpu_id) >= 0})
    return list(range(max(0, int(status.cpu_count_logical))))


def _lease_reserved_cpu_core_ids(lease: SlotLease) -> list[int]:
    reserved = lease.reserved_cpu_core_ids or lease.cpu_core_ids[: max(1, int(lease.request.cpu_cores))]
    return sorted({int(cpu_id) for cpu_id in reserved if int(cpu_id) >= 0})


def _elastic_cpu_assignments(eligible_cpu_ids: list[int], leases: list[SlotLease]) -> dict[str, list[int]]:
    eligible = sorted({int(cpu_id) for cpu_id in eligible_cpu_ids if int(cpu_id) >= 0})
    assignments: dict[str, list[int]] = {}
    reserved_all: set[int] = set()
    for lease in leases:
        reserved = [cpu_id for cpu_id in _lease_reserved_cpu_core_ids(lease) if cpu_id in eligible]
        assignments[lease.lease_id] = list(reserved)
        reserved_all.update(reserved)
    spare = [cpu_id for cpu_id in eligible if cpu_id not in reserved_all]
    if not leases:
        return assignments
    for index, cpu_id in enumerate(spare):
        lease = leases[index % len(leases)]
        assignments[lease.lease_id].append(cpu_id)
    return {lease_id: sorted(cpu_ids) for lease_id, cpu_ids in assignments.items()}


def _local_cpu_affinity_capability(cpu_count: int) -> tuple[bool, list[int]]:
    fallback = list(range(max(0, int(cpu_count))))
    system = platform.system()
    if system == "Linux":
        eligible = fallback
        if hasattr(os, "sched_getaffinity"):
            try:
                eligible = sorted({int(cpu_id) for cpu_id in os.sched_getaffinity(0) if int(cpu_id) >= 0})
            except OSError:
                eligible = fallback
        return hasattr(os, "sched_setaffinity"), eligible or fallback
    if system in {"Windows", "FreeBSD"}:
        try:
            eligible = sorted({int(cpu_id) for cpu_id in psutil.Process().cpu_affinity() if int(cpu_id) >= 0})
        except (AttributeError, NotImplementedError, OSError, psutil.Error):
            return False, fallback
        return True, eligible or fallback
    return False, fallback


def _candidate_score(status: SchedulerHostStatus, request: ComputeSlotRequest | None = None) -> float:
    cpu_pressure = max(0.0, status.load_per_cpu) + max(0.0, status.cpu_percent / 100.0)
    memory_pressure = 1.0
    if status.memory_total_bytes:
        memory_pressure = 1.0 - (status.free_memory_bytes / status.memory_total_bytes)
    storage_pressure = 1.0
    if status.work_total_bytes:
        storage_pressure = 1.0 - (status.free_temp_storage_bytes / status.work_total_bytes)
    preferred_bonus = 0.0
    if request is not None and request.preferred_host_tags:
        matches = len(set(request.preferred_host_tags) & _status_tag_set(status))
        preferred_bonus = min(0.75, matches * 0.25)
    return round(
        max(0.0, cpu_pressure + memory_pressure + storage_pressure + status.queue_length * 0.1 - preferred_bonus), 6
    )


def _host_policy_rejection(status: SchedulerHostStatus, request: ComputeSlotRequest) -> str:
    status_tags = _status_tag_set(status)
    required = set(request.required_host_tags)
    missing = sorted(required - status_tags)
    if missing:
        return "missing required host tags: " + ", ".join(missing)
    excluded_tags = sorted(set(request.excluded_host_tags) & status_tags)
    if excluded_tags:
        return "excluded host tags present: " + ", ".join(excluded_tags)
    if status.host_name.strip().casefold() in {name.strip().casefold() for name in request.excluded_host_names}:
        return "host name excluded"
    if status.host_url.rstrip("/") in {url.rstrip("/") for url in request.excluded_host_urls}:
        return "host URL excluded"
    return ""


def _status_tag_set(status: SchedulerHostStatus) -> set[str]:
    return {str(tag).strip().casefold() for tag in status.host_tags if str(tag).strip()}


def _normalize_tag_tuple(tags: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(tag).strip().casefold() for tag in tags if str(tag).strip()}))
