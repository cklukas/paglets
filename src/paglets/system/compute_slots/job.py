# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import random
import threading
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Generic, TypeVar, final

from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message
from paglets.core.runtime_values import ServiceScope
from paglets.persistence.persistency import DeactivationPolicy, DeactivationRequest
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire

from .agent import (
    CANDIDATE_HOSTS,
    COMPUTE_SLOTS,
    COMPUTE_USAGE_PATHS_MESSAGE,
    RELEASE_SLOT,
    REQUEST_SLOT,
    CandidateHost,
    CandidateHostsReply,
    CandidateHostsRequest,
    ComputeSlotRequest,
    SlotDecisionReply,
    SlotReleaseRequest,
)

COMPUTE_STATUS_NEW = "NEW"
COMPUTE_STATUS_PLACING = "PLACING"
COMPUTE_STATUS_WAITING_FOR_SLOT = "WAITING_FOR_SLOT"
COMPUTE_STATUS_RUNNING = "RUNNING"
COMPUTE_STATUS_COMPLETED = "COMPLETED"
COMPUTE_STATUS_FAILED_FINAL = "FAILED_FINAL"


@dataclass
class ComputeJobState(PagletState):
    home_host_name: str = ""
    home_host_url: str = ""
    compute_status: str = COMPUTE_STATUS_NEW
    compute_error: str = ""
    cpu_cores: int = 1
    memory_bytes: int = 0
    temp_storage_bytes: int = 0
    estimated_runtime_seconds: float = 0.0
    allow_zero_memory_bytes: bool = False
    allow_zero_runtime_seconds: bool = False
    allow_home_compute: bool = False
    required_host_tags: tuple[str, ...] = ()
    excluded_host_tags: tuple[str, ...] = ()
    preferred_host_tags: tuple[str, ...] = ()
    excluded_host_names: tuple[str, ...] = ()
    excluded_host_urls: tuple[str, ...] = ()
    candidate_limit: int = 8
    scheduler_timeout_seconds: float = 5.0
    release_timeout_seconds: float = 2.0
    selected_host_url: str = ""
    slot_request_id: str = ""
    slot_lease_id: str = ""
    redirect_count: int = 0
    last_redirect_at: float = 0.0
    last_redirect_from_host_url: str = ""
    cpu_core_ids: list[int] = field(default_factory=list)
    cpu_affinity_supported: bool = False
    cpu_affinity_enforced: bool = False
    cpu_affinity_error: str = ""
    restart_running_on_host_startup: bool = True
    restart_initial_state: dict[str, Any] = field(default_factory=dict)


StateT = TypeVar("StateT", bound=ComputeJobState)


class ComputeJobPaglet(Paglet[StateT], Generic[StateT]):
    """Base class for paglets that use the built-in compute-slot scheduler."""

    def __init__(self, state: StateT | None = None, *, agent_id: str | None = None):
        super().__init__(state=state, agent_id=agent_id)
        self._compute_thread: threading.Thread | None = None
        self._ensure_restart_initial_state()

    def run(self) -> None:
        self.start_compute_worker()

    def handle_message(self, message: Message):
        handled = self.handle_compute_slot_message(message)
        if handled is not None:
            return handled
        usage = self.handle_compute_usage_message(message)
        if usage is not None:
            return usage
        custom = self.handle_compute_job_message(message)
        if custom is not None:
            return custom
        return self.not_handled()

    def handle_compute_slot_message(self, message: Message) -> dict[str, bool] | None:
        if message.kind == "compute_slot_granted":
            reply = dataclass_from_wire(SlotDecisionReply, dict(message.args))
            with self.locked_state() as state:
                self._record_compute_slot_grant_locked(state, reply)
                state.compute_status = COMPUTE_STATUS_RUNNING
            self.start_compute_worker()
            return {"ok": True}
        if message.kind == "compute_slot_redirect":
            reply = dataclass_from_wire(SlotDecisionReply, dict(message.args))
            with self.locked_state() as state:
                self._record_compute_slot_redirect_locked(state, reply)
                state.compute_status = COMPUTE_STATUS_PLACING
            self.start_compute_worker()
            return {"ok": True}
        return None

    def handle_compute_job_message(self, message: Message) -> Any | None:
        _ = message
        return None

    def handle_compute_usage_message(self, message: Message) -> dict[str, list[str]] | None:
        if message.kind != COMPUTE_USAGE_PATHS_MESSAGE:
            return None
        paths = [str(path) for path in (self.compute_usage_paths() or ()) if str(path)]
        return {"paths": paths}

    def compute_usage_paths(self) -> Iterable[str | Path] | None:
        return ()

    def start_compute_worker(self) -> None:
        with self.locked():
            if self._compute_thread is not None and self._compute_thread.is_alive():
                return
            self._compute_thread = threading.Thread(
                target=self._advance_compute_job_safe,
                name=f"paglets-compute-job-{self.agent_id}",
                daemon=True,
            )
            self._compute_thread.start()

    def _advance_compute_job_safe(self) -> None:
        try:
            self.advance_compute_job()
        except Exception as exc:
            self._fail_compute_job(str(exc))

    def on_activation(self, event) -> None:
        _ = event
        should_start = False
        with self.locked_state() as state:
            if state.compute_status == COMPUTE_STATUS_RUNNING and state.restart_running_on_host_startup:
                self._restore_restart_initial_state_locked(state)
            should_start = state.compute_status in {
                COMPUTE_STATUS_NEW,
                COMPUTE_STATUS_PLACING,
                COMPUTE_STATUS_WAITING_FOR_SLOT,
            }
        if should_start:
            self.start_compute_worker()

    def advance_compute_job(self) -> None:
        with self.locked_state() as state:
            status = state.compute_status
        if status in {COMPUTE_STATUS_NEW, COMPUTE_STATUS_PLACING, COMPUTE_STATUS_WAITING_FOR_SLOT}:
            if self._place_or_request_compute_slot():
                self._run_granted_compute_job()
        elif status == COMPUTE_STATUS_RUNNING:
            self._run_granted_compute_job()
        elif status == COMPUTE_STATUS_COMPLETED:
            self.continue_after_compute_success()

    def _place_or_request_compute_slot(
        self,
        *,
        estimated_runtime_seconds: float | None = None,
        candidate_limit: int | None = None,
    ) -> bool:
        error = self.validate_compute_estimates()
        if error:
            self._fail_compute_job(error)
            return False
        selected = ""
        with self.locked_state() as state:
            self._ensure_compute_home_locked(state)
            estimated_runtime_seconds = (
                estimated_runtime_seconds
                if estimated_runtime_seconds is not None
                else max(0.0, float(state.estimated_runtime_seconds))
            )
            candidate_limit = candidate_limit if candidate_limit is not None else int(state.candidate_limit)
            if self.is_compute_home_locked(state):
                state.compute_status = COMPUTE_STATUS_PLACING
                slot = self._compute_slot_request_locked(
                    state,
                    estimated_runtime_seconds=estimated_runtime_seconds,
                )
            else:
                selected = state.selected_host_url.rstrip("/")
                slot = self._compute_slot_request_locked(
                    state,
                    estimated_runtime_seconds=estimated_runtime_seconds,
                )
        if self.is_compute_home():
            with self.locked_state() as state:
                if state.allow_home_compute:
                    return self._request_local_compute_slot(slot)
            self._dispatch_to_compute_candidate(slot, candidate_limit=candidate_limit)
            return False
        if selected and selected != self.context.address.rstrip("/"):
            self.dispatch(selected)
            return False
        return self._request_local_compute_slot(slot)

    def _dispatch_to_compute_candidate(self, slot: ComputeSlotRequest, *, candidate_limit: int = 8) -> None:
        handle = self.require_contract(COMPUTE_SLOTS, operation=CANDIDATE_HOSTS, scope=ServiceScope.MESH)
        with self.locked_state() as state:
            timeout = max(0.0, float(state.scheduler_timeout_seconds))
        reply = handle.call(
            CANDIDATE_HOSTS,
            CandidateHostsRequest(slot=slot, limit=candidate_limit, include_self=False),
            timeout=timeout,
        )
        candidates = [candidate for candidate in reply.candidates if self.accept_compute_candidate(candidate)]
        if not candidates:
            self._on_no_compute_candidate(reply)
            return
        chosen = reply.selected if reply.selected in candidates else None
        if chosen is None:
            rng = random.Random(slot.job_id or slot.request_id)
            chosen = rng.choice(candidates[: min(3, len(candidates))])
        with self.locked_state() as state:
            state.selected_host_url = chosen.status.host_url.rstrip("/")
            state.compute_status = COMPUTE_STATUS_PLACING
        self.dispatch(chosen.status.host_url)

    def _request_local_compute_slot(self, slot: ComputeSlotRequest) -> bool:
        handle = self.require_contract(COMPUTE_SLOTS, operation=REQUEST_SLOT, scope=ServiceScope.LOCAL)
        with self.locked_state() as state:
            timeout = max(0.0, float(state.scheduler_timeout_seconds))
        reply = handle.call(REQUEST_SLOT, slot, timeout=timeout)
        if reply.decision == "run_now":
            with self.locked_state() as state:
                self._record_compute_slot_grant_locked(state, reply)
                state.compute_status = COMPUTE_STATUS_RUNNING
            return True
        if reply.decision == "redirect" and reply.host_url:
            with self.locked_state() as state:
                self._record_compute_slot_redirect_locked(state, reply)
                state.compute_status = COMPUTE_STATUS_PLACING
            self.dispatch(reply.host_url)
            return False
        if reply.decision == "sleep":
            with self.locked_state() as state:
                state.compute_status = COMPUTE_STATUS_WAITING_FOR_SLOT
            self.deactivate(
                policy=DeactivationPolicy(
                    activate_on_message=True,
                    activate_on_startup=True,
                    queue_messages_when_inactive=True,
                )
            )
            return False
        self._on_compute_slot_rejected(reply)
        return False

    def _release_compute_slot(self) -> None:
        with self.locked_state() as state:
            lease_id = state.slot_lease_id
            state.slot_lease_id = ""
        if not lease_id:
            return
        try:
            handle = self.require_contract(COMPUTE_SLOTS, operation=RELEASE_SLOT, scope=ServiceScope.LOCAL)
            with self.locked_state() as state:
                timeout = max(0.0, float(state.release_timeout_seconds))
            handle.call(RELEASE_SLOT, SlotReleaseRequest(lease_id=lease_id, agent_id=self.agent_id), timeout=timeout)
        except Exception:
            pass

    def _run_granted_compute_job(self) -> None:
        try:
            self.run_compute_job()
        except Exception as exc:
            self._fail_compute_job(str(exc))
            return
        finally:
            self._release_compute_slot()
        with self.locked_state() as state:
            state.compute_status = COMPUTE_STATUS_COMPLETED
        self.after_compute_success()

    @final
    def compute_job_id(self) -> str:
        """Return the scheduler-facing runtime ID for this compute job."""
        return f"compute-job-{self.agent_id}"

    def run_compute_job(self) -> None:
        raise NotImplementedError

    def after_compute_success(self) -> None:
        """Run once immediately after successful compute and automatic lease release."""
        self.continue_after_compute_success()

    def continue_after_compute_success(self) -> None:
        """Run after compute completion, including later wakeups of a completed job."""
        return

    def after_compute_failure(self, message: str) -> None:
        _ = message

    def validate_compute_estimates(self) -> str:
        with self.locked_state() as state:
            if int(state.cpu_cores) < 1:
                return "cpu_cores must be at least 1"
            if float(state.estimated_runtime_seconds) <= 0.0 and not state.allow_zero_runtime_seconds:
                return "estimated_runtime_seconds must be greater than 0"
            if int(state.memory_bytes) <= 0 and not state.allow_zero_memory_bytes:
                return "memory_bytes must be greater than 0"
            if int(state.temp_storage_bytes) < 0:
                return "temp_storage_bytes must be greater than or equal to 0"
        return ""

    def notify_user(
        self,
        severity: str,
        title: str,
        message: str,
        *,
        job_id: str | None = None,
        timeout: float = 2.0,
        scope: ServiceScope = ServiceScope.MESH,
    ) -> bool:
        try:
            from paglets.system.user_info import NOTIFY_USER, USER_INFO, UserInfoRequest

            handle = self.require_contract(USER_INFO, operation=NOTIFY_USER, scope=scope)
            handle.call(
                NOTIFY_USER,
                UserInfoRequest(
                    severity=severity,
                    title=title,
                    message=message,
                    source_agent_id=self.agent_id,
                    job_id=job_id if job_id is not None else self.compute_job_id(),
                    timestamp=time.time(),
                ),
                timeout=max(0.0, float(timeout)),
            )
            return True
        except Exception:
            return False

    def _compute_slot_request_locked(
        self,
        state: StateT,
        *,
        estimated_runtime_seconds: float,
    ) -> ComputeSlotRequest:
        if not state.slot_request_id:
            state.slot_request_id = f"compute-slot-{uuid.uuid4().hex}"
        return ComputeSlotRequest(
            request_id=state.slot_request_id,
            agent_id=self.agent_id,
            agent_host_url=self.context.address.rstrip("/"),
            job_id=self.compute_job_id(),
            cpu_cores=state.cpu_cores,
            memory_bytes=state.memory_bytes,
            temp_storage_bytes=state.temp_storage_bytes,
            estimated_runtime_seconds=estimated_runtime_seconds,
            required_host_tags=tuple(state.required_host_tags),
            excluded_host_tags=tuple(state.excluded_host_tags),
            preferred_host_tags=tuple(state.preferred_host_tags),
            excluded_host_names=tuple(state.excluded_host_names),
            excluded_host_urls=tuple(state.excluded_host_urls),
            submitted_at=time.time(),
            redirect_count=state.redirect_count,
            last_redirect_at=state.last_redirect_at,
            last_redirect_from_host_url=state.last_redirect_from_host_url,
        )

    def accept_compute_candidate(self, candidate: CandidateHost) -> bool:
        with self.locked_state() as state:
            if state.allow_home_compute:
                return True
            return candidate.status.host_url.rstrip("/") != state.home_host_url.rstrip("/") and (
                candidate.status.host_name != state.home_host_name
            )

    def _on_no_compute_candidate(self, reply: CandidateHostsReply) -> None:
        _ = reply
        self._on_compute_slot_error("no suitable compute host")

    def _on_compute_slot_rejected(self, reply: SlotDecisionReply) -> None:
        self._on_compute_slot_error(reply.message or f"slot request rejected: {reply.decision}")

    def _on_compute_slot_error(self, message: str) -> None:
        self._fail_compute_job(message)

    def _fail_compute_job(self, message: str) -> None:
        self._release_compute_slot()
        with self.locked_state() as state:
            state.compute_status = COMPUTE_STATUS_FAILED_FINAL
            state.compute_error = message
        self.after_compute_failure(message)

    def deactivation_policy(self, request: DeactivationRequest) -> DeactivationPolicy:
        with self.locked_state() as state:
            if request.reason == "shutdown" and state.compute_status == COMPUTE_STATUS_RUNNING:
                if not state.restart_running_on_host_startup:
                    return request.policy or DeactivationPolicy()
                return DeactivationPolicy(
                    activate_on_message=True,
                    activate_on_startup=True,
                    queue_messages_when_inactive=True,
                )
        return super().deactivation_policy(request)

    def on_deactivating(self, event) -> None:
        _ = event
        with self.locked_state() as state:
            if state.compute_status == COMPUTE_STATUS_RUNNING and state.restart_running_on_host_startup:
                self._restore_restart_initial_state_locked(state)

    def _ensure_compute_home_locked(self, state: ComputeJobState) -> None:
        current_url = self.context.address.rstrip("/")
        if not state.home_host_name and not state.home_host_url:
            state.home_host_name = self.context.name
            state.home_host_url = current_url
            return
        if self.is_compute_home_locked(state):
            if not state.home_host_name:
                state.home_host_name = self.context.name
            if not state.home_host_url:
                state.home_host_url = current_url

    def _ensure_restart_initial_state(self) -> None:
        with self.locked_state() as state:
            if state.restart_initial_state:
                return
            if state.compute_status != COMPUTE_STATUS_NEW:
                return
            state.restart_initial_state = dataclass_to_wire(replace(state, restart_initial_state={}))

    def _restore_restart_initial_state_locked(self, state: StateT) -> None:
        if not state.restart_initial_state:
            state.compute_status = COMPUTE_STATUS_NEW
            state.compute_error = ""
            state.slot_lease_id = ""
            state.slot_request_id = ""
            state.cpu_core_ids = []
            state.cpu_affinity_supported = False
            state.cpu_affinity_enforced = False
            state.cpu_affinity_error = ""
            return
        restored = dataclass_from_wire(type(state), state.restart_initial_state)
        current_initial = dict(state.restart_initial_state)
        for item in fields(state):
            setattr(state, item.name, getattr(restored, item.name))
        state.restart_initial_state = current_initial
        state.restart_running_on_host_startup = restored.restart_running_on_host_startup

    @staticmethod
    def _record_compute_slot_grant_locked(state: ComputeJobState, reply: SlotDecisionReply) -> None:
        state.slot_lease_id = reply.lease_id
        state.cpu_core_ids = list(reply.cpu_core_ids)
        state.cpu_affinity_supported = bool(reply.cpu_affinity_supported)
        state.cpu_affinity_enforced = bool(reply.cpu_affinity_enforced)
        state.cpu_affinity_error = reply.cpu_affinity_error

    @staticmethod
    def _record_compute_slot_redirect_locked(state: ComputeJobState, reply: SlotDecisionReply) -> None:
        state.selected_host_url = reply.host_url.rstrip("/")
        state.redirect_count = max(state.redirect_count, int(reply.redirect_count))
        state.last_redirect_at = time.time()
        state.last_redirect_from_host_url = reply.redirected_from_host_url.rstrip("/")

    def is_compute_home(self) -> bool:
        with self.locked_state() as state:
            return self.is_compute_home_locked(state)

    def is_compute_home_locked(self, state: ComputeJobState) -> bool:
        return self.context.name == state.home_host_name or self.context.address.rstrip(
            "/"
        ) == state.home_host_url.rstrip("/")


__all__ = [
    "COMPUTE_STATUS_COMPLETED",
    "COMPUTE_STATUS_FAILED_FINAL",
    "COMPUTE_STATUS_NEW",
    "COMPUTE_STATUS_PLACING",
    "COMPUTE_STATUS_RUNNING",
    "COMPUTE_STATUS_WAITING_FOR_SLOT",
    "ComputeJobPaglet",
    "ComputeJobState",
]
