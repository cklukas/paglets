# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from paglets.core.messages import Message
from paglets.persistence.persistency import DeactivationRequest
from paglets.serialization.codec import dataclass_to_wire
from paglets.system.compute_slots import (
    COMPUTE_STATUS_COMPLETED,
    COMPUTE_STATUS_FAILED_FINAL,
    COMPUTE_STATUS_NEW,
    COMPUTE_STATUS_PLACING,
    COMPUTE_STATUS_RUNNING,
    COMPUTE_STATUS_WAITING_FOR_SLOT,
    CandidateHost,
    CandidateHostsReply,
    ComputeJobPaglet,
    ComputeJobState,
    ComputeSlotRequest,
    SchedulerHostStatus,
    SlotDecisionReply,
)
from paglets.system.compute_slots.agent import REQUEST_SLOT


@dataclass
class DemoComputeState(ComputeJobState):
    estimated_runtime_seconds: float = 10.0
    memory_bytes: int = 1024 * 1024
    events: list[str] = field(default_factory=list)
    fail_during_compute: bool = False


class DemoComputePaglet(ComputeJobPaglet[DemoComputeState]):
    State = DemoComputeState

    def run_compute_job(self) -> None:
        with self.locked_state() as state:
            state.events.append("run")
            if state.fail_during_compute:
                raise RuntimeError("compute failed")

    def after_compute_success(self) -> None:
        with self.locked_state() as state:
            state.events.append("success")

    def after_compute_failure(self, message: str) -> None:
        with self.locked_state() as state:
            state.events.append(f"failure:{message}")


class ContinuingComputePaglet(DemoComputePaglet):
    def continue_after_compute_success(self) -> None:
        with self.locked_state() as state:
            state.events.append("continue")


def test_compute_slot_grant_wakes_and_advances_without_subclass_boilerplate():
    paglet = DemoComputePaglet(DemoComputeState())
    reply = SlotDecisionReply(
        decision="run_now",
        request_id="request-0",
        lease_id="lease-0",
        cpu_core_ids=[0],
    )

    result = paglet.handle_message(Message("compute_slot_granted", dataclass_to_wire(reply)))

    assert result == {"ok": True}
    _wait_until(lambda: paglet.state.events == ["run", "success"])
    assert paglet.state.compute_status == COMPUTE_STATUS_COMPLETED
    assert paglet.state.slot_lease_id == ""


def test_compute_slot_redirect_records_metadata_and_starts_placement():
    paglet = DemoComputePaglet(DemoComputeState())
    started: list[bool] = []
    paglet.start_compute_worker = lambda: started.append(True)  # type: ignore[method-assign]
    reply = SlotDecisionReply(
        decision="redirect",
        request_id="request-0",
        host_url="http://beta",
        redirect_count=2,
        redirected_from_host_url="http://alpha",
    )

    result = paglet.handle_message(Message("compute_slot_redirect", dataclass_to_wire(reply)))

    assert result == {"ok": True}
    assert started == [True]
    assert paglet.state.compute_status == COMPUTE_STATUS_PLACING
    assert paglet.state.selected_host_url == "http://beta"
    assert paglet.state.redirect_count == 2
    assert paglet.state.last_redirect_from_host_url == "http://alpha"


def test_compute_job_default_failure_records_status_and_error():
    paglet = DemoComputePaglet(DemoComputeState())

    paglet._on_no_compute_candidate(CandidateHostsReply(generated_at=time.time()))

    assert paglet.state.compute_status == COMPUTE_STATUS_FAILED_FINAL
    assert paglet.state.compute_error == "no suitable compute host"
    assert paglet.state.events == ["failure:no suitable compute host"]


def test_compute_job_id_is_derived_from_runtime_agent_id():
    paglet = DemoComputePaglet(DemoComputeState(), agent_id="agent-123")

    first = paglet.compute_job_id()
    second = paglet.compute_job_id()

    assert first == "compute-job-agent-123"
    assert second == first


def test_compute_job_home_host_is_captured_automatically():
    paglet = DemoComputePaglet(DemoComputeState())
    paglet._attach(_FakeContext(name="laptop", address="http://laptop:8765"))  # type: ignore[arg-type]

    with paglet.locked_state() as state:
        paglet._ensure_compute_home_locked(state)

    assert paglet.state.home_host_name == "laptop"
    assert paglet.state.home_host_url == "http://laptop:8765"


def test_compute_slot_request_uses_state_runtime_estimate_and_host_policy():
    paglet = DemoComputePaglet(
        DemoComputeState(
            estimated_runtime_seconds=42.5,
            required_host_tags=("linux",),
            excluded_host_tags=("laptop",),
            preferred_host_tags=("gpu",),
            excluded_host_names=("collector",),
            excluded_host_urls=("http://home",),
        )
    )
    paglet._attach(_FakeContext(name="alpha", address="http://alpha:8765"))  # type: ignore[arg-type]

    with paglet.locked_state() as state:
        request = paglet._compute_slot_request_locked(state, estimated_runtime_seconds=state.estimated_runtime_seconds)

    assert request.estimated_runtime_seconds == 42.5
    assert request.job_id == paglet.compute_job_id()
    assert request.required_host_tags == ("linux",)
    assert request.excluded_host_tags == ("laptop",)
    assert request.preferred_host_tags == ("gpu",)
    assert request.excluded_host_names == ("collector",)
    assert request.excluded_host_urls == ("http://home",)


def test_compute_job_success_and_failure_release_lease():
    success = DemoComputePaglet(DemoComputeState(slot_lease_id="lease-success"))
    success._run_granted_compute_job()

    failure = DemoComputePaglet(DemoComputeState(slot_lease_id="lease-failure", fail_during_compute=True))
    failure._run_granted_compute_job()

    assert success.state.slot_lease_id == ""
    assert success.state.events == ["run", "success"]
    assert success.state.compute_status == COMPUTE_STATUS_COMPLETED
    assert failure.state.slot_lease_id == ""
    assert failure.state.compute_status == COMPUTE_STATUS_FAILED_FINAL
    assert failure.state.compute_error == "compute failed"
    assert failure.state.events == ["run", "failure:compute failed"]


def test_compute_job_completed_status_runs_continuation_hook():
    paglet = ContinuingComputePaglet(DemoComputeState(compute_status=COMPUTE_STATUS_COMPLETED))

    paglet.advance_compute_job()

    assert paglet.state.events == ["continue"]


def test_compute_job_home_candidate_policy_is_configurable():
    candidate = CandidateHost(
        status=SchedulerHostStatus(
            host_name="home",
            host_url="http://home",
            observed_at=time.time(),
        ),
        score=0.0,
    )
    disallowed = DemoComputePaglet(DemoComputeState(home_host_name="home", home_host_url="http://home"))
    allowed = DemoComputePaglet(
        DemoComputeState(home_host_name="home", home_host_url="http://home", allow_home_compute=True)
    )

    assert disallowed.accept_compute_candidate(candidate) is False
    assert allowed.accept_compute_candidate(candidate) is True


def test_compute_job_scheduler_and_release_timeouts_are_configurable():
    paglet = DemoComputePaglet(
        DemoComputeState(
            scheduler_timeout_seconds=12.5,
            release_timeout_seconds=3.5,
            slot_lease_id="lease-0",
        )
    )
    calls: list[tuple[Any, float | None]] = []

    class Handle:
        def call(self, operation, payload, *, timeout=None, **kwargs):
            calls.append((operation, timeout))
            if operation == REQUEST_SLOT:
                return SlotDecisionReply(decision="sleep", request_id=payload.request_id)
            return {"ok": True}

    paglet.require_contract = lambda *args, **kwargs: Handle()  # type: ignore[method-assign]
    paglet.deactivate = lambda *args, **kwargs: None  # type: ignore[method-assign]
    slot = ComputeSlotRequest(request_id="request-0", agent_id=paglet.agent_id)

    assert paglet._request_local_compute_slot(slot) is False
    paglet._release_compute_slot()

    assert calls[0] == (REQUEST_SLOT, 12.5)
    assert calls[1][1] == 3.5
    assert paglet.state.compute_status == COMPUTE_STATUS_WAITING_FOR_SLOT


def test_compute_job_sleep_deactivation_is_startup_recoverable():
    paglet = DemoComputePaglet(DemoComputeState())
    policies: list[Any] = []

    class Handle:
        def call(self, operation, payload, *, timeout=None, **kwargs):
            return SlotDecisionReply(decision="sleep", request_id=payload.request_id)

    paglet.require_contract = lambda *args, **kwargs: Handle()  # type: ignore[method-assign]
    paglet.deactivate = lambda *args, **kwargs: policies.append(kwargs["policy"])  # type: ignore[method-assign]

    assert paglet._request_local_compute_slot(ComputeSlotRequest(request_id="request-0")) is False

    [policy] = policies
    assert policy.activate_on_message is True
    assert policy.activate_on_startup is True
    assert policy.queue_messages_when_inactive is True


def test_waiting_compute_job_requeues_slot_request_after_startup_activation():
    paglet = DemoComputePaglet(
        DemoComputeState(
            allow_home_compute=True,
            compute_status=COMPUTE_STATUS_WAITING_FOR_SLOT,
            home_host_name="alpha",
            home_host_url="http://alpha:8765",
            slot_request_id="request-existing",
        )
    )
    paglet._attach(_FakeContext(name="alpha", address="http://alpha:8765"))  # type: ignore[arg-type]
    requests: list[ComputeSlotRequest] = []
    policies: list[Any] = []

    class Handle:
        def call(self, operation, payload, *, timeout=None, **kwargs):
            requests.append(payload)
            return SlotDecisionReply(decision="sleep", request_id=payload.request_id)

    paglet.require_contract = lambda *args, **kwargs: Handle()  # type: ignore[method-assign]
    paglet.deactivate = lambda *args, **kwargs: policies.append(kwargs["policy"])  # type: ignore[method-assign]

    paglet.advance_compute_job()

    [request] = requests
    assert request.request_id == "request-existing"
    assert request.agent_id == paglet.agent_id
    assert paglet.state.compute_status == COMPUTE_STATUS_WAITING_FOR_SLOT
    assert len(policies) == 1


def test_running_compute_job_shutdown_restarts_from_submitted_state():
    paglet = DemoComputePaglet(DemoComputeState(allow_home_compute=True))
    with paglet.locked_state() as state:
        state.compute_status = COMPUTE_STATUS_RUNNING
        state.compute_error = "old error"
        state.slot_request_id = "request-running"
        state.slot_lease_id = "lease-running"
        state.cpu_core_ids = [0, 1]
        state.events.append("running")

    policy = paglet.deactivation_policy(DeactivationRequest(reason="shutdown", source="host"))
    paglet.on_deactivating(None)

    assert policy.activate_on_startup is True
    assert policy.activate_on_message is True
    assert policy.queue_messages_when_inactive is True
    assert paglet.state.compute_status == COMPUTE_STATUS_NEW
    assert paglet.state.compute_error == ""
    assert paglet.state.slot_request_id == ""
    assert paglet.state.slot_lease_id == ""
    assert paglet.state.cpu_core_ids == []
    assert paglet.state.events == []


def test_running_compute_job_restart_policy_can_be_disabled():
    paglet = DemoComputePaglet(DemoComputeState(restart_running_on_host_startup=False))
    with paglet.locked_state() as state:
        state.compute_status = COMPUTE_STATUS_RUNNING
        state.slot_lease_id = "lease-running"
        state.events.append("running")

    policy = paglet.deactivation_policy(DeactivationRequest(reason="shutdown", source="host"))
    paglet.on_deactivating(None)

    assert policy.activate_on_startup is False
    assert paglet.state.compute_status == COMPUTE_STATUS_RUNNING
    assert paglet.state.slot_lease_id == "lease-running"
    assert paglet.state.events == ["running"]


def test_running_compute_job_without_initial_snapshot_restarts_fresh_on_activation():
    paglet = DemoComputePaglet(
        DemoComputeState(
            compute_status=COMPUTE_STATUS_RUNNING,
            slot_request_id="request-running",
            slot_lease_id="lease-running",
            events=["running"],
        )
    )

    paglet.on_activation(None)

    assert paglet.state.compute_status == COMPUTE_STATUS_NEW
    assert paglet.state.compute_error == ""
    assert paglet.state.slot_request_id == ""
    assert paglet.state.slot_lease_id == ""
    assert paglet.state.cpu_core_ids == []
    assert paglet.state.events == ["running"]


def test_compute_job_validation_rejects_invalid_estimates_before_scheduler_contact():
    paglet = DemoComputePaglet(DemoComputeState(estimated_runtime_seconds=0.0))
    paglet._attach(_FakeContext(name="alpha", address="http://alpha:8765"))  # type: ignore[arg-type]
    calls: list[str] = []
    paglet.require_contract = lambda *args, **kwargs: calls.append("called")  # type: ignore[method-assign]

    assert paglet._place_or_request_compute_slot() is False

    assert calls == []
    assert paglet.state.compute_status == COMPUTE_STATUS_FAILED_FINAL
    assert paglet.state.compute_error == "estimated_runtime_seconds must be greater than 0"
    assert paglet.state.events == ["failure:estimated_runtime_seconds must be greater than 0"]


def test_compute_job_validation_rejects_missing_memory():
    paglet = DemoComputePaglet(DemoComputeState(memory_bytes=0))

    assert paglet.validate_compute_estimates() == "memory_bytes must be greater than 0"


def test_compute_job_validation_rejects_invalid_cpu_cores():
    paglet = DemoComputePaglet(DemoComputeState(cpu_cores=0))

    assert paglet.validate_compute_estimates() == "cpu_cores must be at least 1"


def test_compute_job_validation_allows_zero_temp_storage():
    paglet = DemoComputePaglet(DemoComputeState(temp_storage_bytes=0))

    assert paglet.validate_compute_estimates() == ""


def test_compute_job_validation_allows_explicit_zero_estimate_opt_outs():
    paglet = DemoComputePaglet(
        DemoComputeState(
            estimated_runtime_seconds=0.0,
            memory_bytes=0,
            allow_zero_runtime_seconds=True,
            allow_zero_memory_bytes=True,
        )
    )

    assert paglet.validate_compute_estimates() == ""


class _FakeContext:
    def __init__(self, *, name: str, address: str):
        self.name = name
        self.address = address


def _wait_until(predicate, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not reached before timeout")
