# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from paglets.system.compute_slots import (
    CancelSlotRequestsRequest,
    ComputeJobRuntimeInfo,
    ComputeSlotRequest,
    CpuAffinityResult,
    SchedulerHostStatus,
    SchedulerStatusRequest,
    SlotLease,
    SlotReleaseRequest,
)
from paglets.system.compute_slots.agent import (
    COMPUTE_SLOTS_EXPIRED_ACTIVE_LEASE_ERROR_KEY,
    ComputeSlotsAgent,
    ComputeSlotsState,
    _can_ever_satisfy,
    _can_run_now,
    _candidate_score,
    _directory_usage,
    _elastic_cpu_assignments,
    _paths_usage,
    _redirect_budget,
)


def affinity_result(cpu_core_ids: list[int], *, enforced: bool) -> CpuAffinityResult:
    return CpuAffinityResult(
        requested_cpu_ids=list(cpu_core_ids),
        supported=True,
        enforced=enforced,
        error="" if enforced else "not enforced",
    )


def test_compute_slot_rejects_gpu_required_hosts_when_gpu_unsupported():
    status = SchedulerHostStatus(
        host_name="alpha",
        host_url="http://alpha",
        observed_at=time.time(),
        supports_gpu_jobs=False,
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
    )
    request = ComputeSlotRequest(requires_gpu=True, gpu_memory_mb=4096)

    assert _can_ever_satisfy(status, request) == "GPU jobs unsupported"


def test_compute_slot_distinguishes_eventual_fit_from_current_capacity():
    status = SchedulerHostStatus(
        host_name="alpha",
        host_url="http://alpha",
        observed_at=time.time(),
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        memory_available_bytes=8 * 1024**3,
        work_total_bytes=100 * 1024**3,
        work_free_bytes=50 * 1024**3,
        free_cpu_cores=0,
        free_memory_bytes=8 * 1024**3,
        free_temp_storage_bytes=50 * 1024**3,
    )
    request = ComputeSlotRequest(cpu_cores=2, memory_bytes=2 * 1024**3, temp_storage_bytes=1024**3)

    assert _can_ever_satisfy(status, request) == ""
    assert _can_run_now(status, request) is False


def test_compute_slot_can_run_when_reserved_free_resources_fit():
    status = SchedulerHostStatus(
        host_name="alpha",
        host_url="http://alpha",
        observed_at=time.time(),
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        memory_available_bytes=8 * 1024**3,
        work_total_bytes=100 * 1024**3,
        work_free_bytes=50 * 1024**3,
        free_cpu_cores=4,
        free_memory_bytes=8 * 1024**3,
        free_temp_storage_bytes=50 * 1024**3,
    )
    request = ComputeSlotRequest(cpu_cores=2, memory_bytes=2 * 1024**3, temp_storage_bytes=1024**3)

    assert _can_run_now(status, request) is True


def test_compute_slot_direct_requests_respect_reserved_capacity():
    agent = ComputeSlotsAgent(ComputeSlotsState())
    agent._context = SimpleNamespace(address="http://alpha")  # type: ignore[assignment]

    def local_status():
        reserved = agent._reserved_resources()
        return SchedulerHostStatus(
            host_name="alpha",
            host_url="http://alpha",
            observed_at=time.time(),
            cpu_count_logical=8,
            memory_total_bytes=16 * 1024**3,
            memory_available_bytes=16 * 1024**3,
            work_total_bytes=100 * 1024**3,
            work_free_bytes=100 * 1024**3,
            free_cpu_cores=max(0, 8 - reserved["cpu"]),
            free_memory_bytes=max(0, 16 * 1024**3 - reserved["memory"]),
            free_temp_storage_bytes=max(0, 100 * 1024**3 - reserved["storage"]),
        )

    agent._local_status = local_status  # type: ignore[method-assign]
    requests = [
        ComputeSlotRequest(
            request_id=f"request-{index}",
            agent_id=f"agent-{index}",
            cpu_cores=2,
            memory_bytes=2 * 1024**3,
            temp_storage_bytes=10 * 1024**3,
        )
        for index in range(5)
    ]

    replies = [agent.request_slot(request) for request in requests]

    assert [reply.decision for reply in replies] == ["run_now", "run_now", "run_now", "sleep", "sleep"]
    with agent.locked_state() as state:
        assert len(state.leases) == 3
        assert len(state.queued_requests) == 2


def test_compute_slot_rejects_request_larger_than_eligible_cpu_set():
    status = SchedulerHostStatus(
        host_name="alpha",
        host_url="http://alpha",
        observed_at=time.time(),
        cpu_count_logical=8,
        eligible_cpu_ids=[0, 1],
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
    )
    request = ComputeSlotRequest(cpu_cores=3, memory_bytes=1024, temp_storage_bytes=1024)

    assert _can_ever_satisfy(status, request) == "requested CPU cores exceed host eligible CPU count"


def test_compute_slot_host_tag_and_exclusion_policies_are_hard_constraints():
    status = SchedulerHostStatus(
        host_name="linux-a",
        host_url="http://linux-a",
        observed_at=time.time(),
        cpu_count_logical=8,
        host_tags=("linux", "gpu"),
    )

    assert _can_ever_satisfy(status, ComputeSlotRequest(required_host_tags=("linux",))) == ""
    assert (
        _can_ever_satisfy(status, ComputeSlotRequest(required_host_tags=("windows",)))
        == "missing required host tags: windows"
    )
    assert (
        _can_ever_satisfy(status, ComputeSlotRequest(excluded_host_tags=("gpu",))) == "excluded host tags present: gpu"
    )
    assert _can_ever_satisfy(status, ComputeSlotRequest(excluded_host_names=("linux-a",))) == "host name excluded"
    assert _can_ever_satisfy(status, ComputeSlotRequest(excluded_host_urls=("http://linux-a",))) == "host URL excluded"


def test_compute_slot_preferred_tags_lower_candidate_score_without_requiring_match():
    plain = SchedulerHostStatus(
        host_name="linux-a",
        host_url="http://linux-a",
        observed_at=time.time(),
        cpu_count_logical=8,
        free_cpu_cores=8,
        host_tags=("linux",),
    )
    gpu = SchedulerHostStatus(
        host_name="linux-b",
        host_url="http://linux-b",
        observed_at=time.time(),
        cpu_count_logical=8,
        free_cpu_cores=8,
        host_tags=("linux", "gpu"),
    )
    request = ComputeSlotRequest(preferred_host_tags=("gpu",))

    assert _can_ever_satisfy(plain, request) == ""
    assert _candidate_score(gpu, request) < _candidate_score(plain, request)


def test_compute_slot_grant_reserves_exact_cpu_ids_when_affinity_supported():
    request = ComputeSlotRequest(request_id="request-0", agent_id="agent-0", cpu_cores=2)
    status = SchedulerHostStatus(
        host_name="alpha",
        host_url="http://alpha",
        observed_at=time.time(),
        cpu_count_logical=8,
        cpu_affinity_supported=True,
        eligible_cpu_ids=[0, 1, 2, 3],
        memory_total_bytes=16 * 1024**3,
        work_dir_base="/tmp/paglets",
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=4,
        free_memory_bytes=16 * 1024**3,
        free_temp_storage_bytes=100 * 1024**3,
    )
    agent = ComputeSlotsAgent(ComputeSlotsState())
    applied: list[tuple[str, list[int]]] = []
    agent._set_agent_cpu_affinity = (  # type: ignore[method-assign]
        lambda agent_id, cpu_ids: (
            applied.append((agent_id, list(cpu_ids))) or affinity_result(list(cpu_ids), enforced=True)
        )
    )

    reply = agent._grant_now(request, status, send_message=False)

    assert reply.cpu_core_ids == [0, 1]
    assert reply.cpu_affinity_supported is True
    assert reply.cpu_affinity_enforced is True
    assert applied == [("agent-0", [0, 1])]
    with agent.locked_state() as state:
        [lease_wire] = list(state.leases.values())
    lease = dataclass_from_wire(SlotLease, lease_wire)
    assert lease.cpu_core_ids == [0, 1]
    assert lease.reserved_cpu_core_ids == [0, 1]


def test_compute_slot_grant_skips_cpu_ids_already_reserved_by_leases():
    first_request = ComputeSlotRequest(request_id="request-0", agent_id="agent-0", cpu_cores=2)
    first_lease = SlotLease(
        lease_id="lease-0",
        request=first_request,
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=time.time(),
        expires_at=time.time() + 60.0,
        cpu_core_ids=[0, 1],
        cpu_affinity_supported=True,
    )
    request = ComputeSlotRequest(request_id="request-1", agent_id="agent-1", cpu_cores=1)
    status = SchedulerHostStatus(
        host_name="alpha",
        host_url="http://alpha",
        observed_at=time.time(),
        cpu_count_logical=8,
        cpu_affinity_supported=True,
        eligible_cpu_ids=[0, 1, 2, 3],
        memory_total_bytes=16 * 1024**3,
        work_dir_base="/tmp/paglets",
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=2,
        free_memory_bytes=16 * 1024**3,
        free_temp_storage_bytes=100 * 1024**3,
    )
    agent = ComputeSlotsAgent(ComputeSlotsState(leases={first_lease.lease_id: dataclass_to_wire(first_lease)}))
    agent._set_agent_cpu_affinity = lambda agent_id, cpu_ids: affinity_result(list(cpu_ids), enforced=True)  # type: ignore[method-assign]

    reply = agent._grant_now(request, status, send_message=False)

    assert reply.cpu_core_ids == [2]


def test_compute_slot_elastic_affinity_spreads_spare_cpus_evenly():
    now = time.time()
    first = SlotLease(
        lease_id="lease-0",
        request=ComputeSlotRequest(request_id="request-0", agent_id="agent-0", cpu_cores=2),
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=now,
        expires_at=now + 60.0,
        cpu_core_ids=[0, 1],
        reserved_cpu_core_ids=[0, 1],
        cpu_affinity_supported=True,
    )
    second = SlotLease(
        lease_id="lease-1",
        request=ComputeSlotRequest(request_id="request-1", agent_id="agent-1", cpu_cores=2),
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=now,
        expires_at=now + 60.0,
        cpu_core_ids=[2, 3],
        reserved_cpu_core_ids=[2, 3],
        cpu_affinity_supported=True,
    )

    assignments = _elastic_cpu_assignments(list(range(10)), [first, second])

    assert len(assignments["lease-0"]) == 5
    assert len(assignments["lease-1"]) == 5
    assert sorted(assignments["lease-0"] + assignments["lease-1"]) == list(range(10))


def test_compute_slot_cleanup_removes_lease_for_missing_active_agent():
    lease = SlotLease(
        lease_id="lease-0",
        request=ComputeSlotRequest(request_id="request-0", agent_id="missing", cpu_cores=1),
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=time.time(),
        expires_at=time.time() + 60.0,
        cpu_core_ids=[0],
        reserved_cpu_core_ids=[0],
        cpu_affinity_supported=True,
    )
    agent = ComputeSlotsAgent(ComputeSlotsState(leases={lease.lease_id: dataclass_to_wire(lease)}))
    agent._is_agent_active = lambda agent_id: False  # type: ignore[method-assign]

    agent._cleanup_inactive_leases()

    with agent.locked_state() as state:
        assert state.leases == {}


def test_compute_slot_cancel_by_agent_id_removes_matching_queued_requests():
    requests = [
        ComputeSlotRequest(request_id="request-0", agent_id="agent-0", job_id="job-0"),
        ComputeSlotRequest(request_id="request-1", agent_id="agent-1", job_id="job-1"),
        ComputeSlotRequest(request_id="request-2", agent_id="agent-0", job_id="job-2"),
    ]
    agent = ComputeSlotsAgent(ComputeSlotsState(queued_requests=[dataclass_to_wire(item) for item in requests]))

    reply = agent.cancel_slot_requests(CancelSlotRequestsRequest(agent_ids=("agent-0",)))

    assert reply.cancelled_requests == 2
    assert reply.cancelled_leases == 0
    with agent.locked_state() as state:
        remaining = [dataclass_from_wire(ComputeSlotRequest, item).request_id for item in state.queued_requests]
        assert remaining == ["request-1"]


def test_compute_slot_cancel_all_clears_queued_requests_without_leases_by_default():
    request = ComputeSlotRequest(request_id="request-0", agent_id="agent-0", job_id="job-0")
    lease = SlotLease(
        lease_id="lease-0",
        request=request,
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=time.time(),
        expires_at=time.time() + 60.0,
    )
    agent = ComputeSlotsAgent(
        ComputeSlotsState(
            queued_requests=[dataclass_to_wire(request)],
            leases={lease.lease_id: dataclass_to_wire(lease)},
        )
    )

    reply = agent.cancel_slot_requests(CancelSlotRequestsRequest(all=True))

    assert reply.cancelled_requests == 1
    assert reply.cancelled_leases == 0
    with agent.locked_state() as state:
        assert state.queued_requests == []
        assert list(state.leases) == ["lease-0"]


def test_compute_slot_cancel_can_include_matching_leases():
    keep_request = ComputeSlotRequest(request_id="request-keep", agent_id="agent-1", job_id="job-1")
    cancel_request = ComputeSlotRequest(request_id="request-cancel", agent_id="agent-0", job_id="job-0")
    keep_lease = SlotLease(
        lease_id="lease-keep",
        request=keep_request,
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=time.time(),
        expires_at=time.time() + 60.0,
    )
    cancel_lease = SlotLease(
        lease_id="lease-cancel",
        request=cancel_request,
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=time.time(),
        expires_at=time.time() + 60.0,
    )
    agent = ComputeSlotsAgent(
        ComputeSlotsState(
            queued_requests=[dataclass_to_wire(keep_request), dataclass_to_wire(cancel_request)],
            leases={
                keep_lease.lease_id: dataclass_to_wire(keep_lease),
                cancel_lease.lease_id: dataclass_to_wire(cancel_lease),
            },
        )
    )

    reply = agent.cancel_slot_requests(CancelSlotRequestsRequest(job_ids=("job-0",), include_leases=True))

    assert reply.cancelled_requests == 1
    assert reply.cancelled_leases == 1
    with agent.locked_state() as state:
        assert [dataclass_from_wire(ComputeSlotRequest, item).request_id for item in state.queued_requests] == [
            "request-keep"
        ]
        assert list(state.leases) == ["lease-keep"]


def test_compute_slot_cancel_without_filter_is_noop():
    request = ComputeSlotRequest(request_id="request-0", agent_id="agent-0", job_id="job-0")
    agent = ComputeSlotsAgent(ComputeSlotsState(queued_requests=[dataclass_to_wire(request)]))

    reply = agent.cancel_slot_requests(CancelSlotRequestsRequest())

    assert reply.cancelled_requests == 0
    assert reply.cancelled_leases == 0
    with agent.locked_state() as state:
        assert state.queued_requests == [dataclass_to_wire(request)]


def test_compute_slot_expiry_retains_reservation_for_active_agent():
    now = time.time()
    lease = SlotLease(
        lease_id="lease-0",
        request=ComputeSlotRequest(request_id="request-0", agent_id="agent-0", cpu_cores=2),
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=now - 120.0,
        expires_at=now - 60.0,
        cpu_core_ids=[0, 1],
        reserved_cpu_core_ids=[0, 1],
    )
    agent = ComputeSlotsAgent(ComputeSlotsState(leases={lease.lease_id: dataclass_to_wire(lease)}))
    agent._is_agent_active = lambda agent_id: True  # type: ignore[method-assign]

    agent._expire_leases()

    with agent.locked_state() as state:
        [lease_wire] = list(state.leases.values())
        retained = dataclass_from_wire(SlotLease, lease_wire)
        assert retained.expires_at > now
        assert COMPUTE_SLOTS_EXPIRED_ACTIVE_LEASE_ERROR_KEY in state.errors


def test_compute_slot_expiry_removes_reservation_for_definitely_inactive_agent():
    now = time.time()
    lease = SlotLease(
        lease_id="lease-0",
        request=ComputeSlotRequest(request_id="request-0", agent_id="agent-0", cpu_cores=2),
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=now - 120.0,
        expires_at=now - 60.0,
    )
    agent = ComputeSlotsAgent(ComputeSlotsState(leases={lease.lease_id: dataclass_to_wire(lease)}))
    agent._is_agent_active = lambda agent_id: False  # type: ignore[method-assign]

    agent._expire_leases()

    with agent.locked_state() as state:
        assert state.leases == {}


def test_compute_slot_active_check_is_conservative_on_control_error():
    agent = ComputeSlotsAgent(ComputeSlotsState())

    def raise_control_error(agent_id, host_url):
        raise RuntimeError("control channel busy")

    agent._context = SimpleNamespace(address="http://alpha", get_proxy=raise_control_error)  # type: ignore[assignment]

    assert agent._is_agent_active("agent-0") is True


def test_compute_slot_scheduler_status_can_include_active_job_metrics_without_queue():
    lease = SlotLease(
        lease_id="lease-0",
        request=ComputeSlotRequest(request_id="request-0", agent_id="agent-0", job_id="job-0", cpu_cores=1),
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=time.time(),
        expires_at=time.time() + 60.0,
    )
    local = SchedulerHostStatus(host_name="alpha", host_url="http://alpha", observed_at=time.time())
    runtime = ComputeJobRuntimeInfo(
        lease_id="lease-0",
        request_id="request-0",
        job_id="job-0",
        agent_id="agent-0",
        declared_cpu_cores=1,
    )
    agent = ComputeSlotsAgent(ComputeSlotsState(leases={lease.lease_id: dataclass_to_wire(lease)}))
    agent._local_status = lambda: local  # type: ignore[method-assign]
    agent._runtime_info_for_leases = lambda leases, **kwargs: [runtime]  # type: ignore[method-assign]

    reply = agent.scheduler_status(SchedulerStatusRequest(include_jobs=True, include_queue=False))

    assert reply.active_jobs == [runtime]
    assert reply.queued_requests == []
    assert reply.leases == []


def test_compute_slot_usage_sums_files_and_directories(tmp_path: Path):
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    (work_dir / "a.bin").write_bytes(b"1234")
    nested = work_dir / "nested"
    nested.mkdir()
    (nested / "b.bin").write_bytes(b"12")
    file_path = tmp_path / "single.bin"
    file_path.write_bytes(b"123")

    assert _directory_usage(work_dir) == {"bytes": 6, "files": 2, "error": ""}
    assert _paths_usage([str(work_dir), str(file_path), str(tmp_path / "missing")]) == {
        "bytes": 9,
        "files": 3,
        "error": "",
    }


def test_compute_slot_usage_sampling_tracks_maxima_and_finished_history():
    lease = SlotLease(
        lease_id="lease-0",
        request=ComputeSlotRequest(request_id="request-0", agent_id="agent-0", job_id="job-0"),
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=90.0,
        expires_at=3600.0,
    )
    agent = ComputeSlotsAgent(ComputeSlotsState(leases={lease.lease_id: dataclass_to_wire(lease)}))
    samples = [
        ComputeJobRuntimeInfo(
            lease_id="lease-0",
            request_id="request-0",
            job_id="job-0",
            agent_id="agent-0",
            class_name="example:Job",
            current_cpu_percent=12.0,
            current_memory_rss_bytes=100,
            process_tree_memory_rss_bytes=120,
            extra_work_bytes=300,
        ),
        ComputeJobRuntimeInfo(
            lease_id="lease-0",
            request_id="request-0",
            job_id="job-0",
            agent_id="agent-0",
            class_name="example:Job",
            current_cpu_percent=8.0,
            current_memory_rss_bytes=150,
            process_tree_memory_rss_bytes=200,
            work_dir_bytes=25,
            extra_work_bytes=250,
        ),
    ]
    agent._runtime_info_for_leases = lambda leases, **kwargs: [samples.pop(0)]  # type: ignore[method-assign]

    agent._sample_usage([lease], now=100.0)
    agent._sample_usage([lease], now=160.0)
    agent.release_slot(SlotReleaseRequest(lease_id="lease-0", agent_id="agent-0"))

    with agent.locked_state() as state:
        assert state.active_usage == {}
        [finished] = state.finished_usage
    assert finished["job_id"] == "job-0"
    assert finished["class_name"] == "example:Job"
    assert finished["sample_count"] == 2
    assert finished["max_cpu_percent"] == 12.0
    assert finished["max_memory_rss_bytes"] == 150
    assert finished["max_process_tree_memory_rss_bytes"] == 200
    assert finished["max_total_work_bytes"] == 300
    assert finished["finish_reason"] == "released"
    assert finished["runtime_seconds"] >= 0


def test_compute_slot_redirect_target_prefers_suitable_empty_peer_queue():
    now = time.time()
    request = ComputeSlotRequest(cpu_cores=2, memory_bytes=2 * 1024**3, temp_storage_bytes=1024**3)
    unsuitable = SchedulerHostStatus(
        host_name="tiny",
        host_url="http://tiny",
        observed_at=now,
        cpu_count_logical=1,
        memory_total_bytes=1024**3,
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=1,
        free_memory_bytes=1024**3,
        free_temp_storage_bytes=100 * 1024**3,
        queue_length=0,
    )
    backlogged = SchedulerHostStatus(
        host_name="backlog",
        host_url="http://backlog",
        observed_at=now,
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=4,
        free_memory_bytes=8 * 1024**3,
        free_temp_storage_bytes=50 * 1024**3,
        queue_length=5,
    )
    empty = SchedulerHostStatus(
        host_name="empty",
        host_url="http://empty",
        observed_at=now,
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=4,
        free_memory_bytes=8 * 1024**3,
        free_temp_storage_bytes=50 * 1024**3,
        queue_length=0,
    )
    agent = ComputeSlotsAgent(
        ComputeSlotsState(
            peer_statuses={
                unsuitable.host_url: dataclass_to_wire(unsuitable),
                backlogged.host_url: dataclass_to_wire(backlogged),
                empty.host_url: dataclass_to_wire(empty),
            }
        )
    )

    target = agent._redirect_target(request)

    assert target is not None
    assert target.host_url == "http://empty"


def test_compute_slot_redirect_budget_keeps_anchor_except_single_waiter():
    assert _redirect_budget(queue_length=0, max_fraction=0.5, max_per_tick=4) == 0
    assert _redirect_budget(queue_length=1, max_fraction=0.5, max_per_tick=4) == 1
    assert _redirect_budget(queue_length=2, max_fraction=0.5, max_per_tick=4) == 1
    assert _redirect_budget(queue_length=5, max_fraction=0.5, max_per_tick=4) == 3
    assert _redirect_budget(queue_length=20, max_fraction=0.5, max_per_tick=4) == 4


def test_compute_slot_redirects_multiple_jobs_to_projected_peer_capacity():
    now = time.time()
    requests = [
        ComputeSlotRequest(
            request_id=f"request-{index}",
            agent_id=f"agent-{index}",
            cpu_cores=1,
            memory_bytes=1024,
            temp_storage_bytes=1024,
        )
        for index in range(5)
    ]
    peer = SchedulerHostStatus(
        host_name="empty",
        host_url="http://empty",
        observed_at=now,
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=3,
        free_memory_bytes=3 * 1024,
        free_temp_storage_bytes=3 * 1024,
        queue_length=0,
    )
    local = SchedulerHostStatus(
        host_name="backlog",
        host_url="http://backlog",
        observed_at=now,
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=0,
        free_memory_bytes=16 * 1024**3,
        free_temp_storage_bytes=100 * 1024**3,
        queue_length=len(requests),
    )
    agent = ComputeSlotsAgent(
        ComputeSlotsState(
            queued_requests=[dataclass_to_wire(request) for request in requests],
            peer_statuses={peer.host_url: dataclass_to_wire(peer)},
        )
    )
    agent._context = SimpleNamespace(address=local.host_url)  # type: ignore[assignment]
    sent: list[str] = []
    agent._local_status = lambda: local  # type: ignore[method-assign]
    agent._send_redirect = lambda request, reply: sent.append(request.request_id) or True  # type: ignore[method-assign]

    agent._redirect_queued_requests()

    assert sent == ["request-0", "request-1", "request-2"]
    with agent.locked_state() as state:
        remaining = [dataclass_to_wire(request) for request in requests[3:]]
        assert state.queued_requests == remaining


def test_compute_slot_redirect_cooldown_prevents_immediate_ping_pong():
    now = time.time()
    request = ComputeSlotRequest(
        request_id="request-0",
        agent_id="agent-0",
        cpu_cores=1,
        memory_bytes=1024,
        temp_storage_bytes=1024,
        last_redirect_at=now,
        last_redirect_from_host_url="http://empty",
    )
    peer = SchedulerHostStatus(
        host_name="empty",
        host_url="http://empty",
        observed_at=now,
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=4,
        free_memory_bytes=4 * 1024,
        free_temp_storage_bytes=4 * 1024,
        queue_length=0,
    )
    local = SchedulerHostStatus(
        host_name="backlog",
        host_url="http://backlog",
        observed_at=now,
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=0,
        free_memory_bytes=16 * 1024**3,
        free_temp_storage_bytes=100 * 1024**3,
        queue_length=1,
    )
    agent = ComputeSlotsAgent(
        ComputeSlotsState(
            queued_requests=[dataclass_to_wire(request)],
            peer_statuses={peer.host_url: dataclass_to_wire(peer)},
        )
    )
    agent._context = SimpleNamespace(address=local.host_url)  # type: ignore[assignment]
    sent: list[str] = []
    agent._local_status = lambda: local  # type: ignore[method-assign]
    agent._send_redirect = lambda request, reply: sent.append(request.request_id) or True  # type: ignore[method-assign]

    agent._redirect_queued_requests()

    assert sent == []
    with agent.locked_state() as state:
        assert state.queued_requests == [dataclass_to_wire(request)]


def test_compute_slot_keeps_queued_request_when_grant_delivery_fails():
    request = ComputeSlotRequest(request_id="request-0", agent_id="agent-0", cpu_cores=1)
    local = SchedulerHostStatus(
        host_name="backlog",
        host_url="http://backlog",
        observed_at=time.time(),
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=4,
        free_memory_bytes=16 * 1024**3,
        free_temp_storage_bytes=100 * 1024**3,
    )
    agent = ComputeSlotsAgent(
        ComputeSlotsState(
            queued_requests=[dataclass_to_wire(request)],
            last_grant_at=123.0,
        )
    )
    agent._context = SimpleNamespace(address=local.host_url)  # type: ignore[assignment]
    agent._local_status = lambda: local  # type: ignore[method-assign]
    agent._send_grant = lambda request, reply: False  # type: ignore[method-assign]

    agent._grant_queued_requests()

    with agent.locked_state() as state:
        assert state.queued_requests == [dataclass_to_wire(request)]
        assert state.leases == {}
        assert state.last_grant_at == 123.0


def test_compute_slot_respects_configurable_grants_per_tick():
    requests = [
        ComputeSlotRequest(request_id=f"request-{index}", agent_id=f"agent-{index}", cpu_cores=1) for index in range(4)
    ]
    local = SchedulerHostStatus(
        host_name="alpha",
        host_url="http://alpha",
        observed_at=time.time(),
        cpu_count_logical=8,
        memory_total_bytes=16 * 1024**3,
        work_total_bytes=100 * 1024**3,
        free_cpu_cores=8,
        free_memory_bytes=16 * 1024**3,
        free_temp_storage_bytes=100 * 1024**3,
        load_per_cpu=0.1,
    )
    agent = ComputeSlotsAgent(
        ComputeSlotsState(
            queued_requests=[dataclass_to_wire(request) for request in requests],
            grant_interval=0.0,
            max_grants_per_tick=2,
        )
    )
    agent._context = SimpleNamespace(address=local.host_url)  # type: ignore[assignment]
    agent._local_status = lambda: local  # type: ignore[method-assign]
    sent: list[str] = []
    agent._send_grant = lambda request, reply: sent.append(request.request_id) or True  # type: ignore[method-assign]

    agent._grant_queued_requests()

    assert sent == ["request-0", "request-1"]
    with agent.locked_state() as state:
        assert state.queued_requests == [dataclass_to_wire(request) for request in requests[2:]]
