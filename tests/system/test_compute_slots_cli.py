# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import time

from paglets.remote.admin import AgentRecord, ServerRef
from paglets.remote.client import HostClient
from paglets.system.compute_slots import (
    CancelSlotRequestsRequest,
    ComputeSlotRequest,
    SchedulerHostStatus,
    SchedulerStatusReply,
    SlotLease,
)
from paglets.system.compute_slots.cli import (
    _blocked_request_payload,
    _apply_lease_times_to_active_jobs,
    _cancel_preview_payload,
    _core_summary,
    _jobs_list_inclusion,
    _load_compute_jobs,
    _parser,
    _print_jobs,
    _print_status,
    _print_usage_history,
    _public_jobs,
)


def test_compute_slots_status_prints_queue_and_job_resource_details(capsys):
    _print_status(
        {
            "status": {
                "host_name": "alpha",
                "free_cpu_cores": 4,
                "reserved_cpu_cores": 2,
                "load_per_cpu": 0.5,
                "max_load_per_cpu": 1.0,
                "free_memory_bytes": 8 * 1024**3,
                "reserved_memory_bytes": 2 * 1024**3,
                "free_temp_storage_bytes": 10 * 1024**3,
                "reserved_temp_storage_bytes": 5 * 1024**3,
                "queue_length": 1,
                "active_leases": 1,
            },
            "queued_requests": [
                {
                    "request_id": "request-0",
                    "job_id": "job-0",
                    "agent_id": "agent-0",
                    "cpu_cores": 2,
                    "memory_bytes": 512 * 1024**2,
                    "temp_storage_bytes": 128 * 1024**2,
                }
            ],
            "leases": [
                {
                    "lease_id": "lease-0",
                    "request": {
                        "job_id": "job-1",
                        "cpu_cores": 2,
                        "memory_bytes": 2 * 1024**3,
                        "temp_storage_bytes": 5 * 1024**3,
                    },
                    "reserved_cpu_core_ids": [0, 1],
                    "cpu_core_ids": [0, 1, 2, 3],
                }
            ],
            "active_jobs": [
                {
                    "job_id": "job-1",
                    "agent_id": "agent-1",
                    "granted_at": time.time() - 3661,
                    "pid": 123,
                    "declared_cpu_cores": 2,
                    "assigned_cpu_core_ids": [0, 1, 2, 3],
                    "declared_memory_bytes": 2 * 1024**3,
                    "current_memory_rss_bytes": 640 * 1024**2,
                    "current_cpu_percent": 87.5,
                    "current_memory_percent": 1.25,
                    "process_tree_memory_rss_bytes": 768 * 1024**2,
                    "work_dir_bytes": 256 * 1024**2,
                    "work_dir_file_count": 3,
                    "process_status": "running",
                }
            ],
        }
    )

    output = capsys.readouterr().out
    assert "waiting=1" in output
    assert "cores_reserved=2" in output
    assert "load=0.50/1.00" in output
    assert "temp_reserved=5.0G" in output
    assert "queued:" in output
    assert "active jobs:" in output
    assert "5.0G" in output
    assert "87.5%" in output
    assert "0-3" in output


def test_compute_slots_status_prints_usage_details(capsys):
    _print_status(
        {
            "_include_usage": True,
            "status": {
                "host_name": "alpha",
                "free_cpu_cores": 4,
                "reserved_cpu_cores": 2,
                "free_memory_bytes": 8 * 1024**3,
                "reserved_memory_bytes": 2 * 1024**3,
                "free_temp_storage_bytes": 10 * 1024**3,
                "queue_length": 0,
                "active_leases": 1,
            },
            "active_jobs": [
                {
                    "job_id": "job-1",
                    "agent_id": "agent-1",
                    "granted_at": time.time() - 3661,
                    "pid": 123,
                    "declared_cpu_cores": 2,
                    "assigned_cpu_core_ids": [0, 1],
                    "declared_memory_bytes": 2 * 1024**3,
                    "current_memory_rss_bytes": 640 * 1024**2,
                    "process_tree_memory_rss_bytes": 768 * 1024**2,
                    "work_dir_bytes": 256 * 1024**2,
                    "extra_work_bytes": 1024**3,
                    "work_dir_file_count": 3,
                    "extra_work_file_count": 2,
                    "max_memory_rss_bytes": 700 * 1024**2,
                    "max_process_tree_memory_rss_bytes": 900 * 1024**2,
                    "process_status": "running",
                }
            ],
        }
    )

    output = capsys.readouterr().out
    assert "runtime" in output
    assert "1.0h" in output
    assert "tree rss" in output
    assert "max rss" in output
    assert "max tree" in output
    assert "work" in output
    assert "768.0M" in output
    assert "700.0M" in output
    assert "900.0M" in output
    assert "256.0M" in output
    assert "1.0G" in output
    assert "|     5 |" in output


def test_compute_slots_usage_runtime_can_come_from_lease(capsys):
    payload = {
        "status": {
            "host_name": "alpha",
            "free_cpu_cores": 4,
            "reserved_cpu_cores": 1,
            "free_memory_bytes": 8 * 1024**3,
            "reserved_memory_bytes": 1024**3,
            "free_temp_storage_bytes": 10 * 1024**3,
            "queue_length": 0,
            "active_leases": 1,
        },
        "leases": [{"lease_id": "lease-0", "granted_at": time.time() - 3661, "request": {}}],
        "active_jobs": [
            {
                "lease_id": "lease-0",
                "job_id": "job-1",
                "agent_id": "agent-1",
                "pid": 123,
                "declared_cpu_cores": 1,
                "declared_memory_bytes": 1024**3,
                "current_memory_rss_bytes": 128 * 1024**2,
                "process_status": "running",
            }
        ],
        "_include_usage": True,
        "_hide_leases": True,
    }
    _apply_lease_times_to_active_jobs(payload)

    _print_status(payload)

    assert "1.0h" in capsys.readouterr().out


def test_core_summary_compacts_long_affinity_lists():
    assert _core_summary([0, 1, 2, 3]) == "0-3"
    assert _core_summary([1, 9, 18, 27, 36, 45, 54, 63]) == "1,9,18,27,+4"


def test_compute_slots_prints_finished_usage_history(capsys):
    _print_usage_history(
        [
            {
                "finished_at": 1782457708.0,
                "runtime_seconds": 125.0,
                "finish_reason": "released",
                "job_id": "job-0",
                "class_name": "example:ExampleComputeJob",
                "max_cpu_percent": 25.5,
                "max_process_tree_memory_rss_bytes": 768 * 1024**2,
                "max_total_work_bytes": 1024**3,
            }
        ]
    )

    output = capsys.readouterr().out
    assert "runtime" in output
    assert "released" in output
    assert "2.1m" in output
    assert "25.5%" in output
    assert "768.0M" in output
    assert "1.0G" in output


def test_compute_slots_status_prints_blocked_queue_diagnostics(capsys):
    payload = {
        "status": {
            "host_name": "alpha",
            "free_cpu_cores": 4,
            "reserved_cpu_cores": 0,
            "load_per_cpu": 0.2,
            "max_load_per_cpu": 1.0,
            "free_memory_bytes": 64 * 1024**3,
            "reserved_memory_bytes": 0,
            "free_temp_storage_bytes": 132 * 1024**3,
            "queue_length": 2,
            "active_leases": 0,
        },
        "queued_requests": [
            {
                "request_id": "request-0",
                "job_id": "job-0",
                "agent_id": "agent-0",
                "cpu_cores": 1,
                "memory_bytes": 32 * 1024**3,
                "temp_storage_bytes": 100 * 1024**3,
            },
            {
                "request_id": "request-1",
                "job_id": "job-1",
                "agent_id": "agent-1",
                "cpu_cores": 1,
                "memory_bytes": 32 * 1024**3,
                "temp_storage_bytes": 100 * 1024**3,
            }
        ],
    }
    payload["blocked_requests"] = _blocked_request_payload(payload)

    _print_status(payload)

    output = capsys.readouterr().out
    assert "blocked:" in output
    assert "temp-storage=1" in output
    assert "request-0" in output
    assert "request-1" in output


def test_compute_slots_status_prints_load_blocked_queue_diagnostics(capsys):
    payload = {
        "status": {
            "host_name": "alpha",
            "free_cpu_cores": 4,
            "reserved_cpu_cores": 0,
            "load_per_cpu": 1.2,
            "max_load_per_cpu": 1.0,
            "free_memory_bytes": 64 * 1024**3,
            "reserved_memory_bytes": 0,
            "free_temp_storage_bytes": 132 * 1024**3,
            "queue_length": 1,
            "active_leases": 0,
        },
        "queued_requests": [
            {
                "request_id": "request-0",
                "job_id": "job-0",
                "agent_id": "agent-0",
                "cpu_cores": 1,
                "memory_bytes": 32 * 1024**3,
                "temp_storage_bytes": 100 * 1024**3,
            }
        ],
    }
    payload["blocked_requests"] = _blocked_request_payload(payload)

    _print_status(payload)

    output = capsys.readouterr().out
    assert "blocked:" in output
    assert "load=1" in output
    assert "request-0" in output


def test_compute_slots_cancel_preview_matches_requests_and_leases():
    keep_request = ComputeSlotRequest(request_id="request-keep", agent_id="agent-1", job_id="job-1")
    cancel_request = ComputeSlotRequest(request_id="request-cancel", agent_id="agent-0", job_id="job-0")
    cancel_lease = SlotLease(
        lease_id="lease-cancel",
        request=cancel_request,
        host_name="alpha",
        host_url="http://alpha",
        work_dir_base="/tmp/paglets",
        granted_at=time.time(),
        expires_at=time.time() + 60.0,
    )
    reply = SchedulerStatusReply(
        status=SchedulerHostStatus(host_name="alpha", host_url="http://alpha", observed_at=time.time()),
        queued_requests=[keep_request, cancel_request],
        leases=[cancel_lease],
    )

    payload = _cancel_preview_payload(
        reply,
        CancelSlotRequestsRequest(agent_ids=("agent-0",), include_leases=True),
    )

    assert [item["request_id"] for item in payload["matched_requests"]] == ["request-cancel"]
    assert [item["lease_id"] for item in payload["matched_leases"]] == ["lease-cancel"]


def test_compute_slots_jobs_prints_compute_job_details(capsys):
    _print_jobs(
        [
            {
                "agent_id": "agent-0",
                "active": False,
                "class_name": "paglets.examples:ExampleComputeJob",
                "compute_status": "WAITING_FOR_SLOT",
                "status": "NEW",
                "job_id": "job-0",
                "request_id": "request-0",
            }
        ]
    )

    output = capsys.readouterr().out
    assert "WAITING_FOR_SLOT" in output
    assert "job-0" in output
    assert "ExampleComputeJob" in output


def test_compute_slots_public_jobs_omits_internal_agent_record():
    agent = AgentRecord(
        server_name="alpha",
        host_url="http://alpha",
        agent_id="agent-0",
        class_name="paglets.examples:ExampleComputeJob",
        state_class_name="paglets.examples:ExampleComputeState",
        active=False,
    )
    [public] = _public_jobs([{"agent_id": "agent-0", "_agent": agent}])

    assert public == {"agent_id": "agent-0"}


def test_compute_slots_load_jobs_uses_bulk_state_payloads():
    class FakeAdmin:
        def list_agent_payloads(self, entry, *, include_state=False):
            assert include_state is True
            return [
                {
                    "agent_id": "agent-0",
                    "class_name": "paglets.examples:ExampleComputeJob",
                    "state_class_name": "paglets.examples:ExampleComputeState",
                    "active": True,
                    "server_name": "alpha",
                    "host_url": "http://alpha",
                    "state": {
                        "compute_status": "RUNNING",
                        "status": "PROCESSING",
                        "job_id": "job-0",
                        "slot_request_id": "request-0",
                    },
                }
            ]

        def get_agent_state(self, agent):
            raise AssertionError("bulk state payload should avoid per-agent state fetches")

    jobs = _load_compute_jobs(
        FakeAdmin(),
        ServerRef("alpha", "http://alpha"),
        include_active=True,
        include_inactive=True,
    )

    assert jobs[0]["agent_id"] == "agent-0"
    assert jobs[0]["compute_status"] == "RUNNING"


def test_compute_slots_json_flag_is_accepted_before_or_after_subcommands():
    assert _parser().parse_args(["--json", "status"]).json is True
    assert _parser().parse_args(["status", "--json"]).json is True
    assert _parser().parse_args(["status", "--blocked"]).blocked is True
    assert _parser().parse_args(["status", "--usage"]).usage is True
    assert _parser().parse_args(["jobs", "list", "--json"]).json is True
    assert _parser().parse_args(["jobs", "clear", "--json"]).json is True
    assert _parser().parse_args(["jobs", "history", "--limit", "3"]).limit == 3


def test_compute_slots_jobs_list_defaults_to_active_and_inactive():
    assert _jobs_list_inclusion(_parser().parse_args(["jobs", "list"])) == (True, True)
    assert _jobs_list_inclusion(_parser().parse_args(["jobs", "list", "--active"])) == (True, False)
    assert _jobs_list_inclusion(_parser().parse_args(["jobs", "list", "--inactive"])) == (False, True)
    assert _jobs_list_inclusion(_parser().parse_args(["jobs", "list", "--active", "--inactive"])) == (True, True)


def test_compute_slots_entry_url_is_used_directly(monkeypatch):
    from paglets.remote.admin import select_reachable_entry_server

    def fake_get_json(url, *, timeout=None):
        assert url == "https://aqre.ap.basf.net/paglets/health"
        return {"name": "aqre", "address": "https://aqre.ap.basf.net/paglets"}

    client = HostClient(timeout=1)
    monkeypatch.setattr(client, "get_json", fake_get_json)

    entry = select_reachable_entry_server(
        entry_name="https://aqre.ap.basf.net/paglets",
        client=client,
    )

    assert entry.name == "aqre"
    assert entry.url == "https://aqre.ap.basf.net/paglets"
