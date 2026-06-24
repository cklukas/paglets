# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from paglets.system.compute_slots.cli import _print_status


def test_compute_slots_status_prints_queue_and_job_resource_details(capsys):
    _print_status(
        {
            "status": {
                "host_name": "alpha",
                "free_cpu_cores": 4,
                "reserved_cpu_cores": 2,
                "free_memory_bytes": 8 * 1024**3,
                "reserved_memory_bytes": 2 * 1024**3,
                "free_temp_storage_bytes": 10 * 1024**3,
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
                    "request": {"job_id": "job-1", "cpu_cores": 2, "memory_bytes": 2 * 1024**3},
                    "reserved_cpu_core_ids": [0, 1],
                    "cpu_core_ids": [0, 1, 2, 3],
                }
            ],
            "active_jobs": [
                {
                    "job_id": "job-1",
                    "agent_id": "agent-1",
                    "pid": 123,
                    "declared_cpu_cores": 2,
                    "assigned_cpu_core_ids": [0, 1, 2, 3],
                    "declared_memory_bytes": 2 * 1024**3,
                    "current_memory_rss_bytes": 640 * 1024**2,
                    "current_cpu_percent": 87.5,
                    "current_memory_percent": 1.25,
                    "process_status": "running",
                }
            ],
        }
    )

    output = capsys.readouterr().out
    assert "waiting=1" in output
    assert "cores_reserved=2" in output
    assert "queued:" in output
    assert "active jobs:" in output
    assert "87.5" in output
    assert "4:0,1,2,3" in output
