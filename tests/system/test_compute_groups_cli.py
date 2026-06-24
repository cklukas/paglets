# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from paglets.system.compute_slots.groups_cli import _print_group_summaries, _summary_from_collector_state


def test_compute_groups_cli_summarizes_collector_state():
    summary = _summary_from_collector_state(
        {"agent_id": "collector-0", "host": "alpha", "active": True},
        {
            "group_id": "group-0",
            "status": "WAITING_FOR_HOME",
            "home_host_name": "laptop",
            "home_host_url": "http://laptop",
            "return_home_when_complete": True,
            "expected_jobs": {"a": {}, "b": {}},
            "results": {"a": {}},
            "failures": {},
        },
        host_url="http://alpha",
    )

    assert summary is not None
    assert summary["group_id"] == "group-0"
    assert summary["expected_count"] == 2
    assert summary["completed_count"] == 1
    assert summary["pending_jobs"] == ["b"]
    assert summary["waiting_for_home"] is True


def test_compute_groups_cli_prints_status_table(capsys):
    _print_group_summaries(
        [
            {
                "group_id": "group-0",
                "status": "COMPLETE",
                "expected_count": 2,
                "completed_count": 1,
                "failed_count": 1,
                "pending_count": 0,
                "waiting_for_home": False,
                "collector": {"host_name": "alpha", "host_url": "http://alpha"},
            }
        ]
    )

    output = capsys.readouterr().out
    assert "group-0" in output
    assert "COMPLETE" in output
    assert "alpha" in output
