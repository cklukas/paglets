# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from examples.disk_survey_demo import DiskSurveyPaglet, DiskSurveyState
from examples.support import local_hosts


def test_disk_survey_paglet_clones_to_advertised_hosts_and_collects_findings():
    with local_hosts("alpha", "beta", mesh=True, mesh_version="disk-survey-test") as hosts:
        parent = hosts[0].create(DiskSurveyPaglet, DiskSurveyState())
        summary = parent.send_message("survey", {"timeout": 3.0})

    assert summary["errors"] == {}
    assert set(summary["findings"]) == {"alpha", "beta"}
    assert all(volumes for volumes in summary["findings"].values())
    assert all(
        {"volume", "total_bytes", "used_bytes", "free_bytes"} <= set(volume)
        for volumes in summary["findings"].values()
        for volume in volumes
    )
    assert any("cloning child to alpha" in line for line in summary["diagnostics"])
    assert any("cloning child to beta" in line for line in summary["diagnostics"])
