# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from paglets.examples.mesh_info import LandscapeReply, MeshHostSnapshot, TargetCandidate, TargetSelectionReply
from paglets.examples.mesh_info.cli import _print_summary, _print_targets


def test_mesh_info_summary_prints_active_and_inactive_counts(capsys):
    _print_summary(
        LandscapeReply(
            generated_at=100.0,
            hosts=[
                MeshHostSnapshot(
                    host_name="alpha",
                    host_url="http://127.0.0.1:8765",
                    code_version="test",
                    observed_at=100.0,
                    active_count=3,
                    inactive_count=2,
                )
            ],
        )
    )

    output = capsys.readouterr().out
    assert "active" in output
    assert "inactive" in output
    assert "     3        2 " in output


def test_mesh_info_targets_prints_active_and_inactive_counts(capsys):
    _print_targets(
        TargetSelectionReply(
            generated_at=100.0,
            targets=[
                TargetCandidate(
                    snapshot=MeshHostSnapshot(
                        host_name="alpha",
                        host_url="http://127.0.0.1:8765",
                        code_version="test",
                        observed_at=100.0,
                        active_count=4,
                        inactive_count=1,
                    ),
                    score=1.25,
                )
            ],
        )
    )

    output = capsys.readouterr().out
    assert "active" in output
    assert "inactive" in output
    assert "     4        1" in output
