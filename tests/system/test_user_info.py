# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from paglets.system.user_info import UserInfoAgent, UserInfoRequest


def test_user_info_notify_prints_to_console(capsys):
    agent = UserInfoAgent()

    reply = agent.notify(
        UserInfoRequest(
            severity="warning",
            title="No suitable host",
            message="GPU jobs unsupported",
            source_agent_id="agent-1",
            job_id="job-1",
            timestamp=1_700_000_000.0,
        )
    )

    assert reply.ok is True
    captured = capsys.readouterr()
    assert "WARNING: No suitable host job=job-1 source=agent-1: GPU jobs unsupported" in captured.err
