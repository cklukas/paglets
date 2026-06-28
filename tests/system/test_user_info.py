# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from paglets.core.messages import Message
from paglets.serialization.codec import dataclass_to_wire
from paglets.system.user_info import UserInfoAgent, UserInfoRequest, UserInfoStreamRequest


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


def test_user_info_pi_output_is_raw_stdout(capsys):
    agent = UserInfoAgent()

    reply = agent.handle_message(Message("pi.output", dataclass_to_wire(UserInfoStreamRequest(text="1415"))))

    assert reply == {"ok": True}
    captured = capsys.readouterr()
    assert captured.out == "1415"
    assert captured.err == ""
