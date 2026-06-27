# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import re

from typer.testing import CliRunner

from paglets.cli.app import app
from paglets.cli.host import _host_error_event_text, _validate_bind_public_values
from paglets.core.context_events import ContextEvent

runner = CliRunner()


def _plain(output: str) -> str:
    text = re.sub(r"\x1b\[[0-9;]*m", "", output)
    return " ".join(text.split())


def test_paglets_root_help_lists_modern_command_groups():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    output = _plain(result.output)
    assert "host" in output
    assert "sys" in output
    assert "jobs" in output
    assert "artifacts" in output
    assert "examples" in output


def test_completion_commands_are_available():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    output = _plain(result.output)
    assert "completion" in output


def test_jobs_queue_help_uses_new_surface():
    result = runner.invoke(app, ["jobs", "queue", "--help"])

    assert result.exit_code == 0
    output = _plain(result.output)
    assert "paglets jobs queue" in output
    assert "--entry" in output
    assert "--json" in output


def test_system_df_help_uses_disk_argument_name():
    result = runner.invoke(app, ["sys", "df", "--help"])

    assert result.exit_code == 0
    output = _plain(result.output)
    assert "paglets sys df" in output
    assert "disk" in output.lower()


def test_examples_file_push_help_is_nested():
    result = runner.invoke(app, ["examples", "file", "push", "--help"])

    assert result.exit_code == 0
    output = _plain(result.output)
    assert "paglets examples file push" in output
    assert "--remote" in output


def test_host_bind_public_requires_value():
    result = runner.invoke(app, ["host", "--name", "alpha", "--bind-public", "--mesh-version", "dev"])

    assert result.exit_code != 0
    assert "No such command" in _plain(result.output)


def test_host_bind_public_rejects_option_like_values():
    try:
        _validate_bind_public_values(["--mesh"])
    except Exception as exc:
        assert "--bind-public value" in str(exc)
    else:
        raise AssertionError("expected option-like bind-public value to be rejected")


def test_host_error_event_text_reports_resident_service_failures():
    event = ContextEvent(
        event_id=1,
        kind="resident-service-failed",
        host_name="uconsole",
        host_address="http://127.0.0.1:8765",
        agent_id="service.server-info",
        service_name="server-info",
        data={"error": "could not import state"},
    )

    text = _host_error_event_text(event)

    assert text is not None
    assert "resident-service-failed" in text
    assert "service.server-info" in text
    assert "could not import state" in text


def test_host_error_event_text_ignores_normal_events():
    event = ContextEvent(
        event_id=1,
        kind="message-delivered",
        host_name="uconsole",
        host_address="http://127.0.0.1:8765",
    )

    assert _host_error_event_text(event) is None
