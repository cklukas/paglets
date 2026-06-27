# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from typer.testing import CliRunner

from paglets.cli.app import app

runner = CliRunner()


def test_paglets_root_help_lists_modern_command_groups():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "host" in result.output
    assert "sys" in result.output
    assert "jobs" in result.output
    assert "artifacts" in result.output
    assert "examples" in result.output


def test_completion_commands_are_available():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "--install-completion" in result.output
    assert "--show-completion" in result.output


def test_jobs_queue_help_uses_new_surface():
    result = runner.invoke(app, ["jobs", "queue", "--help"])

    assert result.exit_code == 0
    assert "Usage: paglets jobs queue" in result.output
    assert "--entry" in result.output
    assert "--json" in result.output


def test_system_df_help_uses_disk_argument_name():
    result = runner.invoke(app, ["sys", "df", "--help"])

    assert result.exit_code == 0
    assert "Usage: paglets sys df" in result.output
    assert "disk" in result.output.lower()


def test_examples_file_push_help_is_nested():
    result = runner.invoke(app, ["examples", "file", "push", "--help"])

    assert result.exit_code == 0
    assert "Usage: paglets examples file push" in result.output
    assert "--remote" in result.output
