# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path


def test_examples_do_not_expose_public_drain_operations():
    root = Path("src/paglets/examples")
    offenders = [
        str(path) for path in root.rglob("*.py") if 'ServiceOperation("drain"' in path.read_text(encoding="utf-8")
    ]

    assert offenders == []


def test_cli_does_not_expose_poll_interval_for_example_jobs():
    roots = [Path("src/paglets/examples"), Path("src/paglets/cli")]
    offenders = [
        str(path)
        for root in roots
        for path in root.rglob("*.py")
        if "--poll-interval" in path.read_text(encoding="utf-8")
    ]

    assert offenders == []
