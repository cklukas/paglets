# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path

from paglets.patterns.file_mobility import (
    FileTransferMode,
    FileTransferRequest,
    FileTransferState,
    SingleFileTransferPaglet,
)
from paglets.patterns.tasks import TaskClient, TaskStatus
from paglets.runtime.host import Host
from tests.support import free_port


def test_single_file_transfer_dry_run_reports_plan(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    source = tmp_path / "source.txt"
    source.write_text("payload", encoding="utf-8")
    host.start_background()
    try:
        task = TaskClient.for_paglet(
            host.create(SingleFileTransferPaglet, FileTransferState()),
            SingleFileTransferPaglet,
        )

        summary = task.start_and_wait(
            FileTransferRequest(
                source_path=str(source),
                destination_path="/tmp/out.txt",
                target_host="http://beta",
                dry_run=True,
                destination_label="beta",
            )
        )

        assert summary.status is TaskStatus.COMPLETED
        assert summary.result is not None
        assert summary.result.dry_run is True
        assert summary.result.destination.path == "/tmp/out.txt"
        assert source.exists()
    finally:
        host.stop()


def test_single_file_transfer_copy_and_move(tmp_path: Path):
    alpha = _host("alpha", tmp_path / "alpha")
    beta = _host("beta", tmp_path / "beta")
    copy_source = tmp_path / "copy-source.txt"
    copy_destination = tmp_path / "copy-destination.txt"
    move_source = tmp_path / "move-source.txt"
    move_destination = tmp_path / "move-destination.txt"
    copy_source.write_text("copy", encoding="utf-8")
    move_source.write_text("move", encoding="utf-8")
    alpha.start_background()
    beta.start_background()
    try:
        copy_task = TaskClient.for_paglet(
            alpha.create(SingleFileTransferPaglet, FileTransferState()),
            SingleFileTransferPaglet,
        )
        move_task = TaskClient.for_paglet(
            alpha.create(SingleFileTransferPaglet, FileTransferState()),
            SingleFileTransferPaglet,
        )

        copy_summary = copy_task.start_and_wait(
            FileTransferRequest(
                source_path=str(copy_source),
                destination_path=str(copy_destination),
                target_host=beta.address,
            )
        )
        move_summary = move_task.start_and_wait(
            FileTransferRequest(
                source_path=str(move_source),
                destination_path=str(move_destination),
                target_host=beta.address,
                mode=FileTransferMode.MOVE,
            )
        )

        assert copy_summary.status is TaskStatus.COMPLETED
        assert move_summary.status is TaskStatus.COMPLETED
        assert copy_destination.read_text(encoding="utf-8") == "copy"
        assert move_destination.read_text(encoding="utf-8") == "move"
        assert copy_source.exists()
        assert not move_source.exists()
    finally:
        beta.stop()
        alpha.stop()


def test_single_file_transfer_overwrite_failure_keeps_destination(tmp_path: Path):
    alpha = _host("alpha", tmp_path / "alpha")
    beta = _host("beta", tmp_path / "beta")
    source = tmp_path / "source.txt"
    destination = tmp_path / "destination.txt"
    source.write_text("new", encoding="utf-8")
    destination.write_text("old", encoding="utf-8")
    alpha.start_background()
    beta.start_background()
    try:
        task = TaskClient.for_paglet(
            alpha.create(SingleFileTransferPaglet, FileTransferState()),
            SingleFileTransferPaglet,
        )

        summary = task.start_and_wait(
            FileTransferRequest(
                source_path=str(source),
                destination_path=str(destination),
                target_host=beta.address,
            )
        )

        assert summary.status is TaskStatus.FAILED
        assert "destination already exists" in summary.error
        assert destination.read_text(encoding="utf-8") == "old"
        assert source.exists()
    finally:
        beta.stop()
        alpha.stop()


def test_single_file_transfer_move_failure_keeps_source(tmp_path: Path):
    alpha = _host("alpha", tmp_path / "alpha")
    beta = _host("beta", tmp_path / "beta")
    source = tmp_path / "source.txt"
    destination = tmp_path / "destination.txt"
    source.write_text("new", encoding="utf-8")
    destination.write_text("old", encoding="utf-8")
    alpha.start_background()
    beta.start_background()
    try:
        task = TaskClient.for_paglet(
            alpha.create(SingleFileTransferPaglet, FileTransferState()),
            SingleFileTransferPaglet,
        )

        summary = task.start_and_wait(
            FileTransferRequest(
                source_path=str(source),
                destination_path=str(destination),
                target_host=beta.address,
                mode=FileTransferMode.MOVE,
            )
        )

        assert summary.status is TaskStatus.FAILED
        assert "destination already exists" in summary.error
        assert destination.read_text(encoding="utf-8") == "old"
        assert source.read_text(encoding="utf-8") == "new"
    finally:
        beta.stop()
        alpha.stop()


def _host(name: str, persistence_dir: Path) -> Host:
    return Host(
        name=name,
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
    )
