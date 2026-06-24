# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from paglets.examples.file_grabber import (
    FileGrabberPaglet,
    FileGrabberState,
    FileGrabMode,
    FileGrabRequest,
    FileGrabResult,
    TaskStatus,
    format_bytes,
)
from paglets.examples.file_grabber.cli import run_transfer
from paglets.patterns.file_mobility import FileMobilityMixin, SingleFileTransferPaglet
from paglets.patterns.tasks import TaskClient
from paglets.remote.admin import ServerRef
from paglets.runtime.host import Host
from tests.support import free_port


def test_file_grabber_dry_run_uses_typed_task_client(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    source = tmp_path / "source.txt"
    source.write_text("payload", encoding="utf-8")
    host.start_background()
    try:
        proxy = host.create(FileGrabberPaglet, FileGrabberState())
        task = TaskClient.for_paglet(proxy, FileGrabberPaglet)
        summary = task.start_and_wait(
            FileGrabRequest(
                source_path=str(source),
                destination_path="/tmp/out.txt",
                target_host="http://beta",
                dry_run=True,
                destination_label="beta",
            )
        )

        assert summary.status is TaskStatus.COMPLETED
        assert isinstance(summary.result, FileGrabResult)
        assert summary.result.dry_run is True
        assert summary.result.size == "7 B"
        assert summary.result.source.path == str(source.resolve(strict=False))
        assert summary.result.destination.path == "/tmp/out.txt"
        assert task.proxy.host_url == host.address
        assert source.read_text(encoding="utf-8") == "payload"
    finally:
        host.stop()


def test_file_grabber_push_copy_dispatches_with_registered_file(tmp_path: Path):
    alpha = _host("alpha", tmp_path / "alpha")
    beta = _host("beta", tmp_path / "beta")
    source = tmp_path / "source.txt"
    destination = tmp_path / "remote" / "out.txt"
    source.write_text("copy-payload", encoding="utf-8")
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create(FileGrabberPaglet, FileGrabberState())
        task = TaskClient.for_paglet(proxy, FileGrabberPaglet)
        summary = task.start_and_wait(
            FileGrabRequest(
                source_path=str(source),
                destination_path=str(destination),
                target_host=beta.address,
                destination_label="beta",
            )
        )

        assert summary.status is TaskStatus.COMPLETED
        assert summary.result is not None
        assert summary.result.destination.host_name == "beta"
        assert task.proxy.host_url == beta.address
        assert destination.read_text(encoding="utf-8") == "copy-payload"
        assert source.read_text(encoding="utf-8") == "copy-payload"
        assert alpha.get_proxy(proxy.agent_id) is None
    finally:
        beta.stop()
        alpha.stop()


def test_file_grabber_pull_move_cli_helper_deletes_remote_source(tmp_path: Path):
    alpha = _host("alpha", tmp_path / "alpha")
    beta = _host("beta", tmp_path / "beta")
    source = tmp_path / "remote-source.txt"
    destination = tmp_path / "local-out.txt"
    source.write_text("move-payload", encoding="utf-8")
    alpha.start_background()
    beta.start_background()
    try:
        args = Namespace(
            command="pull",
            source=str(source),
            dest=str(destination),
            mode=FileGrabMode.MOVE.value,
            dry=False,
            overwrite=False,
            request_timeout=20.0,
        )
        summary = run_transfer(
            ServerRef("alpha", alpha.address),
            ServerRef("beta", beta.address),
            args,
            client=alpha.client,
        )

        assert summary.status is TaskStatus.COMPLETED
        assert summary.result is not None
        assert summary.result.source.host_name == "beta"
        assert summary.result.destination.host_name == "alpha"
        assert destination.read_text(encoding="utf-8") == "move-payload"
        assert not source.exists()
    finally:
        beta.stop()
        alpha.stop()


def test_file_grabber_move_failure_keeps_source_and_destination(tmp_path: Path):
    alpha = _host("alpha", tmp_path / "alpha")
    beta = _host("beta", tmp_path / "beta")
    source = tmp_path / "source.txt"
    destination = tmp_path / "destination.txt"
    source.write_text("new", encoding="utf-8")
    destination.write_text("old", encoding="utf-8")
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create(FileGrabberPaglet, FileGrabberState())
        task = TaskClient.for_paglet(proxy, FileGrabberPaglet)

        summary = task.start_and_wait(
            FileGrabRequest(
                source_path=str(source),
                destination_path=str(destination),
                target_host=beta.address,
                mode=FileGrabMode.MOVE,
                destination_label="beta",
            )
        )

        assert summary.status is TaskStatus.FAILED
        assert "destination already exists" in summary.error
        assert destination.read_text(encoding="utf-8") == "old"
        assert source.read_text(encoding="utf-8") == "new"
    finally:
        beta.stop()
        alpha.stop()


def test_file_grabber_shows_workflow_without_turnkey_base_class():
    assert issubclass(FileGrabberPaglet, FileMobilityMixin)
    assert SingleFileTransferPaglet not in FileGrabberPaglet.__mro__
    assert FileGrabberPaglet.run_task.__qualname__.startswith("FileGrabberPaglet.")
    assert FileGrabberPaglet.on_arrival.__qualname__.startswith("FileGrabberPaglet.")


def test_file_grabber_uses_binary_scaled_byte_units():
    assert format_bytes(1024) == "1.0 KB"
    assert format_bytes(1024**2) == "1.0 MB"


def _host(name: str, persistence_dir: Path) -> Host:
    return Host(
        name=name,
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
    )
