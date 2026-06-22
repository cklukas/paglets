# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message
from paglets.persistence.storage import ManagedStorage, StorageQuotaError
from paglets.runtime.host import Host
from tests.support import free_port


@dataclass
class StorageState(PagletState):
    last_work_path: str = ""


class StorageAgent(Paglet[StorageState]):
    State = StorageState

    def handle_message(self, message: Message):
        if message.kind == "make_work":
            path = self.work_dir() / str(message.args.get("name", "work.txt"))
            path.write_text(str(message.args.get("value", "x")), encoding="utf-8")
            self.state.last_work_path = str(path)
            return {"path": str(path)}
        if message.kind == "write_persistent":
            storage = self.persistent_storage()
            storage.write_text(str(message.args["path"]), str(message.args["value"]))
            status = storage.status()
            return {
                "root": status.root,
                "used_bytes": status.used_bytes,
                "quota_bytes": status.quota_bytes,
                "available_bytes": status.available_bytes,
            }
        if message.kind == "read_persistent":
            return self.persistent_storage().read_bytes(str(message.args["path"])).decode("utf-8")
        return self.not_handled()


def test_work_dir_is_cleared_on_host_startup_and_dispose(tmp_path: Path):
    persistence_dir = tmp_path / "alpha"
    host = _host("alpha", persistence_dir)
    host.start_background()
    try:
        proxy = host.create(StorageAgent, StorageState())
        work_path = Path(proxy.send(Message("make_work", {"name": "a.txt"}))["path"])
        assert work_path.exists()
    finally:
        host.stop()

    restarted = _host("alpha", persistence_dir)
    restarted.start_background()
    try:
        assert not work_path.exists()
        proxy = restarted.create(StorageAgent, StorageState())
        work_path = Path(proxy.send(Message("make_work", {"name": "b.txt"}))["path"])
        proxy.dispose()
        assert not work_path.parent.exists()
    finally:
        restarted.stop()


def test_dispose_inactive_paglet_clears_work_dir(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    host.start_background()
    try:
        proxy = host.create(StorageAgent, StorageState())
        work_path = Path(proxy.send(Message("make_work"))["path"])
        proxy.deactivate()
        assert work_path.exists()

        proxy.dispose()

        assert not work_path.parent.exists()
    finally:
        host.stop()


def test_dispatch_and_retract_clear_source_work_dir(tmp_path: Path):
    alpha = _host("alpha", tmp_path / "alpha")
    beta = _host("beta", tmp_path / "beta")
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create(StorageAgent, StorageState())
        alpha_work = Path(proxy.send(Message("make_work", {"name": "alpha.txt"}))["path"])
        remote = proxy.dispatch(beta.address)

        assert not alpha_work.parent.exists()

        beta_work = Path(remote.send(Message("make_work", {"name": "beta.txt"}))["path"])
        returned = alpha.retract(beta.address, remote.agent_id)

        assert returned.agent_id == proxy.agent_id
        assert not beta_work.parent.exists()
    finally:
        beta.stop()
        alpha.stop()


def test_persistent_storage_quota_is_shared_per_class_and_survives_restart(tmp_path: Path):
    persistence_dir = tmp_path / "alpha"
    host = _host("alpha", persistence_dir, quota=10)
    host.start_background()
    try:
        one = host.create(StorageAgent, StorageState())
        two = host.create(StorageAgent, StorageState())

        one.send(Message("write_persistent", {"path": "one.txt", "value": "123456"}))
        with pytest.raises(StorageQuotaError):
            two.send(Message("write_persistent", {"path": "two.txt", "value": "12345"}))
    finally:
        host.stop()

    restarted = _host("alpha", persistence_dir, quota=10)
    restarted.start_background()
    try:
        proxy = restarted.create(StorageAgent, StorageState())
        assert proxy.send(Message("read_persistent", {"path": "one.txt"})) == "123456"
    finally:
        restarted.stop()


def test_managed_storage_rejects_path_traversal(tmp_path: Path):
    storage = ManagedStorage(tmp_path / "storage")

    with pytest.raises(ValueError):
        storage.write_text("../escape.txt", "x")


def _host(name: str, persistence_dir: Path, *, quota: int | None = None) -> Host:
    kwargs = {}
    if quota is not None:
        kwargs["persistent_storage_quota_bytes"] = quota
    return Host(
        name,
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
        **kwargs,
    )
