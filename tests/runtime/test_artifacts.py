# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import io
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from paglets.artifacts import ArtifactRef, ArtifactStore, PagletFileRef
from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import NotHandledError, TransferError
from paglets.core.messages import Message
from paglets.remote.client import HostClient
from paglets.runtime.host import Host
from tests.support import free_port


@dataclass
class FileState(PagletState):
    arrival_exists: bool = False
    arrival_text: str = ""
    clone_exists: bool = False
    clone_text: str = ""


class FileAgent(Paglet[FileState]):
    State = FileState

    def on_arrival(self, event) -> None:
        files = self.registered_files()
        if files:
            path = self.file_path(files[0])
            self.state.arrival_exists = path.exists()
            self.state.arrival_text = path.read_text(encoding="utf-8")

    def on_clone(self, event) -> None:
        files = self.registered_files()
        if files:
            path = self.file_path(files[0])
            self.state.clone_exists = path.exists()
            self.state.clone_text = path.read_text(encoding="utf-8")

    def handle_message(self, message: Message):
        if message.kind == "register_external":
            ref = self.register_file(
                str(message.args["path"]),
                name=str(message.args.get("name") or "data.txt"),
                mode=str(message.args.get("mode") or "copy"),
            )
            return ref.to_wire()
        if message.kind == "register_work":
            path = self.work_dir() / str(message.args.get("filename") or "work.txt")
            path.write_text(str(message.args.get("text") or "work"), encoding="utf-8")
            ref = self.register_file(path, name=str(message.args.get("name") or path.name), mode="copy")
            return ref.to_wire()
        if message.kind == "read":
            return self.file_path(str(message.args["name"])).read_text(encoding="utf-8")
        if message.kind == "files":
            return [ref.to_wire() for ref in self.registered_files()]
        if message.kind == "state":
            return {
                "arrival_exists": self.state.arrival_exists,
                "arrival_text": self.state.arrival_text,
                "clone_exists": self.state.clone_exists,
                "clone_text": self.state.clone_text,
            }
        if message.kind == "receive_artifact":
            artifact = ArtifactRef.from_wire(message.args["artifact"])
            target = self.work_dir() / "received.bin"
            self.download_artifact(artifact, target)
            return {"text": target.read_text(encoding="utf-8"), "artifact": artifact.to_wire()}
        return self.not_handled()


def test_artifact_ref_and_paglet_file_ref_wire_round_trip(tmp_path: Path):
    source = tmp_path / "data.txt"
    source.write_text("payload", encoding="utf-8")
    store = ArtifactStore(tmp_path / "artifacts", host_url="http://alpha")
    artifact = store.create_from_path(source, owner_agent_id="agent", name="data.txt").ref

    assert ArtifactRef.from_wire(artifact.to_wire()) == artifact

    file_ref = PagletFileRef(
        name="data",
        mode="move",
        source_host_name="alpha",
        source_host_url="http://alpha",
        source_path=str(source),
        size_bytes=source.stat().st_size,
        sha256=artifact.sha256,
        current_host_name="alpha",
        current_host_url="http://alpha",
        current_path=str(source),
    )
    assert PagletFileRef.from_wire(file_ref.to_wire()).to_wire() == file_ref.to_wire()


def test_artifact_store_deletes_temp_blob_on_checksum_failure(tmp_path: Path):
    store = ArtifactStore(tmp_path / "artifacts", host_url="http://alpha")

    with pytest.raises(TransferError):
        store.create_from_stream(io.BytesIO(b"payload"), size_bytes=7, expected_sha256="0" * 64)

    assert list((tmp_path / "artifacts" / "tmp").glob("*.part")) == []
    assert list((tmp_path / "artifacts" / "blobs").glob("*.bin")) == []


def test_artifact_store_deletes_temp_blob_on_interrupted_stream(tmp_path: Path):
    class InterruptedStream:
        def __init__(self):
            self.calls = 0

        def read(self, _size=-1):
            self.calls += 1
            if self.calls == 1:
                return b"partial"
            raise OSError("interrupted")

    store = ArtifactStore(tmp_path / "artifacts", host_url="http://alpha")

    with pytest.raises(OSError):
        store.create_from_stream(InterruptedStream(), size_bytes=-1)

    assert list((tmp_path / "artifacts" / "tmp").glob("*.part")) == []
    assert list((tmp_path / "artifacts" / "blobs").glob("*.bin")) == []


def test_direct_artifact_upload_download_delete(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    source = tmp_path / "source.db"
    source.write_bytes(b"sqlite-bytes")
    target = tmp_path / "target.db"
    host.start_background()
    try:
        ref = host.client.upload_artifact(host.address, source, owner_agent_id="agent", name="result.db")
        assert ref.name == "result.db"
        assert host.client.list_artifacts(host.address, owner_agent_id="agent") == [ref]

        host.client.download_artifact(ref, target)
        assert target.read_bytes() == b"sqlite-bytes"

        host.client.delete_artifact(ref)
        assert host.client.list_artifacts(host.address) == []
    finally:
        host.stop()


def test_host_client_download_artifact_requires_id_when_called_with_host_url(tmp_path: Path):
    client = HostClient()

    with pytest.raises(ValueError, match=r"download_artifact.*artifact_id"):
        client.download_artifact("http://127.0.0.1:1", tmp_path / "target.bin")


def test_host_client_delete_artifact_requires_id_when_called_with_host_url():
    client = HostClient()

    with pytest.raises(ValueError, match=r"delete_artifact.*artifact_id"):
        client.delete_artifact("http://127.0.0.1:1")


def test_proxy_send_artifact_delivers_ref_and_cleans_failed_delivery(tmp_path: Path):
    host = _host("alpha", tmp_path / "alpha")
    source = tmp_path / "payload.bin"
    source.write_text("artifact-text", encoding="utf-8")
    host.start_background()
    try:
        proxy = host.create(FileAgent, FileState())
        reply = proxy.send_artifact(Message("receive_artifact"), source, name="payload.bin")
        assert reply["text"] == "artifact-text"
        assert len(host.client.list_artifacts(host.address)) == 1

        with pytest.raises(NotHandledError):
            proxy.send_artifact(Message("missing_handler"), source, name="failed.bin")
        names = [ref.name for ref in host.client.list_artifacts(host.address)]
        assert "failed.bin" not in names
    finally:
        host.stop()


def test_registered_file_dispatch_moves_non_scratch_source_and_arrives_before_on_arrival(tmp_path: Path):
    alpha = _host("alpha", tmp_path / "alpha")
    beta = _host("beta", tmp_path / "beta")
    source = tmp_path / "external.txt"
    source.write_text("move-me", encoding="utf-8")
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create(FileAgent, FileState())
        registered = proxy.send(Message("register_external", {"path": str(source), "name": "data.txt", "mode": "move"}))

        remote = proxy.dispatch(beta.address)
        state = remote.send(Message("state"))
        files = remote.send(Message("files"))
        target_path = Path(files[0]["current_path"])

        assert registered["source_path"] == str(source.resolve(strict=False))
        assert not source.exists()
        assert state["arrival_exists"] is True
        assert state["arrival_text"] == "move-me"
        assert remote.send(Message("read", {"name": "data.txt"})) == "move-me"
        assert target_path.exists()

        remote.dispose()
        assert not target_path.exists()
    finally:
        beta.stop()
        alpha.stop()


def test_registered_file_clone_copies_even_when_assignment_is_move(tmp_path: Path):
    alpha = _host("alpha", tmp_path / "alpha")
    beta = _host("beta", tmp_path / "beta")
    source = tmp_path / "external.txt"
    source.write_text("clone-me", encoding="utf-8")
    alpha.start_background()
    beta.start_background()
    try:
        proxy = alpha.create(FileAgent, FileState())
        proxy.send(Message("register_external", {"path": str(source), "name": "data.txt", "mode": "move"}))

        clone = proxy.clone(beta.address)
        state = clone.send(Message("state"))
        files = clone.send(Message("files"))

        assert source.read_text(encoding="utf-8") == "clone-me"
        assert state["clone_exists"] is True
        assert state["clone_text"] == "clone-me"
        assert Path(files[0]["current_path"]).exists()
        assert files[0]["source_path"] == str(source.resolve(strict=False))
    finally:
        beta.stop()
        alpha.stop()


def test_registered_file_dispatch_through_relay(tmp_path: Path):
    hub, beta, laptop = _relay_hosts(tmp_path)
    source = tmp_path / "relay.txt"
    source.write_text("relay-payload", encoding="utf-8")
    hub.start_background()
    beta.start_background()
    laptop.start_background()
    try:
        _wait_for(lambda: laptop.mesh.is_online("B"))
        proxy = laptop.create(FileAgent, FileState())
        proxy.send(Message("register_external", {"path": str(source), "name": "relay.txt", "mode": "copy"}))

        remote = proxy.dispatch(beta.address)

        assert remote.send(Message("read", {"name": "relay.txt"})) == "relay-payload"
    finally:
        laptop.stop()
        beta.stop()
        hub.stop()


def _host(name: str, persistence_dir: Path) -> Host:
    return Host(
        name,
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
    )


def _relay_hosts(tmp_path: Path):
    port = free_port()
    public_url = f"http://127.0.0.1:{port}/paglets"
    hub = Host(
        name="A",
        host="127.0.0.1",
        port=port,
        api_key="secret",
        public_url=public_url,
        persistence_dir=tmp_path / "A",
        mesh_multicast=False,
        mesh_lan_discovery=False,
        relay_delivery_timeout=10.0,
    )
    beta = Host(
        name="B",
        api_key="secret",
        connect_to=public_url,
        persistence_dir=tmp_path / "B",
        mesh_multicast=False,
        mesh_lan_discovery=False,
        relay_delivery_timeout=10.0,
    )
    laptop = Host(
        name="L",
        api_key="secret",
        connect_to=public_url,
        persistence_dir=tmp_path / "L",
        mesh_multicast=False,
        mesh_lan_discovery=False,
        relay_delivery_timeout=10.0,
    )
    return hub, beta, laptop


def _wait_for(predicate, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for condition")
