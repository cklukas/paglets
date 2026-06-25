# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
from pathlib import Path

from paglets.runtime.host import Host
from paglets.system.artifacts_cli import main
from tests.support import free_port


def test_artifacts_cli_lists_json_and_table(tmp_path: Path, capsys):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    source = tmp_path / "artifact.bin"
    source.write_bytes(b"payload")
    host.start_background()
    try:
        ref = host.client.upload_artifact(host.address, source, owner_agent_id="agent", name="artifact.bin")

        assert main(["--host", host.address, "list", "--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["artifacts"][0]["artifact_id"] == ref.artifact_id

        assert main(["--host", host.address, "list"]) == 0
        output = capsys.readouterr().out
        assert "artifact.bin" in output
        assert "7 B" in output
    finally:
        host.stop()


def test_artifacts_cli_uses_default_api_key_env(tmp_path: Path, capsys, monkeypatch):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        api_key="secret",
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    source = tmp_path / "artifact.bin"
    source.write_bytes(b"payload")
    monkeypatch.setenv("PAGLETS_API_KEY", "secret")
    host.start_background()
    try:
        ref = host.client.upload_artifact(host.address, source, owner_agent_id="agent", name="artifact.bin")

        assert main(["--host", host.address, "list", "--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["artifacts"][0]["artifact_id"] == ref.artifact_id
    finally:
        host.stop()
