# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
import time
from pathlib import Path

from paglets.config.startup import load_launch_config, sync_launch_config
from paglets.examples.compute.cli import _parser as pi_parser
from paglets.examples.compute.cli import _resolve_output_path
from paglets.examples.compute.cli import main as pi_main
from paglets.remote.admin import ServerRef
from paglets.runtime.host import Host
from tests.support import free_port


def test_pi_compute_cli_uses_output_file_not_client_polling():
    args = pi_parser().parse_args(["--digits", "8", "--output", "custom-pi.txt"])

    assert args.timeout == 0.0
    assert args.output == "custom-pi.txt"
    assert not hasattr(args, "request_timeout")
    assert not hasattr(args, "stream_chunk_size")


def test_pi_compute_cli_resolves_relative_output_against_cwd(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert _resolve_output_path("out/pi.txt") == tmp_path / "out" / "pi.txt"


def test_pi_compute_cli_json_output_is_submission_metadata(tmp_path: Path, capsys, monkeypatch):
    launch_config = _launch_config(tmp_path)
    host = _host("alpha", tmp_path / "alpha", launch_config=launch_config)
    host.start_background()
    output_path = tmp_path / "pi.txt"
    try:
        monkeypatch.setattr(
            "paglets.examples.compute.cli._select_entry_server",
            lambda *, entry_name, client: ServerRef("alpha", host.address),
        )

        result = pi_main(
            [
                "--digits",
                "4",
                "--batch-size",
                "1",
                "--max-cpu-percent",
                "100",
                "--output",
                str(output_path),
                "--json",
            ]
        )

        assert result == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["accepted"] is True
        assert payload["output_path"] == str(output_path)
        assert payload["host_url"] == host.address
        assert "decimal_digits" not in payload
        _wait_until(lambda: output_path.read_text(encoding="utf-8") == "3.1415")
    finally:
        host.stop()


def _launch_config(tmp_path: Path):
    path = tmp_path / "launch.toml"
    sync_launch_config(path, interactive=False)
    return load_launch_config(path)


def _host(name: str, persistence_dir: Path, *, launch_config=None) -> Host:
    return Host(
        name,
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=persistence_dir,
        launch_config=launch_config,
    )


def _wait_until(predicate, *, timeout: float = 3.0, interval: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()
