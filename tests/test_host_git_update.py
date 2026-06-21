# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path
import threading

import pytest

from paglets import Host
from paglets.admin import ServerRef
from paglets import cli as host_cli
from paglets import git_update
from paglets.runtime_values import LaunchConfigSyncAction
from paglets.startup import LaunchConfig, LaunchConfigSyncResult
from tests.test_paglets_core import free_port


def test_host_cli_accepts_auto_update_from_git_flag():
    args = host_cli._parser().parse_args(["--name", "alpha", "--auto-update-from-git"])

    assert args.auto_update_from_git is True


def test_auto_update_discovery_targets_use_mesh_and_lan_discovery(monkeypatch):
    monkeypatch.setattr(
        host_cli,
        "discover_mesh_entry_servers",
        lambda timeout=1.0: [ServerRef("mesh", "http://192.168.86.28:8765")],
    )
    monkeypatch.setattr(
        host_cli,
        "discover_lan_entry_servers",
        lambda *, ports, timeout=0.25: [ServerRef("lan", "http://192.168.86.29:8765")],
    )

    assert host_cli._auto_update_discovery_targets(8765) == [
        "http://192.168.86.28:8765",
        "http://192.168.86.29:8765",
    ]


def test_windows_python_reexec_argv_does_not_put_spaced_path_in_argv0():
    argv = host_cli._python_reexec_argv(
        ["--name", "windows", "--bind-public"],
        executable=r"C:\Users\Christian Klukas\git\paglets\.venv\Scripts\python.exe",
        windows=True,
    )

    assert argv == ["python.exe", "-m", "paglets.cli", "--name", "windows", "--bind-public"]


def test_reexec_command_prefers_uv_run_python_on_windows():
    executable, argv = host_cli._reexec_command(
        ["--name", "windows", "--bind-public"],
        uv_executable=r"C:\Program Files\uv\uv.exe",
        windows=True,
    )

    assert executable == r"C:\Program Files\uv\uv.exe"
    assert argv == ["uv.exe", "run", "python", "-m", "paglets.cli", "--name", "windows", "--bind-public"]


def test_host_cli_reexecutes_runtime_git_restart_from_main_thread(tmp_path: Path, monkeypatch, capsys):
    head = "a" * 40
    restart_threads: list[str] = []

    class RestartRequested(RuntimeError):
        pass

    class FakeMesh:
        code_version = head
        version_warning = None

    class FakeHost:
        def __init__(self, *args, **kwargs):
            self.name = kwargs["name"]
            self.address = "http://127.0.0.1:8765"
            self.port = int(kwargs["port"])
            self.mesh = FakeMesh()
            self._restart_callback = kwargs["auto_update_restart_callback"]

        def start_background(self):
            return None

        def broadcast_git_update(self, targets=None):
            return []

        def serve_forever(self):
            self._restart_callback()

    def fake_reexec(_argv):
        restart_threads.append(threading.current_thread().name)
        raise RestartRequested()

    monkeypatch.setattr(host_cli.git_update, "find_repo_root", lambda _start: tmp_path)
    monkeypatch.setattr(host_cli.git_update, "current_head", lambda _repo: head)
    monkeypatch.setattr(
        host_cli.git_update,
        "update_checkout",
        lambda repo, *, process_start_head: git_update.GitUpdateResult(
            ok=True,
            status="current",
            repo_root=str(repo),
            before_head=head,
            after_head=head,
            process_start_head=process_start_head,
        ),
    )
    monkeypatch.setattr(
        host_cli,
        "sync_launch_config",
        lambda path, **kwargs: LaunchConfigSyncResult(LaunchConfigSyncAction.UNCHANGED, Path(path), "up to date"),
    )
    monkeypatch.setattr(host_cli, "load_launch_config", lambda _path: LaunchConfig())
    monkeypatch.setattr(host_cli, "Host", FakeHost)
    monkeypatch.setattr(host_cli, "_auto_update_discovery_targets", lambda _port: [])
    monkeypatch.setattr(host_cli, "_reexec", fake_reexec)

    with pytest.raises(RestartRequested):
        host_cli.main(["--name", "windows", "--port", "8765", "--auto-update-from-git"])

    assert restart_threads == ["MainThread"]
    assert "git auto-update restart requested; restarting" in capsys.readouterr().err


def test_host_cli_cancels_auto_update_when_checkout_is_dirty(tmp_path: Path, monkeypatch, capsys):
    head = "a" * 40

    monkeypatch.setattr(host_cli.git_update, "find_repo_root", lambda _start: tmp_path)
    monkeypatch.setattr(host_cli.git_update, "current_head", lambda _repo: head)
    monkeypatch.setattr(
        host_cli.git_update,
        "update_checkout",
        lambda repo, *, process_start_head: git_update.GitUpdateResult(
            ok=False,
            status="dirty-worktree",
            repo_root=str(repo),
            before_head=head,
            after_head=head,
            process_start_head=process_start_head,
            stdout=" M src/paglets/cli.py",
            error="git auto-update requires a clean checkout",
        ),
    )

    result = host_cli.main(["--name", "alpha", "--auto-update-from-git"])

    assert result == 1
    err = capsys.readouterr().err
    assert "clean git checkout" in err
    assert "startup cancelled" in err
    assert "src/paglets/cli.py" in err


def test_git_update_endpoint_is_disabled_by_default(tmp_path: Path):
    host = _host(tmp_path)
    host.start_background()
    try:
        response = host.client.post_json(f"{host.address}/admin/git-update", {"target_hash": "a" * 40})
        health = host.client.get_json(f"{host.address}/health")
    finally:
        host.stop()

    assert response["status"] == "disabled"
    assert response["ok"] is False
    assert health["auto_update_from_git"] is False
    assert health["git_update"]["status"] == "disabled"


def test_git_update_endpoint_stores_missing_hash_failure(tmp_path: Path, monkeypatch):
    old_head = "a" * 40

    def fake_update_checkout(repo_root, *, process_start_head, target_hash=None, lock_timeout=0):
        return git_update.GitUpdateResult(
            ok=False,
            status="target-missing",
            repo_root=str(repo_root),
            before_head=old_head,
            after_head=old_head,
            process_start_head=process_start_head,
            target_hash=target_hash or "",
            error=f"requested commit {target_hash} was not found after git fetch; it may not have been pushed yet",
        )

    monkeypatch.setattr("paglets.host.git_update.update_checkout", fake_update_checkout)
    host = _host(tmp_path, auto_update=True, process_start_head=old_head)
    host.start_background()
    try:
        response = host.client.post_json(f"{host.address}/admin/git-update", {"target_hash": "f" * 40})
        health = host.client.get_json(f"{host.address}/health")
    finally:
        host.stop()

    assert response["status"] == "target-missing"
    assert response["ok"] is False
    assert response["restart_scheduled"] is False
    assert health["git_update"]["status"] == "target-missing"
    assert "may not have been pushed" in health["git_update"]["error"]


def test_git_update_endpoint_schedules_restart_when_head_changes(tmp_path: Path, monkeypatch):
    old_head = "a" * 40
    new_head = "b" * 40
    restarted = threading.Event()

    def fake_update_checkout(repo_root, *, process_start_head, target_hash=None, lock_timeout=0):
        return git_update.GitUpdateResult(
            ok=True,
            status="updated",
            repo_root=str(repo_root),
            before_head=old_head,
            after_head=new_head,
            process_start_head=process_start_head,
            target_hash=target_hash or "",
            changed=True,
            restart_required=True,
        )

    monkeypatch.setattr("paglets.host.git_update.update_checkout", fake_update_checkout)
    host = _host(
        tmp_path,
        auto_update=True,
        process_start_head=old_head,
        restart_callback=restarted.set,
        restart_delay=0.01,
    )
    host.start_background()
    try:
        response = host.client.post_json(f"{host.address}/admin/git-update", {"target_hash": new_head})
        assert response["status"] == "updated"
        assert response["restart_scheduled"] is True
        assert restarted.wait(timeout=2.0)
    finally:
        host.stop()


def test_requesting_host_reports_remote_update_failure(tmp_path: Path, monkeypatch):
    old_head = "a" * 40
    messages: list[str] = []

    def fake_update_checkout(repo_root, *, process_start_head, target_hash=None, lock_timeout=0):
        return git_update.GitUpdateResult(
            ok=False,
            status="target-missing",
            repo_root=str(repo_root),
            before_head=old_head,
            after_head=old_head,
            process_start_head=process_start_head,
            target_hash=target_hash or "",
            stderr="fatal: bad object",
            error=f"requested commit {target_hash} was not found after git fetch; it may not have been pushed yet",
        )

    monkeypatch.setattr("paglets.host.git_update.update_checkout", fake_update_checkout)
    alpha = _host(tmp_path / "alpha", auto_update=True, process_start_head=old_head, reporter=messages.append)
    beta = _host(tmp_path / "beta", auto_update=True, process_start_head=old_head)
    alpha.start_background()
    beta.start_background()
    try:
        response = alpha.request_peer_git_update(beta.address, target_hash="f" * 40, throttle=False)
    finally:
        beta.stop()
        alpha.stop()

    assert response is not None
    assert response["status"] == "target-missing"
    assert messages
    assert "git push" in messages[0]
    assert "fatal: bad object" in messages[0]


def _host(
    tmp_path: Path,
    *,
    auto_update: bool = False,
    process_start_head: str = "",
    restart_callback=None,
    reporter=None,
    restart_delay: float = 0.2,
) -> Host:
    return Host(
        name=tmp_path.name or "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        mesh_version="host-git-update-test",
        persistence_dir=tmp_path / "state",
        auto_update_from_git=auto_update,
        git_repo_root=tmp_path,
        git_process_start_head=process_start_head,
        auto_update_restart_callback=restart_callback,
        auto_update_reporter=reporter,
        auto_update_restart_delay=restart_delay,
    )
