# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import threading
import time

import pytest

import paglets.tooling.git_update as git_update


def test_git_update_reports_missing_requested_hash_after_fetch(tmp_path: Path):
    repo = _make_repo(tmp_path)
    head = git_update.current_head(repo)

    result = git_update.update_checkout(repo, process_start_head=head, target_hash="f" * 40)

    assert result.ok is False
    assert result.status == "target-missing"
    assert result.target_hash == "f" * 40
    assert "may not have been pushed" in result.error
    assert result.restart_required is False


def test_git_update_refuses_dirty_checkout_before_fetch(tmp_path: Path):
    repo = _make_repo(tmp_path)
    head = git_update.current_head(repo)
    (repo / "README.md").write_text("local change\n", encoding="utf-8")

    result = git_update.update_checkout(repo, process_start_head=head)

    assert result.ok is False
    assert result.status == "dirty-worktree"
    assert "clean checkout" in result.error
    assert "README.md" in result.stdout
    assert result.restart_required is False


def test_git_update_requires_restart_when_checkout_changed_before_lock(tmp_path: Path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    old_head = "a" * 40
    new_head = "b" * 40
    uv_sync_calls: list[Path] = []

    def fake_run_git(args, _repo_root, *, timeout=git_update.GIT_COMMAND_TIMEOUT_SECONDS):
        if args == ["rev-parse", "HEAD"]:
            return git_update.GitCommandResult(0, f"{new_head}\n")
        if args == ["status", "--porcelain"]:
            return git_update.GitCommandResult(0)
        if args in (["fetch"], ["pull"]):
            return git_update.GitCommandResult(0)
        return git_update.GitCommandResult(1, stderr=f"unexpected git args {args!r}")

    monkeypatch.setattr(git_update, "_run_git", fake_run_git)
    monkeypatch.setattr(
        git_update,
        "_run_uv_sync",
        lambda repo_root: uv_sync_calls.append(repo_root) or git_update.GitCommandResult(0, stdout="synced"),
    )

    result = git_update.update_checkout(repo, process_start_head=old_head)

    assert result.ok is True
    assert result.status == "current"
    assert result.changed is False
    assert result.restart_required is True
    assert result.after_head == new_head
    assert result.uv_sync_run is True
    assert uv_sync_calls == [repo.resolve()]
    assert "synced" in result.stdout


def test_git_update_blocks_restart_when_uv_sync_fails(tmp_path: Path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    old_head = "a" * 40
    new_head = "b" * 40

    def fake_run_git(args, _repo_root, *, timeout=git_update.GIT_COMMAND_TIMEOUT_SECONDS):
        if args == ["rev-parse", "HEAD"]:
            return git_update.GitCommandResult(0, f"{new_head}\n")
        if args == ["status", "--porcelain"]:
            return git_update.GitCommandResult(0)
        if args in (["fetch"], ["pull"]):
            return git_update.GitCommandResult(0)
        return git_update.GitCommandResult(1, stderr=f"unexpected git args {args!r}")

    monkeypatch.setattr(git_update, "_run_git", fake_run_git)
    monkeypatch.setattr(git_update, "_run_uv_sync", lambda _repo_root: git_update.GitCommandResult(1, stderr="sync failed"))

    result = git_update.update_checkout(repo, process_start_head=old_head)

    assert result.ok is False
    assert result.status == "uv-sync-failed"
    assert result.restart_required is False
    assert result.uv_sync_run is True
    assert result.error == "sync failed"


def test_git_update_can_defer_dependency_sync_until_reexec(tmp_path: Path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    old_head = "a" * 40
    new_head = "b" * 40
    uv_sync_calls = 0

    def fake_run_git(args, _repo_root, *, timeout=git_update.GIT_COMMAND_TIMEOUT_SECONDS):
        if args == ["rev-parse", "HEAD"]:
            return git_update.GitCommandResult(0, f"{new_head}\n")
        if args == ["status", "--porcelain"]:
            return git_update.GitCommandResult(0)
        if args in (["fetch"], ["pull"]):
            return git_update.GitCommandResult(0)
        return git_update.GitCommandResult(1, stderr=f"unexpected git args {args!r}")

    def fake_uv_sync(_repo_root):
        nonlocal uv_sync_calls
        uv_sync_calls += 1
        return git_update.GitCommandResult(1, stderr="sync should be deferred")

    monkeypatch.setattr(git_update, "_run_git", fake_run_git)
    monkeypatch.setattr(git_update, "_run_uv_sync", fake_uv_sync)

    result = git_update.update_checkout(repo, process_start_head=old_head, sync_dependencies=False)

    assert result.ok is True
    assert result.restart_required is True
    assert result.uv_sync_run is False
    assert uv_sync_calls == 0


def test_git_update_lock_removes_stale_dead_owner(tmp_path: Path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    lock_path = repo / ".git" / git_update.GIT_UPDATE_LOCK_NAME
    lock_path.mkdir()
    (lock_path / "owner").write_text(f"pid=123456789\ntime={time.time()}\n", encoding="utf-8")
    monkeypatch.setattr(git_update.psutil, "pid_exists", lambda _pid: False)

    with git_update.GitUpdateLock(repo, timeout=0.1):
        owner = (lock_path / "owner").read_text(encoding="utf-8")
        assert f"pid={os.getpid()}" in owner

    assert not lock_path.exists()


def test_git_update_lock_keeps_fresh_unknown_owner(tmp_path: Path):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    lock_path = repo / ".git" / git_update.GIT_UPDATE_LOCK_NAME
    lock_path.mkdir()

    with pytest.raises(git_update.GitUpdateError, match="Timed out waiting"):
        git_update.GitUpdateLock(repo, timeout=0.0).acquire()

    assert lock_path.exists()


def test_git_update_lock_removes_old_unknown_owner(tmp_path: Path):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    lock_path = repo / ".git" / git_update.GIT_UPDATE_LOCK_NAME
    lock_path.mkdir()
    old_time = time.time() - git_update.GIT_UPDATE_STALE_LOCK_GRACE_SECONDS - 1.0
    os.utime(lock_path, (old_time, old_time))

    with git_update.GitUpdateLock(repo, timeout=0.1):
        assert (lock_path / "owner").exists()

    assert not lock_path.exists()


def test_git_update_lock_waits_for_live_owner(tmp_path: Path):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    lock_path = repo / ".git" / git_update.GIT_UPDATE_LOCK_NAME
    lock_path.mkdir()
    (lock_path / "owner").write_text(f"pid={os.getpid()}\ntime={time.time()}\n", encoding="utf-8")

    with pytest.raises(git_update.GitUpdateError, match="live process"):
        git_update.GitUpdateLock(repo, timeout=0.0).acquire()

    assert lock_path.exists()


def test_git_update_lock_serializes_pull_calls(tmp_path: Path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    head = "a" * 40
    active_pulls = 0
    max_active_pulls = 0
    guard = threading.Lock()

    def fake_run_git(args, _repo_root, *, timeout=git_update.GIT_COMMAND_TIMEOUT_SECONDS):
        nonlocal active_pulls, max_active_pulls
        if args == ["rev-parse", "HEAD"]:
            return git_update.GitCommandResult(0, f"{head}\n")
        if args == ["status", "--porcelain"]:
            return git_update.GitCommandResult(0)
        if args == ["fetch"]:
            return git_update.GitCommandResult(0)
        if args == ["pull"]:
            with guard:
                active_pulls += 1
                max_active_pulls = max(max_active_pulls, active_pulls)
            time.sleep(0.05)
            with guard:
                active_pulls -= 1
            return git_update.GitCommandResult(0)
        return git_update.GitCommandResult(1, stderr=f"unexpected git args {args!r}")

    monkeypatch.setattr(git_update, "_run_git", fake_run_git)
    results: list[git_update.GitUpdateResult] = []

    threads = [
        threading.Thread(
            target=lambda: results.append(git_update.update_checkout(repo, process_start_head=head, lock_timeout=1.0))
        )
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2.0)

    assert len(results) == 2
    assert all(result.ok for result in results)
    assert max_active_pulls == 1


def test_git_update_lock_serializes_uv_sync_calls(tmp_path: Path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    old_head = "a" * 40
    new_head = "b" * 40
    active_syncs = 0
    max_active_syncs = 0
    guard = threading.Lock()

    def fake_run_git(args, _repo_root, *, timeout=git_update.GIT_COMMAND_TIMEOUT_SECONDS):
        if args == ["rev-parse", "HEAD"]:
            return git_update.GitCommandResult(0, f"{new_head}\n")
        if args == ["status", "--porcelain"]:
            return git_update.GitCommandResult(0)
        if args in (["fetch"], ["pull"]):
            return git_update.GitCommandResult(0)
        return git_update.GitCommandResult(1, stderr=f"unexpected git args {args!r}")

    def fake_uv_sync(_repo_root):
        nonlocal active_syncs, max_active_syncs
        with guard:
            active_syncs += 1
            max_active_syncs = max(max_active_syncs, active_syncs)
        time.sleep(0.05)
        with guard:
            active_syncs -= 1
        return git_update.GitCommandResult(0)

    monkeypatch.setattr(git_update, "_run_git", fake_run_git)
    monkeypatch.setattr(git_update, "_run_uv_sync", fake_uv_sync)
    results: list[git_update.GitUpdateResult] = []

    threads = [
        threading.Thread(
            target=lambda: results.append(git_update.update_checkout(repo, process_start_head=old_head, lock_timeout=1.0))
        )
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2.0)

    assert len(results) == 2
    assert all(result.ok and result.restart_required for result in results)
    assert max_active_syncs == 1


def _make_repo(tmp_path: Path) -> Path:
    remote = tmp_path / "remote.git"
    repo = tmp_path / "repo"
    _git(tmp_path, "init", "--bare", str(remote))
    _git(tmp_path, "clone", str(remote), str(repo))
    _git(repo, "config", "user.email", "paglets@example.test")
    _git(repo, "config", "user.name", "Paglets Test")
    (repo / "README.md").write_text("initial\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-m", "initial")
    _git(repo, "push", "-u", "origin", "HEAD")
    return repo


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
