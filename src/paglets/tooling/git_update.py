# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from typing import Any

import psutil


GIT_COMMAND_TIMEOUT_SECONDS = 300.0
UV_SYNC_TIMEOUT_SECONDS = 600.0
GIT_UPDATE_LOCK_TIMEOUT_SECONDS = 300.0
GIT_UPDATE_LOCK_RETRY_SECONDS = 0.05
GIT_UPDATE_STALE_LOCK_GRACE_SECONDS = 5.0
GIT_UPDATE_LOCK_NAME = "paglets-auto-update.lock"

_GIT_HASH_RE = re.compile(r"^[0-9a-fA-F]{4,64}$")


@dataclass(frozen=True, slots=True)
class GitCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclass(frozen=True, slots=True)
class GitUpdateResult:
    ok: bool
    status: str
    repo_root: str
    before_head: str
    after_head: str
    process_start_head: str
    target_hash: str = ""
    changed: bool = False
    restart_required: bool = False
    stdout: str = ""
    stderr: str = ""
    error: str = ""
    uv_sync_run: bool = False

    def to_wire(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "repo_root": self.repo_root,
            "before_head": self.before_head,
            "after_head": self.after_head,
            "process_start_head": self.process_start_head,
            "target_hash": self.target_hash,
            "changed": self.changed,
            "restart_required": self.restart_required,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "error": self.error,
            "uv_sync_run": self.uv_sync_run,
        }


class GitUpdateError(RuntimeError):
    """Raised when git auto-update cannot inspect or update a checkout."""


@dataclass(frozen=True, slots=True)
class _LockOwner:
    pid: int | None = None
    timestamp: float | None = None


class GitUpdateLock:
    """Cross-platform lock backed by atomic directory creation under .git."""

    def __init__(self, repo_root: Path | str, *, timeout: float = GIT_UPDATE_LOCK_TIMEOUT_SECONDS):
        self.repo_root = Path(repo_root)
        self.timeout = max(0.0, float(timeout))
        self.path = git_common_dir(self.repo_root) / GIT_UPDATE_LOCK_NAME
        self._acquired = False

    def __enter__(self) -> "GitUpdateLock":
        self.acquire()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.release()

    def acquire(self) -> None:
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self.path.mkdir()
                self._acquired = True
                self._write_owner_file()
                return
            except FileExistsError:
                if self._remove_stale_lock_if_needed():
                    continue
                if time.monotonic() >= deadline:
                    raise GitUpdateError(
                        f"Timed out waiting for git update lock at {self.path}; "
                        f"{self._lock_description()}"
                    ) from None
                time.sleep(GIT_UPDATE_LOCK_RETRY_SECONDS)

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            shutil.rmtree(self.path)
        finally:
            self._acquired = False

    def _write_owner_file(self) -> None:
        try:
            (self.path / "owner").write_text(f"pid={os.getpid()}\ntime={time.time()}\n", encoding="utf-8")
        except OSError:
            pass

    def _remove_stale_lock_if_needed(self) -> bool:
        owner = self._read_owner()
        if owner.pid is not None and self._pid_matches_owner(owner):
            return False
        age = self._lock_age_seconds(owner)
        if owner.pid is None and age < GIT_UPDATE_STALE_LOCK_GRACE_SECONDS:
            return False
        try:
            shutil.rmtree(self.path)
            return True
        except FileNotFoundError:
            return True
        except OSError:
            return False

    def _read_owner(self) -> _LockOwner:
        try:
            text = (self.path / "owner").read_text(encoding="utf-8")
        except OSError:
            return _LockOwner()
        values: dict[str, str] = {}
        for line in text.splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
        try:
            pid = int(values["pid"]) if values.get("pid") else None
        except ValueError:
            pid = None
        try:
            timestamp = float(values["time"]) if values.get("time") else None
        except ValueError:
            timestamp = None
        return _LockOwner(pid=pid, timestamp=timestamp)

    def _pid_matches_owner(self, owner: _LockOwner) -> bool:
        pid = owner.pid
        if pid is None or pid <= 0:
            return False
        if pid == os.getpid():
            return True
        if not psutil.pid_exists(pid):
            return False
        if owner.timestamp is not None:
            try:
                create_time = psutil.Process(pid).create_time()
            except (psutil.Error, OSError):
                return True
            if create_time > owner.timestamp + 1.0:
                return False
        return True

    def _lock_age_seconds(self, owner: _LockOwner) -> float:
        timestamp = owner.timestamp
        if timestamp is None:
            try:
                timestamp = self.path.stat().st_mtime
            except OSError:
                return GIT_UPDATE_STALE_LOCK_GRACE_SECONDS
        return max(0.0, time.time() - timestamp)

    def _lock_description(self) -> str:
        owner = self._read_owner()
        if owner.pid is None:
            return "lock owner is unknown"
        if self._pid_matches_owner(owner):
            return f"lock is held by live process pid={owner.pid}"
        return f"lock owner pid={owner.pid} is no longer running"


def find_repo_root(start: Path | str | None = None) -> Path:
    cwd = Path.cwd() if start is None else Path(start)
    result = _run_git(["rev-parse", "--show-toplevel"], cwd)
    if not result.ok or not result.stdout.strip():
        detail = result.stderr.strip() or result.stdout.strip() or f"{cwd} is not inside a git repository"
        raise GitUpdateError(detail)
    return Path(result.stdout.strip()).resolve()


def git_common_dir(repo_root: Path | str) -> Path:
    repo_path = Path(repo_root)
    direct_git = repo_path / ".git"
    if direct_git.is_dir():
        return direct_git
    result = _run_git(["rev-parse", "--git-common-dir"], repo_path)
    if not result.ok or not result.stdout.strip():
        detail = result.stderr.strip() or result.stdout.strip() or f"could not locate .git for {repo_path}"
        raise GitUpdateError(detail)
    git_dir = Path(result.stdout.strip())
    if not git_dir.is_absolute():
        git_dir = repo_path / git_dir
    return git_dir.resolve()


def current_head(repo_root: Path | str) -> str:
    result = _run_git(["rev-parse", "HEAD"], Path(repo_root))
    if not result.ok or not result.stdout.strip():
        detail = result.stderr.strip() or result.stdout.strip() or "could not read git HEAD"
        raise GitUpdateError(detail)
    return result.stdout.strip()


def commit_exists(repo_root: Path | str, target_hash: str) -> bool:
    if not _GIT_HASH_RE.match(target_hash):
        return False
    result = _run_git(["cat-file", "-e", f"{target_hash}^{{commit}}"], Path(repo_root))
    return result.ok


def worktree_status(repo_root: Path | str) -> GitCommandResult:
    return _run_git(["status", "--porcelain"], Path(repo_root))


def is_worktree_clean(repo_root: Path | str) -> bool:
    status = worktree_status(repo_root)
    return status.ok and not status.stdout.strip()


def update_checkout(
    repo_root: Path | str,
    *,
    process_start_head: str,
    target_hash: str | None = None,
    lock_timeout: float = GIT_UPDATE_LOCK_TIMEOUT_SECONDS,
    sync_dependencies: bool = True,
) -> GitUpdateResult:
    repo_path = Path(repo_root).resolve()
    target = (target_hash or "").strip()
    before_head = ""
    after_head = ""
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []

    try:
        with GitUpdateLock(repo_path, timeout=lock_timeout):
            before_head = current_head(repo_path)
            status = worktree_status(repo_path)
            stdout_parts.append(status.stdout)
            stderr_parts.append(status.stderr)
            if not status.ok:
                after_head = _safe_current_head(repo_path, before_head)
                return _result(
                    False,
                    "status-failed",
                    repo_path,
                    before_head,
                    after_head,
                    process_start_head,
                    target,
                    stdout_parts,
                    stderr_parts,
                    status.stderr.strip() or status.stdout.strip() or "git status failed",
                )
            if status.stdout.strip():
                after_head = before_head
                return _result(
                    False,
                    "dirty-worktree",
                    repo_path,
                    before_head,
                    after_head,
                    process_start_head,
                    target,
                    stdout_parts,
                    stderr_parts,
                    "git auto-update requires a clean checkout; commit, stash, or remove local changes first",
                )

            fetch = _run_git(["fetch"], repo_path)
            stdout_parts.append(fetch.stdout)
            stderr_parts.append(fetch.stderr)
            if not fetch.ok:
                after_head = _safe_current_head(repo_path, before_head)
                return _result(
                    False,
                    "fetch-failed",
                    repo_path,
                    before_head,
                    after_head,
                    process_start_head,
                    target,
                    stdout_parts,
                    stderr_parts,
                    fetch.stderr.strip() or fetch.stdout.strip() or "git fetch failed",
                )

            if target and not commit_exists(repo_path, target):
                after_head = current_head(repo_path)
                return _result(
                    False,
                    "target-missing",
                    repo_path,
                    before_head,
                    after_head,
                    process_start_head,
                    target,
                    stdout_parts,
                    stderr_parts,
                    (
                        f"requested commit {target} was not found after git fetch; "
                        "it may not have been pushed yet"
                    ),
                )

            pull = _run_git(["pull"], repo_path)
            stdout_parts.append(pull.stdout)
            stderr_parts.append(pull.stderr)
            after_head = _safe_current_head(repo_path, before_head)
            if not pull.ok:
                return _result(
                    False,
                    "pull-failed",
                    repo_path,
                    before_head,
                    after_head,
                    process_start_head,
                    target,
                    stdout_parts,
                    stderr_parts,
                    pull.stderr.strip() or pull.stdout.strip() or "git pull failed",
                )

            if target and after_head != target:
                return GitUpdateResult(
                    ok=False,
                    status="target-not-current",
                    repo_root=str(repo_path),
                    before_head=before_head,
                    after_head=after_head,
                    process_start_head=process_start_head,
                    target_hash=target,
                    changed=bool(before_head and after_head and before_head != after_head),
                    restart_required=False,
                    stdout=_join_output(stdout_parts),
                    stderr=_join_output(stderr_parts),
                    error=f"requested commit {target} exists, but git pull left HEAD at {after_head}",
                )

            changed = after_head != before_head
            restart_required = after_head != process_start_head
            uv_sync_run = False
            if restart_required and sync_dependencies:
                uv_sync_run = True
                sync = _run_uv_sync(repo_path)
                stdout_parts.append(sync.stdout)
                stderr_parts.append(sync.stderr)
                if not sync.ok:
                    return GitUpdateResult(
                        ok=False,
                        status="uv-sync-failed",
                        repo_root=str(repo_path),
                        before_head=before_head,
                        after_head=after_head,
                        process_start_head=process_start_head,
                        target_hash=target,
                        changed=changed,
                        restart_required=False,
                        stdout=_join_output(stdout_parts),
                        stderr=_join_output(stderr_parts),
                        error=sync.stderr.strip() or sync.stdout.strip() or "uv sync failed",
                        uv_sync_run=True,
                    )
            return GitUpdateResult(
                ok=True,
                status="updated" if changed else "current",
                repo_root=str(repo_path),
                before_head=before_head,
                after_head=after_head,
                process_start_head=process_start_head,
                target_hash=target,
                changed=changed,
                restart_required=restart_required,
                stdout=_join_output(stdout_parts),
                stderr=_join_output(stderr_parts),
                uv_sync_run=uv_sync_run,
            )
    except GitUpdateError as exc:
        after_head = _safe_current_head(repo_path, before_head)
        return _result(
            False,
            "failed",
            repo_path,
            before_head,
            after_head,
            process_start_head,
            target,
            stdout_parts,
            stderr_parts,
            str(exc),
        )


def _result(
    ok: bool,
    status: str,
    repo_root: Path,
    before_head: str,
    after_head: str,
    process_start_head: str,
    target_hash: str,
    stdout_parts: list[str],
    stderr_parts: list[str],
    error: str,
) -> GitUpdateResult:
    restart_required = bool(after_head and after_head != process_start_head)
    return GitUpdateResult(
        ok=ok,
        status=status,
        repo_root=str(repo_root),
        before_head=before_head,
        after_head=after_head,
        process_start_head=process_start_head,
        target_hash=target_hash,
        changed=bool(before_head and after_head and before_head != after_head),
        restart_required=restart_required,
        stdout=_join_output(stdout_parts),
        stderr=_join_output(stderr_parts),
        error=error,
    )


def _safe_current_head(repo_root: Path, default: str = "") -> str:
    try:
        return current_head(repo_root)
    except GitUpdateError:
        return default


def _join_output(parts: list[str]) -> str:
    return "\n".join(part.strip() for part in parts if part and part.strip())


def _run_git(args: list[str], repo_root: Path, *, timeout: float = GIT_COMMAND_TIMEOUT_SECONDS) -> GitCommandResult:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), *args],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
    except OSError as exc:
        return GitCommandResult(127, stderr=str(exc))
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        detail = stderr or f"git {' '.join(args)} timed out after {timeout:g}s"
        return GitCommandResult(124, stdout=stdout, stderr=detail)
    return GitCommandResult(completed.returncode, completed.stdout, completed.stderr)


def _run_uv_sync(repo_root: Path, *, timeout: float = UV_SYNC_TIMEOUT_SECONDS) -> GitCommandResult:
    try:
        completed = subprocess.run(
            ["uv", "sync"],
            cwd=repo_root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
    except OSError as exc:
        return GitCommandResult(127, stderr=str(exc))
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        detail = stderr or f"uv sync timed out after {timeout:g}s"
        return GitCommandResult(124, stdout=stdout, stderr=detail)
    return GitCommandResult(completed.returncode, completed.stdout, completed.stderr)
