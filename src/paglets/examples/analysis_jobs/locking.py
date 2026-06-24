# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import os
import tempfile
import threading
import time
from pathlib import Path

_LOCKS: dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


class FileLockTimeout(TimeoutError):
    """Raised when a cross-process file lock cannot be acquired in time."""


class CrossProcessFileLock:
    """Process-local plus best-effort OS file lock."""

    def __init__(self, path: str | Path, *, timeout: float = 30.0):
        self.path = Path(path).expanduser().resolve(strict=False)
        self.timeout = max(0.0, float(timeout))
        self.wait_seconds = 0.0
        self._file = None
        self._thread_lock: threading.Lock | None = None
        self._thread_acquired = False
        self._file_acquired = False

    def __enter__(self) -> CrossProcessFileLock:
        started = time.perf_counter()
        deadline = started + self.timeout
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with _LOCKS_GUARD:
            key = str(self.path)
            self._thread_lock = _LOCKS.setdefault(key, threading.Lock())
        if not self._thread_lock.acquire(timeout=self.timeout):
            self.wait_seconds = time.perf_counter() - started
            raise FileLockTimeout(f"file lock busy after {self.wait_seconds:.3f}s: {self.path}")
        self._thread_acquired = True
        self._file = self.path.open("a+b")
        self._file.seek(0, os.SEEK_END)
        if self._file.tell() == 0:
            self._file.write(b"\0")
            self._file.flush()
        while True:
            if _try_lock_file(self._file):
                self._file_acquired = True
                self.wait_seconds = time.perf_counter() - started
                return self
            if time.perf_counter() >= deadline:
                self.wait_seconds = time.perf_counter() - started
                self.__exit__(None, None, None)
                raise FileLockTimeout(f"file lock busy after {self.wait_seconds:.3f}s: {self.path}")
            time.sleep(0.05)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is not None:
            if self._file_acquired:
                _unlock_file(self._file)
                self._file_acquired = False
            self._file.close()
            self._file = None
        if self._thread_acquired and self._thread_lock is not None:
            self._thread_lock.release()
            self._thread_acquired = False


def default_lock_path(db_path: str | Path) -> Path:
    if str(db_path):
        return Path(f"{Path(db_path).expanduser()}.paglets.lock")
    return Path(tempfile.gettempdir()) / "paglets-analysis.sqlite.paglets.lock"


def _try_lock_file(handle) -> bool:
    if os.name == "nt":  # pragma: no cover - exercised on Windows only
        try:
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    try:
        import fcntl
    except ImportError:
        return True
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        return False


def _unlock_file(handle) -> None:
    if os.name == "nt":  # pragma: no cover - exercised on Windows only
        try:
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        return
    try:
        import fcntl
    except ImportError:
        return
    with contextlib.suppress(OSError):
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
