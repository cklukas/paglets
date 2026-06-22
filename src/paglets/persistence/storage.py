# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

from paglets.core.errors import PagletError


DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES = 10 * 1024 * 1024


class StorageQuotaError(PagletError):
    """Raised when a managed storage write would exceed its quota."""


@dataclass(frozen=True, slots=True)
class StorageStatus:
    root: str
    used_bytes: int
    quota_bytes: int | None
    available_bytes: int | None


class ManagedStorage:
    """Path-safe, quota-accounted storage rooted at one directory."""

    def __init__(self, root: Path | str, *, quota_bytes: int | None = DEFAULT_PERSISTENT_STORAGE_QUOTA_BYTES):
        self.root = Path(root).expanduser().resolve(strict=False)
        self.quota_bytes = None if quota_bytes is None else max(0, int(quota_bytes))

    def read_bytes(self, path: Path | str) -> bytes:
        return self._resolve(path).read_bytes()

    def write_bytes(self, path: Path | str, data: bytes) -> Path:
        payload = bytes(data)
        target = self._resolve(path)
        existing_size = target.stat().st_size if target.exists() and target.is_file() else 0
        projected = self._used_bytes() - existing_size + len(payload)
        if self.quota_bytes is not None and projected > self.quota_bytes:
            raise StorageQuotaError(
                f"managed storage quota exceeded: {projected} bytes would exceed {self.quota_bytes} bytes"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        return target

    def write_text(self, path: Path | str, text: str, *, encoding: str = "utf-8") -> Path:
        return self.write_bytes(path, text.encode(encoding))

    def delete(self, path: Path | str) -> None:
        target = self._resolve(path)
        if target.is_dir() and not target.is_symlink():
            shutil.rmtree(target)
            return
        try:
            target.unlink()
        except FileNotFoundError:
            return

    def clear(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def status(self) -> StorageStatus:
        used = self._used_bytes()
        return StorageStatus(
            root=str(self.root),
            used_bytes=used,
            quota_bytes=self.quota_bytes,
            available_bytes=None if self.quota_bytes is None else max(0, self.quota_bytes - used),
        )

    def _resolve(self, path: Path | str) -> Path:
        candidate = (self.root / Path(path)).resolve(strict=False)
        if candidate != self.root and self.root not in candidate.parents:
            raise ValueError(f"managed storage path escapes root: {path!r}")
        return candidate

    def _used_bytes(self) -> int:
        if not self.root.exists():
            return 0
        total = 0
        for path in self.root.rglob("*"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except OSError:
                continue
        return total
