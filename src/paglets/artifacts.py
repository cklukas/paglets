# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import hashlib
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paglets.core.errors import TransferError
from paglets.remote.transport import ChunkedRequestReader, LimitedRequestReader

ARTIFACT_COPY = "copy"
ARTIFACT_MOVE = "move"
ARTIFACT_STATUS_REGISTERED = "registered"
ARTIFACT_STATUS_AVAILABLE = "available"
ARTIFACT_STATUS_FAILED = "failed"
DEFAULT_ARTIFACT_MAX_BYTES = 1024**3
DEFAULT_ARTIFACT_STORAGE_QUOTA_BYTES = 10 * 1024**3
DEFAULT_ARTIFACT_SPOOL_TTL_SECONDS = 24 * 60 * 60
STREAM_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    host_url: str
    artifact_id: str
    name: str = ""
    size_bytes: int = 0
    sha256: str = ""
    compression: str = ""
    created_at: float = 0.0
    expires_at: float = 0.0
    owner_agent_id: str = ""

    def to_wire(self) -> dict[str, Any]:
        return {
            "host_url": self.host_url,
            "artifact_id": self.artifact_id,
            "name": self.name,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "compression": self.compression,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "owner_agent_id": self.owner_agent_id,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> ArtifactRef:
        return cls(
            host_url=str(payload.get("host_url") or ""),
            artifact_id=str(payload["artifact_id"]),
            name=str(payload.get("name") or ""),
            size_bytes=int(payload.get("size_bytes") or 0),
            sha256=str(payload.get("sha256") or ""),
            compression=str(payload.get("compression") or ""),
            created_at=float(payload.get("created_at") or 0.0),
            expires_at=float(payload.get("expires_at") or 0.0),
            owner_agent_id=str(payload.get("owner_agent_id") or ""),
        )


@dataclass(slots=True)
class PagletFileRef:
    name: str
    mode: str = ARTIFACT_COPY
    source_host_name: str = ""
    source_host_url: str = ""
    source_path: str = ""
    source_created_at: float = 0.0
    source_modified_at: float = 0.0
    size_bytes: int = 0
    sha256: str = ""
    current_host_name: str = ""
    current_host_url: str = ""
    current_path: str = ""
    status: str = ARTIFACT_STATUS_REGISTERED
    last_error: str = ""

    def to_wire(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "mode": self.mode,
            "source_host_name": self.source_host_name,
            "source_host_url": self.source_host_url,
            "source_path": self.source_path,
            "source_created_at": self.source_created_at,
            "source_modified_at": self.source_modified_at,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "current_host_name": self.current_host_name,
            "current_host_url": self.current_host_url,
            "current_path": self.current_path,
            "status": self.status,
            "last_error": self.last_error,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> PagletFileRef:
        return cls(
            name=str(payload["name"]),
            mode=_normalize_file_mode(str(payload.get("mode") or ARTIFACT_COPY)),
            source_host_name=str(payload.get("source_host_name") or ""),
            source_host_url=str(payload.get("source_host_url") or ""),
            source_path=str(payload.get("source_path") or ""),
            source_created_at=float(payload.get("source_created_at") or 0.0),
            source_modified_at=float(payload.get("source_modified_at") or 0.0),
            size_bytes=int(payload.get("size_bytes") or 0),
            sha256=str(payload.get("sha256") or ""),
            current_host_name=str(payload.get("current_host_name") or ""),
            current_host_url=str(payload.get("current_host_url") or ""),
            current_path=str(payload.get("current_path") or ""),
            status=str(payload.get("status") or ARTIFACT_STATUS_REGISTERED),
            last_error=str(payload.get("last_error") or ""),
        )


@dataclass(frozen=True, slots=True)
class ArtifactWriteResult:
    ref: ArtifactRef
    path: Path


class ArtifactStore:
    """Host-owned binary artifact storage with atomic temp-file cleanup."""

    def __init__(
        self,
        root: str | Path,
        *,
        host_url: str,
        max_artifact_bytes: int | None = DEFAULT_ARTIFACT_MAX_BYTES,
        quota_bytes: int | None = DEFAULT_ARTIFACT_STORAGE_QUOTA_BYTES,
        spool_ttl_seconds: float = DEFAULT_ARTIFACT_SPOOL_TTL_SECONDS,
    ):
        self.root = Path(root).expanduser().resolve(strict=False)
        self.host_url = host_url.rstrip("/")
        self.max_artifact_bytes = 0 if max_artifact_bytes is None else max(0, int(max_artifact_bytes))
        self.quota_bytes = None if quota_bytes is None else max(0, int(quota_bytes))
        self.spool_ttl_seconds = max(1.0, float(spool_ttl_seconds))
        self._blobs = self.root / "blobs"
        self._meta = self.root / "metadata"
        self._tmp = self.root / "tmp"
        self._ensure_dirs()

    def set_host_url(self, host_url: str) -> None:
        self.host_url = host_url.rstrip("/")

    def cleanup_temporary(self, *, now: float | None = None) -> None:
        current = time.time() if now is None else now
        self._ensure_dirs()
        for path in self._tmp.glob("*.part"):
            with contextlib.suppress(OSError):
                if current - path.stat().st_mtime >= self.spool_ttl_seconds:
                    path.unlink()
        for ref in self.list():
            if ref.expires_at > 0 and ref.expires_at <= current:
                self.delete(ref.artifact_id)

    def create_from_path(
        self,
        source: str | Path,
        *,
        owner_agent_id: str = "",
        name: str | None = None,
        compression: str = "",
        expires_at: float = 0.0,
        expected_sha256: str | None = None,
    ) -> ArtifactWriteResult:
        path = Path(source)
        if not path.is_file():
            raise TransferError(f"artifact source is not a file: {path}")
        size = path.stat().st_size
        with path.open("rb") as handle:
            return self.create_from_stream(
                handle,
                size_bytes=size,
                owner_agent_id=owner_agent_id,
                name=name or path.name,
                compression=compression,
                expires_at=expires_at,
                expected_sha256=expected_sha256,
            )

    def create_from_http_request(
        self,
        headers: Any,
        source: Any,
        *,
        owner_agent_id: str = "",
        name: str = "",
        compression: str = "",
        expires_at: float = 0.0,
        expected_sha256: str | None = None,
    ) -> ArtifactWriteResult:
        transfer_encoding = str(headers.get("Transfer-Encoding") or "").casefold()
        length = int(headers.get("Content-Length") or 0)
        stream = (
            ChunkedRequestReader(source) if "chunked" in transfer_encoding else LimitedRequestReader(source, length)
        )
        size = -1 if "chunked" in transfer_encoding else length
        return self.create_from_stream(
            stream,
            size_bytes=size,
            owner_agent_id=owner_agent_id,
            name=name,
            compression=compression,
            expires_at=expires_at,
            expected_sha256=expected_sha256,
        )

    def create_from_stream(
        self,
        source: Any,
        *,
        size_bytes: int = -1,
        owner_agent_id: str = "",
        name: str = "",
        compression: str = "",
        expires_at: float = 0.0,
        expected_sha256: str | None = None,
    ) -> ArtifactWriteResult:
        self._ensure_dirs()
        artifact_id = uuid.uuid4().hex
        tmp_path = self._tmp / f"{artifact_id}.part"
        blob_path = self._blob_path(artifact_id)
        meta_path = self._metadata_path(artifact_id)
        digest = hashlib.sha256()
        written = 0
        try:
            self._check_declared_size(size_bytes)
            with tmp_path.open("wb") as target:
                while True:
                    chunk = source.read(STREAM_CHUNK_BYTES)
                    if not chunk:
                        break
                    data = bytes(chunk)
                    written += len(data)
                    self._check_declared_size(written)
                    digest.update(data)
                    target.write(data)
            sha256 = digest.hexdigest()
            if expected_sha256 and sha256.casefold() != expected_sha256.casefold():
                raise TransferError(f"artifact checksum mismatch: expected {expected_sha256}, got {sha256}")
            self._reserve_quota(written)
            blob_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp_path, blob_path)
            created_at = time.time()
            ref = ArtifactRef(
                host_url=self.host_url,
                artifact_id=artifact_id,
                name=name,
                size_bytes=written,
                sha256=sha256,
                compression=compression,
                created_at=created_at,
                expires_at=float(expires_at or 0.0),
                owner_agent_id=owner_agent_id,
            )
            meta_path.write_text(_metadata_json(ref), encoding="utf-8")
            return ArtifactWriteResult(ref=ref, path=blob_path)
        except Exception:
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()
            with contextlib.suppress(FileNotFoundError):
                blob_path.unlink()
            with contextlib.suppress(FileNotFoundError):
                meta_path.unlink()
            raise

    def ref(self, artifact_id: str) -> ArtifactRef:
        path = self._metadata_path(artifact_id)
        if not path.exists():
            raise TransferError(f"No artifact {artifact_id!r}")
        import json

        payload = json.loads(path.read_text(encoding="utf-8"))
        ref = ArtifactRef.from_wire(payload)
        if ref.host_url != self.host_url:
            ref = ArtifactRef(
                host_url=self.host_url,
                artifact_id=ref.artifact_id,
                name=ref.name,
                size_bytes=ref.size_bytes,
                sha256=ref.sha256,
                compression=ref.compression,
                created_at=ref.created_at,
                expires_at=ref.expires_at,
                owner_agent_id=ref.owner_agent_id,
            )
        return ref

    def list(self, *, owner_agent_id: str | None = None) -> list[ArtifactRef]:
        self._ensure_dirs()
        refs: list[ArtifactRef] = []
        for path in sorted(self._meta.glob("*.json")):
            try:
                ref = self.ref(path.stem)
            except Exception:
                continue
            if owner_agent_id is not None and ref.owner_agent_id != owner_agent_id:
                continue
            refs.append(ref)
        return refs

    def open_reader(self, artifact_id: str):
        self.ref(artifact_id)
        return self._blob_path(artifact_id).open("rb")

    def blob_path(self, artifact_id: str) -> Path:
        self.ref(artifact_id)
        return self._blob_path(artifact_id)

    def export_to_path(
        self,
        artifact_id: str,
        target: str | Path,
        *,
        expected_sha256: str | None = None,
    ) -> ArtifactRef:
        ref = self.ref(artifact_id)
        target_path = Path(target)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_name(f".{target_path.name}.{uuid.uuid4().hex}.part")
        digest = hashlib.sha256()
        written = 0
        try:
            with self.open_reader(artifact_id) as source, tmp_path.open("wb") as output:
                while True:
                    chunk = source.read(STREAM_CHUNK_BYTES)
                    if not chunk:
                        break
                    data = bytes(chunk)
                    digest.update(data)
                    written += len(data)
                    output.write(data)
            sha256 = digest.hexdigest()
            if written != ref.size_bytes:
                raise TransferError(f"artifact size mismatch: expected {ref.size_bytes}, got {written}")
            expected = expected_sha256 or ref.sha256
            if expected and sha256.casefold() != expected.casefold():
                raise TransferError(f"artifact checksum mismatch: expected {expected}, got {sha256}")
            os.replace(tmp_path, target_path)
            return ref
        except Exception:
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()
            raise

    def delete(self, artifact_id: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            self._blob_path(artifact_id).unlink()
        with contextlib.suppress(FileNotFoundError):
            self._metadata_path(artifact_id).unlink()

    def delete_owner(self, owner_agent_id: str) -> None:
        for ref in self.list(owner_agent_id=owner_agent_id):
            self.delete(ref.artifact_id)

    def _ensure_dirs(self) -> None:
        self._blobs.mkdir(parents=True, exist_ok=True)
        self._meta.mkdir(parents=True, exist_ok=True)
        self._tmp.mkdir(parents=True, exist_ok=True)

    def _check_declared_size(self, size_bytes: int) -> None:
        if self.max_artifact_bytes and size_bytes > self.max_artifact_bytes:
            raise TransferError(
                f"artifact exceeds maximum size: {size_bytes} bytes would exceed {self.max_artifact_bytes} bytes"
            )

    def _reserve_quota(self, incoming_size: int) -> None:
        if self.quota_bytes is None:
            return
        used = 0
        for path in self._blobs.glob("*.bin"):
            with contextlib.suppress(OSError):
                used += path.stat().st_size
        projected = used + max(0, incoming_size)
        if projected > self.quota_bytes:
            raise TransferError(f"artifact storage quota exceeded: {projected} bytes would exceed {self.quota_bytes}")

    def _blob_path(self, artifact_id: str) -> Path:
        return self._blobs / f"{_safe_artifact_id(artifact_id)}.bin"

    def _metadata_path(self, artifact_id: str) -> Path:
        return self._meta / f"{_safe_artifact_id(artifact_id)}.json"


def paglet_file_ref_from_path(
    path: str | Path,
    *,
    name: str | None,
    mode: str,
    host_name: str,
    host_url: str,
) -> PagletFileRef:
    source = Path(path).expanduser().resolve(strict=False)
    if not source.is_file():
        raise TransferError(f"registered path is not a file: {source}")
    stat = source.stat()
    digest = file_sha256(source)
    logical_name = name or source.name
    return PagletFileRef(
        name=_validate_file_name(logical_name),
        mode=_normalize_file_mode(mode),
        source_host_name=host_name,
        source_host_url=host_url.rstrip("/"),
        source_path=str(source),
        source_created_at=getattr(stat, "st_birthtime", stat.st_ctime),
        source_modified_at=stat.st_mtime,
        size_bytes=stat.st_size,
        sha256=digest,
        current_host_name=host_name,
        current_host_url=host_url.rstrip("/"),
        current_path=str(source),
        status=ARTIFACT_STATUS_REGISTERED,
        last_error="",
    )


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(STREAM_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def safe_target_filename(name: str) -> str:
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in name).strip("._") or "artifact"


def _validate_file_name(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        raise TransferError("registered file name cannot be empty")
    return text


def _normalize_file_mode(mode: str) -> str:
    value = str(mode or ARTIFACT_COPY).strip().casefold()
    if value not in {ARTIFACT_COPY, ARTIFACT_MOVE}:
        raise TransferError("registered file mode must be 'copy' or 'move'")
    return value


def _safe_artifact_id(value: str) -> str:
    text = str(value or "").strip()
    if not text or any(char not in "0123456789abcdefABCDEF" for char in text):
        raise TransferError(f"invalid artifact id {value!r}")
    return text


def _metadata_json(ref: ArtifactRef) -> str:
    import json

    return json.dumps(ref.to_wire(), indent=2, sort_keys=True) + "\n"


def copy_stream(source: Any, target: Any, *, expected_bytes: int | None = None) -> tuple[int, str]:
    digest = hashlib.sha256()
    written = 0
    while True:
        chunk = source.read(STREAM_CHUNK_BYTES)
        if not chunk:
            break
        data = bytes(chunk)
        target.write(data)
        digest.update(data)
        written += len(data)
    if expected_bytes is not None and written != expected_bytes:
        raise TransferError(f"artifact size mismatch: expected {expected_bytes}, got {written}")
    return written, digest.hexdigest()


def copy_path(source: str | Path, target: str | Path) -> None:
    target_path = Path(target)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_name(f".{target_path.name}.{uuid.uuid4().hex}.part")
    try:
        shutil.copyfile(source, tmp_path)
        os.replace(tmp_path, target_path)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise
