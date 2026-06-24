# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import os
import shutil
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, cast

from paglets.remote.proxy import PagletProxy

from .notifications import NotificationMixin, NotificationSeverity
from .tasks import TaskPaglet, TaskState, TaskStatus


class FileTransferMode(Enum):
    COPY = "copy"
    MOVE = "move"


@dataclass(frozen=True, slots=True)
class FileTransferRequest:
    source_path: str
    destination_path: str = ""
    target_host: str = ""
    mode: FileTransferMode = FileTransferMode.COPY
    dry_run: bool = False
    overwrite: bool = False
    transfer_id: str = ""
    source_label: str = ""
    destination_label: str = ""


@dataclass(frozen=True, slots=True)
class FileTransferEndpoint:
    host_name: str = ""
    host_url: str = ""
    path: str = ""
    created_at: float = 0.0
    modified_at: float = 0.0


@dataclass(frozen=True, slots=True)
class FileTransferResult:
    transfer_id: str = ""
    mode: FileTransferMode = FileTransferMode.COPY
    dry_run: bool = False
    source: FileTransferEndpoint = field(default_factory=FileTransferEndpoint)
    destination: FileTransferEndpoint = field(default_factory=FileTransferEndpoint)
    file_name: str = ""
    size_bytes: int = 0
    size: str = ""


@dataclass(frozen=True, slots=True)
class FileTransferSource:
    path: Path
    file_name: str
    size_bytes: int
    created_at: float
    modified_at: float


@dataclass(frozen=True, slots=True)
class FileTransferPlan:
    request: FileTransferRequest
    source: FileTransferSource
    destination_path: str
    target_host: str


@dataclass(frozen=True, slots=True)
class FileTransferArrival:
    scratch_path: Path
    destination_path: str
    source_name: str
    overwrite: bool
    mode: FileTransferMode


@dataclass
class FileTransferState(TaskState):
    transfer_id: str = ""
    mode: FileTransferMode = FileTransferMode.COPY
    dry_run: bool = False
    overwrite: bool = False
    source_host_name: str = ""
    source_host_url: str = ""
    destination_host_name: str = ""
    destination_host_url: str = ""
    source_path: str = ""
    destination_path: str = ""
    final_path: str = ""
    file_name: str = ""
    size_bytes: int = 0
    source_created_at: float = 0.0
    source_modified_at: float = 0.0


class FileMobilityMixin(NotificationMixin):
    """Reusable helpers for paglets that move one registered file naturally."""

    registered_file_name: ClassVar[str] = "file"
    notification_title: ClassVar[str] = "File transfer"

    def require_transfer_target(self, request: FileTransferRequest) -> str:
        if not request.target_host:
            raise ValueError("target_host is required")
        return request.target_host

    def stat_transfer_source(self, source_path: str | Path) -> FileTransferSource:
        source = Path(source_path).expanduser()
        if not source.is_file():
            raise ValueError(f"source is not a file: {source}")
        stat = source.stat()
        return FileTransferSource(
            path=source,
            file_name=source.name,
            size_bytes=int(stat.st_size),
            created_at=float(getattr(stat, "st_birthtime", stat.st_ctime)),
            modified_at=float(stat.st_mtime),
        )

    def prepare_file_transfer(self, request: FileTransferRequest) -> FileTransferPlan:
        target_host = self.require_transfer_target(request)
        source = self.stat_transfer_source(request.source_path)
        destination = self.remember_transfer_request(request, source)
        return FileTransferPlan(
            request=request,
            source=source,
            destination_path=destination,
            target_host=target_host,
        )

    def remember_transfer_request(self, request: FileTransferRequest, source: FileTransferSource) -> str:
        self.require_transfer_target(request)
        transfer_id = request.transfer_id or f"file-transfer-{uuid.uuid4().hex}"
        destination = self.plan_destination_path(request.destination_path, source.file_name)
        with cast(Any, self).locked_state() as state:
            state.transfer_id = transfer_id
            state.mode = request.mode
            state.dry_run = bool(request.dry_run)
            state.overwrite = bool(request.overwrite)
            state.source_host_name = cast(Any, self).context.name
            state.source_host_url = cast(Any, self).context.address
            state.destination_host_name = request.destination_label
            state.destination_host_url = request.target_host
            state.source_path = str(source.path.resolve(strict=False))
            state.destination_path = request.destination_path
            state.final_path = destination
            state.file_name = source.file_name
            state.size_bytes = source.size_bytes
            state.source_created_at = source.created_at
            state.source_modified_at = source.modified_at
        return destination

    def remember_transfer_arrival(self, destination: Path) -> str:
        final_path = str(destination.resolve(strict=False))
        with cast(Any, self).locked_state() as state:
            state.destination_host_name = cast(Any, self).context.name
            state.destination_host_url = cast(Any, self).context.address
            state.final_path = final_path
        return final_path

    def register_transfer_file(self, source: FileTransferSource, mode: FileTransferMode) -> None:
        cast(Any, self).register_file(source.path, name=self.registered_file_name, mode=mode.value)

    def register_planned_file(self, plan: FileTransferPlan) -> None:
        self.register_transfer_file(plan.source, plan.request.mode)

    def mark_waiting_for_arrival(self) -> None:
        cast(Any, self).set_task_status(TaskStatus.WAITING_FOR_ARRIVAL, done=False)

    def build_transfer_result(self, *, destination_path: str, dry_run: bool) -> FileTransferResult:
        with cast(Any, self).locked_state() as state:
            return FileTransferResult(
                transfer_id=state.transfer_id,
                mode=state.mode,
                dry_run=dry_run,
                source=FileTransferEndpoint(
                    host_name=state.source_host_name,
                    host_url=state.source_host_url,
                    path=state.source_path,
                    created_at=state.source_created_at,
                    modified_at=state.source_modified_at,
                ),
                destination=FileTransferEndpoint(
                    host_name=state.destination_host_name,
                    host_url=state.destination_host_url,
                    path=destination_path,
                ),
                file_name=state.file_name,
                size_bytes=state.size_bytes,
                size=format_bytes(state.size_bytes),
            )

    def plan_destination_path(self, destination: str, source_name: str) -> str:
        return _planned_destination_text(destination, source_name)

    def resolve_destination_path(self, destination: str, source_name: str, *, check_existing_dir: bool = True) -> Path:
        return _destination_path(destination, source_name, check_existing_dir=check_existing_dir)

    def atomic_copy_file(self, source: Path, destination: Path, *, overwrite: bool) -> Path:
        if destination.exists() and not overwrite:
            raise FileExistsError(f"destination already exists: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
        try:
            shutil.copy2(source, tmp_path)
            os.replace(tmp_path, destination)
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()
        return destination.resolve(strict=False)

    def save_registered_file_to_destination(self, *, destination: str, source_name: str, overwrite: bool) -> Path:
        scratch_path = cast(Any, self).file_path(self.registered_file_name)
        final_destination = self.resolve_destination_path(destination, source_name, check_existing_dir=True)
        return self.atomic_copy_file(scratch_path, final_destination, overwrite=overwrite)

    def current_transfer_arrival(self) -> FileTransferArrival | None:
        with cast(Any, self).locked_state() as state:
            if state.status is not TaskStatus.WAITING_FOR_ARRIVAL:
                return None
            source_name = state.file_name
            destination_path = state.destination_path
            overwrite = bool(state.overwrite)
            mode = state.mode
        return FileTransferArrival(
            scratch_path=cast(Any, self).file_path(self.registered_file_name),
            destination_path=destination_path,
            source_name=source_name,
            overwrite=overwrite,
            mode=mode,
        )

    def save_arrived_file(self, arrival: FileTransferArrival) -> Path:
        destination = self.resolve_destination_path(arrival.destination_path, arrival.source_name)
        return self.atomic_copy_file(arrival.scratch_path, destination, overwrite=arrival.overwrite)

    def notify_transfer_info(self, title: str, message: str) -> bool:
        return self.notify_user_info(
            NotificationSeverity.INFO,
            f"{self.notification_title} {title}",
            message,
            job_id=cast(Any, self).state.transfer_id,
        )

    def notify_transfer_error(self, title: str, message: str) -> bool:
        return self.notify_user_info(
            NotificationSeverity.ERROR,
            f"{self.notification_title} {title}",
            message,
            job_id=cast(Any, self).state.transfer_id,
        )


class SingleFileTransferPaglet(
    FileMobilityMixin,
    TaskPaglet[FileTransferRequest, FileTransferResult, FileTransferState],
):
    """Convenience task paglet that moves one registered file to a destination host."""

    Request = FileTransferRequest
    Result = FileTransferResult
    State = FileTransferState

    def run_task(self, request: FileTransferRequest) -> FileTransferResult | PagletProxy | None:
        plan = self.prepare_file_transfer(request)
        self._notify_info(
            "found file",
            f"Found {plan.source.path} ({format_bytes(plan.source.size_bytes)}); "
            f"destination {request.destination_label or request.target_host}:{plan.destination_path}",
        )
        if request.dry_run:
            result = self.build_transfer_result(destination_path=plan.destination_path, dry_run=True)
            self.complete_task(result)
            self._notify_info(
                "dry run",
                f"Would {request.mode.value} {plan.source.path} to "
                f"{request.destination_label or request.target_host}:{plan.destination_path}",
            )
            return None
        self.register_planned_file(plan)
        self.mark_waiting_for_arrival()
        self._notify_info(
            "dispatching",
            f"Dispatching to {request.destination_label or request.target_host} "
            f"with registered file {plan.source.file_name}",
        )
        return self.dispatch(plan.target_host)

    def on_arrival(self, event) -> None:
        _ = event
        arrival = self.current_transfer_arrival()
        if arrival is None:
            return
        try:
            destination = self.save_arrived_file(arrival)
        except Exception as exc:
            self.fail_task(exc)
            self._notify_error("failed", str(exc))
            raise
        final_path = self.remember_transfer_arrival(destination)
        result = self.build_transfer_result(destination_path=final_path, dry_run=False)
        self.complete_task(result)
        self._notify_info(
            "saved file",
            f"Saved {format_bytes(self.state.size_bytes)} to "
            f"{self.context.name}:{self.state.final_path} ({arrival.mode.value})",
        )

    def _notify_info(self, title: str, message: str) -> bool:
        return self.notify_transfer_info(title, message)

    def _notify_error(self, title: str, message: str) -> bool:
        return self.notify_transfer_error(title, message)


def _planned_destination_text(destination: str, source_name: str) -> str:
    text = (destination or source_name).strip()
    if not text or text == ".":
        return source_name
    if text.endswith(("/", "\\")):
        return f"{text}{source_name}"
    return text


def _destination_path(destination: str, source_name: str, *, check_existing_dir: bool) -> Path:
    text = (destination or source_name).strip()
    if not text or text == ".":
        return Path(source_name).expanduser()
    if text.endswith(("/", "\\")):
        return (Path(text).expanduser() / source_name).resolve(strict=False)
    path = Path(text).expanduser()
    if check_existing_dir and path.exists() and path.is_dir():
        return (path / source_name).resolve(strict=False)
    return path.resolve(strict=False)


def format_bytes(size_bytes: int) -> str:
    size = float(max(0, int(size_bytes)))
    units = ("B", "KB", "MB", "GB", "TB")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} B"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{int(size_bytes)} B"
