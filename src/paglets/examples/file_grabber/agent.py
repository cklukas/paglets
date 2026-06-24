# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass

from paglets.patterns.file_mobility import (
    FileMobilityMixin,
    FileTransferMode,
    FileTransferRequest,
    FileTransferResult,
    FileTransferState,
    format_bytes,
)
from paglets.patterns.tasks import TaskPaglet, TaskStatus
from paglets.remote.proxy import PagletProxy

FileGrabMode = FileTransferMode
FileGrabRequest = FileTransferRequest
FileGrabResult = FileTransferResult


@dataclass
class FileGrabberState(FileTransferState):
    """State for the file grabber example."""


class FileGrabberPaglet(
    FileMobilityMixin,
    TaskPaglet[FileTransferRequest, FileTransferResult, FileGrabberState],
):
    """Small example paglet: register one file, dispatch, and save it on arrival."""

    State = FileGrabberState
    Request = FileTransferRequest
    Result = FileTransferResult
    registered_file_name = "grabbed-file"
    notification_title = "File grabber"

    def run_task(self, request: FileTransferRequest) -> FileTransferResult | PagletProxy | None:
        plan = self.prepare_file_transfer(request)

        self.notify_transfer_info(
            "found file",
            f"Found {plan.source.path} ({format_bytes(plan.source.size_bytes)}); "
            f"destination {request.destination_label or request.target_host}:{plan.destination_path}",
        )

        if request.dry_run:
            result = self.build_transfer_result(destination_path=plan.destination_path, dry_run=True)
            self.complete_task(result)
            self.notify_transfer_info(
                "dry run",
                f"Would {request.mode.value} {plan.source.path} to "
                f"{request.destination_label or request.target_host}:{plan.destination_path}",
            )
            return None

        self.register_planned_file(plan)
        self.mark_waiting_for_arrival()
        self.notify_transfer_info(
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
            self.notify_transfer_error("failed", str(exc))
            raise

        final_path = self.remember_transfer_arrival(destination)
        result = self.build_transfer_result(destination_path=final_path, dry_run=False)
        self.complete_task(result)
        self.notify_transfer_info(
            "saved file",
            f"Saved {format_bytes(self.state.size_bytes)} to "
            f"{self.context.name}:{self.state.final_path} ({arrival.mode.value})",
        )


__all__ = [
    "FileGrabMode",
    "FileGrabRequest",
    "FileGrabResult",
    "FileGrabberPaglet",
    "FileGrabberState",
    "TaskStatus",
    "format_bytes",
]
