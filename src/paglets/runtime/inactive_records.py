# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from paglets.core.errors import (
    PagletError,
)
from paglets.core.runtime_values import (
    EnvelopeKind,
)
from paglets.persistence.persistency import DeactivationPolicy, DeactivationRequest, InactiveRecord, QueuedMessage
from paglets.remote.transport import json_safe

SHUTDOWN_DEACTIVATE_TIMEOUT_SECONDS = 0.5
MESH_SERVICE_LOOKUP_TIMEOUT_SECONDS = 1.0


class _InactiveRecordsMixin:
    def _load_inactive_records(self) -> None:
        if not self._inactive_dir.exists():
            return
        records: dict[str, InactiveRecord] = {}
        for path in sorted(self._inactive_dir.glob("*.json")):
            try:
                record = InactiveRecord.from_wire(json.loads(path.read_text(encoding="utf-8")))
            except Exception:
                continue
            records[record.agent_id] = record
        with self._lock:
            self._inactive.update(records)

    def _write_inactive_record(self, record: InactiveRecord) -> None:
        self._inactive_dir.mkdir(parents=True, exist_ok=True)
        path = self._inactive_path(record.agent_id)
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(json_safe(record.to_wire()), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp_path.replace(path)

    def _delete_inactive_record(self, agent_id: str) -> None:
        path = self._inactive_path(agent_id)
        try:
            path.unlink()
        except FileNotFoundError:
            return

    def _inactive_path(self, agent_id: str) -> Path:
        return self._inactive_dir / f"{agent_id}.json"

    def _activate_startup_records(self) -> None:
        with self._lock:
            startup_ids = [agent_id for agent_id, record in self._inactive.items() if record.policy.activate_on_startup]
        for agent_id in startup_ids:
            try:
                self.activate(agent_id)
            except PagletError:
                continue

    def _start_activation_scheduler(self) -> None:
        if self._activation_thread is not None and self._activation_thread.is_alive():
            return
        self._activation_thread = threading.Thread(
            target=self._activation_scheduler_loop,
            name=f"paglets-activation-{self.name}",
            daemon=True,
        )
        self._activation_thread.start()

    def _stop_activation_scheduler(self) -> None:
        self._activation_stop.set()
        thread = self._activation_thread
        self._activation_thread = None
        if thread is not None and thread.is_alive():
            thread.join(timeout=2)

    def _activation_scheduler_loop(self) -> None:
        while not self._activation_stop.wait(self._next_activation_delay()):
            now = time.time()
            self._resident_maintenance(now)
            with self._lock:
                due_ids = [
                    agent_id
                    for agent_id, record in self._inactive.items()
                    if record.policy.activate_at is not None and record.policy.activate_at <= now
                ]
            for agent_id in due_ids:
                try:
                    self.activate(agent_id)
                except PagletError:
                    continue

    def _next_activation_delay(self) -> float:
        with self._lock:
            activate_at_values = [
                record.policy.activate_at for record in self._inactive.values() if record.policy.activate_at is not None
            ]
        if not activate_at_values:
            return 1.0
        return max(0.05, min(1.0, min(activate_at_values) - time.time()))

    def _deactivate_active_for_shutdown(self) -> None:
        with self._lock:
            records = list(self._agents.items())
        for agent_id, record in records:
            with self._lock:
                if self._agents.get(agent_id) is not record:
                    continue
            request = DeactivationRequest(
                reason="shutdown",
                source="host",
                policy=self._resident_service_shutdown_policy(agent_id),
            )
            try:
                prepared = record.request(
                    "deactivate_prepare",
                    {"request": request.to_wire()},
                    timeout=SHUTDOWN_DEACTIVATE_TIMEOUT_SECONDS,
                )
                record._update_from_reply(prepared)
                policy = DeactivationPolicy.from_wire(prepared.get("policy"))
                info = {"name": self.name, "address": self.address}
                envelope = self._make_envelope(record, EnvelopeKind.ACTIVATION, info)
                inactive = InactiveRecord(envelope=envelope, policy=policy, request=request)
                self._write_inactive_record(inactive)
                with self._lock:
                    if self._agents.get(agent_id) is record:
                        self._inactive[agent_id] = inactive
                self._remove_active_agent(agent_id, expected=record, terminate=True)
                self._emit(
                    "deactivate",
                    agent_id=agent_id,
                    class_name=envelope.agent_class_name,
                    data={"reason": request.reason},
                )
            except Exception:
                continue

    def _terminate_active_children(self) -> None:
        with self._lock:
            records = list(self._agents.items())
            self._agents.clear()
            mailboxes = list(self._mailboxes.values())
            self._mailboxes.clear()
        for mailbox in mailboxes:
            mailbox.close()
        for agent_id, record in records:
            record.departing = True
            record.terminate(timeout=0.5, kill_timeout=0.5)
            for service in self._services.remove_agent(agent_id, keep=self._is_resident_service_record):
                self._emit("service-remove", agent_id=agent_id, service_name=service.name)

    def _drain_queued_messages(self, record: InactiveRecord) -> None:
        for index, queued in enumerate(record.queued_messages):
            if self.get_proxy(record.agent_id) is None:
                self._requeue_messages(record.agent_id, record.queued_messages[index:])
                return
            try:
                self.deliver_message(
                    record.agent_id,
                    queued.message,
                    oneway=queued.oneway,
                    activate_if_inactive=False,
                    no_delay=True,
                )
            except PagletError:
                continue

    def _requeue_messages(self, agent_id: str, messages: list[QueuedMessage]) -> None:
        if not messages:
            return
        with self._lock:
            record = self._inactive.get(agent_id)
        if record is None:
            return
        record.queued_messages.extend(messages)
        self._write_inactive_record(record)
