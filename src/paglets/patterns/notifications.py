# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import time
from enum import Enum

from paglets.core.runtime_values import ServiceScope
from paglets.system.user_info import NOTIFY_USER, USER_INFO, UserInfoRequest


class NotificationSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class NotificationMixin:
    """Non-fatal user-info notifications for paglets."""

    def notify_user_info(
        self,
        severity: NotificationSeverity,
        title: str,
        message: str,
        *,
        job_id: str = "",
        timeout: float = 2.0,
        scope: ServiceScope = ServiceScope.MESH,
        metadata: dict[str, str] | None = None,
    ) -> bool:
        with contextlib.suppress(Exception):
            handle = self.require_contract(USER_INFO, operation=NOTIFY_USER, scope=scope)
            handle.call(
                NOTIFY_USER,
                UserInfoRequest(
                    severity=severity.value,
                    title=title,
                    message=message,
                    source_agent_id=getattr(self, "agent_id", ""),
                    job_id=job_id,
                    timestamp=time.time(),
                    metadata=dict(metadata or {}),
                ),
                timeout=max(0.0, float(timeout)),
            )
            return True
        return False
