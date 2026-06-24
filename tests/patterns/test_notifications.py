# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass

from paglets.core.agent import Paglet, PagletState
from paglets.patterns.notifications import NotificationMixin, NotificationSeverity


@dataclass
class NoticeState(PagletState):
    pass


class NoticePaglet(NotificationMixin, Paglet[NoticeState]):
    State = NoticeState


def test_user_info_notification_is_non_fatal_when_no_context_is_attached():
    paglet = NoticePaglet(NoticeState())

    assert paglet.notify_user_info(NotificationSeverity.INFO, "title", "message") is False
