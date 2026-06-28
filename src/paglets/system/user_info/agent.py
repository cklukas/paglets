# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field

from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message
from paglets.core.runtime_values import ResidentLifecycle, ServiceScope
from paglets.services.contracts import ServiceContract, ServiceOperation
from paglets.services.resident import ResidentServiceSpec


@dataclass(frozen=True, slots=True)
class UserInfoRequest:
    severity: str = "info"
    title: str = ""
    message: str = ""
    source_agent_id: str = ""
    job_id: str = ""
    timestamp: float = 0.0
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class UserInfoReply:
    ok: bool = True


@dataclass(frozen=True, slots=True)
class UserInfoStreamRequest:
    stream_id: str = ""
    text: str = ""
    target: str = "stdout"
    flush: bool = True


NOTIFY_USER = ServiceOperation("notify", UserInfoRequest, UserInfoReply)
STREAM_USER = ServiceOperation("stream", UserInfoStreamRequest, UserInfoReply)
PI_OUTPUT_USER = ServiceOperation("pi.output", UserInfoStreamRequest, UserInfoReply)
PI_PROGRESS_USER = ServiceOperation("pi.progress", UserInfoStreamRequest, UserInfoReply)
PI_DONE_USER = ServiceOperation("pi.done", UserInfoRequest, UserInfoReply)
PI_FAILED_USER = ServiceOperation("pi.failed", UserInfoRequest, UserInfoReply)

USER_INFO = ServiceContract(
    "user-info",
    operations=(NOTIFY_USER, STREAM_USER, PI_OUTPUT_USER, PI_PROGRESS_USER, PI_DONE_USER, PI_FAILED_USER),
    version="1",
)


@dataclass
class UserInfoState(PagletState):
    service_scope: ServiceScope = ServiceScope.MESH


class UserInfoAgent(Paglet[UserInfoState]):
    """Resident user-facing notification service.

    The first implementation prints to the host console. Future implementations
    can replace this service with desktop notifications or other sinks without
    changing paglets that call the service contract.
    """

    State = UserInfoState
    RESIDENT_SERVICES = (
        ResidentServiceSpec(
            contract=USER_INFO,
            scope=ServiceScope.MESH,
            lifecycle=ResidentLifecycle.LAZY,
            idle_timeout=30.0,
            agent_id="service.user-info",
            singleton=True,
            state={"service_scope": ServiceScope.MESH.value},
        ),
    )

    def on_creation(self, event):
        self.advertise_contract(USER_INFO, scope=self.state.service_scope)

    def on_activation(self, event):
        self.advertise_contract(USER_INFO, scope=self.state.service_scope)

    def handle_message(self, message: Message):
        return USER_INFO.route(
            message,
            {
                NOTIFY_USER: self.notify,
                STREAM_USER: self.stream,
                PI_OUTPUT_USER: self.stream,
                PI_PROGRESS_USER: self.stream,
                PI_DONE_USER: self.notify,
                PI_FAILED_USER: self.notify,
            },
            default=self.not_handled(),
        )

    def notify(self, request: UserInfoRequest) -> UserInfoReply:
        timestamp = request.timestamp or time.time()
        severity = (request.severity or "info").upper()
        job = f" job={request.job_id}" if request.job_id else ""
        source = f" source={request.source_agent_id}" if request.source_agent_id else ""
        title = request.title or "paglets"
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}] "
            f"{severity}: {title}{job}{source}: {request.message}",
            file=sys.stderr,
            flush=True,
        )
        return UserInfoReply(ok=True)

    def stream(self, request: UserInfoStreamRequest) -> UserInfoReply:
        handle = sys.stderr if request.target == "stderr" else sys.stdout
        handle.write(request.text)
        if request.flush:
            handle.flush()
        return UserInfoReply(ok=True)
