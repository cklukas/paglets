# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from .agent import ACTIVE, INACTIVE
from .client import HostClient
from .errors import PagletError
from .messages import FUTURE, ONEWAY, FutureReply, Message
from .persistency import DeactivationPolicy, DeactivationRequest
from .references import PagletProxyRef
from .transfer import TransferTicket


def _agent_url(host_url: str, agent_id: str, suffix: str = "") -> str:
    base = f"{host_url.rstrip('/')}/agents/{agent_id}"
    return base + suffix


_EXECUTOR = ThreadPoolExecutor(thread_name_prefix="paglets-message")


@dataclass(frozen=True, slots=True)
class PagletProxy:
    """A controlled handle to a paglet, local or remote.

    Like Aglets' proxy, callers do not reach into the object directly; all
    control and messaging goes through the host API.
    """

    host_url: str
    agent_id: str
    client: HostClient

    def to_wire(self) -> dict[str, str]:
        return {"host_url": self.host_url, "agent_id": self.agent_id}

    def ref(self) -> PagletProxyRef:
        return PagletProxyRef.from_proxy(self)

    @classmethod
    def from_wire(cls, payload: dict[str, str], client: HostClient | None = None) -> "PagletProxy":
        return cls(
            host_url=payload["host_url"],
            agent_id=payload["agent_id"],
            client=client or HostClient(),
        )

    def info(self) -> dict[str, Any]:
        return self.client.get_json(_agent_url(self.host_url, self.agent_id))

    def is_valid(self) -> bool:
        try:
            self.info()
            return True
        except PagletError:
            return False

    def is_active(self) -> bool:
        try:
            return bool(self.info().get("active"))
        except PagletError:
            return False

    def is_state(self, state: int | bool) -> bool:
        if isinstance(state, bool):
            return self.is_active() is state
        try:
            active = bool(self.info().get("active"))
        except PagletError:
            return False
        return bool((state & ACTIVE and active) or (state & INACTIVE and not active))

    def is_remote(self, local_host_url: str | None = None) -> bool:
        if local_host_url is None:
            return True
        return self.host_url.rstrip("/") != local_host_url.rstrip("/")

    def get_address(self) -> str:
        return self.host_url

    def get_agent_id(self) -> str:
        return self.agent_id

    def get_agent_class_name(self) -> str:
        return str(self.info()["class_name"])

    def send(self, message: Message, *, activate_if_inactive: bool = True, no_delay: bool = False) -> Any:
        response = self.client.post_json(
            _agent_url(self.host_url, self.agent_id, "/messages"),
            {
                "message": message.to_wire(),
                "activate_if_inactive": activate_if_inactive,
                "no_delay": no_delay,
            },
        )
        return response.get("result")

    def send_oneway(
        self,
        message: Message,
        *,
        activate_if_inactive: bool = True,
        no_delay: bool = False,
    ) -> None:
        message.message_type = ONEWAY
        self.client.post_json(
            _agent_url(self.host_url, self.agent_id, "/messages"),
            {
                "message": message.to_wire(),
                "oneway": True,
                "activate_if_inactive": activate_if_inactive,
                "no_delay": no_delay,
            },
        )

    def send_future(
        self,
        message: Message,
        *,
        activate_if_inactive: bool = True,
        no_delay: bool = False,
    ) -> FutureReply:
        message.message_type = FUTURE
        return FutureReply(
            _EXECUTOR.submit(
                self.send,
                message,
                activate_if_inactive=activate_if_inactive,
                no_delay=no_delay,
            )
        )

    def dispatch(self, target: str | TransferTicket) -> "PagletProxy":
        ticket = TransferTicket.from_target(target)
        response = self.client.post_json(
            _agent_url(self.host_url, self.agent_id, "/dispatch"),
            {"ticket": ticket.to_wire()},
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def clone(self, target: str | TransferTicket | None = None) -> "PagletProxy":
        payload = {"target": None} if target is None else {"ticket": TransferTicket.from_target(target).to_wire()}
        response = self.client.post_json(
            _agent_url(self.host_url, self.agent_id, "/clone"),
            payload,
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def deactivate(
        self,
        *,
        reason: str = "deactivate",
        policy: DeactivationPolicy | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "PagletProxy":
        response = self.client.post_json(
            _agent_url(self.host_url, self.agent_id, "/deactivate"),
            {
                "request": DeactivationRequest(
                    reason=reason,
                    source="external",
                    policy=policy,
                    metadata=metadata or {},
                ).to_wire()
            },
        )
        return PagletProxy.from_wire(response["proxy"], self.client)

    def activate(self) -> "PagletProxy":
        response = self.client.post_json(_agent_url(self.host_url, self.agent_id, "/activate"), {})
        return PagletProxy.from_wire(response["proxy"], self.client)

    def dispose(self) -> None:
        self.client.post_json(_agent_url(self.host_url, self.agent_id, "/dispose"), {})
