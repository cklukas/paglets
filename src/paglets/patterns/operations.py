# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, TypeVar

from paglets.core.agent import Paglet, PagletState
from paglets.core.errors import ServiceContractError
from paglets.core.messages import Message
from paglets.remote.proxy import PagletProxy
from paglets.services.contracts import ServiceOperation

StateT = TypeVar("StateT", bound=PagletState)
ReqT = TypeVar("ReqT")
RepT = TypeVar("RepT")


class OperationPaglet(Paglet[StateT], Generic[StateT]):
    """Paglet base class for typed multi-operation protocols."""

    Operations: ClassVar[tuple[ServiceOperation[Any, Any], ...]] = ()

    def handle_message(self, message: Message):
        operation = self.operation_for_message(message)
        if operation is not None:
            return self.dispatch_operation(operation, message)
        fallback = self.handle_operation_message(message)
        if fallback is not None:
            return fallback
        return self.not_handled()

    def operation_handlers(self) -> Mapping[ServiceOperation[Any, Any], Callable[[Any], Any]]:
        return {}

    def handle_operation_message(self, message: Message) -> Any | None:
        _ = message
        return None

    def operation_for_message(self, message: Message) -> ServiceOperation[Any, Any] | None:
        handlers = self.operation_handlers()
        for operation in (*self.Operations, *handlers.keys()):
            if operation.name == message.kind:
                return operation
        return None

    def dispatch_operation(self, operation: ServiceOperation[Any, Any], message: Message) -> dict[str, Any]:
        handler = self.operation_handlers().get(operation)
        if handler is None:
            raise ServiceContractError(f"No handler registered for operation {operation.name!r}")
        request = operation.decode_request(message)
        reply = handler(request)
        return operation.encode_reply(reply)


@dataclass(slots=True)
class OperationClient:
    """Small proxy wrapper for typed operation calls."""

    proxy: PagletProxy

    def call(
        self,
        operation: ServiceOperation[ReqT, RepT],
        request: ReqT | None = None,
        *,
        activate_if_inactive: bool = True,
        no_delay: bool = False,
        timeout: float | None = None,
    ) -> RepT:
        payload = self.proxy.send(
            operation.to_message(request),
            activate_if_inactive=activate_if_inactive,
            no_delay=no_delay,
            timeout=timeout,
        )
        return operation.decode_reply(payload)

    def send_oneway(
        self,
        operation: ServiceOperation[ReqT, Any],
        request: ReqT | None = None,
        *,
        activate_if_inactive: bool = True,
        no_delay: bool = False,
        timeout: float | None = None,
    ) -> None:
        self.proxy.send_oneway(
            operation.to_message(request),
            activate_if_inactive=activate_if_inactive,
            no_delay=no_delay,
            timeout=timeout,
        )
