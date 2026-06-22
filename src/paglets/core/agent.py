# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import is_dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, ClassVar, Concatenate, Generic, ParamSpec, TypeVar

from paglets.core.errors import HostError, NotHandledError
from paglets.core.events import CloneEvent, CreationEvent, MobilityEvent, PersistencyEvent
from paglets.core.messages import Message, ReplySet
from paglets.core.runtime_values import ServiceScope
from paglets.persistence.persistency import DeactivationPolicy, DeactivationRequest
from paglets.runtime.resources import ResourceRegistry

if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path

    from paglets.persistence.storage import ManagedStorage
    from paglets.remote.mesh import HostRef
    from paglets.remote.proxy import PagletProxy
    from paglets.remote.references import PagletProxyRef
    from paglets.remote.transfer import TransferTicket
    from paglets.runtime.host import Host
    from paglets.services.contracts import ServiceContract, ServiceHandle, ServiceOperation, ServiceRecord
    from paglets.services.resident import ServiceLease


ACTIVE = 0x1
INACTIVE = 0x1 << 1


class PagletState:
    """Marker base class for dataclass state objects.

    Subclass this with ``@dataclass``. Only this state object moves. Everything
    stored directly on the paglet instance is transient runtime state.
    """


class _NotHandled:
    pass


NOT_HANDLED = _NotHandled()


StateT = TypeVar("StateT", bound=PagletState)
PagletT = TypeVar("PagletT", bound="Paglet[Any]")
P = ParamSpec("P")
ReturnT = TypeVar("ReturnT")


def state_locked(method: Callable[Concatenate[PagletT, P], ReturnT]) -> Callable[Concatenate[PagletT, P], ReturnT]:
    """Run a paglet method under the paglet's reentrant state lock."""

    @wraps(method)
    def wrapper(self: PagletT, *args: P.args, **kwargs: P.kwargs) -> ReturnT:
        with self.locked():
            return method(self, *args, **kwargs)

    return wrapper


class PagletContext:
    """Host-provided environment visible to a running paglet."""

    def __init__(self, host: Host, agent_id: str | None = None):
        self._host = host
        self._agent_id = agent_id

    @property
    def name(self) -> str:
        return self._host.name

    @property
    def address(self) -> str:
        return self._host.address

    @property
    def host(self) -> Host:
        return self._host

    @property
    def agent_id(self) -> str | None:
        return self._agent_id

    def get_proxy(self, agent_id: str, host_url: str | None = None) -> PagletProxy | None:
        if host_url is None or host_url.rstrip("/") == self.address.rstrip("/"):
            return self._host.get_proxy(agent_id)
        from paglets.remote.proxy import PagletProxy

        return PagletProxy(host_url=host_url, agent_id=agent_id, client=self._host.client)

    def get_proxies(self, state: int = ACTIVE) -> list[PagletProxy]:
        return self._host.get_proxies(state)

    def get_property(self, key: str, default: Any = None) -> Any:
        return self._host.get_property(key, default)

    def set_property(self, key: str, value: Any) -> None:
        self._host.set_property(key, value)

    def create_paglet(
        self,
        agent_cls: type[Paglet],
        state: PagletState | None = None,
        *,
        init: Any = None,
        host_url: str | None = None,
    ) -> PagletProxy:
        if host_url is not None and host_url.rstrip("/") != self.address.rstrip("/"):
            return self._host.create_remote(host_url, agent_cls, state, init=init)
        return self._host.create(agent_cls, state, init=init)

    def dispatch(self, agent_id: str, target: str | TransferTicket) -> PagletProxy:
        return self._host.dispatch(agent_id, target)

    def clone(self, agent_id: str, target: str | TransferTicket | None = None) -> PagletProxy:
        return self._host.clone(agent_id, target=target)

    def deactivate(
        self,
        agent_id: str,
        request: DeactivationRequest | None = None,
    ) -> PagletProxy:
        return self._host.deactivate(agent_id, request=request)

    def available_hosts(self, *, online_only: bool = True, include_self: bool = True) -> list[HostRef]:
        return self._host.mesh.hosts(online_only=online_only, include_self=include_self)

    def host_status(self, name_or_url: str) -> HostRef | None:
        return self._host.mesh.lookup(name_or_url)

    def is_host_online(self, name_or_url: str) -> bool:
        return self._host.mesh.is_online(name_or_url)

    def wait_for_host(self, name_or_url: str, *, timeout: float = 10.0, interval: float = 0.25) -> HostRef:
        return self._host.mesh.wait_for_host(name_or_url, timeout=timeout, interval=interval)

    def dispatch_to(self, agent_id: str, name_or_url: str) -> PagletProxy:
        return self.dispatch(agent_id, self._host.mesh.resolve_url(name_or_url))

    def clone_to(self, agent_id: str, name_or_url: str) -> PagletProxy:
        return self.clone(agent_id, self._host.mesh.resolve_url(name_or_url))

    def send(self, target_agent_id: str, message: Message, *, host_url: str | None = None) -> Any:
        proxy = self.get_proxy(target_agent_id, host_url)
        if proxy is None:
            raise HostError(f"No such local paglet: {target_agent_id}")
        if message.sender is None:
            message.sender = self.address
        return proxy.send(message)

    def multicast(
        self, kind: str | Message, args: dict[str, Any] | None = None, *, exclude: set[str] | None = None
    ) -> ReplySet:
        return self._host.multicast_message(kind, args, exclude=exclude)

    def advertise_service(
        self,
        name: str,
        *,
        capabilities: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
        ttl: float | None = None,
        agent_id: str | None = None,
    ) -> ServiceRecord:
        owner_id = agent_id or self._agent_id
        if owner_id is None:
            raise HostError("advertise_service requires an attached paglet or explicit agent_id")
        return self._host.advertise_service(
            owner_id,
            name,
            capabilities=capabilities,
            metadata=metadata,
            scope=scope,
            ttl=ttl,
        )

    def unadvertise_service(self, name: str, *, agent_id: str | None = None) -> list[ServiceRecord]:
        owner_id = agent_id or self._agent_id
        if owner_id is None:
            raise HostError("unadvertise_service requires an attached paglet or explicit agent_id")
        return self._host.unadvertise_service(name, agent_id=owner_id)

    def lookup_service(
        self,
        name: str,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> PagletProxyRef | None:
        record = self._host.lookup_service(name, capability=capability, scope=scope)
        return record.proxy if record is not None else None

    def lookup_services(
        self,
        name: str | None = None,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> list[ServiceRecord]:
        return self._host.lookup_services(name, capability=capability, scope=scope)

    def advertise_contract(
        self,
        contract: ServiceContract,
        *,
        scope: ServiceScope = ServiceScope.LOCAL,
        ttl: float | None = None,
        metadata: dict[str, Any] | None = None,
        agent_id: str | None = None,
    ) -> ServiceRecord:
        owner_id = agent_id or self._agent_id
        if owner_id is None:
            raise HostError("advertise_contract requires an attached paglet or explicit agent_id")
        return self.advertise_service(
            contract.name,
            capabilities=contract.capabilities,
            metadata=contract.advertise_metadata(metadata),
            scope=scope,
            ttl=ttl,
            agent_id=owner_id,
        )

    def lookup_contract(
        self,
        contract: ServiceContract,
        *,
        operation: ServiceOperation[Any, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> ServiceHandle | None:
        handles = self.lookup_contracts(contract, operation=operation, scope=scope)
        return handles[0] if handles else None

    def lookup_contracts(
        self,
        contract: ServiceContract,
        *,
        operation: ServiceOperation[Any, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> list[ServiceHandle]:
        from paglets.services.contracts import ServiceHandle

        if operation is not None:
            operation = contract.require_operation(operation)
        capability = operation.name if operation is not None else None
        return [
            ServiceHandle(contract, record, self)
            for record in self.lookup_services(contract.name, capability=capability, scope=scope)
            if contract.matches_record(record)
        ]

    def require_contract(
        self,
        contract: ServiceContract,
        *,
        operation: ServiceOperation[Any, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> ServiceHandle:
        from paglets.core.errors import ServiceNotFoundError

        handle = self.lookup_contract(contract, operation=operation, scope=scope)
        if handle is None:
            operation_text = f" operation {operation.name!r}" if operation is not None else ""
            contract_text = f"contract {contract.name!r} version {contract.version!r}{operation_text}"
            raise ServiceNotFoundError(f"No service {contract_text} found in {scope} scope")
        return handle

    def lease_contract(
        self,
        contract: ServiceContract,
        *,
        operation: ServiceOperation[Any, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
        ttl: float = 60.0,
    ) -> ServiceLease:
        handle = self.require_contract(contract, operation=operation, scope=scope)
        lease = self._host.lease_service_handle(handle, ttl=ttl)
        if self._agent_id is not None:
            self._host.resources_for(self._agent_id).register(
                f"service-lease:{lease.lease_id}",
                lease.release,
                suppress=True,
            )
        return lease

    def resources(self, agent_id: str | None = None) -> ResourceRegistry:
        owner_id = agent_id or self._agent_id
        if owner_id is None:
            raise HostError("resources requires an attached paglet or explicit agent_id")
        return self._host.resources_for(owner_id)

    def work_dir(self, *, create: bool = True, agent_id: str | None = None) -> Path:
        owner_id = agent_id or self._agent_id
        if owner_id is None:
            raise HostError("work_dir requires an attached paglet or explicit agent_id")
        return self._host.work_dir_for(owner_id, create=create)

    def persistent_storage(
        self,
        *,
        quota_bytes: int | None = None,
        agent_id: str | None = None,
    ) -> ManagedStorage:
        owner_id = agent_id or self._agent_id
        if owner_id is None:
            raise HostError("persistent_storage requires an attached paglet or explicit agent_id")
        return self._host.persistent_storage_for(owner_id, quota_bytes=quota_bytes)


class Paglet(Generic[StateT]):
    """Base class for mobile Python objects.

    Subclasses set ``State`` to a dataclass type and override lifecycle hooks.
    The runtime instantiates paglets on each host from class path + dataclass
    state, mirroring Aglets' mobile object plus event system without moving a
    call stack.
    """

    State: ClassVar[type[StateT]]
    ACTIVE: ClassVar[int] = ACTIVE
    INACTIVE: ClassVar[int] = INACTIVE
    MAILBOX_WORKERS: ClassVar[int] = 4

    def __init__(self, state: StateT | None = None, *, agent_id: str | None = None):
        state_cls = self.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        if not is_dataclass(state):
            raise HostError(f"{self.__class__.__name__}.State must be a dataclass state object")
        self.agent_id = agent_id or uuid.uuid4().hex
        self.state: StateT = state
        self._state_lock = threading.RLock()
        self._state_condition = threading.Condition(self._state_lock)
        self._context: PagletContext | None = None
        self._last_proxy: PagletProxy | None = None
        self.resources = ResourceRegistry()

    @classmethod
    def state_class(cls) -> type[StateT]:
        state_cls = getattr(cls, "State", None)
        if state_cls is None:
            raise HostError(f"{cls.__name__} must define a dataclass State class")
        if not is_dataclass(state_cls):
            raise HostError(f"{cls.__name__}.State must be decorated with @dataclass")
        return state_cls

    @property
    def context(self) -> PagletContext:
        if self._context is None:
            raise HostError("Paglet is not attached to a host context")
        return self._context

    def _attach(self, context: PagletContext) -> None:
        self._context = context

    @contextmanager
    def locked(self) -> Iterator[None]:
        """Enter the paglet's reentrant lock for agent-local critical sections."""

        with self._state_lock:
            yield

    @contextmanager
    def locked_state(self) -> Iterator[StateT]:
        """Yield this paglet's dataclass state under the paglet lock."""

        with self._state_lock:
            yield self.state

    def wait_state(self, predicate: Callable[[StateT], bool], timeout: float | None = None) -> bool:
        """Wait until ``predicate(state)`` is true.

        This is for coordination between handlers/background work that mutate
        paglet state and another handler waiting for that state to change. It
        does not replace normal message delivery; incoming messages still call
        ``handle_message`` through the paglet mailbox.
        """

        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)

        with self._state_condition:
            if predicate(self.state):
                return True
            while True:
                if deadline is None:
                    remaining = None
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return bool(predicate(self.state))
                self._state_condition.wait(remaining)
                if predicate(self.state):
                    return True

    def notify_state_changed(self) -> None:
        """Wake one waiter blocked in :meth:`wait_state`."""

        with self._state_condition:
            self._state_condition.notify(1)

    def notify_all_state_changed(self) -> None:
        """Wake all waiters blocked in :meth:`wait_state`."""

        with self._state_condition:
            self._state_condition.notify_all()

    # Convenience operations available from inside lifecycle/message handlers.
    def dispatch(self, target: str | TransferTicket) -> PagletProxy:
        proxy = self.context.dispatch(self.agent_id, target)
        self._last_proxy = proxy
        return proxy

    def clone(self, target: str | TransferTicket | None = None) -> PagletProxy:
        proxy = self.context.clone(self.agent_id, target)
        self._last_proxy = proxy
        return proxy

    def dispatch_to(self, name_or_url: str) -> PagletProxy:
        proxy = self.context.dispatch_to(self.agent_id, name_or_url)
        self._last_proxy = proxy
        return proxy

    def clone_to(self, name_or_url: str) -> PagletProxy:
        proxy = self.context.clone_to(self.agent_id, name_or_url)
        self._last_proxy = proxy
        return proxy

    def deactivate(
        self,
        *,
        reason: str = "deactivate",
        policy: DeactivationPolicy | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PagletProxy:
        proxy = self.context.deactivate(
            self.agent_id,
            DeactivationRequest(
                reason=reason,
                source="self",
                policy=policy,
                metadata=metadata or {},
            ),
        )
        self._last_proxy = proxy
        return proxy

    def send(self, target_agent_id: str, message: Message, *, host_url: str | None = None) -> Any:
        return self.context.send(target_agent_id, message, host_url=host_url)

    def multicast(
        self, kind: str | Message, args: dict[str, Any] | None = None, *, include_self: bool = True
    ) -> ReplySet:
        exclude = None if include_self else {self.agent_id}
        return self.context.multicast(kind, args, exclude=exclude)

    def wait_message(self, timeout: float | None = None) -> bool:
        return self.context.host.wait_message(self.agent_id, timeout=timeout)

    def notify_message(self) -> None:
        self.context.host.notify_message(self.agent_id)

    def notify_all_messages(self) -> None:
        self.context.host.notify_all_messages(self.agent_id)

    def advertise_service(
        self,
        name: str,
        *,
        capabilities: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
        ttl: float | None = None,
    ) -> ServiceRecord:
        return self.context.advertise_service(
            name,
            capabilities=capabilities,
            metadata=metadata,
            scope=scope,
            ttl=ttl,
            agent_id=self.agent_id,
        )

    def unadvertise_service(self, name: str) -> list[ServiceRecord]:
        return self.context.unadvertise_service(name, agent_id=self.agent_id)

    def lookup_service(
        self,
        name: str,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> PagletProxyRef | None:
        return self.context.lookup_service(name, capability=capability, scope=scope)

    def lookup_services(
        self,
        name: str | None = None,
        *,
        capability: str | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> list[ServiceRecord]:
        return self.context.lookup_services(name, capability=capability, scope=scope)

    def advertise_contract(
        self,
        contract: ServiceContract,
        *,
        scope: ServiceScope = ServiceScope.LOCAL,
        ttl: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ServiceRecord:
        return self.context.advertise_contract(
            contract,
            scope=scope,
            ttl=ttl,
            metadata=metadata,
            agent_id=self.agent_id,
        )

    def lookup_contract(
        self,
        contract: ServiceContract,
        *,
        operation: ServiceOperation[Any, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> ServiceHandle | None:
        return self.context.lookup_contract(contract, operation=operation, scope=scope)

    def lookup_contracts(
        self,
        contract: ServiceContract,
        *,
        operation: ServiceOperation[Any, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> list[ServiceHandle]:
        return self.context.lookup_contracts(contract, operation=operation, scope=scope)

    def require_contract(
        self,
        contract: ServiceContract,
        *,
        operation: ServiceOperation[Any, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
    ) -> ServiceHandle:
        return self.context.require_contract(contract, operation=operation, scope=scope)

    def lease_contract(
        self,
        contract: ServiceContract,
        *,
        operation: ServiceOperation[Any, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
        ttl: float = 60.0,
    ) -> ServiceLease:
        return self.context.lease_contract(contract, operation=operation, scope=scope, ttl=ttl)

    def work_dir(self, *, create: bool = True) -> Path:
        return self.context.work_dir(create=create, agent_id=self.agent_id)

    def persistent_storage(self, *, quota_bytes: int | None = None) -> ManagedStorage:
        return self.context.persistent_storage(quota_bytes=quota_bytes, agent_id=self.agent_id)

    @staticmethod
    def not_handled() -> _NotHandled:
        return NOT_HANDLED

    # Lifecycle/event hooks. Override these in subclasses.
    def on_creation(self, event: CreationEvent) -> None:
        pass

    def on_dispatching(self, event: MobilityEvent) -> None:
        pass

    def on_arrival(self, event: MobilityEvent) -> None:
        pass

    def on_reverting(self, event: MobilityEvent) -> None:
        pass

    def on_cloning(self, event: CloneEvent) -> None:
        pass

    def on_clone(self, event: CloneEvent) -> None:
        pass

    def on_cloned(self, event: CloneEvent) -> None:
        pass

    def on_deactivating(self, event: PersistencyEvent) -> None:
        pass

    def on_activation(self, event: PersistencyEvent) -> None:
        pass

    def on_disposing(self, event: PersistencyEvent) -> None:
        pass

    def deactivation_policy(self, request: DeactivationRequest) -> DeactivationPolicy:
        return request.policy or DeactivationPolicy()

    def run(self) -> None:
        pass

    def handle_message(self, message: Message) -> Any:
        raise NotHandledError(f"{self.__class__.__name__} did not handle {message.kind!r}")
