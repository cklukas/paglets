# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import is_dataclass
from typing import Any, ClassVar, Generic, TypeVar, TYPE_CHECKING
import uuid

from .errors import HostError, NotHandledError
from .events import CloneEvent, CreationEvent, MobilityEvent, PersistencyEvent
from .messages import Message, ReplySet

if TYPE_CHECKING:  # pragma: no cover
    from .host import Host
    from .mesh import HostRef
    from .proxy import PagletProxy


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


class PagletContext:
    """Host-provided environment visible to a running paglet."""

    def __init__(self, host: "Host"):
        self._host = host

    @property
    def name(self) -> str:
        return self._host.name

    @property
    def address(self) -> str:
        return self._host.address

    @property
    def host(self) -> "Host":
        return self._host

    def get_proxy(self, agent_id: str, host_url: str | None = None) -> "PagletProxy | None":
        if host_url is None or host_url.rstrip("/") == self.address.rstrip("/"):
            return self._host.get_proxy(agent_id)
        from .proxy import PagletProxy

        return PagletProxy(host_url=host_url, agent_id=agent_id, client=self._host.client)

    def get_proxies(self, state: int = ACTIVE) -> list["PagletProxy"]:
        return self._host.get_proxies(state)

    def get_property(self, key: str, default: Any = None) -> Any:
        return self._host.get_property(key, default)

    def set_property(self, key: str, value: Any) -> None:
        self._host.set_property(key, value)

    def create_paglet(
        self,
        agent_cls: type["Paglet"],
        state: PagletState | None = None,
        *,
        init: Any = None,
        host_url: str | None = None,
    ) -> "PagletProxy":
        if host_url is not None and host_url.rstrip("/") != self.address.rstrip("/"):
            return self._host.create_remote(host_url, agent_cls, state, init=init)
        return self._host.create(agent_cls, state, init=init)

    def dispatch(self, agent_id: str, target: str) -> "PagletProxy":
        return self._host.dispatch(agent_id, target)

    def clone(self, agent_id: str, target: str | None = None) -> "PagletProxy":
        return self._host.clone(agent_id, target=target)

    def available_hosts(self, *, online_only: bool = True, include_self: bool = True) -> list["HostRef"]:
        return self._host.mesh.hosts(online_only=online_only, include_self=include_self)

    def host_status(self, name_or_url: str) -> "HostRef | None":
        return self._host.mesh.lookup(name_or_url)

    def is_host_online(self, name_or_url: str) -> bool:
        return self._host.mesh.is_online(name_or_url)

    def wait_for_host(self, name_or_url: str, *, timeout: float = 10.0, interval: float = 0.25) -> "HostRef":
        return self._host.mesh.wait_for_host(name_or_url, timeout=timeout, interval=interval)

    def dispatch_to(self, agent_id: str, name_or_url: str) -> "PagletProxy":
        return self.dispatch(agent_id, self._host.mesh.resolve_url(name_or_url))

    def clone_to(self, agent_id: str, name_or_url: str) -> "PagletProxy":
        return self.clone(agent_id, self._host.mesh.resolve_url(name_or_url))

    def send(self, target_agent_id: str, kind: str, args: dict[str, Any] | None = None, *, host_url: str | None = None) -> Any:
        proxy = self.get_proxy(target_agent_id, host_url)
        if proxy is None:
            raise HostError(f"No such local paglet: {target_agent_id}")
        return proxy.send_message(kind, args or {}, sender=self.address)

    def multicast(self, kind: str | Message, args: dict[str, Any] | None = None, *, exclude: set[str] | None = None) -> ReplySet:
        return self._host.multicast_message(kind, args, exclude=exclude)


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

    def __init__(self, state: StateT | None = None, *, agent_id: str | None = None):
        state_cls = self.state_class()
        if state is None:
            state = state_cls()  # type: ignore[call-arg]
        if not is_dataclass(state):
            raise HostError(f"{self.__class__.__name__}.State must be a dataclass state object")
        self.agent_id = agent_id or uuid.uuid4().hex
        self.state: StateT = state
        self._context: PagletContext | None = None
        self._last_proxy: PagletProxy | None = None

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

    # Convenience operations available from inside lifecycle/message handlers.
    def dispatch(self, target: str) -> "PagletProxy":
        proxy = self.context.dispatch(self.agent_id, target)
        self._last_proxy = proxy
        return proxy

    def clone(self, target: str | None = None) -> "PagletProxy":
        proxy = self.context.clone(self.agent_id, target)
        self._last_proxy = proxy
        return proxy

    def dispatch_to(self, name_or_url: str) -> "PagletProxy":
        proxy = self.context.dispatch_to(self.agent_id, name_or_url)
        self._last_proxy = proxy
        return proxy

    def clone_to(self, name_or_url: str) -> "PagletProxy":
        proxy = self.context.clone_to(self.agent_id, name_or_url)
        self._last_proxy = proxy
        return proxy

    def send(self, target_agent_id: str, kind: str, args: dict[str, Any] | None = None, *, host_url: str | None = None) -> Any:
        return self.context.send(target_agent_id, kind, args or {}, host_url=host_url)

    def multicast(self, kind: str | Message, args: dict[str, Any] | None = None, *, include_self: bool = True) -> ReplySet:
        exclude = None if include_self else {self.agent_id}
        return self.context.multicast(kind, args, exclude=exclude)

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

    def run(self) -> None:
        pass

    def handle_message(self, message: Message) -> Any:
        raise NotHandledError(f"{self.__class__.__name__} did not handle {message.kind!r}")
