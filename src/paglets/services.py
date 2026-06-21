# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, is_dataclass
import time
from typing import Any, Generic, TypeVar

from .errors import SerializationError, ServiceContractError, ServiceNotFoundError
from .messages import Message
from .references import PagletProxyRef
from .runtime_values import ServiceScope, enum_from_wire, require_enum
from .serde import dataclass_from_wire, dataclass_to_wire, qualified_name, resolve_qualified_name


CONTRACT_METADATA_KEY = "paglets.service_contract"
_ROUTE_DEFAULT_UNSET = object()

ReqT = TypeVar("ReqT")
RepT = TypeVar("RepT")


@dataclass(frozen=True, slots=True)
class EmptyPayload:
    """Dataclass payload for operations with no request or reply body."""


@dataclass(frozen=True, slots=True)
class ServiceOperation(Generic[ReqT, RepT]):
    """Typed operation exposed by a service contract."""

    name: str
    request_type: type[ReqT] = EmptyPayload  # type: ignore[assignment]
    reply_type: type[RepT] = EmptyPayload  # type: ignore[assignment]

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            raise ServiceContractError("Service operation name cannot be empty")
        object.__setattr__(self, "name", name)
        _schema_name(self.request_type, "request_type")
        _schema_name(self.reply_type, "reply_type")

    @property
    def request_schema(self) -> str:
        return _schema_name(self.request_type, "request_type")

    @property
    def reply_schema(self) -> str:
        return _schema_name(self.reply_type, "reply_type")

    def to_message(self, request: ReqT | None = None) -> Message:
        return Message(self.name, self.encode_request(request))

    def encode_request(self, request: ReqT | None = None) -> dict[str, Any]:
        payload = _default_payload(self.request_type) if request is None else request
        _require_instance(payload, self.request_type, f"{self.name} request")
        return dataclass_to_wire(payload)

    def decode_request(self, message: Message | dict[str, Any]) -> ReqT:
        payload = message.args if isinstance(message, Message) else message
        return _decode_payload(self.request_type, payload, f"{self.name} request")

    def encode_reply(self, reply: RepT | None = None) -> dict[str, Any]:
        payload = _default_payload(self.reply_type) if reply is None else reply
        _require_instance(payload, self.reply_type, f"{self.name} reply")
        return dataclass_to_wire(payload)

    def decode_reply(self, payload: Any) -> RepT:
        if payload is None and self.reply_type is EmptyPayload:
            payload = {}
        if isinstance(payload, self.reply_type):
            return payload
        return _decode_payload(self.reply_type, payload, f"{self.name} reply")


@dataclass(frozen=True, slots=True)
class ServiceContract:
    """Typed service interface advertised through the service registry."""

    name: str
    operations: tuple[ServiceOperation[Any, Any], ...]
    version: str = "1"

    def __post_init__(self) -> None:
        name = self.name.strip()
        version = self.version.strip()
        if not name:
            raise ServiceContractError("Service contract name cannot be empty")
        if not version:
            raise ServiceContractError("Service contract version cannot be empty")
        if not self.operations:
            raise ServiceContractError("Service contract must define at least one operation")

        operations = tuple(self.operations)
        names: set[str] = set()
        for operation in operations:
            if not isinstance(operation, ServiceOperation):
                raise ServiceContractError("Service contract operations must be ServiceOperation instances")
            if operation.name in names:
                raise ServiceContractError(f"Duplicate service operation {operation.name!r}")
            names.add(operation.name)

        object.__setattr__(self, "name", name)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "operations", operations)

    @property
    def capabilities(self) -> tuple[str, ...]:
        return tuple(operation.name for operation in self.operations)

    def operation_for(self, name: str) -> ServiceOperation[Any, Any] | None:
        for operation in self.operations:
            if operation.name == name:
                return operation
        return None

    def require_operation(self, operation: ServiceOperation[Any, Any]) -> ServiceOperation[Any, Any]:
        known = self.operation_for(operation.name)
        if known is None or known != operation:
            raise ServiceContractError(f"{operation.name!r} is not part of service contract {self.name!r}")
        return known

    def metadata(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "operations": [
                {
                    "name": operation.name,
                    "request_type": operation.request_schema,
                    "reply_type": operation.reply_schema,
                }
                for operation in self.operations
            ],
        }

    def advertise_metadata(self, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        merged = dict(metadata or {})
        if CONTRACT_METADATA_KEY in merged:
            raise ServiceContractError(f"Metadata key {CONTRACT_METADATA_KEY!r} is reserved")
        merged[CONTRACT_METADATA_KEY] = self.metadata()
        return merged

    def matches_record(self, record: "ServiceRecord") -> bool:
        return (
            record.name == self.name
            and tuple(record.capabilities) == self.capabilities
            and record.metadata.get(CONTRACT_METADATA_KEY) == self.metadata()
        )

    def route(
        self,
        message: Message,
        handlers: Mapping[ServiceOperation[Any, Any], Callable[[Any], Any]],
        *,
        default: Any = _ROUTE_DEFAULT_UNSET,
    ) -> Any:
        operation = self.operation_for(message.kind)
        if operation is None:
            if default is _ROUTE_DEFAULT_UNSET:
                raise ServiceContractError(f"{message.kind!r} is not part of service contract {self.name!r}")
            return default
        handler = handlers.get(operation)
        if handler is None:
            raise ServiceContractError(f"No handler registered for operation {operation.name!r}")
        request = operation.decode_request(message)
        reply = handler(request)
        return operation.encode_reply(reply)


@dataclass(frozen=True, slots=True)
class ServiceHandle:
    """Resolved typed service client for one advertised service record."""

    contract: ServiceContract
    record: "ServiceRecord"
    context_or_client: Any = None

    def __post_init__(self) -> None:
        if not self.contract.matches_record(self.record):
            raise ServiceContractError(f"Service record {self.record.name!r} does not match contract {self.contract.name!r}")

    def call(
        self,
        operation: ServiceOperation[ReqT, RepT],
        request: ReqT | None = None,
        *,
        activate_if_inactive: bool = True,
        no_delay: bool = False,
        timeout: float | None = None,
    ) -> RepT:
        operation = self.contract.require_operation(operation)  # type: ignore[assignment]
        message = operation.to_message(request)
        payload = self.record.proxy.resolve(self.context_or_client).send(
            message,
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
        operation = self.contract.require_operation(operation)  # type: ignore[assignment]
        message = operation.to_message(request)
        self.record.proxy.resolve(self.context_or_client).send_oneway(
            message,
            activate_if_inactive=activate_if_inactive,
            no_delay=no_delay,
            timeout=timeout,
        )


@dataclass(frozen=True, slots=True)
class ServiceRecord:
    name: str
    proxy: PagletProxyRef
    capabilities: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    scope: ServiceScope = ServiceScope.LOCAL
    host_name: str = ""
    host_url: str = ""
    advertised_at: float = field(default_factory=time.time)
    expires_at: float | None = None

    def __post_init__(self) -> None:
        require_enum(self.scope, ServiceScope, "scope")

    @property
    def expired(self) -> bool:
        return self.expires_at is not None and self.expires_at <= time.time()

    def matches(self, name: str, capability: str | None = None) -> bool:
        if self.name != name or self.expired:
            return False
        return capability is None or capability in self.capabilities

    def to_wire(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "proxy": self.proxy.to_wire(),
            "capabilities": list(self.capabilities),
            "metadata": self.metadata,
            "scope": self.scope.value,
            "host_name": self.host_name,
            "host_url": self.host_url,
            "advertised_at": self.advertised_at,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "ServiceRecord":
        return cls(
            name=str(payload["name"]),
            proxy=PagletProxyRef.from_wire(payload["proxy"]),
            capabilities=tuple(str(item) for item in payload.get("capabilities", [])),
            metadata=dict(payload.get("metadata") or {}),
            scope=enum_from_wire(payload.get("scope") or ServiceScope.LOCAL.value, ServiceScope, "scope"),
            host_name=str(payload.get("host_name") or ""),
            host_url=str(payload.get("host_url") or ""),
            advertised_at=float(payload.get("advertised_at", time.time())),
            expires_at=float(payload["expires_at"]) if payload.get("expires_at") is not None else None,
        )


class ServiceRegistry:
    def __init__(self):
        self._records: dict[tuple[str, str], ServiceRecord] = {}

    def advertise(
        self,
        *,
        host_name: str,
        host_url: str,
        name: str,
        proxy: PagletProxyRef,
        capabilities: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
        scope: ServiceScope = ServiceScope.LOCAL,
        ttl: float | None = None,
    ) -> ServiceRecord:
        require_enum(scope, ServiceScope, "scope")
        record = ServiceRecord(
            name=name,
            proxy=proxy,
            capabilities=tuple(capabilities or ()),
            metadata=metadata or {},
            scope=scope,
            host_name=host_name,
            host_url=host_url,
            expires_at=time.time() + ttl if ttl is not None else None,
        )
        self._records[(name, proxy.agent_id)] = record
        return record

    def unadvertise(self, name: str, agent_id: str | None = None) -> list[ServiceRecord]:
        removed: list[ServiceRecord] = []
        for key, record in list(self._records.items()):
            if record.name == name and (agent_id is None or record.proxy.agent_id == agent_id):
                removed.append(self._records.pop(key))
        return removed

    def record(self, name: str, agent_id: str) -> ServiceRecord | None:
        self._expire()
        return self._records.get((name, agent_id))

    def remove_agent(
        self,
        agent_id: str,
        *,
        keep: Callable[[ServiceRecord], bool] | None = None,
    ) -> list[ServiceRecord]:
        removed: list[ServiceRecord] = []
        for key, record in list(self._records.items()):
            if record.proxy.agent_id == agent_id:
                if keep is not None and keep(record):
                    continue
                removed.append(self._records.pop(key))
        return removed

    def lookup(self, name: str, capability: str | None = None) -> ServiceRecord | None:
        self._expire()
        matches = [record for record in self._records.values() if record.matches(name, capability)]
        return matches[0] if matches else None

    def lookup_all(
        self,
        name: str | None = None,
        capability: str | None = None,
        scope: ServiceScope | None = None,
    ) -> list[ServiceRecord]:
        if scope is not None:
            require_enum(scope, ServiceScope, "scope")
        self._expire()
        return [
            record
            for record in self._records.values()
            if (name is None or record.name == name)
            and (capability is None or capability in record.capabilities)
            and (scope is None or record.scope == scope)
        ]

    def _expire(self) -> None:
        for key, record in list(self._records.items()):
            if record.expired:
                self._records.pop(key, None)


def _schema_name(schema_type: type[Any], field_name: str) -> str:
    if not isinstance(schema_type, type) or not is_dataclass(schema_type):
        raise ServiceContractError(f"Service operation {field_name} must be an importable dataclass class")
    try:
        name = qualified_name(schema_type)
        resolved = resolve_qualified_name(name)
    except SerializationError as exc:
        raise ServiceContractError(f"Service operation {field_name} must be importable") from exc
    if resolved is not schema_type:
        raise ServiceContractError(f"Service operation {field_name} must resolve to the same dataclass class")
    return name


def _default_payload(payload_type: type[ReqT]) -> ReqT:
    try:
        return payload_type()
    except TypeError as exc:
        raise ServiceContractError(f"{payload_type.__name__} requires an explicit payload instance") from exc


def _require_instance(value: Any, payload_type: type[Any], label: str) -> None:
    if not isinstance(value, payload_type):
        raise ServiceContractError(f"{label} must be {payload_type.__module__}.{payload_type.__qualname__}")


def _decode_payload(payload_type: type[ReqT], payload: Any, label: str) -> ReqT:
    if not isinstance(payload, dict):
        raise ServiceContractError(f"{label} payload must be a JSON object")
    try:
        return dataclass_from_wire(payload_type, payload)
    except SerializationError as exc:
        raise ServiceContractError(f"Could not decode {label} payload") from exc
