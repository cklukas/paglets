# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from enum import StrEnum
from typing import Any, TypeVar


class ServiceScope(StrEnum):
    LOCAL = "local"
    MESH = "mesh"


class ResidentLifecycle(StrEnum):
    LAZY = "lazy"
    EAGER = "eager"


class ArrivalMode(StrEnum):
    ACTIVATE = "activate"
    INACTIVE = "inactive"


class EnvelopeKind(StrEnum):
    DISPATCH = "dispatch"
    CLONE = "clone"
    RETRACT = "retract"
    ACTIVATION = "activation"


class LaunchConfigSyncAction(StrEnum):
    COPIED = "copied"
    UPDATED = "updated"
    UNCHANGED = "unchanged"
    SKIPPED = "skipped"
    UPDATE_AVAILABLE = "update-available"


EnumT = TypeVar("EnumT", bound=StrEnum)


def require_enum(value: Any, enum_cls: type[EnumT], field_name: str) -> EnumT:
    if not isinstance(value, enum_cls):
        raise TypeError(f"{field_name} must be {enum_cls.__name__}")
    return value


def enum_from_wire(value: Any, enum_cls: type[EnumT], field_name: str) -> EnumT:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    try:
        return enum_cls(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in enum_cls)
        raise ValueError(f"{field_name} must be one of: {allowed}") from exc
