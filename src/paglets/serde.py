from __future__ import annotations

from dataclasses import fields, is_dataclass
from enum import Enum
import importlib
import types
from pathlib import Path, PurePath
from typing import Any, get_args, get_origin, get_type_hints

from .errors import SerializationError


def qualified_name(obj: type | object) -> str:
    """Return an importable ``module:qualname`` for a class or object."""

    cls = obj if isinstance(obj, type) else obj.__class__
    module = getattr(cls, "__module__", None)
    qualname = getattr(cls, "__qualname__", None)
    if not module or not qualname:
        raise SerializationError(f"Cannot qualify {obj!r}")
    if "<locals>" in qualname:
        raise SerializationError(
            f"{module}:{qualname} is local and cannot be imported on another host"
        )
    return f"{module}:{qualname}"


def resolve_qualified_name(name: str) -> Any:
    """Resolve a ``module:qualname`` produced by :func:`qualified_name`."""

    if ":" not in name:
        raise SerializationError(f"Expected module:qualname, got {name!r}")
    module_name, qualname = name.split(":", 1)
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # pragma: no cover - exact import errors vary
        raise SerializationError(f"Cannot import module {module_name!r}") from exc
    obj: Any = module
    try:
        for part in qualname.split("."):
            obj = getattr(obj, part)
    except AttributeError as exc:
        raise SerializationError(f"Cannot resolve {name!r}") from exc
    return obj


def dataclass_to_wire(instance: Any) -> dict[str, Any]:
    """Serialize a dataclass instance to a JSON-compatible dict.

    This is intentionally one approach: paglet state is explicit dataclass state.
    Runtime fields on the paglet object itself are transient and never move.
    """

    if not is_dataclass(instance) or isinstance(instance, type):
        raise SerializationError("Paglet state must be a dataclass instance")
    return {field.name: _to_wire_value(getattr(instance, field.name)) for field in fields(instance)}


def dataclass_from_wire(cls: type, payload: dict[str, Any]) -> Any:
    """Restore a dataclass instance from a wire dict."""

    if not is_dataclass(cls) or not isinstance(cls, type):
        raise SerializationError(f"{cls!r} is not a dataclass class")
    if not isinstance(payload, dict):
        raise SerializationError(f"Expected dict payload for {cls!r}, got {type(payload)!r}")

    type_hints = get_type_hints(cls)
    kwargs: dict[str, Any] = {}
    for field in fields(cls):
        if field.name in payload:
            field_type = type_hints.get(field.name, field.type)
            kwargs[field.name] = _from_wire_value(field_type, payload[field.name])
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise SerializationError(f"Could not construct {cls!r} from {payload!r}") from exc


def _to_wire_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return dataclass_to_wire(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, PurePath):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_to_wire_value(item) for item in value]
    if isinstance(value, tuple):
        return [_to_wire_value(item) for item in value]
    if isinstance(value, set):
        items = [_to_wire_value(item) for item in value]
        try:
            return sorted(items)
        except TypeError:
            return items
    if isinstance(value, dict):
        return {str(key): _to_wire_value(item) for key, item in value.items()}
    raise SerializationError(
        f"Unsupported state value {value!r} of type {type(value).__name__}; "
        "use dataclasses, primitives, lists, sets, tuples, dicts, enums, or pathlib paths"
    )


def _from_wire_value(annotation: Any, value: Any) -> Any:
    if value is None:
        return None
    if annotation is Any or annotation is object:
        return value

    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin in (types.UnionType, None) and isinstance(annotation, types.UnionType):
        return _from_union(args, value)
    if origin is types.UnionType:  # defensive for older typing internals
        return _from_union(args, value)
    if str(origin) == "typing.Union":
        return _from_union(args, value)

    if origin in (list, tuple, set, frozenset):
        inner = args[0] if args else Any
        items = [_from_wire_value(inner, item) for item in value]
        if origin is tuple:
            return tuple(items)
        if origin is set:
            return set(items)
        if origin is frozenset:
            return frozenset(items)
        return items

    if origin is dict:
        key_type = args[0] if args else str
        value_type = args[1] if len(args) > 1 else Any
        return {
            _coerce_key(key_type, key): _from_wire_value(value_type, item)
            for key, item in value.items()
        }

    if isinstance(annotation, type):
        if is_dataclass(annotation):
            return dataclass_from_wire(annotation, value)
        if issubclass(annotation, Enum):
            return annotation(value)
        if issubclass(annotation, PurePath):
            return Path(value)
        if annotation in (str, int, float, bool):
            return annotation(value)

    return value


def _from_union(args: tuple[Any, ...], value: Any) -> Any:
    last_error: Exception | None = None
    for arg in args:
        if arg is type(None):
            continue
        try:
            return _from_wire_value(arg, value)
        except Exception as exc:  # try the next option
            last_error = exc
    if last_error is not None:
        raise last_error
    return value


def _coerce_key(annotation: Any, key: str) -> Any:
    if annotation in (Any, object, str):
        return key
    try:
        return annotation(key)
    except Exception:
        return key
