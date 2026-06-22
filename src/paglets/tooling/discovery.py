# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import importlib
import inspect
import sys
from collections.abc import Iterator
from dataclasses import MISSING, dataclass, fields, is_dataclass
from pathlib import Path
from typing import Any

from paglets.core.agent import Paglet
from paglets.remote.admin import AgentDiscoveryConfig
from paglets.serialization.serde import _to_wire_value, qualified_name


@dataclass(frozen=True, slots=True)
class AgentClassRecord:
    class_name: str
    state_class_name: str
    display_name: str
    description: str
    state_template: dict[str, Any]
    required_state_fields: list[str]


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    agent_classes: list[AgentClassRecord]
    errors: list[str]


def discover_agent_classes(config: AgentDiscoveryConfig) -> DiscoveryResult:
    records_by_name: dict[str, AgentClassRecord] = {}
    errors: list[str] = []

    for module_name in config.modules:
        _discover_module(module_name, None, records_by_name, errors)

    for raw_path in config.paths:
        path = Path(raw_path)
        if not path.exists():
            errors.append(f"Discovery path does not exist: {path}")
            continue
        for module_name, search_path in _module_candidates_for_path(path, errors):
            _discover_module(module_name, search_path, records_by_name, errors)

    return DiscoveryResult(
        agent_classes=sorted(records_by_name.values(), key=lambda record: record.class_name),
        errors=errors,
    )


def _module_candidates_for_path(path: Path, errors: list[str]) -> Iterator[tuple[str, Path]]:
    if path.is_file():
        if path.suffix != ".py" or not path.stem.isidentifier():
            errors.append(f"Discovery file is not an importable Python module: {path}")
            return
        yield path.stem, path.parent
        return

    if not path.is_dir():
        errors.append(f"Discovery path is not a file or directory: {path}")
        return

    if path.name.isidentifier():
        prefix = f"{path.name}."
        search_path = path.parent
    else:
        prefix = ""
        search_path = path

    for child in sorted(path.iterdir(), key=lambda item: item.name):
        if child.name.startswith("_"):
            continue
        if child.is_file() and child.suffix == ".py" and child.stem.isidentifier():
            yield f"{prefix}{child.stem}", search_path
        elif child.is_dir() and child.name.isidentifier() and (child / "__init__.py").exists():
            yield f"{prefix}{child.name}", search_path


def _discover_module(
    module_name: str,
    search_path: Path | None,
    records_by_name: dict[str, AgentClassRecord],
    errors: list[str],
) -> None:
    try:
        with _temporary_sys_path(search_path):
            module = importlib.import_module(module_name)
    except Exception as exc:
        errors.append(f"Could not import {module_name}: {exc}")
        return

    for _, obj in inspect.getmembers(module, inspect.isclass):
        if obj.__module__ != module.__name__:
            continue
        if obj is Paglet or not issubclass(obj, Paglet) or inspect.isabstract(obj):
            continue
        record = _record_for_agent_class(obj, errors)
        if record is not None:
            records_by_name[record.class_name] = record


def _record_for_agent_class(agent_cls: type[Paglet], errors: list[str]) -> AgentClassRecord | None:
    try:
        state_cls = agent_cls.state_class()
    except Exception as exc:
        errors.append(f"{qualified_name(agent_cls)} is not discoverable: {exc}")
        return None

    if not is_dataclass(state_cls):
        errors.append(f"{qualified_name(agent_cls)} State is not a dataclass")
        return None

    state_template, required_fields, template_errors = _state_template(state_cls)
    errors.extend(f"{qualified_name(agent_cls)}.{error}" for error in template_errors)
    doc = inspect.getdoc(agent_cls) or ""
    return AgentClassRecord(
        class_name=qualified_name(agent_cls),
        state_class_name=qualified_name(state_cls),
        display_name=agent_cls.__name__,
        description=doc.splitlines()[0] if doc else "",
        state_template=state_template,
        required_state_fields=required_fields,
    )


def _state_template(state_cls: type) -> tuple[dict[str, Any], list[str], list[str]]:
    template: dict[str, Any] = {}
    required: list[str] = []
    errors: list[str] = []
    for field in fields(state_cls):
        if field.default is not MISSING:
            value = field.default
        elif field.default_factory is not MISSING:  # type: ignore[attr-defined]
            try:
                value = field.default_factory()  # type: ignore[misc]
            except Exception as exc:
                required.append(field.name)
                errors.append(f"state field {field.name!r} default_factory failed: {exc}")
                continue
        else:
            required.append(field.name)
            continue

        try:
            template[field.name] = _to_wire_value(value)
        except Exception as exc:
            required.append(field.name)
            errors.append(f"state field {field.name!r} default is not JSON-compatible: {exc}")
    return template, required, errors


class _temporary_sys_path:
    def __init__(self, path: Path | None):
        self.path = None if path is None else str(path)
        self.added = False

    def __enter__(self) -> None:
        if self.path and self.path not in sys.path:
            sys.path.insert(0, self.path)
            self.added = True

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.added and self.path in sys.path:
            sys.path.remove(self.path)
