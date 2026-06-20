# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from paglets.serde import dataclass_from_wire, dataclass_to_wire, qualified_name, resolve_qualified_name
from paglets.errors import SerializationError


@dataclass
class Nested:
    count: int


@dataclass
class ComplexState:
    name: str
    nested: Nested
    values: list[int] = field(default_factory=list)
    tags: set[str] = field(default_factory=set)
    optional: int | None = None
    path: Path | None = None


def test_dataclass_state_round_trip_preserves_declared_fields():
    state = ComplexState(
        name="agent",
        nested=Nested(3),
        values=[1, 2, 3],
        tags={"b", "a"},
        optional=None,
        path=Path("runs/out.txt"),
    )

    wire = dataclass_to_wire(state)
    restored = dataclass_from_wire(ComplexState, wire)

    assert restored == state
    assert wire == {
        "name": "agent",
        "nested": {"count": 3},
        "values": [1, 2, 3],
        "tags": ["a", "b"],
        "optional": None,
        "path": "runs/out.txt",
    }


def test_qualified_names_resolve_importable_classes():
    name = qualified_name(ComplexState)
    assert name == "tests.test_serde:ComplexState"
    assert resolve_qualified_name(name) is ComplexState


def test_non_dataclass_state_is_rejected():
    with pytest.raises(SerializationError):
        dataclass_to_wire({"not": "a dataclass"})
