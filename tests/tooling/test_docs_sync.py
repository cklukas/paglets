# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import dataclasses
import enum
import sys
import types
from pathlib import Path

import pytest

from tools import docs_sync

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_repository_documentation_is_in_sync():
    stale = docs_sync.stale_files(REPO_ROOT)

    assert stale == [], "run: uv run python tools/docs_sync.py"


def test_snippet_section_is_extracted_and_dedented(tmp_path: Path):
    source = tmp_path / "src" / "demo.py"
    source.parent.mkdir()
    source.write_text(
        "def outer():\n    # --8<-- [start:body]\n    value = 1\n    return value\n    # --8<-- [end:body]\n",
        encoding="utf-8",
    )
    text = "Intro\n\n<!-- snippet: src/demo.py:body | outer in demo.py -->\nold\n<!-- /snippet -->\n"

    synced = docs_sync.sync_text(tmp_path, text)

    assert synced == (
        "Intro\n\n<!-- snippet: src/demo.py:body | outer in demo.py -->\n"
        f"Source: [outer in demo.py]({docs_sync.REPOSITORY_URL}/src/demo.py)\n\n"
        "```python\nvalue = 1\nreturn value\n```\n"
        "<!-- /snippet -->\n"
    )
    assert docs_sync.sync_text(tmp_path, synced) == synced


def test_snippet_without_section_includes_whole_file(tmp_path: Path):
    (tmp_path / "config.toml").write_text("[a]\nb = 1\n", encoding="utf-8")

    synced = docs_sync.sync_text(tmp_path, "<!-- snippet: config.toml -->\n<!-- /snippet -->")

    assert "```toml\n[a]\nb = 1\n```\n" in synced


def test_missing_snippet_section_is_an_error(tmp_path: Path):
    (tmp_path / "a.py").write_text("x = 1\n", encoding="utf-8")

    with pytest.raises(docs_sync.DocsSyncError, match="section 'missing' not found"):
        docs_sync.sync_text(tmp_path, "<!-- snippet: a.py:missing -->\n<!-- /snippet -->")


def test_api_reference_lists_documented_public_members(monkeypatch: pytest.MonkeyPatch):
    module = types.ModuleType("docs_sync_demo")
    module.__doc__ = "Demo module."

    class Color(enum.Enum):
        """Colors."""

        RED = "red"

    @dataclasses.dataclass
    class Settings:
        """Settings for the demo."""

        name: str
        retries: int = 3
        color: Color = Color.RED
        tags: list[str] = dataclasses.field(default_factory=list)

        def describe(self, verbose: bool = False) -> str:
            """Describe the settings."""
            return self.name if verbose else ""

        def undocumented(self) -> None:
            return None

    def helper(value: int, *, scale: float = 1.0) -> float:
        """Scale a value."""
        return value * scale

    def _private() -> None:
        """Hidden."""

    for obj in (Color, Settings, helper, _private):
        obj.__module__ = "docs_sync_demo"
        module.__dict__[obj.__name__] = obj
    monkeypatch.setitem(sys.modules, "docs_sync_demo", module)

    text = docs_sync.render_module("docs_sync_demo")

    assert text.startswith("### `docs_sync_demo`\n\nDemo module.\n")
    assert "| `RED` | `'red'` |" in text
    assert "| `retries` | `int` | `3` |" in text
    assert "| `color` | `Color` | `Color.RED` |" in text
    assert "| `tags` | `list[str]` | `list()` |" in text
    assert "##### `Settings.describe`" in text
    assert "def describe(verbose: bool = False) -> str" in text
    assert "undocumented" not in text
    assert "def helper(value: int, *, scale: float = 1.0) -> float" in text
    assert "_private" not in text
