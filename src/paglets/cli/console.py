# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
import sys
from collections.abc import Iterable, Sequence
from dataclasses import asdict, is_dataclass
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

console = Console()
err_console = Console(stderr=True)


def configure_console(*, no_color: bool = False) -> None:
    global console, err_console
    console = Console(no_color=no_color or not sys.stdout.isatty())
    err_console = Console(stderr=True, no_color=no_color or not sys.stderr.isatty())


def print_json(payload: Any) -> None:
    console.print_json(json.dumps(_jsonable(payload), sort_keys=True))


def print_error(command: str, exc: Exception) -> None:
    err_console.print(f"[bold red]{command}:[/bold red] {exc}")


def fail(command: str, exc: Exception, *, code: int = 2) -> None:
    print_error(command, exc)
    raise typer.Exit(code)


def table(
    title: str | None,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    right: set[str] | None = None,
) -> Table:
    result = Table(title=title, show_lines=False)
    right = right or set()
    for column in columns:
        result.add_column(column, justify="right" if column in right else "left", no_wrap=True)
    for row in rows:
        result.add_row(*(str(value) for value in row))
    return result


def print_table(
    title: str | None,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    right: set[str] | None = None,
) -> None:
    console.print(table(title, columns, rows, right=right))


def bytes_text(value: int) -> str:
    amount = float(max(0, int(value)))
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if amount < 1024.0 or unit == "PB":
            if unit == "B":
                return f"{int(amount)} B"
            return f"{amount:.1f} {unit}"
        amount /= 1024.0
    return f"{amount:.1f} PB"


def duration_text(seconds: float) -> str:
    value = max(0.0, float(seconds))
    if value < 60:
        return f"{value:.1f}s"
    if value < 3600:
        return f"{value / 60:.1f}m"
    return f"{value / 3600:.1f}h"


def _jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value
