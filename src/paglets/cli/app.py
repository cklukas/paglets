# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from typing import Annotated

import typer

from . import artifacts, examples, host, jobs, mesh, search, system
from .console import configure_console

app = typer.Typer(
    name="paglets",
    help="Run and operate Paglets hosts, jobs, artifacts, mesh resources, and examples.",
    no_args_is_help=True,
)

app.add_typer(host.app, name="host")
app.add_typer(system.app, name="sys")
app.add_typer(mesh.app, name="mesh")
app.add_typer(jobs.app, name="jobs")
app.add_typer(artifacts.app, name="artifacts")
app.add_typer(search.app, name="search")
app.add_typer(examples.app, name="examples")


@app.callback()
def root(
    no_color: Annotated[bool, typer.Option("--no-color", help="Disable terminal colors and styled output.")] = False,
) -> None:
    configure_console(no_color=no_color)


def main() -> None:
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
