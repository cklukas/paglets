# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from typing import Annotated

import typer

from paglets.config.env import DEFAULT_API_KEY_ENV
from paglets.examples.analysis_jobs import cli as analysis_cli
from paglets.examples.compute import cli as pi_cli
from paglets.examples.file_grabber import cli as file_cli
from paglets.examples.mesh_benchmark import cli as mesh_benchmark_cli
from paglets.examples.performance import cli as perf_cli

app = typer.Typer(help="Run packaged Paglets examples.", no_args_is_help=True)
file_app = typer.Typer(help="Copy or move one file between two hosts.", no_args_is_help=True)
app.add_typer(file_app, name="file")


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def pi(
    ctx: typer.Context,
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    start: Annotated[int, typer.Option("--start", help="Zero-based decimal digit position after the point.")] = 0,
    digits: Annotated[int, typer.Option("--digits", help="Number of decimal digits to compute.")] = 16,
    batch_size: Annotated[int, typer.Option("--batch-size", help="Chudnovsky terms per worker batch.")] = 1,
    max_in_flight: Annotated[int, typer.Option("--max-in-flight", help="Global in-flight batch cap.")] = 0,
    max_workers_per_host: Annotated[int, typer.Option("--max-workers-per-host", help="Per-host worker cap.")] = 0,
    timeout: Annotated[float, typer.Option("--timeout", help="Whole-job timeout in seconds; 0 disables it.")] = 0.0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help=f"API key environment variable; defaults to {DEFAULT_API_KEY_ENV}."),
    ] = None,
) -> None:
    argv = [
        *_base(entry, api_key_env),
        "--start",
        str(start),
        "--digits",
        str(digits),
        "--batch-size",
        str(batch_size),
        "--max-in-flight",
        str(max_in_flight),
        "--max-workers-per-host",
        str(max_workers_per_host),
        "--timeout",
        str(timeout),
    ]
    if json_output:
        argv.append("--json")
    argv.extend(ctx.args)
    raise typer.Exit(pi_cli.main(argv))


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def analysis(
    ctx: typer.Context,
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry/home host name.")] = None,
    tasks: Annotated[int, typer.Option("--tasks", help="Number of analysis jobs.")] = 8,
    db: Annotated[str | None, typer.Option("--db", help="SQLite result DB path on the home host.")] = None,
    rows: Annotated[int, typer.Option("--rows", help="Synthetic rows per job.")] = 10000,
    features: Annotated[int, typer.Option("--features", help="Synthetic features per job.")] = 16,
    trees: Annotated[int, typer.Option("--trees", help="Random forest tree count.")] = 80,
    wait: Annotated[float, typer.Option("--wait", help="Seconds to wait for seeder completion.")] = 5.0,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    argv = [
        *_base(entry, api_key_env),
        "--tasks",
        str(tasks),
        "--rows",
        str(rows),
        "--features",
        str(features),
        "--trees",
        str(trees),
        "--wait",
        str(wait),
    ]
    if db is not None:
        argv.extend(["--db", db])
    argv.extend(ctx.args)
    raise typer.Exit(analysis_cli.main(argv))


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def perf(
    ctx: typer.Context,
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Seconds to wait for replies.")] = 120.0,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    verbose: Annotated[bool, typer.Option("--verbose", help="Print skipped disk targets and diagnostics.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    argv = [*_base(entry, api_key_env), "--timeout", str(timeout)]
    if json_output:
        argv.append("--json")
    if verbose:
        argv.append("--verbose")
    argv.extend(ctx.args)
    raise typer.Exit(perf_cli.main(argv))


@app.command("mesh-benchmark", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def mesh_benchmark(
    ctx: typer.Context,
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Seconds to wait for completion.")] = 60.0,
    repeats: Annotated[int, typer.Option("--repeats", help="Repeat the directed mesh route this many times.")] = 1,
    payload_size: Annotated[str, typer.Option("--payload-size", help="Random ASCII payload size, e.g. 64K.")] = "0",
    exclude_self: Annotated[bool, typer.Option("--exclude-self", help="Skip self-pair movements.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    argv = [
        *_base(entry, api_key_env),
        "--timeout",
        str(timeout),
        "--repeats",
        str(repeats),
        "--payload-size",
        payload_size,
    ]
    if exclude_self:
        argv.append("--exclude-self")
    if json_output:
        argv.append("--json")
    argv.extend(ctx.args)
    raise typer.Exit(mesh_benchmark_cli.main(argv))


@file_app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def push(
    ctx: typer.Context,
    source: Annotated[str, typer.Argument(help="Source file path on the entry host.")],
    remote: Annotated[str, typer.Option("--remote", help="Remote host name or URL.")],
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry/start host name.")] = None,
    dest: Annotated[str, typer.Option("--dest", help="Destination path; defaults to source basename.")] = "",
    mode: Annotated[str, typer.Option("--mode", help="Transfer mode: copy or move.")] = "copy",
    dry: Annotated[bool, typer.Option("--dry", help="Only stat the source and report the plan.")] = False,
    overwrite: Annotated[bool, typer.Option("--overwrite", help="Replace the destination if it exists.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    raise typer.Exit(
        _file("push", source, remote, entry, dest, mode, dry, overwrite, json_output, api_key_env, ctx.args)
    )


@file_app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def pull(
    ctx: typer.Context,
    source: Annotated[str, typer.Argument(help="Source file path on the remote host.")],
    remote: Annotated[str, typer.Option("--remote", help="Remote host name or URL.")],
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry/start host name.")] = None,
    dest: Annotated[str, typer.Option("--dest", help="Destination path; defaults to source basename.")] = "",
    mode: Annotated[str, typer.Option("--mode", help="Transfer mode: copy or move.")] = "copy",
    dry: Annotated[bool, typer.Option("--dry", help="Only stat the source and report the plan.")] = False,
    overwrite: Annotated[bool, typer.Option("--overwrite", help="Replace the destination if it exists.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    raise typer.Exit(
        _file("pull", source, remote, entry, dest, mode, dry, overwrite, json_output, api_key_env, ctx.args)
    )


def _file(
    command: str,
    source: str,
    remote: str,
    entry: str | None,
    dest: str,
    mode: str,
    dry: bool,
    overwrite: bool,
    json_output: bool,
    api_key_env: str | None,
    extra: list[str],
) -> int:
    argv = [*_base(entry, api_key_env), command, source, "--remote", remote, "--mode", mode]
    if dest:
        argv.extend(["--dest", dest])
    if dry:
        argv.append("--dry")
    if overwrite:
        argv.append("--overwrite")
    if json_output:
        argv.append("--json")
    argv.extend(extra)
    return file_cli.main(argv)


def _base(entry: str | None, api_key_env: str | None) -> list[str]:
    argv: list[str] = []
    if entry:
        argv.extend(["--entry", entry])
    if api_key_env:
        argv.extend(["--api-key-env", api_key_env])
    return argv
