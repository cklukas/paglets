# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from typing import Annotated

import typer

from paglets.config.env import DEFAULT_API_KEY_ENV
from paglets.examples.search import cli as legacy_search

app = typer.Typer(help="Search files across a Paglets mesh.", no_args_is_help=True)


def _global_args(
    entry: str | None,
    host: list[str] | None,
    timeout: float,
    poll_interval: float,
    json_output: bool,
    jsonl: bool,
    no_stream: bool,
    api_key_env: str | None,
) -> list[str]:
    argv: list[str] = []
    if entry:
        argv.extend(["--entry", entry])
    for value in host or []:
        argv.extend(["--host", value])
    argv.extend(["--timeout", str(timeout), "--poll-interval", str(poll_interval), "--color", "auto"])
    if json_output:
        argv.append("--json")
    if jsonl:
        argv.append("--jsonl")
    if no_stream:
        argv.append("--no-stream")
    if api_key_env:
        argv.extend(["--api-key-env", api_key_env])
    return argv


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def grep(
    ctx: typer.Context,
    pattern: Annotated[str, typer.Argument(help="Pattern to search for.")],
    paths: Annotated[list[str] | None, typer.Argument(help="Paths to search on each target host.")] = None,
    ignore_case: Annotated[bool, typer.Option("-i", "--ignore-case", help="Case-insensitive search.")] = False,
    smart_case: Annotated[
        bool, typer.Option("-S", "--smart-case", help="Case-insensitive unless the pattern has uppercase letters.")
    ] = False,
    fixed_strings: Annotated[
        bool, typer.Option("-F", "--fixed-strings", help="Treat the pattern as a literal string.")
    ] = False,
    word_regexp: Annotated[bool, typer.Option("-w", "--word-regexp", help="Only match whole words.")] = False,
    glob: Annotated[list[str] | None, typer.Option("-g", "--glob", help="Include or exclude paths.")] = None,
    type_name: Annotated[
        list[str] | None, typer.Option("-t", "--type", help="Only search a supported file type.")
    ] = None,
    hidden: Annotated[bool, typer.Option("--hidden", help="Search hidden files and directories.")] = False,
    no_ignore: Annotated[bool, typer.Option("--no-ignore", help="Do not use ignore files.")] = False,
    after_context: Annotated[int, typer.Option("-A", "--after-context", help="Print lines after each match.")] = 0,
    before_context: Annotated[int, typer.Option("-B", "--before-context", help="Print lines before each match.")] = 0,
    context: Annotated[
        int | None, typer.Option("-C", "--context", help="Print lines before and after each match.")
    ] = None,
    line_number: Annotated[
        bool, typer.Option("-n", "--line-number/--no-line-number", help="Print line numbers.")
    ] = True,
    count: Annotated[bool, typer.Option("-c", "--count", help="Print matching-line counts per file.")] = False,
    files_with_matches: Annotated[
        bool, typer.Option("-l", "--files-with-matches", help="Print only paths with matches.")
    ] = False,
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    host: Annotated[list[str] | None, typer.Option("--host", help="Restrict search to a host name or URL.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Seconds to wait for mesh replies.")] = 60.0,
    poll_interval: Annotated[float, typer.Option("--poll-interval", help="Seconds each drain call may wait.")] = 0.5,
    json_output: Annotated[bool, typer.Option("--json", help="Print final summary JSON.")] = False,
    jsonl: Annotated[bool, typer.Option("--jsonl", help="Stream event JSON lines.")] = False,
    no_stream: Annotated[bool, typer.Option("--no-stream", help="Buffer events and print after completion.")] = False,
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help=f"API key environment variable; defaults to {DEFAULT_API_KEY_ENV}."),
    ] = None,
) -> None:
    argv = _global_args(entry, host, timeout, poll_interval, json_output, jsonl, no_stream, api_key_env)
    options = _common_search_args(
        ignore_case, smart_case, fixed_strings, word_regexp, glob, type_name, hidden, no_ignore
    )
    if after_context:
        options.extend(["--after-context", str(after_context)])
    if before_context:
        options.extend(["--before-context", str(before_context)])
    if context is not None:
        options.extend(["--context", str(context)])
    options.append("--line-number" if line_number else "--no-line-number")
    if count:
        options.append("--count")
    if files_with_matches:
        options.append("--files-with-matches")
    argv.extend(["grep", *options, pattern, *(paths or []), *ctx.args])
    raise typer.Exit(legacy_search.main(argv))


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def find(
    ctx: typer.Context,
    pattern: Annotated[str, typer.Argument(help="Name pattern; omit for every matching path.")] = "",
    paths: Annotated[list[str] | None, typer.Argument(help="Paths to search on each target host.")] = None,
    ignore_case: Annotated[bool, typer.Option("-i", "--ignore-case", help="Case-insensitive search.")] = False,
    smart_case: Annotated[
        bool, typer.Option("-S", "--smart-case", help="Case-insensitive unless the pattern has uppercase letters.")
    ] = False,
    fixed_strings: Annotated[
        bool, typer.Option("-F", "--fixed-strings", help="Treat the pattern as a literal string.")
    ] = False,
    word_regexp: Annotated[bool, typer.Option("-w", "--word-regexp", help="Only match whole words.")] = False,
    glob: Annotated[list[str] | None, typer.Option("-g", "--glob", help="Include or exclude paths.")] = None,
    type_name: Annotated[
        list[str] | None, typer.Option("-t", "--type", help="Only search a supported file type.")
    ] = None,
    hidden: Annotated[bool, typer.Option("--hidden", help="Search hidden files and directories.")] = False,
    no_ignore: Annotated[bool, typer.Option("--no-ignore", help="Do not use ignore files.")] = False,
    full_path: Annotated[bool, typer.Option("--full-path", help="Match against the full path.")] = False,
    extension: Annotated[list[str] | None, typer.Option("-e", "--extension", help="Limit to extension.")] = None,
    kind: Annotated[str, typer.Option("--kind", help="Path kind to emit: any, file, dir, symlink.")] = "any",
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    host: Annotated[list[str] | None, typer.Option("--host", help="Restrict search to a host name or URL.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Seconds to wait for mesh replies.")] = 60.0,
    poll_interval: Annotated[float, typer.Option("--poll-interval", help="Seconds each drain call may wait.")] = 0.5,
    json_output: Annotated[bool, typer.Option("--json", help="Print final summary JSON.")] = False,
    jsonl: Annotated[bool, typer.Option("--jsonl", help="Stream event JSON lines.")] = False,
    no_stream: Annotated[bool, typer.Option("--no-stream", help="Buffer events and print after completion.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    argv = _global_args(entry, host, timeout, poll_interval, json_output, jsonl, no_stream, api_key_env)
    options = _common_search_args(
        ignore_case, smart_case, fixed_strings, word_regexp, glob, type_name, hidden, no_ignore
    )
    if full_path:
        options.append("--full-path")
    for value in extension or []:
        options.extend(["--extension", value])
    options.extend(["--kind", kind])
    argv.extend(["find", *options])
    if pattern:
        argv.append(pattern)
    argv.extend(paths or [])
    argv.extend(ctx.args)
    raise typer.Exit(legacy_search.main(argv))


def _common_search_args(
    ignore_case: bool,
    smart_case: bool,
    fixed_strings: bool,
    word_regexp: bool,
    glob: list[str] | None,
    type_name: list[str] | None,
    hidden: bool,
    no_ignore: bool,
) -> list[str]:
    argv: list[str] = []
    if ignore_case:
        argv.append("--ignore-case")
    if smart_case:
        argv.append("--smart-case")
    if fixed_strings:
        argv.append("--fixed-strings")
    if word_regexp:
        argv.append("--word-regexp")
    for value in glob or []:
        argv.extend(["--glob", value])
    for value in type_name or []:
        argv.extend(["--type", value])
    if hidden:
        argv.append("--hidden")
    if no_ignore:
        argv.append("--no-ignore")
    return argv
