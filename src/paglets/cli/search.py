# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.examples.search import cli as search_runtime
from paglets.examples.search.models import SearchRequest
from paglets.remote.admin import select_reachable_entry_server
from paglets.remote.client import HostClient

from .console import fail, print_json

app = typer.Typer(help="Search files across a Paglets mesh.", no_args_is_help=True)


@app.command()
def grep(
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
    output: Annotated[Path, typer.Option("--output", help="Output JSONL file on the entry host.")] = Path(
        "search.jsonl"
    ),
    json_output: Annotated[bool, typer.Option("--json", help="Print submission metadata as JSON.")] = False,
    jsonl: Annotated[bool, typer.Option("--jsonl", help="Write event JSON lines to the output file.")] = False,
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help=f"API key environment variable; defaults to {DEFAULT_API_KEY_ENV}."),
    ] = None,
) -> None:
    request = _request(
        "grep",
        pattern,
        paths,
        ignore_case,
        smart_case,
        fixed_strings,
        word_regexp,
        glob,
        type_name,
        hidden,
        no_ignore,
        before_context=before_context,
        after_context=after_context,
        context=context,
        line_number=line_number,
        count=count,
        files_with_matches=files_with_matches,
    )
    _run(request, entry, host, timeout, output, json_output, jsonl, api_key_env)


@app.command()
def find(
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
    output: Annotated[Path, typer.Option("--output", help="Output JSONL file on the entry host.")] = Path(
        "search.jsonl"
    ),
    json_output: Annotated[bool, typer.Option("--json", help="Print submission metadata as JSON.")] = False,
    jsonl: Annotated[bool, typer.Option("--jsonl", help="Write event JSON lines to the output file.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    request = _request(
        "find",
        pattern,
        paths,
        ignore_case,
        smart_case,
        fixed_strings,
        word_regexp,
        glob,
        type_name,
        hidden,
        no_ignore,
        full_path=full_path,
        extensions=extension,
        kind=kind,
    )
    _run(request, entry, host, timeout, output, json_output, jsonl, api_key_env)


def _request(
    mode: str,
    pattern: str,
    paths: list[str] | None,
    ignore_case: bool,
    smart_case: bool,
    fixed_strings: bool,
    word_regexp: bool,
    glob: list[str] | None,
    type_name: list[str] | None,
    hidden: bool,
    no_ignore: bool,
    *,
    before_context: int = 0,
    after_context: int = 0,
    context: int | None = None,
    line_number: bool = True,
    count: bool = False,
    files_with_matches: bool = False,
    full_path: bool = False,
    extensions: list[str] | None = None,
    kind: str = "any",
) -> SearchRequest:
    if context is not None:
        before_context = context
        after_context = context
    return SearchRequest(
        mode=mode,
        pattern=pattern,
        paths=list(paths or ["."]),
        ignore_case=ignore_case,
        smart_case=smart_case,
        fixed_strings=fixed_strings,
        word_regexp=word_regexp,
        before_context=max(0, before_context),
        after_context=max(0, after_context),
        line_number=line_number,
        count=count,
        files_with_matches=files_with_matches,
        globs=list(glob or []),
        type_names=list(type_name or []),
        hidden=hidden,
        no_ignore=no_ignore,
        full_path=full_path,
        extensions=list(extensions or []),
        kind=kind,
    )


def _run(
    request: SearchRequest,
    entry: str | None,
    host: list[str] | None,
    timeout: float,
    output: Path,
    json_output: bool,
    jsonl: bool,
    api_key_env: str | None,
) -> None:
    try:
        client = HostClient(timeout=max(1.0, timeout + 5.0), api_key=resolve_api_key(api_key_env))
        entry_ref = select_reachable_entry_server(entry_name=entry, client=client)
        args = _SearchRunOptions(
            host=list(host or []),
            timeout=timeout,
            output=str(output),
            json=json_output,
            jsonl=jsonl,
            color="auto",
        )
        reply = search_runtime._submit_search(entry_ref, request, args, client=client)
        if json_output:
            print_json(reply)
        else:
            from .console import console

            console.print(f"submitted {reply['job_id']} on {reply['host_url']} output={reply['output_path']}")
        raise typer.Exit(0)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets search", exc)


class _SearchRunOptions:
    def __init__(
        self,
        *,
        host: list[str],
        timeout: float,
        output: str,
        json: bool,
        jsonl: bool,
        color: str,
    ) -> None:
        self.host = host
        self.timeout = timeout
        self.output = output
        self.json = json
        self.jsonl = jsonl
        self.color = color
