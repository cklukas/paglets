# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from ...admin import (
    PagletsAdminClient,
    ServerRef,
    select_reachable_entry_server,
)
from ...client import HostClient
from ...messages import Message
from ...proxy import PagletProxy
from ...serde import dataclass_from_wire, dataclass_to_wire
from .agent import (
    DEFAULT_DRAIN_WAIT_SECONDS,
    DEFAULT_SEARCH_TIMEOUT_SECONDS,
    SEARCH_TYPES,
    HostSearchSummary,
    SearchEvent,
    SearchRequest,
)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.type_list:
        _print_type_list()
        return 0
    if args.command is None:
        parser.error("a command is required unless --type-list is used")

    try:
        client = HostClient(timeout=max(1.0, args.timeout + 5.0))
        entry = _select_entry_server(entry_name=args.entry, client=client)
        request = _search_request(args)
        summary, events = _run_search(entry, request, args, client=client)
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        elif args.no_stream:
            use_color = _use_color(args)
            for event in events:
                _print_event(event, use_color=use_color)
            _print_summary_notes(summary)
        return 0 if _has_hits(summary) and not _has_failures(summary) else 1
    except Exception as exc:
        print(f"paglets-search: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search files across a paglets mesh with mobile agents")
    parser.add_argument("--entry", default=None, help="Discovered entry host name")
    parser.add_argument("--host", action="append", default=[], help="Restrict search to a mesh host name or URL; repeatable")
    parser.add_argument("--timeout", type=float, default=DEFAULT_SEARCH_TIMEOUT_SECONDS, help="Seconds to wait for mesh replies")
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_DRAIN_WAIT_SECONDS, help="Seconds each drain call may wait for new events")
    parser.add_argument("--drain-limit", type=int, default=200, help=argparse.SUPPRESS)
    parser.add_argument("--type-list", action="store_true", help="List supported file type filters and exit")
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--json", action="store_true", help="Print final machine-readable summary JSON")
    output.add_argument("--jsonl", action="store_true", help="Stream machine-readable event JSON lines")
    parser.add_argument("--no-stream", action="store_true", help="Buffer events and print them after completion")
    parser.add_argument("--color", choices=("auto", "always", "never"), default="auto", help="Highlight text matches")

    subparsers = parser.add_subparsers(dest="command")
    grep = subparsers.add_parser("grep", help="Search file contents")
    grep.add_argument("pattern", help="Pattern to search for")
    grep.add_argument("paths", nargs="*", help="Paths to search on each target host")
    _add_common_search_options(grep)
    _add_grep_options(grep)

    find = subparsers.add_parser("find", help="Search file and directory names")
    find.add_argument("pattern", nargs="?", default="", help="Name pattern; omitted means every matching path")
    find.add_argument("paths", nargs="*", help="Paths to search on each target host")
    _add_common_search_options(find)
    find.add_argument("--full-path", action="store_true", help="Match against the full path instead of the basename")
    find.add_argument("-e", "--extension", action="append", default=[], help="Limit to extension; repeatable")
    find.add_argument("--kind", choices=("any", "file", "dir", "symlink"), default="any", help="Path kind to emit")
    return parser


def _add_common_search_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-i", "--ignore-case", action="store_true", help="Case-insensitive search")
    parser.add_argument("-S", "--smart-case", action="store_true", help="Case-insensitive unless the pattern has uppercase letters")
    parser.add_argument("-F", "--fixed-strings", action="store_true", help="Treat the pattern as a literal string")
    parser.add_argument("-w", "--word-regexp", action="store_true", help="Only match whole words")
    parser.add_argument("-g", "--glob", dest="globs", action="append", default=[], help="Include or exclude paths; prefix with ! to exclude")
    parser.add_argument("-t", "--type", dest="type_names", action="append", default=[], help="Only search a supported file type")
    parser.add_argument("-T", "--type-not", dest="type_not_names", action="append", default=[], help="Exclude a supported file type")
    parser.add_argument("--hidden", action="store_true", help="Search hidden files and directories")
    parser.add_argument("--no-ignore", action="store_true", help="Do not use .gitignore, .ignore, or .fdignore files")
    parser.add_argument("--ignore-file", dest="ignore_files", action="append", default=[], help="Additional ignore file name to read in each directory")
    parser.add_argument("--follow", action="store_true", help="Follow symbolic links while traversing")
    parser.add_argument("--max-depth", type=int, default=None, help="Maximum directory depth below each root")
    parser.add_argument("--max-file-size", default="0", help="Skip content files larger than this size, e.g. 10M; 0 disables")
    parser.add_argument("--encoding", default="utf-8", help="Text encoding for content search")
    parser.add_argument("--text", action="store_true", help="Search binary-looking files as text")
    parser.add_argument("--absolute-path", action="store_true", help="Print absolute paths")
    parser.add_argument("--max-results-per-host", type=int, default=0, help="Stop emitting after this many hits per host")


def _add_grep_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-A", "--after-context", type=int, default=0, help="Print lines after each match")
    parser.add_argument("-B", "--before-context", type=int, default=0, help="Print lines before each match")
    parser.add_argument("-C", "--context", type=int, default=None, help="Print lines before and after each match")
    parser.add_argument("-n", "--line-number", action=argparse.BooleanOptionalAction, default=True, help="Print line numbers")
    parser.add_argument("--column", action="store_true", help="Print first match column")
    parser.add_argument("-o", "--only-matching", action="store_true", help="Print only matching text")
    parser.add_argument("-c", "--count", action="store_true", help="Print matching-line counts per file")
    parser.add_argument("-l", "--files-with-matches", action="store_true", help="Print only paths with matches")
    parser.add_argument("--files-without-match", action="store_true", help="Print only paths without matches")
    parser.add_argument("-m", "--max-count", type=int, default=0, help="Stop reading a file after this many matching lines")


def _search_request(args: argparse.Namespace) -> SearchRequest:
    context = getattr(args, "context", None)
    before_context = getattr(args, "before_context", 0)
    after_context = getattr(args, "after_context", 0)
    if context is not None:
        before_context = context
        after_context = context
    return SearchRequest(
        mode=args.command,
        pattern=args.pattern,
        paths=list(args.paths or ["."]),
        ignore_case=bool(args.ignore_case),
        smart_case=bool(args.smart_case),
        fixed_strings=bool(args.fixed_strings),
        word_regexp=bool(args.word_regexp),
        before_context=max(0, int(before_context or 0)),
        after_context=max(0, int(after_context or 0)),
        line_number=bool(getattr(args, "line_number", True)),
        column=bool(getattr(args, "column", False)),
        only_matching=bool(getattr(args, "only_matching", False)),
        count=bool(getattr(args, "count", False)),
        files_with_matches=bool(getattr(args, "files_with_matches", False)),
        files_without_match=bool(getattr(args, "files_without_match", False)),
        max_count=max(0, int(getattr(args, "max_count", 0) or 0)),
        globs=list(args.globs or []),
        type_names=list(args.type_names or []),
        type_not_names=list(args.type_not_names or []),
        hidden=bool(args.hidden),
        no_ignore=bool(args.no_ignore),
        follow=bool(args.follow),
        max_depth=None if args.max_depth is None else max(0, int(args.max_depth)),
        max_file_size=parse_size(args.max_file_size),
        encoding=str(args.encoding or "utf-8"),
        text=bool(args.text),
        absolute_path=bool(args.absolute_path),
        max_results_per_host=max(0, int(args.max_results_per_host or 0)),
        full_path=bool(getattr(args, "full_path", False)),
        extensions=list(getattr(args, "extension", []) or []),
        kind=str(getattr(args, "kind", "any")),
        ignore_files=list(args.ignore_files or []),
    )


def _select_entry_server(*, entry_name: str | None, client: HostClient) -> ServerRef:
    return select_reachable_entry_server(
        entry_name=entry_name,
        client=client,
    )


def _run_search(
    entry: ServerRef,
    request: SearchRequest,
    args: argparse.Namespace,
    *,
    client: HostClient,
) -> tuple[dict[str, Any], list[SearchEvent]]:
    admin = PagletsAdminClient([entry], client=client)
    proxy_wire = admin.create_agent(
        entry,
        "paglets.examples.search.agent:MeshSearchAgent",
        "paglets.examples.search.agent:MeshSearchState",
        {},
    )
    proxy = PagletProxy.from_wire(proxy_wire, client)
    events: list[SearchEvent] = []
    try:
        proxy.send(
            Message(
                "start",
                {
                    "request": dataclass_to_wire(request),
                    "targets": list(args.host),
                    "timeout": max(0.0, float(args.timeout)),
                },
            )
        )
        cursor = 0
        use_color = _use_color(args)
        while True:
            reply = proxy.send(
                Message(
                    "drain",
                    {
                        "after_cursor": cursor,
                        "wait_timeout": max(0.0, float(args.poll_interval)),
                        "limit": max(1, int(args.drain_limit)),
                    },
                )
            )
            for event_wire in reply.get("events") or []:
                event = dataclass_from_wire(SearchEvent, event_wire)
                events.append(event)
                cursor = max(cursor, event.cursor)
                if args.jsonl:
                    print(json.dumps(dataclass_to_wire(event), sort_keys=True))
                elif not args.json and not args.no_stream:
                    _print_event(event, use_color=use_color)
            if reply.get("done"):
                return dict(reply.get("summary") or {}), events
    finally:
        try:
            proxy.send(Message("cleanup"))
        except Exception:
            pass
        try:
            proxy.dispose()
        except Exception:
            pass


def _print_event(event: SearchEvent, *, use_color: bool) -> None:
    if event.event == "file":
        print(f"{event.host_name}:{event.path}")
        return
    if event.event == "count":
        print(f"{event.host_name}:{event.path}:{event.count}")
        return
    if event.event == "error":
        print(f"{event.host_name}: error: {event.message}", file=sys.stderr)
        return
    if event.event not in {"match", "context"}:
        return

    sep = "-" if event.event == "context" else ":"
    parts = [event.host_name, event.path]
    if event.line_number:
        parts.append(str(event.line_number))
    if event.column:
        parts.append(str(event.column))
    text = _highlight(event.text, event.match_start, event.match_end, use_color=use_color) if event.event == "match" else event.text
    print(sep.join(parts) + sep + text)


def _highlight(text: str, start: int, end: int, *, use_color: bool) -> str:
    if not use_color or end <= start:
        return text
    return f"{text[:start]}\033[1;31m{text[start:end]}\033[0m{text[end:]}"


def _print_summary_notes(summary: dict[str, Any]) -> None:
    errors = summary.get("errors") or {}
    cleanup_errors = summary.get("cleanup_errors") or {}
    if not errors and not cleanup_errors:
        return
    print("\nnotes:", file=sys.stderr)
    for host, error in sorted(errors.items()):
        print(f"  - {host}: {error}", file=sys.stderr)
    for host, error in sorted(cleanup_errors.items()):
        print(f"  - {host}: cleanup failed: {error}", file=sys.stderr)


def _has_hits(summary: dict[str, Any]) -> bool:
    for item in (summary.get("results") or {}).values():
        result = dataclass_from_wire(HostSearchSummary, item)
        if result.matches > 0 or result.paths_matched > 0:
            return True
    return False


def _has_failures(summary: dict[str, Any]) -> bool:
    if summary.get("errors") or summary.get("cleanup_errors"):
        return True
    for item in (summary.get("results") or {}).values():
        result = dataclass_from_wire(HostSearchSummary, item)
        if result.errors:
            return True
    return False


def _use_color(args: argparse.Namespace) -> bool:
    if args.color == "always":
        return True
    if args.color == "never" or args.json or args.jsonl:
        return False
    return sys.stdout.isatty()


def _print_type_list() -> None:
    for name, globs in sorted(SEARCH_TYPES.items()):
        print(f"{name}: {', '.join(globs)}")


def parse_size(value: str) -> int:
    text = str(value).strip()
    if not text or text == "0":
        return 0
    unit = text[-1].upper()
    if unit in {"K", "M", "G"}:
        number = text[:-1]
        multiplier = {"K": 1024, "M": 1024**2, "G": 1024**3}[unit]
    else:
        number = text[:-1] if unit == "B" else text
        multiplier = 1
    try:
        amount = float(number)
    except ValueError as exc:
        raise ValueError(f"invalid size {value!r}") from exc
    if amount < 0:
        raise ValueError("size must be non-negative")
    return int(amount * multiplier)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
