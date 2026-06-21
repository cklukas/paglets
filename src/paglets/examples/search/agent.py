# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import fnmatch
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, Iterable

import pathspec

from ...agent import Paglet, PagletState, state_locked
from ...messages import Message
from ...proxy import PagletProxy
from ...serde import dataclass_from_wire, dataclass_to_wire


DEFAULT_SEARCH_TIMEOUT_SECONDS = 60.0
DEFAULT_DRAIN_WAIT_SECONDS = 0.5
DEFAULT_BATCH_SIZE = 20

SEARCH_TYPES: dict[str, tuple[str, ...]] = {
    "c": ("*.c", "*.h"),
    "cpp": ("*.cc", "*.cpp", "*.cxx", "*.hh", "*.hpp", "*.hxx"),
    "css": ("*.css",),
    "go": ("*.go",),
    "html": ("*.html", "*.htm"),
    "java": ("*.java",),
    "js": ("*.js", "*.jsx", "*.mjs", "*.cjs"),
    "json": ("*.json",),
    "md": ("*.md", "*.markdown"),
    "py": ("*.py",),
    "python": ("*.py",),
    "rs": ("*.rs",),
    "sh": ("*.sh", "*.bash", "*.zsh"),
    "text": ("*.txt", "*.text"),
    "toml": ("*.toml",),
    "ts": ("*.ts", "*.tsx"),
    "txt": ("*.txt",),
    "yaml": ("*.yaml", "*.yml"),
}

_IGNORE_FILES = (".gitignore", ".ignore", ".fdignore")


@dataclass(frozen=True, slots=True)
class SearchRequest:
    mode: str = "grep"
    pattern: str = ""
    paths: list[str] = field(default_factory=lambda: ["."])
    ignore_case: bool = False
    smart_case: bool = False
    fixed_strings: bool = False
    word_regexp: bool = False
    before_context: int = 0
    after_context: int = 0
    line_number: bool = True
    column: bool = False
    only_matching: bool = False
    count: bool = False
    files_with_matches: bool = False
    files_without_match: bool = False
    max_count: int = 0
    globs: list[str] = field(default_factory=list)
    type_names: list[str] = field(default_factory=list)
    type_not_names: list[str] = field(default_factory=list)
    hidden: bool = False
    no_ignore: bool = False
    follow: bool = False
    max_depth: int | None = None
    max_file_size: int = 0
    encoding: str = "utf-8"
    text: bool = False
    absolute_path: bool = False
    max_results_per_host: int = 0
    full_path: bool = False
    extensions: list[str] = field(default_factory=list)
    kind: str = "any"
    ignore_files: list[str] = field(default_factory=list)
    batch_size: int = DEFAULT_BATCH_SIZE


@dataclass(frozen=True, slots=True)
class SearchEvent:
    event: str
    host_name: str
    host_url: str
    path: str = ""
    line_number: int = 0
    column: int = 0
    text: str = ""
    match_text: str = ""
    match_start: int = 0
    match_end: int = 0
    context: str = ""
    count: int = 0
    message: str = ""
    cursor: int = 0
    sequence: int = 0
    truncated: bool = False


@dataclass(frozen=True, slots=True)
class HostSearchSummary:
    host_name: str
    host_url: str
    files_seen: int = 0
    files_searched: int = 0
    files_matched: int = 0
    matches: int = 0
    paths_matched: int = 0
    errors: list[str] = field(default_factory=list)
    truncated: bool = False
    duration_seconds: float = 0.0


@dataclass
class MeshSearchState(PagletState):
    role: str = "parent"
    request: dict[str, Any] = field(default_factory=dict)
    timeout: float = DEFAULT_SEARCH_TIMEOUT_SECONDS
    deadline: float = 0.0
    parent_host_url: str = ""
    parent_agent_id: str = ""
    target_host_name: str = ""
    target_host_url: str = ""
    requested_targets: list[str] = field(default_factory=list)
    pending_hosts: list[str] = field(default_factory=list)
    done_hosts: list[str] = field(default_factory=list)
    children: dict[str, dict[str, str]] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: int = 1
    summaries: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)
    started: bool = False


class MeshSearchAgent(Paglet[MeshSearchState]):
    """Clone across the mesh and stream local filesystem search hits."""

    State = MeshSearchState

    def run(self) -> None:
        with self.locked_state() as state:
            is_child = state.role == "child"
        if is_child:
            thread = threading.Thread(
                target=self._run_child,
                name=f"paglets-search-{self.context.name}",
                daemon=True,
            )
            thread.start()

    def handle_message(self, message: Message):
        if message.kind == "start":
            with self.locked_state() as state:
                state.request = dict(message.args.get("request") or {})
                state.timeout = float(message.args.get("timeout", DEFAULT_SEARCH_TIMEOUT_SECONDS))
                state.requested_targets = [str(item) for item in message.args.get("targets") or []]
            return self.start()
        if message.kind == "child_events":
            return self.record_child_events(message.args)
        if message.kind == "child_done":
            return self.record_child_done(message.args)
        if message.kind == "drain":
            return self.drain(
                after_cursor=int(message.args.get("after_cursor", 0)),
                wait_timeout=float(message.args.get("wait_timeout", DEFAULT_DRAIN_WAIT_SECONDS)),
                limit=int(message.args.get("limit", 200)),
            )
        if message.kind == "summary":
            return self.summary()
        if message.kind == "cleanup":
            return self.cleanup_children()
        return self.not_handled()

    def start(self) -> dict[str, Any]:
        with self.locked_state() as state:
            state.role = "parent"
            state.parent_host_url = self.context.address
            state.parent_agent_id = self.agent_id
            state.pending_hosts = []
            state.done_hosts = []
            state.children = {}
            state.events = []
            state.next_cursor = 1
            state.summaries = {}
            state.errors = {}
            state.cleanup_errors = {}
            state.started = True
            state.deadline = time.monotonic() + max(0.0, state.timeout)
            requested_targets = list(state.requested_targets)
        hosts = self._target_hosts(requested_targets)
        for host in hosts:
            with self.locked_state() as state:
                state.pending_hosts.append(host.name)
                state.role = "child"
                state.target_host_name = host.name
                state.target_host_url = host.url
            try:
                child = self.clone_to(host.name)
                with self.locked_state() as state:
                    state.children[host.name] = child.to_wire()
            except Exception as exc:
                with self.locked_state() as state:
                    state.pending_hosts = [name for name in state.pending_hosts if name != host.name]
                    state.errors[host.name] = str(exc)
                self.notify_all_state_changed()
            finally:
                with self.locked_state() as state:
                    state.role = "parent"
                    state.target_host_name = ""
                    state.target_host_url = ""
        if not hosts:
            with self.locked_state() as state:
                state.errors["mesh"] = "no online target hosts found"
            self.notify_all_state_changed()
        return {
            "targets": [{"name": host.name, "url": host.url} for host in hosts],
            "summary": self.summary(),
        }

    def drain(self, *, after_cursor: int, wait_timeout: float, limit: int) -> dict[str, Any]:
        limit = max(1, limit)
        self._expire_timed_out_hosts()

        def ready(state: MeshSearchState) -> bool:
            return (
                state.next_cursor > after_cursor + 1
                or not state.pending_hosts
                or bool(state.errors)
            )

        timeout = max(0.0, wait_timeout)
        with self.locked_state() as state:
            if state.deadline > 0:
                timeout = min(timeout, max(0.0, state.deadline - time.monotonic()))
        self.wait_state(ready, timeout=timeout)
        self._expire_timed_out_hosts()

        with self.locked_state() as state:
            matching = [event for event in state.events if int(event.get("cursor", 0)) > after_cursor]
            events = matching[:limit]
            last_cursor = after_cursor
            if events:
                last_cursor = max(int(event.get("cursor", 0)) for event in events)
            more_events = len(matching) > len(events)
            done = not state.pending_hosts and not more_events
            summary = self._summary_from_state(state)
        return {
            "events": events,
            "cursor": last_cursor,
            "done": done,
            "summary": summary,
        }

    @state_locked
    def summary(self) -> dict[str, Any]:
        return self._summary_from_state(self.state)

    def cleanup_children(self) -> dict[str, Any]:
        with self.locked_state() as state:
            children = {name: dict(proxy) for name, proxy in state.children.items()}
        for host_name, proxy_wire in children.items():
            try:
                PagletProxy.from_wire(proxy_wire, self.context.host.client).dispose()
            except Exception as exc:
                with self.locked_state() as state:
                    state.cleanup_errors[host_name] = str(exc)
        return self.summary()

    def _target_hosts(self, requested_targets: list[str]):
        hosts = self.context.available_hosts(online_only=True, include_self=True)
        if not requested_targets:
            return hosts
        selected = []
        for target in requested_targets:
            ref = self.context.host_status(target)
            if ref is None or not ref.online:
                with self.locked_state() as state:
                    state.errors[target] = "target host is not online or not visible in the mesh"
                continue
            selected.append(ref)
        return selected

    def _run_child(self) -> None:
        with self.locked_state() as state:
            request_wire = dict(state.request)
            target_host_name = state.target_host_name or self.context.name
            target_host_url = state.target_host_url or self.context.address
            parent_agent_id = state.parent_agent_id
            parent_host_url = state.parent_host_url
        parent = self.context.get_proxy(parent_agent_id, parent_host_url)
        buffer: list[dict[str, Any]] = []

        def flush() -> None:
            if parent is None or not buffer:
                buffer.clear()
                return
            payload = {
                "host_name": target_host_name,
                "host_url": target_host_url,
                "events": list(buffer),
            }
            buffer.clear()
            parent.send(Message("child_events", payload))

        try:
            request = dataclass_from_wire(SearchRequest, request_wire)
            batch_size = max(1, int(request.batch_size))

            def emit(events: list[SearchEvent]) -> None:
                for event in events:
                    buffer.append(dataclass_to_wire(event))
                    if len(buffer) >= batch_size:
                        flush()

            summary = run_local_search(
                request,
                host_name=target_host_name,
                host_url=target_host_url,
                emit=emit,
            )
            flush()
            payload = {
                "host_name": target_host_name,
                "host_url": target_host_url,
                "summary": dataclass_to_wire(summary),
            }
        except Exception as exc:
            flush()
            payload = {
                "host_name": target_host_name,
                "host_url": target_host_url,
                "error": str(exc),
            }
        if parent is not None:
            parent.send(Message("child_done", payload))

    @state_locked
    def record_child_events(self, payload: dict[str, Any]) -> dict[str, Any]:
        for event in payload.get("events") or []:
            item = dict(event)
            item["cursor"] = self.state.next_cursor
            self.state.next_cursor += 1
            self.state.events.append(item)
        self.notify_all_state_changed()
        return {"ok": True, "cursor": self.state.next_cursor - 1}

    @state_locked
    def record_child_done(self, payload: dict[str, Any]) -> dict[str, Any]:
        host_name = str(payload.get("host_name") or "")
        if host_name:
            self.state.pending_hosts = [name for name in self.state.pending_hosts if name != host_name]
            if host_name not in self.state.done_hosts:
                self.state.done_hosts.append(host_name)
        if payload.get("error"):
            self.state.errors[host_name or "unknown"] = str(payload["error"])
        elif payload.get("summary"):
            self.state.summaries[host_name] = dict(payload["summary"])
        self.notify_all_state_changed()
        return {"ok": True}

    def _expire_timed_out_hosts(self) -> None:
        with self.locked_state() as state:
            if not state.pending_hosts or state.deadline <= 0 or time.monotonic() < state.deadline:
                return
            timed_out = list(state.pending_hosts)
            for host_name in timed_out:
                state.errors[host_name] = "timed out waiting for search result"
            state.pending_hosts = []
        self.notify_all_state_changed()

    @staticmethod
    def _summary_from_state(state: MeshSearchState) -> dict[str, Any]:
        return {
            "results": dict(state.summaries),
            "errors": dict(state.errors),
            "cleanup_errors": dict(state.cleanup_errors),
            "pending_hosts": list(state.pending_hosts),
            "done_hosts": list(state.done_hosts),
            "event_count": len(state.events),
        }


class _PatternMatcher:
    def __init__(self, request: SearchRequest):
        self.request = request
        self.pattern = request.pattern
        self.ignore_case = request.ignore_case or (
            request.smart_case and not any(char.isupper() for char in request.pattern)
        )
        self._fixed_pattern = request.pattern.casefold() if self.ignore_case else request.pattern
        self._regex: re.Pattern[str] | None = None
        if not request.fixed_strings and request.pattern:
            flags = re.IGNORECASE if self.ignore_case else 0
            pattern = request.pattern
            if request.word_regexp:
                pattern = rf"(?<!\w)(?:{pattern})(?!\w)"
            self._regex = re.compile(pattern, flags)

    def finditer(self, text: str) -> list[tuple[int, int]]:
        if not self.pattern:
            return [(0, 0)]
        if self._regex is not None:
            return [(match.start(), match.end()) for match in self._regex.finditer(text)]
        haystack = text.casefold() if self.ignore_case else text
        needle = self._fixed_pattern
        spans: list[tuple[int, int]] = []
        start = 0
        while True:
            index = haystack.find(needle, start)
            if index < 0:
                break
            end = index + len(needle)
            if not self.request.word_regexp or _word_boundaries(text, index, end):
                spans.append((index, end))
            start = max(end, index + 1)
        return spans

    def matches(self, text: str) -> bool:
        return bool(self.finditer(text))


class _IgnoreMatcher:
    def __init__(self, request: SearchRequest):
        self.request = request
        self._specs: list[tuple[Path, pathspec.PathSpec]] = []
        self._loaded: set[Path] = set()

    def load_for_directory(self, directory: Path) -> None:
        if self.request.no_ignore:
            return
        directory = directory.resolve(strict=False)
        if directory in self._loaded:
            return
        self._loaded.add(directory)
        names = [*_IGNORE_FILES, *self.request.ignore_files]
        lines: list[str] = []
        for name in names:
            ignore_path = directory / name
            if not ignore_path.is_file():
                continue
            try:
                lines.extend(ignore_path.read_text(encoding="utf-8", errors="replace").splitlines())
            except OSError:
                continue
        if lines:
            self._specs.append((directory, pathspec.PathSpec.from_lines("gitwildmatch", lines)))

    def ignored(self, path: Path, *, is_dir: bool) -> bool:
        if self.request.no_ignore:
            return False
        candidate = path.resolve(strict=False)
        for base, spec in self._specs:
            try:
                rel = candidate.relative_to(base).as_posix()
            except ValueError:
                continue
            if is_dir and rel and not rel.endswith("/"):
                rel = f"{rel}/"
            if rel and spec.match_file(rel):
                return True
        return False


@dataclass
class _SearchStats:
    files_seen: int = 0
    files_searched: int = 0
    files_matched: int = 0
    matches: int = 0
    paths_matched: int = 0
    errors: list[str] = field(default_factory=list)
    truncated: bool = False
    emitted_results: int = 0
    sequence: int = 0


def run_local_search(
    request: SearchRequest,
    *,
    host_name: str,
    host_url: str,
    emit: Callable[[list[SearchEvent]], None] | None = None,
) -> HostSearchSummary:
    started = time.perf_counter()
    stats = _SearchStats()
    emitter = emit or (lambda events: None)
    try:
        matcher = _PatternMatcher(request)
    except re.error as exc:
        stats.errors.append(f"invalid regular expression: {exc}")
        return _summary(request, host_name, host_url, stats, started)

    for path, root, is_dir, is_symlink in _iter_paths(request, stats):
        if stats.truncated:
            break
        if request.mode == "find":
            _search_path(request, matcher, host_name, host_url, path, root, is_dir, is_symlink, stats, emitter)
        elif not is_dir:
            _search_file(request, matcher, host_name, host_url, path, root, stats, emitter)
    return _summary(request, host_name, host_url, stats, started)


def _summary(
    request: SearchRequest,
    host_name: str,
    host_url: str,
    stats: _SearchStats,
    started: float,
) -> HostSearchSummary:
    return HostSearchSummary(
        host_name=host_name,
        host_url=host_url,
        files_seen=stats.files_seen,
        files_searched=stats.files_searched,
        files_matched=stats.files_matched,
        matches=stats.matches,
        paths_matched=stats.paths_matched,
        errors=list(stats.errors),
        truncated=stats.truncated,
        duration_seconds=time.perf_counter() - started,
    )


def _iter_paths(request: SearchRequest, stats: _SearchStats) -> Iterable[tuple[Path, Path, bool, bool]]:
    roots = request.paths or ["."]
    for raw_path in roots:
        root = Path(raw_path).expanduser()
        if not root.exists():
            stats.errors.append(f"{raw_path}: path does not exist")
            continue
        base = root if root.is_dir() else root.parent
        matcher = _IgnoreMatcher(request)
        yield from _walk(request, root, base, matcher, stats, depth=0, explicit=True)


def _walk(
    request: SearchRequest,
    path: Path,
    base: Path,
    matcher: _IgnoreMatcher,
    stats: _SearchStats,
    *,
    depth: int,
    explicit: bool = False,
) -> Iterable[tuple[Path, Path, bool, bool]]:
    try:
        is_symlink = path.is_symlink()
        is_dir = path.is_dir() if request.follow else path.is_dir() and not is_symlink
        is_file = path.is_file() if request.follow else path.is_file() and not is_symlink
    except OSError as exc:
        stats.errors.append(f"{path}: {exc}")
        return

    if not explicit and not request.hidden and _is_hidden(path):
        return
    if is_dir:
        matcher.load_for_directory(path)
    if not explicit and matcher.ignored(path, is_dir=is_dir):
        return
    if not explicit and _excluded_by_glob(request, path, base, is_dir=is_dir):
        return

    if is_dir:
        if _candidate_kind_matches(request, is_dir=True, is_symlink=is_symlink):
            yield path, base, True, is_symlink
        if request.max_depth is not None and depth >= request.max_depth:
            return
        try:
            entries = sorted(os.scandir(path), key=lambda item: item.name)
        except OSError as exc:
            stats.errors.append(f"{path}: {exc}")
            return
        for entry in entries:
            yield from _walk(
                request,
                Path(entry.path),
                base,
                matcher,
                stats,
                depth=depth + 1,
                explicit=False,
            )
        return

    if is_file or is_symlink:
        if is_file:
            stats.files_seen += 1
        if _candidate_kind_matches(request, is_dir=False, is_symlink=is_symlink):
            yield path, base, False, is_symlink


def _search_path(
    request: SearchRequest,
    matcher: _PatternMatcher,
    host_name: str,
    host_url: str,
    path: Path,
    root: Path,
    is_dir: bool,
    is_symlink: bool,
    stats: _SearchStats,
    emit: Callable[[list[SearchEvent]], None],
) -> None:
    if not _included_by_glob(request, path, root, is_dir=is_dir):
        return
    if not _extension_matches(request, path):
        return
    text = path.as_posix() if request.full_path else path.name
    if request.pattern and not matcher.matches(text):
        return
    stats.paths_matched += 1
    event = _event(
        stats,
        "file",
        host_name,
        host_url,
        path=_display_path(path, request),
        text=_display_path(path, request),
    )
    _emit_result(request, stats, emit, event)


def _search_file(
    request: SearchRequest,
    matcher: _PatternMatcher,
    host_name: str,
    host_url: str,
    path: Path,
    root: Path,
    stats: _SearchStats,
    emit: Callable[[list[SearchEvent]], None],
) -> None:
    if not _included_by_glob(request, path, root, is_dir=False):
        return
    if not _type_matches(request, path):
        return
    try:
        stat = path.stat()
    except OSError as exc:
        stats.errors.append(f"{path}: {exc}")
        return
    if request.max_file_size > 0 and stat.st_size > request.max_file_size:
        return
    if not request.text and _looks_binary(path):
        return

    before_context = max(0, int(request.before_context))
    after_context = max(0, int(request.after_context))
    prior: deque[tuple[int, str]] = deque(maxlen=before_context)
    after_remaining = 0
    emitted_context: set[int] = set()
    file_match_lines = 0
    file_matches = 0
    matched = False
    file_events: list[SearchEvent] = []
    stats.files_searched += 1

    try:
        with path.open("r", encoding=request.encoding or "utf-8", errors="replace") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                line = raw_line.rstrip("\n")
                spans = matcher.finditer(line)
                if spans:
                    matched = True
                    file_match_lines += 1
                    file_matches += len(spans)
                    if request.files_with_matches or request.files_without_match or request.count:
                        if request.max_count and file_match_lines >= request.max_count:
                            break
                        continue
                    if request.only_matching:
                        for start, end in spans:
                            file_events.append(
                                _event(
                                    stats,
                                    "match",
                                    host_name,
                                    host_url,
                                    path=_display_path(path, request),
                                    line_number=line_number,
                                    column=start + 1,
                                    text=line[start:end],
                                    match_text=line[start:end],
                                    match_start=0,
                                    match_end=end - start,
                                )
                            )
                    else:
                        for context_line_number, context_text in prior:
                            if context_line_number not in emitted_context:
                                emitted_context.add(context_line_number)
                                file_events.append(
                                    _event(
                                        stats,
                                        "context",
                                        host_name,
                                        host_url,
                                        path=_display_path(path, request),
                                        line_number=context_line_number,
                                        text=context_text,
                                        context="before",
                                    )
                                )
                        start, end = spans[0]
                        file_events.append(
                            _event(
                                stats,
                                "match",
                                host_name,
                                host_url,
                                path=_display_path(path, request),
                                line_number=line_number,
                                column=start + 1,
                                text=line,
                                match_text=line[start:end],
                                match_start=start,
                                match_end=end,
                            )
                        )
                        after_remaining = after_context
                    prior.clear()
                    if request.max_count and file_match_lines >= request.max_count:
                        break
                    continue

                if after_remaining > 0 and not request.only_matching:
                    file_events.append(
                        _event(
                            stats,
                            "context",
                            host_name,
                            host_url,
                            path=_display_path(path, request),
                            line_number=line_number,
                            text=line,
                            context="after",
                        )
                    )
                    emitted_context.add(line_number)
                    after_remaining -= 1
                if before_context:
                    prior.append((line_number, line))
    except OSError as exc:
        stats.errors.append(f"{path}: {exc}")
        return

    if not matched and request.files_without_match:
        _emit_result(
            request,
            stats,
            emit,
            _event(stats, "file", host_name, host_url, path=_display_path(path, request), text=_display_path(path, request)),
        )
        return
    if not matched:
        return

    stats.files_matched += 1
    stats.matches += file_match_lines
    if request.files_with_matches:
        _emit_result(
            request,
            stats,
            emit,
            _event(stats, "file", host_name, host_url, path=_display_path(path, request), text=_display_path(path, request)),
        )
        return
    if request.count:
        _emit_result(
            request,
            stats,
            emit,
            _event(
                stats,
                "count",
                host_name,
                host_url,
                path=_display_path(path, request),
                count=file_match_lines,
            ),
        )
        return
    for event in file_events:
        if stats.truncated:
            break
        _emit_result(request, stats, emit, event, count_result=event.event == "match")


def _event(
    stats: _SearchStats,
    event: str,
    host_name: str,
    host_url: str,
    **kwargs: Any,
) -> SearchEvent:
    stats.sequence += 1
    return SearchEvent(
        event=event,
        host_name=host_name,
        host_url=host_url,
        sequence=stats.sequence,
        **kwargs,
    )


def _emit_result(
    request: SearchRequest,
    stats: _SearchStats,
    emit: Callable[[list[SearchEvent]], None],
    event: SearchEvent,
    *,
    count_result: bool = True,
) -> None:
    if count_result:
        if request.max_results_per_host > 0 and stats.emitted_results >= request.max_results_per_host:
            stats.truncated = True
            return
        stats.emitted_results += 1
    emit([event])


def _display_path(path: Path, request: SearchRequest) -> str:
    if request.absolute_path:
        return str(path.resolve(strict=False))
    return str(path)


def _looks_binary(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            return b"\0" in handle.read(8192)
    except OSError:
        return False


def _word_boundaries(text: str, start: int, end: int) -> bool:
    before = text[start - 1] if start > 0 else ""
    after = text[end] if end < len(text) else ""
    return (not before or not (before.isalnum() or before == "_")) and (
        not after or not (after.isalnum() or after == "_")
    )


def _is_hidden(path: Path) -> bool:
    return path.name.startswith(".")


def _candidate_kind_matches(request: SearchRequest, *, is_dir: bool, is_symlink: bool) -> bool:
    if request.mode != "find":
        return not is_dir and not is_symlink
    kind = request.kind
    if kind == "any":
        return True
    if kind == "file":
        return not is_dir and not is_symlink
    if kind == "dir":
        return is_dir
    if kind == "symlink":
        return is_symlink
    return True


def _extension_matches(request: SearchRequest, path: Path) -> bool:
    if not request.extensions:
        return True
    suffix = path.suffix.lower().lstrip(".")
    return suffix in {item.lower().lstrip(".") for item in request.extensions}


def _type_matches(request: SearchRequest, path: Path) -> bool:
    included = _type_globs(request.type_names)
    excluded = _type_globs(request.type_not_names)
    name = path.name
    if included and not any(fnmatch.fnmatch(name, pattern) for pattern in included):
        return False
    if excluded and any(fnmatch.fnmatch(name, pattern) for pattern in excluded):
        return False
    return True


def _type_globs(type_names: list[str]) -> list[str]:
    globs: list[str] = []
    for name in type_names:
        globs.extend(SEARCH_TYPES.get(name, ()))
    return globs


def _excluded_by_glob(request: SearchRequest, path: Path, root: Path, *, is_dir: bool) -> bool:
    rel = _relative_path(path, root)
    for pattern in request.globs:
        if not pattern.startswith("!"):
            continue
        if _glob_matches(pattern[1:], rel, path.name):
            return True
    return False


def _included_by_glob(request: SearchRequest, path: Path, root: Path, *, is_dir: bool) -> bool:
    if _excluded_by_glob(request, path, root, is_dir=is_dir):
        return False
    positives = [pattern for pattern in request.globs if not pattern.startswith("!")]
    if not positives:
        return True
    rel = _relative_path(path, root)
    return any(_glob_matches(pattern, rel, path.name) for pattern in positives)


def _relative_path(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _glob_matches(pattern: str, rel: str, name: str) -> bool:
    return fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(name, pattern)
