# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import fnmatch
import os
import re
import time
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pathspec

from .models import _IGNORE_FILES, SEARCH_TYPES, HostSearchSummary, SearchEvent, SearchRequest


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
            _event(
                stats, "file", host_name, host_url, path=_display_path(path, request), text=_display_path(path, request)
            ),
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
            _event(
                stats, "file", host_name, host_url, path=_display_path(path, request), text=_display_path(path, request)
            ),
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
    return not (excluded and any(fnmatch.fnmatch(name, pattern) for pattern in excluded))


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
