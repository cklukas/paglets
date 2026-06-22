# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field

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
