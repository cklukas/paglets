# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
import time

from paglets.examples.search import HostSearchSummary, MeshSearchAgent, SearchEvent, SearchRequest, run_local_search
from paglets.examples.search.agent import (
    SEARCH_CLEANUP,
    SEARCH_START,
    SEARCH_SUMMARY,
    SearchStartRequest,
)
from paglets.examples.search.cli import main as search_main
from paglets.patterns.operations import OperationClient
from paglets.remote.admin import ServerRef
from paglets.runtime.host import Host
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from tests.support import free_port


def test_search_dataclasses_round_trip_through_wire():
    request = SearchRequest(
        mode="grep",
        pattern="Needle",
        paths=["."],
        smart_case=True,
        before_context=2,
        globs=["*.py", "!vendor/**"],
        type_names=["py"],
        max_depth=3,
    )
    assert dataclass_from_wire(SearchRequest, dataclass_to_wire(request)) == request

    event = SearchEvent(
        event="match",
        host_name="alpha",
        host_url="http://alpha",
        path="app.py",
        line_number=3,
        column=5,
        text="has Needle",
        match_text="Needle",
        match_start=4,
        match_end=10,
        cursor=7,
    )
    assert dataclass_from_wire(SearchEvent, dataclass_to_wire(event)) == event

    summary = HostSearchSummary("alpha", "http://alpha", files_matched=1, matches=2)
    assert dataclass_from_wire(HostSearchSummary, dataclass_to_wire(summary)) == summary


def test_local_grep_supports_context_globs_ignores_and_binary_skip(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    target = src / "app.py"
    target.write_text("before\nneedle here\nafter\n", encoding="utf-8")
    (tmp_path / ".hidden.py").write_text("needle hidden\n", encoding="utf-8")
    (tmp_path / "ignored.log").write_text("needle ignored\n", encoding="utf-8")
    (tmp_path / ".gitignore").write_text("ignored.log\n", encoding="utf-8")
    (tmp_path / "binary.py").write_bytes(b"needle\x00binary")

    events: list[SearchEvent] = []
    summary = run_local_search(
        SearchRequest(
            mode="grep",
            pattern="needle",
            paths=[str(tmp_path)],
            before_context=1,
            after_context=1,
            globs=["*.py"],
        ),
        host_name="alpha",
        host_url="http://alpha",
        emit=events.extend,
    )

    assert summary.files_matched == 1
    assert summary.matches == 1
    assert [event.event for event in events] == ["context", "match", "context"]
    assert events[1].path == str(target)
    assert all("ignored" not in event.path for event in events)
    assert all("binary" not in event.path for event in events)
    assert all(".hidden" not in event.path for event in events)


def test_local_find_supports_name_extension_and_kind_filters(tmp_path):
    (tmp_path / "report.md").write_text("x", encoding="utf-8")
    (tmp_path / "report.txt").write_text("x", encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "REPORT.md").write_text("x", encoding="utf-8")

    events: list[SearchEvent] = []
    summary = run_local_search(
        SearchRequest(
            mode="find",
            pattern="report",
            paths=[str(tmp_path)],
            ignore_case=True,
            extensions=["md"],
            kind="file",
        ),
        host_name="alpha",
        host_url="http://alpha",
        emit=events.extend,
    )

    assert summary.paths_matched == 2
    assert sorted(event.path for event in events) == [
        str(nested / "REPORT.md"),
        str(tmp_path / "report.md"),
    ]


def test_mesh_search_writes_events_without_cli_polling(tmp_path):
    (tmp_path / "haystack.txt").write_text("needle\n", encoding="utf-8")
    proxy = None
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh_version="search-stream-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    beta = Host(
        "beta",
        host="127.0.0.1",
        port=free_port(),
        peers=[alpha.address],
        mesh_version="search-stream-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "beta",
    )

    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        proxy = alpha.create(MeshSearchAgent)
        operations = OperationClient(proxy)
        output_path = tmp_path / "search.jsonl"
        reply = operations.call(
            SEARCH_START,
            SearchStartRequest(
                request=dataclass_to_wire(
                    SearchRequest(mode="grep", pattern="needle", paths=[str(tmp_path)], batch_size=1)
                ),
                timeout=3.0,
                output_path=str(output_path),
            ),
        )

        assert reply.accepted is True
        _wait_until(lambda: bool(output_path.read_text(encoding="utf-8").strip()))
        _wait_until(lambda: set(operations.call(SEARCH_SUMMARY).results) == {"alpha", "beta"})
    finally:
        if proxy is not None:
            try:
                OperationClient(proxy).call(SEARCH_CLEANUP)
                proxy.dispose()
            except Exception:
                pass
        beta.stop()
        alpha.stop()


def test_paglets_search_cli_jsonl_submits_mesh_job(tmp_path, capsys, monkeypatch):
    (tmp_path / "haystack.txt").write_text("needle\n", encoding="utf-8")
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh_version="search-cli-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    beta = Host(
        "beta",
        host="127.0.0.1",
        port=free_port(),
        peers=[alpha.address],
        mesh_version="search-cli-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "beta",
    )
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        monkeypatch.setattr(
            "paglets.examples.search.cli._select_entry_server",
            lambda *, entry_name, client: ServerRef("alpha", alpha.address),
        )

        result = search_main(
            [
                "--timeout",
                "5",
                "--jsonl",
                "--output",
                str(tmp_path / "search.jsonl"),
                "grep",
                "needle",
                str(tmp_path / "haystack.txt"),
            ]
        )

        assert result == 0
        output = capsys.readouterr().out
        assert "paglets-search: submitted" in output
        output_path = tmp_path / "search.jsonl"
        _wait_until(lambda: _match_hosts(output_path) == {"alpha", "beta"})
        events = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
        assert {event["host_name"] for event in events if event["event"] == "match"} == {"alpha", "beta"}
    finally:
        beta.stop()
        alpha.stop()


def _wait_until(predicate, *, timeout: float = 3.0, interval: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()


def _match_hosts(path) -> set[str]:
    if not path.exists():
        return set()
    return {
        event["host_name"]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
        for event in [json.loads(line)]
        if event.get("event") == "match"
    }
