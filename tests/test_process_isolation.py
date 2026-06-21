# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import sys
import time

import pytest

from paglets import Host, Message, Paglet, PagletState
from paglets.errors import HostError, PagletCrashedError
from tests.test_paglets_core import free_port


@dataclass
class IsolationState(PagletState):
    count: int = 0


class StableIsolationAgent(Paglet[IsolationState]):
    State = IsolationState

    def handle_message(self, message: Message):
        if message.kind == "ping":
            return "pong"
        if message.kind == "count":
            self.state.count += 1
            return self.state.count
        if message.kind == "boom":
            raise RuntimeError("normal handler error")
        return self.not_handled()


class SysExitAgent(Paglet[IsolationState]):
    State = IsolationState

    def handle_message(self, message: Message):
        if message.kind == "exit":
            sys.exit(3)
        if message.kind == "ping":
            return "pong"
        return self.not_handled()


class HardExitAgent(Paglet[IsolationState]):
    State = IsolationState

    def handle_message(self, message: Message):
        if message.kind == "exit":
            os._exit(7)
        if message.kind == "ping":
            return "pong"
        return self.not_handled()


class BlockingAgent(Paglet[IsolationState]):
    State = IsolationState

    def handle_message(self, message: Message):
        if message.kind == "block":
            while True:
                time.sleep(1.0)
        if message.kind == "ping":
            return "pong"
        return self.not_handled()


def test_normal_exception_returns_message_error_and_child_survives(tmp_path: Path):
    host = _host(tmp_path)
    host.start_background()
    try:
        proxy = host.create(StableIsolationAgent, IsolationState())

        with pytest.raises(HostError):
            proxy.send(Message("boom"))

        assert proxy.send(Message("ping")) == "pong"
        info = proxy.info()
        assert info["active"] is True
        assert info["crashed"] is False
        assert info["exitcode"] is None
    finally:
        host.stop()


def test_sys_exit_kills_only_that_paglet_process(tmp_path: Path):
    host = _host(tmp_path)
    host.start_background()
    try:
        stable = host.create(StableIsolationAgent, IsolationState())
        exiting = host.create(SysExitAgent, IsolationState())

        with pytest.raises(PagletCrashedError):
            exiting.send(Message("exit"))

        _wait_until(lambda: _crashed(exiting, exitcode=3))
        assert stable.send(Message("ping")) == "pong"
        assert host.health()["active_count"] == 1
    finally:
        host.stop()


def test_os_exit_is_reported_as_crash_and_other_paglets_continue(tmp_path: Path):
    host = _host(tmp_path)
    host.start_background()
    try:
        stable = host.create(StableIsolationAgent, IsolationState())
        exiting = host.create(HardExitAgent, IsolationState())

        with pytest.raises(PagletCrashedError):
            exiting.send(Message("exit"))

        _wait_until(lambda: _crashed(exiting, exitcode=7))
        assert stable.send(Message("count")) == 1
        assert stable.send(Message("count")) == 2
    finally:
        host.stop()


def test_host_stop_terminates_blocked_child_process(tmp_path: Path):
    host = _host(tmp_path)
    host.start_background()
    proxy = host.create(BlockingAgent, IsolationState())
    pid = int(proxy.info()["pid"])
    proxy.send_oneway(Message("block"))
    time.sleep(0.2)

    host.stop()

    _wait_until(lambda: not _pid_alive(pid), timeout=5.0)


def test_shutdown_terminates_child_that_cannot_deactivate(tmp_path: Path):
    host = _host(tmp_path)
    host.start_background()
    proxy = host.create(BlockingAgent, IsolationState())
    pid = int(proxy.info()["pid"])
    proxy.send_oneway(Message("block"))
    time.sleep(0.2)

    started = time.monotonic()
    host.shutdown()

    assert time.monotonic() - started < 4.0
    _wait_until(lambda: not _pid_alive(pid), timeout=5.0)


def test_shutdown_persists_inactive_state(tmp_path: Path):
    host = _host(tmp_path)
    host.start_background()
    proxy = host.create(StableIsolationAgent, IsolationState())
    assert proxy.send(Message("count")) == 1

    host.shutdown()

    reloaded = _host(tmp_path)
    agents = reloaded.list_agents(active=False, inactive=True)
    assert len(agents) == 1
    assert agents[0]["agent_id"] == proxy.agent_id


def _host(tmp_path: Path) -> Host:
    return Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )


def _crashed(proxy, *, exitcode: int) -> bool:
    try:
        info = proxy.info()
    except Exception:
        return False
    return bool(info.get("crashed")) and info.get("exitcode") == exitcode


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_until(predicate, *, timeout: float = 3.0, interval: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()
