from __future__ import annotations

import asyncio

import pytest

from paglets.admin import AgentDiscoveryConfig, AgentRecord, ServerRef, ServerStatus, load_server_config, load_tui_config
import paglets.tui as tui


def test_tui_main_reports_missing_textual(monkeypatch, tmp_path, capsys):
    def missing_textual():
        raise ImportError("no textual")

    monkeypatch.setattr(tui, "_load_textual_runtime", missing_textual)

    result = tui.main(["--config", str(tmp_path / "servers.json")])

    assert result == 2
    assert "Textual is required for paglets-tui" in capsys.readouterr().err


def test_tui_app_renders_server_and_agent_rows_with_fake_client(tmp_path):
    textual = pytest.importorskip("textual")
    from textual.widgets import DataTable

    servers = [ServerRef("alpha", "http://127.0.0.1:8765")]
    fake_client = FakeAdminClient(servers)

    async def run_app() -> None:
        app = tui.create_tui_app(
            servers,
            config_path=tmp_path / "servers.json",
            refresh_interval=60,
            admin_client=fake_client,
        )
        async with app.run_test(size=(120, 40)) as _pilot:
            server_table = app.query_one("#server-table", DataTable)
            agent_table = app.query_one("#agent-table", DataTable)
            assert server_table.row_count == 2
            assert agent_table.row_count == 1

    asyncio.run(run_app())
    assert textual is not None


def test_tui_add_server_key_opens_modal_without_worker_error(tmp_path):
    textual = pytest.importorskip("textual")

    servers = [ServerRef("alpha", "http://127.0.0.1:8765")]
    fake_client = FakeAdminClient(servers)

    async def run_app() -> None:
        app = tui.create_tui_app(
            servers,
            config_path=tmp_path / "servers.json",
            refresh_interval=60,
            admin_client=fake_client,
        )
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.press("a")
            await pilot.pause()
            assert type(app.screen).__name__ == "TextForm"

    asyncio.run(run_app())
    assert textual is not None


def test_tui_starts_selected_offline_local_server(tmp_path, monkeypatch):
    textual = pytest.importorskip("textual")

    servers = [ServerRef("alpha", "http://127.0.0.1:8765", True, True)]
    fake_client = OfflineFakeAdminClient(servers)
    started = []

    class FakeProcess:
        pid = 4242

    def fake_start(server: ServerRef, *, peers: list[str] | None = None) -> FakeProcess:
        started.append((server, peers or []))
        return FakeProcess()

    monkeypatch.setattr(tui, "start_local_server", fake_start)

    async def run_app() -> None:
        config_path = tmp_path / "servers.json"
        app = tui.create_tui_app(
            servers,
            config_path=config_path,
            refresh_interval=60,
            admin_client=fake_client,
        )
        async with app.run_test(size=(120, 40)) as _pilot:
            app.selected_server = "alpha"
            await app.action_start_server()
            assert started == [(servers[0], [])]
            assert "Started local server alpha with PID 4242." == app.last_result
            assert load_server_config(config_path) == [ServerRef("alpha", "http://127.0.0.1:8765", True, True)]

    asyncio.run(run_app())
    assert textual is not None


def test_tui_create_key_opens_discovered_class_form(tmp_path):
    textual = pytest.importorskip("textual")
    from textual.widgets import Input, Select

    servers = [ServerRef("alpha", "http://127.0.0.1:8765")]
    fake_client = FakeAdminClient(servers)

    async def run_app() -> None:
        app = tui.create_tui_app(
            servers,
            config_path=tmp_path / "servers.json",
            refresh_interval=60,
            admin_client=fake_client,
            agent_discovery=AgentDiscoveryConfig(paths=[], modules=["tests.test_paglets_core"]),
        )
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.press("c")
            await pilot.pause()
            assert type(app.screen).__name__ == "CreateAgentForm"

            agent_input = app.screen.query_one("#agent_class", Input)
            state_input = app.screen.query_one("#state_class", Input)
            select = app.screen.query_one("#agent_choice", Select)

            assert agent_input.value == "tests.test_paglets_core:CloneAgent"
            assert state_input.value == "tests.test_paglets_core:CloneState"

            select.value = "tests.test_paglets_core:TravelAgent"
            await pilot.pause()
            assert agent_input.value == "tests.test_paglets_core:TravelAgent"
            assert state_input.value == "tests.test_paglets_core:TravelState"

    asyncio.run(run_app())
    assert textual is not None


def test_tui_create_key_falls_back_to_manual_form_when_discovery_is_empty(tmp_path):
    textual = pytest.importorskip("textual")
    from textual.widgets import Input

    servers = [ServerRef("alpha", "http://127.0.0.1:8765")]
    fake_client = FakeAdminClient(servers)

    async def run_app() -> None:
        app = tui.create_tui_app(
            servers,
            config_path=tmp_path / "servers.json",
            refresh_interval=60,
            admin_client=fake_client,
            agent_discovery=AgentDiscoveryConfig(paths=[], modules=[]),
        )
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.press("c")
            await pilot.pause()
            assert type(app.screen).__name__ == "CreateAgentForm"
            assert app.screen.query_one("#agent_class", Input).value == ""
            assert app.screen.query_one("#state_class", Input).value == ""
            assert app.screen.query_one("#state", Input).value == "{}"

    asyncio.run(run_app())
    assert textual is not None


def test_tui_adds_and_removes_discovery_sources_in_config(tmp_path, monkeypatch):
    textual = pytest.importorskip("textual")

    servers = [ServerRef("alpha", "http://127.0.0.1:8765")]
    fake_client = FakeAdminClient(servers)
    config_path = tmp_path / "servers.json"

    async def run_app() -> None:
        app = tui.create_tui_app(
            servers,
            config_path=config_path,
            refresh_interval=60,
            admin_client=fake_client,
            agent_discovery=AgentDiscoveryConfig(paths=[], modules=[]),
        )
        async with app.run_test(size=(120, 40)) as _pilot:
            async def add_module(_screen):
                return {"kind": "module", "value": "tests.test_paglets_core"}

            monkeypatch.setattr(app, "push_screen_wait", add_module)
            await app.action_add_discovery_source().wait()
            assert load_tui_config(config_path).agent_discovery.modules == ["tests.test_paglets_core"]

            async def remove_module(_screen):
                return {"kind": "module", "value": "tests.test_paglets_core"}

            monkeypatch.setattr(app, "push_screen_wait", remove_module)
            await app.action_remove_discovery_source().wait()
            assert load_tui_config(config_path).agent_discovery.modules == []

    asyncio.run(run_app())
    assert textual is not None


class FakeAdminClient:
    def __init__(self, servers: list[ServerRef]):
        self.servers = servers

    def set_servers(self, servers: list[ServerRef]) -> None:
        self.servers = servers

    def enabled_servers(self) -> list[ServerRef]:
        return [server for server in self.servers if server.enabled]

    def health_all(self) -> list[ServerStatus]:
        return [
            ServerStatus(
                name="alpha",
                url="http://127.0.0.1:8765",
                reachable=True,
                latency_ms=1.0,
                active_count=1,
                inactive_count=0,
            )
        ]

    def list_agents(self, server: ServerRef) -> list[AgentRecord]:
        return [
            AgentRecord(
                server_name=server.name,
                host_url=server.url,
                agent_id="agent-1",
                class_name="tests:FakeAgent",
                state_class_name="tests:FakeState",
                active=True,
            )
        ]

    def list_hosts(self, server: ServerRef) -> list:
        return []


class OfflineFakeAdminClient:
    def __init__(self, servers: list[ServerRef]):
        self.servers = servers

    def set_servers(self, servers: list[ServerRef]) -> None:
        self.servers = servers

    def enabled_servers(self) -> list[ServerRef]:
        return [server for server in self.servers if server.enabled]

    def health_all(self) -> list[ServerStatus]:
        return [
            ServerStatus(
                name="alpha",
                url="http://127.0.0.1:8765",
                reachable=False,
                latency_ms=1.0,
                active_count=0,
                inactive_count=0,
                error="connection refused",
            )
        ]

    def list_agents(self, server: ServerRef) -> list[AgentRecord]:
        return []

    def list_hosts(self, server: ServerRef) -> list:
        return []
