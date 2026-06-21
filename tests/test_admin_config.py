# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
from pathlib import Path

import pytest

import paglets.admin as admin_module
from paglets.admin import (
    AgentDiscoveryConfig,
    ServerRef,
    TuiConfig,
    add_server_ref,
    can_start_local_server,
    default_agent_discovery_config,
    is_local_server_url,
    load_server_config,
    load_tui_config,
    local_server_command,
    normalize_discovery_path,
    parse_server_arg,
    register_running_server,
    remove_server_ref,
    save_server_config,
    save_tui_config,
    select_reachable_entry_server,
    upsert_server_ref,
)


def test_server_config_load_save_add_remove(tmp_path):
    path = tmp_path / "servers.json"
    assert load_server_config(path) == []

    servers = add_server_ref([], ServerRef("alpha", "http://127.0.0.1:8765"))
    servers = add_server_ref(servers, ServerRef("beta", "127.0.0.1:8766", enabled=False))
    save_server_config(servers, path)

    loaded = load_server_config(path)
    assert loaded == [
        ServerRef("alpha", "http://127.0.0.1:8765", True, True),
        ServerRef("beta", "http://127.0.0.1:8766", False, True),
    ]

    assert remove_server_ref(loaded, "alpha") == [ServerRef("beta", "http://127.0.0.1:8766", False, True)]


def test_server_config_rejects_duplicate_names_and_supports_upsert():
    servers = [ServerRef("alpha", "http://127.0.0.1:8765")]

    with pytest.raises(ValueError):
        add_server_ref(servers, ServerRef("alpha", "http://127.0.0.1:9000"))

    assert upsert_server_ref(servers, ServerRef("alpha", "http://127.0.0.1:9000")) == [
        ServerRef("alpha", "http://127.0.0.1:9000")
    ]


def test_register_running_server_upserts_actual_runtime_url_and_preserves_config(tmp_path):
    config_path = tmp_path / "servers.json"
    config_path.write_text(
        json.dumps(
            {
                "agent_discovery": {"paths": [str(tmp_path / "agents")], "modules": ["tests.test_paglets_core"]},
                "servers": [{"name": "alpha", "url": "http://127.0.0.1:8765", "enabled": True}],
            }
        ),
        encoding="utf-8",
    )

    server = register_running_server("alpha", "http://192.168.86.38:8765", config_path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))

    assert server == ServerRef("alpha", "http://192.168.86.38:8765", True, False)
    assert payload["agent_discovery"] == {"paths": [str(tmp_path / "agents")], "modules": ["tests.test_paglets_core"]}
    assert load_server_config(config_path) == [ServerRef("alpha", "http://192.168.86.38:8765", True, False)]


def test_select_reachable_entry_server_falls_back_from_loopback_to_lan(monkeypatch):
    monkeypatch.setattr(admin_module, "detect_lan_host", lambda: "192.168.86.38")
    calls: list[str] = []

    class FakeClient:
        def get_json(self, url: str, *, timeout: float | None = None):
            calls.append(url)
            if "127.0.0.1" in url:
                raise OSError("connection refused")
            return {
                "name": "mac-studio",
                "address": "http://192.168.86.38:8765",
                "code_version": "dev",
                "active_count": 0,
                "inactive_count": 0,
            }

    selected = select_reachable_entry_server(
        [ServerRef("alpha", "http://127.0.0.1:8765")],
        entry_name=None,
        client=FakeClient(),  # type: ignore[arg-type]
    )

    assert calls == ["http://127.0.0.1:8765/health", "http://192.168.86.38:8765/health"]
    assert selected == ServerRef("mac-studio", "http://192.168.86.38:8765", True, False)


def test_select_reachable_entry_server_ignores_stale_config_ip_and_uses_ambient_lan(monkeypatch):
    monkeypatch.setattr(admin_module, "detect_lan_host", lambda: "192.168.86.38")
    monkeypatch.setattr(admin_module, "discover_mesh_entry_servers", lambda timeout=0.75: [])
    calls: list[str] = []

    class FakeClient:
        def get_json(self, url: str, *, timeout: float | None = None):
            calls.append(url)
            if url == "http://192.168.86.38:8765/health":
                return {
                    "name": "mac-studio",
                    "address": "http://192.168.86.38:8765",
                    "code_version": "dev",
                    "active_count": 0,
                    "inactive_count": 0,
                }
            raise OSError("stale")

    selected = select_reachable_entry_server(
        [ServerRef("old", "http://192.168.86.99:8765")],
        entry_name=None,
        client=FakeClient(),  # type: ignore[arg-type]
    )

    assert "http://192.168.86.99:8765/health" in calls
    assert "http://192.168.86.38:8765/health" in calls
    assert selected == ServerRef("mac-studio", "http://192.168.86.38:8765", True, False)


def test_select_reachable_entry_server_uses_ambient_lan_without_config(monkeypatch):
    monkeypatch.setattr(admin_module, "detect_lan_host", lambda: "192.168.86.38")
    monkeypatch.setattr(admin_module, "discover_mesh_entry_servers", lambda timeout=0.75: [])

    class FakeClient:
        def get_json(self, url: str, *, timeout: float | None = None):
            if url == "http://192.168.86.38:8765/health":
                return {
                    "name": "mac-studio",
                    "address": "http://192.168.86.38:8765",
                    "code_version": "dev",
                    "active_count": 0,
                    "inactive_count": 0,
                }
            raise OSError("not here")

    selected = select_reachable_entry_server([], entry_name=None, client=FakeClient())  # type: ignore[arg-type]

    assert selected == ServerRef("mac-studio", "http://192.168.86.38:8765", True, False)


def test_parse_server_arg_normalizes_name_url_pair():
    assert parse_server_arg("alpha=127.0.0.1:8765") == ServerRef("alpha", "http://127.0.0.1:8765", True, True)

    with pytest.raises(ValueError):
        parse_server_arg("http://127.0.0.1:8765")


def test_local_server_detection_and_command():
    server = ServerRef("alpha", "http://localhost:8765")

    assert is_local_server_url(server.url)
    assert can_start_local_server(server)
    assert local_server_command(server)[-6:] == [
        "--name",
        "alpha",
        "--host",
        "127.0.0.1",
        "--port",
        "8765",
    ]
    assert local_server_command(server, peers=["http://127.0.0.1:8766"])[-2:] == [
        "--peer",
        "http://127.0.0.1:8766",
    ]

    assert not is_local_server_url("https://example.com:8765")
    assert not can_start_local_server(ServerRef("remote", "https://example.com:8765"))


def test_tui_config_loads_default_examples_discovery_for_missing_config(tmp_path):
    config = load_tui_config(tmp_path / "missing.json")
    examples_path = str(Path(__file__).resolve().parents[1] / "examples")

    assert config.servers == []
    assert examples_path in config.agent_discovery.paths
    assert config.agent_discovery == default_agent_discovery_config()


def test_tui_config_load_save_discovery_sources(tmp_path):
    config_path = tmp_path / "servers.json"
    config = TuiConfig(
        servers=[ServerRef("alpha", "http://127.0.0.1:8765", True, True)],
        agent_discovery=AgentDiscoveryConfig(paths=[str(tmp_path / "agents")], modules=["tests.test_paglets_core"]),
    )

    save_tui_config(config, config_path)
    loaded = load_tui_config(config_path, base_path=tmp_path)

    assert loaded == config
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["agent_discovery"] == {
        "modules": ["tests.test_paglets_core"],
        "paths": [str(tmp_path / "agents")],
    }


def test_tui_config_normalizes_relative_discovery_paths(tmp_path):
    config_path = tmp_path / "servers.json"
    config_path.write_text(
        json.dumps({"agent_discovery": {"paths": ["agents"], "modules": ["tests.test_paglets_core"]}, "servers": []}),
        encoding="utf-8",
    )

    loaded = load_tui_config(config_path, base_path=tmp_path)

    assert loaded.agent_discovery.paths == [str((tmp_path / "agents").resolve(strict=False))]
    assert normalize_discovery_path("agents", base_path=tmp_path) == loaded.agent_discovery.paths[0]
