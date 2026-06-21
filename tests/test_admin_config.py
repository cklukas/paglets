# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path

import paglets.admin as admin_module
from paglets.admin import (
    ServerRef,
    default_agent_discovery_config,
    normalize_discovery_path,
    normalize_server_url,
    select_reachable_entry_server,
)


def test_select_reachable_entry_server_falls_back_from_loopback_to_lan(monkeypatch):
    monkeypatch.setattr(admin_module, "detect_lan_host", lambda: "192.168.86.38")
    monkeypatch.setattr(admin_module, "discover_mesh_entry_servers", lambda timeout=0.75: [])
    monkeypatch.setattr(admin_module, "discover_lan_entry_servers", lambda *, ports: [])
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
        entry_name=None,
        client=FakeClient(),  # type: ignore[arg-type]
    )

    assert calls == ["http://127.0.0.1:8765/health", "http://192.168.86.38:8765/health"]
    assert selected == ServerRef("mac-studio", "http://192.168.86.38:8765", True, False)


def test_select_reachable_entry_server_uses_dynamic_lan_discovery(monkeypatch):
    monkeypatch.setattr(admin_module, "detect_lan_host", lambda: "192.168.86.38")
    monkeypatch.setattr(admin_module, "discover_mesh_entry_servers", lambda timeout=0.75: [])
    monkeypatch.setattr(
        admin_module,
        "discover_lan_entry_servers",
        lambda *, ports: [ServerRef("windows", "http://192.168.86.28:8765")],
    )
    calls: list[str] = []

    class FakeClient:
        def get_json(self, url: str, *, timeout: float | None = None):
            calls.append(url)
            if url == "http://192.168.86.28:8765/health":
                return {
                    "name": "windows",
                    "address": "http://192.168.86.28:8765",
                    "code_version": "dev",
                    "active_count": 0,
                    "inactive_count": 0,
                }
            raise OSError("not here")

    selected = select_reachable_entry_server(entry_name=None, client=FakeClient())  # type: ignore[arg-type]

    assert "http://192.168.86.28:8765/health" in calls
    assert selected == ServerRef("windows", "http://192.168.86.28:8765", True, False)


def test_select_reachable_entry_server_filters_by_discovered_entry_name(monkeypatch):
    monkeypatch.setattr(admin_module, "detect_lan_host", lambda: "192.168.86.38")
    monkeypatch.setattr(
        admin_module,
        "discover_mesh_entry_servers",
        lambda timeout=0.75: [
            ServerRef("alpha", "http://192.168.86.38:8765"),
            ServerRef("windows", "http://192.168.86.28:8765"),
        ],
    )
    monkeypatch.setattr(admin_module, "discover_lan_entry_servers", lambda *, ports: [])

    class FakeClient:
        def get_json(self, url: str, *, timeout: float | None = None):
            if url == "http://192.168.86.38:8765/health":
                return {"name": "alpha", "address": "http://192.168.86.38:8765"}
            if url == "http://192.168.86.28:8765/health":
                return {"name": "windows", "address": "http://192.168.86.28:8765"}
            raise OSError("not here")

    selected = select_reachable_entry_server(entry_name="windows", client=FakeClient())  # type: ignore[arg-type]

    assert selected == ServerRef("windows", "http://192.168.86.28:8765", True, False)


def test_normalize_server_url_adds_http_scheme():
    assert normalize_server_url("127.0.0.1:8765") == "http://127.0.0.1:8765"


def test_default_agent_discovery_config_points_at_examples_when_present():
    config = default_agent_discovery_config()
    examples_path = str(Path(__file__).resolve().parents[1] / "examples")

    assert examples_path in config.paths


def test_normalize_discovery_path_resolves_relative_paths(tmp_path):
    assert normalize_discovery_path("agents", base_path=tmp_path) == str((tmp_path / "agents").resolve(strict=False))
