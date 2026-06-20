from __future__ import annotations

import json
from pathlib import Path

import pytest

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
    remove_server_ref,
    save_server_config,
    save_tui_config,
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
