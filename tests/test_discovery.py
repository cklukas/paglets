# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path

from paglets.runtime.host import Host
from paglets.remote.admin import AgentDiscoveryConfig, PagletsAdminClient, ServerRef, default_agent_discovery_config
from paglets.tooling.discovery import discover_agent_classes
from tests.test_paglets_core import free_port


def test_default_examples_discovery_finds_paglet_classes():
    result = discover_agent_classes(default_agent_discovery_config())
    class_names = {record.class_name for record in result.agent_classes}

    assert result.errors == []
    assert "examples.start_hello_demo:FirstPaglet" in class_names
    assert "examples.start_hello_demo:VanillaPaglet" in class_names
    assert "examples.mobility_events_demo:MobilityEventsPaglet" in class_names
    assert "examples.finder_demo:TravellerPaglet" in class_names
    assert "examples.itinerary_demo:CirculateAgent" in class_names
    assert "examples.clone_workers_demo:SumWorkerAgent" in class_names
    assert "examples.simple_master_slave_demo:MasterAgent" in class_names
    assert all("State" not in record.display_name for record in result.agent_classes)


def test_module_discovery_returns_state_details_and_templates():
    result = discover_agent_classes(AgentDiscoveryConfig(paths=[], modules=["tests.test_paglets_core"]))
    records = {record.class_name: record for record in result.agent_classes}

    travel = records["tests.test_paglets_core:TravelAgent"]
    assert travel.state_class_name == "tests.test_paglets_core:TravelState"
    assert travel.state_template == {"events": [], "last_message": None}
    assert travel.required_state_fields == []

    clone = records["tests.test_paglets_core:CloneAgent"]
    assert clone.state_class_name == "tests.test_paglets_core:CloneState"
    assert clone.state_template == {"label": "original", "events": []}


def test_path_discovery_reports_bad_sources_without_aborting(tmp_path):
    good_path = Path(__file__).resolve().parents[1] / "examples"
    result = discover_agent_classes(
        AgentDiscoveryConfig(
            paths=[str(good_path), str(tmp_path / "missing")],
            modules=["not_a_real_paglets_module"],
        )
    )

    assert any(record.class_name == "examples.start_hello_demo:FirstPaglet" for record in result.agent_classes)
    assert any("not_a_real_paglets_module" in error for error in result.errors)
    assert any("Discovery path does not exist" in error for error in result.errors)


def test_discovered_example_paglet_can_be_created_through_http():
    result = discover_agent_classes(default_agent_discovery_config())
    record = next(
        record
        for record in result.agent_classes
        if record.class_name == "examples.start_hello_demo:FirstPaglet"
    )
    host = Host(name="alpha", host="127.0.0.1", port=free_port())
    host.start_background()
    try:
        server = ServerRef("alpha", host.address)
        admin = PagletsAdminClient([server], client=host.client)
        proxy = admin.create_agent(
            server,
            record.class_name,
            record.state_class_name,
            record.state_template,
        )
        agents = admin.list_agents_all()
    finally:
        host.stop()

    assert proxy["host_url"] == host.address
    assert any(agent.agent_id == proxy["agent_id"] and agent.class_name == record.class_name for agent in agents)
