# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from paglets.admin import AgentRecord, PagletsAdminClient, ServerRef
from tests.test_paglets_core import TravelAgent, TravelState, free_port
from paglets import Host, Message


def test_admin_client_manages_agents_across_two_hosts():
    alpha = Host(name="alpha", host="127.0.0.1", port=free_port(), mesh=False, mesh_multicast=False)
    beta = Host(name="beta", host="127.0.0.1", port=free_port(), mesh=False, mesh_multicast=False)
    alpha.start_background()
    beta.start_background()
    try:
        alpha_ref = ServerRef("alpha", alpha.address)
        beta_ref = ServerRef("beta", beta.address)
        admin = PagletsAdminClient([alpha_ref, beta_ref], client=alpha.client)

        health = admin.health_all()
        assert [(status.name, status.reachable) for status in health] == [("alpha", True), ("beta", True)]
        hosts = admin.list_hosts(alpha_ref)
        assert [(host.name, host.url, host.online) for host in hosts] == [("alpha", alpha.address, True)]
        assert hosts[0].code_version

        proxy = admin.create_agent(
            alpha_ref,
            "tests.test_paglets_core:TravelAgent",
            "tests.test_paglets_core:TravelState",
            {},
            init="seed",
        )
        agent_id = proxy["agent_id"]
        record = _find(admin.list_agents_all(), agent_id, "alpha")

        assert admin.send(record, Message("remember", {"value": "hello"})) == "remembered:hello"
        assert admin.get_agent_state(record)["state"]["last_message"] == "hello"

        clone = admin.clone(record, beta.address)
        assert clone["host_url"] == beta.address

        moved = admin.dispatch(record, beta.address)
        assert moved == {"host_url": beta.address, "agent_id": agent_id}
        remote = _find(admin.list_agents_all(), agent_id, "beta")

        returned = admin.retract(remote, alpha.address)
        assert returned == {"host_url": alpha.address, "agent_id": agent_id}
        local = _find(admin.list_agents_all(), agent_id, "alpha")

        admin.deactivate(local)
        inactive_state = admin.get_agent_state(local)
        assert inactive_state["active"] is False
        assert inactive_state["state"]["last_message"] == "hello"

        admin.activate(local)
        local = _find(admin.list_agents_all(), agent_id, "alpha")
        admin.dispose(local)
        assert not [agent for agent in admin.list_agents_all() if agent.agent_id == agent_id]
    finally:
        beta.stop()
        alpha.stop()


def _find(records: list[AgentRecord], agent_id: str, server_name: str) -> AgentRecord:
    for record in records:
        if record.agent_id == agent_id and record.server_name == server_name:
            return record
    raise AssertionError(f"missing {agent_id} on {server_name}")
