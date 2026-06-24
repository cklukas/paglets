# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path

import pytest

from paglets.config.startup import LaunchConfigSyncResult, load_launch_config
from paglets.core.errors import HostError
from paglets.core.runtime_values import (
    ArrivalMode,
    EnvelopeKind,
    LaunchConfigSyncAction,
    ResidentLifecycle,
    ServiceScope,
)
from paglets.remote.references import PagletProxyRef
from paglets.remote.transfer import TransferTicket
from paglets.runtime.envelope import PagletEnvelope
from paglets.services.contracts import ServiceRecord
from paglets.services.resident import ResidentServiceSpec
from paglets.system.server_info import SERVER_INFO


def test_python_runtime_value_fields_reject_raw_strings(tmp_path: Path):
    proxy = PagletProxyRef("http://127.0.0.1:9", "agent")

    with pytest.raises(TypeError):
        ServiceRecord("svc", proxy, scope="mesh")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        TransferTicket("http://127.0.0.1:9", arrival_mode="inactive")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ResidentServiceSpec(SERVER_INFO, scope="mesh")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ResidentServiceSpec(SERVER_INFO, lifecycle="lazy")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        LaunchConfigSyncResult("copied", tmp_path / "launch.toml", "copied")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        PagletEnvelope(
            kind="clone",  # type: ignore[arg-type]
            agent_id="agent",
            agent_class_name="example:Agent",
            state_class_name="example:State",
            state={},
            source_host_name="alpha",
            source_host_address="http://alpha",
            target_host_name="beta",
            target_host_address="http://beta",
        )


def test_wire_runtime_value_fields_parse_and_serialize_strings():
    proxy = PagletProxyRef("http://127.0.0.1:9", "agent")
    record = ServiceRecord.from_wire(
        {
            "name": "svc",
            "proxy": proxy.to_wire(),
            "scope": "mesh",
        }
    )
    assert record.scope is ServiceScope.MESH
    assert record.to_wire()["scope"] == "mesh"

    ticket = TransferTicket.from_wire({"destination": "http://127.0.0.1:9", "arrival_mode": "inactive"})
    assert ticket.arrival_mode is ArrivalMode.INACTIVE
    assert ticket.to_wire()["arrival_mode"] == "inactive"

    envelope = PagletEnvelope.from_wire(
        {
            "kind": "clone",
            "agent_id": "agent",
            "agent_class_name": "example:Agent",
            "state_class_name": "example:State",
            "state": {},
            "source_host_name": "alpha",
            "source_host_address": "http://alpha",
            "target_host_name": "beta",
            "target_host_address": "http://beta",
        }
    )
    assert envelope.kind is EnvelopeKind.CLONE
    assert envelope.to_wire()["kind"] == "clone"


def test_invalid_wire_runtime_values_fail_clearly():
    proxy = PagletProxyRef("http://127.0.0.1:9", "agent")

    with pytest.raises(ValueError, match="scope must be one of"):
        ServiceRecord.from_wire({"name": "svc", "proxy": proxy.to_wire(), "scope": "global"})
    with pytest.raises(ValueError, match="arrival_mode must be one of"):
        TransferTicket.from_wire({"destination": "http://127.0.0.1:9", "arrival_mode": "parked"})
    with pytest.raises(ValueError, match="kind must be one of"):
        PagletEnvelope.from_wire(
            {
                "kind": "move",
                "agent_id": "agent",
                "agent_class_name": "example:Agent",
                "state_class_name": "example:State",
                "state": {},
                "source_host_name": "alpha",
                "source_host_address": "http://alpha",
                "target_host_name": "beta",
                "target_host_address": "http://beta",
            }
        )


def test_launch_config_string_values_become_enums(tmp_path: Path):
    path = tmp_path / "launch.toml"
    path.write_text(
        """
[launch]
demo_config_id = "test"
demo_config_version = "1"

[[resident_services]]
class = "paglets.system.server_info.agent:ServerInfoAgent"
lifecycle = "eager"
scope = "mesh"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_launch_config(path)

    assert config.resident_services[0].lifecycle is ResidentLifecycle.EAGER
    assert config.resident_services[0].scope is ServiceScope.MESH


def test_invalid_launch_config_enum_values_raise_host_error(tmp_path: Path):
    path = tmp_path / "launch.toml"
    path.write_text(
        """
[launch]
demo_config_id = "test"
demo_config_version = "1"

[[resident_services]]
class = "paglets.system.server_info.agent:ServerInfoAgent"
lifecycle = "sleeping"
scope = "mesh"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(HostError, match="resident service lifecycle"):
        load_launch_config(path)


def test_launch_config_sync_action_is_enum(tmp_path: Path):
    result = LaunchConfigSyncResult(LaunchConfigSyncAction.COPIED, tmp_path / "launch.toml", "copied")

    assert result.action is LaunchConfigSyncAction.COPIED
