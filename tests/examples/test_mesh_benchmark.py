# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paglets.examples.mesh_benchmark import (
    ClockOffsetSample,
    MeshBenchmarkHost,
    MeshBenchmarkRequest,
    MeshBenchmarkSummary,
    MeshTravelRecord,
    MessageTimingSummary,
    aggregate_clock_offsets,
    aggregate_matrix,
    aggregate_message_timings,
    aggregate_payload_transfer_speeds,
    benchmark_transfer_ticket,
    build_route_edges,
    build_summary,
    entry_time_for_local_reference,
    local_minus_entry_offset,
    parse_size,
)
from paglets.examples.mesh_benchmark.cli import _format_markdown, _format_transfer_speed, _parser
from paglets.examples.mesh_benchmark.cli import main as mesh_benchmark_main
from paglets.remote.admin import ServerRef
from paglets.runtime.host import Host
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire
from tests.support import free_port


def test_route_generation_covers_directed_pairs_with_self_edges():
    hosts = [
        MeshBenchmarkHost("alpha", "http://alpha"),
        MeshBenchmarkHost("beta", "http://beta"),
        MeshBenchmarkHost("gamma", "http://gamma"),
    ]

    edges = build_route_edges(hosts, repeats=2, include_self=True)

    assert len(edges) == 3 * 3 * 2
    pairs = [(edge.source_name, edge.target_name, edge.repeat) for edge in edges]
    for repeat in range(2):
        assert {(source.name, target.name) for source in hosts for target in hosts} == {
            (source, target) for source, target, item_repeat in pairs if item_repeat == repeat
        }
    assert edges[0].source_name == "alpha"
    assert edges[-1].target_name == "alpha"


def test_route_generation_skips_self_edges_when_disabled():
    hosts = [
        MeshBenchmarkHost("alpha", "http://alpha"),
        MeshBenchmarkHost("beta", "http://beta"),
        MeshBenchmarkHost("gamma", "http://gamma"),
    ]

    edges = build_route_edges(hosts, repeats=3, include_self=False)

    assert len(edges) == 3 * (3 - 1) * 3
    assert all(edge.source_name != edge.target_name for edge in edges)


def test_directional_aggregation_keeps_opposite_directions_separate():
    hosts = [MeshBenchmarkHost("alpha", "http://alpha"), MeshBenchmarkHost("beta", "http://beta")]
    records = [
        _record("alpha", "beta", 0.010),
        _record("alpha", "beta", 0.020),
        _record("beta", "alpha", 0.100),
    ]

    matrix = aggregate_matrix(records, hosts)

    assert matrix["alpha"]["beta"] == 0.015
    assert matrix["beta"]["alpha"] == 0.100
    assert "alpha" not in matrix["alpha"]


def test_parse_size_accepts_binary_units_and_zero():
    assert parse_size("0") == 0
    assert parse_size("64K") == 64 * 1024
    assert parse_size("128K") == 128 * 1024
    assert parse_size("1M") == 1024 * 1024


def test_cli_default_digits_is_one_decimal_place():
    args = _parser().parse_args([])

    assert args.digits == 1


def test_benchmark_transfer_ticket_uses_request_timeout_for_large_payload_moves():
    request = MeshBenchmarkRequest(timeout_seconds=180.0)

    ticket = benchmark_transfer_ticket("http://beta", request)

    assert ticket.destination == "http://beta"
    assert ticket.timeout == 180.0


def test_mesh_benchmark_dataclasses_round_trip_through_wire():
    request = MeshBenchmarkRequest(repeats=2, payload_size_bytes=64 * 1024, include_self=False, clock_probes=3)
    assert dataclass_from_wire(MeshBenchmarkRequest, dataclass_to_wire(request)) == request

    summary = MeshBenchmarkSummary(
        run_id="run",
        entry_host_name="alpha",
        entry_host_url="http://alpha",
        hosts=[MeshBenchmarkHost("alpha", "http://alpha")],
        records=[_record("alpha", "alpha", 0.001)],
        message_timings=[
            MessageTimingSummary(
                host_name="alpha",
                host_url="http://alpha",
                entry_host_name="alpha",
                entry_host_url="http://alpha",
                sample_count=1,
                median_rtt_seconds=0.001,
                mean_rtt_seconds=0.001,
                best_rtt_seconds=0.001,
                worst_rtt_seconds=0.001,
            )
        ],
    )
    assert dataclass_from_wire(MeshBenchmarkSummary, dataclass_to_wire(summary)) == summary


def test_clock_offset_aggregation_reports_median_mean_and_best_rtt():
    samples = [
        _sample("beta", 0.003, 0.050),
        _sample("beta", 0.009, 0.010),
        _sample("beta", 0.006, 0.020),
    ]

    [summary] = aggregate_clock_offsets(samples)

    assert summary.sample_count == 3
    assert summary.median_offset_seconds == 0.006
    assert summary.mean_offset_seconds == pytest.approx(0.006)
    assert summary.best_rtt_offset_seconds == 0.009
    assert summary.best_rtt_seconds == 0.010


def test_local_reference_converts_to_entry_time_using_best_rtt_offset():
    samples = [
        _sample("beta", 0.250, 0.100),
        _sample("beta", 0.240, 0.010),
        _sample("beta", 0.260, 0.050),
    ]

    assert entry_time_for_local_reference(100.0, samples) == pytest.approx(99.760)


def test_ntp_style_offset_is_local_minus_entry():
    offset = local_minus_entry_offset(
        local_send=100.000,
        local_receive=100.020,
        entry_receive=99.760,
        entry_send=99.760,
    )

    assert offset == pytest.approx(0.250)


def test_message_timing_aggregation_reports_median_mean_best_and_worst_rtt():
    samples = [
        _sample("beta", 0.003, 0.003),
        _sample("beta", 0.009, 0.009),
        _sample("beta", 0.006, 0.006),
    ]

    [summary] = aggregate_message_timings(samples)

    assert summary.sample_count == 3
    assert summary.median_rtt_seconds == 0.006
    assert summary.mean_rtt_seconds == pytest.approx(0.006)
    assert summary.best_rtt_seconds == 0.003
    assert summary.worst_rtt_seconds == 0.009


def test_payload_speed_aggregation_groups_self_and_other_by_destination_host():
    hosts = [MeshBenchmarkHost("alpha", "http://alpha"), MeshBenchmarkHost("beta", "http://beta")]
    records = [
        _record("alpha", "beta", 0.5, payload_size_bytes=1024 * 1024),
        _record("alpha", "beta", 1.5, payload_size_bytes=1024 * 1024),
        _record("beta", "beta", 2.0, payload_size_bytes=1024 * 1024),
        _record("alpha", "alpha", 0.0, payload_size_bytes=1024 * 1024),
        _record("beta", "alpha", 1.0, payload_size_bytes=0),
    ]

    speeds = aggregate_payload_transfer_speeds(records, hosts)

    by_key = {(speed.host_name, speed.relation): speed for speed in speeds}
    assert by_key[("beta", "other")].sample_count == 2
    assert by_key[("beta", "other")].payload_bytes == 2 * 1024 * 1024
    assert by_key[("beta", "other")].elapsed_seconds == pytest.approx(2.0)
    assert by_key[("beta", "other")].bytes_per_second == pytest.approx(1024 * 1024)
    assert by_key[("beta", "self")].sample_count == 1
    assert by_key[("beta", "self")].bytes_per_second == pytest.approx(512 * 1024)
    assert ("alpha", "self") not in by_key
    assert ("alpha", "other") not in by_key


def test_payload_speed_format_reports_binary_bytes_and_decimal_bits():
    assert _format_transfer_speed(1024 * 1024, 1) == "1.0 MB/s / 8.4 Mbit/s"
    assert _format_transfer_speed(1024, 1) == "1.0 KB/s / 8.2 kbit/s"
    assert _format_transfer_speed(125, 1) == "125.0 B/s / 1.0 kbit/s"


def test_summary_separates_setup_measured_travel_and_route_overhead():
    hosts = [MeshBenchmarkHost("alpha", "http://alpha"), MeshBenchmarkHost("beta", "http://beta")]
    records = [_record("alpha", "beta", 0.010), _record("beta", "alpha", 0.020)]

    summary = build_summary(
        run_id="run",
        entry_host=hosts[0],
        hosts=hosts,
        records=records,
        clock_samples=[],
        measured_round_trip_seconds=0.050,
        setup_seconds=0.200,
        overall_benchmark_seconds=0.300,
    )

    assert summary.total_elapsed_seconds == pytest.approx(0.030)
    assert summary.measured_overhead_seconds == pytest.approx(0.020)
    assert summary.setup_seconds == pytest.approx(0.200)
    assert summary.measured_round_trip_seconds == pytest.approx(0.050)
    assert summary.overall_benchmark_seconds == pytest.approx(0.300)


def test_markdown_rendering_aligns_directional_matrix_and_missing_diagonal():
    hosts = [MeshBenchmarkHost("alpha", "http://alpha"), MeshBenchmarkHost("beta", "http://beta")]
    samples = [_sample("beta", 0.002, 0.001)]
    summary = MeshBenchmarkSummary(
        run_id="run",
        entry_host_name="alpha",
        entry_host_url="http://alpha",
        hosts=hosts,
        records=[
            _record("alpha", "beta", 0.001234, payload_size_bytes=1024),
            _record("beta", "alpha", 0.002345, payload_size_bytes=1024),
        ],
        matrix_seconds={"alpha": {"beta": 0.001234}, "beta": {"alpha": 0.002345}},
        clock_offsets=aggregate_clock_offsets(samples),
        message_timings=aggregate_message_timings(samples),
        payload_transfer_speeds=aggregate_payload_transfer_speeds(
            [
                _record("alpha", "beta", 0.001234, payload_size_bytes=1024),
                _record("beta", "alpha", 0.002345, payload_size_bytes=1024),
            ],
            hosts,
        ),
        movement_count=2,
        measured_round_trip_seconds=0.010,
        setup_seconds=0.004,
        total_elapsed_seconds=0.003579,
        measured_overhead_seconds=0.006421,
        overall_benchmark_seconds=0.020,
        average_elapsed_seconds=0.0017895,
    )

    output = _format_markdown(summary, digits=3, include_self=False)

    assert "unit: ms" in output
    assert "| from \\ to | alpha |  beta |" in output
    assert "| alpha     |     - | 1.234 |" in output
    assert "| beta      | 2.345 |     - |" in output
    assert "average travel time: 1.790 ms" in output
    assert "sum measured travel time: 3.579 ms" in output
    assert "measured round trip time: 10.000 ms" in output
    assert "measured route overhead: 6.421 ms" in output
    assert "setup time before first movement: 4.000 ms" in output
    assert "measured movements: 2" in output
    assert "clock offsets vs entry:" in output
    assert "message passing times vs entry:" in output
    assert "| host | samples | median round trip | average round trip | best round trip | worst round trip |" in output
    assert "| beta |       1 |          1.000 ms |           1.000 ms |        1.000 ms |         1.000 ms |" in output
    assert "average payload transfer speed to other hosts: alpha (" in output
    assert "KB/s / " in output
    assert "Mbit/s" in output
    assert "beta (" in output
    assert "average payload transfer speed to self hosts:" not in output
    assert output.endswith("overall benchmark time: 20.000 ms")


def test_markdown_omits_payload_speed_lines_when_payload_is_zero():
    hosts = [MeshBenchmarkHost("alpha", "http://alpha")]
    summary = build_summary(
        run_id="run",
        entry_host=hosts[0],
        hosts=hosts,
        records=[_record("alpha", "alpha", 0.001, payload_size_bytes=0)],
        clock_samples=[],
        measured_round_trip_seconds=0.001,
        overall_benchmark_seconds=0.002,
    )

    output = _format_markdown(summary, digits=1, include_self=True)

    assert "average payload transfer speed" not in output


def test_paglets_mesh_benchmark_json_collects_two_host_directional_mesh(tmp_path: Path, capsys, monkeypatch):
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh_version="mesh-benchmark-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    beta = Host(
        "beta",
        host="127.0.0.1",
        port=free_port(),
        peers=[alpha.address],
        mesh_version="mesh-benchmark-test",
        mesh_multicast=False,
        persistence_dir=tmp_path / "beta",
    )
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        monkeypatch.setattr(
            "paglets.examples.mesh_benchmark.cli._select_entry_server",
            lambda *, entry_name, client: ServerRef("alpha", alpha.address),
        )

        result = mesh_benchmark_main(
            [
                "--entry",
                "alpha",
                "--timeout",
                "20",
                "--json",
                "--output",
                str(tmp_path / "mesh-benchmark.json"),
                "--repeats",
                "1",
                "--payload-size",
                "0",
                "--clock-probes",
                "1",
            ]
        )

        assert result == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["accepted"] is True
        assert payload["output_path"] == str(tmp_path / "mesh-benchmark.json")
        _wait_for(lambda: (tmp_path / "mesh-benchmark.json").read_text(encoding="utf-8").strip())
        payload = json.loads((tmp_path / "mesh-benchmark.json").read_text(encoding="utf-8"))["summary"]
        assert payload["movement_count"] == 2 * 2
        assert {host["name"] for host in payload["hosts"]} == {"alpha", "beta"}
        assert set(payload["matrix_seconds"]) == {"alpha", "beta"}
        assert set(payload["matrix_seconds"]["alpha"]) == {"alpha", "beta"}
        assert set(payload["matrix_seconds"]["beta"]) == {"alpha", "beta"}
        assert payload["message_timings"]
        assert payload["overall_benchmark_seconds"] >= payload["measured_round_trip_seconds"]
        assert payload["errors"] == {}
    finally:
        beta.stop()
        alpha.stop()


def test_paglets_mesh_benchmark_uses_relay_without_intermediate_arrival(tmp_path: Path, capsys, monkeypatch):
    port = free_port()
    public_url = f"http://127.0.0.1:{port}/paglets"
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=port,
        api_key="secret",
        public_url=public_url,
        mesh_version="mesh-benchmark-relay-test",
        mesh_multicast=False,
        mesh_lan_discovery=False,
        persistence_dir=tmp_path / "alpha",
    )
    beta = Host(
        "beta",
        api_key="secret",
        connect_to=public_url,
        mesh_version="mesh-benchmark-relay-test",
        mesh_multicast=False,
        mesh_lan_discovery=False,
        persistence_dir=tmp_path / "beta",
    )
    gamma = Host(
        "gamma",
        api_key="secret",
        connect_to=public_url,
        mesh_version="mesh-benchmark-relay-test",
        mesh_multicast=False,
        mesh_lan_discovery=False,
        persistence_dir=tmp_path / "gamma",
    )
    alpha.start_background()
    beta.start_background()
    gamma.start_background()
    try:
        _wait_for(lambda: alpha.mesh.lookup("beta") is not None and alpha.mesh.lookup("gamma") is not None)
        monkeypatch.setenv("PAGLETS_API_KEY", "secret")
        monkeypatch.setattr(
            "paglets.examples.mesh_benchmark.cli._select_entry_server",
            lambda *, entry_name, client: ServerRef("alpha", public_url),
        )

        result = mesh_benchmark_main(
            [
                "--entry",
                "alpha",
                "--timeout",
                "30",
                "--json",
                "--output",
                str(tmp_path / "mesh-benchmark-relay.json"),
                "--repeats",
                "1",
                "--payload-size",
                "0",
                "--clock-probes",
                "1",
                "--exclude-self",
            ]
        )

        assert result == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["accepted"] is True
        assert payload["output_path"] == str(tmp_path / "mesh-benchmark-relay.json")
        _wait_for(lambda: (tmp_path / "mesh-benchmark-relay.json").read_text(encoding="utf-8").strip())
        payload = json.loads((tmp_path / "mesh-benchmark-relay.json").read_text(encoding="utf-8"))["summary"]
        records = payload["records"]
        assert payload["errors"] == {}
        assert payload["movement_count"] == 3 * (3 - 1)
        assert {host["name"] for host in payload["hosts"]} == {"alpha", "beta", "gamma"}
        assert {(record["source_name"], record["target_name"]) for record in records} == {
            ("alpha", "beta"),
            ("alpha", "gamma"),
            ("beta", "alpha"),
            ("beta", "gamma"),
            ("gamma", "alpha"),
            ("gamma", "beta"),
        }
        assert all("/relay/hosts/alpha" not in record["source_url"] for record in records)
        assert all("/relay/hosts/alpha" not in record["target_url"] for record in records)

        traveler_arrivals_on_alpha = [
            event
            for event in alpha.list_events(since=0, limit=1000)
            if event.kind == "arrival"
            and event.class_name == "paglets.examples.mesh_benchmark.agent:MeshBenchmarkTravelerAgent"
        ]
        logical_alpha_arrivals = [record for record in records if record["target_name"] == "alpha"]
        assert len(traveler_arrivals_on_alpha) == len(logical_alpha_arrivals)
    finally:
        gamma.stop()
        beta.stop()
        alpha.stop()


def _wait_for(predicate, *, timeout: float = 5.0) -> None:
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for condition")


def _record(source: str, target: str, elapsed: float, *, payload_size_bytes: int = 0) -> MeshTravelRecord:
    return MeshTravelRecord(
        run_id="run",
        sequence=0,
        repeat=0,
        source_name=source,
        source_url=f"http://{source}",
        target_name=target,
        target_url=f"http://{target}",
        source_wall_start=1.0,
        source_wall_end=1.0 + elapsed,
        elapsed_seconds=elapsed,
        payload_size_bytes=payload_size_bytes,
    )


def _sample(host: str, offset: float, rtt: float) -> ClockOffsetSample:
    return ClockOffsetSample(
        host_name=host,
        host_url=f"http://{host}",
        entry_host_name="alpha",
        entry_host_url="http://alpha",
        offset_seconds=offset,
        rtt_seconds=rtt,
        sampled_at=1.0,
    )
