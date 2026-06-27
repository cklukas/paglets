# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
import time
from types import SimpleNamespace
from typing import Annotated

import typer

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.examples.analysis_jobs.agent import (
    ANALYSIS_CAMPAIGN_START,
    ANALYSIS_CAMPAIGN_SUMMARY,
    DEFAULT_CPU_CORES,
    DEFAULT_DB_LOCK_TIMEOUT_SECONDS,
    DEFAULT_ESTIMATOR_TREES,
    DEFAULT_FEATURE_COUNT,
    DEFAULT_MEMORY_BYTES,
    DEFAULT_ROW_COUNT,
    DEFAULT_TARGET_RUNTIME_SECONDS,
    DEFAULT_TASK_COUNT,
    DEFAULT_TEMP_STORAGE_BYTES,
    AnalysisCampaignRequest,
    AnalysisCampaignStartRequest,
    default_result_db,
)
from paglets.examples.compute import cli as pi_backend
from paglets.examples.compute.models import DEFAULT_STREAM_CHUNK_DIGITS, PiComputeRequest
from paglets.examples.file_grabber import cli as file_backend
from paglets.examples.mesh_benchmark import cli as mesh_backend
from paglets.examples.mesh_benchmark.analysis import normalize_request
from paglets.examples.mesh_benchmark.analysis import parse_size as parse_payload_size
from paglets.examples.mesh_benchmark.models import (
    DEFAULT_CLOCK_PROBES,
    DEFAULT_DIGITS,
    DEFAULT_TIMEOUT_SECONDS,
    MeshBenchmarkRequest,
    MeshBenchmarkSummary,
)
from paglets.examples.performance import cli as perf_backend
from paglets.examples.performance.kernels import parse_size
from paglets.examples.performance.models import (
    DEFAULT_BENCHMARK_DURATION_SECONDS,
    DEFAULT_DISK_SIZE_BYTES,
    DEFAULT_LOCK_TIMEOUT_SECONDS,
    BenchmarkRequest,
)
from paglets.patterns.operations import OperationClient
from paglets.patterns.tasks import TaskStatus
from paglets.remote.admin import PagletsAdminClient, select_reachable_entry_server
from paglets.remote.client import HostClient
from paglets.remote.proxy import PagletProxy
from paglets.serialization.codec import dataclass_from_wire, dataclass_to_wire

from .console import console, fail, print_json

app = typer.Typer(help="Run packaged Paglets examples.", no_args_is_help=True)
file_app = typer.Typer(help="Copy or move one file between two hosts.", no_args_is_help=True)
app.add_typer(file_app, name="file")


@app.command()
def pi(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    start: Annotated[int, typer.Option("--start", help="Zero-based decimal digit position after the point.")] = 0,
    digits: Annotated[int, typer.Option("--digits", help="Number of decimal digits to compute.")] = 16,
    batch_size: Annotated[int, typer.Option("--batch-size", help="Chudnovsky terms per worker batch.")] = 1,
    max_in_flight: Annotated[int, typer.Option("--max-in-flight", help="Global in-flight batch cap.")] = 0,
    max_workers_per_host: Annotated[int, typer.Option("--max-workers-per-host", help="Per-host worker cap.")] = 0,
    timeout: Annotated[float, typer.Option("--timeout", help="Whole-job timeout in seconds; 0 disables it.")] = 0.0,
    stream_chunk_size: Annotated[
        int, typer.Option("--stream-chunk-size", help="Maximum newly available decimal digits per text poll.")
    ] = DEFAULT_STREAM_CHUNK_DIGITS,
    request_timeout: Annotated[
        float, typer.Option("--request-timeout", help="HTTP request timeout for coordinator calls.")
    ] = 300.0,
    max_load_per_cpu: Annotated[float, typer.Option("--max-load-per-cpu", help="Maximum 1-minute load per CPU.")] = 1.0,
    max_cpu_percent: Annotated[float, typer.Option("--max-cpu-percent", help="Maximum sampled CPU percent.")] = 100.0,
    mem: Annotated[str, typer.Option("--mem", help="Minimum available RAM, e.g. 512M.")] = "0",
    disk: Annotated[str, typer.Option("--disk", help="Minimum free work storage, e.g. 1G.")] = "0",
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help=f"API key environment variable; defaults to {DEFAULT_API_KEY_ENV}."),
    ] = None,
) -> None:
    try:
        client = HostClient(timeout=max(1.0, request_timeout), api_key=resolve_api_key(api_key_env))
        entry_ref = select_reachable_entry_server(entry_name=entry, client=client)
        request = PiComputeRequest(
            start=max(0, start),
            digits=max(0, digits),
            batch_size=max(1, batch_size),
            max_in_flight=max(0, max_in_flight),
            max_workers_per_host=max(0, max_workers_per_host),
            timeout=max(0.0, timeout),
            max_load_per_cpu=max_load_per_cpu,
            max_cpu_percent=max_cpu_percent,
            min_memory_available_bytes=max(0, pi_backend._parse_size(mem)),
            min_work_free_bytes=max(0, pi_backend._parse_size(disk)),
        )
        summary = (
            pi_backend._run(entry_ref, request, client=client)
            if json_output
            else pi_backend._run_stream(entry_ref, request, client=client, stream_chunk_size=max(0, stream_chunk_size))
        )
        if json_output:
            print_json(summary)
        raise typer.Exit(0 if summary.get("done") and not summary.get("errors") else 1)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets examples pi", exc)


@app.command()
def analysis(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry/home host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout in seconds.")] = 20.0,
    tasks: Annotated[int, typer.Option("--tasks", help="Number of analysis jobs.")] = DEFAULT_TASK_COUNT,
    db: Annotated[str, typer.Option("--db", help="SQLite result DB path on the home host.")] = default_result_db(),
    rows: Annotated[int, typer.Option("--rows", help="Synthetic rows per job.")] = DEFAULT_ROW_COUNT,
    features: Annotated[int, typer.Option("--features", help="Synthetic features per job.")] = DEFAULT_FEATURE_COUNT,
    trees: Annotated[int, typer.Option("--trees", help="Random forest tree count.")] = DEFAULT_ESTIMATOR_TREES,
    target_runtime: Annotated[
        float, typer.Option("--target-runtime", help="Minimum compute duration per job in seconds.")
    ] = DEFAULT_TARGET_RUNTIME_SECONDS,
    memory: Annotated[str, typer.Option("--memory", help="Requested RAM per job.")] = str(DEFAULT_MEMORY_BYTES),
    cpu_cores: Annotated[
        int, typer.Option("--cpu-cores", help="Requested logical CPU cores per job.")
    ] = DEFAULT_CPU_CORES,
    temp_storage: Annotated[str, typer.Option("--temp-storage", help="Requested temp storage per job.")] = str(
        DEFAULT_TEMP_STORAGE_BYTES
    ),
    db_lock_timeout: Annotated[
        float, typer.Option("--db-lock-timeout", help="Seconds to wait for SQLite write lock.")
    ] = DEFAULT_DB_LOCK_TIMEOUT_SECONDS,
    wait: Annotated[float, typer.Option("--wait", help="Seconds to wait for seeder completion.")] = 5.0,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = HostClient(timeout=timeout, api_key=resolve_api_key(api_key_env))
        entry_ref = select_reachable_entry_server(entry_name=entry, client=client)
        request = AnalysisCampaignRequest(
            task_count=max(1, tasks),
            db_path=db,
            row_count=max(100, rows),
            feature_count=max(4, features),
            estimator_trees=max(1, trees),
            target_runtime_seconds=max(0.0, target_runtime),
            cpu_cores=max(1, cpu_cores),
            memory_bytes=max(0, pi_backend._parse_size(memory)),
            temp_storage_bytes=max(0, pi_backend._parse_size(temp_storage)),
            db_lock_timeout_seconds=max(0.0, db_lock_timeout),
        )
        admin = PagletsAdminClient([entry_ref], client=client)
        proxy_wire = admin.create_agent(
            entry_ref,
            "paglets.examples.analysis_jobs.agent:CampaignSeederPaglet",
            "paglets.examples.analysis_jobs.agent:CampaignSeederState",
            {},
        )
        operations = OperationClient(PagletProxy.from_wire(proxy_wire, client))
        summary = operations.call(
            ANALYSIS_CAMPAIGN_START, AnalysisCampaignStartRequest(dataclass_to_wire(request)), timeout=timeout
        )
        deadline = time.monotonic() + max(0.0, wait)
        while time.monotonic() < deadline:
            summary = operations.call(ANALYSIS_CAMPAIGN_SUMMARY, timeout=timeout)
            if summary.done:
                break
            time.sleep(0.5)
        console.print(json.dumps(dataclass_to_wire(summary), indent=2, sort_keys=True))
    except Exception as exc:
        fail("paglets examples analysis", exc)


@app.command()
def perf(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Seconds to wait for replies.")] = 120.0,
    duration: Annotated[
        float, typer.Option("--duration", help="Seconds per CPU/memory kernel.")
    ] = DEFAULT_BENCHMARK_DURATION_SECONDS,
    disk_size: Annotated[str, typer.Option("--disk-size", help="Temporary file size per tested volume.")] = str(
        DEFAULT_DISK_SIZE_BYTES
    ),
    workers: Annotated[int, typer.Option("--workers", help="Multi-core worker count; 0 uses logical CPU count.")] = 0,
    path: Annotated[list[str] | None, typer.Option("--path", help="Disk path to benchmark; repeatable.")] = None,
    cpu: Annotated[bool, typer.Option("--cpu/--no-cpu", help="Run CPU benchmarks.")] = True,
    memory: Annotated[bool, typer.Option("--memory/--no-memory", help="Run memory benchmarks.")] = True,
    disk: Annotated[bool, typer.Option("--disk/--no-disk", help="Run disk benchmarks.")] = True,
    lock_timeout: Annotated[
        float, typer.Option("--lock-timeout", help="Seconds to wait for local benchmark lock.")
    ] = DEFAULT_LOCK_TIMEOUT_SECONDS,
    verbose: Annotated[bool, typer.Option("--verbose", help="Print skipped disk targets and diagnostics.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = HostClient(timeout=max(1.0, timeout + 10.0), api_key=resolve_api_key(api_key_env))
        entry_ref = select_reachable_entry_server(entry_name=entry, client=client)
        request = BenchmarkRequest(
            include_cpu=cpu,
            include_memory=memory,
            include_disk=disk,
            duration_seconds=max(0.01, duration),
            disk_size_bytes=parse_size(disk_size),
            workers=max(0, workers),
            paths=list(path or []),
            lock_timeout_seconds=max(0.0, lock_timeout),
        )
        summary = perf_backend._collect(entry_ref, request, timeout=timeout, client=client)
        if json_output:
            print_json(summary)
        else:
            perf_backend._print_text(summary, verbose=verbose)
        raise typer.Exit(1 if perf_backend._has_failures(summary) else 0)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets examples perf", exc)


@app.command("mesh-benchmark")
def mesh_benchmark(
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry host name.")] = None,
    timeout: Annotated[
        float, typer.Option("--timeout", help="Seconds to wait for completion.")
    ] = DEFAULT_TIMEOUT_SECONDS,
    repeats: Annotated[int, typer.Option("--repeats", help="Repeat the directed mesh route this many times.")] = 1,
    payload_size: Annotated[str, typer.Option("--payload-size", help="Random ASCII payload size, e.g. 64K.")] = "0",
    exclude_self: Annotated[bool, typer.Option("--exclude-self", help="Skip self-pair movements.")] = False,
    digits: Annotated[
        int, typer.Option("--digits", help="Digits after the decimal point in text output.")
    ] = DEFAULT_DIGITS,
    clock_probes: Annotated[
        int, typer.Option("--clock-probes", help="Clock request/reply probes per arrival host.")
    ] = DEFAULT_CLOCK_PROBES,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    try:
        client = HostClient(timeout=max(1.0, timeout + 10.0), api_key=resolve_api_key(api_key_env))
        entry_ref = select_reachable_entry_server(entry_name=entry, client=client)
        request = normalize_request(
            MeshBenchmarkRequest(
                repeats=repeats,
                payload_size_bytes=parse_payload_size(payload_size),
                include_self=not exclude_self,
                timeout_seconds=timeout,
                digits=digits,
                clock_probes=clock_probes,
            )
        )
        result = mesh_backend._run(entry_ref, request, client=client)
        summary_payload = dict(result.get("summary") or {})
        if json_output:
            print_json(summary_payload)
        elif mesh_backend._is_summary_payload(summary_payload):
            summary = dataclass_from_wire(MeshBenchmarkSummary, summary_payload)
            console.print(
                mesh_backend._format_markdown(summary, digits=request.digits, include_self=request.include_self)
            )
        elif summary_payload.get("errors"):
            mesh_backend._print_errors(dict(summary_payload["errors"]))
        else:
            fail("paglets examples mesh-benchmark", RuntimeError("no summary returned"), code=1)
        errors = dict(result.get("errors") or {})
        errors.update(dict(summary_payload.get("errors") or {}))
        raise typer.Exit(1 if errors else 0)
    except typer.Exit:
        raise
    except Exception as exc:
        fail("paglets examples mesh-benchmark", exc)


@file_app.command()
def push(
    source: Annotated[str, typer.Argument(help="Source file path on the entry host.")],
    remote: Annotated[str, typer.Option("--remote", help="Remote host name or URL.")],
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry/start host name.")] = None,
    request_timeout: Annotated[
        float, typer.Option("--request-timeout", help="HTTP request timeout in seconds.")
    ] = 60.0,
    dest: Annotated[str, typer.Option("--dest", help="Destination path; defaults to source basename.")] = "",
    mode: Annotated[str, typer.Option("--mode", help="Transfer mode: copy or move.")] = "copy",
    dry: Annotated[bool, typer.Option("--dry", help="Only stat the source and report the plan.")] = False,
    overwrite: Annotated[bool, typer.Option("--overwrite", help="Replace the destination if it exists.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    _file("push", source, remote, entry, request_timeout, dest, mode, dry, overwrite, json_output, api_key_env)


@file_app.command()
def pull(
    source: Annotated[str, typer.Argument(help="Source file path on the remote host.")],
    remote: Annotated[str, typer.Option("--remote", help="Remote host name or URL.")],
    entry: Annotated[str | None, typer.Option("--entry", help="Discovered entry/start host name.")] = None,
    request_timeout: Annotated[
        float, typer.Option("--request-timeout", help="HTTP request timeout in seconds.")
    ] = 60.0,
    dest: Annotated[str, typer.Option("--dest", help="Destination path; defaults to source basename.")] = "",
    mode: Annotated[str, typer.Option("--mode", help="Transfer mode: copy or move.")] = "copy",
    dry: Annotated[bool, typer.Option("--dry", help="Only stat the source and report the plan.")] = False,
    overwrite: Annotated[bool, typer.Option("--overwrite", help="Replace the destination if it exists.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print JSON output.")] = False,
    api_key_env: Annotated[str | None, typer.Option("--api-key-env", help="API key environment variable.")] = None,
) -> None:
    _file("pull", source, remote, entry, request_timeout, dest, mode, dry, overwrite, json_output, api_key_env)


def _file(
    command: str,
    source: str,
    remote: str,
    entry: str | None,
    request_timeout: float,
    dest: str,
    mode: str,
    dry: bool,
    overwrite: bool,
    json_output: bool,
    api_key_env: str | None,
) -> None:
    try:
        client = HostClient(timeout=max(1.0, request_timeout), api_key=resolve_api_key(api_key_env))
        entry_ref = select_reachable_entry_server(entry_name=entry, client=client)
        remote_ref = file_backend._resolve_remote(entry_ref, remote, client=client)
        args = SimpleNamespace(
            command=command,
            source=source,
            dest=dest,
            mode=mode,
            dry=dry,
            overwrite=overwrite,
            request_timeout=request_timeout,
        )
        summary = file_backend.run_transfer(entry_ref, remote_ref, args, client=client)
        if json_output:
            print_json(summary.to_wire())
        else:
            file_backend._print_summary(summary)
        raise typer.Exit(0 if summary.status is TaskStatus.COMPLETED else 1)
    except typer.Exit:
        raise
    except Exception as exc:
        fail(f"paglets examples file {command}", exc)
