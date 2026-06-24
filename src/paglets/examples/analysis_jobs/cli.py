# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import os
import sys
import time

from paglets.core.messages import Message
from paglets.remote.admin import PagletsAdminClient, select_reachable_entry_server
from paglets.remote.client import HostClient
from paglets.remote.proxy import PagletProxy
from paglets.serialization.codec import dataclass_to_wire

from .agent import (
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
    default_result_db,
)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        api_key = os.environ.get(args.api_key_env) if args.api_key_env else None
        if args.api_key_env and not api_key:
            raise ValueError(f"--api-key-env {args.api_key_env!r} is not set or is empty")
        client = HostClient(timeout=args.timeout, api_key=api_key)
        entry = select_reachable_entry_server(entry_name=args.entry, client=client)
        request = AnalysisCampaignRequest(
            task_count=max(1, args.tasks),
            db_path=args.db,
            row_count=max(100, args.rows),
            feature_count=max(4, args.features),
            estimator_trees=max(1, args.trees),
            target_runtime_seconds=max(0.0, args.target_runtime),
            cpu_cores=max(1, args.cpu_cores),
            memory_bytes=max(0, args.memory),
            temp_storage_bytes=max(0, args.temp_storage),
            db_lock_timeout_seconds=max(0.0, args.db_lock_timeout),
        )
        admin = PagletsAdminClient([entry], client=client)
        proxy_wire = admin.create_agent(
            entry,
            "paglets.examples.analysis_jobs.agent:CampaignSeederPaglet",
            "paglets.examples.analysis_jobs.agent:CampaignSeederState",
            {},
        )
        proxy = PagletProxy.from_wire(proxy_wire, client)
        summary = proxy.send(Message("start", {"request": dataclass_to_wire(request)}), timeout=args.timeout)
        if args.wait:
            deadline = time.monotonic() + max(0.0, args.wait)
            while time.monotonic() < deadline:
                summary = proxy.send(Message("summary"), timeout=args.timeout)
                if summary.get("done"):
                    break
                time.sleep(0.5)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        print(f"paglets-analysis-jobs: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start the synthetic distributed analysis example")
    parser.add_argument("--entry", default=None, help="Discovered entry/home host name")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds")
    parser.add_argument(
        "--api-key-env", default=None, help="Environment variable containing the paglets bearer API key"
    )
    parser.add_argument("--tasks", type=int, default=DEFAULT_TASK_COUNT, help="Number of analysis jobs")
    parser.add_argument("--db", default=default_result_db(), help="SQLite result DB path on the home host")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROW_COUNT, help="Synthetic rows per job")
    parser.add_argument("--features", type=int, default=DEFAULT_FEATURE_COUNT, help="Synthetic features per job")
    parser.add_argument("--trees", type=int, default=DEFAULT_ESTIMATOR_TREES, help="Random forest tree count")
    parser.add_argument(
        "--target-runtime",
        type=float,
        default=DEFAULT_TARGET_RUNTIME_SECONDS,
        help="Minimum compute duration per job in seconds",
    )
    parser.add_argument("--memory", type=_parse_size, default=DEFAULT_MEMORY_BYTES, help="Requested RAM per job")
    parser.add_argument("--cpu-cores", type=int, default=DEFAULT_CPU_CORES, help="Requested logical CPU cores per job")
    parser.add_argument(
        "--temp-storage",
        type=_parse_size,
        default=DEFAULT_TEMP_STORAGE_BYTES,
        help="Requested temp storage per job",
    )
    parser.add_argument(
        "--db-lock-timeout",
        type=float,
        default=DEFAULT_DB_LOCK_TIMEOUT_SECONDS,
        help="Seconds to wait for SQLite write lock",
    )
    parser.add_argument("--wait", type=float, default=5.0, help="Seconds to wait for seeder completion")
    return parser


def _parse_size(value: str) -> int:
    text = str(value).strip()
    if not text:
        raise argparse.ArgumentTypeError("size cannot be empty")
    unit = text[-1].upper()
    if unit in {"K", "M", "G"}:
        number = text[:-1]
        multiplier = {"K": 1024, "M": 1024**2, "G": 1024**3}[unit]
    else:
        number = text[:-1] if unit == "B" else text
        multiplier = 1
    try:
        amount = float(number)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid size {value!r}") from exc
    if amount < 0:
        raise argparse.ArgumentTypeError("size must be non-negative")
    return int(amount * multiplier)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
