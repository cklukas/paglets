# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key
from paglets.core.messages import Message
from paglets.remote.admin import (
    PagletsAdminClient,
    ServerRef,
    select_reachable_entry_server,
)
from paglets.remote.client import HostClient
from paglets.remote.proxy import PagletProxy
from paglets.serialization.codec import dataclass_to_wire

from .models import DEFAULT_OUTPUT_CHUNK_DIGITS, PiComputeRequest, PiJobStartRequest


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        api_key = resolve_api_key(args.api_key_env)
        client = HostClient(api_key=api_key)
        entry = _select_entry_server(entry_name=args.entry, client=client)
        request = PiComputeRequest(
            start=max(0, args.start),
            digits=max(0, args.digits),
            batch_size=max(1, args.batch_size),
            max_in_flight=max(0, args.max_in_flight),
            max_workers_per_host=max(0, args.max_workers_per_host),
            timeout=max(0.0, args.timeout),
            max_load_per_cpu=float(args.max_load_per_cpu),
            max_cpu_percent=float(args.max_cpu_percent),
            min_memory_available_bytes=max(0, int(args.min_memory)),
            min_work_free_bytes=max(0, int(args.min_work_free)),
        )
        output_path = _resolve_output_path(args.output)
        reply = _submit(entry, request, output_path=output_path, client=client)
        if args.json:
            print(json.dumps(reply, indent=2, sort_keys=True))
        else:
            print(
                f"paglets-pi-compute: submitted {reply['job_id']} on {reply['host_url']} output={reply['output_path']}"
            )
        return 0
    except Exception as exc:
        print(f"paglets-pi-compute: {exc}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Submit a message-driven Pi compute job to a paglets host")
    parser.add_argument("--entry", default=None, help="Discovered entry host name")
    parser.add_argument("--start", type=int, default=0, help="Zero-based decimal digit position after the point")
    parser.add_argument("--digits", type=int, default=16, help="Number of decimal digits to compute")
    parser.add_argument("--batch-size", type=int, default=1, help="Chudnovsky terms per worker batch")
    parser.add_argument(
        "--max-in-flight", type=int, default=0, help="Global in-flight batch cap; 0 uses free load slots"
    )
    parser.add_argument(
        "--max-workers-per-host", type=int, default=0, help="Per-host worker cap; 0 uses free load slots"
    )
    parser.add_argument("--timeout", type=float, default=0.0, help="Reserved job timeout value; 0 disables it")
    parser.add_argument("--max-load-per-cpu", type=float, default=1.0, help="Maximum 1-minute load divided by CPUs")
    parser.add_argument(
        "--max-cpu-percent", type=float, default=100.0, help="Maximum sampled CPU percent for placement"
    )
    parser.add_argument("--min-memory", type=_parse_size, default=0, help="Minimum available RAM, e.g. 512M")
    parser.add_argument("--min-work-free", type=_parse_size, default=0, help="Minimum free work storage, e.g. 1G")
    parser.add_argument("--output", default="pi.txt", help="Output file on the entry host; relative to this shell")
    parser.add_argument("--json", action="store_true", help="Print submission metadata as JSON")
    parser.add_argument(
        "--api-key-env",
        default=None,
        help=f"Environment variable to read the paglets bearer API key from; defaults to {DEFAULT_API_KEY_ENV}",
    )
    return parser


def _select_entry_server(*, entry_name: str | None, client: HostClient) -> ServerRef:
    return select_reachable_entry_server(entry_name=entry_name, client=client)


def _resolve_output_path(output: str | Path) -> Path:
    path = Path(output).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


def _submit(entry: ServerRef, request: PiComputeRequest, *, output_path: Path, client: HostClient) -> dict:
    proxy = _create_job(entry, client=client)
    start = PiJobStartRequest(
        request=dataclass_to_wire(request),
        job_id=f"pi-{uuid.uuid4().hex}",
        output_path=str(output_path),
        output_chunk_digits=DEFAULT_OUTPUT_CHUNK_DIGITS,
    )
    reply = proxy.send(Message("pi.start", dataclass_to_wire(start)))
    return dict(reply)


def _create_job(entry: ServerRef, *, client: HostClient) -> PagletProxy:
    admin = PagletsAdminClient([entry], client=client)
    proxy_wire = admin.create_agent(
        entry,
        "paglets.examples.compute.agent:PiJobPaglet",
        "paglets.examples.compute.agent:PiJobState",
        {},
    )
    return PagletProxy.from_wire(proxy_wire, client)


def _parse_size(value: str) -> int:
    text = value.strip()
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
        raise argparse.ArgumentTypeError(f"invalid size: {value!r}") from exc
    if amount < 0:
        raise argparse.ArgumentTypeError("size must be non-negative")
    return int(amount * multiplier)


if __name__ == "__main__":
    raise SystemExit(main())
