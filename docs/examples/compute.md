# Pi Compute

`paglets-pi-compute` demonstrates using `mesh-info` as lightweight placement
input for a distributed compute job. It computes decimal digits of Pi by
distributing Chudnovsky term batches and combining the integer partial sums on
the coordinator.

```bash
uv run paglets-pi-compute --digits 16 --batch-size 1
uv run paglets-pi-compute --digits 32 --max-load-per-cpu 0.75 --max-workers-per-host 2 --json
```

The coordinator stays on the dynamically discovered entry host, partitions the
requested digit range into the required Chudnovsky terms, asks local
`mesh-info` for eligible targets across the mesh, treats approximate free load
slots as additional launch capacity, creates short-lived
`PiBatchWorkerAgent` instances remotely, and receives `batch_result` messages.
Worker creation requests are issued in parallel so process-spawn overhead does
not keep free slots empty. The free-slot estimate is based on `cpu_count *
--max-load-per-cpu - load_1m`; existing in-flight workers are added back before
new launches are capped by `--max-workers-per-host`. `--max-in-flight` caps the
whole job. A dedicated `PiPostProcessAgent` runs on the entry host for each
active job; it incrementally merges finalized term fragments and performs
`drain`/`format` work so the coordinator can focus on scheduling and state
tracking.

In text mode the CLI starts the coordinator asynchronously, long-polls with
`drain_stream`, and appends each returned decimal fragment to the terminal.
`drain_stream` first refills worker slots, then returns compact progress counters
and new text only; it does not return the full raw term history, keeping messages
small while preserving `3.1415...` output. Increase
`--stream-chunk-size` when larger terminal bursts are useful. Use `--json` for a
final summary object instead of live output. The default job timeout is disabled
so long calculations can run to completion; add `--timeout SECONDS` when a run
should be bounded, and increase `--request-timeout` if an exceptionally large
coordinator response needs longer than the default HTTP request window. Workers
re-check local load before computing; if a host has become busy, the worker
reports `skipped`, the coordinator requeues that batch, and the worker disposes
itself. If all hosts are above the load/CPU thresholds and no batch is running,
the coordinator sends one fallback worker anyway so a long job still makes
minimum progress.

The worker result payloads encode large Chudnovsky partial integers in
hexadecimal internally. The coordinator forwards only finalized `ok` term fragments
to the post-processor for incremental merge; the post-processor formats digits
on demand. This keeps scheduling messages compact and avoids expensive terminal-side
recombination while still producing normal `3.1415...` output and avoiding Python
integer string conversion limits for very large jobs.

Programmatic use:

```python
from paglets.examples.compute import (
    PI_START_ASYNC,
    PiComputeCoordinatorAgent,
    PiComputeRequest,
    PiStartRequest,
)
from paglets.patterns.operations import OperationClient
from paglets.serialization.codec import dataclass_to_wire

coordinator = self.context.create_paglet(PiComputeCoordinatorAgent)
client = OperationClient(coordinator)
reply = client.call(
    PI_START_ASYNC,
    PiStartRequest(
        request=dataclass_to_wire(PiComputeRequest(start=0, digits=16, batch_size=1))
    )
)
summary = reply.summary
```
