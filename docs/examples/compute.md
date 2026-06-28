# Pi Compute

`paglets examples pi` demonstrates using `mesh-info` as lightweight placement
input for a distributed compute job. It computes decimal digits of Pi by
distributing Chudnovsky term batches and combining the integer partial sums on
the entry-host job paglet.

```bash
uv run paglets examples pi --digits 16 --batch-size 1
uv run paglets examples pi --digits 32 --max-load-per-cpu 0.75 --max-workers-per-host 2 --output runs/pi.txt --json
```

The CLI creates one `PiJobPaglet` on the selected entry host, sends one
`pi.start` message, prints a small acknowledgement, and exits. The job paglet
then partitions the requested digit range into the required Chudnovsky terms,
asks local `mesh-info` for eligible targets across the mesh, creates short-lived
`PiBatchWorkerAgent` instances remotely, and receives `pi.batch_result` or
`pi.batch_failed` messages. Worker creation requests are issued in parallel so
process-spawn overhead does not keep free slots empty. The free-slot estimate is
based on `cpu_count * --max-load-per-cpu - load_1m`; existing in-flight workers
are added back before new launches are capped by `--max-workers-per-host`.
`--max-in-flight` caps the whole job.

Digits are produced on the entry host, not by the CLI. The job paglet emits raw
digit chunks through the local `user-info` service and appends the exact same
chunks to the output file. The default output is `pi.txt` in the directory where
the CLI was started; use `--output PATH` to choose another file. Relative output
paths are resolved before submission. If `--start 0`, the file and host-side
console output begin with `3.` once; otherwise only the requested decimal range
is written. Use `--json` to print submission metadata, including `job_id`,
`host_url`, and `output_path`.

Failures are explicit. A failed worker batch marks the job failed, sends a
`pi.failed` user-info notification, and leaves the partial output file in place
for inspection. Failed batches are not automatically retried or requeued.

The worker result payloads encode large Chudnovsky partial integers in
hexadecimal internally. The job paglet incrementally merges only contiguous
`ok` term fragments and formats newly available digits. This keeps scheduling
messages compact, avoids client-side recombination, and avoids Python integer
string conversion limits for very large jobs.

Programmatic use:

```python
from paglets.examples.compute import (
    PiComputeRequest,
    PiJobPaglet,
    PiJobStartRequest,
)
from paglets.core.messages import Message
from paglets.serialization.codec import dataclass_to_wire

job = self.context.create_paglet(PiJobPaglet)
reply = job.send(
    Message(
        "pi.start",
        dataclass_to_wire(
            PiJobStartRequest(
                request=dataclass_to_wire(PiComputeRequest(start=0, digits=16, batch_size=1)),
                output_path="/tmp/pi.txt",
            )
        ),
    )
)
```
