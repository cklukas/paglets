# Mesh Movement Benchmark

`paglets-mesh-benchmark` measures the cost of moving a paglet through the mesh
instead of measuring CPU, memory, or disk throughput. The entry host keeps a
starter/coordinator agent active and sends one mobile traveler across every
directed host pair.

Run the default directional route:

```bash
uv run paglets-mesh-benchmark
```

Useful variations:

```bash
uv run paglets-mesh-benchmark --repeats 3
uv run paglets-mesh-benchmark --payload-size 64K
uv run paglets-mesh-benchmark --exclude-self
uv run paglets-mesh-benchmark --clock-probes 7 --digits 4
uv run paglets-mesh-benchmark --json
```

The text output is Markdown that is also padded for plain terminal reading. It
prints one timing unit before the matrix, then reports source hosts as rows and
destination hosts as columns. Cell A/B contains only A->B samples; cell B/A
contains only B->A samples. When self-visits are disabled, diagonal cells are
shown as `-`. With `--repeats`, matrix cells are averages for that exact
direction; the sum line below the matrix covers all repeated measured movement
samples.

`--payload-size` adds random printable state to the mobile traveler. This uses
the same binary paglet movement envelope as any other large paglet state, so the
benchmark exercises the normal dispatch path rather than a benchmark-specific
bulk-transfer shortcut. The same runtime path streams state through HTTP and
through the host/child process handoff. For very large payloads, increase
`--timeout`; the value is used for the overall benchmark deadline and for each
movement transfer. When payload data is present, the text output also reports
average payload transfer speed grouped by destination host, split into
cross-host movements and self-host movements. Byte units use binary scaling
such as MB/s = bytes / 1024 / 1024, while network bit units use decimal
scaling such as Mbit/s = bits / 1,000,000.

Timing is based on the stable starter clock. Before each dispatch, the traveler
probes the starter, estimates entry-host time for the local instant immediately
before `dispatch()`, and carries that timestamp with the traveler. On arrival,
the traveler captures its local arrival time before the delayed continuation,
then uses the next starter probe batch to convert that captured arrival instant
back to entry-host time. The stored per-hop duration excludes the post-arrival
probe time and the continuation delay. At the end, the traveler performs an
uncounted collection round, clears the run's local storage files, and sends the
summary back to the starter.

Clock diagnostics use repeated request/reply probes against the starter. The
displayed value is the median host-minus-entry offset; JSON output also
includes raw samples, mean offset, and the offset from the best round-trip
probe. The same probe samples are aggregated into a separate message passing
table with median, average, best, and worst request/reply round-trip times
versus the starter. The final line reports the overall benchmark time from
start through the collection round.

Programmatic callers can use the typed starter operations and leave the
traveler protocol internal to the example:

```python
from paglets.examples.mesh_benchmark import (
    MESH_BENCHMARK_DRAIN,
    MESH_BENCHMARK_START,
    MeshBenchmarkCoordinatorAgent,
    MeshBenchmarkDrainRequest,
    MeshBenchmarkRequest,
    MeshBenchmarkStartRequest,
)
from paglets.patterns.operations import OperationClient
from paglets.serialization.codec import dataclass_to_wire

coordinator = self.context.create_paglet(MeshBenchmarkCoordinatorAgent)
client = OperationClient(coordinator)
reply = client.call(
    MESH_BENCHMARK_START,
    MeshBenchmarkStartRequest(request=dataclass_to_wire(MeshBenchmarkRequest(repeats=1))),
)

while not reply.done:
    reply = client.call(MESH_BENCHMARK_DRAIN, MeshBenchmarkDrainRequest(wait_timeout=0.5))
```
