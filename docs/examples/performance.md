# Performance Benchmark

`paglets examples perf` demonstrates a pure mobile-agent fan-out pattern. It does
not use a resident service and it is not started from launch config. The
benchmark code is carried by the mobile agent class itself.

The core agent is:

```python
from paglets.examples.performance import PerformanceBenchmarkAgent
```

The CLI creates one parent benchmark agent on the entry host. The parent clones
children to online same-version mesh hosts. Each child runs benchmarks locally
and sends one result back to the parent. The CLI submits the job, prints accepted
job metadata, and exits. The parent writes the final JSON summary to the
entry-host output file and sends a `user-info` completion message. Parent result
bookkeeping uses the paglet state lock, but the actual benchmark work and remote
calls happen outside that lock. The public parent protocol uses typed
operations, while `MeshFanoutMixin` handles the repeated parent/child clone
bookkeeping.

### Benchmarks

The default run includes all categories:

| Category | Measurements |
| --- | --- |
| CPU single-core | Python integer loop, Python float loop, SHA-256 throughput. |
| CPU multi-core | Same kernels through worker processes. |
| Memory | Byte-buffer copy throughput and byte-buffer scan/checksum throughput. |
| Disk | Sequential write, fsync, sequential read, and small-file metadata rate. |

Disk benchmarks are intentionally bounded:

- only writable real volumes are selected by default;
- when a mountpoint is not directly writable, the benchmark also tries
  per-user writable directories such as `~/.paglets/benchmarks` and the OS temp
  directory on that same volume;
- special, pseudo, read-only, duplicate, missing, and unwritable volumes are
  skipped;
- each tested volume gets a temporary benchmark directory;
- temporary files are cleaned up afterward;
- a volume is skipped if free space is less than twice the requested test size.

Normal text output hides skipped read-only, special, and duplicate targets. Use
`--verbose` or `--debug` when you want to inspect those skipped targets. JSON
output always includes the full skipped-target list.

These numbers are practical comparison data for a paglets mesh. They are not
calibrated hardware certification results.

### CLI Commands

Run all benchmark categories:

```bash
uv run paglets examples perf
```

Useful variations:

```bash
uv run paglets examples perf --json --output perf-summary.json
uv run paglets examples perf --duration 2 --disk-size 256M
uv run paglets examples perf --path /data --path /scratch
uv run paglets examples perf --no-disk
uv run paglets examples perf --workers 4
uv run paglets examples perf --verbose
```

Example with two local hosts running in separate terminals:

```bash
uv run paglets host --name alpha --port 8765 --mesh-version dev
uv run paglets host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

Across machines, use `--bind-public 192.0.2.10` on each host instead of loopback.
Repeat explicit values such as `--bind-public 192.0.2.10` only when the host
must listen on multiple specific interfaces.

Then run the benchmark from the repository checkout:

```text
klukas@mac-studio paglets % uv run paglets examples perf --output perf-summary.json
paglets-perf-test: submitted perf-... on http://192.168.86.10:8765 output=/.../perf-summary.json
```

Important options:

| Option | Meaning |
| --- | --- |
| `--duration` | Seconds per CPU and memory kernel. Default: `1.0`. |
| `--disk-size` | Temporary file size per tested volume. Default: `128M`. |
| `--workers` | Multi-core worker count. Default: logical CPU count. |
| `--path` | Limit disk I/O to explicit paths. Can be repeated. |
| `--no-cpu` | Skip CPU tests. |
| `--no-memory` | Skip memory tests. |
| `--no-disk` | Skip disk I/O tests. |
| `--lock-timeout` | Seconds to wait for another local benchmark run to finish. |
| `--output` | JSON summary file on the entry host. |
| `--verbose` | Print skipped disk targets and cleanup diagnostics. |
| `--debug` | Same diagnostic output as `--verbose`. |

### Agent Flow

The benchmark agent uses cloning because benchmark work should run in parallel
on different hosts:

1. The CLI creates a parent `PerformanceBenchmarkAgent` on the entry host.
2. The parent discovers online same-version mesh hosts.
3. The parent clones a child to each host.
4. Each child starts benchmark work in a background thread so clone arrival does
   not serialize the fan-out.
5. Children on different hosts run in parallel.
6. A host-local benchmark lock prevents two benchmark children on the same
   server from running expensive tests at the same time.
7. Each child reports `HostBenchmarkResult` or an error to the parent.
8. The parent writes a summary with `results`, `errors`, and non-fatal
   `cleanup_errors` to the output file.

The lock has two layers: a process-local `threading.Lock` for threads inside one
benchmark child, and a best-effort OS file lock in the system temp directory.
The OS file lock is the important cross-process guard in the process-isolated
runtime; it serializes benchmark paglets started by the same user on the same
machine while still allowing different physical hosts to work in parallel.

### Programmatic Use

The request and reply dataclasses are importable:

```python
from paglets.examples.performance import (
    PERFORMANCE_COLLECT,
    BenchmarkRequest,
    PerformanceBenchmarkAgent,
    PerformanceCollectRequest,
)
from paglets.patterns.operations import OperationClient
from paglets.serialization.codec import dataclass_to_wire

proxy = self.context.create_paglet(PerformanceBenchmarkAgent)
client = OperationClient(proxy)
reply = client.call(
    PERFORMANCE_COLLECT,
    PerformanceCollectRequest(
        request=dataclass_to_wire(
            BenchmarkRequest(duration_seconds=0.5, disk_size_bytes=64 * 1024 * 1024)
        ),
        timeout=120.0,
        output_path="/tmp/perf-summary.json",
    )
)
output_path = reply.output_path
```

Most applications should use the `paglets examples perf` CLI unless they need to
embed benchmark collection into another paglet workflow.
