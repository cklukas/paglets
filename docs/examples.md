# Example Agents

`paglets` ships two packaged example agents that demonstrate different runtime
patterns without mixing application code into the root runtime namespace:

- `paglets.examples.system_info`: a resident typed service agent plus a
  mesh-wide collector CLI.
- `paglets.examples.performance`: a pure mobile benchmark agent plus a
  mesh-wide benchmark CLI.

The root `paglets` package is reserved for runtime infrastructure such as
`Paglet`, `Host`, `Message`, service contracts, mesh discovery, startup config,
and transfer mechanics. Example agents live under `paglets.examples.*` so their
imports make that boundary explicit.

## Running The Examples

Start two same-version hosts:

```bash
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

On first start, `paglets-host` copies the bundled demo launch config to
`~/.paglets/launch.toml`. The bundled config starts the packaged `server-info`
example service:

```toml
[launch]
demo_config_id = "paglets-default-launch"
demo_config_version = "2"

[[startup_agents]]
class = "paglets.examples.system_info.agent:ServerInfoAgent"
enabled = true
agent_id = "service.server-info"
singleton = true
state = { service_scope = "mesh" }
```

The command-line examples use `~/.paglets/servers.json` to choose an entry host.
You can also pass an entry server directly:

```bash
uv run paglets-sysinfo --server alpha=http://127.0.0.1:8765 --entry alpha df
uv run paglets-perf-test --server alpha=http://127.0.0.1:8765 --entry alpha
```

There is no authentication layer yet. These examples are useful for trusted
local or lab meshes, not untrusted networks.

## Server Info

`server-info` demonstrates the resident-service pattern:

1. A host starts `ServerInfoAgent` from launch config.
2. The agent advertises a typed service contract named `server-info`.
3. A caller discovers that contract locally or across the mesh.
4. Requests are ordinary paglet messages, but payloads and replies are typed
   dataclasses.
5. The `paglets-sysinfo` CLI creates a short-lived collector paglet that clones
   to mesh hosts, calls each host's local `server-info` service, and prints an
   aggregate result.

### Contract And Operations

Import the example contract explicitly from the example package:

```python
from paglets.examples.system_info import (
    GET_DISK,
    GET_LOAD,
    GET_SUMMARY,
    LIST_PROCESSES,
    SERVER_INFO,
    DiskRequest,
    LoadRequest,
    ProcessListRequest,
)
```

The contract has four operations:

| Operation | Request | Reply | Purpose |
| --- | --- | --- | --- |
| `GET_LOAD` / `load` | `LoadRequest` | `LoadReply` | CPU percent, load average, memory, swap, and best-effort GPU info. |
| `GET_DISK` / `df` | `DiskRequest` | `DiskReply` | Disk usage for selected paths or mounted volumes. |
| `LIST_PROCESSES` / `plist` | `ProcessListRequest` | `ProcessListReply` | Process search by name or command line. |
| `GET_SUMMARY` / `summary` | `EmptyPayload` | `SummaryReply` | Compact host, Python, CPU, memory, and boot summary. |

Provider-side routing is in `ServerInfoAgent.handle_message`:

```python
return SERVER_INFO.route(
    message,
    {
        GET_LOAD: self.get_load,
        GET_DISK: self.get_disk,
        LIST_PROCESSES: self.list_processes,
        GET_SUMMARY: self.get_summary,
    },
    default=self.not_handled(),
)
```

Consumer code can call the typed service directly:

```python
service = self.require_contract(SERVER_INFO, operation=GET_DISK, scope="mesh")
reply = service.call(GET_DISK, DiskRequest(paths=["/"], all_volumes=False))
```

### CLI Commands

`paglets-sysinfo` provides familiar host-inspection commands across the mesh:

```bash
uv run paglets-sysinfo summary
uv run paglets-sysinfo load
uv run paglets-sysinfo df
uv run paglets-sysinfo df / /data
uv run paglets-sysinfo plist python --limit 10
uv run paglets-sysinfo plist postgres --args --json
```

The CLI does not require a `--server` argument when `~/.paglets/servers.json`
has at least one enabled reachable server. It uses that server as the entry host,
then lets the collector paglet discover online same-version mesh hosts.

### Collector Flow

The collector is `SystemInfoCollectorAgent`. It is not a resident service:

1. The CLI creates one collector on the entry host.
2. The collector calls `available_hosts(online_only=True, include_self=True)`.
3. It clones itself to each host.
4. Each child clone resolves the local `SERVER_INFO` contract and calls the
   requested operation with `scope="local"`.
5. Each child sends a `child_result` message back to the parent.
6. The parent returns a summary with `results` and `errors`.

This pattern keeps collection logic mobile while the service itself stays local
to each host. The parent protects `pending_hosts`, `results`, and `errors` with
short `locked_state()` and `@state_locked` sections because child replies can
arrive while the parent is still waiting.

### GPU And Process Notes

GPU information is best effort. The agent runs `nvidia-smi` if available; when
the command is missing or fails, the reply records the reason instead of failing
the whole request.

Process inspection uses `psutil`. Processes that disappear or deny access while
being read are skipped so one protected process does not fail the whole host
reply.

## Performance Benchmark

`paglets-perf-test` demonstrates a pure mobile-agent fan-out pattern. It does
not use a resident service and it is not started from launch config. The
benchmark code is carried by the mobile agent class itself.

The core agent is:

```python
from paglets.examples.performance import PerformanceBenchmarkAgent
```

The CLI creates one parent benchmark agent on the entry host. The parent clones
children to online same-version mesh hosts. Each child runs benchmarks locally
and sends one result back to the parent. Parent result bookkeeping uses the
paglet state lock, but the actual benchmark work and remote calls happen
outside that lock.

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
uv run paglets-perf-test
```

Useful variations:

```bash
uv run paglets-perf-test --json
uv run paglets-perf-test --duration 2 --disk-size 256M
uv run paglets-perf-test --path /data --path /scratch
uv run paglets-perf-test --no-disk
uv run paglets-perf-test --workers 4
uv run paglets-perf-test --verbose
```

Example with two local hosts running in separate terminals:

```bash
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

Then run the benchmark from the repository checkout:

```text
klukas@mac-studio paglets % uv run paglets-perf-test
host                int/s    float/s        sha  multi-int/s   mem copy    disk wr    disk rd err
alpha               17.2M      19.8M     2.1G/s       140.2M    30.6G/s     3.7G/s    16.0G/s   0
beta                17.2M      20.0M     2.2G/s       147.6M    31.0G/s     3.4G/s    15.7G/s   0

disks:
host           path                                  size      write       read   metadata
alpha          /Users/klukas/.paglets/benchmark    128.0M     3.7G/s    16.0G/s      9130/s
beta           /Users/klukas/.paglets/benchmark    128.0M     3.4G/s    15.7G/s      9301/s
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
8. The parent returns a summary with `results`, `errors`, and non-fatal
   `cleanup_errors`.

The lock has two layers: an in-process `threading.Lock` and a best-effort OS
file lock in the system temp directory. That is enough to serialize benchmark
runs from multiple paglets hosts started by the same user on the same machine,
while still allowing different physical hosts to work in parallel.

### Programmatic Use

The request and reply dataclasses are importable:

```python
from paglets.examples.performance import BenchmarkRequest, PerformanceBenchmarkAgent
from paglets.messages import Message
from paglets.serde import dataclass_to_wire

proxy = self.context.create_paglet(PerformanceBenchmarkAgent)
summary = proxy.send(
    Message(
        "collect",
        {
            "request": dataclass_to_wire(
                BenchmarkRequest(duration_seconds=0.5, disk_size_bytes=64 * 1024 * 1024)
            ),
            "timeout": 120.0,
        },
    )
)
```

Most applications should use the `paglets-perf-test` CLI unless they need to
embed benchmark collection into another paglet workflow.

## Source-Tree Demos

The repository also has simple runnable demos under the top-level `examples/`
directory. These are not installed as packaged example modules; they are small
scripts meant for reading and experimentation from a checkout:

```bash
uv run python examples/disk_survey_demo.py --hosts alpha beta gamma
uv run python examples/clone_workers_demo.py
uv run python examples/itinerary_demo.py
uv run python examples/message_patterns_demo.py
```

Use packaged examples when you want installed CLI commands or importable example
agents. Use source-tree demos when you want compact scripts that illustrate one
runtime concept at a time.
