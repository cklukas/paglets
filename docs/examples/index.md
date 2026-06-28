# Example Agents

`paglets` ships packaged example agents that demonstrate different runtime
patterns without mixing application code into the built-in service namespace:

- `paglets.examples.analysis_jobs`: synthetic dataframe jobs that use
  built-in compute-slot scheduling and return results to a home SQLite DB.
- Detached compute collector guide: reusable job-group and collector helpers
  for laptop-submit, worker-compute, collector-status workflows.
- `paglets.examples.compute`: a coordinator/worker decimal Pi compute example
  plus a mesh-aware CLI.
- `paglets.examples.file_grabber`: a small one-file push/pull example that
  registers a file, dispatches with it, and writes it on the destination host.
- `paglets.examples.performance`: a pure mobile benchmark agent plus a
  mesh-wide benchmark CLI.
- `paglets.examples.mesh_benchmark`: a directional mesh movement benchmark
  with a stable starter and one mobile traveler.
- `paglets.examples.search`: a pure mobile filesystem search agent plus a
  streaming mesh search CLI.

Runtime infrastructure lives in topic packages such as `paglets.core`,
`paglets.runtime`, `paglets.remote`, and `paglets.services`. Built-in resident
services live under `paglets.system.*`. Example agents live under
`paglets.examples.*` so their imports make that boundary explicit.

The larger examples are split the same way internally: request/result
dataclasses live in `models.py`, pure computation or analysis helpers live in
focused modules such as `chudnovsky.py`, `local_search.py`, `kernels.py`, and
`analysis.py`, and mobile-agent orchestration stays in `agent.py`. Package-level
example imports such as `from paglets.examples.compute import PiComputeRequest`
remain available for examples, while runtime imports still use explicit topic
modules.

## Running The Examples

Start two same-version hosts:

```bash
uv run paglets host --name alpha --port 8765 --mesh-version dev
uv run paglets host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

For hosts on different machines, use `--bind-public` so each host binds and
publishes a reachable LAN address:

```bash
uv run paglets host --name mac --bind-public auto --port 8765 --mesh-version dev
uv run paglets host --name windows --bind-public 192.0.2.10 --port 8765 --mesh-version dev
```

`--bind-public auto` binds only the detected LAN address. Supplying an explicit
value such as `--bind-public 192.0.2.10` binds only that address, which is
useful on machines with multiple network interfaces. Repeat the flag to bind
multiple specific addresses; the first one is published to the mesh. The auto
form keeps watching for LAN address changes and rebinds/publishes the new
address after DHCP or network reconnect changes it.

On first start, `paglets host` copies the bundled launch config to
`~/.paglets/launch.toml`. The bundled config declares built-in resident
services:

<div class="paglets-code-source">Source: <a href="https://github.com/cklukas/paglets/blob/main/src/paglets/config/defaults/launch.toml">src/paglets/config/defaults/launch.toml</a></div>

```toml
--8<-- "src/paglets/config/defaults/launch.toml"
```

The command-line examples dynamically discover a reachable entry host from
local/LAN probes and mesh multicast beacons. The entry host is only the
bootstrap point; mesh-aware examples still discover and use online
same-version mesh hosts automatically. There is no saved server/IP membership
file to maintain.

```bash
uv run paglets sys df --entry alpha
uv run paglets mesh summary --entry alpha
uv run paglets artifacts list --entry alpha
uv run paglets jobs status --entry alpha
uv run paglets jobs groups --entry alpha
uv run paglets examples analysis --entry alpha --tasks 20
uv run paglets examples file push --entry alpha ./data.bin --remote beta --dest /tmp/data.bin --dry
uv run paglets examples pi --entry alpha --digits 16 --output pi.txt
uv run paglets examples perf --entry alpha
uv run paglets search grep --entry alpha TODO .
```

Direct local examples can still run without an API key. For proxied or shared
networks, set `PAGLETS_API_KEY` and use the relay setup from the main guide so
the HTTP API requires bearer authentication.

All packaged example CLIs that contact an entry host read `PAGLETS_API_KEY`
automatically and accept `--api-key-env NAME` as an override, including
`paglets sys`, `paglets mesh`, `paglets artifacts`,
`paglets jobs`, `paglets jobs groups`, `paglets examples analysis`,
`paglets examples file`, `paglets examples pi`, `paglets examples perf`,
`paglets examples mesh-benchmark`, and `paglets search`. The paglet classes themselves
do not need relay-specific branches; use normal context, proxy, service,
creation, clone, and dispatch APIs and the host transport forwards relayed URLs
transparently.

## Example Pages

- [Detached Compute With A Collector](detached-compute-collector.md)
- [Analysis Jobs](analysis-jobs.md)
- [File Grabber](file-grabber.md)
- [Pi Compute](compute.md)
- [Performance Benchmark](performance.md)
- [Mesh Movement Benchmark](mesh-benchmark.md)
- [Mesh Search](search.md)
- [Source-Tree Demos](source-tree-demos.md)

Built-in service pages live under [System Services](../system/index.md).
