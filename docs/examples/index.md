# Example Agents

`paglets` ships six packaged example agents that demonstrate different runtime
patterns without mixing application code into the root runtime namespace:

- `paglets.examples.system_info`: a resident typed service agent plus a
  mesh-wide collector CLI.
- `paglets.examples.mesh_info`: an eager resident mesh resource landscape
  service plus a summary/target-selection CLI.
- `paglets.examples.compute`: a coordinator/worker decimal Pi compute example
  plus a mesh-aware CLI.
- `paglets.examples.performance`: a pure mobile benchmark agent plus a
  mesh-wide benchmark CLI.
- `paglets.examples.mesh_benchmark`: a directional mesh movement benchmark
  with a stable starter and one mobile traveler.
- `paglets.examples.search`: a pure mobile filesystem search agent plus a
  streaming mesh search CLI.

Runtime infrastructure lives in topic packages such as `paglets.core`,
`paglets.runtime`, `paglets.remote`, and `paglets.services`. Example agents live
under `paglets.examples.*` so their imports make that boundary explicit.

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
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

For hosts on different machines, use `--bind-public` so each host binds and
publishes a reachable LAN address:

```bash
uv run paglets-host --name mac --bind-public --port 8765 --mesh-version dev
uv run paglets-host --name windows --bind-public [IP] --port 8765 --mesh-version dev
```

`--bind-public` without an `IP` binds only the detected LAN address. Supplying
an `IP` binds only that address, which is useful on machines with multiple
network interfaces. Repeat the flag to bind multiple specific addresses; the
first one is published to the mesh. The auto form keeps watching for LAN
address changes and rebinds/publishes the new address after DHCP or network
reconnect changes it.

On first start, `paglets-host` copies the bundled demo launch config to
`~/.paglets/launch.toml`. The bundled config declares lazy `server-info` and
eager `mesh-info` services:

```toml
[launch]
demo_config_id = "paglets-default-launch"
demo_config_version = "4"

[[resident_services]]
class = "paglets.examples.system_info.agent:ServerInfoAgent"
enabled = true
agent_id = "service.server-info"
singleton = true
lifecycle = "lazy"
scope = "mesh"
idle_timeout = 30.0
state = { service_scope = "mesh" }

[[resident_services]]
class = "paglets.examples.mesh_info.agent:MeshInfoAgent"
enabled = true
agent_id = "service.mesh-info"
singleton = true
lifecycle = "eager"
scope = "mesh"
idle_timeout = 0.0
state = { service_scope = "mesh" }
```

The command-line examples dynamically discover a reachable entry host from
local/LAN probes and mesh multicast beacons. The entry host is only the
bootstrap point; mesh-aware examples still discover and use online
same-version mesh hosts automatically. There is no saved server/IP membership
file to maintain.

```bash
uv run paglets-sysinfo [--entry alpha] df
uv run paglets-mesh-info [--entry alpha] summary
uv run paglets-pi-compute [--entry alpha] --digits 16
uv run paglets-perf-test [--entry alpha]
uv run paglets-search [--entry alpha] grep TODO .
```

Direct local examples can still run without an API key. For proxied or shared
networks, start hosts with `--api-key-env` and use the relay setup from the main
guide so the HTTP API requires bearer authentication.

All packaged example CLIs that contact an entry host accept `--api-key-env`,
including `paglets-sysinfo`, `paglets-mesh-info`, `paglets-pi-compute`,
`paglets-perf-test`, `paglets-mesh-benchmark`, and `paglets-search`. The paglet
classes themselves do not need relay-specific branches; use normal context,
proxy, service, creation, clone, and dispatch APIs and the host transport
forwards relayed URLs transparently.

## Example Pages

- [Server Info](system-info.md)
- [Mesh Info](mesh-info.md)
- [Pi Compute](compute.md)
- [Performance Benchmark](performance.md)
- [Mesh Movement Benchmark](mesh-benchmark.md)
- [Mesh Search](search.md)
- [Source-Tree Demos](source-tree-demos.md)

