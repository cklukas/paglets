# paglets

`paglets` is a compact Python runtime inspired by Java Aglets. A paglet is a
mobile object with explicit dataclass state, lifecycle hooks, message handling,
and proxy-based control.

The runtime intentionally uses a Python-friendly mobility model:

- all hosts already have the same code importable;
- every active paglet instance runs in its own child Python process;
- only dataclass state moves between hosts;
- control calls use a JSON HTTP API, while paglet movement uses a binary HTTP
  state payload to avoid JSON encoding large mobile state;
- large state is streamed for host-to-host movement and host/child process
  handoff, while JSON remains the small control-plane format;
- lifecycle hooks resume behavior after create, dispatch, clone, retract, or
  activation;
- deactivation persists inactive paglets to disk until activation;
- agents communicate through `PagletProxy`, `Message`, and per-paglet serial
  child-process mailboxes;
- service discovery, transfer tickets, proxy references, context events, and
  resource cleanup are first-class framework features.

The process-per-paglet model isolates crashes and CPU-heavy work from the host
and from other paglets, and it gives worker paglets real multi-core parallelism.
The tradeoff is stricter importability, process startup overhead, and
actor-style serial message handling inside each individual paglet.

## Quick Start

Install and test the project in development:

```bash
uv run pytest -q
```

Run two hosts:

```bash
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

For hosts on different machines, start each host with `--bind-public`:

```bash
uv run paglets-host --name mac --bind-public --port 8765 --mesh-version dev
uv run paglets-host --name windows --bind-public [IP] --port 8765 --mesh-version dev
```

Without an `IP`, `--bind-public` binds only the detected LAN address. With an
`IP`, it binds only that supplied address. Repeat the flag to bind multiple
specific addresses; the first one is published to the mesh. The auto form
keeps watching the detected address and rebinds/publishes a new one after DHCP
or network reconnect changes it.

On first start, `paglets-host` copies `~/.paglets/launch.toml` from the bundled
demo config. The default launch config declares lazy `server-info` and eager
`mesh-info`, so hosts continuously exchange resource snapshots while still
using `server-info` as the local system information provider:

```bash
uv run paglets-sysinfo df
uv run paglets-sysinfo load
uv run paglets-mesh-info summary
uv run paglets-pi-compute --digits 16
uv run paglets-perf-test
uv run paglets-mesh-benchmark --payload-size 64K
```

`paglets-perf-test` is a pure mobile-agent example: the entry host creates a
parent benchmark paglet, clones workers to online same-version mesh hosts, runs
local CPU, memory, and bounded temporary disk I/O checks, and reports the
summary centrally.

`paglets-mesh-benchmark` measures mobile-agent movement itself. A starter
paglet remains on the entry host while a traveler visits every directed host
pair, stores per-hop timings locally on arrival, then collects and prints a
directional Markdown matrix plus clock-offset and message round-trip
diagnostics, ending with the overall benchmark time.

Run the disk survey demo:

```bash
uv run python examples/disk_survey_demo.py --hosts alpha beta gamma
```

## Documentation Map

- [Implementing Paglets](implementing-paglets.md): how to write paglet classes,
  state objects, lifecycle hooks, message handlers, movement, and mesh-aware
  behavior.
- [Example Agents](examples.md): detailed explanations of packaged example
  agents, including `server-info`, `mesh-info`, Pi compute,
  `paglets-perf-test`, and `paglets-mesh-benchmark`.
- [Git Auto-Update](git-auto-update.md): how trusted host meshes can pull,
  synchronize dependencies, broadcast commit hashes, and restart from updated
  code.
- [Usage Ideas](usage-ideas.md): practical scenarios where mobile state and
  agent-to-agent communication are a useful fit.
- [Internal Workings](internal-workings.md): how the runtime is structured and
  how code, state, messages, proxies, hosts, and the mesh registry fit together.
- [API Reference](api-reference.md): generated Python API documentation for the
  public modules.
- [Glossary](glossary.md): terminology used by the project.

## Build The Docs Locally

```bash
uv run --extra docs mkdocs serve
```

For a production build:

```bash
uv run --extra docs mkdocs build --strict
```
