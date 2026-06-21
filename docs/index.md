# paglets

`paglets` is a compact Python runtime inspired by Java Aglets. A paglet is a
mobile object with explicit dataclass state, lifecycle hooks, message handling,
and proxy-based control.

The runtime intentionally uses a Python-friendly mobility model:

- all hosts already have the same code importable;
- only dataclass state moves between hosts;
- host-to-host transfer uses a JSON HTTP API;
- lifecycle hooks resume behavior after create, dispatch, clone, retract, or
  activation;
- deactivation persists inactive paglets to disk until activation;
- agents communicate through `PagletProxy`, `Message`, and per-paglet
  mailboxes;
- service discovery, transfer tickets, proxy references, context events, and
  resource cleanup are first-class framework features.

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

On first start, `paglets-host` copies `~/.paglets/launch.toml` from the bundled
demo config. The default launch config declares the packaged example
`server-info` service lazily, so mesh-wide commands can query local system state
and start providers only when needed:

```bash
uv run paglets-sysinfo df
uv run paglets-sysinfo load
uv run paglets-perf-test
```

`paglets-perf-test` is a pure mobile-agent example: the entry host creates a
parent benchmark paglet, clones workers to online same-version mesh hosts, runs
local CPU, memory, and bounded temporary disk I/O checks, and reports the
summary centrally.

Run the Textual TUI:

```bash
uv run --extra tui paglets-tui \
  --server alpha=http://127.0.0.1:8765 \
  --server beta=http://127.0.0.1:8766
```

Run the disk survey demo:

```bash
uv run python examples/disk_survey_demo.py --hosts alpha beta gamma
```

## Documentation Map

- [Implementing Paglets](implementing-paglets.md): how to write paglet classes,
  state objects, lifecycle hooks, message handlers, movement, and mesh-aware
  behavior.
- [Example Agents](examples.md): detailed explanations of packaged example
  agents, including `server-info`, `paglets-sysinfo`, and
  `paglets-perf-test`.
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
