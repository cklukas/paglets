# paglets

[![CI](https://github.com/cklukas/paglets/actions/workflows/ci.yml/badge.svg)](https://github.com/cklukas/paglets/actions/workflows/ci.yml)
[![Publish Package](https://github.com/cklukas/paglets/actions/workflows/publish.yml/badge.svg)](https://github.com/cklukas/paglets/actions/workflows/publish.yml)
[![Docs](https://github.com/cklukas/paglets/actions/workflows/docs.yml/badge.svg)](https://cklukas.github.io/paglets/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

`paglets` is a compact Python re-imagining of the Java Aglets mobile-agent idea: stateful objects with identity, lifecycle hooks, message passing, proxies, movement between hosts, durable deactivation, resident services, and explicit dataclass state serialization.

Hosts are expected to already have the same paglet code importable. Movement transfers the paglet class name, state class name, and serialized dataclass state; it does not upload code or move Python stacks, threads, sockets, or arbitrary live resources.

## Quick Start

Install and run tests from a checkout:

```bash
uv run pytest
```

Build the Python package locally:

```bash
uv build
```

After a tagged release is published, install the package with:

```bash
python -m pip install paglets
```

Start two local hosts:

```bash
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

Run a packaged example CLI:

```bash
uv run paglets-sysinfo summary
uv run paglets-search grep TODO .
uv run paglets-pi-compute --digits 32
```

Run a source-tree demo:

```bash
uv run python demos/disk_survey_demo.py --hosts alpha beta gamma
```

## Minimal Paglet

```python
from dataclasses import dataclass, field

from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message


@dataclass
class CounterState(PagletState):
    count: int = 0
    events: list[str] = field(default_factory=list)


class CounterPaglet(Paglet[CounterState]):
    State = CounterState

    def handle_message(self, message: Message):
        if message.kind == "increment":
            self.state.count += int(message.args.get("by", 1))
            return {"count": self.state.count}
        return self.not_handled()
```

Public imports are intentionally explicit:

```python
from paglets.runtime.host import Host
from paglets.core.agent import Paglet, PagletState
from paglets.core.messages import Message
from paglets.remote.proxy import PagletProxy
```

Flat imports such as `from paglets import Host` are unsupported.

## Documentation

The full documentation is published at <https://cklukas.github.io/paglets/>.

Useful entry points:

- [Implementing Paglets](https://cklukas.github.io/paglets/implementing-paglets/)
- [Examples](https://cklukas.github.io/paglets/examples/)
- [Operations](https://cklukas.github.io/paglets/operations/)
- [Technical Reference](https://cklukas.github.io/paglets/technical/overview/)
- [Status And Limitations](https://cklukas.github.io/paglets/project/status/)

Build docs locally:

```bash
uv run --extra docs mkdocs build --strict
uv run --extra docs mkdocs serve
```

## Project Layout

```text
src/paglets/core/            paglet model, messages, lifecycle events
src/paglets/runtime/         host facade, child processes, HTTP, relay, storage runtime
src/paglets/remote/          clients, proxies, transfer tickets, mesh, admin API
src/paglets/persistence/     inactive records and managed storage
src/paglets/services/        service contracts and resident services
src/paglets/serialization/   dataclass wire conversion and import resolution
src/paglets/config/          launch config and bundled defaults
src/paglets/tooling/         CLI, discovery, git auto-update
src/paglets/examples/        packaged example agents and CLIs
demos/                       runnable source-tree demo scripts
tests/                       behavior-oriented test suites by topic
```

## Status

`paglets` is early-stage software for experiments and trusted local/LAN meshes. Use API-key authentication for shared networks and relay deployments.

## License

MIT. See [LICENSE](LICENSE).
