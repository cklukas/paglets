<img src="https://cklukas.github.io/paglets/repository-open-graph-1280x640.png" alt="paglets social preview" width="640">

# paglets

[![CI](https://github.com/cklukas/paglets/actions/workflows/ci.yml/badge.svg)](https://github.com/cklukas/paglets/actions/workflows/ci.yml)
[![Docs](https://github.com/cklukas/paglets/actions/workflows/docs.yml/badge.svg)](https://cklukas.github.io/paglets/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/paglets.svg)](https://pypi.org/project/paglets/)

Introduction / overview paper:

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.20820155.svg)](https://doi.org/10.5281/zenodo.20820155)

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
uv run paglets-artifacts list
uv run paglets-compute-slots status
uv run paglets-compute-groups
uv run paglets-analysis-jobs --tasks 3 --target-runtime 3
uv run paglets-file-grabber push ./data.bin --remote beta --dest /tmp/data.bin --dry
uv run paglets-search grep TODO .
uv run paglets-pi-compute --digits 32
```

The built-in `compute-slots` service admits coarse jobs by explicit
`cpu_cores`, expected RAM, and temp-storage estimates. It also gates new grants
by `max_load_per_cpu`, which defaults to `1.0` and means a host with `N` logical
CPUs stops starting additional jobs once its one-minute load reaches `N`. On
Linux and Windows it can best-effort pin granted jobs to allocated CPU IDs. New
compute job paglets can derive from `ComputeJobPaglet` so scheduling, wakeup,
redirects, affinity metadata, and lease release stay out of job-specific code.
`paglets-compute-slots status --blocked --usage` explains blocked queued jobs
and reports active job process-tree memory plus Paglets and application scratch
usage. `paglets-compute-slots jobs history` shows recent finished job runtime
and peak usage summaries.

For detached multi-job workflows, `ResultCollectorPaglet`,
`CollectingComputeJobPaglet`, and `submit_compute_job_group(...)` provide a
small job-group plus collector layer. Hosts can advertise placement metadata
with `paglets-host --tag TAG --property KEY=VALUE`, and compute jobs can
require, exclude, or prefer host tags.

Files that belong to a paglet instance can be registered with
`register_file(...)` and then move naturally with dispatch or clone. Larger
explicit payloads can use `ArtifactRef`, `HostClient.upload_artifact(...)`,
`PagletProxy.send_artifact(...)`, and the `paglets-artifacts` CLI.
The `paglets-file-grabber` example demonstrates this natural file mobility for
one-file push and pull operations between an entry host and one remote host.
Simple request/result paglets can use `TaskPaglet` and `TaskClient` from
`paglets.patterns.tasks`. Paglets with several named operations can use
`OperationPaglet` and `OperationClient`; clone fan-out examples can reuse
`MeshFanoutMixin` and `CursorDrainMixin` for child bookkeeping and streaming
drains. File-transfer paglets can subclass `SingleFileTransferPaglet` for the
default workflow or use `FileMobilityMixin` to keep custom file-transfer code
readable.

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
- [Detached Compute With A Collector](https://cklukas.github.io/paglets/examples/detached-compute-collector/)
- [Artifact Transport](https://cklukas.github.io/paglets/system/artifacts/)
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
src/paglets/artifacts.py     artifact references and host artifact storage
src/paglets/runtime/         host facade, child processes, HTTP, relay, storage runtime
src/paglets/remote/          clients, proxies, transfer tickets, mesh, admin API
src/paglets/patterns/        typed task, operation, coordination, notification, and file mobility helpers
src/paglets/persistence/     inactive records and managed storage
src/paglets/services/        service contracts and resident services
src/paglets/system/          built-in resident service agents
src/paglets/serialization/   dataclass wire conversion and import resolution
src/paglets/config/          launch config and bundled defaults
src/paglets/tooling/         CLI, discovery, git auto-update
src/paglets/examples/        packaged example agents and CLIs
demos/                       runnable source-tree demo scripts
tests/                       behavior-oriented test suites by topic
```

## Status

`paglets` is early-stage software for experiments and trusted local/LAN meshes.
Use API-key authentication for shared networks and relay deployments. Packaged
commands read `PAGLETS_API_KEY` automatically; pass `--api-key-env NAME` only
when the key lives in a different environment variable.

## License

MIT. See [LICENSE](LICENSE).
