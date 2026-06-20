# Paglets Usage Ideas

`paglets` is useful when a workflow benefits from moving a small stateful object
to where data, services, or local machine context already live. In this runtime,
hosts are expected to have the same paglet code importable; the moving part is
the paglet identity, dataclass state, and behavior class reference.

For simple CRUD calls, public APIs, auth-heavy business operations, or short
request/response actions, a normal HTTP API is usually the better tool.

## Good Fits

### Infrastructure Inspection

Send a paglet to each server and let it inspect local-only state:

- disk usage and mounted volumes
- service status
- local logs
- configuration drift
- installed software versions

The paglet returns summarized findings instead of streaming raw machine data
back to a central client.

Example:

```bash
uv run python examples/disk_survey_demo.py --hosts alpha beta gamma
```

This starts local hosts, lets the parent paglet discover same-version mesh
hosts through `context.available_hosts()`, clones child paglets to each target,
and prints per-host volume summaries.

### Edge and IoT Coordination

An edge paglet can move close to sensors, devices, or local gateways, keep its
own retry/progress state, and report later when connectivity is available.

This is useful when:

- network links are slow, unstable, or expensive
- local devices are not exposed through a central API
- a node should keep working while the original controller is offline

### Data-Local Analytics

Instead of pulling large data to one process, send computation to the host that
already has the data. The paglet can filter, aggregate, redact, and return only
small results.

Good examples:

- log summarization
- local index search
- privacy-preserving aggregation
- per-site metrics collection

### Distributed Search and Crawling

A parent paglet can clone children to multiple hosts. Each child searches local
files, indexes, or services, then returns findings to the parent. The parent
deduplicates, ranks, or summarizes the results.

This fits the paglets model well because clone and message operations are part
of the runtime:

```python
for host in self.context.available_hosts():
    self.clone_to(host.name)
```

### Stateful Long-Running Workflows

A paglet can carry:

- progress
- partial results
- retry state
- itinerary
- next-step logic

That makes it useful for workflows where an object should visit multiple hosts
or services over time without a central process driving every small step.

### Mesh Administration

In a trusted mesh, paglets can help with operational checks:

- discover online/offline hosts
- compare host versions
- inspect agent counts
- dispatch diagnostics
- collect and summarize health reports

Use the context helpers from inside a paglet:

```python
hosts = self.context.available_hosts()
target = self.context.wait_for_host("beta", timeout=5.0)
self.clone_to(target.name)
```

Use the host mesh from the command line:

```bash
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

### Federated Experiments and Simulations

Paglets can represent participants, workers, jobs, or simulated entities that
move between hosts and accumulate state through lifecycle events.

This is useful when modeling:

- mobile actors
- market or resource simulations
- distributed worker behavior
- autonomous task delegation

## When Not To Use Paglets

Prefer a normal API when:

- the operation is simple request/response
- the caller only needs CRUD over a stable resource
- remote code execution would add unnecessary risk
- different hosts do not share the same codebase
- strong authentication, authorization, and audit controls are required but not
  yet implemented

The sweet spot is trusted hosts, shared code, local data/resources, distributed
inspection or computation, and stateful workflows.
