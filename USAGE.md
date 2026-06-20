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

## Agent-to-Agent Scenarios

Paglets become more interesting when mobile agents can talk to resident service
agents or to other visiting agents. The current runtime already supports this
through `PagletProxy` and `Message`:

```python
reply = service_proxy.send_message("quote", {"from": "FRA", "to": "SFO"})
```

Inside an agent, implement message handling:

```python
def handle_message(self, message):
    if message.kind == "quote":
        return {"price": 742, "currency": "EUR"}
    return self.not_handled()
```

### Market and Brokerage Agents

A user sends a buyer paglet with preferences and budget. It moves to service
hosts, talks to resident quote agents, compares offers, and returns with ranked
findings.

Examples:

- flight tickets
- hotels
- car rental
- cloud spot compute
- energy prices
- shipping quotes
- data-provider pricing

Resident service agents can expose message kinds such as `quote`, `watch`,
`terms`, `hold`, `reserve`, and `cancel_hold`.

### Negotiation and Auction Agents

Agents can meet on an exchange host and negotiate with each other directly. One
resident auctioneer paglet can mediate bids, or buyer/seller agents can exchange
messages through proxies.

Examples:

- buyer agents bidding for limited compute
- seller agents publishing offers
- resource auctions
- agents revising bids over time from their own budget and strategy state

This is more agent-like than a plain API because each participant carries its
own constraints, preferences, and accumulated negotiation state.

### Resident Local Expert Agents

Each host can run resident expert paglets for local resources. A visiting paglet
does not need raw shell or database access everywhere; it asks local expert
agents for approved operations.

Examples:

- `DiskService`
- `LogService`
- `ProcessService`
- `PackageService`
- `DatabaseService`
- `MetricsService`

A visiting diagnostic paglet can move to a host, locate the local experts, ask
for summaries, and move on.

### Distributed Incident Response

A coordinator paglet clones responders to hosts in the mesh. Each responder
talks to local service-monitor, log-reader, and config-check paglets, then sends
findings back to the coordinator.

Example questions:

- why is checkout slow?
- which hosts have errors in the last five minutes?
- which service version changed recently?
- which cache nodes are unhealthy?

The parent paglet can correlate findings from web, database, cache, and worker
hosts.

### Data Pipelines

A workflow paglet can move through data-processing hosts and talk to local
extractor, transformer, validator, and loader agents.

Example flow:

1. Visit source host and ask an extractor agent for a batch summary.
2. Move to validation host and ask a validator agent for schema errors.
3. Move to warehouse host and ask a loader agent to stage accepted records.
4. Return with an audit trail and rejected-record summary.

### Federated Learning and Model Evaluation

A model-evaluator paglet can move to data-holding hosts, talk to local data
agents about permitted evaluation slices, run local evaluation, and return
metrics. Raw data stays on the host.

This works best when hosts share the same model/evaluator code and expose local
data access through resident service paglets.

### Scientific and Lab Automation

A protocol paglet can move between instrument hosts and talk to resident
instrument agents.

Examples:

- microscope agent
- sequencer agent
- storage agent
- analysis agent

The mobile paglet carries experiment state and decides the next step based on
local results.

### Personal Assistant Delegates

A user can send a delegate paglet to trusted service hosts. The delegate carries
user preferences and returns with findings or completed reservations.

Examples:

- calendar agents negotiating meeting times
- travel agents watching flight and hotel prices
- shopping agents monitoring price drops
- research agents visiting trusted index/search hosts

### Compliance and Audit Agents

An audit paglet can move to each host and talk to resident policy/config agents.

Example questions:

- which hosts expose port 443?
- which hosts have outdated dependencies?
- which data stores contain personal data?
- which systems are missing backup metadata?

Real compliance use would need stronger signing, authentication, audit logs, and
policy controls than this first runtime provides.

### Multi-Agent Workflows

Instead of one large agent, split behavior into specialists:

- planner paglet
- worker paglets
- verifier paglets
- reporter paglet

The planner clones workers, workers talk to local services, verifiers check
results, and the reporter summarizes the final state.

## Service Discovery Direction

Agent-to-agent scenarios work today if an agent already has a proxy, knows an
agent ID, or uses an application-specific registry. A useful future runtime
feature would be first-class service registration:

```python
self.context.advertise_service("flight-ticket", capabilities=["quote", "watch"])
ticket_service = self.context.lookup_service("flight-ticket")
```

That would let resident paglets advertise service names and capabilities while
mobile paglets discover them without hard-coding agent IDs.

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
