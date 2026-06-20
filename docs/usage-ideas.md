# Usage Ideas

`paglets` is useful when a workflow benefits from moving a small stateful object
to where data, services, or local machine context already live. In this runtime,
hosts are expected to have the same paglet code importable; the moving part is
the paglet identity, dataclass state, and behavior class reference.

For simple CRUD calls, public APIs, auth-heavy business operations, or short
request/response actions, a normal HTTP API is usually the better tool.

## Good Fits

### Infrastructure Inspection

Send a paglet to each server and let it inspect local-only state:

- disk usage and mounted volumes;
- service status;
- local logs;
- configuration drift;
- installed software versions.

The paglet returns summarized findings instead of streaming raw machine data
back to a central client.

```bash
uv run python examples/disk_survey_demo.py --hosts alpha beta gamma
```

### Edge And IoT Coordination

An edge paglet can move close to sensors, devices, or local gateways, keep its
own retry/progress state, and report later when connectivity is available.

This is useful when network links are slow, local devices are not exposed
through a central API, or a node should keep working while the original
controller is offline.

### Data-Local Analytics

Instead of pulling large data to one process, send computation to the host that
already has the data. The paglet can filter, aggregate, redact, and return only
small results.

Examples:

- log summarization;
- local index search;
- privacy-preserving aggregation;
- per-site metrics collection.

### Distributed Search And Crawling

A parent paglet can clone children to multiple hosts. Each child searches local
files, indexes, or services, then returns findings to the parent. The parent
deduplicates, ranks, or summarizes the results.

```python
for host in self.context.available_hosts():
    self.clone_to(host.name)
```

### Stateful Long-Running Workflows

A paglet can carry progress, partial results, retry state, itinerary, and
next-step logic. That makes it useful for workflows where an object should visit
multiple hosts or services over time without a central process driving every
small step.

### Mesh Administration

In a trusted mesh, paglets can discover online/offline hosts, compare host
versions, inspect agent counts, dispatch diagnostics, and collect summarized
health reports.

```python
hosts = self.context.available_hosts()
target = self.context.wait_for_host("beta", timeout=5.0)
self.clone_to(target.name)
```

## Agent-To-Agent Scenarios

Paglets become more interesting when mobile agents talk to resident service
agents or other visiting agents. The runtime already supports this through
`PagletProxy` and `Message`.

### Market And Brokerage Agents

A user sends a buyer paglet with preferences and budget. It moves to service
hosts, talks to resident quote agents, compares offers, and returns with ranked
findings.

Examples include flight tickets, hotels, car rental, cloud spot compute, energy
prices, shipping quotes, and data-provider pricing.

Resident service agents can expose message kinds such as `quote`, `watch`,
`terms`, `hold`, `reserve`, and `cancel_hold`.

### Negotiation And Auction Agents

Agents can meet on an exchange host and negotiate directly. A resident
auctioneer paglet can mediate bids, or buyer/seller agents can exchange
messages through proxies.

This is more agent-like than a plain API because each participant carries its
own constraints, preferences, and accumulated negotiation state.

### Resident Local Expert Agents

Each host can run resident expert paglets for local resources. A visiting paglet
does not need raw shell or database access everywhere; it asks local expert
agents for approved operations.

Examples include `DiskService`, `LogService`, `ProcessService`,
`PackageService`, `DatabaseService`, and `MetricsService`.

### Distributed Incident Response

A coordinator paglet clones responders to hosts in the mesh. Each responder
talks to local service-monitor, log-reader, and config-check paglets, then sends
findings back to the coordinator.

Example questions:

- why is checkout slow?
- which hosts have errors in the last five minutes?
- which service version changed recently?
- which cache nodes are unhealthy?

### Data Pipelines

A workflow paglet can move through data-processing hosts and talk to local
extractor, transformer, validator, and loader agents.

### Federated Learning And Model Evaluation

A model-evaluator paglet can move to data-holding hosts, talk to local data
agents about permitted evaluation slices, run local evaluation, and return
metrics. Raw data stays on the host.

### Scientific And Lab Automation

A protocol paglet can move between instrument hosts and talk to resident
instrument agents such as microscope, sequencer, storage, and analysis agents.

### Personal Assistant Delegates

A user can send a delegate paglet to trusted service hosts. The delegate carries
user preferences and returns with findings or completed reservations.

Examples include calendar negotiation, travel monitoring, shopping price-drop
watching, and trusted research/search delegation.

### Compliance And Audit Agents

An audit paglet can move to each host and talk to resident policy/config agents.
Real compliance use would need stronger signing, authentication, audit logs, and
policy controls than this first runtime provides.

### Multi-Agent Workflows

Instead of one large agent, split behavior into specialists: planner, worker,
verifier, and reporter paglets. The planner clones workers, workers talk to
local services, verifiers check results, and the reporter summarizes final
state.

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
