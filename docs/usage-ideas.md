# Usage Ideas

`paglets` is useful when a workflow benefits from moving a small stateful object
to where data, services, or local machine context already live. In this runtime,
hosts are expected to have the same paglet code importable; the moving part is
the paglet identity, dataclass state, and behavior class reference.

For simple CRUD calls, public APIs, auth-heavy business operations, or short
request/response actions, a normal HTTP API is usually the better tool.

The process-isolated runtime shifts the tradeoff further toward coarse,
stateful, location-aware work. Each active paglet is a child Python process, so
crashes and CPU-bound loops are isolated and multiple worker paglets can use
multiple cores. The cost is process startup, pipe IPC, and the requirement that
all paglet classes be importable by module path on every target host. Very tiny
high-frequency calls are usually better batched or implemented as a resident
service operation.

## Good Fits

### Infrastructure Inspection

Send a paglet to each server and let it inspect local-only state:

- disk usage and mounted volumes;
- service status;
- local logs;
- configuration drift;
- installed software versions.

The paglet returns summarized findings instead of streaming raw machine data
back to a central client. The packaged example `server-info` service and
`paglets-sysinfo` CLI provide this as a concrete demo:

```bash
uv run paglets-sysinfo df
uv run paglets-sysinfo load
uv run paglets-sysinfo plist python --limit 10
```

For performance checks, `paglets-perf-test` demonstrates a service-free mobile
agent pattern. The entry host creates one parent paglet, the parent clones
benchmark workers to all online same-version mesh hosts, and each worker runs
local CPU, memory, and bounded disk I/O tests before reporting centrally:

```bash
uv run paglets-perf-test
uv run paglets-perf-test --duration 2 --disk-size 256M
uv run paglets-perf-test --path /data --json
```

Disk benchmarks write temporary files only under writable benchmark directories,
falling back to `~/.paglets/benchmarks` or the OS temp directory when a volume
mountpoint itself is not writable. Normal output hides skipped read-only and
special mounts; use `--verbose` for that detail. Results are useful for
practical host-to-host comparison, not calibrated hardware certification.

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

## LLM And AI Agents

LLM-backed agents are a natural fit for paglets because the useful mobile part
is often not the model itself, but the agent's state: persona, task, constraints,
memory summary, current plan, partial findings, tool permissions, and itinerary.
The model runtime can stay local to each host while the paglet moves the
serializable work context through the mesh.

A practical AI paglet might carry state such as:

- persona and operating style;
- user task and success criteria;
- allowed tools and safety constraints;
- short-term memory or summarized conversation history;
- open questions and hypotheses;
- per-host findings and confidence notes;
- next hosts to visit or clone targets.

When the agent arrives on a host, it can ask a local LLM adapter, such as a
resident paglet wrapping Ollama, a hosted model API, Codex-like code assistant
workflow, or an application-specific planner. The adapter can provide reasoning,
summarization, code analysis, natural-language planning, or report writing
without requiring model handles, API clients, credentials, or GPU resources to
move with the paglet.

```python
@dataclass
class AnalysisAgentState:
    persona: str
    task: str
    constraints: list[str]
    memory_summary: str
    findings: dict[str, str]
    itinerary: list[str]
```

This makes several patterns possible:

- a code-review paglet moves to servers with local source trees, asks a local
  model or code assistant to inspect specific modules, and returns summarized
  findings;
- an operations paglet visits production, staging, and build hosts, talks to
  resident log, metrics, and LLM-summary agents, then composes an incident
  report;
- a research paglet clones itself across data-holding hosts, with each clone
  using local retrieval and summarization before returning only conclusions and
  citations;
- a planner paglet creates specialist clones for implementation, verification,
  security review, and documentation, then merges their results into one final
  state;
- a personal delegate carries user preferences and negotiates with trusted
  service agents, using an LLM only to interpret offers, explain trade-offs, or
  prepare a final recommendation.

In this model, an "intelligent agent" is still an ordinary paglet with explicit
state and lifecycle hooks. The LLM is a host-local capability the paglet may use
at each stop. That keeps mobility clear: clone and dispatch move persona, task,
memory, and plan; local agents provide tools, retrieval, execution, and model
access.

Real deployments should treat AI paglets as security-sensitive. Hosts should
control which resident services an arriving paglet may call, what local data can
be included in prompts, how outputs are audited, and whether an agent is allowed
to clone itself. For sensitive meshes, the useful default is to move summaries,
decisions, and provenance rather than raw prompts, credentials, private files, or
unbounded conversation history.

## Service Discovery

Agent-to-agent scenarios do not need hard-coded agent IDs. Resident paglets can
advertise local or mesh-visible services, and mobile paglets can discover them
by service contract:

```python
from paglets.core.runtime_values import ServiceScope

QUOTE = ServiceOperation("quote", QuoteRequest, QuoteReply)
FLIGHT_TICKETS = ServiceContract("flight-ticket", operations=(QUOTE,), version="1")

self.advertise_contract(FLIGHT_TICKETS, scope=ServiceScope.MESH)
tickets = self.require_contract(FLIGHT_TICKETS, operation=QUOTE, scope=ServiceScope.MESH)
reply = tickets.call(QUOTE, QuoteRequest("FRA", "SFO"))
```

The stable wire identifiers are still strings, but they are declared once in a
shared importable contract instead of repeated across agents. The contract also
defines dataclass request and reply schemas, so callers get a typed handle while
the runtime keeps the existing JSON message and service registry model.
Closed runtime values such as service scope are enums in Python APIs; TOML and
JSON keep string values and are converted at the boundary.
