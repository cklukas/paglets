# paglets/cpp: round 2 paglets

Status: idea collection, accepted for later. These paglets are implemented
once the system is mature: after the first release and the demo paglets in
[cpp-demo-paglets.md](cpp-demo-paglets.md). Companion to
[cpp-edition-plan.md](cpp-edition-plan.md) and
[cpp-security-and-communication.md](cpp-security-and-communication.md).

## Why mobile agents now

Mobile agents were ahead of their time when Java Aglets appeared. Several
things that held them back are solved in paglets/cpp: code runs safely
anywhere (Wasm sandbox, capabilities), agents move with their full memory,
access is governed mesh-wide, and local AI gives some hosts real
"intelligence" to visit.

Moving an agent is worth it when at least one of these holds:

1. **Data gravity**: the data is too big, confidential, or bound to hosts by
   residency rules.
2. **Local capability**: something exists only on some hosts (AI, internet
   access, GPU, instrument, license).
3. **Intermittent connectivity**: laptops, field devices, sleeping machines.
4. **Survival**: the work must outlive any single host.
5. **Meeting places**: parties that do not fully trust each other let their
   agents meet on a neutral host.

Every round 2 paglet below names the reasons that make it mobile.

## Overview

| # | Paglet | Group | Mobile because of |
|---|---|---|---|
| R2-01 | Federated Statistics | Compute to data | Data gravity |
| R2-02 | Travelling Model Trainer | Compute to data | Data gravity |
| R2-03 | Query Agent | Compute to data | Data gravity |
| R2-04 | Instrument Harvester | Lab and instruments | Local capability, data gravity |
| R2-05 | Pipeline Traveller | Lab and instruments | Local capability |
| R2-06 | Hot-Folder Watcher | Lab and instruments | Local capability |
| R2-07 | Data Mule | Disconnected operation | Intermittent connectivity |
| R2-08 | Follow-Me Assistant | Disconnected operation | Intermittent connectivity |
| R2-09 | Follow the Awake | Disconnected operation | Survival |
| R2-10 | Janitor | Operations and security | Local capability |
| R2-11 | Patrol Auditor | Operations and security | Local capability |
| R2-12 | First Responder | Operations and security | Local capability, survival |
| R2-13 | Maintenance Evacuator | Operations and security | Survival |
| R2-14 | Energy-Aware Placement | Operations and security | Local capability |
| R2-15 | Clean Room | Meeting places | Meeting places, data gravity |
| R2-16 | Resource Broker | Meeting places | Meeting places |
| R2-17 | Mission Agent | Mobile AI agents | Local capability |
| R2-18 | MCP Gateway | Mobile AI agents | Local capability |
| R2-19 | Expert Agents | Mobile AI agents | Local capability, data gravity |

## Compute goes to the data

### R2-01 Federated Statistics

- **Idea**: visits hosts with confidential tables, computes aggregates locally
  (counts, sums, distributions, correlations) and carries only the aggregates
  on; optionally adds calibrated noise (differential privacy) so individual
  records cannot be inferred.
- **Why mobile**: raw data stays where it is; residency marks prevent it from
  leaving, and only an admin-approved `declassify` step for the aggregate
  result lets the answer travel.
- **Needs**: data residency with declassify rules (WP17), a small statistics
  library in the guest SDK, reviewed aggregation modules as the only ones
  allowed to declassify.

### R2-02 Travelling Model Trainer

- **Idea**: a small machine-learning model is the paglet's memory. It trains
  for a while on each host's local data and moves on; the model improves with
  every visit while the data never moves.
- **Why mobile**: data gravity and residency; the model image is small
  compared to the data.
- **Needs**: numeric libraries compiled to Wasm (SIMD), dedicated workers,
  checkpoints between hosts, optional declassify rule for the final model.

### R2-03 Query Agent

- **Idea**: answers questions like "how many samples of type X per month?"
  against CSV or Parquet files wherever they are, and returns only the answer
  (and optionally the query plan and row counts per host).
- **Why mobile**: large or restricted files are read in place.
- **Needs**: a query engine compiled to Wasm (for example a small SQL engine
  over CSV/Parquet), `files` range reads or mounts, residency-aware result
  handling.

## Lab and instrument work

### R2-04 Instrument Harvester

- **Idea**: lives on instrument PCs (often locked-down Windows machines),
  waits until a measurement file is complete (stable size, no writer lock,
  optional end marker), verifies it, carries it to a processing host and
  starts the analysis there; reports to the owner.
- **Why mobile**: the instrument PC is the only place where the data appears;
  it should run nothing heavy and may have restricted network access.
- **Needs**: `files` change notifications or efficient polling with
  completeness checks (per platform), resident paglets on instrument hosts,
  artifacts with resume for large raw files.

### R2-05 Pipeline Traveller

- **Idea**: one paglet carries a sample's data through all stages, each on the
  right host: instrument, processing (for example GPU), AI host for report
  text, archive. It keeps the full history of its journey in memory and is
  therefore its own audit trail.
- **Why mobile**: each stage has its own local capability.
- **Needs**: service offers for processing capabilities, itineraries with
  conditions and retries, provenance export (journey log as artifact).

### R2-06 Hot-Folder Watcher

- **Idea**: watches folders; when files appear, it starts the right processing
  paglet and sends it to the best host for the job (by offers and load).
- **Why mobile**: processing capability is elsewhere; the watcher is small and
  stays put, the workers travel.
- **Needs**: `files` change notifications, offer-based placement, rules per
  file type.

## Disconnected operation

### R2-07 Data Mule

- **Idea**: hops onto a laptop before it leaves the network, and delivers,
  collects or synchronizes when the laptop comes back or reaches another site
  (store and forward).
- **Why mobile**: intermittent connectivity; the laptop is the only link.
- **Needs**: hosts that go offline and come back gracefully, deferred message
  delivery (store and forward), ledger sync after long offline periods.

### R2-08 Follow-Me Assistant

- **Idea**: follows its owner to whichever machine they are currently active
  on and carries notifications, pending results and small handoffs ("open this
  file here").
- **Why mobile**: the user moves between machines; the assistant should be
  where the user is.
- **Needs**: a `presence` system paglet (which owner is active on which host,
  with the owner's consent), desktop notification integration per platform.

### R2-09 Follow the Awake

- **Idea**: a small service (team key/value store, notice board, scheduler)
  that keeps itself alive by moving away from machines that are going to
  sleep or shutting down; clients always reach it through the locator.
- **Why mobile**: survival without any fixed server, matching "all hosts are
  equal".
- **Needs**: host shutdown and sleep announcements (per platform power
  events), fast evacuation, replicated checkpoints.

## Operations and security

### R2-10 Janitor

- **Idea**: removes old scratch data, temporary files and orphaned artifacts
  according to rules, with narrow delete grants, and reports what it removed
  and how much space it freed.
- **Why mobile**: the cleanup has to happen on each host; one paglet tours the
  mesh.
- **Needs**: `files` delete grants with age and pattern scopes, dry-run mode,
  artifact store queries.

### R2-11 Patrol Auditor

- **Idea**: read-only rounds over the mesh: expiring certificates, world-
  writable folders, unexpectedly large files, outdated software versions,
  hosts missing updates. Produces a findings report and trends.
- **Why mobile**: checks need local access on every host, without installing
  audit tools everywhere.
- **Needs**: read grants on system roots, certificate parsing in the guest
  SDK, `server-info` software version queries.

### R2-12 First Responder

- **Idea**: when a host shows trouble (disk full, load spike, errors reported
  by Log Scout), it moves there immediately and captures a snapshot
  (processes, logs, volumes, recent events) before the evidence disappears,
  then carries it to the admins.
- **Why mobile**: speed and locality; the evidence exists only on that host
  and only for a short time.
- **Needs**: triggers from monitoring paglets, pre-approved incident grants,
  high-priority placement.

### R2-13 Maintenance Evacuator

- **Idea**: before a host goes down for updates, moves all roaming paglets to
  suitable hosts (respecting offers, residency and pins) and brings them back
  afterwards if wanted.
- **Why mobile**: survival across planned downtime.
- **Needs**: host maintenance mode, admin-triggered evacuation, placement
  respecting grants and residency; later part of the host itself.

### R2-14 Energy-Aware Placement

- **Idea**: moves flexible compute to hosts that are idle, in off-peak hours
  or running on surplus solar power, and away when conditions change.
- **Why mobile**: the best place to compute changes over time.
- **Needs**: energy and tariff information as service offers, compute-slots
  integration, live rebalancing.

## Meeting places

### R2-15 Clean Room

- **Idea**: agents from two parties (for example two departments) meet on a
  neutral host, compute an agreed function over both data sets (overlap, join,
  comparison: "which compounds do we both have?"), and each carries home only
  the agreed result.
- **Why mobile**: neither party gives its data to the other; a neutral host
  and residency marks enforce that raw data never leaves the clean room.
- **Needs**: clean-room hosts and roots with residency rules, reviewed and
  signed clean-room modules that both parties' policies accept, declassify
  rules for the agreed result only.

### R2-16 Resource Broker

- **Idea**: agents with a budget bid for compute slots, AI time or web
  bandwidth; hosts sell capacity. Scheduling emerges from negotiation instead
  of a central scheduler.
- **Why mobile**: agents negotiate where the resources are and move to the
  winning host.
- **Needs**: budgets or credits in the ledger, an auction protocol between
  system paglets and agents, fairness rules.

## Mobile AI agents

### R2-17 Mission Agent

- **Idea**: a paglet with a goal and a plan, for example "find all reports on
  project X, check which are outdated, draft a summary of open issues". On
  each host it uses the local tools (`files`, `server-info`, `web`) through
  system paglets, and asks an AI host for its next step. Irreversible or
  sensitive actions go through `ask` grants, so a human approves them.
- **Why mobile**: tools and data are spread over the mesh; the agent goes to
  them. It is also the answer to a current problem: autonomous AI agents lack
  confinement. Here the agent only holds the handles it was granted, every
  action is audited, and humans stay in the loop.
- **Needs**: tool descriptions generated from service contracts, planning
  loop helpers in the guest SDK, `ai.generate` with structured output, budget
  limits (steps, tokens, time).
- **Continued in round 3**: generalized into chat-capable persona agents, see
  [cpp-persona-agents.md](cpp-persona-agents.md).

### R2-18 MCP Gateway

- **Idea**: a system paglet that exposes mesh services (`files`,
  `server-info`, `web`, `ai`, demo paglets) as tools over the Model Context
  Protocol, so existing AI clients can use the mesh, with every call checked
  against grants and audited under the calling owner's key.
- **Why mobile**: the gateway itself stays put; the tool calls it makes can
  dispatch paglets to wherever the work is.
- **Needs**: a network-facing system paglet (MCP transport), owner
  authentication for MCP clients, mapping from contracts to tool schemas.

### R2-19 Expert Agents

- **Idea**: specialized agents stay resident near their domain (for example a
  "chemistry literature" agent on a host with the right documents and model).
  Mission agents visit them to ask questions instead of every host needing
  every model and every document.
- **Why mobile**: data gravity for the expert's knowledge; the questioners
  travel.
- **Needs**: resident paglets with published offers, semantic search (demo
  17) as a building block, access rules per expert.

## Platform features needed for round 2

| Feature | Needed by |
|---|---|
| Declassify rules for reviewed modules | R2-01, R2-02, R2-03, R2-15 |
| `files` change notifications and completeness checks | R2-04, R2-06 |
| Store-and-forward delivery, long offline periods | R2-07 |
| Presence system paglet | R2-08 |
| Power and shutdown announcements, host maintenance mode | R2-09, R2-13 |
| Energy and tariff offers | R2-14 |
| Budgets and credits in the ledger, auction protocol | R2-16 |
| Tool descriptions from contracts, planning helpers, structured AI output | R2-17, R2-18 |
| Network-facing system paglets (MCP) | R2-18 |
| Numeric and query engines compiled to Wasm | R2-02, R2-03 |
