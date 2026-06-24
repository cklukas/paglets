# Glossary

Agent
: A general term for a running autonomous object. In this project the concrete
  class is called `Paglet`.

Aglet
: The Java mobile-agent concept that inspired this project.

Context
: The host-provided environment visible to a paglet through `PagletContext`.
  It exposes host name/address, proxy lookup, host properties, mesh helpers,
  movement helpers, service discovery, resources, and message helpers.

ContextEvent
: A host-level event record emitted for lifecycle, transfer, service, and
  message activity. Hosts keep a bounded in-memory event log.

AutoStartSpec
: A class-level marker declaring that a paglet can be started from launch
  config, usually with an alias, fixed agent ID, and singleton behavior.

Dispatch
: Move a paglet from one host to another. The source host removes the original
  after successful delivery.

Clone
: Copy a paglet to a target host while keeping the original alive. The clone
  receives a new agent ID.

Envelope
: The serialized transfer record containing movement kind, agent ID, class
  names, dataclass state, source host, target host, and clone metadata.

Host
: A runtime context that supervises active paglet processes, durable inactive
  records, mesh state, and the JSON HTTP API.

Inactive
: A deactivated paglet stored as a durable record instead of an active child
  process. Inactive paglets can be activated explicitly, by policy, or by
  incoming messages.

HostRef
: A mesh registry record containing host name, URL, code version, online status,
  last-seen timestamp, agent counts, and optional error text.

Itinerary
: A serializable plan that lets a paglet visit hosts and run tasks at lifecycle
  points.

LaunchConfig
: The `~/.paglets/launch.toml` startup configuration read by `paglets-host`.
  The bundled config declares built-in resident services such as `server-info`,
  `mesh-info`, `compute-slots`, and `user-info`.

Message
: A JSON-compatible command delivered to a paglet's `handle_message` method.

MessageMailbox
: The per-active-paglet queue used for normal message delivery. It orders queued
  work by priority and FIFO order within one priority. The process runtime sends
  at most one queued message at a time to a paglet child process.

Paglet Process
: The child Python process that runs one active paglet instance. The host
  supervises it, communicates with it over a private pipe, and reports abnormal
  exits as `PagletCrashedError`.

Mesh
: The same-version host registry built from seed-list gossip and optional
  multicast beacons.

MeshInfoAgent
: The built-in eager resident service agent advertised as `mesh-info`. It
  samples local system data through `server-info`, syncs snapshots with peers,
  and ranks eligible compute targets.

Paglet
: A Python mobile object with explicit dataclass state, lifecycle hooks, and
  message handling.

PagletProxy
: A controlled handle to a paglet. Proxies send messages and request lifecycle
  operations without exposing direct object references.

PagletProxyRef
: A serializable reference containing host URL and agent ID. It can be stored in
  paglet state and later resolved to a `PagletProxy`.

PerformanceBenchmarkAgent
: The packaged example mobile benchmark agent used by `paglets-perf-test`. It
  clones workers across the mesh, runs local CPU, memory, and bounded disk I/O
  tests, and reports results to the parent agent.

Performance Benchmark Lock
: A host-local lock used by benchmark workers so multiple benchmark paglets on
  the same server run sequentially while different servers can run in parallel.

ResourceRegistry
: Runtime-only cleanup registry owned by a paglet. Cleanup runs before
  dispatch, deactivate, retract, or dispose.

Runtime Value Enum
: A closed runtime value represented by a Python enum, such as
  `ServiceScope`, `ResidentLifecycle`, `ArrivalMode`, `EnvelopeKind`, or
  `LaunchConfigSyncAction`. Python APIs require these enum values. TOML, JSON,
  and HTTP wire formats store strings and convert them at the boundary.

ResidentService
: A launch-config managed service declaration. Lazy resident services are
  discoverable before the provider is active and start on first use; eager
  resident services activate immediately.

ServiceContract
: An importable typed service definition containing a service name, exact
  version, and typed operations. Providers advertise it; callers look it up to
  get a typed `ServiceHandle`.

ServiceHandle
: A resolved typed client for a `ServiceContract`. It wraps a discovered
  `ServiceRecord`, sends normal paglet messages, and decodes typed replies.

ServiceLease
: A TTL-backed handle that keeps a lazy managed resident service active across
  multiple calls until released or expired.

ServiceOperation
: A typed operation within a `ServiceContract`, including the stable wire
  message name and dataclass request/reply schemas.

ServiceRecord
: A service registry entry containing a service name, capabilities, metadata,
  scope, and a `PagletProxyRef` for the providing paglet.

ServerInfoAgent
: The built-in resident service agent advertised as `server-info`. It
  starts lazily on first use and reports load, memory, disk usage, process
  matches, and host summary data.

State
: The dataclass object that moves with a paglet. Ordinary instance attributes
  are transient.

State Lock
: The reentrant per-paglet lock used by `locked_state()`, `locked()`, and
  `@state_locked` to protect short shared-state critical sections.

TransferTicket
: Transfer options for dispatch or clone, including destination, retry policy,
  required host capabilities, expected code version, and arrival mode.
