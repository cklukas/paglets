# Implementing Paglets

This guide focuses on the code you write when creating paglets.

## Define State Explicitly

A paglet moves with one dataclass state object. Keep durable workflow state in
that dataclass. Treat ordinary instance attributes as transient runtime data.

```python
from dataclasses import dataclass, field
from paglets import Paglet, PagletState


@dataclass
class TravellerState(PagletState):
    visits: list[str] = field(default_factory=list)
    home_url: str = ""


class Traveller(Paglet[TravellerState]):
    State = TravellerState
```

State values must be JSON-compatible after dataclass serialization. That keeps
movement between hosts explicit and inspectable.

Active paglets run in child Python processes started with the `spawn` method.
Both the paglet class and the state class must be importable by module path, for
example `myapp.agents:Traveller`. Classes defined in `__main__`, a REPL, stdin,
or a throwaway script cannot be started as paglets because the child process
cannot re-import them.

Process isolation means a crash or `sys.exit()` in one paglet does not directly
kill the host or other paglets, and multiple worker paglets can use multiple CPU
cores. It also means paglet startup is heavier than constructing an in-memory
object, so prefer coarser batches for very small units of work.

## Implement Lifecycle Hooks

Override only the lifecycle hooks you need:

```python
def on_creation(self, event):
    self.state.visits.append(f"created:{event.host_name}")

def on_dispatching(self, event):
    self.state.visits.append(f"leaving:{event.source_host_name}")

def on_arrival(self, event):
    self.state.visits.append(f"arrived:{event.host_name}")

def on_clone(self, event):
    self.state.visits.append(f"clone:{event.host_name}")

def run(self):
    self.state.visits.append(f"run:{self.context.name}")
```

`run()` is invoked after creation, arrival, clone arrival, and activation. Do not
expect call stacks, threads, sockets, or open files to move with the paglet.

## Handle Messages

Paglets talk through messages delivered to `handle_message`.

```python
from paglets import Message


def handle_message(self, message: Message):
    if message.kind == "status":
        return {"host": self.context.name, "visits": list(self.state.visits)}
    return self.not_handled()
```

Callers use a proxy:

```python
reply = proxy.send(Message("status"))
```

Supported communication patterns:

- synchronous replies with `send(Message(...))`;
- fire-and-forget delivery with `send_oneway(Message(...))`;
- future-style replies with `send_future(Message(...))`;
- local broadcast through `context.multicast`.

Normal messages are delivered through the target paglet's mailbox. The mailbox
selects higher-priority queued messages before lower-priority queued messages
and keeps FIFO order within one priority. Use `UNQUEUED_PRIORITY` only for the
explicit immediate bypass.

Paglets can coordinate mailbox handlers:

```python
self.wait_message(timeout=1.0)
self.notify_message()
self.notify_all_messages()
```

Use `wait_message()` when you need a low-level mailbox notification. When one
handler or background thread is waiting for another handler to change paglet
state, prefer the predicate-based state wait:

```python
if self.wait_state(lambda state: not state.pending, timeout=5.0):
    with self.locked_state() as state:
        return state.result
```

After mutating state that may satisfy a waiter, wake it explicitly:

```python
with self.locked_state() as state:
    state.result = reply
    state.pending = False
self.notify_all_state_changed()
```

Inactive paglets can still receive messages. By default, the host activates the
paglet and delivers the message. Use `no_delay=True` when the caller wants a
fast failure instead of activation or queueing:

```python
reply = proxy.send(Message("status"), no_delay=True)
```

## Protect Shared State

A paglet child handles one lifecycle or message command at a time. `run()` may
also start background threads. When two code paths read or write the dataclass
state, protect that short critical section with the paglet lock:

```python
with self.locked_state() as state:
    state.completed += 1
    state.results.append(result)
```

Use `locked()` for transient instance attributes that must be updated together
with other agent-local data:

```python
with self.locked():
    self._last_seen = time.monotonic()
    self.state.touched += 1
```

For small helper methods, `@state_locked` keeps the handler readable:

```python
from paglets import state_locked


@state_locked
def remember_result(self, result):
    self.state.results.append(result)
```

Keep locks around short state reads and writes only. Do not hold them while
waiting for another message, sleeping, calling a remote proxy, doing disk I/O,
or running a long computation.

`Paglet.MAILBOX_WORKERS` is ignored by the process runtime. Queued
`handle_message` calls are actor-style serial inside one paglet process.
Parallel CPU work should be split into multiple paglet instances, not multiple
message workers in the same instance.

Do not make a parent message handler block while waiting for child paglets to
send result messages back to that same parent. The parent cannot process those
messages until the current handler returns. Use this pattern instead:

1. A `start` or `collect` message stores request state, creates/clones workers,
   and returns quickly.
2. Workers run in their own paglet processes and send `child_result` messages.
3. The parent records results, calls `notify_all_state_changed()`, and exposes a
   `drain` or `summary` message for clients to poll.

## Move Between Hosts

Use `dispatch` when the current paglet should move away:

```python
self.dispatch("http://127.0.0.1:8766")
```

Use `clone` when the current paglet should keep running and send a copy:

```python
clone_proxy = self.clone("http://127.0.0.1:8766")
```

When hosts are in the mesh, prefer name-based helpers:

```python
target = self.context.wait_for_host("beta", timeout=5.0)
self.clone_to(target.name)
```

Use a `TransferTicket` for preflight checks, retries, or inactive arrival:

```python
from paglets import ArrivalMode, TransferTicket

self.dispatch(
    TransferTicket(
        "beta",
        required_capabilities=("agents:create",),
        expected_code_version=self.context.host.mesh.code_version,
        arrival_mode=ArrivalMode.INACTIVE,
    )
)
```

## Discover Hosts

Paglets can inspect the local host's mesh registry:

```python
for host in self.context.available_hosts():
    if host.online:
        self.clone_to(host.name)
```

The registry is version-gated. Hosts with different code versions are ignored by
the mesh so paglets do not move into a host that probably cannot import the same
classes.

## Deactivate And Activate

Use `deactivate` when a paglet should stop its active child process but keep
durable state:

```python
proxy.deactivate()
proxy.activate()
```

A paglet can deactivate itself and choose its own inactive policy:

```python
import time
from paglets import DeactivationPolicy


def handle_message(self, message: Message):
    if message.kind == "pause":
        return self.deactivate(
            policy=DeactivationPolicy(activate_at=time.time() + 3600)
        ).to_wire()
    return self.not_handled()
```

Override `deactivation_policy` when the paglet should decide how external
deactivation requests behave:

```python
def deactivation_policy(self, request):
    return DeactivationPolicy(
        activate_on_message=False,
        queue_messages_when_inactive=True,
        activate_on_startup=request.reason == "shutdown",
    )
```

If `activate_on_message` is false and queueing is enabled, normal messages are
stored with the inactive record and delivered after activation. A `no_delay`
message fails immediately instead.

## Talk To Resident Services

Prefer typed service contracts for resident services. The packaged example
`server-info` service is a ready-made example: each host declares it from launch
config, callers can discover the `SERVER_INFO` contract immediately, and the
provider agent starts lazily on first use:

```python
from paglets import ServiceScope
from paglets.examples.system_info import GET_DISK, SERVER_INFO, DiskRequest


service = self.require_contract(SERVER_INFO, operation=GET_DISK, scope=ServiceScope.MESH)
reply = service.call(GET_DISK, DiskRequest(paths=["/"], all_volumes=False))
```

For custom services, put the `ServiceContract`, `ServiceOperation`, and payload
dataclasses in an importable module shared by provider and caller. The provider
uses `advertise_contract`, routes with `contract.route(...)`, and the caller
uses `require_contract` or `lookup_contract`.

Managed resident services are declared in launch config:

```toml
[[resident_services]]
class = "myapp.services.ticket_agent:TicketServiceAgent"
agent_id = "service.ticket"
lifecycle = "lazy"
scope = "mesh"
idle_timeout = 30.0
```

Use `lifecycle = "lazy"` when the service only needs to run while requests are
active. Use `lifecycle = "eager"` for continuous monitors or services that must
keep live local resources open. Lazy services deactivate after their idle
timeout, but their service record stays discoverable and a later call activates
them again.

TOML and JSON store these closed values as strings because those formats do not
have enums. Python code uses enum values such as `ServiceScope.MESH`;
configuration loading converts strings like `scope = "mesh"` at the boundary.

Use a lease when several calls should keep a lazy provider active:

```python
with self.lease_contract(SERVER_INFO, operation=GET_DISK, scope=ServiceScope.MESH) as service:
    first = service.call(GET_DISK, DiskRequest(paths=["/"]))
    second = service.call(GET_DISK, DiskRequest(paths=["/data"]))
```

The lower-level string API remains available when a fully typed contract is not
needed. `lookup_service` returns a serializable `PagletProxyRef`, which can be
stored in dataclass state or resolved to a proxy:

```python
service_ref = self.lookup_service("flight-ticket", capability="quote", scope=ServiceScope.MESH)
if service_ref is not None:
    reply = service_ref.resolve(self.context).send(Message("quote", {"from": "FRA", "to": "SFO"}))
```

## Observe Context Events

Hosts keep a bounded in-memory context event log and deliver events to
listeners. Events cover create, arrival, dispatch, clone, retract, deactivate,
activate, dispose, message delivery/failure, service changes, and transfer
failures.

```python
host.add_listener(lambda event: print(event.event_id, event.kind))
events = host.list_events(since=0, limit=100)
```

The HTTP API exposes the same log at `GET /events?since=<id>&limit=<n>`.

## Clean Up Runtime Resources

Only dataclass state moves or persists. Register transient resources that need
cleanup before dispatch, deactivate, retract, or dispose:

```python
self.resources.track_closeable("socket", sock)
self.resources.register("temp-file", lambda: path.unlink(), suppress=True)
```

Cleanup failures cancel the lifecycle operation unless that resource was
registered with `suppress=True`.

## Use Managed Storage

Use `work_dir()` for per-instance scratch files that should not survive final
departure or host restart:

```python
path = self.work_dir() / "batch.tmp"
path.write_text("intermediate", encoding="utf-8")
```

The host clears all work directories on startup. It also clears an instance's
work directory on dispatch, retract, or dispose. Deactivation keeps the work
directory while the same host runtime remains up, but a restart clears it.

Use `persistent_storage()` for small class-level state that should survive host
restart:

```python
store = self.persistent_storage()
store.write_text("checkpoint.txt", "ok")
data = store.read_bytes("checkpoint.txt")
```

Persistent storage is rooted under the host persistence directory, shared by
paglet class, and quota-accounted by the API. The default quota is 10 MiB per
class and can be changed with `paglets-host --persistent-storage-quota 20M` or
the `Host(..., persistent_storage_quota_bytes=...)` constructor argument.

## Query Mesh Placement

The packaged `mesh-info` resident service keeps fresh host resource snapshots,
including active/inactive paglet counts, and ranks eligible compute targets:

```python
from paglets import ServiceScope
from paglets.examples.mesh_info import MESH_INFO, SELECT_TARGETS, TargetSelectionRequest

mesh_info = self.require_contract(MESH_INFO, operation=SELECT_TARGETS, scope=ServiceScope.LOCAL)
targets = mesh_info.call(SELECT_TARGETS, TargetSelectionRequest(limit=2, max_load_per_cpu=1.0))
```

For distributed compute, keep the coordinator's accumulated job state on one
host and create short-lived worker paglets remotely. Workers should report
results by message and dispose themselves after sending the result. The
coordinator should return from its launch message quickly and use `drain` or
`summary` for progress, because it cannot handle worker result messages while a
previous message handler is still running. For
CPU-style batch work, treat a selected host as several placement slots instead
of only one target: estimate slots from `cpu_count * target_load_per_cpu -
load_1m`, subtract already in-flight workers on that host, and optionally cap
the result with a per-host limit. Keep one small fallback worker available when
all hosts are above the load threshold so long-running jobs still make minimum
progress.

## Keep Imports Stable

Movement sends class names like `examples.disk_survey_demo:DiskSurveyPaglet`.
Every target host must be able to import the same module and class name.

Use package modules, not ad-hoc script-only code, for paglets that should move
between independently started hosts.

## Test Locally

For local tests, create multiple host objects in one process:

```python
from paglets import Host

alpha = Host("alpha", port=8765, mesh_version="dev")
beta = Host("beta", port=8766, peers=["http://127.0.0.1:8765"], mesh_version="dev")
alpha.start_background()
beta.start_background()
```

Stop hosts in `finally` blocks or test fixtures so background server threads do
not leak between tests.
