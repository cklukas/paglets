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

Inactive paglets can still receive messages. By default, the host activates the
paglet and delivers the message. Use `no_delay=True` when the caller wants a
fast failure instead of activation or queueing:

```python
reply = proxy.send(Message("status"), no_delay=True)
```

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
from paglets import TransferTicket

self.dispatch(
    TransferTicket(
        "beta",
        required_capabilities=("agents:create",),
        expected_code_version=self.context.host.mesh.code_version,
        arrival_mode="inactive",
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

Use `deactivate` when a paglet should leave memory but keep durable state:

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

Advertise a service from the owning paglet:

```python
self.advertise_service(
    "flight-ticket",
    capabilities=("quote", "watch"),
    metadata={"version": 1},
    scope="mesh",
)
```

Look up a local or mesh-visible service. Lookups return a serializable
`PagletProxyRef`, which can be stored in dataclass state or resolved to a proxy:

```python
service_ref = self.lookup_service("flight-ticket", capability="quote", scope="mesh")
if service_ref is not None:
    quote = service_ref.resolve(self.context).send(Message("quote", {"from": "FRA", "to": "SFO"}))
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
