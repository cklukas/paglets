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
reply = proxy.send_message("status")
```

Supported communication patterns:

- synchronous replies with `send_message`;
- fire-and-forget delivery with `send_oneway_message`;
- future-style replies with `send_future_message`;
- local broadcast through `context.multicast`.

Inactive paglets can still receive messages. By default, the host activates the
paglet and delivers the message. Use `no_delay=True` when the caller wants a
fast failure instead of activation or queueing:

```python
reply = proxy.send_message("status", no_delay=True)
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

Today, agent-to-agent communication works when the paglet already has a proxy,
knows an agent ID, or uses an application-specific registry such as the finder
demo.

```python
service = self.context.get_proxy(service_agent_id, service_host_url)
quote = service.send_message("quote", {"from": "FRA", "to": "SFO"})
```

A future first-class service registry could make this more direct:

```python
# Conceptual future API
self.context.advertise_service("flight-ticket", capabilities=["quote", "watch"])
ticket_service = self.context.lookup_service("flight-ticket")
```

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
