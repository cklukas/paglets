# paglets

`paglets` is a compact Python re-imagining of the Java Aglets idea: mobile,
object-oriented agents with lifecycle events, message passing, proxy-based
control, and explicit serializable state.

## Why mobile agents?

Aglets was a Java mobile-agent system from the mid-1990s, developed when Java's
portable bytecode, object serialization, applets, and network APIs made it
plausible to treat programs as objects that could move between networked hosts.
An aglet was not just a remote procedure call or a batch job. It was an object
with identity, state, lifecycle callbacks, a mailbox-backed message interface, and
a proxy through which other code interacted with it. The original idea was that
computation could travel to the place where data, services, or users were,
continue there, and later move again.

That was innovative because most distributed systems were built around fixed
services: a client called a server, a scheduler launched a job on a selected
machine, or messages were queued for workers that already lived somewhere.
Mobile agents inverted part of that model. Instead of repeatedly shipping data
back to one program, the program's state could move toward the data or service.
Instead of only submitting a fire-and-forget job, callers could keep a proxy to a
living object, ask it questions, dispatch it elsewhere, clone it, retract it, or
dispose of it.

This is still an interesting model even though modern systems usually avoid
moving executable code between arbitrary machines for security and operations
reasons. A mobile agent is a useful mental model for workflows that have
identity, itinerary, accumulated state, and local behavior at each stop: data
collection, distributed inspection, edge/IoT coordination, simulation, crawling,
federated administration, and experiments where an object-oriented unit of work
is easier to understand than a loose set of jobs and callbacks.

`paglets` keeps that core idea but makes the mobility explicit and Pythonic.
Hosts are assumed to already have the same code importable, and only a dataclass
state object moves. The receiving host reconstructs the paglet from
`module:qualname` plus serialized state, then invokes lifecycle hooks such as
arrival, clone, activation, and disposal. This avoids pretending that Python can
transparently move stacks, threads, open sockets, or arbitrary runtime resources,
while preserving the useful Aglets blueprint: mobile objects with state,
messages, proxies, and lifecycle events.

This first implementation intentionally uses **one approach**:

- every target host already has the same Python code version installed/importable;
- no authentication or code upload is attempted;
- only a dataclass state object moves between hosts;
- runtime fields on the agent instance are transient;
- hosts communicate with a tiny JSON HTTP API;
- migration works equally across different machines or between two host processes
  on the same Mac using different ports.

The Aglets source in `aglets-git/src/` informed the names and
shape of this API: `Paglet`, `Host`/context, `PagletProxy`, `Message`, mobility
hooks, clone hooks, persistency hooks, `dispatch`, `clone`, `retract`, `dispose`,
and synchronous/one-way message delivery.

## Install / run in development

```bash
cd paglets
uv run --with pytest pytest tests -q
```

Build or preview the documentation:

```bash
uv run --extra docs mkdocs build --strict
uv run --extra docs mkdocs serve
```

The project documentation is published with GitHub Pages from a GitHub Actions
artifact, without a separate documentation branch:

<https://cklukas.github.io/paglets/>

Run the demos:

```bash
uv run python examples/start_hello_demo.py
uv run python examples/mobility_events_demo.py
uv run python examples/message_patterns_demo.py
uv run python examples/itinerary_demo.py
uv run python examples/finder_demo.py
uv run python examples/clone_workers_demo.py
uv run python examples/simple_master_slave_demo.py
uv run python examples/disk_survey_demo.py
```

Run a standalone host process:

```bash
uv run paglets-host --name alpha --port 8765
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765
```

On first start, `paglets-host` copies the bundled demo launch config to
`~/.paglets/launch.toml`. That config declares the packaged example
`server-info` service on each host. It is lazy by default: callers can discover
it immediately, and the provider agent is created on first use:

```toml
[launch]
demo_config_id = "paglets-default-launch"
demo_config_version = "3"

[[resident_services]]
class = "paglets.examples.system_info.agent:ServerInfoAgent"
enabled = true
agent_id = "service.server-info"
singleton = true
lifecycle = "lazy"
scope = "mesh"
idle_timeout = 30.0
state = { service_scope = "mesh" }
```

If a newer bundled demo config ships later, interactive host starts ask whether
to replace the user config and move the old file to `launch.toml.old`.
Non-interactive starts never block; they keep the existing file and print a
warning. Use `--yes` to accept the update or `--no-sync-launch-config` to skip
demo config syncing.

Hosts form a lightweight version-gated mesh. A host exposes its own view at
`GET /hosts`, accepts seed joins at `POST /hosts/join`, and includes its
`code_version` in `/health`. Same-version peers are visible to paglets through:

```python
hosts = self.context.available_hosts()
beta = self.context.wait_for_host("beta", timeout=5.0)
self.clone_to(beta.name)
```

Useful host mesh flags:

```bash
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
uv run paglets-host --name gamma --port 8767 --no-mesh-multicast --peer http://127.0.0.1:8765
```

`--mesh/--no-mesh` controls the registry, `--peer URL` can be repeated,
`--mesh-multicast/--no-mesh-multicast` controls UDP beacons, and
`--persistence-dir` overrides the host's durable inactive-paglet directory.
Version resolution uses `--mesh-version`, then `PAGLETS_MESH_VERSION`, then the
current git commit, then a package-version fallback. Different versions are
ignored by the mesh.

Start the multi-server TUI admin console:

```bash
uv run --extra tui paglets-tui \
  --server alpha=http://127.0.0.1:8765 \
  --server beta=http://127.0.0.1:8766
```

You can also start it from a saved server config:

```bash
uv run --extra tui paglets-tui
```

The TUI loads and persists server entries in `~/.paglets/servers.json` by
default. Use `--config /path/to/servers.json` to point it at another file.
`--server NAME=URL` entries are added to that config when the TUI starts:

```json
{
  "agent_discovery": {
    "paths": ["/path/to/paglets/examples"],
    "modules": []
  },
  "servers": [
    {
      "name": "alpha",
      "url": "http://127.0.0.1:8765",
      "enabled": true,
      "local_start": true
    },
    {
      "name": "beta",
      "url": "http://127.0.0.1:8766",
      "enabled": true,
      "local_start": true
    }
  ]
}
```

The TUI polls all enabled servers. If a configured local server
(`localhost`, `127.0.0.1`, or `::1`) is offline, select that server and press
`p` or the `Start` button. The TUI starts it as a local subprocess using the
same Python environment and passes the other configured servers as mesh peers:

```bash
python -m paglets.cli --name alpha --host 127.0.0.1 --port 8765 --peer http://127.0.0.1:8766
```

Remote URLs are monitored and administered, but are not started by the TUI.

Press `c` to create a paglet. The create dialog lists discovered `Paglet`
classes from `agent_discovery.paths` and `agent_discovery.modules`, fills the
agent class, state class, and initial state JSON for the selected class, and
still allows manual `module:qualname` entry. Press `g` to add a discovery path
or module, and `y` to remove one. Discovery is a TUI convenience only: target
servers must already be able to import the selected module name.

Query the packaged example server-info service across all online same-version mesh
hosts:

```bash
uv run paglets-sysinfo df
uv run paglets-sysinfo load
uv run paglets-sysinfo plist python --limit 10
```

`paglets-sysinfo` uses the first enabled reachable server in
`~/.paglets/servers.json` as the entry host, then creates a collector paglet
there. The collector clones to all online mesh hosts, calls each local
`server-info` service, starts lazy providers on demand, and prints the aggregate
result. For one-off use, pass `--server alpha=http://127.0.0.1:8765`; for
scripts, add `--json`.

Run a mesh-wide benchmark with a mobile agent:

```bash
uv run paglets-perf-test
uv run paglets-perf-test --json
uv run paglets-perf-test --duration 2 --disk-size 256M
```

`paglets-perf-test` also uses the first enabled reachable server as the entry
host, but it does not talk to a resident service. It creates a parent benchmark
paglet and clones workers to all online same-version mesh hosts. Each worker
runs CPU, memory, and bounded disk I/O tests locally, then reports results back
to the parent. Disk tests use temporary files under writable benchmark
directories, preferring `~/.paglets/benchmarks` and the OS temp directory when a
volume mountpoint itself is not writable. Use `--path /some/mount` to limit disk
tests, `--no-disk` to skip disk I/O, or `--verbose` to show skipped read-only
and special volumes.

Run a parent/child clone survey example:

```bash
uv run python examples/disk_survey_demo.py --hosts alpha beta gamma
```

This starts local hosts, advertises the available host list through the host
mesh, creates one parent paglet on `alpha`, and has the parent query
`context.available_hosts()` before cloning child paglets to each online
same-version host. Each child waits for the parent host to be online, collects
local volume usage, and messages the findings back to the parent. The command
prints mesh diagnostics, clone destinations, and then a table like:

```text
host         volume                                        size_gb    used_gb    free_gb
alpha        /                                              494.38     403.91      90.47
beta         /                                              494.38     403.91      90.47
gamma        /                                              494.38     403.91      90.47
```

## Core model

A paglet is a Python object with one explicit dataclass state object:

```python
from dataclasses import dataclass, field
from paglets import Message, Paglet, PagletState

@dataclass
class TravellerState(PagletState):
    itinerary: list[str] = field(default_factory=list)
    visits: list[str] = field(default_factory=list)

class Traveller(Paglet[TravellerState]):
    State = TravellerState

    def on_creation(self, event):
        self.state.visits.append(f"created@{event.host_name}")

    def on_dispatching(self, event):
        self.state.visits.append(f"leaving:{event.source_host_name}->{event.target_host_name}")

    def on_arrival(self, event):
        self.state.visits.append(f"arrived@{event.host_name}")

    def handle_message(self, message: Message):
        if message.kind == "go":
            return self.dispatch(message.args["target"]).to_wire()
        return self.not_handled()
```

Start two hosts in one Python process for local development:

```python
from paglets import Host, Message

alpha = Host("alpha", port=8765)
beta = Host("beta", port=8766)
alpha.start_background()
beta.start_background()

proxy = alpha.create(Traveller, TravellerState())
proxy.send(Message("go", {"target": beta.address}))
```

The agent with the same `agent_id` is removed from `alpha`, reconstructed on
`beta` from `Traveller`'s class path plus `TravellerState`, receives
`on_arrival`, and then `run` is invoked.

## Lifecycle hooks

Override only what you need:

```python
def on_creation(self, event): ...      # new local creation
def on_dispatching(self, event): ...   # before this host sends the agent away
def on_arrival(self, event): ...       # after another host receives it
def on_reverting(self, event): ...     # before a remote host retracts it back
def on_cloning(self, event): ...       # original before clone state is captured
def on_clone(self, event): ...         # clone after arriving/being created
def on_cloned(self, event): ...        # original after clone succeeds
def on_deactivating(self, event): ...  # before in-memory deactivation
def on_activation(self, event): ...    # after activation
def on_disposing(self, event): ...     # before disposal
def run(self): ...                     # after create, arrival, clone, activation
def handle_message(self, message): ... # message event handler
```

## Proxy operations

`Host.create` and host lookups return a `PagletProxy`. The proxy is the only
public control handle, mirroring Aglets' proxy idea:

```python
proxy.send(Message("kind", {"x": 1}))       # synchronous reply
proxy.send_oneway(Message("kind", {...}))   # no reply
future = proxy.send_future(Message("kind", {...}))
future.get_reply(timeout=2)
remote_proxy = proxy.dispatch(beta.address)
clone_proxy = proxy.clone(target=beta.address)
proxy_ref = proxy.ref()
proxy.deactivate()
proxy.activate()
proxy.dispose()
proxy.info()
proxy.is_active()
```

Pull an agent back from another host:

```python
returned = alpha.retract(beta.address, agent_id)
```

## Durable deactivation

Deactivation persists a paglet's transfer envelope to disk and removes the live
object from memory. Activation reconstructs the paglet, calls `on_activation`,
and then invokes `run()`:

```python
import time
from paglets import DeactivationPolicy

proxy.deactivate()
proxy.activate()

proxy.deactivate(policy=DeactivationPolicy(activate_at=time.time() + 3600))
```

By default, a message sent to an inactive paglet activates it and then delivers
the message. Paglets can choose a stricter policy by overriding
`deactivation_policy`:

```python
def deactivation_policy(self, request):
    return DeactivationPolicy(
        activate_on_message=False,
        queue_messages_when_inactive=True,
    )
```

When activation on message is disabled, normal messages are queued and return a
queued acknowledgement. Use `no_delay=True` to fail fast instead:

```python
proxy.send(Message("status"), no_delay=True)
```

CLI hosts persist inactive paglets under
`~/.paglets/hosts/{host-name}/inactive` by default. On graceful CLI shutdown,
active paglets are deactivated with a startup-activation policy so they resume
when the host starts again. Lazy managed resident services are the exception:
they remain discoverable but stay inactive until first use.

## Message patterns

`Message` supports both named arguments and Aglets-style single arguments:

```python
Message("echo", {"value": "named"})
Message("echo", arg="single")
```

For future replies and multicast:

```python
reply_set = alpha.multicast_message("status")
for future in reply_set:
    print(future.get_reply())
```

Normal delivery goes through a per-paglet mailbox. Higher-priority queued
messages are selected before lower-priority queued messages, FIFO is preserved
within one priority, and `UNQUEUED_PRIORITY` is the explicit immediate bypass.
Paglets can coordinate mailbox handlers with:

```python
self.wait_message(timeout=1.0)
self.notify_message()
self.notify_all_messages()
```

Handlers can run concurrently. Protect shared dataclass state with short locked
sections:

```python
with self.locked_state() as state:
    state.completed += 1
    state.results.append(result)
```

Use `with self.locked():` for other agent-local critical sections, or
`@state_locked` for small helper methods. `Paglet.MAILBOX_WORKERS` defaults to
`4`; set `MAILBOX_WORKERS = 1` on simple agents that want queued
`handle_message` calls serialized. Background threads and unqueued messages
still need explicit state locking when they share mutable state.

## Services, Tickets, Events, And Resources

Paglets can advertise local or mesh-visible services through typed contracts.
The packaged example `server-info` service is one example. It is declared as a
lazy resident service by launch config, advertises a typed contract before the
provider is active, and starts on first use:

```python
from paglets import ServiceScope
from paglets.examples.system_info import GET_DISK, SERVER_INFO, DiskRequest

service = self.require_contract(SERVER_INFO, operation=GET_DISK, scope=ServiceScope.MESH)
reply = service.call(GET_DISK, DiskRequest(paths=["/"], all_volumes=False))
```

Use a lease when a lazy resident service should stay active across several
calls:

```python
with self.lease_contract(SERVER_INFO, operation=GET_DISK, scope=ServiceScope.MESH) as service:
    first = service.call(GET_DISK, DiskRequest(paths=["/"]))
    second = service.call(GET_DISK, DiskRequest(paths=["/data"]))
```

Use `TransferTicket` when dispatch or clone needs target checks or inactive
arrival:

```python
from paglets import ArrivalMode, TransferTicket

ticket = TransferTicket(
    beta.address,
    required_capabilities=("agents:create",),
    expected_code_version=alpha.mesh.code_version,
    arrival_mode=ArrivalMode.INACTIVE,
)
proxy.dispatch(ticket)
```

Python APIs use enum values such as `ServiceScope.MESH` and
`ArrivalMode.INACTIVE` for closed runtime domains. Config and wire formats still
store plain strings, for example TOML `scope = "mesh"`, and the runtime converts
them at the boundary.

Hosts keep an in-memory context event log and accept listeners:

```python
host.add_listener(lambda event: print(event.kind, event.agent_id))
events = host.list_events(since=0, limit=100)
```

Runtime-only resources can be registered for lifecycle cleanup before dispatch,
deactivate, retract, or dispose:

```python
self.resources.track_closeable("socket", sock)
self.resources.register("temporary-file", lambda: path.unlink(), suppress=True)
```

## Itinerary helpers

The `paglets.itinerary` module converts the portable parts of the Aglets
itinerary utilities:

- `ItineraryPlan` stores destinations, current route position, visited
  destinations, immutability, and optional circular looping.
- `TaskItineraryPlan` adds default and destination-specific tasks for arrival,
  dispatch, and reverting phases.
- `ItineraryAgentMixin` wires itinerary state to an agent's `dispatch`.

Tasks are serializable descriptors (`ItineraryTask`) rather than function
objects. The agent implements `execute_itinerary_task`, keeping executable code
in importable Python classes instead of moving code objects between hosts.

## State serialization rules

`paglets` serializes dataclass fields to JSON-compatible values. Supported state
values include:

- nested dataclasses;
- `str`, `int`, `float`, `bool`, `None`;
- `list`, `tuple`, `set`, `dict`;
- `Enum` values;
- `pathlib.Path` values.

Everything stored directly on the `Paglet` object is intentionally transient and
will be rebuilt by `__init__`, `on_arrival`, `on_clone`, or `run` on the target
host. This is the modern Python equivalent of explicit mobile object state: no
call-stack transfer, no socket/thread/GPU-context migration.

## Same-machine multi-host development

For development you can run multiple hosts on one Mac:

```python
alpha = Host("alpha", host="127.0.0.1", port=9001)
beta = Host("beta", host="127.0.0.1", port=9002)
```

They are separate paglet contexts. Dispatching from `alpha` to `beta` uses the
same HTTP envelope as dispatching to another machine.

## Project layout

```text
src/paglets/
  agent.py      Paglet, PagletState, PagletContext, lifecycle hooks
  host.py       Host runtime + JSON HTTP server
  proxy.py      PagletProxy control handle
  messages.py   Message model
  events.py     Lifecycle event dataclasses
  envelope.py   Mobile-state transfer envelope
  runtime_values.py Closed runtime enums for scopes, lifecycles, and transfer modes
  persistency.py Durable deactivation policy and inactive records
  itinerary.py  Serializable itinerary/task helpers
  admin.py      Multi-server admin client/config helpers
  tui.py        Optional Textual TUI admin console
  serde.py      Dataclass state serializer/restorer
  cli.py        paglets-host CLI
examples/
  start_hello_demo.py
  mobility_events_demo.py
  message_patterns_demo.py
  itinerary_demo.py
  finder_demo.py
  clone_workers_demo.py
  simple_master_slave_demo.py
tests/
  regression and behavior tests
```

## Current intentional limitations

- No authentication/authorization.
- No code upload: classes must already be importable on every host.
- No call-stack migration; movement resumes through lifecycle events and `run`.
- Message arguments and replies are expected to be JSON-compatible.

Those constraints keep the first stub robust and understandable while preserving
the central Aglets idea: mobile objects with explicit state, events, and proxies.

## License

This project is licensed under the MIT License.

Copyright (c) 2026 by C. Klukas.
