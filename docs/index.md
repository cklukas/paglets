# paglets

`paglets` is a compact Python runtime inspired by Java Aglets. A paglet is a
mobile object with explicit dataclass state, lifecycle hooks, message handling,
and proxy-based control.

Resources: [application note PDF](https://github.com/cklukas/paglets/releases/download/paper-2026-06-23/paglets-application-note.pdf)
and [PyPI package](https://pypi.org/project/paglets/).

The runtime intentionally uses a Python-friendly mobility model:

- all hosts already have the same code importable;
- every active paglet instance runs in its own child Python process;
- only dataclass state moves between hosts;
- control calls use a JSON HTTP API, while paglet movement uses a binary HTTP
  state payload to avoid JSON encoding large mobile state;
- large state is streamed for host-to-host movement and shared-memory local
  host/child handoff, while JSON remains the small control-plane format;
- same-host movement bypasses HTTP/TCP and delivers the serialized envelope
  directly inside the local host; different host processes on the same machine
  still communicate over loopback HTTP;
- lifecycle hooks resume behavior after create, dispatch, clone, retract, or
  activation;
- deactivation persists inactive paglets to disk until activation;
- agents communicate through `PagletProxy`, `Message`, and per-paglet serial
  child-process mailboxes;
- service discovery, transfer tickets, proxy references, context events, and
  resource cleanup are first-class framework features.

The process-per-paglet model isolates crashes and CPU-heavy work from the host
and from other paglets, and it gives worker paglets real multi-core parallelism.
The tradeoff is stricter importability, process startup overhead, and
actor-style serial message handling inside each individual paglet.

## Quick Start

Install and test the project in development:

```bash
uv run pytest -q
```

Run two hosts:

```bash
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

For hosts on different machines, start each host with `--bind-public`:

```bash
uv run paglets-host --name mac --bind-public --port 8765 --mesh-version dev
uv run paglets-host --name windows --bind-public [IP] --port 8765 --mesh-version dev
```

Without an `IP`, `--bind-public` binds only the detected LAN address. With an
`IP`, it binds only that supplied address. Repeat the flag to bind multiple
specific addresses; the first one is published to the mesh. The auto form
keeps watching the detected address and rebinds/publishes a new one after DHCP
or network reconnect changes it.

For locked-down networks, one public HTTPS endpoint can act as a relay. Bind
the paglets backend on server A to localhost, publish it through an existing
reverse-proxy path such as `/paglets`, require an API key, and let other hosts
connect outbound:

Example Nginx drop-in:

```nginx
# Example: /etc/nginx/default.d/paglets.conf
# Include this from an existing HTTPS server block.
location /paglets/ {
    proxy_pass http://127.0.0.1:8765/;
    proxy_http_version 1.1;
    proxy_set_header Authorization $http_authorization;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
    client_max_body_size 256M;
}
```

`/etc/nginx/default.d/paglets.conf` is common on RHEL-style layouts when the
site includes `default.d/*.conf`. On Debian/Ubuntu-style layouts, place the
same `location` in `/etc/nginx/sites-available/<site>` or include it as a
snippet from that server block. Test with `sudo nginx -t` before reloading.

```bash
export PAGLETS_API_KEY='change-me'
uv run paglets-host --name A --host 127.0.0.1 --port 8765 \
  --public-url https://server-a.example.com/paglets \
  --api-key-env PAGLETS_API_KEY
uv run paglets-host --name B --connect-to https://server-a.example.com/paglets \
  --api-key-env PAGLETS_API_KEY
```

Connect-mode hosts do not open inbound ports. Movement and messages are relayed
through A over authenticated HTTP long-polling, and git auto-update is disabled
in this mode.

Relay forwarding is transparent to paglets: lifecycle events name the final
target, arrivals run only there, and the source paglet is kept active when a
relay delivery fails. Hubs expose `GET /paglets/relay/diagnostics` and support
`--relay-offline-after`, `--relay-delivery-timeout`, and `--relay-queue-limit`
for corporate network tuning.

On first start, `paglets-host` copies `~/.paglets/launch.toml` from the bundled
demo config. The default launch config declares lazy `server-info` and eager
`mesh-info`, so hosts continuously exchange resource snapshots while still
using `server-info` as the local system information provider:

```bash
uv run paglets-sysinfo df
uv run paglets-sysinfo load
uv run paglets-mesh-info summary
uv run paglets-pi-compute --digits 16
uv run paglets-perf-test
uv run paglets-mesh-benchmark --payload-size 64K
```

`paglets-perf-test` is a pure mobile-agent example: the entry host creates a
parent benchmark paglet, clones workers to online same-version mesh hosts, runs
local CPU, memory, and bounded temporary disk I/O checks, and reports the
summary centrally.

`paglets-mesh-benchmark` measures mobile-agent movement itself. A starter
paglet remains on the entry host while a traveler visits every directed host
pair, stores per-hop timings locally on arrival, then collects and prints a
directional Markdown matrix plus clock-offset and message round-trip
diagnostics, ending with the overall benchmark time.

Run the disk survey demo:

```bash
uv run python demos/disk_survey_demo.py --hosts alpha beta gamma
```

## Documentation Map

- [Implementing Paglets](implementing-paglets.md): how to write paglet classes,
  state objects, lifecycle hooks, message handlers, movement, and mesh-aware
  behavior.
- [Example Agents](examples/index.md): detailed explanations of packaged example
  agents, including `server-info`, `mesh-info`, Pi compute,
  `paglets-perf-test`, and `paglets-mesh-benchmark`.
- [Git Auto-Update](git-auto-update.md): how trusted host meshes can pull,
  synchronize dependencies, broadcast commit hashes, and restart from updated
  code.
- [Usage Ideas](usage-ideas.md): practical scenarios where mobile state and
  agent-to-agent communication are a useful fit.
- [Technical Overview](technical/overview.md): how the topic packages fit
  together and where implementation details live.
- [Core](technical/core.md), [Runtime](technical/runtime.md), and
  [Remote](technical/remote.md): package-level implementation notes and
  generated API references for the main runtime subsystems.
- [Glossary](glossary.md): terminology used by the project.

## Build The Docs Locally

```bash
uv run --extra docs mkdocs serve
```

For a production build:

```bash
uv run --extra docs mkdocs build --strict
```
