# Host CLI

`paglets-host` starts a host process that supervises paglet child processes, exposes the HTTP API, joins the mesh, and optionally connects through a relay.

## Common Commands

```bash
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

For hosts on different machines, publish a reachable LAN address:

```bash
uv run paglets-host --name mac --bind-public --port 8765 --mesh-version dev
uv run paglets-host --name labbox --bind-public 192.168.86.42 --port 8765 --mesh-version dev
```

Set `PAGLETS_API_KEY` on shared or proxied networks so HTTP requests require
bearer authentication. Use `--api-key-env NAME` when the key lives in a
different environment variable.

Advertise host roles and compatibility hints with tags and properties:

```bash
uv run paglets-host --name linux-a --bind-public --mesh-version dev --tag linux
uv run paglets-host --name gpu-a --bind-public --mesh-version dev --tag linux --tag gpu --property python=3.12
```

Compute jobs can require, exclude, or prefer these tags through
`ComputeJobState` host-selection fields and the `paglets-compute-slots
candidates` CLI.

Configure artifact limits when hosts exchange result files or registered
paglet files:

```bash
uv run paglets-host --name linux-a --bind-public --mesh-version dev \
  --artifact-max-size 2G --artifact-storage-quota 20G --artifact-spool-ttl 86400
```

Artifact sizes use binary-scaled units such as `KB`, `MB`, and `GB`.

## Related Pages

- [Configuration](configuration.md) covers launch config and bundled defaults.
- [Deployment And Importability](deployment-importability.md) covers packaging app-owned paglets for all hosts.
- [Artifact Transport](../system/artifacts.md) covers registered-file mobility and low-level artifacts.
- [Runtime](../technical/runtime.md) covers the host internals.
- [Remote](../technical/remote.md) covers clients, proxies, relay transport, and admin calls.
