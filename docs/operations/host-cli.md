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

Use `--api-key-env NAME` on shared or proxied networks so HTTP requests require bearer authentication.

## Related Pages

- [Configuration](configuration.md) covers launch config and bundled defaults.
- [Runtime](../technical/runtime.md) covers the host internals.
- [Remote](../technical/remote.md) covers clients, proxies, relay transport, and admin calls.
