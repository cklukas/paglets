# Configuration

On first interactive host start, `paglets-host` copies the bundled demo launch config to `~/.paglets/launch.toml`. The config declares resident services and optional startup agents.

The bundled defaults currently declare lazy `server-info` and eager `mesh-info` services for demo and inspection workflows. Interactive starts can sync newer bundled defaults; non-interactive starts keep existing config and print a warning.

Useful flags:

```bash
uv run paglets-host --yes
uv run paglets-host --no-sync-launch-config
uv run paglets-host --launch-config /path/to/launch.toml
```

See [Technical Configuration](../technical/configuration.md) for parser details and API reference.
