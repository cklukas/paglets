# Tooling Package

`paglets.tooling` contains command-line and repository-oriented helpers.

## Responsibilities

- Provide the `paglets-host` command-line entry point.
- Discover importable paglet classes in configured paths.
- Keep trusted host meshes aligned with git auto-update.
- Provide repository quality gates through ruff, pyright, pytest, MkDocs, and
  CLI smoke checks.

## Main Modules

`paglets.tooling.cli`
: Parses host CLI flags, syncs launch config, optionally performs git
  auto-update, constructs `Host`, and starts the runtime.

`paglets.tooling.discovery`
: Discovers importable `Paglet` subclasses from configured source paths for
  admin and startup workflows.

`paglets.tooling.git_update`
: Wraps git fetch/pull/status operations, update locking, dependency sync, and
  restart decisions.

## Implementation Notes

The console script points at `paglets.tooling.cli:main`. Runtime restarts use
`python -m paglets.tooling.cli` so the process does not depend on a locked
console-script wrapper on Windows.

Git auto-update only runs for trusted direct hosts. Relay/connect mode disables
auto-update because connect-mode hosts do not accept inbound update requests.

The repository CI runs the same commands expected locally:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
uv run --extra docs mkdocs build --strict
```

## API Reference

::: paglets.tooling.cli

::: paglets.tooling.discovery

::: paglets.tooling.git_update

## Related Pages

- [Configuration](configuration.md) covers launch config loaded by the CLI.
- [Remote](remote.md) covers admin clients and mesh discovery.
