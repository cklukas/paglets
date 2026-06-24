# Configuration Package

`paglets.config` owns startup and launch configuration.

## Responsibilities

- Parse `~/.paglets/launch.toml`.
- Sync the bundled launch configuration on first start or when requested.
- Resolve startup agent classes, initial state, singleton settings, and IDs.
- Resolve resident service declarations and lifecycle settings.

## Main Modules

`paglets.config.startup`
: Defines launch-config dataclasses, bundled config loading, config sync, and
  startup/resident-service resolution helpers.

`paglets.config.defaults`
: Contains package data for the bundled `launch.toml` configuration.

## Implementation Notes

Startup config references classes by importable qualified name or by class-level
startup metadata. The resolver materializes initial state through the
serialization layer.

Sync behavior is controlled by `LaunchConfigSyncAction` and the CLI flags for
interactive confirmation, forced sync, or disabling launch-config sync.

## API Reference

::: paglets.config.startup

## Related Pages

- [Services](services.md) covers resident service contracts and leases.
- [Tooling](tooling.md) covers the host CLI that loads launch configuration.
