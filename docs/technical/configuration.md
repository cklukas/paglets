# Configuration Package

`paglets.config` owns startup and launch configuration.

## Responsibilities

- Parse `~/.paglets/launch.toml`.
- Sync the bundled launch configuration on first start or when requested.
- Resolve startup agent classes, initial state, singleton settings, and IDs.
- Resolve resident service declarations and lifecycle settings.

## Main Modules

- **`paglets.config.startup`**: Defines launch-config dataclasses, bundled config loading, config sync, and
  startup/resident-service resolution helpers.

- **`paglets.config.defaults`**: Contains package data for the bundled `launch.toml` configuration.

## Implementation Notes

Startup config references classes by importable qualified name or by class-level
startup metadata. The resolver materializes initial state through the
serialization layer.

Sync behavior is controlled by `LaunchConfigSyncAction` and the CLI flags for
interactive confirmation, forced sync, or disabling launch-config sync.

## API Reference

<!-- api: paglets.config.startup -->
### `paglets.config.startup`

#### `AutoStartSpec`

```python
class AutoStartSpec
```

Class-level marker for agents that can be started from launch config.

| Field | Type | Default |
|---|---|---|
| `alias` | `str` | required |
| `agent_id` | `str | None` | `None` |
| `singleton` | `bool` | `True` |
| `state` | `dict[str, Any]` | `dict()` |

#### `StartupAgentConfig`

```python
class StartupAgentConfig
```

One launch-config entry describing an agent to start.

| Field | Type | Default |
|---|---|---|
| `use` | `str | None` | `None` |
| `class_name` | `str | None` | `None` |
| `enabled` | `bool` | `True` |
| `agent_id` | `str | None` | `None` |
| `singleton` | `bool` | `True` |
| `state` | `dict[str, Any]` | `dict()` |
| `init` | `Any` | `None` |

#### `ResidentServiceConfig`

```python
class ResidentServiceConfig
```

One launch-config entry describing a managed resident service.

| Field | Type | Default |
|---|---|---|
| `use` | `str | None` | `None` |
| `class_name` | `str | None` | `None` |
| `service_name` | `str | None` | `None` |
| `enabled` | `bool` | `True` |
| `agent_id` | `str | None` | `None` |
| `singleton` | `bool` | `True` |
| `lifecycle` | `ResidentLifecycle | None` | `None` |
| `scope` | `ServiceScope | None` | `None` |
| `idle_timeout` | `float | None` | `None` |
| `state` | `dict[str, Any]` | `dict()` |
| `init` | `Any` | `None` |

#### `LaunchConfig`

```python
class LaunchConfig
```

Parsed paglets launch config.

| Field | Type | Default |
|---|---|---|
| `path` | `Path | None` | `None` |
| `demo_config_id` | `str | None` | `None` |
| `demo_config_version` | `str | None` | `None` |
| `sync_demo_config` | `bool` | `True` |
| `startup_agents` | `tuple[StartupAgentConfig, ...]` | `()` |
| `resident_services` | `tuple[ResidentServiceConfig, ...]` | `()` |

#### `LaunchConfigSyncResult`

```python
class LaunchConfigSyncResult
```

Result of syncing the bundled demo launch config to the user path.

| Field | Type | Default |
|---|---|---|
| `action` | `LaunchConfigSyncAction` | required |
| `path` | `Path` | required |
| `message` | `str` | required |
| `backup_path` | `Path | None` | `None` |
<!-- /api -->
## Related Pages

- [Services](services.md) covers resident service contracts and leases.
- [Tooling](tooling.md) covers the host CLI that loads launch configuration.
