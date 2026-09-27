# Remote Package

`paglets.remote` contains the host-to-host and client-to-host communication
surface: HTTP clients, proxies, transfer tickets, transport helpers, mesh
membership, and admin tooling.

## Responsibilities

- Provide a reusable HTTP client for host control endpoints.
- Represent remote paglets through `PagletProxy` and serializable proxy refs.
- Validate and carry movement intent with `TransferTicket`.
- Stream binary state payloads for movement and shared-memory handoff helpers.
- Maintain mesh membership, multicast beacons, version compatibility, and relay
  routing state.
- Provide administration client records and dynamic entry-host discovery.

## Main Modules

- **`paglets.remote.client`**: Implements `HostClient`, request helpers, error decoding, and binary movement
  upload support, including streamed artifact upload/download helpers.

- **`paglets.remote.proxy` and `paglets.remote.references`**: Provide the controlled handle and serializable reference form used to inspect,
  message, move, deactivate, activate, and dispose paglets.

- **`paglets.remote.transfer`**: Defines `TransferTicket`, including required capabilities, expected code
  version, arrival mode, and target selection data.

- **`paglets.remote.transport`**: Implements chunked pickle HTTP payloads, local pickle streams, shared-memory
  readers/writers, and JSON-safe binary tagging.

- **`paglets.remote.mesh`**: Tracks known hosts, peer compatibility, multicast beacons, relayed hosts, and
  host name resolution.

- **`paglets.remote.admin`**: Defines admin records, server URL normalization, LAN/mesh entry discovery,
  and `PagletsAdminClient`.

## Implementation Notes

Control-plane operations stay JSON-oriented. Movement payloads are streamed
pickle data because paglet state can contain binary values and large nested
dataclass structures.

Large files should travel as artifacts rather than message bytes. Registered
paglet files use artifact upload internally before the movement envelope is
accepted on the target host, so the scratch copy is present before activation.

Mesh peers must agree on mesh version and compatible code version before they
are used as movement targets. Relay/connect mode avoids inbound ports on
clients by long-polling a hub host.

## API Reference

<!-- api: paglets.remote.client -->
### `paglets.remote.client`

#### `HostClient`

```python
class HostClient
```

Tiny JSON HTTP client used by proxies and hosts.
<!-- /api -->
<!-- api: paglets.remote.proxy -->
### `paglets.remote.proxy`

#### `PagletProxy`

```python
class PagletProxy
```

A controlled handle to a paglet, local or remote.

Like Aglets' proxy, callers do not reach into the object directly; all
control and messaging goes through the host API.

| Field | Type | Default |
|---|---|---|
| `host_url` | `str` | required |
| `agent_id` | `str` | required |
| `client` | `HostClient` | required |
<!-- /api -->
<!-- api: paglets.remote.references -->
### `paglets.remote.references`

#### `PagletProxyRef`

```python
class PagletProxyRef
```

Serializable reference to a paglet proxy.

| Field | Type | Default |
|---|---|---|
| `host_url` | `str` | required |
| `agent_id` | `str` | required |
<!-- /api -->
<!-- api: paglets.remote.transfer -->
### `paglets.remote.transfer`

#### `TransferTicket`

```python
class TransferTicket
```

Options and preflight requirements for dispatching or cloning a paglet.

| Field | Type | Default |
|---|---|---|
| `destination` | `str` | required |
| `timeout` | `float` | `10.0` |
| `retries` | `int` | `0` |
| `retry_interval` | `float` | `0.25` |
| `required_capabilities` | `tuple[str, ...]` | `()` |
| `expected_code_version` | `str | None` | `None` |
| `arrival_mode` | `ArrivalMode` | `ArrivalMode.ACTIVATE` |
<!-- /api -->
<!-- api: paglets.remote.transport -->
### `paglets.remote.transport`

#### `restore_json_safe`

```python
def restore_json_safe(value: Any) -> Any
```

Recursively restore values produced by :func:`json_safe`.
<!-- /api -->
<!-- api: paglets.remote.mesh -->
### `paglets.remote.mesh`

#### `MeshRegistry`

```python
class MeshRegistry
```

Version-gated host registry owned by a paglets host.
<!-- /api -->
<!-- api: paglets.remote.admin -->
### `paglets.remote.admin`

#### `detect_lan_host`

```python
def detect_lan_host() -> str
```

Return the local IPv4 address used for default-route traffic.
<!-- /api -->
## Related Pages

- [Runtime](runtime.md) covers how hosts receive remote requests.
- [Configuration](configuration.md) covers launch configuration for resident
  services and startup agents.
- [Tooling](tooling.md) covers CLI and git auto-update.
