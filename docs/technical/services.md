# Services Package

`paglets.services` defines service contracts, service registry records, service
handles, and resident service metadata.

## Responsibilities

- Describe service operations with request/reply payload dataclasses.
- Validate service requests and replies against a contract.
- Encode service records for discovery and lookup.
- Manage resident service leases and lifecycle metadata.

## Main Modules

- **`paglets.services.contracts`**: Defines `ServiceContract`, `ServiceOperation`, `ServiceHandle`,
  `ServiceRecord`, `ServiceRegistry`, and service-specific errors.

- **`paglets.services.resident`**: Defines `ResidentServiceSpec`, `ServiceLease`, resident lifecycle defaults,
  and metadata keys used by host-managed services.

## Implementation Notes

Service operations are message-backed. A service handle builds typed messages
from a contract and sends them to the paglet that owns the service.

`ServiceScope` controls whether a service is local to one host or advertised
through the mesh. Resident services can be started lazily or eagerly from the
launch configuration.

## API Reference

<!-- api: paglets.services.contracts -->
### `paglets.services.contracts`

#### `EmptyPayload`

```python
class EmptyPayload
```

Dataclass payload for operations with no request or reply body.

#### `ServiceOperation`

```python
class ServiceOperation(Generic)
```

Typed operation exposed by a service contract.

| Field | Type | Default |
|---|---|---|
| `name` | `str` | required |
| `request_type` | `type[ReqT]` | `<class 'paglets.services.contracts.EmptyPayload'>` |
| `reply_type` | `type[RepT]` | `<class 'paglets.services.contracts.EmptyPayload'>` |

#### `ServiceContract`

```python
class ServiceContract
```

Typed service interface advertised through the service registry.

| Field | Type | Default |
|---|---|---|
| `name` | `str` | required |
| `operations` | `tuple[ServiceOperation[Any, Any], ...]` | required |
| `version` | `str` | `'1'` |

#### `ServiceHandle`

```python
class ServiceHandle
```

Resolved typed service client for one advertised service record.

| Field | Type | Default |
|---|---|---|
| `contract` | `ServiceContract` | required |
| `record` | `ServiceRecord` | required |
| `context_or_client` | `Any` | `None` |
<!-- /api -->
<!-- api: paglets.services.resident -->
### `paglets.services.resident`

#### `ResidentServiceSpec`

```python
class ResidentServiceSpec
```

Class-level declaration for a managed resident service.

| Field | Type | Default |
|---|---|---|
| `contract` | `ServiceContract` | required |
| `scope` | `ServiceScope` | `ServiceScope.LOCAL` |
| `lifecycle` | `ResidentLifecycle` | `ResidentLifecycle.LAZY` |
| `agent_id` | `str | None` | `None` |
| `singleton` | `bool` | `True` |
| `idle_timeout` | `float` | `30.0` |
| `state` | `dict[str, Any]` | `dict()` |

#### `ServiceLease`

```python
class ServiceLease
```

TTL-backed lease that keeps a managed resident service active.

| Field | Type | Default |
|---|---|---|
| `handle` | `ServiceHandle` | required |
| `lease_id` | `str` | required |
| `host_url` | `str` | required |
| `expires_at` | `float` | required |
| `client` | `HostClient` | `HostClient()` |
<!-- /api -->
## Related Pages

- [Core](core.md) covers messages and service scope enums.
- [Configuration](configuration.md) covers resident services in launch config.
- [Remote](remote.md) covers mesh-visible service lookup.

