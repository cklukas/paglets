# Persistence Package

`paglets.persistence` contains durable inactive paglet records and per-paglet
managed storage.

## Responsibilities

- Represent deactivation policy and deactivation requests.
- Store inactive paglet envelopes, queued messages, and restore metadata.
- Provide managed storage paths scoped to a host and paglet.
- Enforce persistent storage quotas and expose storage status.

## Main Modules

- **`paglets.persistence.persistency`**: Defines `DeactivationPolicy`, `DeactivationRequest`, queued inactive
  messages, and inactive record dataclasses used by the host.

- **`paglets.persistence.storage`**: Defines `ManagedStorage`, `StorageStatus`, quota errors, and the default
  storage quota constant.

## Implementation Notes

Inactive records are host-owned. A deactivated paglet is not running, but the
host can activate it before delivery unless the caller requests a fast failure.

Managed storage is separate from mobile dataclass state. It is useful for local
artifacts that should stay on a host, while mobile workflow state belongs in the
paglet state dataclass.

Storage sizes use binary-scaled units in user-facing output: `KB`, `MB`, and
`GB` scale by 1024.

## API Reference

<!-- api: paglets.persistence.persistency -->
### `paglets.persistence.persistency`

#### `DeactivationPolicy`

```python
class DeactivationPolicy
```

Policy chosen by a paglet for its inactive lifecycle.

| Field | Type | Default |
|---|---|---|
| `activate_on_message` | `bool` | `True` |
| `queue_messages_when_inactive` | `bool` | `True` |
| `activate_on_startup` | `bool` | `False` |
| `activate_at` | `float | None` | `None` |

#### `DeactivationRequest`

```python
class DeactivationRequest
```

Context for a deactivation request before the paglet chooses policy.

| Field | Type | Default |
|---|---|---|
| `reason` | `str` | `'deactivate'` |
| `source` | `str` | `'external'` |
| `policy` | `DeactivationPolicy | None` | `None` |
| `metadata` | `dict[str, Any]` | `dict()` |

#### `QueuedMessage`

```python
class QueuedMessage
```

Message persisted while a paglet is inactive.

| Field | Type | Default |
|---|---|---|
| `message` | `Message` | required |
| `oneway` | `bool` | `False` |
| `queued_at` | `float` | `time()` |

#### `InactiveRecord`

```python
class InactiveRecord
```

Durable representation of a deactivated paglet.

| Field | Type | Default |
|---|---|---|
| `envelope` | `PagletEnvelope` | required |
| `policy` | `DeactivationPolicy` | required |
| `request` | `DeactivationRequest` | required |
| `deactivated_at` | `float` | `time()` |
| `queued_messages` | `list[QueuedMessage]` | `list()` |
<!-- /api -->
<!-- api: paglets.persistence.storage -->
### `paglets.persistence.storage`

#### `StorageQuotaError`

```python
class StorageQuotaError(PagletError)
```

Raised when a managed storage write would exceed its quota.

#### `ManagedStorage`

```python
class ManagedStorage
```

Path-safe, quota-accounted storage rooted at one directory.
<!-- /api -->
## Related Pages

- [Runtime](runtime.md) covers activation/deactivation orchestration.
- [Core](core.md) covers paglet state and lifecycle hooks.

