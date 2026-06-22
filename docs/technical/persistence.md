# Persistence Package

`paglets.persistence` contains durable inactive paglet records and per-paglet
managed storage.

## Responsibilities

- Represent deactivation policy and deactivation requests.
- Store inactive paglet envelopes, queued messages, and restore metadata.
- Provide managed storage paths scoped to a host and paglet.
- Enforce persistent storage quotas and expose storage status.

## Main Modules

`paglets.persistence.persistency`
: Defines `DeactivationPolicy`, `DeactivationRequest`, queued inactive
  messages, and inactive record dataclasses used by the host.

`paglets.persistence.storage`
: Defines `ManagedStorage`, `StorageStatus`, quota errors, and the default
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

::: paglets.persistence.persistency

::: paglets.persistence.storage

## Related Pages

- [Runtime](runtime.md) covers activation/deactivation orchestration.
- [Core](core.md) covers paglet state and lifecycle hooks.

