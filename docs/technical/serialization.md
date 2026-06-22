# Serialization Package

`paglets.serialization` provides dataclass wire conversion and qualified-name
resolution.

## Responsibilities

- Convert dataclass instances to JSON-compatible wire dictionaries.
- Reconstruct dataclass instances from wire dictionaries.
- Resolve importable classes and objects by qualified name.
- Preserve binary values through JSON-safe tagged values where needed.

## Main Modules

`paglets.serialization.serde`
: Implements `qualified_name`, `resolve_qualified_name`,
  `dataclass_to_wire`, and `dataclass_from_wire`.

## Implementation Notes

The runtime requires importable class names for paglet classes, state classes,
service payload classes, and discovered agent classes. This is why paglet
classes cannot be defined in transient modules such as `__main__`.

Host-to-host movement uses pickle transport for full state payloads, but JSON
inspection and service messages use dataclass wire conversion.

## API Reference

::: paglets.serialization.serde

## Related Pages

- [Core](core.md) covers state requirements.
- [Remote](remote.md) covers transport payload paths.

