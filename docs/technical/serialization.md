# Serialization Package

`paglets.serialization` provides dataclass wire conversion and qualified-name
resolution.

## Responsibilities

- Convert dataclass instances to JSON-compatible wire dictionaries.
- Reconstruct dataclass instances from wire dictionaries.
- Resolve importable classes and objects by qualified name.
- Preserve binary values through JSON-safe tagged values where needed.

## Main Modules

- **`paglets.serialization.codec`**: Implements `qualified_name`, `resolve_qualified_name`,
  `dataclass_to_wire`, and `dataclass_from_wire`.

## Implementation Notes

The runtime requires importable class names for paglet classes, state classes,
service payload classes, and discovered agent classes. This is why paglet
classes cannot be defined in transient modules such as `__main__`.

Host-to-host movement uses pickle transport for full state payloads, but JSON
inspection and service messages use dataclass wire conversion.

## API Reference

<!-- api: paglets.serialization.codec -->
### `paglets.serialization.codec`

#### `qualified_name`

```python
def qualified_name(obj: type | object) -> str
```

Return an importable ``module:qualname`` for a class or object.

#### `resolve_qualified_name`

```python
def resolve_qualified_name(name: str) -> Any
```

Resolve a ``module:qualname`` produced by :func:`qualified_name`.

#### `dataclass_to_wire`

```python
def dataclass_to_wire(instance: Any) -> WirePayload
```

Serialize a dataclass instance to explicit movement/control values.

This is intentionally one approach: paglet state is explicit dataclass state.
Runtime fields on the paglet object itself are transient and never move.

#### `dataclass_from_wire`

```python
def dataclass_from_wire(cls: type, payload: WirePayload) -> Any
```

Restore a dataclass instance from a wire dict.
<!-- /api -->
## Related Pages

- [Core](core.md) covers state requirements.
- [Remote](remote.md) covers transport payload paths.

