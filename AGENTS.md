# Agent Instructions

## License Headers

New code files must include the project copyright and license notice.

For Python files, use this exact header at the top of the file:

```python
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
```

For other source-code file types, use the equivalent comment syntax for that
language while preserving the same text.

Do not add project license headers to generated files, vendored dependencies,
virtual environments, caches, build outputs, or other ignored artifacts.

## Documentation

When adding or changing user-visible features, update the MkDocs documentation
under `docs/`.

Also update root-level `README.md` when the change affects installation,
quick-start usage, command-line entry points, major examples, public APIs, or
project-level behavior.

Keep documentation examples current with the code and prefer commands that can
be run from the repository root.

## Units

Use classic binary-scaled byte units for storage, payload sizes, and byte
throughput: label them as `KB`, `MB`, `GB`, etc., and scale by 1024. Do not use
IEC labels such as `KiB`, `MiB`, or `GiB`.

For network-style bit throughput, use decimal-scaled units such as `kbit/s`,
`Mbit/s`, and `Gbit/s`, scaled by 1000.
