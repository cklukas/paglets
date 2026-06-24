# Artifacts Package

`paglets.artifacts` contains the value types and host-owned store used for
binary artifact transport and registered paglet file mobility.

## Responsibilities

- Represent hosted artifact blobs with `ArtifactRef`.
- Represent paglet-owned registered files with `PagletFileRef`.
- Stream artifact data through temporary `.part` files and checksum validation.
- Remove failed temporary receives immediately and clean stale temporary files
  during host sweeps.
- Keep low-level artifact storage separate from per-paglet scratch/work
  directories.

## API Reference

::: paglets.artifacts

## Related Pages

- [Artifact Transport](../system/artifacts.md) covers user-facing file mobility
  and low-level artifact workflows.
- [Remote](remote.md) covers client and proxy transfer helpers.
