# Changes

## 0.1.0 - 2026-06-23

Initial public package release.

### Added

- Python Aglets-inspired paglet runtime with explicit dataclass state movement,
  lifecycle hooks, message passing, proxies, cloning, dispatch, retraction,
  disposal, and durable deactivation.
- Process-isolated paglet execution with host supervision, child process IPC,
  mailbox delivery, managed resources, persistent storage, and inactive record
  handling.
- Remote host APIs for clients, proxies, transfer tickets, host mesh discovery,
  admin operations, authenticated relay mode, and large binary state transport.
- Resident service contracts, lazy and eager service startup, mesh-scoped
  services, leases, and bundled launch defaults.
- Packaged example agents and CLIs for system information, mesh information,
  Pi computation, local/mesh search, host performance benchmarking, and mesh
  movement benchmarking.
- Source-tree demo scripts under `demos/`.
- MkDocs documentation site with getting started, examples, operations,
  technical reference, glossary, and status pages.
- Python package metadata, console entry points, CI checks, documentation
  publishing, PyPI trusted publishing workflow, and package provenance through
  GitHub Actions.

### Changed

- Organized public imports into explicit topic namespaces under
  `paglets.core`, `paglets.runtime`, `paglets.remote`, `paglets.persistence`,
  `paglets.services`, `paglets.serialization`, `paglets.config`, and
  `paglets.tooling`.
- Split runtime, process, example, documentation, and test modules into focused
  topic files.
- Renamed the serialization implementation module to
  `paglets.serialization.codec`.

### Notes

- Flat root imports and old compatibility modules are intentionally unsupported.
- Version `0.1.0` is early-stage software for experiments and trusted local/LAN
  meshes.
