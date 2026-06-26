# Changes

## Unreleased

### Fixed

- Fixed compute-slot lease cleanup so inactive local jobs do not keep stale
  CPU, memory, and temp-storage reservations after host restarts.
- Prevented duplicate compute-slot leases and queued requests for the same
  compute job when a waiting job re-requests a slot.
- Recorded one final compute-slot usage sample when a lease is released so
  short jobs can populate `jobs history` max CPU, RSS, and disk columns even
  when they finish before the periodic sampler runs.

## 1.1.0 - 2026-06-26

### Added

- Added artifact transport APIs and typed paglet patterns for sending and
  tracking larger file-shaped payloads.
- Added compute-slot lease expiration handling, active lease retention, and job
  restart policy support for host restarts.
- Added bulk agent state retrieval to speed up compute job listing.
- Added `paglets-compute-slots status --blocked` to explain which resource is
  preventing queued jobs from starting.
- Added `paglets-compute-slots status --usage` with process RSS, process-tree
  RSS, Paglets work-dir usage, application-provided extra work usage, sampled
  maxima, and sample counts.
- Added `ComputeJobPaglet.compute_usage_paths()` so compute jobs can report
  application-specific scratch files or directories for scheduler diagnostics.
- Added `paglets-compute-slots jobs history` to show recent finished job
  runtime, finish reason, job class, max CPU, max RAM, and max disk usage.

### Changed

- Updated compute-slot CLI status tables to use Markdown-style table output and
  to show temp storage reservations explicitly.
- Updated compute job listing to use bulk state payloads and include active and
  inactive jobs by default.
- Improved Paglets API key handling across CLI tools and relayed deployments.
- Improved child endpoint and runtime error handling around malformed control
  calls.

### Fixed

- Fixed inactive local paglet proxy lookup so scheduler grant messages can wake
  inactive queued compute jobs with `activate_if_inactive=True`.
- Repaired stale inactive compute job policies so older `WAITING_FOR_SLOT`
  records are startup-recoverable.
- Started resident services before startup activation so restarted compute jobs
  see required services such as `compute-slots`.
- Fixed compute-slot diagnostics for stale leases, missing active agents, and
  authentication failures with missing API keys.

## 1.0.0 - 2026-06-24

### Breaking Changes

- Moved built-in resident services from example packages into
  `paglets.system.*`; imports from `paglets.examples.system_info` and
  `paglets.examples.mesh_info` are no longer supported.
- Updated bundled launch configuration to start built-in `server-info`,
  `mesh-info`, `compute-slots`, and `user-info` services from the
  `paglets.system` namespace.
- Updated the `paglets-sysinfo` and `paglets-mesh-info` CLI entry points to use
  the new system service packages.

### Added

- Added the built-in `compute-slots` scheduler service for coarse compute jobs
  with candidate host preflight, local queueing, bounded spillover, startup
  throttling, explicit CPU-core/RAM/temp-storage estimates, and scheduler status
  inspection.
- Added host-managed best-effort CPU affinity metadata and process reporting for
  compute-slot leases.
- Added the built-in `user-info` notification service for operator-facing
  messages from paglets.
- Added `ComputeJobPaglet` and `ComputeJobState` as a template-method base API
  so compute job authors implement application work and result handling without
  reimplementing scheduling protocol details.
- Added the `paglets-analysis-jobs` example, including synthetic
  pandas/scikit-learn analysis jobs, home-host result return, SQLite result
  appends, and cross-process DB write locking.
- Added `paglets-compute-slots` CLI commands for scheduler status and candidate
  inspection.

### Changed

- Added `pandas` and `scikit-learn` as normal package dependencies for the
  analysis jobs example.
- Updated documentation navigation to separate operations, system services,
  examples, technical reference, and project pages.
- Added expandable Mermaid diagram overlays and source-linked embedded code
  snippets in the MkDocs site.
- Updated README quick-start examples to include compute-slot scheduling and the
  analysis jobs demo.

### Fixed

- Fixed performance benchmark multi-core worker behavior.
- Updated release and documentation publishing workflow metadata.
- Refreshed citation, DOI, PyPI, social preview, and paper links in project
  documentation.

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
