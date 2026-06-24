# System Services

`paglets.system` contains built-in resident services that are useful across many
applications. They are normal paglets with typed service contracts, but they are
shipped as reusable infrastructure rather than as examples.

The bundled launch config starts:

- [Server Info](server-info.md), a host-local system information service.
- [Mesh Info](mesh-info.md), a mesh-wide resource landscape service.
- [Compute Slots](compute-slots.md), a reusable scheduler for coarse compute
  jobs.
- [User Info](user-info.md), a user-facing notification sink.

Example paglets under `paglets.examples.*` may depend on these services. The
[Analysis Jobs example](../examples/analysis-jobs.md) demonstrates how a custom
compute paglet can use `compute-slots` and `user-info`.

