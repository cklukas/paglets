# Patterns Package

`paglets.patterns` contains small ergonomic layers on top of the raw paglet
runtime. These helpers are additive: the core `Message`, lifecycle, dispatch,
clone, and registered-file mobility APIs remain available for paglets that need
custom protocols.

## Responsibilities

- Provide typed task status, request, result, and client helpers.
- Provide typed operation routing for paglets that expose several public
  operations without writing a manual `handle_message()` switch.
- Provide small coordinator helpers for clone fan-out, child cleanup,
  explicit child-result messages, and timeout expiry.
- Wrap mesh user-info notifications so notification failures stay non-fatal.
- Provide reusable file mobility helpers and an optional one-file transfer task
  built on natural registered-file mobility.

## API Reference

::: paglets.patterns.tasks

::: paglets.patterns.operations

::: paglets.patterns.coordination

::: paglets.patterns.notifications

::: paglets.patterns.file_mobility

## Related Pages

- [Implementing Paglets](../implementing-paglets.md) introduces the pattern
  selection guide and the lower-level raw message API.
- [File Grabber](../examples/file-grabber.md) shows a file transfer paglet that
  keeps the workflow visible while reusing `FileMobilityMixin`.
- [Performance Benchmark](../examples/performance.md) and
  [Mesh Search](../examples/search.md) use typed operations with fan-out and
  explicit result/update messages while keeping benchmark and search work in
  the examples.
