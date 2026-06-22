# Core Package

`paglets.core` contains the user-facing programming model for paglet classes
and the shared value types used across the runtime.

## Responsibilities

- Define `Paglet`, `PagletState`, `PagletContext`, state locking helpers, and
  lifecycle conveniences.
- Define `Message`, `FutureReply`, `ReplySet`, and message priority constants.
- Define lifecycle event dataclasses used by creation, mobility, cloning, and
  persistence hooks.
- Define itinerary abstractions for repeatable movement workflows.
- Centralize enums and exceptions that other packages share.

## Main Modules

`paglets.core.agent`
: Implements the base paglet class, context object, state locking, lifecycle
  hook defaults, messaging helpers, movement helpers, service lookup helpers,
  and storage access helpers.

`paglets.core.messages`
: Defines the message object and reply coordination helpers used by proxies,
  mailboxes, and service calls.

`paglets.core.events` and `paglets.core.context_events`
: Define lifecycle event payloads and the host-side event log/listener protocol.

`paglets.core.itinerary`
: Provides reusable itinerary plans for paglets that should move through a
  sequence of hosts and execute named tasks at specific movement phases.

`paglets.core.runtime_values`
: Provides shared enums such as `ServiceScope`, `ResidentLifecycle`,
  `ArrivalMode`, `EnvelopeKind`, and `LaunchConfigSyncAction`.

`paglets.core.errors`
: Defines the exception hierarchy used across runtime, remote, persistence, and
  service code.

## Implementation Notes

Paglet state must be explicit dataclass state. Active processes do not preserve
call stacks, threads, sockets, or arbitrary instance attributes across
movement. The runtime snapshots state through the child-process protocol and
reconstructs the paglet from importable class names on arrival or activation.

`PagletContext` is the paglet's capability boundary. It exposes host operations
through a facade inside child processes and through the real host object in
local host-side tests.

Message handling is actor-style per paglet process. Handlers are serialized by
the runtime mailbox path, while background work inside a paglet must protect
shared dataclass state with the paglet lock.

## API Reference

::: paglets.core.agent

::: paglets.core.messages

::: paglets.core.events

::: paglets.core.context_events

::: paglets.core.itinerary

::: paglets.core.runtime_values

::: paglets.core.errors

## Related Pages

- [Runtime](runtime.md) covers host supervision and child processes.
- [Remote](remote.md) covers proxy calls and message transport.
- [Serialization](serialization.md) covers dataclass wire conversion.

