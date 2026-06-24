# Runtime Package

`paglets.runtime` owns host execution, active child processes, mailbox
delivery, transfer envelopes, and runtime resource cleanup.

## Responsibilities

- Start, supervise, stop, deactivate, activate, clone, dispatch, and dispose
  paglet instances.
- Expose the host HTTP API and route incoming control/movement requests.
- Run active paglets in spawned child Python processes.
- Serialize active state over shared-memory streams at the host/child boundary.
- Deliver queued messages through per-paglet mailboxes.
- Store and clean up host-owned artifact blobs used for registered file
  mobility and explicit artifact transfer.
- Track resources that must be cleaned up during lifecycle transitions.

## Main Modules

`paglets.runtime.host`
: The orchestration center. `Host` is the public runtime facade and owns active
  child controllers, inactive records, service records, storage roots, mesh
  state, authentication, placement, and lifecycle operations.

`paglets.runtime.lifecycle`
: Contains the `Host` movement and lifecycle flows: create, dispatch, clone,
  retract, deactivate, activate, dispose, transfer tickets, and movement
  envelope handling.

`paglets.runtime.resident_services`
: Contains resident service declaration, activation, service leasing, registry
  lookup, and idle shutdown behavior.

`paglets.runtime.child_calls`
: Routes host calls arriving from child processes and completes child-initiated
  dispatch, clone, deactivate, dispose, service, storage, and messaging
  operations.

`paglets.runtime.inactive_records`
: Loads and writes inactive records, schedules activation, drains queued
  messages, and deactivates active paglets during shutdown.

`paglets.runtime.http_api`
: Contains the host HTTP server and request handler. It maps endpoint shape,
  authentication, JSON control payloads, binary movement payloads, admin calls,
  and relay HTTP endpoints onto `Host` methods without owning host state.

`paglets.runtime.relay`
: Contains relay/connect-mode state, relay delivery queues, polling,
  acknowledgements, local relay URL submission, and client registration loops.
  `Host` mixes this behavior in while keeping the public facade at
  `paglets.runtime.host.Host`.

`paglets.runtime.binding`
: Resolves bind hosts, public host names, auto LAN addresses, and
  `--bind-public` behavior for the host CLI/runtime boundary.

`paglets.runtime.process_runtime`
: Compatibility facade for the split process runtime modules.

`paglets.runtime.process_controller`, `paglets.runtime.child_endpoint`,
`paglets.runtime.child_facade`, `paglets.runtime.child_bootstrap`, and
`paglets.runtime.process_protocol`
: Implement parent-side child process control, child pipe protocol handling,
  child-visible host/storage facades, process bootstrap, and shared protocol
  values.

`paglets.runtime.mailbox`
: Implements queued delivery, priority ordering, mailbox status, and wait/notify
  behavior for message handlers.

`paglets.runtime.envelope`
: Defines the transfer envelope used for create, dispatch, clone, retract, and
  activation flows.

`paglets.runtime.resources`
: Tracks resource cleanup callbacks and reports cleanup failures as lifecycle
  errors.

## Implementation Notes

The host is the only component that mutates host-wide registries. Child
processes request operations through a facade, and the parent host validates and
performs those operations.

Same-host movement bypasses HTTP and delivers the envelope directly to the
local host instance. Different host processes on the same machine still use the
HTTP transport path over loopback.

HTTP routing and relay mechanics deliberately live outside `host.py`; they
delegate into the host facade and do not define a second public runtime object.
This keeps endpoint behavior stable while making the implementation easier to
read and test.

The child process must be able to import the paglet class and state class by
qualified name. Classes defined in `__main__`, REPL sessions, or temporary
scripts are not valid paglet classes.

## API Reference

::: paglets.runtime.host

::: paglets.runtime.lifecycle

::: paglets.runtime.resident_services

::: paglets.runtime.child_calls

::: paglets.runtime.inactive_records

::: paglets.runtime.http_api

::: paglets.runtime.relay

::: paglets.runtime.binding

::: paglets.runtime.process_runtime

::: paglets.runtime.process_controller

::: paglets.runtime.child_endpoint

::: paglets.runtime.child_facade

::: paglets.runtime.child_bootstrap

::: paglets.runtime.process_protocol

::: paglets.runtime.mailbox

::: paglets.runtime.envelope

::: paglets.runtime.resources

## Related Pages

- [Core](core.md) covers the paglet programming model.
- [Remote](remote.md) covers HTTP transport and proxies.
- [Persistence](persistence.md) covers inactive records and storage.
