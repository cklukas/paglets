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

- **`paglets.core.agent`**: Implements the base paglet class, context object, state locking, lifecycle
  hook defaults, messaging helpers, movement helpers, service lookup helpers,
  and storage access helpers.

- **`paglets.core.messages`**: Defines the message object and reply coordination helpers used by proxies,
  mailboxes, and service calls.

- **`paglets.core.events` and `paglets.core.context_events`**: Define lifecycle event payloads and the host-side event log/listener protocol.

- **`paglets.core.itinerary`**: Provides reusable itinerary plans for paglets that should move through a
  sequence of hosts and execute named tasks at specific movement phases.

- **`paglets.core.runtime_values`**: Provides shared enums such as `ServiceScope`, `ResidentLifecycle`,
  `ArrivalMode`, `EnvelopeKind`, and `LaunchConfigSyncAction`.

- **`paglets.core.wire`**: Defines shared aliases for JSON/pickle-safe wire payloads used by messages,
  serialization, and runtime protocol boundaries.

- **`paglets.core.errors`**: Defines the exception hierarchy used across runtime, remote, persistence, and
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

<!-- api: paglets.core.agent -->
### `paglets.core.agent`

#### `PagletState`

```python
class PagletState
```

Marker base class for dataclass state objects.

Subclass this with ``@dataclass``. Only this state object moves. Everything
stored directly on the paglet instance is transient runtime state.

#### `state_locked`

```python
def state_locked(
    method: Callable[Concatenate[PagletT, P], ReturnT],
) -> Callable[Concatenate[PagletT, P], ReturnT]
```

Run a paglet method under the paglet's reentrant state lock.

#### `PagletContext`

```python
class PagletContext
```

Host-provided environment visible to a running paglet.

#### `Paglet`

```python
class Paglet(Generic)
```

Base class for mobile Python objects.

Subclasses set ``State`` to a dataclass type and override lifecycle hooks.
The runtime instantiates paglets on each host from class path + dataclass
state, mirroring Aglets' mobile object plus event system without moving a
call stack.

##### `Paglet.locked`

```python
def locked() -> Iterator[None]
```

Enter the paglet's reentrant lock for agent-local critical sections.

##### `Paglet.locked_state`

```python
def locked_state() -> Iterator[StateT]
```

Yield this paglet's dataclass state under the paglet lock.

##### `Paglet.wait_state`

```python
def wait_state(predicate: Callable[[StateT], bool], timeout: float | None = None) -> bool
```

Wait until ``predicate(state)`` is true.

This is for coordination between handlers/background work that mutate
paglet state and another handler waiting for that state to change. It
does not replace normal message delivery; incoming messages still call
``handle_message`` through the paglet mailbox.

##### `Paglet.notify_state_changed`

```python
def notify_state_changed() -> None
```

Wake one waiter blocked in :meth:`wait_state`.

##### `Paglet.notify_all_state_changed`

```python
def notify_all_state_changed() -> None
```

Wake all waiters blocked in :meth:`wait_state`.
<!-- /api -->
<!-- api: paglets.core.messages -->
### `paglets.core.messages`

#### `Message`

```python
class Message
```

A message delivered to a paglet.

Mirrors the useful part of Aglets' ``Message``: a kind, named arguments,
optional single argument, priority, sender metadata, and a timestamp.
Replies are represented by the return value of ``Paglet.handle_message``.

| Field | Type | Default |
|---|---|---|
| `kind` | `str` | required |
| `args` | `WirePayload` | `dict()` |
| `arg` | `Any` | `None` |
| `sender` | `str | None` | `None` |
| `reply_to` | `str | None` | `None` |
| `priority` | `int` | `5` |
| `message_type` | `int` | `0` |
| `timestamp` | `float` | `time()` |
| `message_id` | `str` | computed |

#### `FutureReply`

```python
class FutureReply
```

Result handle for an asynchronous paglet message.

This is the Python analogue of Aglets' ``FutureReply``. It wraps a
``concurrent.futures.Future`` and exposes Aglets-style names while still
preserving the original exception behavior of ``Future.result()``.

#### `ReplySet`

```python
class ReplySet
```

Container that yields ``FutureReply`` objects as replies arrive.
<!-- /api -->
<!-- api: paglets.core.events -->
### `paglets.core.events`
<!-- /api -->
<!-- api: paglets.core.context_events -->
### `paglets.core.context_events`

#### `ContextEvent`

```python
class ContextEvent
```

Host-level event emitted by the paglet context.

| Field | Type | Default |
|---|---|---|
| `event_id` | `int` | required |
| `kind` | `str` | required |
| `host_name` | `str` | required |
| `host_address` | `str` | required |
| `timestamp` | `float` | `time()` |
| `agent_id` | `str | None` | `None` |
| `class_name` | `str | None` | `None` |
| `message_id` | `str | None` | `None` |
| `service_name` | `str | None` | `None` |
| `data` | `dict[str, Any]` | `dict()` |
| `error` | `str | None` | `None` |

#### `ContextEventLog`

```python
class ContextEventLog
```

Bounded in-memory context event log with best-effort listeners.
<!-- /api -->
<!-- api: paglets.core.itinerary -->
### `paglets.core.itinerary`

#### `ItineraryPlan`

```python
class ItineraryPlan
```

Serializable itinerary state for a mobile paglet.

The Java helpers increment their internal index after calling dispatch.
In paglets the serializable state is captured during dispatch, so this
Python helper advances before dispatching to ensure the arrived copy sees
the updated route position.

| Field | Type | Default |
|---|---|---|
| `destinations` | `list[str]` | `list()` |
| `current_index` | `int` | `0` |
| `current_location` | `str | None` | `None` |
| `visited_destinations` | `list[str]` | `list()` |
| `mutable` | `bool` | `True` |
| `circular` | `bool` | `False` |
| `loop_count` | `int` | `0` |
| `completed` | `bool` | `False` |

#### `ItineraryTask`

```python
class ItineraryTask
```

Serializable task descriptor for a task itinerary.

| Field | Type | Default |
|---|---|---|
| `name` | `str` | required |
| `execution` | `str` | `'arrival'` |
| `args` | `dict[str, Any]` | `dict()` |

#### `ItineraryAgentMixin`

```python
class ItineraryAgentMixin
```

Mixin for paglets whose state stores an ``itinerary`` field.
<!-- /api -->
<!-- api: paglets.core.runtime_values -->
### `paglets.core.runtime_values`
<!-- /api -->
<!-- api: paglets.core.wire -->
### `paglets.core.wire`
<!-- /api -->
<!-- api: paglets.core.errors -->
### `paglets.core.errors`

#### `PagletError`

```python
class PagletError(Exception)
```

Base exception for paglets.

#### `SerializationError`

```python
class SerializationError(PagletError)
```

Raised when paglet state cannot be serialized or restored.

#### `HostError`

```python
class HostError(PagletError)
```

Raised for local host/runtime errors.

#### `AuthenticationError`

```python
class AuthenticationError(PagletError)
```

Raised when a host API request is missing valid credentials.

#### `ForbiddenError`

```python
class ForbiddenError(PagletError)
```

Raised when a valid request is not allowed by host policy.

#### `ServiceContractError`

```python
class ServiceContractError(HostError)
```

Raised when a typed service contract is invalid or misused.

#### `ServiceNotFoundError`

```python
class ServiceNotFoundError(ServiceContractError)
```

Raised when a required typed service contract cannot be found.

#### `RemoteHostError`

```python
class RemoteHostError(PagletError)
```

Raised when a remote host returns an error response.

#### `InvalidAgentError`

```python
class InvalidAgentError(PagletError)
```

Raised when an agent id no longer refers to an active/deactivated paglet.

#### `PagletInactiveError`

```python
class PagletInactiveError(PagletError)
```

Raised when an inactive paglet cannot be activated for an operation.

#### `PagletCrashedError`

```python
class PagletCrashedError(PagletError)
```

Raised when an isolated paglet process exits unexpectedly.

#### `NotHandledError`

```python
class NotHandledError(PagletError)
```

Raised when a paglet did not handle a message.

#### `LifecycleError`

```python
class LifecycleError(PagletError)
```

Raised when a lifecycle operation fails.

#### `TransferError`

```python
class TransferError(PagletError)
```

Raised when a paglet transfer cannot complete.
<!-- /api -->
## Related Pages

- [Runtime](runtime.md) covers host supervision and child processes.
- [Remote](remote.md) covers proxy calls and message transport.
- [Serialization](serialization.md) covers dataclass wire conversion.
