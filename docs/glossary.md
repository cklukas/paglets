# Glossary

Agent
: A general term for a running autonomous object. In this project the concrete
  class is called `Paglet`.

Aglet
: The Java mobile-agent concept that inspired this project.

Context
: The host-provided environment visible to a paglet through `PagletContext`.
  It exposes host name/address, proxy lookup, host properties, mesh helpers,
  movement helpers, and message helpers.

Dispatch
: Move a paglet from one host to another. The source host removes the original
  after successful delivery.

Clone
: Copy a paglet to a target host while keeping the original alive. The clone
  receives a new agent ID.

Envelope
: The serialized transfer record containing movement kind, agent ID, class
  names, dataclass state, source host, target host, and clone metadata.

Host
: A runtime context that owns active paglets, inactive envelopes, mesh state, and
  the JSON HTTP API.

HostRef
: A mesh registry record containing host name, URL, code version, online status,
  last-seen timestamp, agent counts, and optional error text.

Itinerary
: A serializable plan that lets a paglet visit hosts and run tasks at lifecycle
  points.

Message
: A JSON-compatible command delivered to a paglet's `handle_message` method.

Mesh
: The same-version host registry built from seed-list gossip and optional
  multicast beacons.

Paglet
: A Python mobile object with explicit dataclass state, lifecycle hooks, and
  message handling.

PagletProxy
: A controlled handle to a paglet. Proxies send messages and request lifecycle
  operations without exposing direct object references.

State
: The dataclass object that moves with a paglet. Ordinary instance attributes
  are transient.

TUI
: The optional Textual terminal UI for administering multiple paglets hosts.
