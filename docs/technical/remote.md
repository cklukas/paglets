# Remote Package

`paglets.remote` contains the host-to-host and client-to-host communication
surface: HTTP clients, proxies, transfer tickets, transport helpers, mesh
membership, and admin tooling.

## Responsibilities

- Provide a reusable HTTP client for host control endpoints.
- Represent remote paglets through `PagletProxy` and serializable proxy refs.
- Validate and carry movement intent with `TransferTicket`.
- Stream binary state payloads for movement and shared-memory handoff helpers.
- Maintain mesh membership, multicast beacons, version compatibility, and relay
  routing state.
- Provide administration client records and dynamic entry-host discovery.

## Main Modules

`paglets.remote.client`
: Implements `HostClient`, request helpers, error decoding, and binary movement
  upload support.

`paglets.remote.proxy` and `paglets.remote.references`
: Provide the controlled handle and serializable reference form used to inspect,
  message, move, deactivate, activate, and dispose paglets.

`paglets.remote.transfer`
: Defines `TransferTicket`, including required capabilities, expected code
  version, arrival mode, and target selection data.

`paglets.remote.transport`
: Implements chunked pickle HTTP payloads, local pickle streams, shared-memory
  readers/writers, and JSON-safe binary tagging.

`paglets.remote.mesh`
: Tracks known hosts, peer compatibility, multicast beacons, relayed hosts, and
  host name resolution.

`paglets.remote.admin`
: Defines admin records, server URL normalization, LAN/mesh entry discovery,
  and `PagletsAdminClient`.

## Implementation Notes

Control-plane operations stay JSON-oriented. Movement payloads are streamed
pickle data because paglet state can contain binary values and large nested
dataclass structures.

Mesh peers must agree on mesh version and compatible code version before they
are used as movement targets. Relay/connect mode avoids inbound ports on
clients by long-polling a hub host.

## API Reference

::: paglets.remote.client

::: paglets.remote.proxy

::: paglets.remote.references

::: paglets.remote.transfer

::: paglets.remote.transport

::: paglets.remote.mesh

::: paglets.remote.admin

## Related Pages

- [Runtime](runtime.md) covers how hosts receive remote requests.
- [Configuration](configuration.md) covers launch configuration for resident
  services and startup agents.
- [Tooling](tooling.md) covers CLI and git auto-update.

