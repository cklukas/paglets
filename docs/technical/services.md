# Services Package

`paglets.services` defines service contracts, service registry records, service
handles, and resident service metadata.

## Responsibilities

- Describe service operations with request/reply payload dataclasses.
- Validate service requests and replies against a contract.
- Encode service records for discovery and lookup.
- Manage resident service leases and lifecycle metadata.

## Main Modules

`paglets.services.contracts`
: Defines `ServiceContract`, `ServiceOperation`, `ServiceHandle`,
  `ServiceRecord`, `ServiceRegistry`, and service-specific errors.

`paglets.services.resident`
: Defines `ResidentServiceSpec`, `ServiceLease`, resident lifecycle defaults,
  and metadata keys used by host-managed services.

## Implementation Notes

Service operations are message-backed. A service handle builds typed messages
from a contract and sends them to the paglet that owns the service.

`ServiceScope` controls whether a service is local to one host or advertised
through the mesh. Resident services can be started lazily or eagerly from the
launch configuration.

## API Reference

::: paglets.services.contracts

::: paglets.services.resident

## Related Pages

- [Core](core.md) covers messages and service scope enums.
- [Configuration](configuration.md) covers resident services in launch config.
- [Remote](remote.md) covers mesh-visible service lookup.

