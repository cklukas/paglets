# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations


class PagletError(Exception):
    """Base exception for paglets."""


class SerializationError(PagletError):
    """Raised when paglet state cannot be serialized or restored."""


class HostError(PagletError):
    """Raised for local host/runtime errors."""


class AuthenticationError(PagletError):
    """Raised when a host API request is missing valid credentials."""


class ForbiddenError(PagletError):
    """Raised when a valid request is not allowed by host policy."""


class ServiceContractError(HostError):
    """Raised when a typed service contract is invalid or misused."""


class ServiceNotFoundError(ServiceContractError):
    """Raised when a required typed service contract cannot be found."""


class RemoteHostError(PagletError):
    """Raised when a remote host returns an error response."""


class InvalidAgentError(PagletError):
    """Raised when an agent id no longer refers to an active/deactivated paglet."""


class PagletInactiveError(PagletError):
    """Raised when an inactive paglet cannot be activated for an operation."""


class PagletCrashedError(PagletError):
    """Raised when an isolated paglet process exits unexpectedly."""


class NotHandledError(PagletError):
    """Raised when a paglet did not handle a message."""


class LifecycleError(PagletError):
    """Raised when a lifecycle operation fails."""


class TransferError(PagletError):
    """Raised when a paglet transfer cannot complete."""
