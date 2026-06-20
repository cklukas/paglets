from __future__ import annotations


class PagletError(Exception):
    """Base exception for paglets."""


class SerializationError(PagletError):
    """Raised when paglet state cannot be serialized or restored."""


class HostError(PagletError):
    """Raised for local host/runtime errors."""


class RemoteHostError(PagletError):
    """Raised when a remote host returns an error response."""


class InvalidAgentError(PagletError):
    """Raised when an agent id no longer refers to an active/deactivated paglet."""


class NotHandledError(PagletError):
    """Raised when a paglet did not handle a message."""


class LifecycleError(PagletError):
    """Raised when a lifecycle operation fails."""
