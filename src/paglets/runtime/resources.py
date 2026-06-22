# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass

from paglets.core.errors import LifecycleError

Cleanup = Callable[[], None]


@dataclass(slots=True)
class ResourceRegistration:
    name: str
    cleanup: Cleanup
    suppress: bool = False


class ResourceCleanupError(LifecycleError):
    """Raised when lifecycle-managed resource cleanup fails."""

    def __init__(self, failures: dict[str, Exception]):
        self.failures = failures
        details = ", ".join(f"{name}: {exc}" for name, exc in failures.items())
        super().__init__(f"Resource cleanup failed for {details}")


class ResourceRegistry:
    """Lifecycle-managed cleanup callbacks owned by one paglet."""

    def __init__(self):
        self._resources: OrderedDict[str, ResourceRegistration] = OrderedDict()

    def register(self, name: str, cleanup: Cleanup, *, suppress: bool = False) -> None:
        if not name:
            raise ValueError("Resource name cannot be empty")
        self._resources[name] = ResourceRegistration(name=name, cleanup=cleanup, suppress=suppress)

    def track_closeable(self, name: str, obj: object, *, method: str = "close", suppress: bool = False) -> None:
        cleanup = getattr(obj, method)
        if not callable(cleanup):
            raise TypeError(f"{obj!r}.{method} is not callable")
        self.register(name, cleanup, suppress=suppress)

    def remove(self, name: str) -> None:
        self._resources.pop(name, None)

    def cleanup(self, *, reason: str = "lifecycle") -> None:
        failures: dict[str, Exception] = {}
        for name, registration in reversed(list(self._resources.items())):
            try:
                registration.cleanup()
            except Exception as exc:
                if not registration.suppress:
                    failures[name] = exc
                else:
                    self._resources.pop(name, None)
            else:
                self._resources.pop(name, None)
        if failures:
            raise ResourceCleanupError(failures)

    def clear(self) -> None:
        self._resources.clear()

    def status(self) -> dict[str, bool]:
        return {name: registration.suppress for name, registration in self._resources.items()}
