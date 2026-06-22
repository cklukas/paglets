# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
import pickle
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .errors import (
    InvalidAgentError,
    HostError,
    LifecycleError,
    NotHandledError,
    PagletCrashedError,
    PagletError,
    PagletInactiveError,
    RemoteHostError,
    ServiceContractError,
    ServiceNotFoundError,
    TransferError,
)
from .storage import StorageQuotaError


_ERROR_TYPES: dict[str, type[PagletError]] = {
    "InvalidAgentError": InvalidAgentError,
    "HostError": HostError,
    "LifecycleError": LifecycleError,
    "NotHandledError": NotHandledError,
    "PagletCrashedError": PagletCrashedError,
    "PagletInactiveError": PagletInactiveError,
    "RemoteHostError": RemoteHostError,
    "ResourceCleanupError": LifecycleError,
    "ServiceContractError": ServiceContractError,
    "ServiceNotFoundError": ServiceNotFoundError,
    "StorageQuotaError": StorageQuotaError,
    "TransferError": TransferError,
}

PICKLE_CONTENT_TYPE = "application/x-paglets-pickle"


class HostClient:
    """Tiny JSON HTTP client used by proxies and hosts."""

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout

    def get_json(self, url: str, *, timeout: float | None = None) -> Any:
        return self._request("GET", url, None, timeout=timeout)

    def post_json(self, url: str, payload: dict[str, Any], *, timeout: float | None = None) -> Any:
        return self._request("POST", url, payload, timeout=timeout)

    def post_pickle(self, url: str, payload: dict[str, Any], *, timeout: float | None = None) -> Any:
        data = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
        req = Request(
            url,
            data=data,
            method="POST",
            headers={"Content-Type": PICKLE_CONTENT_TYPE, "Accept": "application/json"},
        )
        try:
            with urlopen(req, timeout=self.timeout if timeout is None else timeout) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else None
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise _error_from_response(exc.code, raw, url) from exc
        except URLError as exc:
            raise RemoteHostError(f"Could not reach {url}: {exc.reason}") from exc

    def _request(self, method: str, url: str, payload: dict[str, Any] | None, *, timeout: float | None = None) -> Any:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = Request(
            url,
            data=data,
            method=method,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urlopen(req, timeout=self.timeout if timeout is None else timeout) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else None
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise _error_from_response(exc.code, raw, url) from exc
        except URLError as exc:
            raise RemoteHostError(f"Could not reach {url}: {exc.reason}") from exc


def _error_from_response(status: int, raw: str, url: str) -> PagletError:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = {"error": raw or f"HTTP {status} from {url}", "error_type": "RemoteHostError"}
    error_type = payload.get("error_type", "RemoteHostError")
    error_cls = _ERROR_TYPES.get(error_type, RemoteHostError)
    return error_cls(payload.get("error", f"HTTP {status} from {url}"))
