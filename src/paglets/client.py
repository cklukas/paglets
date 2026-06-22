# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import http.client
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
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
from .transport import PICKLE_CONTENT_TYPE, dump_http_chunked_pickle


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

class HostClient:
    """Tiny JSON HTTP client used by proxies and hosts."""

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout

    def get_json(self, url: str, *, timeout: float | None = None) -> Any:
        return self._request("GET", url, None, timeout=timeout)

    def post_json(self, url: str, payload: dict[str, Any], *, timeout: float | None = None) -> Any:
        return self._request("POST", url, payload, timeout=timeout)

    def post_pickle(self, url: str, payload: dict[str, Any], *, timeout: float | None = None) -> Any:
        parsed = urlparse(url)
        connection = _connection(parsed, timeout=self.timeout if timeout is None else timeout)
        try:
            connection.putrequest("POST", _request_target(parsed))
            connection.putheader("Host", parsed.netloc)
            connection.putheader("Content-Type", PICKLE_CONTENT_TYPE)
            connection.putheader("Accept", "application/json")
            connection.putheader("Transfer-Encoding", "chunked")
            connection.endheaders()
            dump_http_chunked_pickle(connection, payload)
            response = connection.getresponse()
            raw = response.read().decode("utf-8")
            if response.status >= 400:
                raise _error_from_response(response.status, raw, url)
            return json.loads(raw) if raw else None
        except (OSError, http.client.HTTPException) as exc:
            raise RemoteHostError(f"Could not reach {url}: {exc}") from exc
        finally:
            connection.close()

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


def _connection(parsed: Any, *, timeout: float) -> http.client.HTTPConnection:
    if parsed.scheme == "https":
        return http.client.HTTPSConnection(parsed.netloc, timeout=timeout)
    return http.client.HTTPConnection(parsed.netloc, timeout=timeout)


def _request_target(parsed: Any) -> str:
    target = parsed.path or "/"
    if parsed.query:
        return f"{target}?{parsed.query}"
    return target
