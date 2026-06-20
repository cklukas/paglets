# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .errors import InvalidAgentError, NotHandledError, PagletError, RemoteHostError


_ERROR_TYPES: dict[str, type[PagletError]] = {
    "InvalidAgentError": InvalidAgentError,
    "NotHandledError": NotHandledError,
    "RemoteHostError": RemoteHostError,
}


class HostClient:
    """Tiny JSON HTTP client used by proxies and hosts."""

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout

    def get_json(self, url: str) -> Any:
        return self._request("GET", url, None)

    def post_json(self, url: str, payload: dict[str, Any]) -> Any:
        return self._request("POST", url, payload)

    def _request(self, method: str, url: str, payload: dict[str, Any] | None) -> Any:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = Request(
            url,
            data=data,
            method=method,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urlopen(req, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else None
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"error": raw or exc.reason, "error_type": "RemoteHostError"}
            error_type = payload.get("error_type", "RemoteHostError")
            error_cls = _ERROR_TYPES.get(error_type, RemoteHostError)
            raise error_cls(payload.get("error", f"HTTP {exc.code} from {url}")) from exc
        except URLError as exc:
            raise RemoteHostError(f"Could not reach {url}: {exc.reason}") from exc
