# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import http.client
import json
import os
import uuid
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from paglets.artifacts import STREAM_CHUNK_BYTES, ArtifactRef, copy_stream, file_sha256
from paglets.config.env import DEFAULT_API_KEY_ENV
from paglets.core.errors import (
    AuthenticationError,
    ForbiddenError,
    HostError,
    InvalidAgentError,
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
from paglets.persistence.storage import StorageQuotaError
from paglets.remote.transport import PICKLE_CONTENT_TYPE, dump_http_chunked_pickle, json_safe, restore_json_safe

_ERROR_TYPES: dict[str, type[PagletError]] = {
    "AuthenticationError": AuthenticationError,
    "ForbiddenError": ForbiddenError,
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

    def __init__(self, timeout: float = 10.0, *, api_key: str | None = None):
        self.timeout = timeout
        self.api_key = api_key

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
            if self.api_key:
                connection.putheader("Authorization", f"Bearer {self.api_key}")
            connection.putheader("Transfer-Encoding", "chunked")
            connection.endheaders()
            dump_http_chunked_pickle(connection, payload)
            response = connection.getresponse()
            raw = response.read().decode("utf-8")
            if response.status >= 400:
                raise _error_from_response(response.status, raw, url)
            return restore_json_safe(json.loads(raw)) if raw else None
        except (OSError, http.client.HTTPException) as exc:
            raise RemoteHostError(f"Could not reach {url}: {exc}") from exc
        finally:
            connection.close()

    def upload_artifact(
        self,
        host_url: str,
        path: str | Path,
        *,
        owner_agent_id: str = "",
        name: str | None = None,
        compression: str = "",
        expires_at: float = 0.0,
        expected_sha256: str | None = None,
        timeout: float | None = None,
    ) -> ArtifactRef:
        source = Path(path)
        size = source.stat().st_size
        sha256 = expected_sha256 or file_sha256(source)
        query = urlencode(
            {
                "owner_agent_id": owner_agent_id,
                "name": name or source.name,
                "compression": compression,
                "expires_at": str(float(expires_at or 0.0)),
                "sha256": sha256,
                "size": str(size),
            }
        )
        parsed = urlparse(f"{host_url.rstrip('/')}/artifacts?{query}")
        connection = _connection(parsed, timeout=self.timeout if timeout is None else timeout)
        try:
            connection.putrequest("POST", _request_target(parsed))
            connection.putheader("Host", parsed.netloc)
            connection.putheader("Content-Type", "application/octet-stream")
            connection.putheader("Accept", "application/json")
            connection.putheader("Content-Length", str(size))
            if self.api_key:
                connection.putheader("Authorization", f"Bearer {self.api_key}")
            connection.endheaders()
            with source.open("rb") as handle:
                while True:
                    chunk = handle.read(STREAM_CHUNK_BYTES)
                    if not chunk:
                        break
                    connection.send(chunk)
            response = connection.getresponse()
            raw = response.read().decode("utf-8", errors="replace")
            if response.status >= 400:
                raise _error_from_response(response.status, raw, parsed.geturl())
            payload = restore_json_safe(json.loads(raw)) if raw else {}
            return ArtifactRef.from_wire(payload["artifact"])
        except (OSError, http.client.HTTPException) as exc:
            raise RemoteHostError(f"Could not reach {host_url}: {exc}") from exc
        finally:
            connection.close()

    def download_artifact(
        self,
        artifact: ArtifactRef | str,
        target: str | Path,
        artifact_id: str | None = None,
        *,
        move: bool = False,
        timeout: float | None = None,
    ) -> ArtifactRef:
        ref = (
            artifact
            if isinstance(artifact, ArtifactRef)
            else self.artifact_metadata(
                str(artifact),
                _required_artifact_id(artifact_id, operation="download_artifact"),
                timeout=timeout,
            )
        )
        target_path = Path(target)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_name(f".{target_path.name}.{uuid.uuid4().hex}.part")
        parsed = urlparse(f"{ref.host_url.rstrip('/')}/artifacts/{ref.artifact_id}")
        connection = _connection(parsed, timeout=self.timeout if timeout is None else timeout)
        try:
            connection.putrequest("GET", _request_target(parsed))
            connection.putheader("Host", parsed.netloc)
            if self.api_key:
                connection.putheader("Authorization", f"Bearer {self.api_key}")
            connection.endheaders()
            response = connection.getresponse()
            if response.status >= 400:
                raw = response.read().decode("utf-8", errors="replace")
                raise _error_from_response(response.status, raw, parsed.geturl())
            with tmp_path.open("wb") as output:
                written, sha256 = copy_stream(response, output, expected_bytes=ref.size_bytes)
            if written != ref.size_bytes:
                raise TransferError(f"artifact size mismatch: expected {ref.size_bytes}, got {written}")
            if ref.sha256 and sha256.casefold() != ref.sha256.casefold():
                raise TransferError(f"artifact checksum mismatch: expected {ref.sha256}, got {sha256}")
            os.replace(tmp_path, target_path)
            if move:
                self.delete_artifact(ref)
            return ref
        except (OSError, http.client.HTTPException) as exc:
            raise RemoteHostError(f"Could not reach {ref.host_url}: {exc}") from exc
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()
            connection.close()

    def artifact_metadata(self, host_url: str, artifact_id: str, *, timeout: float | None = None) -> ArtifactRef:
        response = self.get_json(
            f"{host_url.rstrip('/')}/artifacts/{artifact_id}/metadata",
            timeout=timeout,
        )
        return ArtifactRef.from_wire(response["artifact"])

    def list_artifacts(
        self,
        host_url: str,
        *,
        owner_agent_id: str | None = None,
        timeout: float | None = None,
    ) -> list[ArtifactRef]:
        query = "" if owner_agent_id is None else f"?{urlencode({'owner_agent_id': owner_agent_id})}"
        response = self.get_json(f"{host_url.rstrip('/')}/artifacts{query}", timeout=timeout)
        return [ArtifactRef.from_wire(item) for item in response.get("artifacts", [])]

    def delete_artifact(
        self,
        artifact: ArtifactRef | str,
        artifact_id: str | None = None,
        *,
        timeout: float | None = None,
    ) -> None:
        if isinstance(artifact, ArtifactRef):
            host_url = artifact.host_url
            artifact_id = artifact.artifact_id
        else:
            host_url = artifact
            artifact_id = _required_artifact_id(artifact_id, operation="delete_artifact")
        self._request("DELETE", f"{host_url.rstrip('/')}/artifacts/{artifact_id}", None, timeout=timeout)

    def _request(self, method: str, url: str, payload: dict[str, Any] | None, *, timeout: float | None = None) -> Any:
        data = None if payload is None else json.dumps(json_safe(payload)).encode("utf-8")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        req = Request(
            url,
            data=data,
            method=method,
            headers=headers,
        )
        try:
            with urlopen(req, timeout=self.timeout if timeout is None else timeout) as response:
                raw = response.read().decode("utf-8")
                return restore_json_safe(json.loads(raw)) if raw else None
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            error = _error_from_response(exc.code, raw, url)
            if isinstance(error, AuthenticationError) and not self.api_key:
                raise AuthenticationError(
                    f"{error}; set {DEFAULT_API_KEY_ENV} or pass --api-key-env with an environment variable "
                    "containing a Paglets bearer API key"
                ) from exc
            raise error from exc
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


def _required_artifact_id(artifact_id: str | None, *, operation: str) -> str:
    if artifact_id is None or not str(artifact_id).strip():
        raise ValueError(
            f"HostClient.{operation}() requires artifact_id when the first argument is a host URL; "
            "pass an ArtifactRef or provide artifact_id explicitly"
        )
    return str(artifact_id)
