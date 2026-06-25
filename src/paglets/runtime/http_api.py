# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import hmac
import json
import shutil
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

from paglets.artifacts import STREAM_CHUNK_BYTES
from paglets.core.errors import (
    AuthenticationError,
    ForbiddenError,
    HostError,
    InvalidAgentError,
    NotHandledError,
    PagletError,
    RemoteHostError,
)
from paglets.core.messages import Message
from paglets.core.runtime_values import ServiceScope, enum_from_wire
from paglets.persistence.persistency import DeactivationRequest
from paglets.remote.transfer import TransferTicket
from paglets.remote.transport import PICKLE_CONTENT_TYPE, json_safe, load_http_pickle_payload, restore_json_safe
from paglets.runtime.envelope import PagletEnvelope
from paglets.services.resident import DEFAULT_SERVICE_LEASE_TTL_SECONDS

if TYPE_CHECKING:
    from paglets.runtime.host import Host


class PagletHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], handler_cls: type[BaseHTTPRequestHandler], host_runtime: Host):
        super().__init__(server_address, handler_cls)
        self.host_runtime = host_runtime


class RequestHandler(BaseHTTPRequestHandler):
    server: PagletHTTPServer

    def log_message(self, format: str, *args: Any) -> None:  # keep examples/tests quiet
        return

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        self._handle("POST")

    def do_DELETE(self) -> None:
        self._handle("DELETE")

    def _handle(self, method: str) -> None:
        try:
            self._require_auth()
            if self._handle_binary_artifact_route(method):
                return
            payload = self._read_payload() if method == "POST" else {}
            result = self._route(method, self.path, payload)
            self._write_json(200, result)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return
        except AuthenticationError as exc:
            self._write_error(401, exc, authenticate=True)
        except ForbiddenError as exc:
            self._write_error(403, exc)
        except NotHandledError as exc:
            self._write_error(422, exc)
        except InvalidAgentError as exc:
            self._write_error(404, exc)
        except PagletError as exc:
            self._write_error(400, exc)
        except Exception as exc:  # pragma: no cover - defensive server boundary
            self._write_error(500, RemoteHostError(str(exc)))

    def _require_auth(self) -> None:
        expected = self.server.host_runtime.api_key
        if not expected:
            return
        header = str(self.headers.get("Authorization") or "")
        scheme, _, value = header.partition(" ")
        if scheme.casefold() != "bearer" or not value or not hmac.compare_digest(value, expected):
            raise AuthenticationError("Authentication required")

    def _route(self, method: str, path: str, payload: dict[str, Any]) -> Any:
        host = self.server.host_runtime
        parsed = urlparse(path)
        parts = [part for part in parsed.path.split("/") if part]
        query = parse_qs(parsed.query)
        public_path_parts = _public_path_parts(host.public_url)
        if public_path_parts and parts[: len(public_path_parts)] == public_path_parts:
            parts = parts[len(public_path_parts) :]

        if method == "GET" and parts == ["health"]:
            return host.health()
        if method == "GET" and parts == ["hosts"]:
            return {"hosts": [ref.to_wire() for ref in host.list_hosts(include_self=True)]}
        if method == "POST" and parts == ["hosts", "join"]:
            return {"hosts": [ref.to_wire() for ref in host.join_mesh(payload)]}
        if method == "POST" and parts == ["admin", "git-update"]:
            if host.relay_mode:
                raise ForbiddenError("Git auto-update is disabled in relay mode")
            return host.handle_git_update_request(payload)
        if method == "POST" and parts == ["relay", "connect"]:
            return host.relay_connect(payload)
        if method == "GET" and len(parts) == 3 and parts[:2] == ["relay", "poll"]:
            timeout = float((query.get("timeout") or ["25"])[0])
            return host.relay_poll(parts[2], timeout=timeout)
        if method == "POST" and len(parts) == 3 and parts[:2] == ["relay", "ack"]:
            return host.relay_ack(parts[2], payload)
        if method == "GET" and parts == ["relay", "diagnostics"]:
            return host.relay_diagnostics()
        if method == "GET" and len(parts) == 4 and parts[:2] == ["relay", "hosts"] and parts[3] == "health":
            return host.relay_host_health(parts[2])
        if method == "POST" and len(parts) == 4 and parts[:2] == ["relay", "hosts"] and parts[3] == "agents":
            if "envelope" in payload:
                envelope = PagletEnvelope.from_wire(payload["envelope"])
                response = host.relay_receive_envelope(
                    parts[2],
                    envelope,
                    timeout=_relay_payload_timeout(payload, envelope=envelope, fallback=host.relay_delivery_timeout),
                )
            else:
                response = host.relay_receive_creation(
                    parts[2],
                    payload,
                    timeout=_relay_payload_timeout(payload, fallback=host.relay_delivery_timeout),
                )
            return {"proxy": response["proxy"]}
        if (
            method == "POST"
            and len(parts) == 6
            and parts[:2] == ["relay", "hosts"]
            and parts[3] == "agents"
            and parts[5] == "messages"
        ):
            message = Message.from_wire(payload["message"])
            result = host.relay_deliver_message(
                parts[2],
                parts[4],
                message,
                oneway=bool(payload.get("oneway", False)),
                activate_if_inactive=bool(payload.get("activate_if_inactive", True)),
                no_delay=bool(payload.get("no_delay", False)),
                timeout=_relay_payload_timeout(payload, fallback=host.relay_delivery_timeout),
            )
            return {"result": result}
        if len(parts) >= 3 and parts[:2] == ["relay", "hosts"]:
            relay_path = "/" + "/".join(parts[3:])
            if parsed.query:
                relay_path = f"{relay_path}?{parsed.query}"
            return host.relay_api(parts[2], method, relay_path, payload)
        if method == "GET" and parts == ["events"]:
            since = int((query.get("since") or ["0"])[0])
            limit = int((query.get("limit") or ["100"])[0])
            return {"events": [event.to_wire() for event in host.list_events(since=since, limit=limit)]}
        if method == "GET" and parts == ["artifacts"]:
            owner = (query.get("owner_agent_id") or [None])[0]
            return {"artifacts": [ref.to_wire() for ref in host.artifacts.list(owner_agent_id=owner)]}
        if method == "GET" and len(parts) == 3 and parts[0] == "artifacts" and parts[2] == "metadata":
            return {"artifact": host.artifacts.ref(parts[1]).to_wire()}
        if method == "DELETE" and len(parts) == 2 and parts[0] == "artifacts":
            host.artifacts.delete(parts[1])
            return {"ok": True}
        if method == "GET" and parts == ["services"]:
            name = (query.get("name") or [None])[0]
            capability = (query.get("capability") or [None])[0]
            scope = enum_from_wire((query.get("scope") or [ServiceScope.LOCAL.value])[0], ServiceScope, "scope")
            if scope is ServiceScope.MESH:
                records = host._services.lookup_all(name, capability, scope=ServiceScope.MESH)
            else:
                records = host._services.lookup_all(name, capability)
            return {"services": [record.to_wire() for record in records]}
        if method == "POST" and parts == ["services", "leases"]:
            return host.acquire_resident_service_lease(
                str(payload["agent_id"]),
                str(payload["service_name"]),
                ttl=float(payload.get("ttl", DEFAULT_SERVICE_LEASE_TTL_SECONDS)),
            )
        if method == "POST" and len(parts) == 4 and parts[:2] == ["services", "leases"] and parts[3] == "release":
            return host.release_resident_service_lease(parts[2])
        if method == "GET" and parts == ["agents"]:
            state = (query.get("state") or ["active"])[0]
            include_state = (query.get("include_state") or [""])[0].lower() in {"1", "true", "yes"}
            if state == "all":
                return {"agents": host.list_agents(active=True, inactive=True, include_state=include_state)}
            if state == "inactive":
                return {"agents": host.list_agents(active=False, inactive=True, include_state=include_state)}
            return {"agents": host.list_agents(active=True, inactive=False, include_state=include_state)}
        if method == "POST" and parts == ["agents"]:
            if "envelope" in payload:
                proxy = host._receive_envelope(PagletEnvelope.from_wire(payload["envelope"]))
            else:
                proxy = host._receive_creation(payload)
            return {"proxy": proxy.to_wire()}

        if len(parts) >= 2 and parts[0] == "agents":
            agent_id = parts[1]
            if method == "GET" and len(parts) == 3 and parts[2] == "state":
                return host._state_payload(agent_id)
            if method == "GET" and len(parts) == 2:
                with host._lock:
                    agent = host._agents.get(agent_id)
                    if agent is not None:
                        return host._summary(agent)
                    inactive = host._inactive.get(agent_id)
                    if inactive is not None:
                        return host._inactive_summary(inactive)
                raise InvalidAgentError(f"No paglet {agent_id!r} on {host.name}")
            if method == "POST" and len(parts) == 3:
                action = parts[2]
                if action == "messages":
                    message = Message.from_wire(payload["message"])
                    result = host.deliver_message(
                        agent_id,
                        message,
                        oneway=bool(payload.get("oneway", False)),
                        activate_if_inactive=bool(payload.get("activate_if_inactive", True)),
                        no_delay=bool(payload.get("no_delay", False)),
                    )
                    return {"result": result}
                if action == "dispatch":
                    target = TransferTicket.from_wire(payload["ticket"]) if "ticket" in payload else payload["target"]
                    proxy = host.dispatch(agent_id, target)
                    return {"proxy": proxy.to_wire()}
                if action == "clone":
                    if "ticket" in payload:
                        proxy = host.clone(agent_id, target=TransferTicket.from_wire(payload["ticket"]))
                    else:
                        proxy = host.clone(agent_id, target=payload.get("target"))
                    return {"proxy": proxy.to_wire()}
                if action == "retract":
                    proxy = host._retract_to(agent_id, payload["target"])
                    return {"proxy": proxy.to_wire()}
                if action == "deactivate":
                    proxy = host.deactivate(agent_id, DeactivationRequest.from_wire(payload.get("request")))
                    return {"proxy": proxy.to_wire(), "ok": True}
                if action == "activate":
                    proxy = host.activate(agent_id)
                    return {"proxy": proxy.to_wire()}
                if action == "dispose":
                    host.dispose(agent_id)
                    return {"ok": True}
                if action == "services":
                    record = host.advertise_service(
                        agent_id,
                        str(payload["name"]),
                        capabilities=payload.get("capabilities"),
                        metadata=payload.get("metadata"),
                        scope=enum_from_wire(
                            payload.get("scope") or ServiceScope.LOCAL.value,
                            ServiceScope,
                            "scope",
                        ),
                        ttl=float(payload["ttl"]) if payload.get("ttl") is not None else None,
                    )
                    return {"service": record.to_wire()}
                if action == "unadvertise-service":
                    removed = host.unadvertise_service(str(payload["name"]), agent_id=agent_id)
                    return {"services": [record.to_wire() for record in removed]}

        raise HostError(f"No route for {method} {path}")

    def _handle_binary_artifact_route(self, method: str) -> bool:
        host = self.server.host_runtime
        parsed = urlparse(self.path)
        parts = [part for part in parsed.path.split("/") if part]
        query = parse_qs(parsed.query)
        public_path_parts = _public_path_parts(host.public_url)
        if public_path_parts and parts[: len(public_path_parts)] == public_path_parts:
            parts = parts[len(public_path_parts) :]

        if method == "POST" and parts == ["artifacts"]:
            result = host.artifacts.create_from_http_request(
                self.headers,
                self.rfile,
                owner_agent_id=(query.get("owner_agent_id") or [""])[0],
                name=(query.get("name") or [""])[0],
                compression=(query.get("compression") or [""])[0],
                expires_at=float((query.get("expires_at") or ["0"])[0] or 0.0),
                expected_sha256=(query.get("sha256") or [None])[0],
            )
            self._write_json(200, {"artifact": result.ref.to_wire()})
            return True
        if method == "GET" and len(parts) == 2 and parts[0] == "artifacts":
            self._write_artifact_blob(parts[1])
            return True
        if method == "POST" and len(parts) == 4 and parts[:2] == ["relay", "hosts"] and parts[3] == "artifacts":
            result = host.relay_receive_artifact_upload(parts[2], self.headers, self.rfile, query)
            self._write_json(200, {"artifact": result.to_wire()})
            return True
        if method == "GET" and len(parts) == 5 and parts[:2] == ["relay", "hosts"] and parts[3] == "artifacts":
            spool = host.relay_export_artifact(parts[2], parts[4])
            try:
                self._write_artifact_blob(spool.artifact_id)
            finally:
                with contextlib.suppress(Exception):
                    host.artifacts.delete(spool.artifact_id)
            return True
        return False

    def _write_artifact_blob(self, artifact_id: str) -> None:
        ref = self.server.host_runtime.artifacts.ref(artifact_id)
        path = self.server.host_runtime.artifacts.blob_path(artifact_id)
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(ref.size_bytes))
        self.send_header("X-Paglets-Artifact-Sha256", ref.sha256)
        self.end_headers()
        with path.open("rb") as source:
            shutil.copyfileobj(source, self.wfile, length=STREAM_CHUNK_BYTES)

    def _read_payload(self) -> dict[str, Any]:
        content_type = str(self.headers.get("Content-Type") or "").split(";", 1)[0].strip().casefold()
        if content_type == PICKLE_CONTENT_TYPE:
            return load_http_pickle_payload(self.headers, self.rfile)
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        payload = json.loads(raw.decode("utf-8"))
        return restore_json_safe(payload)

    def _write_json(self, status: int, payload: Any, *, extra_headers: dict[str, str] | None = None) -> None:
        raw = json.dumps(json_safe(payload)).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _write_error(self, status: int, exc: Exception, *, authenticate: bool = False) -> None:
        try:
            headers = {"WWW-Authenticate": "Bearer"} if authenticate else None
            self._write_json(status, {"error_type": exc.__class__.__name__, "error": str(exc)}, extra_headers=headers)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return


def _public_path_parts(public_url: str | None) -> list[str]:
    if not public_url:
        return []
    return [part for part in urlparse(public_url).path.split("/") if part]


def _relay_payload_timeout(
    payload: dict[str, Any],
    *,
    envelope: PagletEnvelope | None = None,
    fallback: float,
) -> float:
    if payload.get("timeout") is not None:
        return float(payload["timeout"])
    if envelope is not None:
        ticket_payload = envelope.metadata.get("transfer_ticket")
        if isinstance(ticket_payload, dict) and ticket_payload.get("timeout") is not None:
            return float(ticket_payload["timeout"])
    return float(fallback)
