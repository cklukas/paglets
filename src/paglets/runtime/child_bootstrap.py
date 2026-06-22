# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import contextlib
import signal
import threading
from dataclasses import is_dataclass
from multiprocessing.connection import Connection
from typing import Any

from paglets.core.agent import NOT_HANDLED, Paglet, PagletContext
from paglets.core.errors import (
    HostError,
    NotHandledError,
)
from paglets.core.events import CreationEvent, PersistencyEvent
from paglets.core.messages import Message
from paglets.persistence.persistency import DeactivationPolicy, DeactivationRequest
from paglets.remote.transport import (
    receive_local_pickle,
    wait_for_local_pickle_senders,
)
from paglets.runtime.child_endpoint import _ChildEndpoint
from paglets.runtime.child_facade import _ChildHostFacade
from paglets.runtime.process_protocol import (
    ChildConfig,
    _agent_snapshot,
    _clone_event,
    _mobility_event,
    _set_process_title,
)
from paglets.serialization.codec import dataclass_from_wire, resolve_qualified_name


def _child_main(config: ChildConfig, conn: Connection) -> None:
    with contextlib.suppress(Exception):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    _set_process_title(config.process_title)
    endpoint = _ChildEndpoint(conn)
    reader = endpoint.start_reader()
    agent_cls = resolve_qualified_name(config.agent_class_name)
    state_cls = resolve_qualified_name(config.state_class_name)
    if not issubclass(agent_cls, Paglet):
        raise HostError(f"{config.agent_class_name} is not a Paglet subclass")
    if not is_dataclass(state_cls):
        raise HostError(f"{config.state_class_name} is not a dataclass state")
    if config.state_stream is not None:
        state_wire = receive_local_pickle(config.state_stream)
        endpoint._send(
            {
                "type": "event",
                "event": "local_pickle_stream_received",
                "token": str(config.state_stream.get("token") or ""),
            }
        )
    else:
        state_wire = config.state or {}
    state = dataclass_from_wire(state_cls, state_wire)
    agent = agent_cls(state=state, agent_id=config.agent_id)
    facade = _ChildHostFacade(endpoint, config)
    facade.attach_agent(agent)
    endpoint.agent = agent
    endpoint.facade = facade
    agent._attach(PagletContext(facade, agent.agent_id))

    try:
        while True:
            request = endpoint.next_request()
            if request is None:
                break
            request_id = str(request.get("id") or "")
            op = str(request.get("op") or "")
            payload = dict(request.get("payload") or {})
            try:
                result = _handle_child_request(agent, facade, op, payload)
            except Exception as exc:
                endpoint.reply_error(request_id, exc)
            else:
                if facade.terminal and op == "message" and isinstance(result, dict):
                    result = {"result": result.get("result"), "resources": result.get("resources", {})}
                endpoint.reply_ok(request_id, result)
                if facade.terminal:
                    break
    finally:
        endpoint.close()
        wait_for_local_pickle_senders()
        reader.join(timeout=0.2)


def _handle_child_request(agent: Paglet, facade: _ChildHostFacade, op: str, payload: dict[str, Any]) -> dict[str, Any]:
    if op == "lifecycle":
        _run_lifecycle(agent, facade, str(payload["name"]), dict(payload.get("event") or {}))
        return _agent_snapshot(agent)
    if op == "message":
        message = Message.from_wire(payload["message"])
        result = agent.handle_message(message)
        if result is NOT_HANDLED:
            raise NotHandledError(f"{agent.__class__.__name__} did not handle {message.kind!r}")
        snapshot = _agent_snapshot(agent)
        snapshot["result"] = None if payload.get("oneway") else result
        return snapshot
    if op == "cleanup_resources":
        agent.resources.cleanup(reason=str(payload.get("reason") or "lifecycle"))
        return _agent_snapshot(agent)
    if op == "resource_remove":
        agent.resources.remove(str(payload["name"]))
        return _agent_snapshot(agent)
    if op == "deactivate_prepare":
        request = DeactivationRequest.from_wire(payload.get("request"))
        policy = agent.deactivation_policy(request)
        if not isinstance(policy, DeactivationPolicy):
            raise HostError(f"{agent.__class__.__name__}.deactivation_policy() must return DeactivationPolicy")
        agent.on_deactivating(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=facade.name,
                host_address=facade.address,
                reason=request.reason,
                request=request,
                policy=policy,
            )
        )
        agent.resources.cleanup(reason="deactivate")
        snapshot = _agent_snapshot(agent)
        snapshot["policy"] = policy.to_wire()
        return snapshot
    if op == "dispose_prepare":
        agent.on_disposing(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=facade.name,
                host_address=facade.address,
                reason=str(payload.get("reason") or "dispose"),
            )
        )
        agent.resources.cleanup(reason="dispose")
        return _agent_snapshot(agent)
    if op == "shutdown":
        facade._terminal = True
        return _agent_snapshot(agent)
    raise HostError(f"Unknown child operation {op!r}")


def _run_lifecycle(agent: Paglet, facade: _ChildHostFacade, name: str, payload: dict[str, Any]) -> None:
    if name == "creation":
        agent.on_creation(
            CreationEvent(
                agent_id=agent.agent_id,
                host_name=str(payload["host_name"]),
                host_address=str(payload["host_address"]),
                init=payload.get("init"),
            )
        )
        agent.run()
        return
    if name == "arrival":
        agent.on_arrival(_mobility_event(agent.agent_id, payload))
        _run_agent_async(agent, facade._endpoint)
        return
    if name == "clone":
        agent.on_clone(_clone_event(agent.agent_id, payload))
        agent.run()
        return
    if name == "activation":
        request = DeactivationRequest.from_wire(payload.get("request"))
        policy = DeactivationPolicy.from_wire(payload.get("policy"))
        agent.on_activation(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=str(payload["host_name"]),
                host_address=str(payload["host_address"]),
                reason=str(payload.get("reason") or "activate"),
                request=request,
                policy=policy,
            )
        )
        agent.run()
        return
    if name == "dispatching":
        agent.on_dispatching(_mobility_event(agent.agent_id, payload))
        return
    if name == "reverting":
        agent.on_reverting(_mobility_event(agent.agent_id, payload))
        return
    if name == "cloning":
        agent.on_cloning(_clone_event(agent.agent_id, payload))
        return
    if name == "cloned":
        agent.on_cloned(_clone_event(agent.agent_id, payload))
        return
    if name == "deactivating":
        request = DeactivationRequest.from_wire(payload.get("request"))
        policy = DeactivationPolicy.from_wire(payload.get("policy"))
        agent.on_deactivating(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=str(payload["host_name"]),
                host_address=str(payload["host_address"]),
                reason=str(payload.get("reason") or request.reason),
                request=request,
                policy=policy,
            )
        )
        return
    if name == "disposing":
        agent.on_disposing(
            PersistencyEvent(
                agent_id=agent.agent_id,
                host_name=str(payload["host_name"]),
                host_address=str(payload["host_address"]),
                reason=str(payload.get("reason") or "dispose"),
            )
        )
        return
    raise HostError(f"Unknown lifecycle {name!r}")


def _run_agent_async(agent: Paglet, endpoint: _ChildEndpoint) -> None:
    def run() -> None:
        try:
            agent.run()
        finally:
            with contextlib.suppress(Exception):
                endpoint._send({"type": "event", "event": "run_complete"})

    threading.Thread(target=run, name=f"paglets-run-{agent.agent_id[:8]}", daemon=True).start()
