# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from importlib import resources
from pathlib import Path
import shutil
import sys
import tomllib
from typing import Any, Callable, TextIO

from paglets.core.errors import HostError, SerializationError
from paglets.services.resident import ResidentServiceSpec
from paglets.core.runtime_values import (
    LaunchConfigSyncAction,
    ResidentLifecycle,
    ServiceScope,
    enum_from_wire,
    require_enum,
)
from paglets.serialization.serde import dataclass_from_wire, resolve_qualified_name


DEFAULT_LAUNCH_CONFIG_PATH = Path.home() / ".paglets" / "launch.toml"
DEFAULT_DEMO_CONFIG_ID = "paglets-default-launch"


@dataclass(frozen=True, slots=True)
class AutoStartSpec:
    """Class-level marker for agents that can be started from launch config."""

    alias: str
    agent_id: str | None = None
    singleton: bool = True
    state: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StartupAgentConfig:
    """One launch-config entry describing an agent to start."""

    use: str | None = None
    class_name: str | None = None
    enabled: bool = True
    agent_id: str | None = None
    singleton: bool = True
    state: dict[str, Any] = field(default_factory=dict)
    init: Any = None


@dataclass(frozen=True, slots=True)
class ResidentServiceConfig:
    """One launch-config entry describing a managed resident service."""

    use: str | None = None
    class_name: str | None = None
    service_name: str | None = None
    enabled: bool = True
    agent_id: str | None = None
    singleton: bool = True
    lifecycle: ResidentLifecycle | None = None
    scope: ServiceScope | None = None
    idle_timeout: float | None = None
    state: dict[str, Any] = field(default_factory=dict)
    init: Any = None

    def __post_init__(self) -> None:
        if self.lifecycle is not None:
            require_enum(self.lifecycle, ResidentLifecycle, "lifecycle")
        if self.scope is not None:
            require_enum(self.scope, ServiceScope, "scope")


@dataclass(frozen=True, slots=True)
class LaunchConfig:
    """Parsed paglets launch config."""

    path: Path | None = None
    demo_config_id: str | None = None
    demo_config_version: str | None = None
    sync_demo_config: bool = True
    startup_agents: tuple[StartupAgentConfig, ...] = ()
    resident_services: tuple[ResidentServiceConfig, ...] = ()


@dataclass(frozen=True, slots=True)
class LaunchConfigSyncResult:
    """Result of syncing the bundled demo launch config to the user path."""

    action: LaunchConfigSyncAction
    path: Path
    message: str
    backup_path: Path | None = None

    def __post_init__(self) -> None:
        require_enum(self.action, LaunchConfigSyncAction, "action")


@dataclass(frozen=True, slots=True)
class ResolvedStartupAgent:
    agent_cls: type[Any]
    state: Any
    agent_id: str | None
    singleton: bool
    init: Any


@dataclass(frozen=True, slots=True)
class ResolvedResidentService:
    agent_cls: type[Any]
    state: Any
    agent_id: str
    singleton: bool
    init: Any
    spec: ResidentServiceSpec
    lifecycle: ResidentLifecycle
    scope: ServiceScope
    idle_timeout: float


def bundled_launch_config_text() -> str:
    return resources.files("paglets.config.defaults").joinpath("launch.toml").read_text(encoding="utf-8")


def load_launch_config(path: Path | str = DEFAULT_LAUNCH_CONFIG_PATH) -> LaunchConfig:
    config_path = Path(path).expanduser()
    if not config_path.exists():
        return LaunchConfig(path=config_path)
    payload = _load_toml(config_path.read_text(encoding="utf-8"), config_path)
    return _launch_config_from_payload(payload, config_path)


def sync_launch_config(
    path: Path | str = DEFAULT_LAUNCH_CONFIG_PATH,
    *,
    enabled: bool = True,
    yes: bool = False,
    interactive: bool | None = None,
    input_func: Callable[[str], str] = input,
    output: TextIO | None = None,
) -> LaunchConfigSyncResult:
    config_path = Path(path).expanduser()
    out = output or sys.stderr
    bundled_text = bundled_launch_config_text()
    bundled_payload = _load_toml(bundled_text, None)
    bundled_launch = _launch_table(bundled_payload)
    bundled_id = str(bundled_launch.get("demo_config_id") or DEFAULT_DEMO_CONFIG_ID)
    bundled_version = str(bundled_launch.get("demo_config_version") or "")

    if not enabled:
        return LaunchConfigSyncResult(LaunchConfigSyncAction.SKIPPED, config_path, "launch config sync disabled")

    if not config_path.exists():
        _write_launch_config(config_path, bundled_text)
        return LaunchConfigSyncResult(
            LaunchConfigSyncAction.COPIED,
            config_path,
            f"copied bundled launch config to {config_path}",
        )

    current_text = config_path.read_text(encoding="utf-8")
    current_payload = _load_toml(current_text, config_path)
    current_launch = _launch_table(current_payload)
    if not bool(current_launch.get("sync_demo_config", True)):
        return LaunchConfigSyncResult(
            LaunchConfigSyncAction.SKIPPED,
            config_path,
            "launch config disables bundled demo sync",
        )

    current_id = str(current_launch.get("demo_config_id") or "")
    current_version = str(current_launch.get("demo_config_version") or "")
    if current_id == bundled_id and current_version == bundled_version:
        return LaunchConfigSyncResult(LaunchConfigSyncAction.UNCHANGED, config_path, "launch config is up to date")

    if yes:
        backup_path = _replace_launch_config(config_path, bundled_text)
        return LaunchConfigSyncResult(
            LaunchConfigSyncAction.UPDATED,
            config_path,
            f"updated launch config from bundled demo at {config_path}",
            backup_path,
        )

    if interactive is None:
        interactive = sys.stdin.isatty()
    if not interactive:
        print(
            f"paglets host warning: bundled launch config {bundled_id} version {bundled_version} is available; "
            f"keeping existing {config_path}. Run paglets-host with --yes to update or --no-sync-launch-config to suppress.",
            file=out,
            flush=True,
        )
        return LaunchConfigSyncResult(
            LaunchConfigSyncAction.UPDATE_AVAILABLE,
            config_path,
            "bundled launch config update available",
        )

    answer = input_func(
        f"Bundled paglets launch config {bundled_id} version {bundled_version} is available. "
        f"Replace {config_path} and move the old file aside? [y/N] "
    ).strip().lower()
    if answer not in {"y", "yes"}:
        return LaunchConfigSyncResult(LaunchConfigSyncAction.SKIPPED, config_path, "launch config update declined")

    backup_path = _replace_launch_config(config_path, bundled_text)
    return LaunchConfigSyncResult(
        LaunchConfigSyncAction.UPDATED,
        config_path,
        f"updated launch config from bundled demo at {config_path}",
        backup_path,
    )


def resolve_startup_agent(config: StartupAgentConfig) -> ResolvedStartupAgent:
    from paglets.core.agent import Paglet

    agent_cls: type[Paglet[Any]]
    spec: AutoStartSpec | None = None
    if config.use:
        raise HostError(f"Unknown startup agent alias {config.use!r}; use 'class' for importable paglet classes")
    elif config.class_name:
        resolved = resolve_qualified_name(config.class_name)
        if not isinstance(resolved, type) or not issubclass(resolved, Paglet):
            raise HostError(f"{config.class_name!r} is not a Paglet class")
        agent_cls = resolved
        maybe_spec = getattr(agent_cls, "AUTO_START", None)
        spec = maybe_spec if isinstance(maybe_spec, AutoStartSpec) else None
    else:
        raise HostError("startup agent entry must set 'use' or 'class'")

    state_payload: dict[str, Any] = {}
    if spec is not None:
        state_payload.update(spec.state)
    state_payload.update(config.state)
    state_cls = agent_cls.state_class()
    try:
        state = dataclass_from_wire(state_cls, state_payload)
    except SerializationError as exc:
        raise HostError(f"Could not build state for startup agent {agent_cls.__name__}") from exc
    agent_id = config.agent_id or (spec.agent_id if spec is not None else None)
    return ResolvedStartupAgent(
        agent_cls=agent_cls,
        state=state,
        agent_id=agent_id,
        singleton=config.singleton,
        init=config.init,
    )


def resolve_resident_service(config: ResidentServiceConfig) -> ResolvedResidentService:
    from paglets.core.agent import Paglet

    if config.use:
        raise HostError(f"Unknown resident service alias {config.use!r}; use 'class' for importable paglet classes")
    if not config.class_name:
        raise HostError("resident service entry must set 'class'")
    resolved = resolve_qualified_name(config.class_name)
    if not isinstance(resolved, type) or not issubclass(resolved, Paglet):
        raise HostError(f"{config.class_name!r} is not a Paglet class")
    agent_cls: type[Paglet[Any]] = resolved
    spec = _select_resident_service_spec(agent_cls, config)

    state_payload = dict(spec.state)
    state_payload.update(config.state)
    state_cls = agent_cls.state_class()
    try:
        state = dataclass_from_wire(state_cls, state_payload)
    except SerializationError as exc:
        raise HostError(f"Could not build state for resident service {agent_cls.__name__}") from exc

    lifecycle = config.lifecycle or spec.lifecycle
    require_enum(lifecycle, ResidentLifecycle, "lifecycle")
    scope = config.scope or spec.scope
    require_enum(scope, ServiceScope, "scope")
    idle_timeout = spec.idle_timeout if config.idle_timeout is None else config.idle_timeout
    if idle_timeout < 0:
        raise HostError("resident service idle_timeout must be non-negative")

    agent_id = config.agent_id or spec.agent_id or f"service.{spec.contract.name}"
    if not agent_id:
        raise HostError("resident service requires an agent_id")
    return ResolvedResidentService(
        agent_cls=agent_cls,
        state=state,
        agent_id=agent_id,
        singleton=config.singleton and spec.singleton,
        init=config.init,
        spec=spec,
        lifecycle=lifecycle,
        scope=scope,
        idle_timeout=float(idle_timeout),
    )


def _launch_config_from_payload(payload: dict[str, Any], path: Path) -> LaunchConfig:
    launch = _launch_table(payload)
    raw_agents = payload.get("startup_agents", [])
    if not isinstance(raw_agents, list):
        raise HostError(f"{path} startup_agents must be a list")
    raw_resident_services = payload.get("resident_services", [])
    if not isinstance(raw_resident_services, list):
        raise HostError(f"{path} resident_services must be a list")
    return LaunchConfig(
        path=path,
        demo_config_id=str(launch["demo_config_id"]) if launch.get("demo_config_id") is not None else None,
        demo_config_version=str(launch["demo_config_version"]) if launch.get("demo_config_version") is not None else None,
        sync_demo_config=bool(launch.get("sync_demo_config", True)),
        startup_agents=tuple(_startup_agent_from_payload(item, path) for item in raw_agents),
        resident_services=tuple(_resident_service_from_payload(item, path) for item in raw_resident_services),
    )


def _startup_agent_from_payload(payload: Any, path: Path) -> StartupAgentConfig:
    if not isinstance(payload, dict):
        raise HostError(f"{path} startup_agents entries must be tables")
    state = payload.get("state", {})
    if not isinstance(state, dict):
        raise HostError(f"{path} startup agent state must be an inline table or table")
    class_name = payload.get("class")
    return StartupAgentConfig(
        use=str(payload["use"]) if payload.get("use") is not None else None,
        class_name=str(class_name) if class_name is not None else None,
        enabled=bool(payload.get("enabled", True)),
        agent_id=str(payload["agent_id"]) if payload.get("agent_id") is not None else None,
        singleton=bool(payload.get("singleton", True)),
        state=dict(state),
        init=payload.get("init"),
    )


def _resident_service_from_payload(payload: Any, path: Path) -> ResidentServiceConfig:
    if not isinstance(payload, dict):
        raise HostError(f"{path} resident_services entries must be tables")
    state = payload.get("state", {})
    if not isinstance(state, dict):
        raise HostError(f"{path} resident service state must be an inline table or table")
    lifecycle = payload.get("lifecycle")
    scope = payload.get("scope")
    idle_timeout = payload.get("idle_timeout")
    class_name = payload.get("class")
    service_name = payload.get("service")
    return ResidentServiceConfig(
        use=str(payload["use"]) if payload.get("use") is not None else None,
        class_name=str(class_name) if class_name is not None else None,
        service_name=str(service_name) if service_name is not None else None,
        enabled=bool(payload.get("enabled", True)),
        agent_id=str(payload["agent_id"]) if payload.get("agent_id") is not None else None,
        singleton=bool(payload.get("singleton", True)),
        lifecycle=(
            _enum_from_config(lifecycle, ResidentLifecycle, "lifecycle", path)
            if lifecycle is not None
            else None
        ),
        scope=_enum_from_config(scope, ServiceScope, "scope", path) if scope is not None else None,
        idle_timeout=float(idle_timeout) if idle_timeout is not None else None,
        state=dict(state),
        init=payload.get("init"),
    )


def _select_resident_service_spec(agent_cls: type[Any], config: ResidentServiceConfig) -> ResidentServiceSpec:
    raw_specs = getattr(agent_cls, "RESIDENT_SERVICES", ())
    specs = tuple(spec for spec in raw_specs if isinstance(spec, ResidentServiceSpec))
    if not specs:
        raise HostError(f"{agent_cls.__name__} must declare RESIDENT_SERVICES for resident service launch config")
    if config.service_name is not None:
        for spec in specs:
            if spec.contract.name == config.service_name:
                return spec
        raise HostError(f"{agent_cls.__name__} does not declare resident service {config.service_name!r}")
    if len(specs) != 1:
        raise HostError(f"{agent_cls.__name__} declares multiple resident services; set service in launch config")
    return specs[0]


def _enum_from_config(value: Any, enum_cls: type[Any], field_name: str, path: Path) -> Any:
    try:
        return enum_from_wire(value, enum_cls, field_name)
    except (TypeError, ValueError) as exc:
        raise HostError(f"{path} resident service {field_name}: {exc}") from exc


def _load_toml(text: str, path: Path | None) -> dict[str, Any]:
    try:
        payload = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        location = f"{path}: " if path is not None else ""
        raise HostError(f"{location}invalid launch config TOML: {exc}") from exc
    if not isinstance(payload, dict):
        raise HostError(f"{path or 'bundled launch config'} must be a TOML table")
    return payload


def _launch_table(payload: dict[str, Any]) -> dict[str, Any]:
    launch = payload.get("launch", {})
    if launch is None:
        return {}
    if not isinstance(launch, dict):
        raise HostError("launch config [launch] must be a table")
    return launch


def _write_launch_config(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _replace_launch_config(path: Path, text: str) -> Path:
    backup_path = _backup_path(path)
    path.replace(backup_path)
    _write_launch_config(path, text)
    return backup_path


def _backup_path(path: Path) -> Path:
    candidate = path.with_name(f"{path.name}.old")
    if not candidate.exists():
        return candidate
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return path.with_name(f"{path.name}.old-{stamp}")
