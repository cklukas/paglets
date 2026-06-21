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
from typing import Any, Callable, Literal, TextIO

from .errors import HostError, SerializationError
from .serde import dataclass_from_wire, resolve_qualified_name


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
class LaunchConfig:
    """Parsed paglets launch config."""

    path: Path | None = None
    demo_config_id: str | None = None
    demo_config_version: str | None = None
    sync_demo_config: bool = True
    startup_agents: tuple[StartupAgentConfig, ...] = ()


@dataclass(frozen=True, slots=True)
class LaunchConfigSyncResult:
    """Result of syncing the bundled demo launch config to the user path."""

    action: Literal["copied", "updated", "unchanged", "skipped", "update-available"]
    path: Path
    message: str
    backup_path: Path | None = None


@dataclass(frozen=True, slots=True)
class ResolvedStartupAgent:
    agent_cls: type[Any]
    state: Any
    agent_id: str | None
    singleton: bool
    init: Any


def bundled_launch_config_text() -> str:
    return resources.files("paglets.defaults").joinpath("launch.toml").read_text(encoding="utf-8")


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
        return LaunchConfigSyncResult("skipped", config_path, "launch config sync disabled")

    if not config_path.exists():
        _write_launch_config(config_path, bundled_text)
        return LaunchConfigSyncResult("copied", config_path, f"copied bundled launch config to {config_path}")

    current_text = config_path.read_text(encoding="utf-8")
    current_payload = _load_toml(current_text, config_path)
    current_launch = _launch_table(current_payload)
    if not bool(current_launch.get("sync_demo_config", True)):
        return LaunchConfigSyncResult("skipped", config_path, "launch config disables bundled demo sync")

    current_id = str(current_launch.get("demo_config_id") or "")
    current_version = str(current_launch.get("demo_config_version") or "")
    if current_id == bundled_id and current_version == bundled_version:
        return LaunchConfigSyncResult("unchanged", config_path, "launch config is up to date")

    if yes:
        backup_path = _replace_launch_config(config_path, bundled_text)
        return LaunchConfigSyncResult("updated", config_path, f"updated launch config from bundled demo at {config_path}", backup_path)

    if interactive is None:
        interactive = sys.stdin.isatty()
    if not interactive:
        print(
            f"paglets host warning: bundled launch config {bundled_id} version {bundled_version} is available; "
            f"keeping existing {config_path}. Run paglets-host with --yes to update or --no-sync-launch-config to suppress.",
            file=out,
            flush=True,
        )
        return LaunchConfigSyncResult("update-available", config_path, "bundled launch config update available")

    answer = input_func(
        f"Bundled paglets launch config {bundled_id} version {bundled_version} is available. "
        f"Replace {config_path} and move the old file aside? [y/N] "
    ).strip().lower()
    if answer not in {"y", "yes"}:
        return LaunchConfigSyncResult("skipped", config_path, "launch config update declined")

    backup_path = _replace_launch_config(config_path, bundled_text)
    return LaunchConfigSyncResult("updated", config_path, f"updated launch config from bundled demo at {config_path}", backup_path)


def resolve_startup_agent(config: StartupAgentConfig) -> ResolvedStartupAgent:
    from .agent import Paglet

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

def _launch_config_from_payload(payload: dict[str, Any], path: Path) -> LaunchConfig:
    launch = _launch_table(payload)
    raw_agents = payload.get("startup_agents", [])
    if not isinstance(raw_agents, list):
        raise HostError(f"{path} startup_agents must be a list")
    return LaunchConfig(
        path=path,
        demo_config_id=str(launch["demo_config_id"]) if launch.get("demo_config_id") is not None else None,
        demo_config_version=str(launch["demo_config_version"]) if launch.get("demo_config_version") is not None else None,
        sync_demo_config=bool(launch.get("sync_demo_config", True)),
        startup_agents=tuple(_startup_agent_from_payload(item, path) for item in raw_agents),
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
