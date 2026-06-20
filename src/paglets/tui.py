from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
from typing import Any

from .admin import (
    DEFAULT_CONFIG_PATH,
    AgentDiscoveryConfig,
    AgentRecord,
    PagletsAdminClient,
    ServerRef,
    ServerStatus,
    TuiConfig,
    can_start_local_server,
    is_local_server_url,
    load_tui_config,
    normalize_discovery_path,
    normalize_server_url,
    parse_server_arg,
    remove_server_ref,
    save_tui_config,
    start_local_server,
    upsert_server_ref,
)
from .discovery import AgentClassRecord, discover_agent_classes


ALL_SERVERS_KEY = "__all__"


def _load_textual_runtime() -> dict[str, Any]:
    from textual import work
    from textual.app import App, ComposeResult
    from textual.containers import Horizontal, Vertical
    from textual.screen import ModalScreen
    from textual.widgets import Button, DataTable, Footer, Header, Input, Select, Static

    return {
        "App": App,
        "Button": Button,
        "ComposeResult": ComposeResult,
        "DataTable": DataTable,
        "Footer": Footer,
        "Header": Header,
        "Horizontal": Horizontal,
        "Input": Input,
        "ModalScreen": ModalScreen,
        "Select": Select,
        "Static": Static,
        "Vertical": Vertical,
        "work": work,
    }


def _json_or_empty(value: str, default: Any) -> Any:
    if not value.strip():
        return default
    return json.loads(value)


def _row_key_value(row_key: Any) -> str:
    return str(getattr(row_key, "value", row_key))


def create_tui_app(
    servers: list[ServerRef],
    *,
    config_path: Path | str = DEFAULT_CONFIG_PATH,
    refresh_interval: float = 2.0,
    admin_client: PagletsAdminClient | None = None,
    agent_discovery: AgentDiscoveryConfig | None = None,
    discovery_base_path: Path | str | None = None,
) -> Any:
    runtime = _load_textual_runtime()
    App = runtime["App"]
    Button = runtime["Button"]
    DataTable = runtime["DataTable"]
    Footer = runtime["Footer"]
    Header = runtime["Header"]
    Horizontal = runtime["Horizontal"]
    Input = runtime["Input"]
    ModalScreen = runtime["ModalScreen"]
    Select = runtime["Select"]
    Static = runtime["Static"]
    Vertical = runtime["Vertical"]
    work = runtime["work"]

    class TextForm(ModalScreen):
        def __init__(self, title: str, fields: list[tuple[str, str, str]], submit_label: str = "Submit"):
            super().__init__()
            self.title = title
            self.fields = fields
            self.submit_label = submit_label

        def compose(self):
            yield Vertical(
                Static(self.title, classes="dialog-title"),
                *[
                    Input(value=default, placeholder=label, id=field_id)
                    for field_id, label, default in self.fields
                ],
                Horizontal(
                    Button(self.submit_label, id="submit", variant="primary"),
                    Button("Cancel", id="cancel"),
                    classes="dialog-buttons",
                ),
                classes="dialog",
            )

        def on_button_pressed(self, event):
            if event.button.id == "cancel":
                self.dismiss(None)
                return
            if event.button.id == "submit":
                values = {
                    field_id: self.query_one(f"#{field_id}", Input).value
                    for field_id, _, _ in self.fields
                }
                self.dismiss(values)

    class ConfirmScreen(ModalScreen):
        def __init__(self, message: str):
            super().__init__()
            self.message = message

        def compose(self):
            yield Vertical(
                Static(self.message, classes="dialog-title"),
                Horizontal(
                    Button("Confirm", id="confirm", variant="error"),
                    Button("Cancel", id="cancel"),
                    classes="dialog-buttons",
                ),
                classes="dialog",
            )

        def on_button_pressed(self, event):
            self.dismiss(event.button.id == "confirm")

    class CreateAgentForm(ModalScreen):
        MANUAL = "__manual__"

        def __init__(
            self,
            agent_classes: list[AgentClassRecord],
            *,
            default_server: str,
            note: str,
        ):
            super().__init__()
            self.agent_classes = agent_classes
            self.records_by_class = {record.class_name: record for record in agent_classes}
            self.default_server = default_server
            self.note = note

        def compose(self):
            options = [("Manual entry", self.MANUAL)]
            options.extend(
                (self.option_label(record), record.class_name)
                for record in self.agent_classes
            )
            initial = self.agent_classes[0].class_name if self.agent_classes else self.MANUAL
            yield Vertical(
                Static("Create paglet", classes="dialog-title"),
                Input(value=self.default_server, placeholder="Server name or URL", id="server"),
                Select(options, value=initial, allow_blank=False, id="agent_choice"),
                Input(value="", placeholder="Agent class module:qualname", id="agent_class"),
                Input(value="", placeholder="State class module:qualname", id="state_class"),
                Input(value="{}", placeholder="State JSON", id="state"),
                Input(value="", placeholder="Init JSON optional", id="init"),
                Static(self.note, id="create-note"),
                Horizontal(
                    Button("Create", id="submit", variant="primary"),
                    Button("Cancel", id="cancel"),
                    classes="dialog-buttons",
                ),
                classes="dialog",
            )

        def on_mount(self):
            choice = self.query_one("#agent_choice", Select).value
            self.apply_selection(str(choice))

        def on_select_changed(self, event):
            if event.select.id == "agent_choice":
                self.apply_selection(str(event.value))

        def on_button_pressed(self, event):
            if event.button.id == "cancel":
                self.dismiss(None)
                return
            if event.button.id == "submit":
                values = {
                    field_id: self.query_one(f"#{field_id}", Input).value
                    for field_id in ("server", "agent_class", "state_class", "state", "init")
                }
                self.dismiss(values)

        def apply_selection(self, class_name: str) -> None:
            record = self.records_by_class.get(class_name)
            note = self.note
            if record is None:
                if not self.agent_classes:
                    note = "No discovered paglet classes. Enter module:qualname values manually."
                else:
                    note = "Manual entry. The target server must already import this module name."
                self.query_one("#create-note", Static).update(note)
                return
            self.query_one("#agent_class", Input).value = record.class_name
            self.query_one("#state_class", Input).value = record.state_class_name
            self.query_one("#state", Input).value = json.dumps(record.state_template, sort_keys=True)
            details = [self.note]
            if record.description:
                details.append(record.description)
            if record.required_state_fields:
                details.append("Required state fields: " + ", ".join(record.required_state_fields))
            self.query_one("#create-note", Static).update("\n".join(details))

        @staticmethod
        def option_label(record: AgentClassRecord) -> str:
            description = f" - {record.description}" if record.description else ""
            return f"{record.display_name} ({record.class_name}){description}"

    class PagletsTUI(App):
        CSS = """
        Screen {
            layout: vertical;
        }
        #body {
            height: 1fr;
        }
        #left {
            width: 34;
            border: solid $accent;
        }
        #center {
            width: 1fr;
            border: solid $accent;
        }
        #right {
            width: 48;
            border: solid $accent;
        }
        #warning {
            height: auto;
            color: $warning;
            padding: 0 1;
        }
        #server-table, #agent-table {
            height: 1fr;
        }
        #detail {
            height: 1fr;
            padding: 1;
            overflow-y: auto;
        }
        .dialog {
            width: 82;
            height: auto;
            border: thick $accent;
            background: $surface;
            padding: 1 2;
        }
        .dialog-title {
            text-style: bold;
            margin-bottom: 1;
        }
        .dialog-buttons {
            height: auto;
            margin-top: 1;
        }
        .server-actions {
            height: auto;
            padding: 0 1 1 1;
        }
        #start-server {
            width: 1fr;
        }
        """

        BINDINGS = [
            ("r", "refresh", "Refresh"),
            ("p", "start_server", "Start"),
            ("a", "add_server", "Add"),
            ("e", "edit_server", "Edit"),
            ("x", "remove_server", "Remove"),
            ("g", "add_discovery_source", "AddSrc"),
            ("y", "remove_discovery_source", "RmSrc"),
            ("s", "inspect_state", "State"),
            ("m", "message_agent", "Message"),
            ("c", "create_agent", "Create"),
            ("d", "dispatch_agent", "Dispatch"),
            ("l", "clone_agent", "Clone"),
            ("t", "retract_agent", "Retract"),
            ("v", "toggle_active", "Activate/Deactivate"),
            ("delete", "dispose_agent", "Dispose"),
            ("q", "quit", "Quit"),
        ]

        def __init__(
            self,
            server_refs: list[ServerRef],
            config_file: Path,
            interval: float,
            client: PagletsAdminClient | None,
            discovery_config: AgentDiscoveryConfig,
            discovery_base: Path,
        ):
            super().__init__()
            self.config_path = config_file
            self.servers = list(server_refs)
            self.admin = client or PagletsAdminClient(self.servers)
            self.agent_discovery = discovery_config
            self.discovery_base_path = discovery_base
            self.agent_classes: list[AgentClassRecord] = []
            self.discovery_errors: list[str] = []
            self.refresh_interval = interval
            self.server_statuses = {}
            self.agent_cache: dict[str, list[AgentRecord]] = {}
            self.host_cache = {}
            self.agent_rows: dict[str, AgentRecord] = {}
            self.selected_server = ALL_SERVERS_KEY
            self.selected_agent_key: str | None = None
            self.last_result = "Ready."
            self.started_processes: list[Any] = []
            self.refresh_agent_discovery()

        def compose(self):
            yield Header()
            yield Horizontal(
                Vertical(
                    Static("", id="warning"),
                    DataTable(id="server-table"),
                    Horizontal(
                        Button("Start", id="start-server", variant="success"),
                        classes="server-actions",
                    ),
                    id="left",
                ),
                Vertical(DataTable(id="agent-table"), id="center"),
                Vertical(Static("", id="detail"), id="right"),
                id="body",
            )
            yield Footer()

        def refresh_agent_discovery(self) -> None:
            result = discover_agent_classes(self.agent_discovery)
            self.agent_classes = result.agent_classes
            self.discovery_errors = result.errors

        def save_current_config(self) -> None:
            save_tui_config(
                TuiConfig(servers=self.servers, agent_discovery=self.agent_discovery),
                self.config_path,
            )

        async def on_mount(self):
            server_table = self.query_one("#server-table", DataTable)
            server_table.cursor_type = "row"
            server_table.add_columns("Server", "Status", "Active", "Inactive", "Latency")
            agent_table = self.query_one("#agent-table", DataTable)
            agent_table.cursor_type = "row"
            agent_table.add_columns("Server", "State", "Class", "Agent ID")
            self.set_interval(self.refresh_interval, self.action_refresh)
            await self.refresh_data()

        async def action_refresh(self):
            await self.refresh_data()

        async def refresh_data(self):
            self.admin.set_servers(self.servers)
            statuses = self.admin.health_all()
            self.server_statuses = {status.name: status for status in statuses}
            for server in self.admin.enabled_servers():
                status = self.server_statuses.get(server.name)
                if status is not None and not status.reachable:
                    continue
                try:
                    self.agent_cache[server.name] = self.admin.list_agents(server)
                    self.host_cache[server.name] = self.admin.list_hosts(server)
                except Exception as exc:
                    self.server_statuses[server.name] = self.server_statuses.get(server.name) or ServerStatus(
                        name=server.name,
                        url=server.url,
                        reachable=False,
                        latency_ms=0,
                        active_count=0,
                        inactive_count=0,
                        error=str(exc),
                    )
            self.render_servers()
            self.render_agents()
            self.render_detail()

        def render_servers(self):
            table = self.query_one("#server-table", DataTable)
            table.clear()
            active_total = sum(status.active_count for status in self.server_statuses.values())
            inactive_total = sum(status.inactive_count for status in self.server_statuses.values())
            table.add_row("All", "mixed", str(active_total), str(inactive_total), "", key=ALL_SERVERS_KEY)
            for server in self.servers:
                status = self.server_statuses.get(server.name)
                if not server.enabled:
                    row = (server.name, "disabled", "-", "-", "")
                elif status is None:
                    row = (server.name, "unknown", "-", "-", "")
                elif status.reachable:
                    row = (
                        server.name,
                        "up",
                        str(status.active_count),
                        str(status.inactive_count),
                        f"{status.latency_ms:.0f}ms",
                    )
                else:
                    state = "down/start" if self.server_start_available(server) else "down"
                    row = (server.name, state, "-", "-", f"{status.latency_ms:.0f}ms")
                table.add_row(*row, key=server.name)

            warning = self.query_one("#warning", Static)
            non_local = [
                server.url
                for server in self.servers
                if server.enabled and not is_local_server_url(server.url)
            ]
            warning.update("Warning: non-local unauthenticated server configured" if non_local else "")
            self.update_start_button()

        def render_agents(self):
            table = self.query_one("#agent-table", DataTable)
            table.clear()
            self.agent_rows.clear()
            servers = [self.selected_server] if self.selected_server != ALL_SERVERS_KEY else list(self.agent_cache)
            for server_name in servers:
                for agent in self.agent_cache.get(server_name, []):
                    row_key = f"{agent.server_name}:{agent.agent_id}"
                    self.agent_rows[row_key] = agent
                    state = "active" if agent.active else "inactive"
                    class_tail = agent.class_name.split(":")[-1]
                    table.add_row(agent.server_name, state, class_tail, agent.agent_id[:12], key=row_key)

        def render_detail(self):
            detail = self.query_one("#detail", Static)
            agent = self.selected_agent()
            lines = []
            if agent is None:
                server = self.selected_server_ref()
                if server is None:
                    lines.append("No agent selected.")
                    lines.extend(["", *self.discovery_detail_lines()])
                else:
                    status = self.server_statuses.get(server.name)
                    lines.extend(
                        [
                            f"Server: {server.name}",
                            f"URL: {server.url}",
                            f"Enabled: {server.enabled}",
                            f"Local start: {can_start_local_server(server)}",
                        ]
                    )
                    if status is None:
                        lines.append("Status: unknown")
                    elif status.reachable:
                        lines.extend(
                            [
                                "Status: up",
                                f"Active agents: {status.active_count}",
                                f"Inactive agents: {status.inactive_count}",
                                f"Latency: {status.latency_ms:.0f}ms",
                                f"Code version: {status.code_version or '-'}",
                            ]
                        )
                        peers = self.host_cache.get(server.name, [])
                        if peers:
                            lines.append("Mesh peers:")
                            for peer in peers:
                                state = "online" if peer.online else "offline"
                                lines.append(
                                    f"  {peer.name}: {state}, active={peer.active_count}, "
                                    f"inactive={peer.inactive_count}, {peer.url}"
                                )
                                if peer.error:
                                    lines.append(f"    error: {peer.error}")
                        mismatches = [
                            other
                            for other in self.server_statuses.values()
                            if other.name != server.name
                            and other.reachable
                            and status.code_version
                            and other.code_version
                            and other.code_version != status.code_version
                        ]
                        if mismatches:
                            lines.append("Configured version mismatches:")
                            lines.extend(
                                f"  {other.name}: {other.code_version} at {other.url}"
                                for other in mismatches
                            )
                    else:
                        lines.extend(["Status: down", f"Error: {status.error}"])
                        if self.server_start_available(server):
                            lines.append("Start available: press p or use the Start button.")
            else:
                lines.extend(
                    [
                        f"Server: {agent.server_name}",
                        f"Host URL: {agent.host_url}",
                        f"Agent ID: {agent.agent_id}",
                        f"Active: {agent.active}",
                        f"Class: {agent.class_name}",
                        f"State class: {agent.state_class_name}",
                    ]
                )
            if self.last_result:
                lines.extend(["", "Last result:", self.last_result])
            detail.update("\n".join(lines))
            self.update_start_button()

        def discovery_detail_lines(self) -> list[str]:
            lines = [
                f"Discovered paglet classes: {len(self.agent_classes)}",
                "Discovery paths:",
            ]
            lines.extend(f"  {path}" for path in self.agent_discovery.paths)
            if not self.agent_discovery.paths:
                lines.append("  -")
            lines.append("Discovery modules:")
            lines.extend(f"  {module}" for module in self.agent_discovery.modules)
            if not self.agent_discovery.modules:
                lines.append("  -")
            if self.discovery_errors:
                lines.append("Discovery errors:")
                lines.extend(f"  {error}" for error in self.discovery_errors[:8])
                if len(self.discovery_errors) > 8:
                    lines.append(f"  ... {len(self.discovery_errors) - 8} more")
            return lines

        def selected_agent(self) -> AgentRecord | None:
            if self.selected_agent_key is None:
                return None
            return self.agent_rows.get(self.selected_agent_key)

        def selected_server_ref(self) -> ServerRef | None:
            if self.selected_server == ALL_SERVERS_KEY:
                return None
            for server in self.servers:
                if server.name == self.selected_server:
                    return server
            return None

        def server_start_available(self, server: ServerRef) -> bool:
            if not server.enabled or not can_start_local_server(server):
                return False
            status = self.server_statuses.get(server.name)
            return status is not None and not status.reachable

        def update_start_button(self) -> None:
            try:
                button = self.query_one("#start-server", Button)
            except Exception:
                return
            server = self.selected_server_ref()
            button.disabled = server is None or not self.server_start_available(server)

        def on_data_table_row_selected(self, event):
            row_key = _row_key_value(event.row_key)
            if event.data_table.id == "server-table":
                self.selected_server = row_key
                self.selected_agent_key = None
                self.render_agents()
                self.render_detail()
            elif event.data_table.id == "agent-table":
                self.selected_agent_key = row_key
                self.render_detail()

        async def on_button_pressed(self, event):
            if event.button.id == "start-server":
                event.stop()
                await self.action_start_server()

        async def action_start_server(self):
            server = self.selected_server_ref()
            if server is None:
                self.last_result = "Select a concrete server to start."
                self.render_detail()
                return
            if not can_start_local_server(server):
                self.last_result = "Only localhost, 127.0.0.1, and ::1 servers can be started locally."
                self.render_detail()
                return
            status = self.server_statuses.get(server.name)
            if status is not None and status.reachable:
                self.last_result = f"Server {server.name} is already online."
                self.render_detail()
                return
            try:
                peers = [
                    candidate.url
                    for candidate in self.servers
                    if candidate.enabled and candidate.name != server.name
                ]
                process = start_local_server(server, peers=peers)
                self.started_processes.append(process)
                updated = ServerRef(server.name, normalize_server_url(server.url), True, True)
                self.servers = upsert_server_ref(self.servers, updated)
                self.save_current_config()
                self.selected_server = updated.name
                pid = getattr(process, "pid", None)
                self.last_result = f"Started local server {server.name}" + (
                    f" with PID {pid}." if pid else "."
                )
                await asyncio.sleep(0.25)
            except Exception as exc:
                self.last_result = f"Error starting server: {exc}"
            await self.refresh_data()

        @work(group="dialogs", exclusive=True)
        async def action_add_server(self):
            values = await self.push_screen_wait(
                TextForm(
                    "Add server",
                    [
                        ("name", "Name", ""),
                        ("url", "URL", "http://127.0.0.1:8765"),
                    ],
                    "Add",
                )
            )
            if not values:
                return
            await self.apply_server(ServerRef(values["name"].strip(), normalize_server_url(values["url"])))

        @work(group="dialogs", exclusive=True)
        async def action_edit_server(self):
            server = self.selected_server_ref()
            if server is None:
                self.last_result = "Select a concrete server to edit."
                self.render_detail()
                return
            values = await self.push_screen_wait(
                TextForm(
                    f"Edit server {server.name}",
                    [
                        ("name", "Name", server.name),
                        ("url", "URL", server.url),
                        ("enabled", "Enabled true/false", str(server.enabled).lower()),
                    ],
                    "Save",
                )
            )
            if not values:
                return
            enabled = values["enabled"].strip().lower() not in {"0", "false", "no", "off"}
            updated = ServerRef(values["name"].strip(), normalize_server_url(values["url"]), enabled)
            self.servers = remove_server_ref(self.servers, server.name)
            await self.apply_server(updated)

        async def apply_server(self, server: ServerRef):
            self.servers = upsert_server_ref(self.servers, server)
            self.save_current_config()
            self.selected_server = server.name
            self.last_result = f"Saved server {server.name}."
            await self.refresh_data()

        async def action_remove_server(self):
            server = self.selected_server_ref()
            if server is None:
                self.last_result = "Select a concrete server to remove."
                self.render_detail()
                return
            self.servers = remove_server_ref(self.servers, server.name)
            self.save_current_config()
            self.agent_cache.pop(server.name, None)
            self.selected_server = ALL_SERVERS_KEY
            self.last_result = f"Removed server {server.name}."
            await self.refresh_data()

        @work(group="dialogs", exclusive=True)
        async def action_add_discovery_source(self):
            values = await self.push_screen_wait(
                TextForm(
                    "Add discovery source",
                    [
                        ("kind", "Kind path/module", "path"),
                        ("value", "Path or module", ""),
                    ],
                    "Add",
                )
            )
            if not values:
                return
            kind = values["kind"].strip().lower()
            value = values["value"].strip()
            try:
                if kind.startswith("m"):
                    if not value:
                        raise ValueError("Module name cannot be empty")
                    if value not in self.agent_discovery.modules:
                        self.agent_discovery.modules.append(value)
                    self.last_result = f"Added discovery module {value}."
                else:
                    path = normalize_discovery_path(value, base_path=self.discovery_base_path)
                    if path not in self.agent_discovery.paths:
                        self.agent_discovery.paths.append(path)
                    self.last_result = f"Added discovery path {path}."
                self.refresh_agent_discovery()
                self.save_current_config()
            except Exception as exc:
                self.last_result = f"Error: {exc}"
            self.render_detail()

        @work(group="dialogs", exclusive=True)
        async def action_remove_discovery_source(self):
            values = await self.push_screen_wait(
                TextForm(
                    "Remove discovery source",
                    [
                        ("kind", "Kind path/module", "path"),
                        ("value", "Path or module", ""),
                    ],
                    "Remove",
                )
            )
            if not values:
                return
            kind = values["kind"].strip().lower()
            value = values["value"].strip()
            try:
                if kind.startswith("m"):
                    self.agent_discovery.modules = [
                        module for module in self.agent_discovery.modules if module != value
                    ]
                    self.last_result = f"Removed discovery module {value}."
                else:
                    path = normalize_discovery_path(value, base_path=self.discovery_base_path)
                    self.agent_discovery.paths = [
                        existing for existing in self.agent_discovery.paths if existing != path
                    ]
                    self.last_result = f"Removed discovery path {path}."
                self.refresh_agent_discovery()
                self.save_current_config()
            except Exception as exc:
                self.last_result = f"Error: {exc}"
            self.render_detail()

        async def action_inspect_state(self):
            agent = self.selected_agent()
            if agent is None:
                self.last_result = "Select an agent first."
            else:
                try:
                    self.last_result = json.dumps(self.admin.get_agent_state(agent), indent=2, sort_keys=True)
                except Exception as exc:
                    self.last_result = f"Error: {exc}"
            self.render_detail()

        @work(group="dialogs", exclusive=True)
        async def action_message_agent(self):
            agent = self.selected_agent()
            if agent is None:
                self.last_result = "Select an agent first."
                self.render_detail()
                return
            values = await self.push_screen_wait(
                TextForm(
                    "Send message",
                    [
                        ("kind", "Kind", ""),
                        ("args", "Args JSON", "{}"),
                        ("arg", "Single arg JSON optional", ""),
                        ("oneway", "Oneway true/false", "false"),
                    ],
                    "Send",
                )
            )
            if not values:
                return
            try:
                result = self.admin.send_message(
                    agent,
                    values["kind"].strip(),
                    _json_or_empty(values["args"], {}),
                    arg=_json_or_empty(values["arg"], None),
                    oneway=values["oneway"].strip().lower() in {"1", "true", "yes", "on"},
                )
                self.last_result = json.dumps(result, indent=2, sort_keys=True)
            except Exception as exc:
                self.last_result = f"Error: {exc}"
            await self.refresh_data()

        @work(group="dialogs", exclusive=True)
        async def action_create_agent(self):
            self.refresh_agent_discovery()
            default_server = self.selected_server_ref() or (self.servers[0] if self.servers else None)
            note = "The target server must already have the selected module importable."
            if self.discovery_errors:
                note += f" Discovery reported {len(self.discovery_errors)} error(s)."
            values = await self.push_screen_wait(
                CreateAgentForm(
                    self.agent_classes,
                    default_server=default_server.name if default_server else "",
                    note=note,
                )
            )
            if not values:
                return
            try:
                server = self.admin.get_server(values["server"])
                proxy = self.admin.create_agent(
                    server,
                    values["agent_class"].strip(),
                    values["state_class"].strip(),
                    _json_or_empty(values["state"], {}),
                    init=_json_or_empty(values["init"], None),
                )
                self.last_result = json.dumps(proxy, indent=2, sort_keys=True)
            except Exception as exc:
                self.last_result = f"Error: {exc}"
            await self.refresh_data()

        @work(group="dialogs", exclusive=True)
        async def action_dispatch_agent(self):
            await self.agent_target_action("dispatch", "Dispatch target server name or URL")

        @work(group="dialogs", exclusive=True)
        async def action_clone_agent(self):
            await self.agent_target_action("clone", "Clone target server name or URL")

        @work(group="dialogs", exclusive=True)
        async def action_retract_agent(self):
            await self.agent_target_action("retract", "Retract destination server name or URL")

        async def agent_target_action(self, action: str, title: str):
            agent = self.selected_agent()
            if agent is None:
                self.last_result = "Select an agent first."
                self.render_detail()
                return
            default = next((server.url for server in self.servers if server.name != agent.server_name), agent.host_url)
            values = await self.push_screen_wait(TextForm(title, [("target", "Target", default)], action.title()))
            if not values:
                return
            try:
                target = self.admin.get_server(values["target"]).url
                if action == "dispatch":
                    result = self.admin.dispatch(agent, target)
                elif action == "clone":
                    result = self.admin.clone(agent, target)
                else:
                    result = self.admin.retract(agent, target)
                self.last_result = json.dumps(result, indent=2, sort_keys=True)
            except Exception as exc:
                self.last_result = f"Error: {exc}"
            await self.refresh_data()

        async def action_toggle_active(self):
            agent = self.selected_agent()
            if agent is None:
                self.last_result = "Select an agent first."
            else:
                try:
                    if agent.active:
                        self.admin.deactivate(agent)
                        self.last_result = "Agent deactivated."
                    else:
                        self.last_result = json.dumps(self.admin.activate(agent), indent=2, sort_keys=True)
                except Exception as exc:
                    self.last_result = f"Error: {exc}"
            await self.refresh_data()

        @work(group="dialogs", exclusive=True)
        async def action_dispose_agent(self):
            agent = self.selected_agent()
            if agent is None:
                self.last_result = "Select an agent first."
                self.render_detail()
                return
            confirmed = await self.push_screen_wait(ConfirmScreen(f"Dispose {agent.agent_id}?"))
            if not confirmed:
                return
            try:
                self.admin.dispose(agent)
                self.last_result = "Agent disposed."
                self.selected_agent_key = None
            except Exception as exc:
                self.last_result = f"Error: {exc}"
            await self.refresh_data()

    return PagletsTUI(
        list(servers),
        Path(config_path),
        refresh_interval,
        admin_client,
        agent_discovery or AgentDiscoveryConfig(paths=[], modules=[]),
        Path.cwd() if discovery_base_path is None else Path(discovery_base_path),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the paglets multi-server TUI")
    parser.add_argument("--server", action="append", default=[], help="Server in NAME=URL format")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Server config path")
    parser.add_argument("--refresh", type=float, default=2.0, help="Refresh interval in seconds")
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    try:
        config = load_tui_config(config_path, base_path=Path.cwd())
        for server_arg in args.server:
            config.servers = upsert_server_ref(config.servers, parse_server_arg(server_arg))
        app = create_tui_app(
            config.servers,
            config_path=config_path,
            refresh_interval=args.refresh,
            agent_discovery=config.agent_discovery,
            discovery_base_path=Path.cwd(),
        )
        if args.server:
            save_tui_config(config, config_path)
    except ImportError:
        print(
            "Textual is required for paglets-tui. Install with: "
            "uv run --extra tui paglets-tui",
            file=sys.stderr,
        )
        return 2
    except Exception as exc:
        print(f"paglets-tui: {exc}", file=sys.stderr)
        return 1

    app.run()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
