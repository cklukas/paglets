from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
import shutil
import time
from typing import Any

from paglets import Message, Paglet, PagletState

try:
    from .support import local_hosts
except ImportError:  # pragma: no cover - direct script execution
    from support import local_hosts


@dataclass
class DiskSurveyState(PagletState):
    role: str = "parent"
    parent_host_url: str = ""
    parent_agent_id: str = ""
    target_host_name: str = ""
    target_host_url: str = ""
    pending_hosts: list[str] = field(default_factory=list)
    child_proxies: dict[str, dict[str, str]] = field(default_factory=dict)
    findings: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    diagnostics: list[str] = field(default_factory=list)
    parent_wait_timeout: float = 5.0


class DiskSurveyPaglet(Paglet[DiskSurveyState]):
    """Clone to mesh-discovered hosts and report local volume disk usage back to the parent."""

    State = DiskSurveyState

    def run(self):
        if self.state.role != "child":
            return
        volumes = collect_volume_usage()
        try:
            self.context.wait_for_host(
                self.state.parent_host_url,
                timeout=self.state.parent_wait_timeout,
                interval=0.1,
            )
        except Exception:
            return
        parent = self.context.get_proxy(self.state.parent_agent_id, self.state.parent_host_url)
        if parent is None:
            return
        parent.send_message(
            "child_result",
            {
                "host_name": self.state.target_host_name,
                "host_url": self.state.target_host_url,
                "child_agent_id": self.agent_id,
                "volumes": volumes,
            },
        )

    def handle_message(self, message: Message):
        if message.kind == "survey":
            timeout = float(message.args.get("timeout", 5.0))
            return self.start_survey(timeout)
        if message.kind == "child_result":
            return self.record_child_result(message.args)
        if message.kind == "summary":
            return self.summary()
        return self.not_handled()

    def start_survey(self, timeout: float) -> dict[str, Any]:
        all_hosts = self.context.available_hosts(online_only=False, include_self=True)
        hosts = [host for host in all_hosts if host.online]
        self.state.role = "parent"
        self.state.parent_host_url = self.context.address
        self.state.parent_agent_id = self.agent_id
        self.state.parent_wait_timeout = timeout
        self.state.pending_hosts = []
        self.state.child_proxies = {}
        self.state.findings = {}
        self.state.errors = {}
        self.state.diagnostics = [
            f"{self.context.name} mesh knows {len(all_hosts)} same-version host(s); {len(hosts)} online",
        ]
        for host in all_hosts:
            state = "online" if host.online else "offline"
            suffix = f" ({host.error})" if host.error else ""
            self.state.diagnostics.append(f"mesh host {host.name} at {host.url}: {state}{suffix}")

        for host in hosts:
            host_name = host.name
            host_url = host.url
            self.state.pending_hosts.append(host_name)
            self.state.diagnostics.append(f"cloning child to {host_name} at {host_url}")
            try:
                self.state.role = "child"
                self.state.target_host_name = host_name
                self.state.target_host_url = host_url
                clone = self.clone_to(host_name)
                self.state.child_proxies[host_name] = clone.to_wire()
            except Exception as exc:
                self.state.errors[host_name] = str(exc)
                self.state.pending_hosts.remove(host_name)
                self.state.diagnostics.append(f"clone to {host_name} failed: {exc}")
            finally:
                self.state.role = "parent"
                self.state.target_host_name = ""
                self.state.target_host_url = ""

        deadline = time.monotonic() + timeout
        while self.state.pending_hosts and time.monotonic() < deadline:
            time.sleep(0.05)

        for host_name in list(self.state.pending_hosts):
            self.state.errors[host_name] = "timed out waiting for child result"
            self.state.diagnostics.append(f"child result from {host_name} timed out")
            self.state.pending_hosts.remove(host_name)

        return self.summary()

    def record_child_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        host_name = str(payload["host_name"])
        volumes = list(payload.get("volumes") or [])
        self.state.findings[host_name] = volumes
        self.state.pending_hosts = [host for host in self.state.pending_hosts if host != host_name]
        self.state.diagnostics.append(
            f"received {len(volumes)} volume finding(s) from {host_name} via child {payload.get('child_agent_id')}"
        )
        return {"ok": True}

    def summary(self) -> dict[str, Any]:
        return {
            "diagnostics": list(self.state.diagnostics),
            "findings": dict(self.state.findings),
            "errors": dict(self.state.errors),
            "children": dict(self.state.child_proxies),
        }


def collect_volume_usage() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in volume_paths():
        try:
            usage = shutil.disk_usage(path)
        except OSError:
            continue
        records.append(
            {
                "volume": str(path),
                "total_bytes": usage.total,
                "used_bytes": usage.used,
                "free_bytes": usage.free,
                "total_gb": round(usage.total / 1_000_000_000, 2),
                "used_gb": round(usage.used / 1_000_000_000, 2),
                "free_gb": round(usage.free / 1_000_000_000, 2),
            }
        )
    return records


def volume_paths() -> list[Path]:
    paths: set[Path] = {Path("/")}
    volumes_dir = Path("/Volumes")
    if volumes_dir.is_dir():
        paths.update(
            path
            for path in volumes_dir.iterdir()
            if path.is_dir()
            and not path.name.startswith(".")
            and not path.name.startswith("com.apple.")
        )
    proc_mounts = Path("/proc/mounts")
    if proc_mounts.exists():
        paths.update(_linux_mount_paths(proc_mounts))
    return sorted(paths, key=lambda path: str(path))


def _linux_mount_paths(proc_mounts: Path) -> set[Path]:
    pseudo_fs = {
        "autofs",
        "bpf",
        "cgroup",
        "cgroup2",
        "configfs",
        "debugfs",
        "devpts",
        "devtmpfs",
        "fusectl",
        "hugetlbfs",
        "mqueue",
        "proc",
        "pstore",
        "securityfs",
        "sysfs",
        "tracefs",
    }
    skipped_prefixes = ("/dev", "/proc", "/run", "/sys")
    paths: set[Path] = set()
    for line in proc_mounts.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        mount_path = parts[1].replace("\\040", " ")
        fs_type = parts[2]
        if fs_type in pseudo_fs or mount_path.startswith(skipped_prefixes):
            continue
        paths.add(Path(mount_path))
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Clone a paglet to known hosts and summarize disk usage")
    parser.add_argument("--hosts", nargs="+", default=["alpha", "beta", "gamma"], help="Local host names to start")
    parser.add_argument("--timeout", type=float, default=5.0, help="Seconds to wait for child clone results")
    args = parser.parse_args(argv)

    with local_hosts(*args.hosts, mesh=True, mesh_version="disk-survey-demo", mesh_multicast=False) as hosts:
        parent_host = hosts[0]
        parent = parent_host.create(DiskSurveyPaglet, DiskSurveyState())
        print(f"parent paglet {parent.agent_id} started on {parent_host.name} at {parent_host.address}")
        print(f"{parent_host.name} mesh registry:")
        for host_ref in parent_host.list_hosts(online_only=False, include_self=True):
            state = "online" if host_ref.online else "offline"
            print(f"  - {host_ref.name} -> {host_ref.url} ({state}, version {host_ref.code_version})")

        summary = parent.send_message("survey", {"timeout": args.timeout})
        print("\ndiagnostics:")
        for line in summary["diagnostics"]:
            print(f"  - {line}")

        print("\nfindings:")
        print(f"{'host':<12} {'volume':<42} {'size_gb':>10} {'used_gb':>10} {'free_gb':>10}")
        for host_name, volumes in sorted(summary["findings"].items()):
            for volume in volumes:
                print(
                    f"{host_name:<12} {volume['volume']:<42} "
                    f"{volume['total_gb']:>10.2f} {volume['used_gb']:>10.2f} {volume['free_gb']:>10.2f}"
                )

        if summary["errors"]:
            print("\nerrors:")
            for host_name, error in sorted(summary["errors"].items()):
                print(f"  - {host_name}: {error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
