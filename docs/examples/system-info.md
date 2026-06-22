# Server Info

`server-info` demonstrates the resident-service pattern:

1. A host declares `ServerInfoAgent` from launch config.
2. The host advertises a typed service contract named `server-info` before the
   provider is active.
3. A caller discovers that contract locally or across the mesh.
4. The first call starts or activates the provider agent.
5. Requests are ordinary paglet messages, but payloads and replies are typed
   dataclasses.
6. The `paglets-sysinfo` CLI creates a short-lived collector paglet that clones
   to mesh hosts, calls each host's local `server-info` service, polls `drain`,
   and prints an aggregate result.
7. After the idle timeout, the provider deactivates while the service remains
   discoverable for later calls.

### Contract And Operations

Import the example contract explicitly from the example package:

```python
from paglets.examples.system_info import (
    GET_DISK,
    GET_LOAD,
    GET_SUMMARY,
    LIST_PROCESSES,
    SERVER_INFO,
    DiskRequest,
    LoadRequest,
    ProcessListRequest,
)
```

The contract has four operations:

| Operation | Request | Reply | Purpose |
| --- | --- | --- | --- |
| `GET_LOAD` / `load` | `LoadRequest` | `LoadReply` | CPU percent, load average, memory, swap, and best-effort GPU info. |
| `GET_DISK` / `df` | `DiskRequest` | `DiskReply` | Disk usage for selected paths or mounted volumes. |
| `LIST_PROCESSES` / `plist` | `ProcessListRequest` | `ProcessListReply` | Process search by name or command line. |
| `GET_SUMMARY` / `summary` | `EmptyPayload` | `SummaryReply` | Compact host, Python, CPU, memory, and boot summary. |

Provider-side routing is in `ServerInfoAgent.handle_message`:

```python
return SERVER_INFO.route(
    message,
    {
        GET_LOAD: self.get_load,
        GET_DISK: self.get_disk,
        LIST_PROCESSES: self.list_processes,
        GET_SUMMARY: self.get_summary,
    },
    default=self.not_handled(),
)
```

Consumer code can call the typed service directly:

```python
from paglets.core.runtime_values import ServiceScope
from paglets.examples.system_info import GET_DISK, SERVER_INFO, DiskRequest

service = self.require_contract(SERVER_INFO, operation=GET_DISK, scope=ServiceScope.MESH)
reply = service.call(GET_DISK, DiskRequest(paths=["/"], all_volumes=False))
```

### CLI Commands

`paglets-sysinfo` provides familiar host-inspection commands across the mesh:

```bash
uv run paglets-sysinfo summary
uv run paglets-sysinfo load
uv run paglets-sysinfo df
uv run paglets-sysinfo df / /data
uv run paglets-sysinfo plist python --limit 10
uv run paglets-sysinfo plist postgres --args --json
```

The CLI discovers a reachable entry host automatically. Use optional
`--entry HOSTNAME` to choose one discovered entry host by name, then the
collector paglet discovers online same-version mesh hosts.

### Collector Flow

The collector is `SystemInfoCollectorAgent`. It is not a resident service:

1. The CLI creates one collector on the entry host.
2. The collector calls `available_hosts(online_only=True, include_self=True)`.
3. It clones itself to each host.
4. Each child clone resolves the local `SERVER_INFO` contract and calls the
   requested operation with `scope=ServiceScope.LOCAL`, starting lazy providers
   on demand.
5. Each child sends a `child_result` message back to the parent.
6. The parent returns a summary with `results` and `errors`.

This pattern keeps collection logic mobile while the service itself stays local
to each host. The parent protects `pending_hosts`, `results`, and `errors` with
short `locked_state()` and `@state_locked` sections because child replies can
arrive while the parent is still waiting.

### GPU And Process Notes

GPU information is best effort. The agent runs `nvidia-smi` if available; when
the command is missing or fails, the reply records the reason instead of failing
the whole request.

Process inspection uses `psutil`. Processes that disappear or deny access while
being read are skipped so one protected process does not fail the whole host
reply.
