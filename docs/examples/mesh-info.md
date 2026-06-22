# Mesh Info

`mesh-info` is an eager resident service that keeps a fresh resource snapshot
for each visible host. It samples local CPU, memory, swap, work-directory disk
space, and active/inactive paglet counts, then exchanges bounded snapshot
batches with peer `mesh-info` services.

The core contract is:

```python
from paglets.examples.mesh_info import MESH_INFO, GET_LANDSCAPE, SELECT_TARGETS
```

Useful CLI commands:

```bash
uv run paglets-mesh-info summary
uv run paglets-mesh-info targets --max-load-per-cpu 1.0 --min-work-free 1G
uv run paglets-mesh-info targets --json
```

The `summary` command prints the fresh landscape known to the entry host. The
`targets` command applies placement constraints and ranks eligible hosts by
load, CPU, memory pressure, and work-storage pressure. Both text tables include
active and inactive paglet counts for each host. Use optional `--entry
HOSTNAME` to choose a discovered entry host by name.

Programmatic target selection:

```python
from paglets.core.runtime_values import ServiceScope
from paglets.examples.mesh_info import MESH_INFO, SELECT_TARGETS, TargetSelectionRequest

mesh_info = self.require_contract(MESH_INFO, operation=SELECT_TARGETS, scope=ServiceScope.LOCAL)
targets = mesh_info.call(
    SELECT_TARGETS,
    TargetSelectionRequest(limit=4, max_load_per_cpu=1.0, min_work_free_bytes=1024**3),
)
```

`mesh-info` is intentionally a resident service rather than a one-shot clone
collector: each host maintains its own current view, and schedulers can query
the nearest local service repeatedly without fan-out for every placement
decision.
