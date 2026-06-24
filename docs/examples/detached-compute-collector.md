# Detached Compute With A Collector

This pattern is for trusted small meshes where a submitter can disappear while
work continues elsewhere:

1. A laptop/home host creates a collector paglet.
2. The collector registers the expected job keys.
3. Compute job paglets run on eligible hosts through `compute-slots`.
4. Each job sends a success or failure report to the collector.
5. The collector exposes `summary` and `drain` messages, and can optionally
   return home after all expected jobs are finished.

This is still a lightweight compute mesh pattern, not a full batch scheduler.
Queues and leases remain local to `compute-slots`, resource estimates are
cooperative, and application code decides how to handle result files.

## Minimal Job Group

Use `CollectingComputeJobPaglet` when each job can report a JSON-sized result:

```python
from dataclasses import dataclass

from paglets.system.compute_slots import (
    CollectingComputeJobPaglet,
    CollectingComputeJobState,
    submit_compute_job_group,
)


@dataclass
class MyJobState(CollectingComputeJobState):
    dataset: str = ""
    value: int = 0


class MyJob(CollectingComputeJobPaglet[MyJobState]):
    State = MyJobState

    def run_compute_job(self) -> None:
        self.state.value = len(self.state.dataset)

    def build_result_payload(self) -> dict:
        return {"dataset": self.state.dataset, "value": self.state.value}
```

Submit a group from a paglet or host context:

```python
states = [
    MyJobState(job_key="bundle-a", dataset="alpha", required_host_tags=("linux",)),
    MyJobState(job_key="bundle-b", dataset="beta", preferred_host_tags=("gpu",)),
]

submission = submit_compute_job_group(
    self.context,
    MyJob,
    states,
    return_home_when_complete=True,
)
```

The helper creates the collector first, registers expected job keys before
workers start, creates the jobs, and records any creation failures back into
the collector.

## Collector Messages

Collectors handle:

- `summary`: returns counts, pending jobs, results, failures, duplicate reports,
  collector location, and return-home state.
- `drain`: waits until all expected jobs have a result or failure, then returns
  the same summary.
- `register_jobs`: adds or updates expected job metadata.
- `job_result` and `job_failure`: used by collecting jobs.
- `return_home`: retries return-home behavior.

Inspect groups from the command line:

```bash
uv run paglets-compute-groups
uv run paglets-compute-groups --group group-abc --json
```

`paglets-compute-slots status --queue --jobs` remains the scheduler-local view
for queues, leases, CPU IDs, and process resource metrics.

## Host Selection

Start hosts with role tags and properties:

```bash
uv run paglets-host --name laptop --bind-public --mesh-version analysis --tag laptop
uv run paglets-host --name linux-a --bind-public --peer http://laptop:8765 \
  --mesh-version analysis --tag linux --property python=3.12
uv run paglets-host --name gpu-a --bind-public --peer http://laptop:8765 \
  --mesh-version analysis --tag linux --tag gpu
```

Compute job states can set:

- `required_host_tags`, such as `("linux",)`.
- `excluded_host_tags`, such as `("laptop",)`.
- `preferred_host_tags`, such as `("gpu",)`.
- `excluded_host_names` and `excluded_host_urls`.
- `allow_home_compute = False`, which remains the default.

You can preview placement with:

```bash
uv run paglets-compute-slots candidates --require-tag linux --prefer-tag gpu --exclude-host laptop
```

## Result Files

Messages are binary-safe for small payloads: `bytes` and `bytearray` in message
args or typed service dataclasses are converted to tagged JSON and restored
automatically. Do not use ordinary messages for large result files such as
SQLite databases.

Use artifacts when the result is naturally a file:

```python
class MyJob(CollectingComputeJobPaglet[MyJobState]):
    State = MyJobState

    def run_compute_job(self) -> None:
        result_path = self.work_dir() / "result.db"
        build_sqlite_result(result_path)
        self.report_compute_artifact(result_path, result={"bundle": self.state.job_key}, move=True)
```

`report_compute_artifact(...)` uploads the file to the collector host and sends
an `ArtifactRef` in the collector result payload. It reports success itself, so
do not also call `report_compute_success(...)` for the same job. The default
`after_compute_success()` hook notices that a report was already sent and will
not send a second JSON result. The collector can later download or delete that
artifact through `HostClient` or `paglets-artifacts`.

For files that should travel with the job paglet itself, use natural file
mobility:

```python
self.register_file(result_path, name="result.db", mode="move")
self.dispatch_to(self.state.home_host_name)
```

The registered file is copied to the target scratch directory before
`on_arrival()` runs. A `move` assignment deletes the source only after the
dispatch succeeds.
