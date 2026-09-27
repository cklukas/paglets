# Analysis Jobs

`paglets examples analysis` is a synthetic distributed dataframe analysis example.
It demonstrates how application paglets can use the built-in
[Compute Slots](../system/compute-slots.md) scheduler without putting
application-specific result collection into the scheduler itself.

The example starts on the laptop/home host, creates configurable job paglets,
sends them to suitable Linux compute hosts, waits for scheduler grants, performs
synthetic pandas/scikit-learn work, stores result frames while the laptop is
offline, then returns home and appends results to SQLite under a cross-process
file lock.

This example keeps SQLite result collection application-specific so the
database write path is visible. For reusable "N jobs plus collector" status and
failure bookkeeping, use the group helpers described in
[Detached Compute With A Collector](detached-compute-collector.md).

## Running It

Start a laptop/home host and one or more Linux-style compute hosts in the same
mesh:

```bash
uv run paglets host --name laptop --bind-public auto --port 8765 --mesh-version analysis
uv run paglets host --name linux-a --bind-public auto --peer http://laptop:8765 --port 8765 --mesh-version analysis
uv run paglets host --name linux-b --bind-public auto --peer http://laptop:8765 --port 8765 --mesh-version analysis
```

Start the campaign from the laptop host:

```bash
uv run paglets examples analysis --entry laptop --tasks 20
```

Useful shorter development run:

```bash
uv run paglets examples analysis --entry laptop --tasks 3 --rows 2000 --target-runtime 3
```

The default target runtime is about 2-3 minutes per job. The job pads with CPU
work when the actual model training finishes sooner.

## Components

| From | To | Interaction |
|---|---|---|
| `paglets examples analysis` CLI | `CampaignSeederPaglet` | create + start |
| `CampaignSeederPaglet` | `AnalysisJobPaglet` | create 20 jobs |
| `AnalysisJobPaglet` | compute-slots | `candidate_hosts` / `request_slot` |
| compute-slots | `AnalysisJobPaglet` | grant / redirect; ask the host to apply CPU affinity |
| `AnalysisJobPaglet` | user-info | notify |
| `AnalysisJobPaglet` | SQLite result DB | return home and commit results |

- **`CampaignSeederPaglet`**: Runs on the home host. It creates a configurable number of
  `AnalysisJobPaglet` instances and records their IDs. Its CLI-facing
  `start`/`summary` protocol uses typed operations, while `_seed_jobs()` keeps
  the job creation workflow explicit in the example.

- **`AnalysisJobPaglet`**: Derives from `ComputeJobPaglet`. It carries job configuration, resource
  wishes, SQLite DB path, and result metadata. The base class captures the home
  host and carries scheduler request IDs, lease IDs, redirects, completion
  state, and affinity metadata.

- **`compute-slots`**: Filters suitable hosts, queues waiting jobs, grants leases, and redirects
  sleepers through bounded work stealing.

- **`user-info`**: Prints user-facing notifications from jobs.

## How The Job Is Implemented

The example has two paglet classes:

- `CampaignSeederPaglet` runs once on the home host and creates many
  `AnalysisJobPaglet` instances.
- `AnalysisJobPaglet` is the actual compute job. It derives from
  `ComputeJobPaglet`, so it only implements analysis and result handling.

The application state extends `ComputeJobState`. Scheduler resource estimates
are normal state fields, while analysis-specific fields such as `job_id`,
`seed`, `db_path`, and result paths belong to this example:

<!-- snippet: src/paglets/examples/analysis_jobs/agent.py:analysis-job-state | AnalysisJobState in agent.py -->
Source: [AnalysisJobState in agent.py](https://github.com/cklukas/paglets/blob/main/src/paglets/examples/analysis_jobs/agent.py)

```python
@dataclass
class AnalysisJobState(ComputeJobState):
    campaign_id: str = ""
    job_id: str = ""
    status: str = STATUS_NEW
    seed: int = 0
    db_path: str = ""
    row_count: int = DEFAULT_ROW_COUNT
    feature_count: int = DEFAULT_FEATURE_COUNT
    estimator_trees: int = DEFAULT_ESTIMATOR_TREES
    target_runtime_seconds: float = DEFAULT_TARGET_RUNTIME_SECONDS
    estimated_runtime_seconds: float = DEFAULT_TARGET_RUNTIME_SECONDS
    home_check_seconds: float = DEFAULT_HOME_CHECK_SECONDS
    db_lock_timeout_seconds: float = DEFAULT_DB_LOCK_TIMEOUT_SECONDS
    cpu_cores: int = DEFAULT_CPU_CORES
    memory_bytes: int = DEFAULT_MEMORY_BYTES
    temp_storage_bytes: int = DEFAULT_TEMP_STORAGE_BYTES
    result_paths: dict[str, str] = field(default_factory=dict)
    result_payloads: dict[str, bytes] = field(default_factory=dict)
    started_at: float = 0.0
    completed_at: float = 0.0
    error: str = ""
```
<!-- /snippet -->

The seeder creates one state object per task. This is where the application
assigns its own `job_id` and resource estimates. The compute scheduler later
uses `cpu_cores`, `memory_bytes`, `temp_storage_bytes`, and
`estimated_runtime_seconds`; it does not interpret the analysis `job_id`.

<!-- snippet: src/paglets/examples/analysis_jobs/agent.py:seed-jobs | CampaignSeederPaglet._seed_jobs in agent.py -->
Source: [CampaignSeederPaglet._seed_jobs in agent.py](https://github.com/cklukas/paglets/blob/main/src/paglets/examples/analysis_jobs/agent.py)

```python
def _seed_jobs(self, request: AnalysisCampaignRequest) -> None:
    campaign_id = f"analysis-{uuid.uuid4().hex}"
    task_count = max(1, int(request.task_count))
    for index in range(task_count):
        job_id = f"{campaign_id}-{index:04d}"
        state = AnalysisJobState(
            campaign_id=campaign_id,
            job_id=job_id,
            seed=index + 10_000,
            db_path=request.db_path,
            row_count=max(100, int(request.row_count)),
            feature_count=max(4, int(request.feature_count)),
            estimator_trees=max(1, int(request.estimator_trees)),
            target_runtime_seconds=max(0.0, float(request.target_runtime_seconds)),
            estimated_runtime_seconds=max(0.0, float(request.target_runtime_seconds)),
            db_lock_timeout_seconds=max(0.0, float(request.db_lock_timeout_seconds)),
            cpu_cores=max(1, int(request.cpu_cores)),
            memory_bytes=max(0, int(request.memory_bytes)),
            temp_storage_bytes=max(0, int(request.temp_storage_bytes)),
        )
        try:
            proxy = self.context.create_paglet(AnalysisJobPaglet, state)
            with self.locked_state() as current:
                current.created_jobs.append({"job_id": job_id, "agent_id": proxy.agent_id})
        except Exception as exc:
            with self.locked_state() as current:
                current.errors[job_id] = str(exc)
    with self.locked_state() as current:
        current.done = True
    self.notify_all_state_changed()
```
<!-- /snippet -->

The compute part is intentionally concentrated in `run_compute_job()`. The base
class calls this method only after a compute slot has been granted. It also
releases the lease when the method returns or raises.

<!-- snippet: src/paglets/examples/analysis_jobs/agent.py:run-compute-job | AnalysisJobPaglet.run_compute_job in agent.py -->
Source: [AnalysisJobPaglet.run_compute_job in agent.py](https://github.com/cklukas/paglets/blob/main/src/paglets/examples/analysis_jobs/agent.py)

```python
def run_compute_job(self) -> None:
    with self.locked_state() as state:
        state.status = STATUS_RUNNING
        state.started_at = time.time()
        job_id = state.job_id
        seed = state.seed
        row_count = state.row_count
        feature_count = state.feature_count
        estimator_trees = state.estimator_trees
        target_runtime = state.target_runtime_seconds
    with self.locked_state() as state:
        cpu_core_ids = list(state.cpu_core_ids)
        cpu_affinity_supported = state.cpu_affinity_supported
        cpu_affinity_enforced = state.cpu_affinity_enforced
        cpu_affinity_error = state.cpu_affinity_error
    data = download_data(job_id=job_id, seed=seed, row_count=row_count, feature_count=feature_count)
    frames = process_data_to_frames(
        data,
        job_id=job_id,
        host_name=self.context.name,
        seed=seed,
        target_runtime_seconds=target_runtime,
        estimator_trees=estimator_trees,
        cpu_core_ids=cpu_core_ids,
        cpu_affinity_supported=cpu_affinity_supported,
        cpu_affinity_enforced=cpu_affinity_enforced,
        cpu_affinity_error=cpu_affinity_error,
    )
    payloads = frames_to_payloads(frames)
    result_paths = self._save_payloads(payloads)
    with self.locked_state() as state:
        state.result_paths = result_paths
        state.result_payloads = {}
        state.completed_at = time.time()
        state.status = STATUS_WAITING_FOR_HOME
```
<!-- /snippet -->

After the compute method returns, the job may need several wakeups before the
home laptop is online again. That repeated post-compute behavior belongs in
`continue_after_compute_success()`:

<!-- snippet: src/paglets/examples/analysis_jobs/agent.py:continue-after-success | AnalysisJobPaglet.continue_after_compute_success in agent.py -->
Source: [AnalysisJobPaglet.continue_after_compute_success in agent.py](https://github.com/cklukas/paglets/blob/main/src/paglets/examples/analysis_jobs/agent.py)

```python
def continue_after_compute_success(self) -> None:
    with self.locked_state() as state:
        status = state.status
    if status == STATUS_RETURNING_HOME:
        self._commit_at_home()
        return
    self._try_return_home()
```
<!-- /snippet -->

`_try_return_home()` is application logic, not scheduler logic. It checks
whether the home host is visible, deactivates for a timed retry when the laptop
is offline, or dispatches the paglet home when the laptop is online:

<!-- snippet: src/paglets/examples/analysis_jobs/agent.py:try-return-home | AnalysisJobPaglet._try_return_home in agent.py -->
Source: [AnalysisJobPaglet._try_return_home in agent.py](https://github.com/cklukas/paglets/blob/main/src/paglets/examples/analysis_jobs/agent.py)

```python
def _try_return_home(self) -> None:
    if self.is_compute_home():
        with self.locked_state() as state:
            state.result_payloads = self._load_payloads(state.result_paths)
            state.status = STATUS_RETURNING_HOME
        self._commit_at_home()
        return
    with self.locked_state() as state:
        home = state.home_host_url or state.home_host_name
        interval = max(1.0, float(state.home_check_seconds))
    if not home or not self.context.is_host_online(home):
        self.deactivate(
            policy=DeactivationPolicy.after(
                interval,
                activate_on_message=True,
                queue_messages_when_inactive=True,
                activate_on_startup=False,
            )
        )
        return
    with self.locked_state() as state:
        state.result_payloads = self._load_payloads(state.result_paths)
        state.status = STATUS_RETURNING_HOME
        target = state.home_host_url or state.home_host_name
    self.dispatch(target)
```
<!-- /snippet -->

Once back home, the example serializes only the final SQLite write. Compute,
movement, and payload loading are not locked:

<!-- snippet: src/paglets/examples/analysis_jobs/agent.py:commit-at-home | AnalysisJobPaglet._commit_at_home in agent.py -->
Source: [AnalysisJobPaglet._commit_at_home in agent.py](https://github.com/cklukas/paglets/blob/main/src/paglets/examples/analysis_jobs/agent.py)

```python
def _commit_at_home(self) -> None:
    with self.locked_state() as state:
        state.status = STATUS_COMMITTING
        payloads = dict(state.result_payloads)
        db_path = state.db_path
        timeout = state.db_lock_timeout_seconds
    append_frames_to_sqlite(db_path, payloads, lock_timeout_seconds=timeout)
    with self.locked_state() as state:
        state.status = STATUS_COMMITTED
    self.notify_user(
        "info",
        "Analysis result saved",
        f"Saved {self.state.job_id} to {db_path}",
        job_id=self.state.job_id,
    )
    self.context.host.dispose(self.agent_id)
```
<!-- /snippet -->

Failure handling is deliberately small. The base class already sets
`compute_status = FAILED_FINAL`, records `compute_error`, and releases any
lease. The example only mirrors the failure into its application status and
sends a notification:

<!-- snippet: src/paglets/examples/analysis_jobs/agent.py:after-compute-failure | AnalysisJobPaglet.after_compute_failure in agent.py -->
Source: [AnalysisJobPaglet.after_compute_failure in agent.py](https://github.com/cklukas/paglets/blob/main/src/paglets/examples/analysis_jobs/agent.py)

```python
def after_compute_failure(self, message: str) -> None:
    with self.locked_state() as state:
        state.status = STATUS_FAILED_FINAL
        state.error = message
    self.notify_user("error", "Analysis job failed", message, job_id=self.state.job_id)
```
<!-- /snippet -->

Everything not shown here is either ordinary data processing in
`workload.py` or private helper code for saving/loading result payloads. New
compute job types should usually need the same small surface: a state dataclass,
`run_compute_job()`, optional post-compute return/commit logic, and optional
failure notification.

## Compute Job API Usage

The example is intentionally small at the scheduler boundary. `AnalysisJobPaglet`
does not implement placement, candidate selection, local slot requests, sleep,
redirect handling, affinity assignment, or lease release. Those mechanics come
from `ComputeJobPaglet`.

The example implements only the application-specific hooks:

| Method | Why the example implements it | What the base class still owns |
| --- | --- | --- |
| `handle_compute_job_message()` | Adds a `status` message that returns the analysis job state for diagnostics. | Scheduler messages such as `compute_slot_granted` and `compute_slot_redirect`. |
| `run_compute_job()` | Runs the pandas/scikit-learn workload after a slot was granted, stores result payloads on the compute host, and changes application `status` to `WAITING_FOR_HOME`. | Starting this method only after a lease exists, recording affinity metadata, and releasing the lease after the method returns or fails. |
| `continue_after_compute_success()` | Handles the post-compute phase: wait for home, dispatch home, or commit once already returning. This hook can run more than once after timed wakeups. | Calling this hook when compute first completes and again when a completed paglet wakes. |
| `after_compute_failure(message)` | Copies the failure into application fields and sends a user notification. | Marking `compute_status = FAILED_FINAL`, recording `compute_error`, and releasing any lease. |

The example also has private application helpers, not scheduler hooks:

- `_try_return_home()` checks whether the laptop/home host is online. If not, it
  deactivates with a timed wakeup. If yes, it loads result payloads and dispatches
  the paglet home.
- `_commit_at_home()` appends result frames to SQLite under the file lock,
  notifies the user, and disposes the completed paglet.
- `_save_payloads()` and `_load_payloads()` move serialized result frames between
  local persistent storage and mobile paglet state.

There are two status fields by design:

| Field | Owner | Values in this example |
| --- | --- | --- |
| `compute_status` | `ComputeJobPaglet` | `NEW`, `PLACING`, `WAITING_FOR_SLOT`, `RUNNING`, `COMPLETED`, `FAILED_FINAL`. |
| `status` | `AnalysisJobPaglet` | `NEW`, `RUNNING`, `WAITING_FOR_HOME`, `RETURNING_HOME`, `COMMITTING`, `COMMITTED`, `FAILED_FINAL`. |

New compute job types should normally copy this shape: put resource estimates
and domain configuration in a `ComputeJobState` subclass, implement
`run_compute_job()`, and add `continue_after_compute_success()` only when
results need a return/wait/commit phase.

## Job State Machine

| From | To | When |
|---|---|---|
| (start) | `NEW` |  |
| `NEW` | `PLACING` | first run on home |
| `PLACING` | `PLACING` | candidate selected, dispatch |
| `PLACING` | `WAITING_FOR_SLOT` | scheduler returns sleep |
| `PLACING` | `RUNNING` | scheduler returns run_now |
| `WAITING_FOR_SLOT` | `RUNNING` | compute_slot_granted |
| `WAITING_FOR_SLOT` | `PLACING` | compute_slot_redirect |
| `RUNNING` | `WAITING_FOR_HOME` | analysis saved remotely |
| `WAITING_FOR_HOME` | `WAITING_FOR_HOME` | home offline, timed wakeup |
| `WAITING_FOR_HOME` | `RETURNING_HOME` | home online |
| `RETURNING_HOME` | `COMMITTING` | arrives home with payloads |
| `COMMITTING` | `COMMITTED` | SQLite append succeeds |
| `NEW` | `FAILED_FINAL` | no suitable host |
| `PLACING` | `FAILED_FINAL` | rejected |
| `RUNNING` | `FAILED_FINAL` | processing failed |
| `COMMITTING` | `FAILED_FINAL` | save failed |

## Placement And Scheduling Flow

1. **AnalysisJobPaglet on home → entry compute-slots**: candidate_hosts(resource wishes)
2. **entry compute-slots → compute-slots A**: scheduler_status
3. **entry compute-slots → compute-slots B**: scheduler_status
4. **compute-slots A → entry compute-slots** (reply): capacity and limits
5. **compute-slots B → entry compute-slots** (reply): capacity and limits
6. **entry compute-slots → AnalysisJobPaglet on home** (reply): suitable candidates + selected candidate
7. If no candidates:
   1. **AnalysisJobPaglet on home → user-info**: notify warning
   2. **AnalysisJobPaglet on home**: FAILED_FINAL
8. If candidates found:
   1. **AnalysisJobPaglet on home**: persist selected candidate
   2. **AnalysisJobPaglet on home → same job on Linux A**: dispatch self to Linux A
   3. **same job on Linux A → compute-slots A**: request_slot
   4. If run_now:
      1. **compute-slots A → same job on Linux A** (reply): lease_id
      2. **compute-slots A → same job on Linux A**: host-managed CPU affinity if supported
      3. **same job on Linux A**: run analysis
   5. If sleep:
      1. **compute-slots A → same job on Linux A** (reply): queued
      2. **same job on Linux A**: deactivate on Linux A
      3. **compute-slots A → same job on Linux A** (reply): compute_slot_granted activates job
      4. **compute-slots A → same job on Linux A**: host-managed CPU affinity if supported
      5. **same job on Linux A**: run analysis
   6. If redirect:
      1. **compute-slots A → same job on Linux A** (reply): target Linux B + redirect metadata
      2. **same job on Linux A → same job on Linux B**: dispatch self to Linux B
      3. **same job on Linux B → compute-slots B**: request_slot again on B

The initial target is selected by `compute-slots` from suitable ranked
candidates. `ComputeJobPaglet` persists that target, dispatches itself there,
and carries redirect metadata so peer schedulers can apply cooldown hysteresis.

## Synthetic Workload

Each job:

1. Generates deterministic synthetic classification data from its job seed.
2. Converts the data into a pandas dataframe.
3. Receives scheduler affinity metadata; the scheduler asks the host to pin the
   job process when the platform supports affinity.
4. Trains a scikit-learn random forest with `n_jobs=1`.
5. Computes prediction accuracy.
6. Produces three result frames:
   - `job_summary`
   - `feature_summary`
   - `prediction_summary`
7. Pads with CPU work until the configured target runtime is reached.

The defaults are intentionally large enough to make scheduling observable:

- `DEFAULT_TASK_COUNT = 20`
- `DEFAULT_ROW_COUNT = 80000`
- `DEFAULT_FEATURE_COUNT = 32`
- `DEFAULT_ESTIMATOR_TREES = 80`
- `DEFAULT_TARGET_RUNTIME_SECONDS = 150`

Lower these values for tests or demos on small machines.

## Result Collection

Result collection is application-specific. In this example, jobs return home and
append to a SQLite database.

While the laptop is offline, a completed job stores result payloads in its
paglet persistent storage on the compute host. Every five minutes it wakes and
checks whether the home host is online.

When home is visible, the job loads the result payloads into mobile state,
dispatches home, and writes the SQLite tables:

- `job_summary`
- `feature_summary`
- `prediction_summary`

`job_summary` includes the scheduler and affinity outcome for the run:

- `cpu_core_ids`, the concrete CPU IDs granted on affinity-capable hosts.
- `cpu_affinity_supported`, whether this host can enforce process affinity.
- `cpu_affinity_enforced`, whether the job process was actually pinned before
  the workload started.
- `cpu_affinity_error`, the non-fatal reason when pinning was requested but not
  enforced.

SQLite writes are serialized with a cross-process file lock at:

```text
<db_path>.paglets.lock
```

The example also uses `BEGIN IMMEDIATE` so the SQLite transaction obtains a
write lock before appending frames.

1. Repeat every 5 minutes:
   1. **completed job on Linux**: check home online
2. **completed job on Linux**: load result payloads
3. **completed job on Linux → laptop host**: dispatch home
4. **completed job on Linux → file lock**: acquire <db>.paglets.lock
5. **completed job on Linux → SQLite DB**: BEGIN IMMEDIATE
6. **completed job on Linux → SQLite DB**: append result tables
7. **completed job on Linux → SQLite DB**: commit
8. **completed job on Linux → file lock**: release
9. **completed job on Linux**: dispose

## Implementing Your Own Job Paglet

Use this example as a template for the application-specific parts of a compute
job. The reusable scheduling mechanics live in `ComputeJobState` and
`ComputeJobPaglet`; new job types should not reimplement candidate selection,
slot requests, redirects, sleep handling, affinity metadata, or lease release.

- Put scheduler fields in a state class derived from `ComputeJobState`.
- Put job configuration and progress in the same dataclass state.
- Derive the job paglet from `ComputeJobPaglet`.
- Estimate CPU cores, RAM, temp storage, runtime, and GPU needs before
  placement.
- Set `estimated_runtime_seconds` in the state and implement `run_compute_job()`.
- Use application-specific fields such as `job_id`, `dataset_name`, or
  `result_key` for result labels.
- Let the base class own advancement, scheduler wakeups, and lease release.
- Use `continue_after_compute_success()` when a completed job needs to wake
  later, return home, or commit results after compute has finished.
- Store results before waiting for home or another destination.
- Keep final result collection task-specific.
- Use application-specific fields for result states such as `WAITING_FOR_HOME`
  or `COMMITTING`; `compute_status` is reserved for the scheduling base.
- Send user-facing messages through the base `notify_user()` helper, which uses
  [User Info](../system/user-info.md).
