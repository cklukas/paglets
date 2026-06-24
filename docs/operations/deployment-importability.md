# Deployment And Importability

Paglets moves state, not source code. Every host that may create, activate,
clone, dispatch, or receive a paglet must be able to import the same paglet
class and state class by `module:qualname`.

Good:

```text
ds_dia/
  pyproject.toml
  src/ds_dia/compute/paglets.py
  src/ds_dia/compute/models.py
```

```python
from ds_dia.compute.paglets import DsDiaBundleJobPaglet
from ds_dia.compute.models import DsDiaBundleJobState
```

Avoid defining paglet classes in notebooks, REPL sessions, `__main__`, stdin,
or one-off scripts. Child processes and remote hosts cannot re-import those
classes reliably.

## Development Installs

For local multi-host development from one checkout:

```bash
uv sync --dev --extra docs
uv run paglets-host --name alpha --port 8765 --mesh-version dev
uv run paglets-host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

For multiple machines, install the same application package on every host. A
common editable workflow is:

```bash
git clone <your-repo>
cd <your-repo>
uv sync
uv run paglets-host --name linux-a --bind-public --mesh-version ds-dia-dev
```

Use the same `--mesh-version` value only when hosts are actually running
compatible code. For stricter deployments, use a release version or commit hash
as the mesh version.

## Pinning Versions

Recommended production-ish small mesh setup:

- Package app-owned paglets in a normal Python package under `src/`.
- Pin `paglets` and application dependencies in the app lockfile.
- Install the same package version on every host.
- Start all hosts with the same deliberate `--mesh-version`, for example
  `ds-dia-0.4.2`.
- Use `--tag` and `--property` to advertise host roles and compatibility hints,
  such as `--tag linux`, `--tag gpu`, or `--property python=3.12`.

`paglets-host --auto-update-from-git` can keep trusted lab hosts aligned from a
git checkout. It runs `git fetch`, `git pull`, `uv sync`, and host restart
logic, so use it only on trusted lab networks.

## Diagnosing Import Failures

If a remote host reports that a class cannot be imported:

1. Check the class path in the failing state or log, for example
   `ds_dia.compute.paglets:DsDiaBundleJobPaglet`.
2. On the remote host, run:

   ```bash
   uv run python -c "from ds_dia.compute.paglets import DsDiaBundleJobPaglet; print(DsDiaBundleJobPaglet)"
   ```

3. Confirm the state dataclass imports too.
4. Confirm both hosts use the same checkout, wheel, or package version.
5. Confirm the host process was restarted after installing new code.
6. Confirm the class is top-level in an importable module and not nested inside
   a function.

Mesh membership is code-version gated, but that gate only compares the host
version string. It does not install missing application packages for you.

## Practical Checklist

Before submitting detached compute jobs:

- `paglets-host` is running on the submitter and compute hosts.
- Every host can import the application paglet class and state classes.
- Hosts use the same `--mesh-version` for compatible code.
- Linux/GPU/collector/laptop roles are advertised with `--tag`.
- Candidate placement is visible with `paglets-compute-slots candidates`.
- Group status is visible with `paglets-compute-groups`.
- Large result files have an application-level storage or return-home plan.
