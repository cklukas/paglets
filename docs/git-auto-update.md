# Git Auto-Update

`paglets host --auto-update-from-git` keeps trusted paglets hosts aligned with a
git checkout. It is intended for small lab meshes where you develop on one
machine, push commits, and want remote hosts to update and restart without
logging into each machine.

The feature is host-side only. Client commands such as `paglets examples pi`,
`paglets sys`, and `paglets search` do not need extra flags.

> [!WARNING]
> **Trusted networks only.**
> The update endpoint runs `git fetch`, `git pull`, and `uv sync`. Use it only
> on trusted direct local or lab meshes. Relay/connect mode rejects
> auto-update and does not accept update requests.

## Quick Start

Start every participating host from a git checkout and include
`--auto-update-from-git`:

```bash
uv run paglets host --name alpha --port 8765 --auto-update-from-git
uv run paglets host --name beta --port 8766 --peer http://127.0.0.1:8765 --auto-update-from-git
```

Across machines, add `--bind-public 192.0.2.10` so each host binds only a reachable
LAN address and publishes that URL to the mesh:

```bash
uv run paglets host --name mac --bind-public auto --port 8765 --auto-update-from-git
uv run paglets host --name windows --bind-public 192.0.2.10 --port 8765 --auto-update-from-git
```

Repeat explicit values such as `--bind-public 192.0.2.10` to listen on multiple
specific addresses. The first bound address is the one published to mesh peers.
The auto form keeps watching for LAN address changes and rebinds/publishes the
new address after DHCP or network reconnect changes it.

The checkout must be clean. If `git status --porcelain` reports uncommitted or
untracked files, startup is cancelled before any fetch or pull runs:

```text
paglets host: --auto-update-from-git requires a clean git checkout; startup cancelled.
```

The normal workflow is:

1. Commit locally.
2. Push the commit.
3. Start or restart one local paglets host with `--auto-update-from-git`.
4. That host advertises its commit to configured peers and known mesh hosts.
5. Remote hosts fetch, pull, run `uv sync`, and restart themselves if needed.

## Startup Flow

On startup, a host records the commit hash that the current Python process
started from. It then serializes git and dependency operations through a lock
inside the checkout.

1. `paglets host` starts with `--auto-update-from-git`.
2. It finds the git checkout and records `process_start_head`.
3. It acquires `.git/paglets-auto-update.lock` and runs `git status --porcelain`.
4. If the checkout is not clean, it prints a message and cancels startup.
5. Otherwise it runs `git fetch` and `git pull`.
6. If `HEAD` changed from `process_start_head`, it runs `uv sync`:
   - if `uv sync` fails, it reports the failure and does not restart;
   - if `uv sync` succeeds, it re-executes the same host command.
7. It releases the lock and the host starts serving.

The lock path is `.git/paglets-auto-update.lock`. It prevents multiple paglets
host processes that share one checkout from running `git pull` or `uv sync`
concurrently. The lock directory contains an `owner` file with the updater PID
and timestamp. If a previous host crashed and left a lock behind, the next
update removes it when the owner PID is no longer running. Locks with missing
or malformed owner data are only removed after a short grace period, so a
second host does not delete a lock that another process has just created. A
lock held by a live process is waited on until the normal lock timeout.

## Mesh Broadcast Flow

After startup, an auto-update-enabled host sends its current commit hash to
configured `--peer` URLs and known mesh host URLs. Normal mesh membership still
uses the existing code-version gate, so mismatched peers are not selected for
dispatch or clone. The update request is separate from normal mesh membership.

1. **Developer Machine → Git Remote**: git push
2. **Developer Machine → Local Host**: start/restart paglets host --auto-update-from-git
3. **Local Host**: git fetch + git pull + uv sync if needed
4. **Local Host → Remote Host**: POST /admin/git-update { target_hash }
5. **Remote Host → Git Remote**: git fetch
6. **Remote Host**: verify target_hash exists
7. If target hash exists:
   1. **Remote Host**: git pull
   2. **Remote Host**: uv sync if process-start HEAD is stale
   3. **Remote Host → Local Host** (reply): update ok, restart scheduled if needed
   4. **Remote Host**: graceful shutdown
   5. **Remote Host**: CLI main thread re-execs through uv
8. If target hash missing:
   1. **Remote Host → Local Host** (reply): target-missing error
   2. **Local Host → Developer Machine**: print "commit may not have been pushed"

If a remote host reports that the requested commit is missing after `git fetch`,
the requesting host prints the error. In practice this usually means the local
commit was not pushed yet. Run `git push`, then restart the local/requesting
host to broadcast the commit again.

## Shared Checkout Behavior

Multiple hosts may run from the same project folder, for example several local
host processes on different ports. They all share one checkout lock.

1. **Host alpha → .git/paglets-auto-update.lock**: acquire lock
2. **Host beta → .git/paglets-auto-update.lock**: wait
3. **Host alpha → Shared checkout**: git fetch + git pull
4. **Host alpha → Shared checkout**: uv sync if HEAD differs from alpha start hash
5. **Host alpha → .git/paglets-auto-update.lock**: release lock
6. **Host beta → .git/paglets-auto-update.lock**: acquire lock
7. **Host beta → Shared checkout**: read current HEAD
8. **Host beta → Shared checkout**: git fetch + git pull
9. **Host beta → Shared checkout**: uv sync if HEAD differs from beta start hash
10. **Host beta → .git/paglets-auto-update.lock**: release lock
11. **Host alpha**: restart if checkout changed
12. **Host beta**: restart if checkout changed

The second host still restarts when the shared checkout already moved before it
got the lock. Restart decisions compare the current checkout `HEAD` with each
process's own `process_start_head`, not only with whether that process ran a
successful pull.

Runtime update requests restart slightly differently from startup updates. A
background update thread may request shutdown, but it does not call `exec`
itself. The foreground CLI thread sees the restart request after the HTTP host
has shut down and then re-execs the same command via `uv run python -m
paglets.tooling.cli ...`. This avoids Windows-specific failures caused by replacing the
process from a worker thread or by reusing a Python executable path that `uv
sync` just recreated.

On Windows, auto-update defers the explicit `uv sync` step until that re-exec
path. This avoids trying to replace the currently running
`.venv\Scripts\paglets host.exe` console wrapper, which Windows keeps locked
while the host process is active. The checkout is still pulled first, and the
restart still goes through `uv run python -m paglets.tooling.cli ...`.

## Runtime API

Participating hosts expose update metadata in `/health`:

```json
{
  "auto_update_from_git": true,
  "git_head": "012345...",
  "git_process_start_head": "012345...",
  "git_update": {
    "status": "current",
    "ok": true,
    "target_hash": "012345..."
  }
}
```

The update endpoint is:

```http
POST /admin/git-update
```

Request payload:

```json
{
  "target_hash": "012345...",
  "source_name": "alpha",
  "source_url": "http://127.0.0.1:8765"
}
```

Important statuses:

- `disabled`: the host was not started with `--auto-update-from-git`.
- `dirty-worktree`: local changes are present; no fetch, pull, sync, or restart
  is attempted.
- `target-missing`: the requested commit was not found after `git fetch`.
- `pull-failed`: `git pull` failed, often because of conflicts or local git
  configuration.
- `uv-sync-failed`: code updated, but dependency sync failed; restart is not
  scheduled.
- `current`: the checkout is already at the process's start hash.
- `updated`: the checkout changed and restart may be scheduled.

The latest update result is kept in memory and returned from `/health`, so the
requesting host can report remote failures without logging into the remote
machine.

## Operational Notes

- Start hosts from the repository root or any path inside the repository.
- The checkout must have a usable upstream remote for `git fetch` and `git pull`.
- Commit or stash local edits before enabling auto-update.
- Push commits before broadcasting them to other hosts.
- Keep `uv` installed on hosts; dependency sync uses `uv sync`.
- Startup broadcasts validate discovered targets before sending update
  requests and suppress unreachable-target noise. Stale loopback beacons on
  random ports, for example from short-lived local test hosts, are ignored.
- Runtime auto-update restarts print `git auto-update restart requested;
  restarting` before re-execing. If that line appears without a new host, check
  that `uv` is on `PATH`.
- Use a process supervisor only if you want extra resilience. The default
  behavior is self re-exec with the same command-line arguments.
