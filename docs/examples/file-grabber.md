# File Grabber

`paglets examples file` is a deliberately small example for one-file transfer
between an entry host and one remote host. It demonstrates the typed file
mobility pattern:

1. Start a paglet on the source host.
2. Stat the source file and optionally stop for `--dry`.
3. Register the file with natural file mobility.
4. Dispatch the paglet to the destination host.
5. On arrival, write the registered scratch copy to the requested destination
   path.

Start two hosts:

```bash
uv run paglets host --name alpha --port 8765 --mesh-version dev
uv run paglets host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

Preview a push from the entry host to the remote host:

```bash
uv run paglets examples file push --entry alpha ./data.bin --remote beta --dest /tmp/data.bin --dry
```

Run the copy:

```bash
uv run paglets examples file push --entry alpha ./data.bin --remote beta --dest /tmp/data.bin
```

Pull a file from the remote host back to the entry host:

```bash
uv run paglets examples file pull --entry alpha /tmp/data.bin --remote beta --dest ./data-from-beta.bin
```

Use `--mode move` to delete the source after the dispatch imports the file on
the destination host:

```bash
uv run paglets examples file pull --entry alpha /tmp/data.bin --remote beta --dest ./data-from-beta.bin --mode move
```

Use `--overwrite` when the destination file may already exist. Without it, the
arrival hook fails before replacing the destination.

Paths are interpreted by the host where that phase runs. In `push`, `SOURCE`
is read on the entry host and `--dest` is written on the remote host. In
`pull`, `SOURCE` is read on the remote host and `--dest` is written on the
entry host.

The paglet sends user-facing progress messages through the mesh-scoped
`user-info` service. With the default host configuration those messages print
to a host console, so you can see when the file was found, when the dry-run
plan was produced, and when the destination file was saved.

The example does not implement a raw message protocol, but it does keep the
actual file-transfer workflow visible. `TaskPaglet` handles `start`, `status`,
and `wait`; `FileMobilityMixin` provides reusable helpers for stat, result
building, destination planning, registration, and atomic writes:

```python
from dataclasses import dataclass

from paglets.patterns.file_mobility import (
    FileMobilityMixin,
    FileTransferRequest,
    FileTransferResult,
    FileTransferState,
)
from paglets.patterns.tasks import TaskPaglet, TaskStatus


@dataclass
class FileGrabberState(FileTransferState):
    pass


class FileGrabberPaglet(
    FileMobilityMixin,
    TaskPaglet[FileTransferRequest, FileTransferResult, FileGrabberState],
):
    State = FileGrabberState
    Request = FileTransferRequest
    Result = FileTransferResult
    registered_file_name = "grabbed-file"
    notification_title = "File grabber"
```

The source-host side is plain paglet code:

```python
def run_task(self, request):
    plan = self.prepare_file_transfer(request)

    if request.dry_run:
        result = self.build_transfer_result(destination_path=plan.destination_path, dry_run=True)
        self.complete_task(result)
        return None

    self.register_planned_file(plan)
    self.mark_waiting_for_arrival()
    return self.dispatch(plan.target_host)
```

On the target host, the registered file is already present in the paglet's
scratch directory before `on_arrival()` runs:

```python
def on_arrival(self, event):
    arrival = self.current_transfer_arrival()
    if arrival is None:
        return
    destination = self.save_arrived_file(arrival)
    final_path = self.remember_transfer_arrival(destination)
    self.complete_task(self.build_transfer_result(destination_path=final_path, dry_run=False))
```

The CLI uses `TaskClient`, so callers do not send raw `Message("start")`,
`Message("status")`, or application-specific dictionary summaries:

```python
from paglets.examples.file_grabber import FileGrabRequest
from paglets.patterns.tasks import TaskClient

task = TaskClient.for_paglet(proxy, FileGrabberPaglet)
summary = task.start_and_wait(
    FileGrabRequest(
        source_path="./data.bin",
        destination_path="/tmp/data.bin",
        target_host="http://127.0.0.1:8766",
    )
)
```

Use `SingleFileTransferPaglet` directly when the default one-file workflow is
enough. Use `FileMobilityMixin` when the paglet should show or customize the
workflow while still avoiding ad hoc path handling and message plumbing. The
low-level raw message and lifecycle hooks remain available for advanced cases.
