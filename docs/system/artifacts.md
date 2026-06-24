# Artifact Transport

Artifacts are host-owned binary blobs used when a payload is too large or too
file-shaped for ordinary messages. The preferred workflow for files that belong
to a paglet instance is natural file mobility: register the file with the
paglet, then dispatch or clone the paglet.

## Natural File Mobility

Register files from inside the paglet:

```python
path = self.work_dir() / "result.db"
path.write_bytes(result_bytes)
self.register_file(path, name="result.db", mode="move")
```

Then move the paglet normally:

```python
def run(self):
    result_path = self.work_dir() / "result.db"
    build_sqlite_result(result_path)
    self.register_file(result_path, name="result.db", mode="move")
    self.dispatch_to("linux-worker")
```

After the dispatch, registered files are transferred before `on_arrival()`
runs on the target host:

```python
def on_arrival(self, event):
    result_path = self.file_path("result.db")
    assert result_path.exists()
```

The target copy is written into the destination paglet's scratch/work
directory. The registered `PagletFileRef` keeps the original source host,
source path, source creation/modification times, checksum, and current target
path.

Dispatch honors the registered assignment:

- `mode="copy"` leaves the source file in place.
- `mode="move"` deletes the source file only after the target import and
  envelope receipt succeed.

Clone always copies registered files, even when the original assignment is
`move`. Source files are never deleted by clone.

When a paglet is disposed, its scratch/work directory is removed. Files outside
scratch are not deleted by dispose; they are removed only after a successful
dispatch with `mode="move"`.

## Low-Level Artifacts

Use low-level artifacts for explicit file exchange, recovery, or collector
results:

```python
artifact = self.upload_artifact("result.db", host_url=collector_host_url)
collector.send(Message("job_result", {"artifact": artifact.to_wire()}))
```

`ArtifactRef` contains host URL, artifact ID, name, byte size, SHA-256,
compression label, timestamps, and owner agent ID. Downloads verify byte count
and SHA-256 before replacing the target file.

`PagletProxy.send_artifact(...)` combines upload and message delivery:

```python
proxy.send_artifact(Message("import_result"), "result.db", name="result.db")
```

The receiver gets an ordinary message with an artifact reference in
`message.args`:

```python
from paglets.artifacts import ArtifactRef


def handle_message(self, message):
    if message.kind == "import_result":
        artifact = ArtifactRef.from_wire(message.args["artifact"])
        target = self.work_dir() / (artifact.name or "result.db")
        self.download_artifact(artifact, target)
        return {"imported": str(target)}
    return self.not_handled()
```

If message delivery fails after upload, the receiver-side artifact is deleted.
With `move=True`, the source file is deleted only after upload and message
delivery both succeed.

Low-level artifacts are intentionally durable until explicit cleanup. They
remain available until they are deleted with `delete_artifact(...)` or
`paglets-artifacts delete`, downloaded with `move=True`, or expire by their
`expires_at` timestamp. `send_artifact(..., move=True)` deletes the sender's
source file after upload and message delivery; it does not delete the hosted
artifact. Disposing the owner paglet removes its scratch/work files, but it
does not delete low-level artifacts by `owner_agent_id` automatically. This
keeps collector-owned result artifacts recoverable after worker paglets are
disposed.

## CLI

Inspect and recover artifacts:

```bash
uv run paglets-artifacts list
uv run paglets-artifacts list --json
uv run paglets-artifacts metadata ARTIFACT_ID
uv run paglets-artifacts download ARTIFACT_ID result.db
uv run paglets-artifacts delete ARTIFACT_ID
```

Use `--host URL` to target a specific host or relay host URL.

## Cleanup

Incoming uploads are written to hidden `.part` files first. A failed or
interrupted receive deletes the temporary blob immediately. Host startup and a
background sweeper remove stale temporary files and abandoned relay spools.

Relay-compatible transfer uses the relay hub as a temporary spool: upload paths
are caller to hub to connected target, and download paths are connected target
to hub to caller. Hub spools are deleted after verified import/export.

Size values in docs and CLI output use binary-scaled `KB`, `MB`, and `GB`.
