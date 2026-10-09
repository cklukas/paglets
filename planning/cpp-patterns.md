# paglets/cpp: the patterns library (WP18)

Status: implemented. Companion to the plan (WP18). The Python edition's
`paglets.patterns` package is the model; the C++ guest SDK is
event-driven (handlers never block), so every pattern takes callbacks
instead of waiting. Code: header-only, `cpp/sdk/include/paglets/patterns/`
(with `paglets/patterns.hpp` including all of it); tests in
`cpp/tests/test_patterns.cpp` with the paglet `cpp/tests/guests/patterns_demo.cpp`.

Modules that use it are built with `paglets_add_module(<name> ... PATTERNS)`,
which adds the service codecs the patterns need (directory, mesh-info,
user-info, locator, files, grants).

## 1. The patterns

| Header | Pattern | Python counterpart |
|---|---|---|
| `services.hpp` | `endpoint(name, next)`: the host's default endpoint, else one from the directory; `lookup`; `lending(cap)`; `here(next)`: this host as mesh-info sees it; `error_text` | service scopes of the context |
| `task.hpp` | `Task<Request, Result>`: a paglet that runs one task; `start` (answered at once with the status), `status`, `wait` (answered when done or after `timeout_ms`); `complete(result)`, `fail(why)`; the result travels encoded in the status | `TaskPaglet` |
| `operations.hpp` | `serve<Request, Reply>(router, name, fn)`: a handler that returns the reply or an error and answers itself; `call<Reply>(endpoint, op, request, next)` | `OperationPaglet`, `OperationClient` |
| `fanout.hpp` | `FanOut`: clones of the paglet to a set of hosts, one report each (`report(parent, result)` in the clone; a clone that ends after it disposes in the `sent` callback, as the report goes out only after a mesh-info lookup), a deadline, failed clones; `select_hosts` through mesh-info | `MeshFanoutMixin` |
| `locate.hpp` | `locate`, `pin`, `release`, `with_pinned(target, duration, reason, work)`: pins, runs the work, releases when it says it is done | locator helpers |
| `notify.hpp` | `notify(level, title, text)`: fire and forget through `user-info` | `NotificationMixin` |
| `files.hpp` | single-file mobility: `granted_dir` (a directory by the mesh policy, through `grants`), `read_file`, `write_file` (in 1 MB chunks), `pick_up(root, path)` and `put_down(file, root, path)` | `file_mobility` |

Notes:

- The task's `wait` keeps the reply capability and a timer; replies are
  answered in order when the task completes or fails.
- Fan-out clones carry only the parent's endpoint (clones made on another
  host can carry nothing else); a clone that never arrives reaches the
  parent as `on_move_failed` with the clone's ID, which `clone_failed`
  turns into a failed child.
- Reading a file marks the paglet with its root (data residency,
  [cpp-residency.md](cpp-residency.md)), so a carried file only goes where
  its content may go.

## 2. Users

The Download Courier and AI Document Digest demos (WP17) and the pi example
use the same building blocks; `patterns_demo` exercises each pattern.

## 3. Exit

| Test | Covers |
|---|---|
| `test_patterns.cpp` | a task (start, wait before and after it is done, failure), a typed operation, fan-out to two other hosts and this one with a destination that does not exist, locate, a pin that refuses a dispatch and is released, a notification, a file carried from one host's root to another's |
