# Log Scout

The Log Scout gives one view of problems across the mesh without a log
collection stack. It sends a scout, a clone of itself, to every host.
Each scout searches the log files in a named root of its host for lines
with given patterns within a time window, and reports counts and
excerpts. It reads only the end of each file.

In live mode the scouts stay on their hosts. Between checks they are
inactive; at each check they read only the lines appended since the last
one and send the new matches home.

Directory: [`cpp/examples/log_scout/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/log_scout)


File selection uses the framework's background scan-task helper. The agent
stays responsive while the directory walk runs; bounded list results and
operation deadlines are separate from handler time. See
[Long-running agents and tasks](../long-running-tasks.md) for the shared
`TaskHandle` API, cancellation, progress and the example audit.

## What it shows

- **Range reads.** The `files` system paglet reads a file from an offset.
  A scout reads only the last `tail_bytes` of each log file (1 MB by
  default), and in live mode only what was appended. A log of gigabytes
  costs no more than its end.
- **State in memory while inactive.** A live scout deactivates itself
  between checks: the host stores its memory image and unloads it, and
  wakes it when the interval is over (see
  [Lifecycle, children and timers](../sdk-reference.md#lifecycle-children-and-timers)).
  The read positions are an ordinary `std::map` member. They survive in
  the stored image, so the scout needs no database and no files of its
  own to know where it stopped reading.
- **A fan-out of clones that stays.** The paglet sends one scout to every
  host with the `FanOut` pattern (see
  [The patterns library](../writing-paglets.md#the-patterns-library)).
  Each scout sends its first report like any clone. In live mode it then
  stays and calls a typed operation, `scout.live`, of the paglet at home.
  The reply tells it whether to go on.
- **Grants on every host.** Each scout asks the `grants` system paglet of
  its host for `read` on the log root. The mesh policy decides.

## Why it is useful

When something goes wrong in a system of several machines, the first
question is where errors appear and since when. The Log Scout answers it
from the logs where they are, without shipping them to a central server.
It reads little: the end of each file within the time window, and later
only the new lines. In live mode it watches the logs for as long as
needed and takes no memory or CPU on the hosts between checks.

## How it works

The paglet starts at home with a `start` message, which names the log
root, a pattern of files (`**/*.log` by default), the patterns to look
for, the time window and optionally the hosts. Without hosts, it asks
`mesh-info` for every host of the mesh. Then it sends a scout to each
host.

A scout asks `grants` for the root, finds the log files (the framework background scan-task helper)
and goes through them one by one. For each file, it decides where to
start reading. On the first scan, a file not written within the window
is skipped, and its end is remembered as the read position. A file the
scout read before is read from its read position. Otherwise, it reads
the last `tail_bytes` of the file:

<!-- snippet: cpp/examples/log_scout/log_scout.cpp#tail -->
Source: [`cpp/examples/log_scout/log_scout.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/log_scout/log_scout.cpp)

```cpp
void read_next(std::size_t i, bool first) {
    if (i >= todo_.size()) return scanned();
    const auto& e = todo_[i];
    auto at = positions_.find(e.path);
    std::uint64_t from = 0;
    if (first && scout_.window_ms > 0 && e.modified_ms < now_ms() - scout_.window_ms) {
        // Not written within the window: nothing to read now, and a live
        // scout reads only what is appended from here on.
        positions_[e.path] = e.size;
        return read_next(i + 1, first);
    }
    if (at != positions_.end() && at->second <= e.size) {
        from = at->second;  // appended since the last check
    } else if (first && scout_.tail_bytes > 0 && e.size > static_cast<std::uint64_t>(scout_.tail_bytes)) {
        from = e.size - static_cast<std::uint64_t>(scout_.tail_bytes);  // only the end
    }
    if (from >= e.size) {
        positions_[e.path] = e.size;
        return read_next(i + 1, first);
    }
    ++found_.files;
    chunk_.clear();
    read_range(i, first, from, from);
}
```
<!-- /snippet -->

It reads from that offset to the end of the file, in parts of 256 KB,
and matches the complete lines only. A line still being written has no
newline yet; the read position stays before it, so the next check reads
it whole. A scan that starts in the middle of a file drops the first,
partial line:

<!-- snippet: cpp/examples/log_scout/log_scout.cpp#range -->
Source: [`cpp/examples/log_scout/log_scout.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/log_scout/log_scout.cpp)

```cpp
// Reads `path` from `offset` to its end (in chunks), then matches the
// complete lines; a line still being written is read next time.
void read_range(std::size_t i, bool first, std::uint64_t start, std::uint64_t offset) {
    constexpr std::uint64_t chunk = 256 * 1024;
    svc::files::Client client{files_};
    auto sent = client.read(
        svc::files::ReadRequest{todo_[i].path, offset, chunk},
        [this, i, first, start, offset](paglets::Result<svc::files::ReadReply> r, Message&) {
            if (!r) return read_next(i + 1, first);  // gone or unreadable: the next file
            chunk_.append(r->data.begin(), r->data.end());
            found_.bytes_read += static_cast<std::int64_t>(r->data.size());
            if (!r->eof && !r->data.empty()) return read_range(i, first, start, offset + r->data.size());
            const auto last_newline = chunk_.rfind('\n');
            const std::size_t complete = last_newline == std::string::npos ? 0 : last_newline + 1;
            // Started in the middle of a file: its first line is partial.
            std::size_t begin = 0;
            if (start > 0 && !positions_.contains(todo_[i].path)) {
                const auto nl = chunk_.find('\n');
                begin = nl == std::string::npos ? complete : std::min(complete, nl + 1);
            }
            match(todo_[i], std::string_view(chunk_).substr(begin, complete - begin), first);
            positions_[todo_[i].path] = start + complete;
            read_next(i + 1, first);
        },
        pt::lending(*dir_));
    if (!sent) read_next(i + 1, first);
}
```
<!-- /snippet -->

A line matches a pattern if it contains it, without regard to case by
default (`ignore_case`). Within the time window, a line that starts with
a timestamp such as `2026-10-09T12:00:00` (UTC) counts by that time, and
other lines by the modification time of their file. The scout counts the
matches per pattern and keeps at most `max_excerpts` lines per pattern
(5 by default), each cut to 300 characters.

When all files are read, the scout drops the directory capability. On
its first scan, it reports to the paglet at home. Without `live` it then
ends. In live mode it stays and goes to sleep. At each later check it
sends what it found to the paglet at home with `scout.live`. If the
reply says not to go on, or the paglet at home cannot be reached, it
disposes itself:

<!-- snippet: cpp/examples/log_scout/log_scout.cpp#live -->
Source: [`cpp/examples/log_scout/log_scout.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/log_scout/log_scout.cpp)

```cpp
void scanned() {
    drop_dir();
    if (scouting_) {
        scouting_ = false;
        if (!scout_.live) return pt::report(parent_, paglets::encode(found_), {}, [] { (void)paglets::dispose(); });
        pt::report(parent_, paglets::encode(found_));
        live_ = true;
        return sleep();
    }
    // A live check: send the new lines home; the reply says whether to go on.
    found_.host_name = host_name_;
    if (host_name_.empty()) {
        pt::here([this](paglets::Result<svc::mesh_info::Snapshot> s) {
            if (s) host_name_ = s->host_name;
            found_.host_name = host_name_;
            send_home();
        });
        return;
    }
    send_home();
}

void send_home() {
    auto sent = pt::call<LiveReply>(parent_, "scout.live", found_, [this](paglets::Result<LiveReply> r) {
        if (!r || !r->go_on) {
            live_ = false;
            return (void)paglets::dispose();
        }
        sleep();
    });
    if (!sent) {
        live_ = false;
        (void)paglets::dispose();
    }
}
```
<!-- /snippet -->

To sleep, the scout deactivates itself with a wake-up time of
`interval_ms` (if the host cannot deactivate it, a timer sends it a
`scout.check` message instead). When the host wakes it, it checks. A
message can wake an inactive paglet as well; then the scout sets a timer
for the rest of the interval instead of checking early:

<!-- snippet: cpp/examples/log_scout/log_scout.cpp#wake -->
Source: [`cpp/examples/log_scout/log_scout.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/log_scout/log_scout.cpp)

```cpp
// A live scout: the host wakes it on time. A message can wake it
// earlier; then it checks on time with a timer instead.
void on_activated() override {
    if (!live_) return;
    const std::int64_t left = next_check_ - now_ms();
    if (left > std::max<std::int64_t>(5, scout_.interval_ms / 2)) {
        (void)paglets::after_raw(left, "scout.check", {});
        return;
    }
    check();
}
```
<!-- /snippet -->

At home, `scout.live` adds the counts and excerpts of each batch to the
report and keeps the latest 100 excerpts. `stop` sets the state to
`stopped`; from then on `scout.live` replies that the scouts are not to
go on, so each scout ends at its next check. `dispose` does the same and
ends the paglet at home as well.

The messages and the report:

<!-- snippet: cpp/examples/log_scout/log_scout_msgs.hpp -->
Source: [`cpp/examples/log_scout/log_scout_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/log_scout/log_scout_msgs.hpp)

```cpp
// Messages of the Log Scout demo (planning/cpp-demo-paglets.md, demo 7).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace log_scout_msgs {

// `start`: search the log files matching `files` below `root` on `hosts`
// (names or key IDs; empty: every host mesh-info knows) for lines that
// contain one of `patterns`, within the last `window_ms` (0: any time).
// A line with a leading timestamp (2026-10-09T12:00:00, UTC) counts by its
// time, other lines by their file's modification time. Only the last
// `tail_bytes` of each file are read. `live`: the scouts stay on their
// hosts, check for new lines every `interval_ms` and send them home.
struct Scout {
    std::vector<std::string> hosts;
    std::string root;
    std::string files = "**/*.log";
    std::vector<std::string> patterns;
    bool ignore_case = true;
    std::int64_t window_ms = 3'600'000;
    std::int64_t tail_bytes = 1024 * 1024;
    std::int64_t max_excerpts = 5;  // per host and pattern
    bool live = false;
    std::int64_t interval_ms = 60'000;
    std::int64_t timeout_ms = 30 * 60 * 1000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

struct Count {
    std::string host_name;
    std::string pattern;
    std::int64_t matches = 0;
};

struct Excerpt {
    std::string host_name;
    std::string path;
    std::string line;
};

// What a scout found on its host: in its report, and in live batches
// (`scout.live`, answered with a LiveReply).
struct Findings {
    std::string host_name;
    std::vector<Count> counts;
    std::vector<Excerpt> excerpts;
    std::int64_t files = 0;
    std::int64_t bytes_read = 0;
};

struct LiveReply {
    bool go_on = true;
};

// `report`: the search, then the live batches.
struct Report {
    std::string state;  // idle, searching, done, live, stopped
    std::vector<std::string> hosts;
    std::vector<Count> counts;      // per host and pattern
    std::vector<Excerpt> excerpts;  // live: the latest
    std::vector<std::string> errors;
    std::int64_t files = 0;
    std::int64_t bytes_read = 0;
    std::int64_t live_batches = 0;
    std::int64_t live_matches = 0;
};

}  // namespace log_scout_msgs
```
<!-- /snippet -->

## Running it on a live mesh

Every host to search needs a named root for its logs, here `logs`, and a
policy rule that lets the owner's paglets read it (see
[Policy](../hosts-and-meshes.md#policy)):

```bash
H=build/macos-arm64/host/paglets-host
$H ledger rule --ledger ledger --admin alice.key --name "read logs" --decision allow \
    --service files --op read --root logs
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Search the last hour on two hosts:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/log_scout.wasm)
$H remote call $R $ID start '{"hosts": ["web1", "web2"], "root": "logs", "files": "**/*.log",
    "patterns": ["error", "warn"], "window_ms": 3600000}'
$H remote call $R $ID report
```

`report` answers with `state` `searching` until every scout has reported,
then `done`, with the matches per host and pattern, the excerpts, the
number of files and the bytes read.

To follow the logs, start another Log Scout with `live`, and stop it
when it is no longer needed:

```bash
LIVE=$($H remote launch $R build/macos-arm64/guests/log_scout.wasm)
$H remote call $R $LIVE start '{"hosts": ["web1", "web2"], "root": "logs", "patterns": ["error"],
    "live": true, "interval_ms": 60000}'
$H remote call $R $LIVE report                   # state "live": the latest excerpts, live_batches, live_matches
$H remote call $R $LIVE stop                     # the scouts end at their next check
```

## Test results

The test runs the Log Scout on a mesh of three hosts inside one test
process: `home`, and `web1` and `web2` with a root `logs` each
(`example: Log Scout ...` in `cpp/tests/test_examples.cpp`). On `web1`,
`app.log` has an error two hours old, a recent error and a recent
warning, and `old.log`, last written three hours ago, has an error. On
`web2`, `app.log` has lines without timestamps, `sub/worker.log` has a
recent error, and `big.log` has 2.2 MB of other lines with an error at
the very end. The test checks that:

- the search for `error` and `warn` within the last hour, reading the
  last 64 KB of each file, counts one error and one warning on `web1`
  (not the old error, not `old.log`) and three errors and one warning on
  `web2`, among them the line at the end of `big.log`; it reads less than
  200 KB of the logs;
- a live scout on `web1` reports the recent error first, and then, after
  a line is appended to `app.log`, sends exactly that new line home;
- a line written in two parts counts once, when it is complete;
- after `stop`, the scouts end, and only the two paglets at home are
  left.

<!-- results: log_scout -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts searched | bytes read of 2.2 MB of logs | live checks while following | new lines found live |
|---|---|---|---|---|---|---|---|---|---|
| example: Log Scout searches logs in a time window and follows them live | in-process | passed | 0.8 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 2 | 65779 | 4 | 2 |
| example: Log Scout searches logs in a time window and follows them live | worker processes | passed | 0.9 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 2 | 65779 | 4 | 2 |
<!-- /results -->
