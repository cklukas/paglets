# Mesh File Finder and Storage Analyzer

The Mesh File Finder searches a named root on every host of the mesh at
once. It sends a clone of itself to each host; each clone finds the
matching files on its host and adds up what it found. The original puts
the answers together: the matching files of every host, the largest of
them, and the totals by file extension, which make it a storage analyzer
as well.

Directory: [`cpp/examples/finder/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/finder)

## What it shows

- **Fan-out of clones.** A clone is a copy of the paglet, made from its
  memory image, that starts with `on_cloned` instead of `on_created`. The
  `FanOut` pattern clones the paglet to a set of hosts, gives each clone
  the parent's endpoint, and collects one report from each. Clones that
  do not report before a deadline, and clones that never arrived, count
  as failed (see [The patterns library](../writing-paglets.md#the-patterns-library)).
- **Named roots and grants.** A host makes directories available to
  paglets as named roots, such as `docs`. A paglet does not open a root
  itself: it asks the `grants` system paglet of its host for a directory
  capability, and the mesh policy decides (see
  [Policy](../hosts-and-meshes.md#policy)). The finder asks for `read` on
  the root.
- **Background file tasks.** With a directory capability,
  `patterns::scan_files` returns bounded batches of matches, such as
  `**/*.csv`, with paths, sizes and modification times. Traversal runs in
  the background, reports progress through nonmatches, and can be cancelled.
- **The work goes to the data.** Each host searches its own root. Only the
  results travel, never the files.

## Why it is useful

To find where a file is, or what fills a disk, you would otherwise log in
to every machine and search there. The finder asks the whole mesh in one
call and gets one answer, in which every host has a section of its own. A
host whose policy refuses the root, or that does not answer in time, does
not stop the search: its section carries the reason, and the others are
complete.

## How it works

`start` names the root, the pattern, and optionally the hosts, a minimum
file size, the number of files to list per host and a deadline. Without
hosts, the finder asks `mesh-info` for every host of the mesh. Its own
host comes first in the landscape, and is named `""`: its clone is made on
this host and does not move. `FanOut::start` sends out the clones with the
search as their arguments, and `start` answers with the number of hosts:

<!-- snippet: cpp/examples/finder/finder.cpp#start -->
Source: [`cpp/examples/finder/finder.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/finder/finder.cpp)

```cpp
void start(const Search& s, Message& m) {
    if (s.root.empty() || s.timeout_ms <= 0 || s.max_results < 0 || s.max_results > 1000 || !fan_.finished() || starting_) {
        (void)m.reply(Started{false, "invalid search or a search is already running", 0});
        return;
    }
    findings_ = Findings{};
    starting_ = true;
    auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
    auto go = [this, s, reply](std::vector<std::string> hosts) {
        starting_ = false;
        if (findings_.cancelled) {
            findings_.finished = true;
            (void)paglets::reply_to(*reply, Started{false, "cancelled", 0});
            return;
        }
        const auto sent = fan_.start(hosts, paglets::encode(s), s.timeout_ms, "finder.cancel");
        findings_.finished = fan_.finished();
        (void)paglets::reply_to(*reply, Started{sent > 0, sent > 0 ? "" : "no clone went out",
                                                static_cast<std::int64_t>(hosts.size())});
    };
    if (!s.hosts.empty()) return go(s.hosts);
    // Every host mesh-info knows; this one by "" (no move).
    auto mi = paglets::service("mesh-info");
    if (!mi) return go({""});
    svc::mesh_info::Client client{*mi};
    auto sent = client.landscape({}, [go](paglets::Result<svc::mesh_info::Landscape> l, Message&) {
        std::vector<std::string> hosts{""};
        if (l) {
            for (std::size_t i = 1; i < l->hosts.size(); ++i) hosts.push_back(l->hosts[i].host);
        }
        go(std::move(hosts));
    });
    if (!sent) go({""});
}
```
<!-- /snippet -->

Each clone takes the parent's endpoint from the capabilities it was given
and searches its host. A clone that the host could not deliver reaches the
parent as `on_move_failed`, which `FanOut` counts as a failed child:

<!-- snippet: cpp/examples/finder/finder.cpp#cloned -->
Source: [`cpp/examples/finder/finder.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/finder/finder.cpp)

```cpp
void on_cloned(paglets::Started& s) override {
    Search q;
    if (s.caps.empty() || !s.decode_args(q)) {
        (void)paglets::dispose();
        return;
    }
    parent_ = paglets::Endpoint(s.caps[0].release());
    search_here(q);
}

void on_move_failed(const paglets::abi::MoveFailed& f) override {
    if (!f.clone.empty()) fan_.clone_failed(f);
}
```
<!-- /snippet -->

On its host, the clone asks `grants` for the root with `read`, then starts
`patterns::scan_files`, a framework service task controlled by a
`tasks::TaskHandle`. It analyzes bounded batches as they arrive, sends
progress counters, and reports its final summary before disposing itself.
An error returns the partial summary with a reason:

<!-- snippet: cpp/examples/finder/finder.cpp#search -->
Source: [`cpp/examples/finder/finder.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/finder/finder.cpp)

```cpp
void search_here(const Search& q) {
    params_ = q;
    if (auto me = paglets::self_info()) clone_id_ = me->id;
    pt::granted_dir(q.root, "", {"read"}, "mesh file finder", [this, q](paglets::Result<paglets::Capability> dir) {
        if (!dir) return finish(std::unexpected(dir.error()));
        dir_ = *dir;
        if (cancel_requested_) return finish(std::unexpected(paglets::abi::cancelled));
        pt::endpoint("files", [this, q](paglets::Result<paglets::Endpoint> files) {
            if (!files) return finish(std::unexpected(files.error()));
            if (cancel_requested_) return finish(std::unexpected(paglets::abi::cancelled));
            svc::files::FindRequest request;
            request.pattern = q.pattern;
            if (q.min_size > 0) request.min_size = static_cast<std::uint64_t>(q.min_size);
            scan_ = pt::scan_files(*files, dir_, request,
                [this](const svc::files::ScanBatch& batch) { accumulate(batch); },
                [this](paglets::Result<void> result) { finish(result); }, q.timeout_ms);
        });
    });
}
```
<!-- /snippet -->

The analysis counts only files, not directories. It keeps the first
`max_results` files by path, the five largest files, and the number and
bytes of files per extension (lower case, `""` for files without one),
largest first. Totals cover the whole successful scan. `truncated` marks
bounded returned path samples; extension overflow is combined into one
group and marked with `extensions_truncated`.

The parent handles each report as it arrives. It adds the host's
findings to the totals; a child that failed, or whose report does not
decode, gets a section with the error and any progress already received:

<!-- snippet: cpp/examples/finder/finder.cpp#collect -->
Source: [`cpp/examples/finder/finder.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/finder/finder.cpp)

```cpp
fan_.attach(router(), [this](const pt::FanOut::Child& c) {
    HostFindings h;
    if (!c.ok || !paglets::decode(c.result, h)) {
        h = HostFindings{};
        h.error = c.error.empty() ? "no report" : c.error;
        h.cancelled = findings_.cancelled;
        const auto progress = std::ranges::find(findings_.progress, c.clone, &Progress::clone);
        if (progress != findings_.progress.end()) {
            h.visited = progress->visited;
            h.files = progress->files;
            h.bytes = progress->bytes;
        }
    }
    std::erase_if(findings_.progress, [&](const auto& p) { return p.clone == c.clone; });
    h.host = c.host;
    h.host_name = c.host_name.empty() ? c.destination : c.host_name;
    findings_.files += h.files;
    findings_.bytes += h.bytes;
    findings_.hosts.push_back(std::move(h));
    findings_.finished = fan_.finished();
});
```
<!-- /snippet -->

`findings` answers with the findings so far; `finished` is true when every
child has reported or failed.

The messages and the findings:

<!-- snippet: cpp/examples/finder/finder_msgs.hpp -->
Source: [`cpp/examples/finder/finder_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/finder/finder_msgs.hpp)

```cpp
// Messages of the Mesh File Finder and Storage Analyzer demos
// (planning/cpp-demo-paglets.md, demos 2 and 3).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace finder_msgs {

// `start`: look below the named root `root` on `hosts` (empty: every host
// mesh-info knows) for files matching `pattern`.
struct Search {
    std::string root;
    std::string pattern = "**";
    std::vector<std::string> hosts;
    std::int64_t min_size = 0;
    std::int64_t max_results = 100;  // files listed per host
    std::int64_t timeout_ms = 30 * 60 * 1000;  // whole search, including clone travel
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

struct Found {
    std::string path;
    std::int64_t size = 0;
    std::int64_t modified_ms = 0;
};

struct ExtensionTotal {
    std::string extension;  // "" for files without one
    std::int64_t files = 0;
    std::int64_t bytes = 0;
};

// What one host has (the storage analysis: counts, sizes, the largest
// files, totals by extension).
struct HostFindings {
    std::string host;
    std::string host_name;
    std::string error;
    std::int64_t files = 0;
    std::int64_t bytes = 0;
    bool truncated = false;
    std::uint64_t visited = 0;
    bool cancelled = false;
    bool extensions_truncated = false;
    std::vector<Found> found;    // the first max_results, by path
    std::vector<Found> largest;  // the five largest
    std::vector<ExtensionTotal> extensions;
};

struct Progress {
    std::string clone;
    std::uint64_t visited = 0;
    std::int64_t files = 0;
    std::int64_t bytes = 0;
};

struct Findings {
    bool finished = false;
    bool cancelled = false;
    std::vector<Progress> progress;  // latest counters from unfinished clones
    std::vector<HostFindings> hosts;
    std::int64_t files = 0;
    std::int64_t bytes = 0;
};

}  // namespace finder_msgs
```
<!-- /snippet -->

## Running it

The hosts need a named root `docs` (`serve --root docs=DIR`), and a rule
that lets the owner's paglets read it (see
[Policy](../hosts-and-meshes.md#policy) and
[ledger](../cli-reference.md#ledger)):

```bash
H=build/macos-arm64/host/paglets-host
$H ledger rule --ledger ledger --admin alice.key --name "read docs" --decision allow \
    --service files --op read --root docs
```

`remote push` sends the rule to a live host, and the hosts spread it by
gossip. Then launch the finder on one host and start it:

```bash
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/finder.wasm)
$H remote call $R $ID start '{"root": "docs", "pattern": "**/*.csv"}'
$H remote call $R $ID findings
```

Fields left out of the body keep their defaults: every host, no minimum
size, 100 files listed per host, and an overall deadline of 30 minutes.
Set `timeout_ms` explicitly for longer searches. Call `findings` while the
search runs: `progress` holds each unfinished clone's latest visited-entry,
matching-file and byte counters. Completed hosts include their final
`visited` count. Progress is sent at most four times per second, plus the
final update; an unknown tree size has no meaningful percentage estimate.

Send `cancel` to finish collection immediately and ask unfinished clones
to cancel their task handles. Cancellation is best effort across the mesh;
each clone also enforces its own scan deadline, and ending a clone cancels
its native scan. The parent reports `cancelled` and retains completed
results and the latest received counters for unfinished hosts. A deadline
also requests cancellation. Starting a second search while one is active
is refused; the parent can be reused after completion or cancellation.

Totals and extension counts cover all files visited by a successful scan,
including beyond the old 10000-entry result limit. `truncated` now describes
the returned path samples, not an incomplete scan: samples are limited by
`max_results` (0 to 1000) and an 8 KB path budget. The largest-file list has
at most five entries and an 8 KB path budget. Extension groups use at most
128 named groups and a 4 KB budget, with overflow combined into
`(other extensions)` and `extensions_truncated` set. These bounds keep
results and agent memory small while totals remain complete. An error or
cancellation makes that host's counts partial.

An operator may enable a handler watchdog, such as five seconds; it does not limit scan duration. Every batch
is handled in a fresh short callback; filesystem work happens on the host's
background pool. The helper supplies progress exchanges, cancellation and
cleanup through `tasks::TaskHandle`. See
[Long-running agents and tasks](../long-running-tasks.md) for why these
budgets differ and how to use the same lifecycle for CPU work.

## Test results

The test `example: Mesh File Finder and Storage Analyzer search every
host's root` in `cpp/tests/test_examples.cpp` runs a mesh of three hosts,
`a`, `b` and `c`, inside one test process. Each host has a root `docs`:

| Host | Files in `docs` | Label `open` |
|---|---|---|
| `a` | `report.csv` (1000 bytes), `deep/data.csv` (5000 bytes), `notes.txt` (10 bytes) | yes |
| `b` | `big.csv` (20 000 bytes) | yes |
| `c` | `other.bin` (300 bytes) | no |

A policy rule allows `read` on `docs` only on hosts with the label `open`.
The finder searches for `**/*.csv` on every host:

- `start` is accepted for three hosts, and the findings have a section for
  each;
- three files with 26 000 bytes in all are found;
- on `a`, two files: the list starts with `deep/data.csv`, the largest
  file has 5000 bytes, and the only extension is `csv`. `notes.txt` does
  not match;
- on `b`, one file of 20 000 bytes;
- on `c`, the policy refuses the root, and its section holds an error
  with `denied`.

<!-- results: finder -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts searched | matching files found | bytes in matching files | hosts that refused access (policy) |
|---|---|---|---|---|---|---|---|---|---|
| example: Mesh File Finder and Storage Analyzer search every host's root | in-process | passed | 0.2 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 3 | 3 | 26000 | 1 |
| example: Mesh File Finder and Storage Analyzer search every host's root | worker processes | passed | 0.2 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 3 | 3 | 26000 | 1 |
<!-- /results -->
