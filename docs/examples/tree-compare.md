# Tree Compare

Tree Compare answers a common question: is a directory tree the same on
several hosts? It sends a clone of itself to every host. Each clone lists
the tree in a named root of its host and, if asked, hashes every file
there. The paglet at home then reports the files that are missing on some
hosts and the files that differ in size or in content.

Directory: [`cpp/examples/tree_compare/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/tree_compare)


File selection uses the framework's background scan-task helper. The agent
stays responsive while the directory walk runs; bounded list results and
operation deadlines are separate from handler time. See
[Long-running agents and tasks](../long-running-tasks.md) for the shared
`TaskHandle` API, cancellation, progress and the example audit.

## What it shows

- **Parallel scans with a fan-out of clones.** The paglet clones itself
  to every host with the `FanOut` pattern (see
  [The patterns library](../writing-paglets.md#the-patterns-library)).
  All clones scan at the same time, and each sends one report home. A
  clone that does not report within the deadline counts as failed, so one
  slow or lost host does not hold up the comparison.
- **Content is hashed where it is.** With `hashes`, each clone computes
  the SHA-256 of its files on its own host. Only paths, sizes, times and
  hashes travel home; the content of the files never leaves its host.
- **Grants on every host.** Each clone asks the `grants` system paglet of
  its host for `read` on the root. The mesh policy of that host decides;
  nothing is configured in the paglet.
- **Hosts from `mesh-info`.** Without a list of hosts, the paglet asks
  `mesh-info` for every host of the mesh.

## Why it is useful

Configuration, deployed software and copies of data sets are meant to be
the same on several machines, and often they are not. Tree Compare finds
the differences without copying any file and without a shared drive. A
comparison by size is quick; a comparison by SHA-256 also finds files of
the same size whose content differs. Each host reads only its own files.

## How it works

The paglet starts at home with a `start` message, which names the root, a
path below it, a pattern of files, and optionally the hosts. Without
hosts, it asks `mesh-info` for the landscape of the mesh. The first host
of the landscape is this host, which it names by an empty destination (no
move). Then it sends a clone to every host and replies:

<!-- snippet: cpp/examples/tree_compare/tree_compare.cpp#start -->
Source: [`cpp/examples/tree_compare/tree_compare.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/tree_compare/tree_compare.cpp)

```cpp
void start(const Compare& q, Message& m) {
    if (q.root.empty()) {
        (void)m.reply(Started{false, "no root", 0});
        return;
    }
    if (report_.state == "scanning") {
        (void)m.reply(Started{false, "already scanning", 0});
        return;
    }
    report_ = Report{"scanning"};
    auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
    auto go = [this, q, reply](std::vector<std::string> hosts) {
        if (hosts.size() < 2) {
            report_.state = "idle";
            (void)paglets::reply_to(*reply, Started{false, "at least two hosts needed", 0});
            return;
        }
        destinations_ = hosts;
        scans_.assign(hosts.size(), Scan{});
        const auto sent = fan_.start(hosts, paglets::encode(q), q.timeout_ms);
        (void)paglets::reply_to(*reply, Started{sent > 0, sent > 0 ? "" : "no clone went out",
                                                static_cast<std::int64_t>(hosts.size())});
    };
    if (!q.hosts.empty()) return go(q.hosts);
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

At least two hosts are needed. Each clone gets the `Compare` request as
its arguments and the endpoint of the paglet at home. On its host, it
asks `grants` for the directory with `read`, finds the matching files
(the framework background scan-task helper, at most 10000 entries and 512 KB), and keeps the path, size and
modification time of each. With `hashes`, it then asks the host to hash
one file after another (`files.hash`). Files larger than `max_file_bytes`
(64 GB by default) are not hashed. When it is done, it drops the
directory capability, reports its tree and ends:

<!-- snippet: cpp/examples/tree_compare/tree_compare.cpp#scan -->
Source: [`cpp/examples/tree_compare/tree_compare.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/tree_compare/tree_compare.cpp)

```cpp
void scan_here(const Compare& q) {
    pt::granted_dir(q.root, q.path, {"read"}, "tree compare", [this, q](paglets::Result<paglets::Capability> dir) {
        if (!dir) return finish_error(pt::error_text("files of " + q.root, dir.error()));
        dir_ = std::make_shared<paglets::Capability>(*dir);
        pt::endpoint("files", [this, q](paglets::Result<paglets::Endpoint> files) {
            if (!files) return finish_error(pt::error_text("files", files.error()));
            svc::files::FindRequest f;
            f.pattern = q.pattern;
            f.limit = 10'000;
            (void)paglets::patterns::find_files(*files, *dir_, f,
                [this, q](paglets::Result<svc::files::FindReply> r) {
                    if (!r) return finish_error(pt::error_text("find", r.error()));
                    tree_.truncated = r->truncated;
                    for (const auto& e : r->entries) {
                        if (e.type != svc::files::EntryType::file) continue;
                        tree_.entries.push_back(TreeEntry{e.path, static_cast<std::int64_t>(e.size), e.modified_ms, {}});
                    }
                    if (!q.hashes) return finish();
                    max_file_bytes_ = static_cast<std::uint64_t>(std::max<std::int64_t>(0, q.max_file_bytes));
                    hash_next(0);
                });
        });
    });
}

void hash_next(std::size_t i) {
    while (i < tree_.entries.size() && static_cast<std::uint64_t>(tree_.entries[i].size) > max_file_bytes_) ++i;
    if (i >= tree_.entries.size()) return finish();
    // The host hashes the file; its content never enters the clone.
    pt::hash_file(*dir_, tree_.entries[i].path, [this, i](paglets::Result<paglets::services::files::Hashed> h) {
        if (h) tree_.entries[i].sha256 = h->sha256;
        hash_next(i + 1);
    });
}
```
<!-- /snippet -->

> [!NOTE]
> `files.hash` computes the SHA-256 on the host, so files of any size are
> hashed without entering the clone's memory. The content
> does not travel; only the hash goes home.

When every host has reported or timed out, the paglet at home groups the
entries of all hosts by path. A path that some scanned hosts lack is
`missing`, and the report lists the hosts without it. A path that all
hosts have is a `size` difference if the sizes differ, and a `content`
difference if the hashes differ. All other paths count as `same`:

<!-- snippet: cpp/examples/tree_compare/tree_compare.cpp#compare -->
Source: [`cpp/examples/tree_compare/tree_compare.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/tree_compare/tree_compare.cpp)

```cpp
for (const auto& [path, have] : by_path) {
    ++report_.files;
    Difference d;
    d.path = path;
    for (const auto& [i, e] : have) d.copies.push_back(Copy{scans_[i].name, e->size, e->modified_ms, e->sha256});
    if (have.size() < scanned.size()) {
        d.kind = "missing";
        for (const auto i : scanned) {
            if (std::ranges::none_of(have, [i](const auto& h) { return h.first == i; }))
                d.missing_on.push_back(scans_[i].name);
        }
    } else if (std::ranges::any_of(have, [&](const auto& h) { return h.second->size != have.front().second->size; })) {
        d.kind = "size";
    } else if (std::ranges::any_of(have, [&](const auto& h) { return h.second->sha256 != have.front().second->sha256; })) {
        d.kind = "content";
    } else {
        ++report_.same;
        continue;
    }
    if (report_.differences.size() < max_differences) {
        report_.differences.push_back(std::move(d));
    } else {
        report_.truncated = true;
    }
}
```
<!-- /snippet -->

Without `hashes`, every hash is empty, so the comparison is by size only.
The same holds for files above `max_file_bytes`. The report keeps at most
1000 differences and sets `truncated` beyond that, or when a host found
more files than the bounded list helper returned. Hosts that could not be scanned
(no grant, no report in time) are listed in `errors` and take no part in
the comparison.

The messages and the report:

<!-- snippet: cpp/examples/tree_compare/tree_compare_msgs.hpp -->
Source: [`cpp/examples/tree_compare/tree_compare_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/tree_compare/tree_compare_msgs.hpp)

```cpp
// Messages of the Tree Compare demo (planning/cpp-demo-paglets.md, demo 6).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace tree_compare_msgs {

// `start`: compare the files matching `pattern` below `root`/`path` on
// `hosts` (names or key IDs; empty: every host mesh-info knows).
// `hashes`: compare content by SHA-256 too, not only sizes.
struct Compare {
    std::vector<std::string> hosts;
    std::string root;
    std::string path;
    std::string pattern = "**";
    bool hashes = false;
    std::int64_t max_file_bytes = 64LL * 1024 * 1024 * 1024;  // larger files are not hashed
    std::int64_t timeout_ms = 30 * 60 * 1000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

// A clone's report: the tree on its host.
struct TreeEntry {
    std::string path;
    std::int64_t size = 0;
    std::int64_t modified_ms = 0;
    std::string sha256;  // with `hashes`
};

struct HostTree {
    std::vector<TreeEntry> entries;
    bool truncated = false;
};

// One host's copy of a file that differs.
struct Copy {
    std::string host_name;
    std::int64_t size = 0;
    std::int64_t modified_ms = 0;
    std::string sha256;
};

struct Difference {
    std::string path;
    std::string kind;                     // missing, size, content
    std::vector<std::string> missing_on;  // missing: the hosts without it
    std::vector<Copy> copies;             // the hosts that have it
};

// `report`: the comparison, once every host answered (or timed out).
struct Report {
    std::string state;               // idle, scanning, done
    std::vector<std::string> hosts;  // compared, in the order asked
    std::int64_t files = 0;          // distinct paths
    std::int64_t same = 0;           // paths equal on every host
    std::vector<Difference> differences;
    std::vector<std::string> errors;  // hosts that could not be scanned
    bool truncated = false;
};

}  // namespace tree_compare_msgs
```
<!-- /snippet -->

## Running it on a live mesh

Every host to compare needs a named root, here `conf`, and a policy rule
that lets the owner's paglets read it (see
[Policy](../hosts-and-meshes.md#policy)). An admin adds the rule and
pushes it to a host, which spreads it through the mesh:

```bash
H=build/macos-arm64/host/paglets-host
$H ledger rule --ledger ledger --admin alice.key --name "read conf" --decision allow \
    --service files --op read --root conf
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Then launch the paglet on the home host and start it. An empty `hosts`
list compares every host of the mesh:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/tree_compare.wasm)
$H remote call $R $ID start '{"hosts": [], "root": "conf", "path": "", "pattern": "**", "hashes": true}'
$H remote call $R $ID report
```

`report` answers with `state` `scanning` until every host has answered,
then with `done`, the hosts compared, the number of distinct paths, the
paths that are the same everywhere, and the differences with each host's
size, modification time and hash.

## Test results

The test runs Tree Compare on a mesh of three hosts, `a`, `b` and `c`,
inside one test process (`example: Tree Compare ...` in
`cpp/tests/test_examples.cpp`). Each host has a root `conf`: `app.conf`
has another size on `c`; `shared/readme.txt` is the same everywhere;
`data.bin` has the same size but other content on `a` and `b`, and is
missing on `c`; `extra.txt` exists only on `b`. The test checks that:

- by size on all three hosts, it compares four paths, finds one the same
  (`data.bin` differs only in content) and three differences: `app.conf`
  of another size with three copies, `data.bin` missing on `c`, and
  `extra.txt` missing on `a` and `c`;
- by SHA-256 on this host and `b` only, it finds two paths the same and
  two differences, among them `data.bin` as a `content` difference whose
  hash on `a` is the SHA-256 of its content;
- no host reports an error.

<!-- results: tree_compare -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts compared by size | files compared | differences by size (missing or other size) | differences by content (two hosts, SHA-256) |
|---|---|---|---|---|---|---|---|---|---|
| example: Tree Compare finds missing and different files on several hosts | in-process | passed | 0.1 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 3 | 4 | 3 | 2 |
| example: Tree Compare finds missing and different files on several hosts | worker processes | passed | 0.3 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 3 | 4 | 3 | 2 |
<!-- /results -->
