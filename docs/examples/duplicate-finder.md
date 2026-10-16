# Duplicate Finder

The Duplicate Finder finds files with the same content anywhere in the
mesh: on one host or on different hosts. It works in two phases. First
every host reports the sizes of its files. Then only the files whose size
occurs more than once are hashed, on the host that has them. The result
lists groups of identical files and the space taken by the extra copies.

Directory: [`cpp/examples/dupes/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/dupes)

## What it shows

- **Coordination in several phases.** The paglet at home runs two rounds
  of fan-out with the `FanOut` pattern (see
  [The patterns library](../writing-paglets.md#the-patterns-library)).
  The result of the first round decides the work of the second: only the
  sizes that occur more than once go out again.
- **Cheap work first.** Listing files with their sizes costs little;
  hashing costs reading every byte. Files with a size that no other file
  has cannot have a duplicate, so they are never read.
- **Content never travels.** Each clone hashes the files of its own host.
  Only paths, sizes and SHA-256 hashes go home.
- **Grants on every host.** Each clone asks the `grants` system paglet of
  its host for `read` on the root. The mesh policy decides.

## Why it is useful

Copies of the same data pile up on shared machines: downloads, exports,
backups of backups. Across several machines they are hard to find, since
no single machine sees all files. The Duplicate Finder compares the
files of every host by content without moving them, and it reads only
the files that can have a duplicate. The result says which copies exist
and how many bytes they take beyond the first.

## How it works

The paglet starts at home with a `start` message, which names the root,
a pattern of files, the smallest size to consider and optionally the
hosts. Without hosts, it asks `mesh-info` for every host of the mesh. It
then sends a clone to every host with `Phase` 1.

A clone asks `grants` for the root and finds the matching files of at
least `min_size` bytes (`files.find`). In phase 1, it reports every file
with its path and size. In phase 2, it keeps only the files whose size is
among the sizes it was given, and hashes them:

<!-- snippet: cpp/examples/dupes/dupes.cpp#find -->
Source: [`cpp/examples/dupes/dupes.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/dupes/dupes.cpp)

```cpp
svc::files::Client client{files_};
svc::files::FindRequest q;
q.pattern = phase_.search.pattern;
q.min_size = static_cast<std::uint64_t>(std::max<std::int64_t>(0, phase_.search.min_size));
q.limit = 100'000;
auto sent = client.find(
    q,
    [this](paglets::Result<svc::files::FindReply> r, Message&) {
        if (!r) return done(pt::error_text("find", r.error()));
        const std::set<std::int64_t> wanted(phase_.sizes.begin(), phase_.sizes.end());
        for (const auto& e : r->entries) {
            if (e.type != svc::files::EntryType::file) continue;
            const auto size = static_cast<std::int64_t>(e.size);
            if (phase_.phase == 2 && !wanted.contains(size)) continue;
            mine_.files.push_back(File{{}, e.path, size, {}});
        }
        if (phase_.phase == 1) return done({});
        hash(0);
    },
    pt::lending(*dir_));
if (!sent) done(pt::error_text("find", sent.error()));
```
<!-- /snippet -->

For hashing, the clone asks the host to compute the SHA-256 of one file
after another (`files.hash`). The content never enters the clone's
memory, so files of any size are hashed; files that cannot be read get no
hash and are left out of the report:

<!-- snippet: cpp/examples/dupes/dupes.cpp#hash -->
Source: [`cpp/examples/dupes/dupes.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/dupes/dupes.cpp)

```cpp
// Phase 2: the files' SHA-256, one file at a time, here.
void hash(std::size_t i) {
    if (i >= mine_.files.size()) return done({});
    // The host hashes the file; its content never enters the clone.
    pt::hash_file(*dir_, mine_.files[i].path, [this, i](paglets::Result<paglets::services::files::Hashed> h) {
        if (h) mine_.files[i].sha256 = h->sha256;
        hash(i + 1);
    });
}
```
<!-- /snippet -->

When it is done, each clone drops the directory capability, reports, and
ends two seconds later.

When every clone of phase 1 has reported or timed out, the paglet at home
counts the files of each size. The files whose size occurs more than once
are the candidates. If there are none, it is done. Otherwise it sends new
clones to the same hosts with `Phase` 2 and the list of those sizes:

<!-- snippet: cpp/examples/dupes/dupes.cpp#sizes -->
Source: [`cpp/examples/dupes/dupes.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/dupes/dupes.cpp)

```cpp
if (result_.state == "sizes") {
    result_.files = static_cast<std::int64_t>(found_.size());
    std::map<std::int64_t, int> count;
    for (const auto& f : found_) ++count[f.size];
    std::vector<std::int64_t> sizes;
    for (const auto& [size, n] : count) {
        if (n > 1) sizes.push_back(size);
    }
    result_.candidates = static_cast<std::int64_t>(
        std::ranges::count_if(found_, [&](const File& f) { return count[f.size] > 1; }));
    found_.clear();
    if (sizes.empty()) {
        result_.state = "done";
        return;
    }
    result_.state = "hashing";
    fan_.start(hosts_, paglets::encode(Phase{2, search_, std::move(sizes)}), search_.timeout_ms);
    return;
}
```
<!-- /snippet -->

When the clones of phase 2 have reported, it groups the files by hash.
Each hash shared by two or more files is a group of duplicates. The
copies beyond the first count as wasted bytes. The groups are sorted by
the space they take, the largest first:

<!-- snippet: cpp/examples/dupes/dupes.cpp#groups -->
Source: [`cpp/examples/dupes/dupes.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/dupes/dupes.cpp)

```cpp
// Hashes: groups of two or more.
std::map<std::string, Group> groups;
for (auto& f : found_) {
    auto& g = groups[f.sha256];
    g.sha256 = f.sha256;
    g.size = f.size;
    g.copies.push_back(std::move(f));
}
for (auto& [hash, g] : groups) {
    if (g.copies.size() < 2) continue;
    result_.wasted += g.size * static_cast<std::int64_t>(g.copies.size() - 1);
    std::ranges::sort(g.copies, [](const File& a, const File& b) {
        return a.host_name != b.host_name ? a.host_name < b.host_name : a.path < b.path;
    });
    result_.groups.push_back(std::move(g));
}
std::ranges::sort(result_.groups, [](const Group& a, const Group& b) {
    return a.size * static_cast<std::int64_t>(a.copies.size()) > b.size * static_cast<std::int64_t>(b.copies.size());
});
result_.state = "done";
```
<!-- /snippet -->

Hosts that could not be searched are listed in `errors`.

The messages and the result:

<!-- snippet: cpp/examples/dupes/dupes_msgs.hpp -->
Source: [`cpp/examples/dupes/dupes_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/dupes/dupes_msgs.hpp)

```cpp
// Messages of the Duplicate Finder demo (planning/cpp-demo-paglets.md, demo 4).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dupes_msgs {

// `start`: files of at least `min_size` bytes below the root `root` on every
// host (or `hosts`) that have the same content somewhere in the mesh.
struct Search {
    std::string root;
    std::string pattern = "**";
    std::vector<std::string> hosts;
    std::int64_t min_size = 1;
    std::int64_t timeout_ms = 30'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// Phase 1, what a host has (path and size), and phase 2, the hashes of the
// files whose size occurs more than once in the mesh.
struct Phase {
    std::int64_t phase = 1;
    Search search;
    std::vector<std::int64_t> sizes;  // phase 2: the sizes to hash
};

struct File {
    std::string host_name;
    std::string path;
    std::int64_t size = 0;
    std::string sha256;  // phase 2
};

struct HostFiles {
    std::vector<File> files;
};

struct Group {
    std::string sha256;
    std::int64_t size = 0;
    std::vector<File> copies;
};

struct Result {
    std::string state;  // idle, sizes, hashing, done
    std::int64_t files = 0;
    std::int64_t candidates = 0;
    std::vector<Group> groups;  // the largest waste first
    std::int64_t wasted = 0;    // bytes in copies beyond the first
    std::vector<std::string> errors;
};

}  // namespace dupes_msgs
```
<!-- /snippet -->

## Running it on a live mesh

Every host to search needs a named root, here `docs`, and a policy rule
that lets the owner's paglets read it (see
[Policy](../hosts-and-meshes.md#policy)):

```bash
H=build/macos-arm64/host/paglets-host
$H ledger rule --ledger ledger --admin alice.key --name "read docs" --decision allow \
    --service files --op read --root docs
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Then launch the Duplicate Finder on the home host and start it for every
host of the mesh, considering files of at least 1 MB:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/dupes.wasm)
$H remote call $R $ID start '{"root": "docs", "pattern": "**", "hosts": [], "min_size": 1048576}'
$H remote call $R $ID result
```

`result` answers with `state` `sizes` during phase 1, `hashing` during
phase 2 and `done` at the end, with the number of files listed, the
candidates hashed, the groups of duplicates with every copy's host and
path, and the wasted bytes.

## Test results

The test runs the Duplicate Finder on a mesh of three hosts, `a`, `b` and
`c`, inside one test process, each with a root `docs`
(`example: Duplicate Finder ...` in `cpp/tests/test_examples.cpp`). A
report of 5000 bytes is on `a` (`q3/report.txt`) and, under another name,
on `b`. Host `c` has a file of the same size with other content, and a
small file of its own. The test checks that:

- the paglet lists four files and hashes only the three of size 5000;
- it finds exactly one group: the report on `a` and on `b`, of 5000
  bytes, with the SHA-256 of its content; the file of the same size on
  `c` is not part of it;
- the extra copy wastes 5000 bytes, and no host reports an error.

<!-- results: dupes -->
| Test | Mode | Outcome | Duration | Platform | Recorded | files listed on three hosts | files hashed (same size as another) | groups of duplicates | bytes in extra copies |
|---|---|---|---|---|---|---|---|---|---|
| example: Duplicate Finder finds the same content on different hosts | in-process | passed | 0.2 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 4 | 3 | 1 | 5000 |
| example: Duplicate Finder finds the same content on different hosts | worker processes | passed | 0.3 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 4 | 3 | 1 | 5000 |
<!-- /results -->
