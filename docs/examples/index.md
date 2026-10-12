# Example paglets and benchmarks

Every directory of
[`cpp/examples/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples)
is an example paglet; this page lists all of them, and each has a page of
its own: what it shows and why, how it works with excerpts of its code, how
to run it, and its test results. Each is a useful tool that shows one part
of paglets/cpp. They also serve as tests: each runs in
CI on meshes of several hosts, both with paglets inside the host process
and in sandboxed worker processes. The
[catalogue](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-demo-paglets.md)
in the planning documents lists the planned examples and which are done.

## Getting started

| Example | Directory | What it does | Shows |
|---|---|---|---|
| [Mesh Journey](mesh-journey.md) | `journey/` | visits every host of the mesh (or the hosts with a label) and comes home with a journal of where it was | memory-image mobility: the journal is an ordinary `std::vector` |
| [Hello](hello.md), [Counter](counter.md), [Ping Pong](ping-pong.md) | `hello/`, `counter/`, `ping_pong/` | the smallest paglets: a reply, persistent state, two paglets talking | the guest SDK, schemas, capabilities |
| [Greeter and Adder](out-of-tree.md) | `out-of-tree/` | a project of its own that builds two paglets against the installed paglets package | `find_package(paglets)`, `paglets_add_module` outside this repository (see [Building and testing](../building.md#modules-outside-the-repository)) |

## Files

| Example | Directory | What it does | Shows |
|---|---|---|---|
| [Mesh File Finder and Storage Analyzer](file-finder.md) | `finder/` | sends a clone to every host; each finds matching files in a named root and adds up what it found | fan-out of clones, `grants`, `files.find` |
| [File Courier](file-courier.md) | `file_courier/` | carries files of any size from a root of one host to a root of another. It checks both hosts before leaving, shares the files on the source host and saves them on the destination, which fetches the content directly from the source and checks every file by SHA-256. It comes home with a receipt. | grants on two hosts; artifact capabilities instead of content in memory ([moving content between hosts](../system-paglets.md#moving-content-between-hosts)); data residency stops content that must stay on its host |
| [Tree Compare](tree-compare.md) | `tree_compare/` | compares a directory tree on several hosts by size or by SHA-256, and reports the files missing on some hosts and the files that differ | parallel scans; content is hashed where it is and never travels |
| [Log Scout](log-scout.md) | `log_scout/` | searches log files on every host for patterns within a time window, with counts and excerpts. In live mode the scouts stay on their hosts and send new matches home. | range reads; read positions kept in memory while the scouts are inactive between checks |
| [Duplicate Finder](duplicate-finder.md) | `dupes/` | finds files with the same content anywhere in the mesh. First every host reports file sizes. Then only files with a repeated size are hashed, on their own host. | multi-phase coordination; content never travels |

## Monitoring

| Example | Directory | What it does | Shows |
|---|---|---|---|
| [Inventory Collector, Process Finder, Mesh Top](inventory.md) | `inventory/` | collects every host's system summary, load, volumes and processes, or finds processes by name | one `server-info` schema on macOS, Linux and Windows |
| [Volume Guard](volume-guard.md) | `guard/` | watches the volumes of its host and tells the owner when one runs low on space | timers, and inactive paglets that the host wakes on time |

## Performance and mobility

| Example | Directory | What it does | Shows |
|---|---|---|---|
| [Mesh Benchmark](mesh-benchmark.md) | `benchmark/` | measures every host in a compute slot of its own: integer and floating-point speed and memory copying in the paglet, and writing and reading through its storage; ranks the hosts | compute slots, the paglet's own storage (its scratch directory) |
| [Latency Map](latency-map.md) | `latency/` | places a probe on every host; each probe pings the others and reports round-trip times, giving a matrix of host pairs | messaging between paglets across hosts |
| [Hide and Seek](hide-and-seek.md) | `seek/` | a hider keeps moving between hosts; the seeker finds it, pins it, and releases it again | `locate_and_pin` while a paglet keeps moving |
| [Pi](pi.md) | `pi/` | computes hex digits of pi in chunks on the hosts of the mesh, and survives the loss of a host | `mesh-info`, `compute-slots`, redirects |

## Web and AI

| Example | Directory | What it does | Shows |
|---|---|---|---|
| [Download Courier](download-courier.md) | `courier/` | starts on a host without internet access, downloads a file on a `web` host, and comes home with the artifact capability; the home host fetches the content from the `web` host | the web gateway, artifacts that work on every host, `offer:` tickets |
| [AI Document Digest](ai-digest.md) | `digest/` | reads documents on two hosts, summarizes them on an `ai` host, and writes the digest on a third | data residency: content that must stay on its host is never sent to the AI host |
| [Semantic Mesh Search](semantic-search.md) | `semantic/` | reads documents on their hosts, embeds them on an `ai` host, and answers questions by meaning | the index is an ordinary vector in the paglet's memory and moves with it; no database |
| [Image Describer](image-describer.md) | `describer/` | reads images on their hosts, gets a caption and tags for each on an `ai` host whose offer can see (`vision=yes`), and writes a JSON catalogue on another host | offer requirements through `mesh-info`, `ai.describe_image`, data residency |
| [Release Watcher](release-watcher.md) | `watcher/` | stays on a `web` host and checks release pages or feeds. It tells its owner about new versions and, on request, starts a Download Courier that brings the release file home. | a long-lived paglet, inactive between checks; paglets starting other paglets |
| [Log Explainer](log-explainer.md) | `explainer/` | starts a Log Scout, groups the lines it found (numbers and IDs do not count), classifies and explains each group on an `ai` host, and tells its owner | paglets cooperating: a child from another module; `ai.classify` and `ai.generate` |
| [Web Researcher](web-researcher.md) | `researcher/` | starts on a host without internet access, searches and reads pages on a `web` host, summarizes them on an `ai` host, and comes home with a report | one paglet using two gateway hosts |

## Running an example

In the tests, each example runs on a mesh of several hosts inside one test
process. The tests also show which messages start each example and what
they reply. On a real mesh, launch an example module with `remote launch` and
start it with `remote call`:

```bash
H=build/linux-gcc16/host/paglets-host
$H remote launch --connect https://lab-1:7443 --key olga.key --ledger ledger \
    build/linux-gcc16/guests/journey.wasm                          # prints the paglet ID
$H remote call --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> start '{"hosts": [], "label": ""}'
$H remote call --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> journal
```

To run the tests of the examples (the web and AI examples are among the
`gateway:` tests, Pi among the `pi:` tests):

```bash
cd cpp/build/linux-gcc16
./tests/paglets_tests guests "example:"
./tests/paglets_tests guests "gateway:"
```

## Test results

Every example page ends with the results of its tests: the outcome and
duration of each test, with paglets inside the host process and in
sandboxed worker processes, and the values the test measured. The tests
record them themselves: with `PAGLETS_RESULTS_DIR` set, each test writes
what it reports to `docs/results/<example>.json`. `paglets-docs-sync`
then writes the tables, and the code excerpts from the marked sections of
the sources, into the pages. One script does both:

```bash
cpp/tools/update_results.sh cpp/build/macos-arm64
```

The `docs_sync` test of `ctest` checks that the excerpts in the pages
match the sources. `cpp/tools/mesh_bench.sh` runs the File Courier, the
Latency Map and the Mesh Benchmark on a mesh of real machines on a network,
and records their results the same way (see
[File Courier](file-courier.md#transfer-speed-between-real-machines)).

## Benchmarks

The `bench:` tests measure request and reply, activation, memory per
paglet, and moves between hosts (see
[Building and testing](../building.md#benchmarks)).

The numbers below come from one machine: Linux x86_64 with 4 CPUs, GCC 16,
the WAMR fast interpreter, and `PAGLETS_BENCH=1`. They change with the
machine.

| Benchmark | In-process | Worker processes |
|---|---|---|
| request and reply | 15 500 per second, median 0.06 ms | 15 200 per second, median 0.05 ms |
| activation of an inactive paglet | median 0.64 ms | median 5.9 ms |
| memory per paglet | 134 KB | 46 KB (the host's share) |
| move between two hosts (median) | 31 ms | 93 ms |
| pages per move | 66 on the first, 2.2 on later moves | the same |

Page reuse works as intended: after the first move, only the pages that
changed travel. A 4 MB ballast of unchanging memory is sent once.
Activation with worker processes takes longer, because the image has to
reach the worker.

The [benchmarks page](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-benchmarks.md)
in the planning documents tracks what is still to measure.
