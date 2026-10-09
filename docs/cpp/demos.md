# Demos and benchmarks

The demo paglets in
[`cpp/examples/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples)
are useful tools, each showing one part of paglets/cpp. They also serve as
tests: each runs in CI on meshes of several hosts, both with paglets inside
the host process and in sandboxed worker processes. The
[demo catalogue](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-demo-paglets.md)
lists the planned demos and which are done.

## Getting started

| Demo | Directory | What it does | Shows |
|---|---|---|---|
| Mesh Journey | `journey/` | visits every host of the mesh (or the hosts with a label) and comes home with a journal of where it was | memory-image mobility: the journal is an ordinary `std::vector` |
| Hello, Counter, Ping Pong | `hello/`, `counter/`, `ping_pong/` | the smallest paglets: a reply, persistent state, two paglets talking | the guest SDK, schemas, capabilities |

## Files

| Demo | Directory | What it does | Shows |
|---|---|---|---|
| Mesh File Finder and Storage Analyzer | `finder/` | sends a clone to every host; each finds matching files in a named root and adds up what it found | fan-out of clones, `grants`, `files.find` |
| Duplicate Finder | `dupes/` | finds files with the same content anywhere in the mesh. First every host reports file sizes. Then only files with a repeated size are hashed, on their own host. | multi-phase coordination; content never travels |

## Monitoring

| Demo | Directory | What it does | Shows |
|---|---|---|---|
| Inventory Collector, Process Finder, Mesh Top | `inventory/` | collects every host's system summary, load, volumes and processes, or finds processes by name | one `server-info` schema on macOS, Linux and Windows |
| Volume Guard | `guard/` | watches the volumes of its host and tells the owner when one runs low on space | timers, and inactive paglets that the host wakes on time |

## Performance and mobility

| Demo | Directory | What it does | Shows |
|---|---|---|---|
| Latency Map | `latency/` | places a probe on every host; each probe pings the others and reports round-trip times, giving a matrix of host pairs | messaging between paglets across hosts |
| Hide and Seek | `seek/` | a hider keeps moving between hosts; the seeker finds it, pins it, and releases it again | `locate_and_pin` while a paglet keeps moving |
| Pi | `pi/` | computes hex digits of pi in chunks on the hosts of the mesh, and survives the loss of a host | `mesh-info`, `compute-slots`, redirects |

## Web and AI

| Demo | Directory | What it does | Shows |
|---|---|---|---|
| Download Courier | `courier/` | starts on a host without internet access, downloads a file on a `web` host, and brings it home as an artifact | the web gateway, artifacts, `offer:` tickets |
| AI Document Digest | `digest/` | reads documents on two hosts, summarizes them on an `ai` host, and writes the digest on a third | data residency: content that must stay on its host is never sent to the AI host |
| Semantic Mesh Search | `semantic/` | reads documents on their hosts, embeds them on an `ai` host, and answers questions by meaning | the index is an ordinary vector in the paglet's memory and moves with it; no database |
| Web Researcher | `researcher/` | starts on a host without internet access, searches and reads pages on a `web` host, summarizes them on an `ai` host, and comes home with a report | one paglet using two gateway hosts |

## Running a demo

In the tests, each demo runs on a mesh of several hosts inside one test
process. The tests also show which messages start each demo and what they
reply. On a real mesh, launch a demo module with `remote launch` and
start it with `remote call`:

```bash
H=build/linux-gcc16/host/paglets-host
$H remote launch --connect https://lab-1:7443 --key olga.key --ledger ledger \
    build/linux-gcc16/guests/journey.wasm                          # prints the paglet ID
$H remote call --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> start '{"hosts": [], "label": ""}'
$H remote call --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> journal
```

To run the demo tests:

```bash
cd cpp/build/linux-gcc16
./tests/paglets_tests guests "demo:"
./tests/paglets_tests guests "gateway:"
```

## Benchmarks

The `bench:` tests measure request and reply, activation, memory per
paglet, and moves between hosts (see
[Building and testing](building.md#benchmarks)).

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
