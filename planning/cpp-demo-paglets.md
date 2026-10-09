# paglets/cpp: demo paglets

Status: proposal for review. Companion to [cpp-edition-plan.md](cpp-edition-plan.md)
and [cpp-security-and-communication.md](cpp-security-and-communication.md).

Demo paglets serve three purposes at once: they are **useful tools** for a real
mesh, they **show off** a specific paglets/cpp feature, and they are **tests**
that run in CI against a multi-host setup. Every demo is a roaming paglet built
with the guest SDK, uses only system paglets and grants for host access, and
ships with a `paglets demo <name>` CLI command.

Each entry lists what it does, why it is useful, which features it
demonstrates and which grants it needs (as declared in its capability
manifest).

## Overview

Status (WP20): demos marked with a directory are implemented in
`cpp/examples/` and tested on meshes of several hosts in
`cpp/tests/test_demos.cpp` (the AI and web demos in `test_gateway.cpp`, the
pi example in `test_compute.cpp`).

| # | Demo | Category | Main features shown | Status |
|---|---|---|---|---|
| 1 | Mesh Journey | Getting started | Memory-image mobility, itinerary | `journey/` |
| 2 | Mesh File Finder | Files | Clone fan-out, `files.find`, grants follow the paglet | `finder/` |
| 3 | Storage Analyzer | Files | Distributed aggregation, named roots | `finder/` (the analysis) |
| 4 | Duplicate Finder | Files | Multi-phase coordination, hashing, artifacts | `dupes/` |
| 5 | File Courier | Files | Grants on two hosts, artifacts, manifest preflight | |
| 6 | Tree Compare | Files | Parallel scans, diff reports | |
| 7 | Log Scout | Files | Range reads, time filters, live mode via pubsub | |
| 8 | Mesh Top | Monitoring | `server-info`, pubsub, long-lived paglets | `inventory/` (load and top processes) |
| 9 | Volume Guard | Monitoring | Timers, inactive paglets, user-info alerts | `guard/` |
| 10 | Inventory Collector | Monitoring | Uniform cross-platform schemas, reports | `inventory/` |
| 11 | Process Finder | Monitoring | `server-info.processes`, fan-out | `inventory/` (with a process name) |
| 12 | Mesh Benchmark | Performance | Dedicated workers, scratch storage, rankings | |
| 13 | Latency Map | Performance | Paglet-to-paglet messaging, host pair matrix | `latency/` |
| 14 | Pi Marathon | Compute | compute-slots, checkpoints, resume after host loss | `pi/` (chunks, slots, loss of a host) |
| 15 | Hide and Seek | Mobility | `locate_and_pin`, continuous movement, stress test | `seek/` |
| 16 | AI Document Digest | AI | Find anywhere, move to the AI host, deliver elsewhere | `digest/` |
| 17 | Semantic Mesh Search | AI | Embeddings, index in paglet memory, natural-language queries | |
| 18 | Image Describer | AI | Vision models, offers with requirements | |
| 19 | Log Explainer | AI | Cooperation with Log Scout, grouping and explaining errors | |
| 20 | Download Courier | Web | Download through a gateway host, delivery to the starting host | `courier/` |
| 21 | Web Researcher | Web + AI | Search and fetch on a web host, summarize on an AI host | |
| 22 | Release Watcher | Web | Periodic checks from a web host, notifications, hand-off to Download Courier | |

## Getting started

### 1. Mesh Journey

- **What it does**: visits every host of the mesh (or a label selection) in
  turn. On each host it records host name, OS, arrival time, load and the
  transfer time in an in-memory journal, then moves on. At the end it returns
  to the starting host and prints the journal.
- **Why useful**: the "hello world" of paglets/cpp and a quick mesh health
  check: every host that can receive and run paglets shows up in the journal.
- **Shows**: memory-image mobility (the journal is an ordinary C++ vector, no
  serialization code), itineraries, `arrived` events, module fetch-on-miss on
  hosts that have never seen the module.
- **Grants**: `server-info: summary, load` on all hosts.

## Files

### 2. Mesh File Finder

- **What it does**: distributed file search. Clones itself to every selected
  host; each clone runs `files.find` in the granted named roots with name
  patterns, size and modification-time filters, and optionally a content match
  on text files (bounded range reads). Results stream back to the originator as
  they are found; the CLI shows them live and can export CSV/JSON.
- **Why useful**: "where is that file?" across all lab machines in one command,
  without logging into each host.
- **Shows**: clone fan-out, grants following clones to each host, transfer
  preflight (hosts without a matching grant are skipped and reported), result
  streaming via messages.
- **Grants**: `files: find, read` on the selected named roots.

### 3. Storage Analyzer

- **What it does**: per host and named root, sums bytes and file counts by
  file type, size class and age (like `du` with statistics), then aggregates
  a mesh-wide report: largest directories, largest files, space by type,
  old data candidates.
- **Why useful**: find what fills the disks across the mesh and what could be
  cleaned up or archived.
- **Shows**: distributed aggregation, named roots mapped differently per
  platform, report output as an artifact.
- **Grants**: `files: find, stat` on the selected roots.

### 4. Duplicate Finder

- **What it does**: finds duplicate files across hosts in phases: collect sizes
  everywhere, keep only sizes that occur more than once, hash only those
  candidates (partial hash first, then full hash), and report duplicate groups
  with host, path and wasted space.
- **Why useful**: finds redundant copies of data sets and installers spread
  over many machines.
- **Shows**: multi-phase coordination between clones, work avoidance, large
  intermediate results as artifacts.
- **Grants**: `files: find, stat, read` on the selected roots.

### 5. File Courier

- **What it does**: travels to a source host, picks up files matching a
  pattern, carries them as artifacts to a destination host and writes them
  into a destination root, then reports a transfer receipt with checksums.
- **Why useful**: move result files between machines without shared drives or
  manual copying; the successor of the Python file grabber.
- **Shows**: different grants on two hosts (read on the source, write on the
  destination), manifest preflight before departure, artifact mobility,
  checksum verification.
- **Grants**: `files: find, read` on the source root; `files: write, mkdir` on
  the destination root.

### 6. Tree Compare

- **What it does**: compares a directory tree on two or more hosts (sizes,
  modification times, optional content hashes) and reports missing, extra and
  different files. Can hand the differences to the File Courier to
  synchronize.
- **Why useful**: check that data sets, configurations or deployments are
  identical across machines.
- **Shows**: parallel scans on several hosts, diff reporting, paglets
  cooperating (Tree Compare passing a derived capability to File Courier).
- **Grants**: `files: find, stat` (plus `read` for hash mode) on the compared
  roots.

### 7. Log Scout

- **What it does**: searches log files across hosts for patterns within a time
  window (for example "errors in the last hour"), counts matches per host and
  pattern, and shows excerpts. A live mode stays on each host, reads only new
  content, and publishes matches to a pubsub topic.
- **Why useful**: one view of problems across all machines, without a log
  collection stack.
- **Shows**: range reads, persistent read positions in paglet memory, pubsub
  publishing, long-lived paglets that deactivate between checks.
- **Grants**: `files: find, read` on log roots; `pubsub: publish` on the
  topic.

## Monitoring

### 8. Mesh Top

- **What it does**: live mesh-wide overview like `top`: CPU load per core,
  memory, free space per volume, number of active paglets and workers, per
  host, refreshed every few seconds in the CLI.
- **Why useful**: the everyday "how busy is my mesh" dashboard.
- **Shows**: one small sampler paglet per host publishing to a pubsub topic;
  the CLI subscribes; `server-info` with uniform schemas on macOS, Linux and
  Windows.
- **Grants**: `server-info: summary, load, volumes`; `pubsub: publish` on the
  topic.

### 9. Volume Guard

- **What it does**: watches free space on all volumes of the selected hosts
  and sends a user-info notification when a volume falls below a threshold or
  fills up quickly (trend over the last checks).
- **Why useful**: early warning before a disk runs full and a long job fails.
- **Shows**: timers, paglets that stay inactive (stored as images) between
  checks and cost nothing while waiting, user-info notifications.
- **Grants**: `server-info: volumes`; `user-info: notify`.

### 10. Inventory Collector

- **What it does**: collects a hardware and software inventory from every
  host: OS and version, CPU model and cores, memory, volumes and capacities,
  paglets/cpp and WAMR version, labels. Produces a mesh-wide table and a
  CSV/JSON artifact, and can diff against a previous inventory.
- **Why useful**: documentation of the machine park, and a quick way to spot
  hosts with outdated software.
- **Shows**: identical result schemas across platforms, report artifacts,
  comparison with a stored earlier result.
- **Grants**: `server-info: summary, volumes`.

### 11. Process Finder

- **What it does**: finds processes by name or command-line pattern across the
  mesh and shows host, PID, CPU and memory usage and run time; can repeat
  periodically to follow a long-running job.
- **Why useful**: "which machines are still running my analysis?" without
  logging in anywhere.
- **Shows**: fan-out with a read-only system service, periodic repetition.
- **Grants**: `server-info: processes`.

## Performance

### 12. Mesh Benchmark

- **What it does**: runs a comparable benchmark on every host: integer and
  floating-point CPU, memory bandwidth, disk read/write in the paglet's scratch
  directory, plus the paglets/cpp numbers: activation time, message
  throughput, move latency and image transfer size with and without page
  dedupe. Produces a ranking table.
- **Why useful**: know which machines are fast for which kind of work, and
  track paglets/cpp performance across releases; successor of the Python
  performance and mesh movement benchmarks.
- **Shows**: dedicated workers, scratch storage, one-benchmark-at-a-time per
  host, image dedupe statistics.
- **Grants**: default grants only (own scratch directory); optional
  `compute-slots: request` for a dedicated worker.

### 13. Latency Map

- **What it does**: places a small responder on every host and measures
  message round-trip time and throughput between all host pairs, directly and
  through relaying peers. Shows a matrix and highlights slow links.
- **Why useful**: network diagnosis for the mesh, for example after moving a
  host behind NAT or into another site.
- **Shows**: paglet-to-paglet messaging with endpoint capabilities and reply
  capabilities, relayed versus direct paths.
- **Grants**: default grants only.

## Compute

### 14. Pi Marathon

- **What it does**: computes many digits of pi split into chunks scheduled
  through compute-slots across the mesh. Each worker paglet checkpoints after
  every chunk. A `--chaos` option shuts down a random host mid-run; the
  affected paglets resume from their checkpoints elsewhere and the result is
  still correct and complete.
- **Why useful**: demonstrates (and continuously tests) that long jobs survive
  host loss, which the Python edition cannot do; successor of the Python pi
  compute example.
- **Shows**: compute-slots, checkpoints, resume after host loss, live
  rebalancing, collector paglet with user-info result output.
- **Grants**: `compute-slots: request`; `user-info: notify`.

## Mobility

### 15. Hide and Seek

- **What it does**: one or more "hider" paglets keep moving between random
  hosts; a "seeker" repeatedly uses `locate_and_pin` to catch a hider, talks to
  it while it is pinned, releases it and continues. Reports catches, locate
  times and redirect hops.
- **Why useful**: a stress test for location, pinning and movement under
  constant change; runs well as a long CI soak test.
- **Shows**: `locate`, `locate_and_pin`, pin leases, redirects, distributed
  location records while hosts join and leave.
- **Grants**: `locator: locate, pin` on the hiders' endpoints (passed from
  the creator).

## AI

These demos use the `ai` system paglet, which only runs on hosts that can
provide it (for example a macOS 27 (Golden Gate) host with Apple Foundation
Models, or any host running Ollama) and announces this as a service offer. The
paglets find files anywhere in the mesh, **move to where the AI is**, and
deliver the results somewhere else. File content never goes to a cloud
service, and data residency rules decide which AI hosts may see which content.

### 16. AI Document Digest

- **What it does**: finds documents matching a pattern and filters (for
  example `*.md`, `*.txt`, reports changed this week) on all selected hosts,
  collects their content, moves to the best host offering `ai.summarize`,
  creates a summary per document and an overall digest (key points, open
  questions, per-host overview), then moves to a destination host and writes
  the digest as Markdown plus a JSON index into a destination root. The owner
  gets a user-info notification with the location.
- **Why useful**: a weekly "what changed in our documents" digest across all
  machines, produced with local AI only.
- **Shows**: the full mobile-agent idea in one paglet: clone fan-out for
  finding, memory-image mobility carrying the collected content, offer-based
  placement (`ai.summarize`, shortest queue), grants on three kinds of hosts,
  data residency (documents from a `label:trusted-ai` root only go to AI hosts
  with that label; the digest carries the same mark).
- **Grants**: `files: find, read` on the source roots; `ai: summarize` on AI
  hosts; `files: write, mkdir` on the destination root; `user-info: notify`.

### 17. Semantic Mesh Search

- **What it does**: builds a semantic index of documents across the mesh:
  collects text chunks, moves to a host offering `ai.embed`, computes
  embeddings and keeps the index in its own memory (the image is the index).
  Queries in natural language ("where did we describe the calibration
  procedure?") return ranked documents with host and path; optionally a
  short answer with citations is generated on an AI host. The index paglet
  deactivates between queries and is refreshed incrementally.
- **Why useful**: search by meaning instead of file names over all machines,
  without a separate search server.
- **Shows**: large state held naturally in paglet memory, inactive paglets as
  cheap long-lived services, incremental updates, `ai.embed` and
  `ai.generate`.
- **Grants**: `files: find, read`; `ai: embed, generate`; `storage` for
  exported snapshots.

### 18. Image Describer

- **What it does**: finds images (photos, plots, screenshots) on selected
  hosts, moves to a host whose offer includes a vision-capable model, creates
  captions and tags per image, and writes a sidecar JSON per folder or a
  combined catalogue on the destination host.
- **Why useful**: makes large image collections searchable by content.
- **Shows**: offer requirements (`vision=true`, minimum model size), artifacts
  for large binary inputs, `ai.describe_image`.
- **Grants**: `files: find, read` on the image roots; `ai: describe_image`;
  `files: write` on the destination root.

### 19. Log Explainer

- **What it does**: takes the matches found by Log Scout (demo 7), moves them
  to an AI host, groups similar errors, explains the likely cause of each
  group in plain language, and publishes the result to the Log Scout topic or
  sends it to the owner.
- **Why useful**: turns hundreds of log lines from many machines into a short
  list of distinct problems.
- **Shows**: paglets cooperating through pubsub and capability passing,
  `ai.classify` and `ai.generate`.
- **Grants**: `pubsub: subscribe, publish` on the Log Scout topic; `ai:
  classify, generate`.

## Web

These demos use the `web` system paglet, which only runs on hosts allowed to
reach the internet or specific sites (for example a host on a firewall
allowlist) and announces its reachable destinations as a service offer. The
paglets move to such a gateway host, get the content there, and carry it back
into the intranet. No other host needs internet access.

### 20. Download Courier

- **What it does**: started on any host with a URL (or a list of URLs) and an
  optional expected checksum. Finds a host whose `web` offer can reach that
  URL and whose grants allow it, moves there, downloads the file into an
  artifact (with resume for large files), verifies size and checksum, then
  returns to the host where it was started and writes the file into a
  destination root there. The owner gets a user-info notification with path
  and checksum.
- **Why useful**: get installers, data sets or documents onto machines that
  cannot download themselves, without manual copying through a USB stick or a
  shared drive.
- **Shows**: offer targets with URL requirements, round trip back to the
  starting host, artifacts for large payloads, checksum verification, grants
  on two hosts (`web: download` on the gateway, `files: write` at home).
- **Grants**: `web: download` for the URL scope; `files: write, mkdir` on the
  destination root; `user-info: notify`.

### 21. Web Researcher

- **What it does**: takes a research question, moves to a web host, runs
  `web.search`, fetches the top pages and extracts their text, then moves to
  an AI host to summarize the findings with source links, and finally delivers
  a Markdown report to the starting host (or a chosen destination root).
- **Why useful**: web research from inside a closed network, with the AI part
  staying on local hosts.
- **Shows**: a paglet chaining two different offers (`web` and `ai`) on
  different hosts, carrying intermediate results in memory, then delivering at
  home.
- **Grants**: `web: search, fetch, extract_text` for the URL scope; `ai:
  summarize`; `files: write` on the destination root.

### 22. Release Watcher

- **What it does**: stays on a web host, periodically checks a list of release
  pages or feeds (for example GitHub releases of tools used in the lab) and
  notifies the owner about new versions. On request it starts a Download
  Courier for the new release file.
- **Why useful**: keeps tool versions on offline machines up to date without
  anyone watching web pages.
- **Shows**: long-lived paglet on a gateway host that deactivates between
  checks, remembered state (last seen versions) in memory, paglets starting
  other paglets.
- **Grants**: `web: fetch` for the watched URL scope; `user-info: notify`;
  permission to create a Download Courier child.

## Coverage of paglets/cpp features

| Feature | Demos |
|---|---|
| Memory-image mobility | 1, 5, 14, 15 |
| Clone fan-out | 2, 3, 4, 6, 11 |
| Grants following paglets, manifest preflight | 2, 5, 6 |
| `files` system paglet | 2, 3, 4, 5, 6, 7 |
| `server-info` system paglet | 1, 8, 9, 10, 11 |
| pubsub | 7, 8 |
| Timers and inactive paglets | 7, 9 |
| Artifacts | 3, 4, 5, 10 |
| compute-slots, checkpoints, resume | 12, 14 |
| Paglet-to-paglet messaging and capability passing | 6, 13, 15 |
| `locate_and_pin` | 15 |
| Cross-platform uniform schemas | 3, 8, 10 |
| Service offers and offer-based placement | 16, 17, 18, 19, 20, 21, 22 |
| `web` system paglet | 20, 21, 22 |
| `ai` system paglet | 16, 17, 18, 19 |
| Data residency | 16, 17, 21 |

## Round 2

Further mobile agents, planned for when the system is more mature, are
collected in [cpp-round2-paglets.md](cpp-round2-paglets.md).

## Suggested order

- **With M1 (single host)**: Mesh Journey (single-host variant), Latency Map
  (local), Pi Marathon without chaos.
- **With M2 (system paglets and grants)**: Mesh File Finder, Storage Analyzer,
  Mesh Top, Inventory Collector, Process Finder, Volume Guard.
- **With M3 (movement)**: Mesh Journey, File Courier, Tree Compare, Duplicate
  Finder, Log Scout, Hide and Seek, Latency Map across hosts.
- **With M4 (compute, AI and web services)**: Mesh Benchmark, Pi Marathon
  with chaos, AI Document Digest, Semantic Mesh Search, Image Describer, Log
  Explainer, Download Courier, Web Researcher, Release Watcher.
