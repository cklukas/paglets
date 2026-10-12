# Inventory Collector, Process Finder, Mesh Top

One paglet module serves three purposes. As the **Inventory Collector**
it collects every host's system summary: operating system, CPU, memory,
uptime and volumes. As **Mesh Top** it adds the current load and the
largest processes of each host. As the **Process Finder** it finds the
processes with a given name on every host. It sends a clone to every
host; each clone asks the `server-info` system paglet of its host and
reports.

Directory: [`cpp/examples/inventory/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/inventory)

## What it shows

- **One schema on every platform.** `server-info` answers `summary`,
  `load`, `volumes` and `processes` in the same schema on macOS, Linux and
  Windows (see [Services](../system-paglets.md#services)). The paglet has
  no code for any particular operating system.
- **Policy per operation.** Each operation of `server-info` is allowed or
  denied by the mesh policy. A mesh can allow the summary to everyone and
  the process list to a few owners.
- **A fan-out of clones.** The paglet clones itself to every host with
  the `FanOut` pattern (see
  [The patterns library](../writing-paglets.md#the-patterns-library)),
  and collects one report from each. A clone that does not report within
  the deadline is listed with an error, so the inventory is complete even
  when a host is down.

## Why it is useful

Knowing what runs where is the first step of managing a group of
machines: which operating system and version, how many CPUs and how much
memory, how full the volumes are, and which processes take the memory.
The inventory needs no agent installed on the hosts besides the paglets
host, and no remote shell. A process search across the mesh answers
questions such as "where does the old version of the service still run?".

## How it works

The paglet starts at home with a `start` message. It names the hosts
(empty: every host `mesh-info` knows), the number of processes to list per
host (`top`, 5 by default), and optionally a process name. The paglet
then sends a clone to each host.

A clone looks up `server-info` on its host and asks it, one after
another, for the summary, the load, the volumes and the processes:

<!-- snippet: cpp/examples/inventory/inventory.cpp#clone -->
Source: [`cpp/examples/inventory/inventory.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/inventory/inventory.cpp)

```cpp
void on_cloned(paglets::Started& s) override {
    Collect q;
    if (s.caps.empty() || !s.decode_args(q)) {
        (void)paglets::dispose();
        return;
    }
    parent_ = paglets::Endpoint(s.caps[0].release());
    query_ = q;
    pt::lookup("server-info", [this](paglets::Result<paglets::Endpoint> ep) {
        if (!ep) return finish(pt::error_text("server-info", ep.error()));
        info_ = *ep;
        summary();
    });
}
```
<!-- /snippet -->

Without the summary, the clone reports an error. The other operations
are optional: if the policy denies one of them, the inventory of that
host lacks those fields. For the processes, `server-info` lists the
largest resident memory first. Without a process name, the clone asks
for the `top` processes. With a name, it asks for up to 10 000 and keeps
those whose name contains the given text. Then it reports and ends two
seconds later:

<!-- snippet: cpp/examples/inventory/inventory.cpp#processes -->
Source: [`cpp/examples/inventory/inventory.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/inventory/inventory.cpp)

```cpp
void processes() {
    si::Client c{info_};
    si::ProcessesRequest q;
    q.limit = query_.process.empty() ? static_cast<std::uint32_t>(std::max<std::int64_t>(0, query_.top)) : 10'000;
    auto sent = c.processes(q, [this](paglets::Result<si::Processes> p, Message&) {
        if (p) {
            inv_.processes_total = p->total;
            for (const auto& x : p->processes) {
                if (!query_.process.empty() && x.name.find(query_.process) == std::string::npos) continue;
                inv_.processes.push_back(ProcessInfo{x.pid, x.name, static_cast<std::int64_t>(x.memory)});
            }
        }
        finish({});
    });
    if (!sent) finish({});
}

void finish(std::string error) {
    if (!error.empty()) return pt::report_error(parent_, std::move(error));
    pt::report(parent_, paglets::encode(inv_));
    (void)paglets::after_raw(2000, "done", {});
    router().on("done", [](Message&) { (void)paglets::dispose(); });
}
```
<!-- /snippet -->

At home, each report becomes the inventory of one host, with the host's
key ID and name. A clone that failed or did not report in time gives an
inventory with only the host and `error` set. When every clone has finished, the
paglet sets `finished` and sorts the hosts by name:

<!-- snippet: cpp/examples/inventory/inventory.cpp#collect -->
Source: [`cpp/examples/inventory/inventory.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/inventory/inventory.cpp)

```cpp
fan_.attach(router(), [this](const pt::FanOut::Child& c) {
    HostInventory h;
    if (!c.ok || !paglets::decode(c.result, h)) {
        h = HostInventory{};
        h.error = c.error.empty() ? "no report" : c.error;
    }
    h.host = c.host;
    h.host_name = c.host_name.empty() ? c.destination : c.host_name;
    report_.hosts.push_back(std::move(h));
    report_.finished = fan_.finished();
    if (report_.finished) {
        std::ranges::sort(report_.hosts, [](const auto& a, const auto& b) { return a.host_name < b.host_name; });
    }
});
```
<!-- /snippet -->

The messages and the report:

<!-- snippet: cpp/examples/inventory/inventory_msgs.hpp -->
Source: [`cpp/examples/inventory/inventory_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/inventory/inventory_msgs.hpp)

```cpp
// Messages of the Inventory Collector, Process Finder and Mesh Top demos
// (planning/cpp-demo-paglets.md, demos 8, 10 and 11).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace inventory_msgs {

// `start`: collect from every host (or `hosts`): the system summary, load,
// volumes and the `top` largest processes; with `process`, only processes
// whose name contains it (Process Finder).
struct Collect {
    std::vector<std::string> hosts;
    std::string process;
    std::int64_t top = 5;
    std::int64_t timeout_ms = 20'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

struct VolumeInfo {
    std::string mount_point;
    std::string filesystem;
    std::int64_t total = 0;
    std::int64_t available = 0;
};

struct ProcessInfo {
    std::int64_t pid = 0;
    std::string name;
    std::int64_t memory = 0;
};

// One host, in the same shape on every operating system.
struct HostInventory {
    std::string host;
    std::string host_name;
    std::string error;
    std::string os;
    std::string os_version;
    std::string architecture;
    std::string cpu_model;
    std::int64_t cpus = 0;
    std::int64_t memory_total = 0;
    std::int64_t memory_available = 0;
    double cpu_percent = 0;
    std::int64_t uptime_s = 0;
    std::vector<VolumeInfo> volumes;
    std::int64_t processes_total = 0;
    std::vector<ProcessInfo> processes;
};

struct Report {
    bool finished = false;
    std::vector<HostInventory> hosts;
};

}  // namespace inventory_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The mesh needs a policy rule that allows the owner's paglets the
operations of `server-info` they use (see
[Policy](../hosts-and-meshes.md#policy)):

```bash
H=build/macos-arm64/host/paglets-host
$H ledger rule --ledger ledger --admin alice.key --name inventory --decision allow \
    --service server-info --op summary --op load --op volumes --op processes --owner <olga-key-id>
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Then launch the paglet on the home host and collect the inventory of
every host, with the ten largest processes of each:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/inventory.wasm)
$H remote call $R $ID start '{"hosts": [], "process": "", "top": 10}'
$H remote call $R $ID report
```

`report` answers with `finished` once every host has reported or timed
out, and with one entry per host: operating system and version,
architecture, CPU model and count, total and available memory, CPU load
in percent, uptime, the volumes with their total and available bytes, the
number of processes, and the processes listed.

To find processes by name, start it again with `process`:

```bash
$H remote call $R $ID start '{"hosts": [], "process": "postgres"}'
$H remote call $R $ID report
```

## Test results

The test runs the paglet on a mesh of three hosts, `a`, `b` and `c`,
inside one test process, with a policy rule that allows the four
operations of `server-info` (`example: Inventory Collector ...` in
`cpp/tests/test_examples.cpp`). The test checks that:

- the inventory has all three hosts, sorted by name, and every host
  reports without error an operating system, CPUs, total memory, at least
  one volume, its number of processes, and at most five processes;
- the Process Finder, asked for `paglets_tests`, finds that process on
  every host. The three hosts of the test share one process, the test
  itself.

<!-- results: inventory -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts reporting | volumes reported on the first host | processes on the first host | hosts where the process finder found the test process |
|---|---|---|---|---|---|---|---|---|---|
| example: Inventory Collector and Process Finder ask server-info on every host | in-process | passed | 0.2 s | Darwin arm64, Release build | 2026-10-12 | 3 | 6 | 714 | 3 |
| example: Inventory Collector and Process Finder ask server-info on every host | worker processes | passed | 0.7 s | Darwin arm64, Release build | 2026-10-12 | 3 | 6 | 768 | 3 |
<!-- /results -->
