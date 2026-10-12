# File Courier

The File Courier carries files from a named root of one host to a named
root of another, without shared drives and without a person copying them.
It checks both hosts before it leaves home, and verifies every file by
SHA-256 on arrival. Then it comes home with a receipt that says which files
arrived and how long each step took.

Directory: [`cpp/examples/file_courier/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/file_courier)

## What it shows

- **Files of any size, carried as capabilities.** A paglet moves as a
  memory image of at most 16 MB, so it must not carry large data in its
  memory. The courier asks the source host to *share* each file, and gets
  an artifact capability of a few bytes for it. Only the capabilities move
  with the courier. On the destination host it *saves* each file: that
  host fetches the content directly from the source host, checks its
  SHA-256 and only then lets the file appear (see
  [Moving content between hosts](../system-paglets.md#moving-content-between-hosts)).
- **Grants on two hosts.** The courier asks the `grants` system paglet of
  each host for a directory: `read` on the source root, `write` and
  `create` on the destination root. The mesh policy decides; nothing is
  configured in the paglet.
- **Checks before departure.** It asks `mesh-info` whether both hosts are
  in the mesh and online, so it does not set out for a host that cannot be
  reached.
- **Data residency.** Content of a root that must stay on its host does
  not leave it: the courier, which listed that root, is not allowed to
  move, and the source host would refuse the content to other hosts
  anyway.

## Why it is useful

Results of a computation often end up on one machine and are needed on
another: a build server, a lab instrument, a laptop without access to the
same network shares. The courier needs nothing on the hosts except a
paglets host with named roots, and the policy decides who may read and
write where. Every transfer is checked end to end, and the receipt says
exactly what arrived.

## How it works

The courier starts at home with a `start` message, which names the source
host and root, a pattern of files, and the destination host, root and
directory. Before it replies, it asks `mesh-info` for the landscape of the
mesh:

<!-- snippet: cpp/examples/file_courier/file_courier.cpp#checks -->
Source: [`cpp/examples/file_courier/file_courier.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/file_courier/file_courier.cpp)

```cpp
// Both hosts must be in the mesh and online before the courier leaves.
svc::mesh_info::Client client{*mi};
auto sent = client.landscape(
    svc::mesh_info::LandscapeRequest{}, [this, reply](paglets::Result<svc::mesh_info::Landscape> l, Message&) {
        if (!l || l->hosts.empty()) {
            (void)paglets::reply_to(*reply, Started{false, "no landscape"});
            return;
        }
        for (const auto& wanted : {params_.from_host, params_.to_host}) {
            bool known = false;
            for (const auto& h : l->hosts) known = known || same_host(wanted, h.host, h.host_name);
            if (!known) {
                (void)paglets::reply_to(*reply, Started{false, "unknown or offline host: " + wanted});
                return;
            }
        }
        home_ = l->hosts.front().host;  // this host comes first
        status_.state = "picking-up";
        (void)paglets::reply_to(*reply, Started{true, {}});
        where([this] {
            if (is_here(params_.from_host)) return pick_up();
            go(params_.from_host);
        });
    });
if (!sent) (void)paglets::reply_to(*reply, Started{false, pt::error_text("mesh-info", sent.error())});
```
<!-- /snippet -->

On the source host, it asks `grants` for the source root with `read`,
finds the matching files (`files.find`), and shares them one by one. The
host hashes each file; the courier keeps the name, size, hash and the
artifact capability, and measures how long the host took:

<!-- snippet: cpp/examples/file_courier/file_courier.cpp#share -->
Source: [`cpp/examples/file_courier/file_courier.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/file_courier/file_courier.cpp)

```cpp
pt::share_file(*dir_, found_[i], [this, i, began](paglets::Result<pt::SharedFile> f) {
    if (!f) return fail(pt::error_text("share " + found_[i], f.error()));
    const std::int64_t took = now_ms(abi::Clock::monotonic) - began;
    status_.bytes += static_cast<std::int64_t>(f->size);
    if (params_.max_bytes > 0 && status_.bytes > params_.max_bytes) {
        (void)f->content.drop();
        return fail("more than " + std::to_string(params_.max_bytes) + " bytes");
    }
    status_.files.push_back(Item{f->name, static_cast<std::int64_t>(f->size), f->sha256, false, took, 0});
    status_.share_ms += took;
    carried_.push_back(std::move(*f));  // name, hash, size and the artifact capability
    share_next(i + 1);
});
```
<!-- /snippet -->

Then it moves to the destination host with `paglets::dispatch`. The
capabilities move with it: they name the host that holds the content and a
token that host made. On the destination host it asks for the destination
root with `write` and `create`, makes the directories, and saves each
file. Here the content travels, in parts of 1 MB, from the source host
straight into a temporary file beside the target:

<!-- snippet: cpp/examples/file_courier/file_courier.cpp#save -->
Source: [`cpp/examples/file_courier/file_courier.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/file_courier/file_courier.cpp)

```cpp
// This host fetches the content from the source host and
// checks its SHA-256 before the file appears.
pt::save_file(*dir_, path, carried_[i].content,
              [this, i, path, began](paglets::Result<svc::files::Entry> saved) {
                  if (saved) {
                      const std::int64_t took = now_ms(abi::Clock::monotonic) - began;
                      status_.files[i].verified = true;
                      status_.files[i].save_ms = took;
                      status_.save_ms += took;
                      return save_next(i + 1);
                  }
                  if (saved.error() != abi::denied) {
                      return fail(pt::error_text("save " + path, saved.error()));
                  }
                  // The source host keeps the content of its root.
                  (void)dir_->drop();
                  dir_.reset();
                  refused_ = true;
                  status_.error = "data residency: " + params_.from_host +
                                  " does not give the content of " + params_.from_root +
                                  " to " + params_.to_host;
                  paglets::log("file courier: " + status_.error);
                  go_home();
              });
```
<!-- /snippet -->

A `denied` answer means that the source host refused the content to this
host (data residency). The courier then comes home and reports it. When
every file is saved, it drops its capabilities, goes home and fills in the
receipt.

The messages and the receipt:

<!-- snippet: cpp/examples/file_courier/file_courier_msgs.hpp -->
Source: [`cpp/examples/file_courier/file_courier_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/file_courier/file_courier_msgs.hpp)

```cpp
// Messages of the File Courier demo (planning/cpp-demo-paglets.md, demo 5).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace file_courier_msgs {

// `start`: carry the files matching `pattern` below the root `from_root` of
// `from_host` to the directory `to_path` below the root `to_root` of
// `to_host` (hosts by name or key ID), then come home with a receipt.
struct Start {
    std::string from_host;
    std::string from_root;
    std::string pattern = "**";
    std::string to_host;
    std::string to_root;
    std::string to_path;  // "" : the root itself
    std::int64_t max_files = 100;
    std::int64_t max_bytes = 0;  // all files together; 0: no limit
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// One file of the receipt.
struct Item {
    std::string path;  // below the source root; the same below `to_path`
    std::int64_t size = 0;
    std::string sha256;     // hex, of the file on the source host
    bool verified = false;  // written on the destination host with the same hash
    std::int64_t share_ms = 0;  // hashing it on the source host
    std::int64_t save_ms = 0;   // fetching, checking and writing it on the destination host
};

// `status`: the errand and, once delivered, the receipt.
struct Status {
    std::string state;  // idle, picking-up, delivering, returning, delivered, refused, failed
    std::vector<Item> files;
    std::int64_t bytes = 0;
    std::vector<std::string> hosts;  // names of the hosts it was on, in order
    std::string error;
    // Timing, in milliseconds: sharing every file on the source host, saving
    // every file on the destination host (the transfer), and the whole errand
    // from `start` to the receipt at home (each measured on one host's clock).
    std::int64_t share_ms = 0;
    std::int64_t save_ms = 0;
    std::int64_t total_ms = 0;
};

}  // namespace file_courier_msgs
```
<!-- /snippet -->

## Running it on a live mesh

With two hosts that have the roots `data` and `inbox`, and a rule that
lets the owner's paglets read `data` and write `inbox` (see
[Hosts and meshes](../hosts-and-meshes.md)), launch the courier on the
home host and start it:

```bash
H=build/macos-arm64/host/paglets-host
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/file_courier.wasm)
$H remote call $R $ID start '{"from_host": "lab-1", "from_root": "data", "pattern": "results/**",
    "to_host": "lab-2", "to_root": "inbox", "to_path": "from-lab-1"}'
$H remote call $R $ID status
```

`status` answers with the receipt: `state` is `delivered` when it is done,
and every file lists its size, SHA-256, `verified` and the milliseconds of
sharing and saving.

## Transfer speed between real machines

`cpp/tools/mesh_bench.sh` measures the courier on a mesh of this machine
and other machines on the network, reached over SSH. It makes a fresh mesh
of all of them with a root `bench` on each, writes files of random bytes
on every host, and lets a courier carry each size for every ordered pair
of hosts. The transfer rate is the file size divided by the time the
destination host took to save it: fetching all parts, checking the
SHA-256, and writing the file. For comparison, it measures plain TCP
between the same two hosts with iperf3.

```bash
cpp/tools/mesh_bench.sh --sizes "16 64 256" \
    --remote pi=klukas@rpi4:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
    --remote uconsole=uconsole:/home/klukas/paglets-live/cpp/build/linux-gcc16/host
```

The same run also records the [Latency Map](latency-map.md#on-real-machines)
and the [Mesh Benchmark](mesh-benchmark.md#on-real-machines) of these
hosts. The hosts of the last run, on a gigabit LAN:

<!-- results: live_hosts -->
| Test | Platform | Recorded |
|---|---|---|
| mac | Darwin arm64, Mac13,1, 10 CPUs | 2026-10-12 |
| pi | Linux aarch64, Raspberry Pi 4 Model B Rev 1.4, 4 CPUs | 2026-10-12 |
| uconsole | Linux aarch64, Raspberry Pi Compute Module 5 Rev 1.0, 4 CPUs | 2026-10-12 |
<!-- /results -->

The transfers:

<!-- results: live_transfer -->
| Test | Mode | Outcome | Duration | Platform | Recorded | sharing (hashing) on the source (ms) | saving on the destination: transfer, SHA-256 check, write (ms) | transfer rate (MB/s) | transfer rate (Mbit/s) | TCP between the hosts (iperf3) (Mbit/s) |
|---|---|---|---|---|---|---|---|---|---|---|
| mac → pi, 16 MB | LAN, worker processes | passed | 1.0 s | Release build | 2026-10-12 | 23 | 783 | 20.4 | 171 | 941 |
| mac → pi, 64 MB | LAN, worker processes | passed | 1.8 s | Release build | 2026-10-12 | 65 | 1618 | 39.6 | 332 | 941 |
| mac → pi, 256 MB | LAN, worker processes | passed | 10.1 s | Release build | 2026-10-12 | 388 | 9564 | 26.8 | 225 | 941 |
| mac → uconsole, 16 MB | LAN, worker processes | passed | 0.8 s | Release build | 2026-10-12 | 17 | 596 | 26.8 | 225 | 341 |
| mac → uconsole, 64 MB | LAN, worker processes | passed | 2.6 s | Release build | 2026-10-12 | 78 | 2366 | 27.0 | 227 | 341 |
| mac → uconsole, 256 MB | LAN, worker processes | passed | 9.7 s | Release build | 2026-10-12 | 194 | 9416 | 27.2 | 228 | 341 |
| pi → mac, 16 MB | LAN, worker processes | passed | 0.7 s | Release build | 2026-10-12 | 120 | 477 | 33.5 | 281 | 934 |
| pi → mac, 64 MB | LAN, worker processes | passed | 3.1 s | Release build | 2026-10-12 | 493 | 2316 | 27.6 | 232 | 934 |
| pi → mac, 256 MB | LAN, worker processes | passed | 11.9 s | Release build | 2026-10-12 | 1841 | 9874 | 25.9 | 217 | 934 |
| pi → uconsole, 16 MB | LAN, worker processes | passed | 1.1 s | Release build | 2026-10-12 | 181 | 749 | 21.4 | 179 | 341 |
| pi → uconsole, 64 MB | LAN, worker processes | passed | 3.5 s | Release build | 2026-10-12 | 499 | 2902 | 22.1 | 185 | 341 |
| pi → uconsole, 256 MB | LAN, worker processes | passed | 14.3 s | Release build | 2026-10-12 | 1835 | 12323 | 20.8 | 174 | 341 |
| uconsole → mac, 16 MB | LAN, worker processes | passed | 0.8 s | Release build | 2026-10-12 | 17 | 644 | 24.8 | 208 | 321 |
| uconsole → mac, 64 MB | LAN, worker processes | passed | 2.6 s | Release build | 2026-10-12 | 56 | 2470 | 25.9 | 217 | 321 |
| uconsole → mac, 256 MB | LAN, worker processes | passed | 11.0 s | Release build | 2026-10-12 | 207 | 10721 | 23.9 | 200 | 321 |
| uconsole → pi, 16 MB | LAN, worker processes | passed | 1.0 s | Release build | 2026-10-12 | 17 | 777 | 20.6 | 173 | 317 |
| uconsole → pi, 64 MB | LAN, worker processes | passed | 3.0 s | Release build | 2026-10-12 | 56 | 2835 | 22.6 | 189 | 317 |
| uconsole → pi, 256 MB | LAN, worker processes | passed | 11.7 s | Release build | 2026-10-12 | 208 | 11291 | 22.7 | 190 | 317 |
<!-- /results -->

## Test results

The tests run the courier on a mesh of three hosts inside one test process
(`example: File Courier ...` in `cpp/tests/test_examples.cpp`):

- it carries three files from `lab` to `archive`, among them one of 24 MB,
  more than a paglet's whole memory, and comes home with a receipt in
  which every file is verified;
- the files on the destination are byte for byte the originals, and no
  temporary files are left;
- for a root whose content must stay on its host, the courier is refused
  and nothing reaches the destination;
- a destination host that is not in the mesh is refused before the courier
  leaves.

The content exchange tests (`exchange: ...` in
`cpp/tests/test_exchange.cpp`) check the same mechanism from the other
side: a 20 MB file is shared, moved as a capability, saved on one host and
read on another; a host that the residency rule excludes gets nothing; a
file that changed after it was shared is no longer served.

<!-- results: file_courier -->
| Test | Mode | Outcome | Duration | Platform | Recorded |
|---|---|---|---|---|---|
| example: File Courier carries files of any size between hosts with checksums, within data residency | in-process | passed | 30.6 s | Darwin arm64, Release build | 2026-10-12 |
| example: File Courier carries files of any size between hosts with checksums, within data residency | worker processes | passed | 40.6 s | Darwin arm64, Release build | 2026-10-12 |
| exchange: a shared file larger than a paglet's memory travels as a capability | in-process | passed | 30.7 s | Darwin arm64, Release build | 2026-10-12 |
| exchange: a shared file larger than a paglet's memory travels as a capability | worker processes | passed | 21.3 s | Darwin arm64, Release build | 2026-10-12 |

example: File Courier carries files of any size between hosts with checksums, within data residency:

| Measured | in-process | worker processes |
|---|---|---|
| files carried | 3 | 3 |
| bytes carried | 25169798 | 25169798 |
| largest file (a paglet's memory holds 16 MB) | 24 MB | 24 MB |
| sharing (hashing) on the source host | 31 ms | 25 ms |
| saving (transfer and check) on the destination | 70 ms | 44 ms |
| transfer rate (in-memory test transport) | 342.91 MB/s | 545.54 MB/s |
| whole errand, start to receipt | 20149 ms | 30192 ms |
| host-only root: refused, nothing delivered | yes | yes |

exchange: a shared file larger than a paglet's memory travels as a capability:

| Measured | in-process | worker processes |
|---|---|---|
| a 20 MB file shared, moved as a capability, saved and read on two other hosts | yes | yes |
<!-- /results -->

The transfer rates of the tests use the in-memory transport between hosts
of one process, so they show the overhead of the exchange itself, not of a
network.
