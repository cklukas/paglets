# Mesh Journey

The Mesh Journey is the "hello world" of paglets on a mesh. The paglet
visits every host of the mesh in turn, or the hosts it is asked to visit,
and writes down each stop in a journal: the host's name, operating system,
architecture and load, and when it arrived. Then it comes home. The
journal shows which hosts can receive and run paglets.

Directory: [`cpp/examples/journey/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/journey)

## What it shows

- **Memory-image mobility.** `paglets::dispatch` moves the paglet to
  another host. The host takes an image of the paglet's WebAssembly
  memory and sends it; the destination host resumes the paglet from it.
  The journal is an ordinary `std::vector` of structs in a member, and it
  travels with the paglet without any serialization code.
- **The landscape of the mesh.** The `mesh-info` system paglet knows every
  host of the mesh, with its names and labels, spread by gossip. The
  journey asks it for the itinerary, and asks it again on every host for
  a description of the host it is on (see
  [System paglets](../system-paglets.md#services)).
- **Lifecycle hooks of a move.** `on_arrived` runs on the new host after a
  move, `on_move_failed` on the old host when a move did not happen. The
  journey continues from both. A synchronous dispatch refusal, such as a
  pin that prevents movement, ends the journey with one recorded error.
- **A deferred reply.** `start` answers only when `mesh-info` has answered,
  with the itinerary.

## Why it is useful

Before a mesh does real work, you want to know which hosts take part: are
they enrolled, online, reachable, and do they run paglets? The journey
answers that in one call, from one host, and the journal names the hosts
it could not reach and why. Its stops also show how long each move took.
The code is short, so it is also the example to read first for a paglet
that moves.

## How it works

The journey starts at home with a `start` message. It asks `mesh-info`
for the landscape. The host it runs on comes first in the landscape, and
that host is home. Every other host goes into the itinerary if it was
asked for (by host name or key ID, or all hosts when `hosts` is empty) and
carries the label asked for (any host when `label` is empty). Hosts asked
for that `mesh-info` does not know go into the journal as skipped:

<!-- snippet: cpp/examples/journey/journey.cpp#itinerary -->
Source: [`cpp/examples/journey/journey.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/journey/journey.cpp)

```cpp
// The itinerary: the hosts asked for, else every host mesh-info knows.
auto mi = paglets::service("mesh-info");
if (!mi) {
    journal_.state = "failed";
    (void)paglets::reply_to(*reply, Started{false, "no mesh-info", {}});
    return;
}
svc::mesh_info::Client client{*mi};
auto sent = client.landscape(
    svc::mesh_info::LandscapeRequest{},
    [this, s, reply](paglets::Result<svc::mesh_info::Landscape> l, Message&) {
        if (!l || l->hosts.empty()) {
            journal_.state = "failed";
            (void)paglets::reply_to(*reply, Started{false, "no landscape", {}});
            return;
        }
        home_ = l->hosts.front().host;  // this host comes first
        itinerary_.clear();
        for (const auto& h : l->hosts) {
            if (h.host == home_) continue;
            const bool asked = s.hosts.empty() || std::ranges::find(s.hosts, h.host_name) != s.hosts.end() ||
                               std::ranges::find(s.hosts, h.host) != s.hosts.end();
            const bool labelled =
                s.label.empty() || std::ranges::find(h.labels, s.label) != h.labels.end();
            if (asked && labelled) itinerary_.push_back(h.host);
        }
        // Hosts asked for by name that mesh-info does not know are skipped.
        for (const auto& want : s.hosts) {
            const bool known = std::ranges::any_of(l->hosts, [&](const auto& h) {
                return h.host_name == want || h.host == want;
            });
            if (!known) journal_.skipped.push_back(want + ": unknown to mesh-info");
        }
        (void)paglets::reply_to(*reply, Started{true, {}, itinerary_});
        record_and_go_on();  // home is the first stop
    });
```
<!-- /snippet -->

At every stop, home included, the journey asks `mesh-info` which host it
is on and records a stop. The time of the move is measured from leaving
the previous host; the two hosts' clocks may differ. When the stop is
home and it is not the first stop, the journey is over:

<!-- snippet: cpp/examples/journey/journey.cpp#record -->
Source: [`cpp/examples/journey/journey.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/journey/journey.cpp)

```cpp
void record_and_go_on() {
    if (journal_.state != "travelling") return;
    const std::int64_t arrived = now_ms();
    pt::here([this, arrived](paglets::Result<svc::mesh_info::Snapshot> s) {
        Stop stop;
        stop.arrived_ms = arrived;
        stop.transfer_ms = left_ms_ > 0 ? std::max<std::int64_t>(0, arrived - left_ms_) : 0;
        if (s) {
            here_ = s->host;
            stop.host = s->host;
            stop.host_name = pt::host_label(*s);
            stop.os = s->os;
            stop.architecture = s->architecture;
            stop.load_per_cpu = s->load_per_cpu;
        }
        journal_.stops.push_back(std::move(stop));
        if (!journal_.stops.empty() && journal_.stops.back().host == home_ && journal_.stops.size() > 1) {
            journal_.state = "home";
            return;
        }
        go_on();
    });
}
```
<!-- /snippet -->

`go_on` dispatches the paglet to the next host of the itinerary, and home
after the last. A dispatch takes effect when the handler returns. A
dispatch that fails at once is noted, and the journey goes on to the next
host:

<!-- snippet: cpp/examples/journey/journey.cpp#go_on -->
Source: [`cpp/examples/journey/journey.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/journey/journey.cpp)

```cpp
void go_on() {
    if (next_ >= itinerary_.size() && here_ == home_) {
        journal_.state = "home";  // nothing (more) to visit
        return;
    }
    std::string destination = next_ < itinerary_.size() ? itinerary_[next_++] : home_;
    left_ms_ = now_ms();
    if (auto moved = paglets::dispatch(destination); !moved) {
        // This is an admission error (for example pinned or denied),
        // before any destination was tried. Later host-specific failures
        // arrive through on_move_failed and may try the next host.
        journal_.state = "failed";
        journal_.skipped.push_back(destination + ": " + pt::error_text("dispatch", moved.error()));
    }
}
```
<!-- /snippet -->

On the new host, `on_arrived` records the next stop. A move can also fail
after the handler returned; the paglet then stays where it is and gets
`on_move_failed`, with the destination and the reason. The journey notes the host
and goes on. If home cannot be reached, the journey ends where it is, in
the state `failed`:

<!-- snippet: cpp/examples/journey/journey.cpp#moves -->
Source: [`cpp/examples/journey/journey.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/journey/journey.cpp)

```cpp
void on_arrived(const paglets::abi::ArrivedEvent&) override { record_and_go_on(); }

void on_move_failed(const paglets::abi::MoveFailed& f) override {
    if (journal_.state != "travelling") return;
    journal_.skipped.push_back(f.destination + ": " + f.reason);
    // Home out of reach: the journey ends where it is.
    if (f.destination == home_) {
        journal_.state = "failed";
        return;
    }
    go_on();  // the next host
}
```
<!-- /snippet -->

The messages and the journal:

<!-- snippet: cpp/examples/journey/journey_msgs.hpp -->
Source: [`cpp/examples/journey/journey_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/journey/journey_msgs.hpp)

```cpp
// Messages of the Mesh Journey demo (planning/cpp-demo-paglets.md, demo 1).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace journey_msgs {

// `start`: visit `hosts` in turn (host names or key IDs; empty: every host
// mesh-info knows, or those with `label`), then come home.
struct Start {
    std::vector<std::string> hosts;
    std::string label;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::vector<std::string> itinerary;  // key IDs, in order
};

// One stop of the journey.
struct Stop {
    std::string host;  // key ID
    std::string host_name;
    std::string os;
    std::string architecture;
    double load_per_cpu = 0;
    std::int64_t arrived_ms = 0;   // Unix milliseconds, this host's clock
    std::int64_t transfer_ms = 0;  // from leaving the previous host (clocks may differ)
};

struct Journal {
    std::string state;  // idle, travelling, home, failed
    std::vector<Stop> stops;
    std::vector<std::string> skipped;  // hosts it could not reach, with the reason
};

}  // namespace journey_msgs
```
<!-- /snippet -->

A `start` while the journey is travelling is refused with the reason
`already travelling`. `journal` answers with the journal at any time; its
`state` is `travelling` until the paglet is home again.

## Running it

On a mesh of enrolled hosts (see
[Running hosts](../hosts-and-meshes.md#running-hosts)), launch the journey
on its home host and start it with `remote launch` and `remote call` (see
[remote](../cli-reference.md#remote)):

```bash
H=build/macos-arm64/host/paglets-host
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/journey.wasm)
$H remote call $R $ID start '{"hosts": [], "label": ""}'
$H remote call $R $ID journal
```

`start` answers with `accepted` and the itinerary, as key IDs. Call
`journal` again until its `state` is `home`. Calls reach only the connected
host: while the journey is away, `journal` fails because the paglet is no
longer there; once it returns home it answers again. `remote locate` finds
its current host. To visit only some hosts,
name them, or give a label:

```bash
$H remote call $R $ID start '{"hosts": ["lab-2", "lab-3"], "label": ""}'
$H remote call $R $ID start '{"hosts": [], "label": "linux"}'
```

## Test results

The test `example: Mesh Journey visits every host and comes home with its
journal` in `cpp/tests/test_examples.cpp` runs a mesh of four hosts
(`home`, `north`, `south` and `east`) inside one test process:

- `start` without hosts or label is accepted with an itinerary of three
  hosts;
- the paglet leaves home and comes back with the state `home` and five
  stops: home, the three other hosts in some order, and home again. Every
  stop has an operating system and an arrival time, and no host was
  skipped;
- a second `start` asks for `south` and for `atlantis`, a host that does
  not exist. The itinerary has one host, the journal three stops (home,
  `south`, home), and `atlantis` is skipped with a reason.

<!-- results: journey -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts in the mesh | hosts visited | stops in the journal (with home twice) | journey time, start to home (ms) | unknown hosts skipped with a reason |
|---|---|---|---|---|---|---|---|---|---|---|
| example: Mesh Journey visits every host and comes home with its journal | in-process | passed | 0.7 s | Windows AMD64, RelWithDebInfo build | 2026-10-15 | 4 | 3 | 5 | 59 | 1 |
| example: Mesh Journey visits every host and comes home with its journal | in-process | passed | 0.2 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 4 | 3 | 5 | 42 | 1 |
| example: Mesh Journey visits every host and comes home with its journal | worker processes | passed | 0.9 s | Windows AMD64, RelWithDebInfo build | 2026-10-15 | 4 | 3 | 5 | 70 | 1 |
| example: Mesh Journey visits every host and comes home with its journal | worker processes | passed | 0.4 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 4 | 3 | 5 | 51 | 1 |
<!-- /results -->
