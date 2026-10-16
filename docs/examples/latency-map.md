# Latency Map

The Latency Map measures how long a message takes between every pair of
hosts of the mesh. It places a probe on every host. The probes learn about
each other, ping each other in turn, and report their round-trip times.
The result is a matrix: one row per host, one cell for each other host.

Directory: [`cpp/examples/latency/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/latency)

## What it shows

- **Messaging between paglets on different hosts.** A probe sends `ping`
  to an endpoint of another probe and gets the reply back. The host routes
  the message to wherever that probe is; the paglet does not know
  addresses, only endpoints.
- **Endpoints passed on in messages.** Paglets talk to each other only
  through capabilities. Each probe sends the parent an endpoint to itself,
  and the parent hands every probe endpoints of the others (see
  [Capabilities](../writing-paglets.md#capabilities)).
- **Narrowed capabilities.** An endpoint can be *derived* with fewer
  operations. A probe gives the parent an endpoint that allows only
  `ping`, `probe.peers` and `probe.done`, and the parent passes on copies
  that allow only `ping`. A probe can therefore ping the others, but not
  give them work.
- **Fan-out.** The `FanOut` pattern sends the probes and collects one row
  from each (see
  [The patterns library](../writing-paglets.md#the-patterns-library)).

## Why it is useful

The matrix shows how the mesh is connected as paglets see it: a host
behind a slow link, a host reached only through relays, or a pair of hosts
whose messages take a detour all stand out. The round trips include
everything a message between paglets passes through: both hosts'
runtimes, the encrypted channel between the hosts, and relays where there
are any. So the numbers say how long a request between paglets on two
hosts takes, which helps to decide where cooperating paglets should run.

## How it works

The paglet starts at home with a `start` message. Without a list of
hosts, it asks `mesh-info` for the landscape and sends a probe (a clone of
itself) to every host it knows, this one included, with the number of
rounds as the clone's argument.

A probe that arrives asks `mesh-info` for the name of its host, derives an
endpoint to itself with the operations the others need, and sends it to
the parent with `probe.ready`:

<!-- snippet: cpp/examples/latency/latency.cpp#ready -->
Source: [`cpp/examples/latency/latency.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/latency/latency.cpp)

```cpp
void on_cloned(paglets::Started& s) override {
    if (s.caps.empty() || !s.decode_args(rounds_)) {
        (void)paglets::dispose();
        return;
    }
    parent_ = Endpoint(s.caps[0].release());
    pt::here([this](paglets::Result<svc::mesh_info::Snapshot> snap) {
        name_ = snap ? pt::host_label(*snap) : std::string("?");
        // An endpoint to this probe: the parent's (it hands copies that
        // can only ping to the other probes).
        auto mine = paglets::self().derive(
            paglets::abi::DeriveSpec{.ops = std::vector<std::string>{"ping", "probe.peers", "probe.done"}});
        if (!mine) return pt::report_error(parent_, pt::error_text("endpoint", mine.error()));
        paglets::SendOptions o;
        o.caps.push_back(*mine);
        (void)parent_.send("probe.ready", Ready{name_}, std::move(o));
    });
}
```
<!-- /snippet -->

The parent keeps each probe's endpoint. When every probe is ready, it
sends each one the names of the others and, as capabilities in the same
order, copies of their endpoints that allow only `ping`:

<!-- snippet: cpp/examples/latency/latency.cpp#peers -->
Source: [`cpp/examples/latency/latency.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/latency/latency.cpp)

```cpp
// Once every probe is ready, each gets the others.
void ready(const Ready& r, Message& m) {
    if (m.cap_count() == 0) return;
    probes_.push_back(Probe_{r.host_name, Endpoint(m.take_cap(0))});
    if (probes_.size() < expected_) return;
    for (const auto& to : probes_) {
        Peers peers;
        paglets::SendOptions o;
        for (const auto& other : probes_) {
            if (other.name == to.name) continue;
            auto copy = other.endpoint.derive(paglets::abi::DeriveSpec{.ops = std::vector<std::string>{"ping"}});
            if (!copy) continue;
            peers.names.push_back(other.name);
            o.caps.push_back(*copy);
        }
        (void)to.endpoint.send("probe.peers", peers, std::move(o));
    }
}
```
<!-- /snippet -->

A probe pings its targets one at a time, `rounds` times each, and waits
for every reply before it sends the next ping, so the pings do not queue
behind each other. For each target it keeps the number of replies, the
shortest round trip and the sum, and turns the sum into the mean at the
end. A ping without a reply in 5 s puts its error into that cell. Then
the probe reports its row to the parent:

<!-- snippet: cpp/examples/latency/latency.cpp#ping -->
Source: [`cpp/examples/latency/latency.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/latency/latency.cpp)

```cpp
// Pings target `t`, round `round`; one at a time.
void ping(std::size_t t, std::int64_t round) {
    if (t >= targets_.size()) {
        for (auto& x : targets_) {
            if (x.cell.replies > 0) x.cell.avg_ms /= static_cast<double>(x.cell.replies);
            row_.cells.push_back(x.cell);
        }
        return pt::report(parent_, paglets::encode(row_));
    }
    if (round >= rounds_) return ping(t + 1, 0);
    const double sent_at = now_ms();
    paglets::RequestOptions o;
    o.timeout_ms = 5000;
    auto sent = targets_[t].endpoint.request(
        "ping",
        [this, t, round, sent_at](Message& reply) {
            Cell& c = targets_[t].cell;
            if (reply.ok()) {
                const double rtt = now_ms() - sent_at;
                c.avg_ms += rtt;
                c.min_ms = c.replies == 0 ? rtt : std::min(c.min_ms, rtt);
                ++c.replies;
            } else {
                c.error = std::string(paglets::abi::error_name(reply.status()));
            }
            ping(t, round + 1);
        },
        std::move(o));
    if (!sent) {
        targets_[t].cell.error = pt::error_text("ping", sent.error());
        ping(t + 1, 0);
    }
}
```
<!-- /snippet -->

When every probe has reported (or the `timeout_ms` has passed), the parent
sorts the rows by host name, marks the map as finished, and sends every
probe `probe.done`, upon which the probes dispose of themselves. Until
then they stay, because the other probes may still ping them.

The messages and the map:

<!-- snippet: cpp/examples/latency/latency_msgs.hpp -->
Source: [`cpp/examples/latency/latency_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/latency/latency_msgs.hpp)

```cpp
// Messages of the Latency Map demo (planning/cpp-demo-paglets.md, demo 13).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace latency_msgs {

// `start`: a probe on every host (or `hosts`); each pings every other probe
// `rounds` times.
struct Probe {
    std::vector<std::string> hosts;
    std::int64_t rounds = 5;
    std::int64_t timeout_ms = 30'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t probes = 0;
};

// A probe tells the parent where it is (its endpoint comes with it).
struct Ready {
    std::string host_name;
};

// The parent tells every probe about the others (their endpoints come
// with it, in this order).
struct Peers {
    std::vector<std::string> names;
};

struct Cell {
    std::string to;
    double avg_ms = 0;
    double min_ms = 0;
    std::int64_t replies = 0;
    std::string error;
};

struct Row {
    std::string from;
    std::vector<Cell> cells;
};

struct LatencyMap {
    bool finished = false;
    std::vector<Row> rows;
    std::vector<std::string> errors;
};

}  // namespace latency_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The Latency Map needs no policy rule. Launch it on any host and start it:

```bash
H=build/macos-arm64/host/paglets-host
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/latency.wasm)
$H remote call $R $ID start '{"hosts": [], "rounds": 5, "timeout_ms": 30000}'
$H remote call $R $ID map
```

An empty `hosts` places a probe on every host of the mesh; otherwise name
the hosts by name or key ID. `start` answers with the number of probes
that went out. `map` answers with `finished: false` until every probe has
reported, then with one row per host. Each cell names the other host and
gives the mean and shortest round trip in milliseconds and the number of
replies; `error` names a ping that failed. Probes that did not report are
listed under `errors`.

## On real machines

`cpp/tools/mesh_bench.sh` (see
[File Courier](file-courier.md#transfer-speed-between-real-machines))
runs this example on a mesh of real machines on the recorded local network (a Mac
Studio, a Raspberry Pi 4 and a Raspberry Pi Compute Module 5): round
trips of messages between probes on every pair of hosts, 20 pings each,
through the HTTPS channels between the hosts and the worker processes
the probes run in. The test results further below come from hosts in one
test process instead. The last run:

<!-- results: live_latency -->
| Test | Mode | Outcome | Platform | Recorded | round trip, mean of 20 (ms) | round trip, fastest (ms) |
|---|---|---|---|---|---|---|
| mac → pi | LAN, worker processes | passed | Release build | 2026-10-12 | 2.84 | 1.08 |
| mac → uconsole | LAN, worker processes | passed | Release build | 2026-10-12 | 4.69 | 1.92 |
| pi → mac | LAN, worker processes | passed | Release build | 2026-10-12 | 4.07 | 1.71 |
| pi → uconsole | LAN, worker processes | passed | Release build | 2026-10-12 | 1.34 | 0.79 |
| uconsole → mac | LAN, worker processes | passed | Release build | 2026-10-12 | 5.16 | 1.83 |
| uconsole → pi | LAN, worker processes | passed | Release build | 2026-10-12 | 1.16 | 0.79 |
<!-- /results -->

## Test results

The test runs the Latency Map on a mesh of three hosts inside one test
process, with three rounds
(`example: Latency Map ...` in `cpp/tests/test_examples.cpp`). It checks
that:

- three probes went out, and the map is finished without errors;
- there is one row for each of the hosts `a`, `b` and `c`, in that order;
- each row has a cell for each of the two other hosts, never for its own,
  and each cell has three replies, no error, and a shortest round trip
  that is not above the mean;
- the probes end once the map is complete, so only the parent is left.

<!-- results: latency -->
| Test | Mode | Outcome | Duration | Platform | Recorded | host pairs measured | pings per pair | mean round trip (in-memory test transport) (ms) |
|---|---|---|---|---|---|---|---|---|
| example: Latency Map measures round trips between every pair of hosts | in-process | passed | 0.1 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 6 | 3 | 0.24 |
| example: Latency Map measures round trips between every pair of hosts | worker processes | passed | 0.2 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 6 | 3 | 0.36 |
<!-- /results -->

The test hosts use the in-memory transport between hosts of one process.
The fixture delivers queued frames and ticks the hosts in rounds, sleeping
2 ms only when a round delivered no frames. Its round trips still include
test scheduling and runtime waits; use the live results for timings
between physical hosts.
