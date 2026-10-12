# Pi

The Pi example computes hexadecimal digits of pi on the hosts of the
mesh. A job paglet cuts the digits into chunks and sends each chunk, in a
clone of itself, to a host with free compute slots. The workers compute
their digits, send them to the job and end. The job puts the chunks
together in order, and sends a chunk out again when its host does not
answer in time, so the result is complete even when a host is lost during
the run.

Directory: [`cpp/examples/pi/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/pi)

## What it shows

- **Choosing hosts with `mesh-info`.** Every host spreads a snapshot of
  itself by gossip: its load, its free compute slots, its labels and
  offers. The job asks `mesh-info` to `select` hosts with a free slot and
  a moderate load (see [System paglets](../system-paglets.md#services)).
- **Compute slots and redirects.** A worker asks its host's
  `compute-slots` for a slot. It runs at once, waits in the host's queue
  until it receives `compute.granted`, or receives `compute.redirect` with
  a host that has free slots now, moves there and asks again. The hosts
  only advise; the scheduler never moves paglets, the worker moves itself.
- **Clones with a destination.** The job clones itself directly onto the
  chosen host. The clone gets its chunk as arguments and the job's
  endpoint as its only capability.
- **Resilience without a central scheduler.** The job keeps the state of
  every chunk in its own memory. A chunk that is not answered in time goes
  out again, so the loss of a host costs only the chunks it held.

## Why it is useful

Pi stands for any computation that splits into independent parts:
parameter sweeps, rendering tiles, batches of files. The
Bailey-Borwein-Plouffe (BBP) formula gives the hexadecimal digits of pi at
any position without the digits before them, so every chunk can be
computed on its own and the result is easy to check. The example shows the
whole pattern in about 300 lines: the job finds hosts with spare capacity,
each host decides when work runs, and lost work is sent again. The mesh
needs no job server and no shared storage.

## How it works

The digits come from the BBP formula in `bbp.hpp`. The function computes
the digits after the point from any position, six at a time, which is well
within the precision of a `double`. The host tests use the same header to
check the result:

<!-- snippet: cpp/examples/pi/bbp.hpp#digits -->
Source: [`cpp/examples/pi/bbp.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/bbp.hpp)

```cpp
// Hex digits of pi after the point, starting at `position` (0: the first
// digit after "3."), 6 at a time (well within double precision).
inline std::string hex_digits(std::int64_t position, std::int64_t count) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(static_cast<std::size_t>(count));
    for (std::int64_t at = position; at < position + count; at += 6) {
        double x = 4.0 * series(1, at) - 2.0 * series(4, at) - series(5, at) - series(6, at);
        x -= std::floor(x);
        for (int i = 0; i < 6 && at + i < position + count; ++i) {
            x *= 16.0;
            const int digit = static_cast<int>(x);
            out.push_back(hex[digit]);
            x -= digit;
        }
    }
    return out;
}
```
<!-- /snippet -->

The job starts with a `start` message. It cuts the digits into chunks of
`chunk` digits and keeps at most `max_in_flight` of them out at a time.
For each batch it asks `mesh-info` to `select` as many hosts as it has
chunks to send, with at least one free slot and a load of at most 4 per
CPU. If `mesh-info` names no host, it uses its own. It then clones
itself onto the hosts, one chunk per clone, round robin:

<!-- snippet: cpp/examples/pi/pi.cpp#clone -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
// `n` chunks, round robin over `hosts` ("" is this host).
void send_to(const std::vector<std::string>& hosts, std::int64_t n) {
    std::size_t next = 0;
    for (auto& c : chunks_) {
        if (n <= 0) break;
        if (c.sent || c.done) continue;
        const std::string& host = hosts[next++ % hosts.size()];
        Task t{c.start, c.count, ++c.attempt, params_.work_ms};
        std::optional<std::string> destination;
        if (!host.empty() && host != host_) destination = host;
        auto worker = paglets::clone_raw(paglets::encode(t), {paglets::self()}, destination);
        if (!worker) {
            paglets::log("pi: no worker for chunk " + std::to_string(c.start) + ": " +
                         std::string(abi::error_name(worker.error())));
            break;
        }
        (void)worker->drop();  // the worker answers through its own endpoint to the job
        c.sent = true;
        c.sent_ms = now_ms();
        --n;
    }
}
```
<!-- /snippet -->

A worker asks the `compute-slots` of its host for one core. With
`run_now` it computes. With `queued` it waits for `compute.granted`. With a
redirect it moves to the host named in the decision and asks there again
when it arrives; if the move fails, it stays where it is. A host
without `compute-slots`, or one that rejects the request, lets it compute
at once:

<!-- snippet: cpp/examples/pi/pi.cpp#worker -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
void ask_for_slot() {
    auto slots = paglets::service("compute-slots");
    if (!slots) return compute();  // a host without compute-slots: just run
    svc::compute_slots::Client client{*slots};
    svc::compute_slots::SlotRequest q;
    q.job = "pi";
    q.cores = 1;
    auto sent = client.request_slot(q, [this](paglets::Result<svc::compute_slots::Decision> d, Message&) {
        if (!d) return compute();
        switch (d->verdict) {
            case svc::compute_slots::Verdict::run_now: run(d->lease); break;
            case svc::compute_slots::Verdict::queued: break;  // compute.granted comes later
            case svc::compute_slots::Verdict::redirect:
                if (!paglets::dispatch(d->host)) compute();
                break;
            case svc::compute_slots::Verdict::rejected: compute(); break;
        }
    });
    if (!sent) compute();
}
```
<!-- /snippet -->

The worker sends its digits to the job in a `result` message, with the
name of the host that computed them, releases its lease and disposes of
itself. With `work_ms`, a worker holds its slot at least that long before
it computes; the tests use this to make the chunks take time.

The job keeps each chunk's state: waiting, sent (with the time and the
attempt), or done with its digits. Every 500 ms it checks the chunks in
flight. A chunk that has had no answer for `chunk_timeout_ms` counts as
waiting again and goes out with the next batch, to the hosts `mesh-info`
selects then. A host that is gone no longer sends snapshots, so it drops
out of the selection once its last snapshot is too old. A late answer for a chunk that is already done is
ignored:

<!-- snippet: cpp/examples/pi/pi.cpp#resend -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
// Chunks without an answer in time go out again (their host may be gone).
void tick() {
    if (worker_) return;
    const std::int64_t now = now_ms();
    for (auto& c : chunks_) {
        if (c.sent && !c.done && now - c.sent_ms > params_.chunk_timeout_ms) {
            c.sent = false;
            ++resent_;
        }
    }
    send_more();
    if (!finished()) (void)paglets::after_raw(500, "tick", {});
}
```
<!-- /snippet -->

The messages, the worker's task and the status:

<!-- snippet: cpp/examples/pi/pi_msgs.hpp -->
Source: [`cpp/examples/pi/pi_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi_msgs.hpp)

```cpp
// Messages of the pi compute example (planning/cpp-compute.md, section 5).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace pi_msgs {

// `start`: compute `digits` hex digits of pi in chunks of `chunk` digits,
// at most `max_in_flight` chunks at a time on hosts mesh-info selects. A
// chunk not answered within `chunk_timeout_ms` (its host may be gone) is
// sent again elsewhere. `work_ms` makes each chunk take at least that long
// (for demonstrations and tests).
struct Start {
    std::int64_t digits = 1000;
    std::int64_t chunk = 60;
    std::int64_t max_in_flight = 8;
    std::int64_t chunk_timeout_ms = 30'000;
    std::int64_t work_ms = 0;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// The arguments of a worker (a clone of the job paglet on another host).
struct Task {
    std::int64_t start = 0;
    std::int64_t count = 0;
    std::int64_t attempt = 0;
    std::int64_t work_ms = 0;
};

// `result`: a worker's digits.
struct Result {
    std::int64_t start = 0;
    std::int64_t count = 0;
    std::int64_t attempt = 0;
    std::string digits;
    std::string host;  // key ID of the host that computed it
    std::string host_name;
};

struct StatusRequest {};

// `status`: progress (the digits so far, from the start, in order).
struct Status {
    std::int64_t digits = 0;  // asked for
    std::int64_t done = 0;    // contiguous from the start
    std::int64_t chunks = 0;
    std::int64_t chunks_done = 0;
    std::int64_t in_flight = 0;
    std::int64_t resent = 0;  // chunks sent again (no answer in time)
    bool finished = false;
    std::string hex;                 // "3." followed by `done` hex digits
    std::vector<std::string> hosts;  // names of the hosts that computed chunks
};

}  // namespace pi_msgs
```
<!-- /snippet -->

## Running it on a live mesh

Pi needs no policy rule. A host offers one compute slot per CPU unless
`serve` is given `--slots N` (see [serve](../cli-reference.md#serve)).
Launch the job on any host and start it:

```bash
H=build/macos-arm64/host/paglets-host
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/pi.wasm)
$H remote call $R $ID start '{"digits": 1000, "chunk": 60, "max_in_flight": 8,
    "chunk_timeout_ms": 30000, "work_ms": 0}'
$H remote call $R $ID status
```

`status` shows the progress: the chunks done and in flight, how many were
sent again, the names of the hosts that computed chunks, and in `hex` the
digits so far, `3.` followed by every digit up to the first chunk that is
still missing. `finished` is `true` when every chunk is done.
`remote landscape` shows the free slots that `mesh-info` sees, and
`remote slots` a host's leases and queue (see
[Working with live hosts](../hosts-and-meshes.md#working-with-live-hosts)).

> [!NOTE]
> Set `chunk_timeout_ms` well above the time a chunk takes, including the
> wait in a host's queue. A chunk that is merely slow is otherwise
> computed twice; the result stays correct, because the job keeps the
> first answer.

## Test results

The test runs the job on a mesh of three hosts inside one test process,
each with two compute slots
(`pi: the pi example computes across three hosts ...` in
`cpp/tests/test_compute.cpp`). It asks for 1200 digits in chunks of 30,
with at most six in flight, a chunk timeout of 3 s, and 250 ms of work per
chunk. It checks that:

- the chunks spread over all three hosts before the job is finished;
- when the third host has a worker of its own running, the test shuts
  that host down, and the job still finishes;
- all 1200 digits and 40 chunks are done, and at least one chunk was sent
  again after the loss;
- the result starts with `3.243F6A8885A308D3...` and equals the digits the
  BBP formula gives on the test host.

<!-- results: pi -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hex digits computed | chunks | hosts used before one was lost | chunks sent again after the loss |
|---|---|---|---|---|---|---|---|---|---|
| pi: the pi example computes across three hosts and survives the loss of one (WP16 exit) | in-process | passed | 6.3 s | Darwin arm64, Release build | 2026-10-12 | 1200 | 40 | 3 | 4 |
| pi: the pi example computes across three hosts and survives the loss of one (WP16 exit) | worker processes | passed | 5.6 s | Darwin arm64, Release build | 2026-10-12 | 1200 | 40 | 3 | 4 |
<!-- /results -->
