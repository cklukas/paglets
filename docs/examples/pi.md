# Pi

The Pi example computes digits of pi on the hosts of the mesh. A job
paglet cuts the hexadecimal digits into chunks and sends each chunk, in a
clone of itself, to a host with free compute slots. The workers compute
their digits, send them to the job and end. The job takes the chunks in
order as they come in, writes them to a file on its host, and sends a
chunk out again when its host does not answer in time, so the result is
complete even when a host is lost during the run. At the end it converts
the hexadecimal digits into the familiar decimal ones, `3.14159...`.

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
- **Results that do not fill the paglet's memory.** A paglet has at most
  16 MB of memory, and a message at most 1 MB. The job therefore keeps
  only the chunks that came early: every chunk that is next in order goes
  to a file below a named root of its host (`files.write`, with a grant)
  and leaves the job's memory. `status` shows the first 4096 digits and
  the progress; the files have all of them.

## Why it is useful

Pi stands for any computation that splits into independent parts:
parameter sweeps, rendering tiles, batches of files. The
Bailey-Borwein-Plouffe (BBP) formula gives the hexadecimal digits of pi at
any position without the digits before them, so every chunk can be
computed on its own and the result is easy to check. The example shows the
whole pattern in about 300 lines: the job finds hosts with spare capacity,
each host decides when work runs, and lost work is sent again. The mesh
needs no job server and no shared storage.

## Hexadecimal and decimal digits

The BBP formula gives hexadecimal digits: `3.243F6A88...` is pi in base 16.
No such formula is known for decimal digits, and turning hexadecimal
digits into decimal ones cannot be done chunk by chunk either: a change of
base touches every digit. The job does it once, at the end. While the
chunks come in, it keeps the fraction of pi as a binary number, half a
byte per hexadecimal digit (8 digits in a 32-bit word). Then it multiplies
that number by 10^9 again and again; each time, the nine decimal digits
that move in front of the point are the next ones. `n` hexadecimal digits
fix `n × log10(16) ≈ 1.2 n` decimal digits; the job leaves out the last two,
so that a carry hidden in a run of nines or zeros cannot make one wrong.
As the digits come out, it also drops the binary digits that the
remaining decimal digits no longer need, so the conversion takes about
`n² / 120` multiplications, in slices of 200 ms between messages, so
that no handler call runs long.

The fraction takes half a byte per hexadecimal digit, so a paglet's
memory holds the conversion of up to 20 million hexadecimal digits; the
job refuses more when `decimal` is set. Without `decimal`, and with
output files, the number of digits is limited only by time. The time is
what limits it in practice: the BBP formula needs work proportional to
the position for every digit, so `n` digits take about `n²` steps. The
[live run](#on-real-machines) shows what that means on real machines.

## How it works

The digits come from the BBP formula in `bbp.hpp`. A `Block` computes six
digits after the point from any position, which is well within the
precision of a `double`. Its powers of 16 are taken modulo `8k + j` in
64-bit integers, which is exact and much faster than floating-point
`fmod` (WebAssembly has no instruction for it). `step` computes a bounded
number of terms, so the work of a block can be spread over several
handler calls. The host tests use the same header to check the result:

<!-- snippet: cpp/examples/pi/bbp.hpp#digits -->
Source: [`cpp/examples/pi/bbp.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/bbp.hpp)

```cpp
// Up to six hex digits of pi after the point, starting at `position` (0: the
// first digit after "3."), from
//   pi = sum_k 16^-k (4/(8k+1) - 2/(8k+4) - 1/(8k+5) - 1/(8k+6)).
// The work grows with the position: about 4 * position terms. step()
// computes at most `terms` of them, so a paglet can spread a block over
// several handler calls (the parts of a sum add up modulo 1).
class Block {
public:
    Block() = default;
    Block(std::int64_t position, int count) : at_(position), count_(count) {}

    // True when the digits are there.
    bool step(std::int64_t terms) {
        static constexpr int js[4] = {1, 4, 5, 6};
        while (series_ < 4 && terms > 0) {
            const std::int64_t end = std::min(at_ + 1, k_ + terms);
            part_[series_] += series_head(js[series_], at_, k_, end);
            part_[series_] -= std::floor(part_[series_]);
            terms -= end - k_;
            k_ = end;
            if (k_ == at_ + 1) {
                part_[series_] += series_tail(js[series_], at_);
                ++series_;
                k_ = 0;
            }
        }
        return series_ == 4;
    }

    std::string digits() const {
        static constexpr char hex[] = "0123456789ABCDEF";
        double x = 4.0 * part_[0] - 2.0 * part_[1] - part_[2] - part_[3];
        x -= std::floor(x);
        std::string out;
        for (int i = 0; i < count_; ++i) {
            x *= 16.0;
            const int digit = static_cast<int>(x);
            out.push_back(hex[digit]);
            x -= digit;
        }
        return out;
    }

private:
    std::int64_t at_ = 0;
    int count_ = 0;
    int series_ = 0;
    std::int64_t k_ = 0;
    double part_[4] = {0, 0, 0, 0};
};
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

A handler call may take at most 5 seconds; the host ends a paglet whose
handler runs longer. Far out, a single block of six digits takes seconds,
so the worker computes in slices of about 200 ms: each slice sums a few
thousand terms of the series (the parts of a sum add up modulo 1), and a
message to itself continues where it stopped:

<!-- snippet: cpp/examples/pi/pi.cpp#slices -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
// A slice of about 200 ms: a handler call may take at most 5 s, and far
// out a single block of six digits takes seconds. The rest follows in
// the next message.
void compute_more() {
    const std::int64_t began = now_ms();
    while (static_cast<std::int64_t>(digits_.size()) < task_.count) {
        if (block_.step(20'000)) {
            digits_ += block_.digits();
            const std::int64_t at = task_.start + static_cast<std::int64_t>(digits_.size());
            const std::int64_t left = task_.start + task_.count - at;
            if (left > 0) block_ = pi_bbp::Block(at, static_cast<int>(std::min<std::int64_t>(6, left)));
        }
        if (now_ms() - began >= 200 && static_cast<std::int64_t>(digits_.size()) < task_.count) {
            (void)paglets::after_raw(0, "compute-more", {});
            return;
        }
    }
    report_digits();
}
```
<!-- /snippet -->

The worker sends its digits to the job in a `result` message, with the
name of the host that computed them, releases its lease and disposes of
itself. With `work_ms`, a worker holds its slot at least that long before
it computes; the tests use this to make the chunks take time.

The job takes chunks in order as they arrive. Each chunk that is next in
order goes into the file and into the fraction, and its digits leave the
job's memory:

<!-- snippet: cpp/examples/pi/pi.cpp#in_order -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
// Chunks that are next in order leave the job's memory: into the file,
// and into the fraction for the decimal conversion.
void take_in_order() {
    while (next_ < chunks_.size() && chunks_[next_].done) {
        Chunk& c = chunks_[next_++];
        done_ += c.count;
        if (shown_hex_.size() < shown_digits) shown_hex_ += c.digits.substr(0, shown_digits - shown_hex_.size());
        if (params_.decimal) {
            for (char h : c.digits) add_nibble(static_cast<std::uint32_t>(h <= '9' ? h - '0' : h - 'A' + 10));
        }
        if (dir_) {
            hex_file_.pending += c.digits;
            flush(hex_file_);
        }
        std::string().swap(c.digits);
    }
    if (finished() && state_ == "computing") {
        if (params_.decimal) return start_conversion();
        state_ = "writing";
        maybe_done();
    }
}

// The fraction, 8 hex digits per 32-bit limb, the most significant first.
void add_nibble(std::uint32_t v) {
    partial_ = (partial_ << 4) | v;
    if (++partial_n_ == 8) {
        limbs_.push_back(partial_);
        partial_ = 0;
        partial_n_ = 0;
    }
}
```
<!-- /snippet -->

When every hexadecimal digit is there, the job converts the fraction, one
slice of passes per message:

<!-- snippet: cpp/examples/pi/pi.cpp#convert -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
// A slice of the conversion: passes of multiplying the fraction by 10^9,
// whose overflow above the point is the next nine decimal digits.
void convert() {
    if (state_ != "converting") return;
    const std::int64_t began = now_ms();
    std::string digits;
    while (decimal_done_ + static_cast<std::int64_t>(digits.size()) < decimal_total_ && now_ms() - began < 200) {
        // Only the precision the remaining digits need (and two limbs more).
        const std::int64_t remaining = decimal_total_ - decimal_done_ - static_cast<std::int64_t>(digits.size());
        const auto need = static_cast<std::size_t>(static_cast<double>(remaining) * 3.3219280949 / 32.0) + 2;
        if (limbs_.size() > need) limbs_.resize(need);
        std::uint64_t carry = 0;
        for (std::size_t i = limbs_.size(); i-- > 0;) {
            const std::uint64_t t = static_cast<std::uint64_t>(limbs_[i]) * 1'000'000'000ULL + carry;
            limbs_[i] = static_cast<std::uint32_t>(t);
            carry = t >> 32;
        }
        char nine[16];
        std::snprintf(nine, sizeof nine, "%09u", static_cast<unsigned>(carry));
        digits.append(nine, static_cast<std::size_t>(std::min<std::int64_t>(9, remaining)));
    }
    decimal_done_ += static_cast<std::int64_t>(digits.size());
    if (shown_dec_.size() < shown_digits) shown_dec_ += digits.substr(0, shown_digits - shown_dec_.size());
    if (dir_) {
        dec_file_.pending += digits;
        flush(dec_file_);
    }
    if (decimal_done_ < decimal_total_) {
        (void)paglets::after_raw(0, "convert", {});
        return;
    }
    std::vector<std::uint32_t>().swap(limbs_);
    state_ = "writing";
    maybe_done();
}
```
<!-- /snippet -->

The job keeps each chunk's state: waiting, sent (with the time and the
attempt), or done with its digits until they are taken in order. Every 500 ms it checks the chunks in
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
//
// With `output_root`, the digits go, in order as they come in, to files
// below that named root of the job's host (a grant for `write` and
// `create`): `<output_path>.hex` and, with `decimal`, `<output_path>.txt`.
// The job then keeps only the chunks that came early, not all digits.
// With `decimal`, the job converts the hex digits into decimal digits at the
// end (about 1.2 per hex digit; the conversion needs every digit, so it
// cannot run per chunk).
struct Start {
    std::int64_t digits = 1000;
    std::int64_t chunk = 60;
    std::int64_t max_in_flight = 8;
    std::int64_t chunk_timeout_ms = 30'000;
    std::int64_t work_ms = 0;
    bool decimal = true;
    std::string output_root;  // empty: the digits stay in the job's memory
    std::string output_path = "pi";
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

// `status`: progress. `hex` and `decimal` show at most the first `shown`
// digits (the files have all of them).
struct Status {
    std::string state;        // idle, computing, converting, done, failed
    std::int64_t digits = 0;  // hex digits asked for
    std::int64_t done = 0;    // hex digits contiguous from the start
    std::int64_t chunks = 0;
    std::int64_t chunks_done = 0;
    std::int64_t in_flight = 0;
    std::int64_t resent = 0;  // chunks sent again (no answer in time)
    bool finished = false;    // every hex digit is there
    std::string hex;          // "3." followed by hex digits
    std::int64_t decimal_digits = 0;  // decimal digits that hex `digits` give
    std::int64_t decimal_done = 0;    // converted so far
    std::string decimal;              // "3." followed by decimal digits
    std::int64_t shown = 0;
    std::vector<std::string> files;  // the files written, below `output_root`
    std::vector<std::string> hosts;  // names of the hosts that computed chunks
    std::string error;
};

}  // namespace pi_msgs
```
<!-- /snippet -->

## Running it on a live mesh

Without output files, Pi needs no policy rule. A host offers one compute
slot per CPU unless `serve` is given `--slots N` (see
[serve](../cli-reference.md#serve)). For output files, the job's host needs
a named root, for example `--root results=/srv/results`, and a rule that
lets the owner's paglets write there:

```bash
$H ledger rule --ledger ledger --admin alice.key --name "pi results" --decision allow \
    --service files --op write --op create --root results
```

Launch the job on any host and start it:

```bash
H=build/macos-arm64/host/paglets-host
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/pi.wasm)
$H remote call $R $ID start '{"digits": 100000, "chunk": 500, "max_in_flight": 32,
    "chunk_timeout_ms": 600000, "decimal": true, "output_root": "results", "output_path": "pi/run-1"}'
$H remote call $R $ID status
```

`status` shows the progress: `state` (`computing`, `converting`,
`writing`, then `done`), the chunks done and in flight, how many were sent
again, the names of the hosts that computed chunks, and the first 4096
digits so far, in hexadecimal (`hex`) and, once converted, in decimal
(`decimal`). `finished` is `true` when every chunk is done. The files are
`pi/run-1.hex` and `pi/run-1.txt` below the root, each `3.` followed by
the digits.
`remote landscape` shows the free slots that `mesh-info` sees, and
`remote slots` a host's leases and queue (see
[Working with live hosts](../hosts-and-meshes.md#working-with-live-hosts)).

> [!NOTE]
> Set `chunk_timeout_ms` well above the time a chunk takes, including the
> wait in a host's queue. A chunk that is merely slow is otherwise
> computed twice; the result stays correct, because the job keeps the
> first answer.

## On real machines

`cpp/tools/mesh_bench.sh` (see
[File Courier](file-courier.md#transfer-speed-between-real-machines)) runs
the job on a mesh of a Mac Studio, a Raspberry Pi 4 and a Raspberry Pi
Compute Module 5 on a LAN, every host with one compute slot per CPU, and
writes the digits into files on the Mac. It checks the first 100 decimal
digits. The last run:

<!-- results: live_pi -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hex digits in the file | decimal digits in the file | chunks of 500 digits | hosts that computed chunks | computing the hex digits (s) | converting into decimal digits (s) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| pi, 100000 hex digits | LAN, worker processes | passed | 5 min 13 s | Release build | 2026-10-12 | 100000 | 120409 | 200 | mac, pi, uconsole | 313 | < 1 |
<!-- /results -->

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
  BBP formula gives on the test host;
- the job converts them into 1442 decimal digits, which start with the
  first 100 decimal digits of pi.

A second test (`example: Pi writes its digits to files ...` in
`cpp/tests/test_examples.cpp`) writes 3000 hexadecimal digits to files
below a named root and checks that the `.hex` file has every digit in
order and the `.txt` file 3610 decimal digits that start with those of
pi, and that a conversion too large for the paglet's memory is refused.
The decimal digits were also checked against `bc` (`4*a(1)`), for 8000
hexadecimal digits: all 9630 decimal digits agree.

<!-- results: pi -->
| Test | Mode | Outcome | Duration | Platform | Recorded |
|---|---|---|---|---|---|
| example: Pi writes its digits to files in order and converts them into decimal digits | in-process | passed | 0.8 s | Darwin arm64, Release build | 2026-10-12 |
| example: Pi writes its digits to files in order and converts them into decimal digits | worker processes | passed | 0.9 s | Darwin arm64, Release build | 2026-10-12 |
| pi: the pi example computes across three hosts and survives the loss of one (WP16 exit) | in-process | passed | 6.1 s | Darwin arm64, Release build | 2026-10-12 |
| pi: the pi example computes across three hosts and survives the loss of one (WP16 exit) | worker processes | passed | 4.2 s | Darwin arm64, Release build | 2026-10-12 |

example: Pi writes its digits to files in order and converts them into decimal digits:

| Measured | in-process | worker processes |
|---|---|---|
| hex digits written to a file in order | 3000 | 3000 |
| decimal digits written to a file | 3610 | 3610 |

pi: the pi example computes across three hosts and survives the loss of one (WP16 exit):

| Measured | in-process | worker processes |
|---|---|---|
| hex digits computed | 1200 | 1200 |
| decimal digits converted from them | 1442 | 1442 |
| chunks | 40 | 40 |
| hosts used before one was lost | 3 | 3 |
| chunks sent again after the loss | 6 | 3 |
<!-- /results -->
