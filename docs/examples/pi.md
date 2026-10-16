<!-- Copyright (c) 2026 by C. Klukas.
     Licensed under the MIT License. See LICENSE for details. -->

# Pi

The Pi paglet computes hexadecimal digits of pi as compute jobs across
its mesh. It stays responsive while the jobs run, takes completed chunks
in order, retries failed chunks and converts the complete hex fraction to
decimal in one separate job. It creates no worker paglets and needs no
manual slices of the computation. See [Compute jobs](../compute-jobs.md).

## Messages

<!-- snippet: cpp/examples/pi/pi_msgs.hpp#messages -->
Source: [`cpp/examples/pi/pi_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi_msgs.hpp)

```cpp
// `start`: compute `digits` hex digits of pi in chunks of `chunk` digits,
// each a compute job (planning/cpp-compute-jobs.md) on one of `hosts`
// ({"any"}: the hosts of the mesh with free slots; empty: this host), at
// most `max_in_flight` at a time. A chunk whose job fails (its host went
// away, it ran longer than `chunk_timeout_ms`) is submitted again.
// `work_ms` makes each chunk take at least that long (for demonstrations
// and tests).
//
// With `output_root`, the digits go, in order as they come in, to files
// below that named root of the job's host (a grant for `read`, `write` and
// `create`): `<output_path>.hex` and, with `decimal`, `<output_path>.txt`.
// The paglet then keeps only the chunks that came early, not all digits.
// With `decimal`, one long compute job converts the hex digits into decimal
// digits at the end (about 1.2 per hex digit; the conversion needs every
// digit, so it cannot run per chunk). Without `output_root` it converts at
// most 800 000 hex digits (its input and output are messages).
struct Start {
    std::int64_t digits = 1000;
    std::int64_t chunk = 1000;
    std::int64_t max_in_flight = 8;
    std::int64_t chunk_timeout_ms = 0;  // wall time of a chunk's job; 0: the host's default
    std::int64_t work_ms = 0;
    bool decimal = true;
    std::string output_root;  // empty: the digits stay in the paglet's memory
    std::string output_path = "pi";
    std::vector<std::string> hosts = {"any"};
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// The input of a chunk's job.
struct Task {
    std::int64_t start = 0;
    std::int64_t count = 0;
    std::int64_t work_ms = 0;
};
```
<!-- /snippet -->

Send `start` with parameters, then poll `status`. `status` gives contiguous
hex digits completed, chunks in flight, retries, participating host names,
AOT chunks and job-reported elapsed times (`compute_ms`, `convert_ms`).
`compute_ms` sums successful chunk jobs; these can overlap on different
hosts and include any configured minimum hold. `convert_ms` reports the
successful conversion job. These values are separate from the benchmark
driver's coarse polling observations, which include RPC and scheduling
delays. Its hex and decimal previews show at most the first 4096 digits.

Using the connection, owner key and local ledger from the mesh setup:

```bash
PAGLET=$(paglets-host remote launch --connect "$URL" --key "$OWNER_KEY" --ledger "$LEDGER" pi.wasm)
paglets-host remote call --connect "$URL" --key "$OWNER_KEY" --ledger "$LEDGER" "$PAGLET" start '{"digits":1000,"chunk":1000,"hosts":["any"],"decimal":true}'
paglets-host remote call --connect "$URL" --key "$OWNER_KEY" --ledger "$LEDGER" "$PAGLET" status
```

With `output_root`, the host needs a named root and a grant for read,
write and create. `output_path` defaults to `pi`; output files are `pi.hex`
and `pi.txt`. Without a root, results remain in the paglet's memory and
decimal conversion accepts at most 800,000 hex digits. Large file-backed
conversions still need the whole fraction in the conversion job's memory;
the configured job memory and time caps apply.

## Digit jobs

The Bailey–Borwein–Plouffe formula computes digits at any hexadecimal
position without calculating earlier digits. The registered `digits`
function computes one chunk in a fresh instance and reports progress:

<!-- snippet: cpp/examples/pi/pi.cpp#digits_job -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
// A chunk of hex digits: as long as it takes, no slicing. The paglet's
// handlers run meanwhile; this is a separate instance of the module.
void digits(compute::JobContext& job) {
    Task t;
    if (!job.input(t) || t.count <= 0) return job.fail(abi::malformed);
    const std::int64_t began = now_ms();
    std::string out;
    out.reserve(static_cast<std::size_t>(t.count));
    while (static_cast<std::int64_t>(out.size()) < t.count) {
        const std::int64_t at = t.start + static_cast<std::int64_t>(out.size());
        pi_bbp::Block block(at, static_cast<int>(std::min<std::int64_t>(6, t.count - static_cast<std::int64_t>(out.size()))));
        while (!block.step(20'000)) {
            if (job.cancelled()) return job.fail(abi::cancelled);
        }
        out += block.digits();
        job.progress(static_cast<std::int64_t>(out.size()), t.count);
    }
    // Demonstrations and tests: a chunk takes at least work_ms.
    while (now_ms() - began < t.work_ms) {
        if (job.cancelled()) return job.fail(abi::cancelled);
    }
    (void)job.output(out);
}
PAGLETS_JOB(digits)
```
<!-- /snippet -->

The coordinator submits up to `max_in_flight` chunks. The host selects
among `hosts` using the mesh's free-slot snapshots, then admits or queues
each job. An empty host list means this host; `{"any"}` permits the mesh.

<!-- snippet: cpp/examples/pi/pi.cpp#submit -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
// Chunks go out as jobs while fewer than max_in_flight run. The host
// picks where (`hosts: any`: the one with the most free slots); on a
// host without a mesh they run here.
void send_more() {
    if (state_ != "computing") return;
    const auto now = now_ms();
    for (auto& c : chunks_) {
        if (in_flight() >= params_.max_in_flight) break;
        if (c.sent || c.done || now < c.retry_at) continue;
        if (++c.attempt > max_attempts) return fail("chunk " + std::to_string(c.start) + " failed every time");
        compute::Options o;
        o.hosts = local_only_ ? std::vector<std::string>{} : params_.hosts;
        o.max_ms = params_.chunk_timeout_ms;
        const bool elsewhere = !o.hosts.empty();
        auto job = compute::submit("digits", Task{c.start, c.count, params_.work_ms},
                                   [this, start = c.start](paglets::Result<compute::Output> r, Message& m) {
                                       on_result(start, std::move(r), m);
                                   },
                                   std::move(o));
        if (!job && job.error() == abi::unsupported && elsewhere) {
            local_only_ = true;  // no mesh: this host only
            --c.attempt;
            return send_more();
        }
        if (!job) return fail("submitting a job: " + std::string(abi::error_name(job.error())));
        c.sent = true;
    }
}
```
<!-- /snippet -->

A failed job makes its chunk eligible for resubmission after an exponential
backoff, starting at 200 ms and bounded by the example's five-attempt limit.
Other completed chunks cannot bypass this delay, and one periodic timer
drives retries. A late result for an already completed chunk is ignored.
`chunk_timeout_ms` caps running time; zero uses the host default.
A remote host leaving causes `gone`, which also permits retry.

<!-- snippet: cpp/examples/pi/pi.cpp#result -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
void on_result(std::int64_t start, paglets::Result<compute::Output> r, Message& m) {
    auto it = std::ranges::find_if(chunks_, [&](const Chunk& c) { return c.start == start; });
    if (it == chunks_.end() || it->done) return;
    Chunk& c = *it;
    std::string digits;
    if (!r || !r->decode(digits) || digits.size() != static_cast<std::size_t>(c.count)) {
        // Its host went away, or it ran too long: again, elsewhere.
        const auto p = m.payload();
        paglets::log("pi: chunk " + std::to_string(c.start) + " again: " +
                     (r ? std::string("malformed output") : std::string(abi::error_name(r.error())) + " " +
                                                                std::string(p.begin(), p.end())));
        c.sent = false;
        ++resent_;
        // Other chunks can finish meanwhile: their send_more() calls
        // must respect this delay too, rather than exhaust every retry.
        c.retry_at = now_ms() + (200 << std::min<std::int64_t>(c.attempt - 1, 4));
        return;
    }
    c.done = true;
    c.digits = std::move(digits);
    auto name = names_.find(r->info.host);
    hosts_.insert(name != names_.end() ? name->second : r->info.host.substr(0, 8));
    if (r->info.aot) ++chunks_aot_;
    compute_ms_ += r->info.elapsed_ms;
    take_in_order();
    send_more();
}
```
<!-- /snippet -->

## Ordered output and conversion

Chunks completed early wait in the coordinator until all preceding chunks
are available. Contiguous chunks go to the output file, or into the
in-memory hex result:

<!-- snippet: cpp/examples/pi/pi.cpp#in_order -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
// Chunks that are next in order leave the paglet's memory: into the file
// (or, without files, into the digits kept for the conversion).
void take_in_order() {
    while (next_ < chunks_.size() && chunks_[next_].done) {
        Chunk& c = chunks_[next_++];
        done_ += c.count;
        if (shown_hex_.size() < shown_digits) shown_hex_ += c.digits.substr(0, shown_digits - shown_hex_.size());
        if (dir_) {
            hex_file_.pending += c.digits;
            flush(hex_file_);
        } else if (params_.decimal) {
            all_hex_ += c.digits;
        }
        std::string().swap(c.digits);
    }
    if (finished() && state_ == "computing") {
        state_ = "writing";
        maybe_converted();
    }
}
```
<!-- /snippet -->

The `to_decimal` job reads the hex fraction as inline input or an artifact,
then performs the whole conversion outside the handler lane. The paglet
stores the resulting bytes, or saves the output artifact as the decimal
file. It remains available for status calls throughout.

<!-- snippet: cpp/examples/pi/pi.cpp#convert -->
Source: [`cpp/examples/pi/pi.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/pi/pi.cpp)

```cpp
// Every hex digit is there (and in the file): one job converts them all,
// here. Its input is the hex file (an artifact of this host) or, without
// files, the digits themselves; its output is the decimal text.
void start_conversion() {
    state_ = "converting";
    decimal_total_ = std::max<std::int64_t>(0, static_cast<std::int64_t>(static_cast<double>(done_) * std::log10(16.0)) - 2);
    compute::Options o;
    o.max_ms = 24LL * 3'600'000;                     // capped by the host
    o.memory_mb = done_ / 2 / (1024 * 1024) + 32;    // the fraction, half a byte per digit
    auto converted = [this](paglets::Result<compute::Output> r, Message& m) {
        if (!r) {
            const auto p = m.payload();
            return fail("conversion: " + std::string(abi::error_name(r.error())) + " " + std::string(p.begin(), p.end()));
        }
        convert_ms_ += r->info.elapsed_ms;
        if (r->artifact) return save_decimal(std::move(*r->artifact));
        std::string text(r->data.begin(), r->data.end());
        decimal_done_ = std::max<std::int64_t>(0, static_cast<std::int64_t>(text.size()) - 2);
        shown_dec_ = text.size() > 2 ? text.substr(2, shown_digits) : std::string();
        converted_ = true;
        if (dir_) {
            dec_file_.path = dec_path_;
            dec_file_.pending = std::move(text);
            flush(dec_file_);
        }
        maybe_done();
    };
    if (!dir_) {
        auto job = compute::submit_raw("to_decimal", paglets::Bytes(all_hex_.begin(), all_hex_.end()), converted,
                                       std::move(o));
        std::string().swap(all_hex_);
        if (!job) fail("conversion: " + std::string(abi::error_name(job.error())));
        return;
    }
    pt::share_file(*dir_, hex_file_.path, [this, o, converted](paglets::Result<pt::SharedFile> f) mutable {
        if (!f) return fail(pt::error_text("share " + hex_file_.path, f.error()));
        o.input_artifact = f->content;
        auto job = compute::submit_raw("to_decimal", {}, converted, std::move(o));
        (void)f->content.drop();
        if (!job) fail("conversion: " + std::string(abi::error_name(job.error())));
    });
}

// A large decimal text came as an artifact: into the file, and its
// beginning into the status.
void save_decimal(paglets::Capability content) {
    pt::save_file(*dir_, dec_path_, content, [this, content](paglets::Result<svc::files::Entry> e) mutable {
        (void)content.drop();
        if (!e) return fail(pt::error_text("save " + dec_path_, e.error()));
        decimal_done_ = static_cast<std::int64_t>(e->size) - 2;
        dec_file_.path = dec_path_;
        converted_ = true;
        pt::endpoint("files", [this](paglets::Result<paglets::Endpoint> files) {
            if (!files) return maybe_done();
            svc::files::Client client{*files};
            auto sent = client.read(
                svc::files::ReadRequest{dec_path_, 2, shown_digits},
                [this](paglets::Result<svc::files::ReadReply> r, Message&) {
                    if (r) shown_dec_.assign(r->data.begin(), r->data.end());
                    maybe_done();
                },
                pt::lending(*dir_));
            if (!sent) maybe_done();
        });
    });
}
```
<!-- /snippet -->

Approximately 1.2 decimal digits are determined per hex digit; the example
omits two trailing digits to avoid rounding the truncated fraction. The
conversion repeatedly multiplies the fraction by 10^9. Large conversions
can be expensive even with AOT; use `decimal: false` when only hex digits
are required.

## Validation and live measurements

The `pi:` and `example: Pi` tests check digits, retries and file output.
The jobs tests check local and remote execution, cancellation, artifacts,
recovery and interpreter/AOT result identity. Run them through the CMake
test preset; [Building](../building.md) explains filtering.

Historical live results in `docs/results/live_pi.json` were collected with
the earlier sliced-worker implementation. They are not performance results
for the compute-jobs implementation. New measurements must record the
host build, execution mode and the same input on each host.

The compute-jobs integration run below used two slots per host and a
200 ms minimum chunk hold to exercise scheduling across the Mac, rpi4 and
uconsole. All three computed chunks; there were no retries, the output
sizes matched and the decimal prefix was checked. This short cold run had
no AOT chunks. Its timing includes the artificial hold and one-second
status polling, so it is an integration measurement. The historical
large Pi results above remain from the earlier implementation.

<!-- results: live_jobs_pi -->
| Test | Mode | Outcome | Duration | Platform | Recorded |
|---|---|---|---|---|---|
| Pi compute jobs, 6000 hex digits | LAN, worker processes | passed | 3.0 s | Mac RelWithDebInfo; both Pis Release build | 2026-10-14 |

Pi compute jobs, 6000 hex digits:

| Measured | LAN, worker processes |
|---|---|
| hex digits in the file | 6000 |
| decimal digits in the file | 7222 |
| minimum chunk hold | 200 ms |
| chunks of 500 digits | 12 |
| hosts that computed chunks | mac, pi, uconsole |
| computing the hex digits | 3 s |
| converting into decimal digits | < 1 s |
<!-- /results -->

The final 2026-10-15 run completed 100,000 hexadecimal digits on all three
physical hosts and converted the output to decimal. The first 100 decimal
digits were checked; this does not independently verify every generated
digit. Two chunks were resubmitted. The conversion took 147 ms as reported
by the job, while the driver includes one-second polling. Compiled jobs use
explicit software bounds checks, correcting the earlier large-conversion
trap. Full receipts are in `planning/validation/cpp-three-host-20261015.json`.

New `mesh_bench.sh` records name the run `Pi example`, report successful job
elapsed times in milliseconds and AOT chunk counts, and label the separate
poll observations and their one-second clock resolution. Historical records
above retain their original names and observation scope.

<!-- results: live_three_host_pi -->
| Test | Mode | Outcome | Duration | Platform | Recorded |
|---|---|---|---|---|---|
| pi, 100000 hex digits | LAN, worker processes | passed | 1 min 09 s | mixed; see live_hosts build | 2026-10-15 |

pi, 100000 hex digits:

| Measured | LAN, worker processes |
|---|---|
| hex digits in the file | 100000 |
| decimal digits in the file | 120409 |
| minimum chunk hold | 0 ms |
| chunks of 500 digits | 200 |
| hosts that computed chunks | mac, pi, uconsole |
| computing the hex digits | 69 s |
| converting into decimal digits | < 1 s |
<!-- /results -->
