// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Pi compute example (planning/cpp-compute.md, section 5): hex digits of pi
// in chunks, computed on the hosts of the mesh.
//
// The job paglet stays where it was started. For each chunk it asks
// mesh-info for a host and sends a clone of itself there (a worker). A
// worker asks the compute-slots of its host for a slot: it runs at once,
// waits for `compute.granted`, or moves on when it gets
// `compute.redirect`. It sends its digits to the job and disposes itself.
// The job puts the chunks together in order; a chunk not answered in time
// (its host may be gone) goes out again to another host.
//
// The job takes the chunks in order as they come in: it writes them to a
// file below a named root of its host (when asked to) and adds them to the
// fraction of pi as a binary number, half a byte per hex digit. It keeps
// only chunks that came early, so many digits need little memory. At the
// end it converts the fraction into decimal digits: every pass multiplies it
// by 10^9 and takes the nine digits that move in front of the point. That
// needs every hex digit, so it cannot run per chunk; it runs in slices of
// 200 ms between messages.

#include "pi.schema.gen.hpp"

#include "bbp.hpp"

#include <paglets/paglet.hpp>
#include <paglets/patterns/files.hpp>
#include <paglets/services/compute_slots.gen.hpp>
#include <paglets/services/mesh_info.gen.hpp>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace {

using namespace pi_msgs;
namespace svc = paglets::services;
namespace abi = paglets::abi;
using paglets::Message;
namespace pt = paglets::patterns;

constexpr std::size_t shown_digits = 4096;           // in `status`
constexpr std::int64_t max_decimal_hex = 20'000'000;  // the fraction must fit the paglet's memory

class Pi : public paglets::Paglet {
public:
    Pi() {
        router().on<Start>("start", [this](const Start& s, Message& m) { m.reply(start(s)); });
        router().on("status", [this](Message& m) { m.reply(status()); });
        router().on<Result>("result", [this](const Result& r, Message&) { on_result(r); });
        router().on("tick", [this](Message&) { tick(); });
        // A worker's turn: a slot, or a host with free slots.
        router().on<svc::compute_slots::Decision>(
            "compute.granted", [this](const svc::compute_slots::Decision& d, Message&) { run(d.lease); });
        router().on<svc::compute_slots::Decision>("compute.redirect",
                                                  [this](const svc::compute_slots::Decision& d, Message&) {
                                                      if (!paglets::dispatch(d.host)) ask_for_slot();
                                                  });
        router().on("work-done", [this](Message&) { compute(); });
        router().on("compute-more", [this](Message&) { compute_more(); });
        router().on("convert", [this](Message&) { convert(); });
    }

    void on_cloned(paglets::Started& s) override {
        // A worker: the job sent it here with a chunk and its endpoint.
        worker_ = true;
        if (!s.decode_args(task_) || s.caps.empty()) {
            (void)paglets::dispose();
            return;
        }
        job_ = paglets::Endpoint(s.caps[0].release());
        chunks_.clear();
        ask_for_slot();
    }

    void on_arrived(const abi::ArrivedEvent&) override {
        if (worker_) ask_for_slot();  // redirected here: ask again
    }

    void on_move_failed(const abi::MoveFailed& f) override {
        if (worker_ && f.clone.empty()) ask_for_slot();  // the redirect failed: stay
    }

private:
    struct Chunk {
        std::int64_t start = 0;
        std::int64_t count = 0;
        std::string digits;
        bool done = false;
        bool sent = false;
        std::int64_t sent_ms = 0;
        std::int64_t attempt = 0;
    };

    static std::int64_t now_ms() { return paglets::now_ns(abi::Clock::monotonic) / 1'000'000; }

    // -- the job ---------------------------------------------------------------------------

    Started start(const Start& s) {
        if (started_) return Started{false, "already started"};
        if (s.digits <= 0 || s.chunk <= 0 || s.max_in_flight <= 0) return Started{false, "invalid parameters"};
        if (s.decimal && s.digits > max_decimal_hex) {
            return Started{false, "the decimal conversion holds every digit in memory: at most " +
                                      std::to_string(max_decimal_hex) + " hex digits"};
        }
        started_ = true;
        params_ = s;
        state_ = "computing";
        for (std::int64_t at = 0; at < s.digits; at += s.chunk) {
            chunks_.push_back(Chunk{at, std::min(s.chunk, s.digits - at)});
        }
        if (s.decimal) limbs_.reserve(static_cast<std::size_t>(s.digits / 8 + 1));
        if (s.output_root.empty()) {
            begin();
            return Started{true, {}};
        }
        // The files: a directory of the root, granted on this host.
        hex_file_.path = s.output_path + ".hex";
        if (s.decimal) dec_file_.path = s.output_path + ".txt";
        hex_file_.pending = dec_file_.pending = "3.";
        pt::granted_dir(s.output_root, "", {"write", "create"}, "pi: digits", [this](paglets::Result<paglets::Capability> d) {
            if (!d) return fail(pt::error_text("files of " + params_.output_root, d.error()));
            dir_ = std::make_shared<paglets::Capability>(*d);
            const auto slash = params_.output_path.rfind('/');
            pt::make_dirs(*dir_, slash == std::string::npos ? std::string() : params_.output_path.substr(0, slash),
                          [this](paglets::Result<void> made) {
                              if (!made) return fail(pt::error_text("directory for " + params_.output_path, made.error()));
                              begin();
                          });
        });
        return Started{true, {}};
    }

    void begin() {
        (void)paglets::after_raw(500, "tick", {});
        // Which host this is (as mesh-info names hosts), then the first chunks.
        auto info = paglets::service("mesh-info");
        if (!info) return send_more();
        svc::mesh_info::Client client{*info};
        auto sent = client.snapshot({}, [this](paglets::Result<svc::mesh_info::Snapshot> snap, Message&) {
            if (snap) host_ = snap->host;
            send_more();
        });
        if (!sent) send_more();
    }

    void fail(std::string why) {
        state_ = "failed";
        error_ = std::move(why);
        paglets::log("pi: " + error_);
    }

    std::int64_t in_flight() const {
        return std::ranges::count_if(chunks_, [](const Chunk& c) { return c.sent && !c.done; });
    }

    // Sends chunks while fewer than max_in_flight are out, to the hosts
    // mesh-info selects (free slots, low load).
    void send_more() {
        if (asking_) return;
        const std::int64_t room = params_.max_in_flight - in_flight();
        const auto waiting = std::ranges::count_if(chunks_, [](const Chunk& c) { return !c.sent && !c.done; });
        if (room <= 0 || waiting == 0) return;
        auto info = paglets::service("mesh-info");
        if (!info) return send_to({""}, std::min<std::int64_t>(room, waiting));
        asking_ = true;
        svc::mesh_info::SelectRequest q;
        q.limit = std::min<std::int64_t>(room, waiting);
        q.max_load_per_cpu = 4.0;
        q.min_slots_free = 1;
        svc::mesh_info::Client client{*info};
        auto sent = client.select(q, [this, n = q.limit](paglets::Result<svc::mesh_info::Selection> r, Message&) {
            asking_ = false;
            std::vector<std::string> hosts;
            if (r) {
                for (const auto& h : r->hosts) hosts.push_back(h.host);
            }
            if (hosts.empty()) hosts.push_back("");  // this host
            send_to(hosts, n);
        });
        if (!sent) {
            asking_ = false;
            send_to({""}, std::min<std::int64_t>(room, waiting));
        }
    }

    // --8<-- [start:clone]
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
    // --8<-- [end:clone]

    void on_result(const Result& r) {
        for (auto& c : chunks_) {
            if (c.start != r.start || c.done || r.digits.size() != static_cast<std::size_t>(c.count)) continue;
            c.done = true;
            c.digits = r.digits;
            if (!r.host_name.empty()) hosts_.insert(r.host_name);
        }
        take_in_order();
        send_more();
    }

    // --8<-- [start:in_order]
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
    // --8<-- [end:in_order]

    // -- the decimal conversion --------------------------------------------------------------

    void start_conversion() {
        if (partial_n_ > 0) limbs_.push_back(partial_ << (4 * (8 - partial_n_)));
        partial_n_ = 0;
        // n hex digits fix n * log10(16) decimal digits; two fewer, so that a
        // run of nines or zeros at the end cannot hide a carry.
        decimal_total_ = std::max<std::int64_t>(0, static_cast<std::int64_t>(static_cast<double>(done_) * std::log10(16.0)) - 2);
        state_ = "converting";
        (void)paglets::after_raw(0, "convert", {});
    }

    // --8<-- [start:convert]
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
    // --8<-- [end:convert]

    // -- the files -----------------------------------------------------------------------------

    struct Sink {
        std::string path;
        std::string pending;  // not written yet
        bool busy = false;
        bool written = false;  // the first write replaces the file, later ones append
    };

    void flush(Sink& sink) {
        if (!dir_ || sink.busy || sink.pending.empty() || state_ == "failed") return;
        constexpr std::size_t block = 1024 * 1024;
        std::string part = sink.pending.substr(0, block);
        sink.pending.erase(0, part.size());
        sink.busy = true;
        Sink* target = &sink;
        pt::endpoint("files", [this, target, part = std::move(part)](paglets::Result<paglets::Endpoint> files) {
            if (!files) return fail(pt::error_text("files", files.error()));
            svc::files::Client client{*files};
            svc::files::WriteRequest q;
            q.path = target->path;
            q.data.assign(part.begin(), part.end());
            q.mode = target->written ? svc::files::WriteMode::append : svc::files::WriteMode::replace;
            auto sent = client.write(
                q,
                [this, target](paglets::Result<svc::files::Entry> w, Message&) {
                    target->busy = false;
                    if (!w) return fail(pt::error_text("write " + target->path, w.error()));
                    target->written = true;
                    flush(*target);
                    maybe_done();
                },
                pt::lending(*dir_));
            if (!sent) fail(pt::error_text("write " + target->path, sent.error()));
        });
    }

    void maybe_done() {
        if (state_ != "writing") return;
        for (const Sink* f : {&hex_file_, &dec_file_}) {
            if (f->busy || !f->pending.empty()) return;
        }
        state_ = "done";
        if (dir_) (void)dir_->drop();
        dir_.reset();
    }

    // --8<-- [start:resend]
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
    // --8<-- [end:resend]

    bool finished() const {
        return started_ && std::ranges::all_of(chunks_, [](const Chunk& c) { return c.done; });
    }

    Status status() const {
        Status s;
        s.state = state_;
        s.digits = params_.digits;
        s.done = done_;
        s.chunks = static_cast<std::int64_t>(chunks_.size());
        s.chunks_done = std::ranges::count_if(chunks_, [](const Chunk& c) { return c.done; });
        s.in_flight = in_flight();
        s.resent = resent_;
        s.finished = finished();
        s.hex = "3." + shown_hex_;
        s.decimal_digits = decimal_total_;
        s.decimal_done = decimal_done_;
        s.decimal = "3." + shown_dec_;
        s.shown = static_cast<std::int64_t>(shown_digits);
        for (const Sink* f : {&hex_file_, &dec_file_}) {
            if (!f->path.empty()) s.files.push_back(f->path);
        }
        s.hosts.assign(hosts_.begin(), hosts_.end());
        s.error = error_;
        return s;
    }

    // -- a worker ----------------------------------------------------------------------------

    // --8<-- [start:worker]
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
    // --8<-- [end:worker]

    void run(const std::string& lease) {
        lease_ = lease;
        if (task_.work_ms > 0) {
            (void)paglets::after_raw(task_.work_ms, "work-done", {});
        } else {
            compute();
        }
    }

    void compute() {
        digits_.clear();
        block_ = pi_bbp::Block(task_.start, static_cast<int>(std::min<std::int64_t>(6, task_.count)));
        compute_more();
    }

    // --8<-- [start:slices]
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
    // --8<-- [end:slices]

    void report_digits() {
        Result r;
        r.start = task_.start;
        r.count = task_.count;
        r.attempt = task_.attempt;
        r.digits = std::move(digits_);
        // Where it ran (for the job's report).
        if (auto mi = paglets::service("mesh-info")) {
            svc::mesh_info::Client client{*mi};
            auto sent = client.snapshot({}, [this, r](paglets::Result<svc::mesh_info::Snapshot> s, Message&) mutable {
                if (s) {
                    r.host = s->host;
                    r.host_name = s->host_name.empty() ? s->host.substr(0, 8) : s->host_name;
                }
                finish(r);
            });
            if (sent) return;
        }
        finish(r);
    }

    void finish(const Result& r) {
        (void)job_.send("result", r);
        if (!lease_.empty()) {
            if (auto slots = paglets::service("compute-slots")) {
                svc::compute_slots::Client client{*slots};
                (void)client.release_slot(
                    svc::compute_slots::ReleaseRequest{lease_},
                    [](paglets::Result<svc::compute_slots::ReleaseReply>, Message&) { (void)paglets::dispose(); });
                return;
            }
        }
        (void)paglets::dispose();
    }

    // The job.
    bool started_ = false;
    std::string state_ = "idle";
    std::string error_;
    std::size_t next_ = 0;   // the first chunk not yet taken in order
    std::int64_t done_ = 0;  // hex digits taken in order
    std::string shown_hex_;  // the first of them
    std::vector<std::uint32_t> limbs_;
    std::uint32_t partial_ = 0;
    int partial_n_ = 0;
    std::int64_t decimal_total_ = 0;
    std::int64_t decimal_done_ = 0;
    std::string shown_dec_;
    std::shared_ptr<paglets::Capability> dir_;
    Sink hex_file_;
    Sink dec_file_;
    bool asking_ = false;
    Start params_;
    std::vector<Chunk> chunks_;
    std::set<std::string> hosts_;
    std::int64_t resent_ = 0;
    std::string host_;
    // A worker.
    bool worker_ = false;
    Task task_;
    pi_bbp::Block block_;
    std::string digits_;
    paglets::Endpoint job_;
    std::string lease_;
};

}  // namespace

PAGLETS_PAGLET(Pi)
