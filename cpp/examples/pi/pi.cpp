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

#include "pi.schema.gen.hpp"

#include "bbp.hpp"

#include <paglets/paglet.hpp>
#include <paglets/services/compute_slots.gen.hpp>
#include <paglets/services/mesh_info.gen.hpp>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace {

using namespace pi_msgs;
namespace svc = paglets::services;
namespace abi = paglets::abi;
using paglets::Message;

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
        started_ = true;
        params_ = s;
        for (std::int64_t at = 0; at < s.digits; at += s.chunk) {
            chunks_.push_back(Chunk{at, std::min(s.chunk, s.digits - at)});
        }
        (void)paglets::after_raw(500, "tick", {});
        // Which host this is (as mesh-info names hosts), then the first chunks.
        auto info = paglets::service("mesh-info");
        if (!info) {
            send_more();
            return Started{true, {}};
        }
        svc::mesh_info::Client client{*info};
        auto sent = client.snapshot({}, [this](paglets::Result<svc::mesh_info::Snapshot> snap, Message&) {
            if (snap) host_ = snap->host;
            send_more();
        });
        if (!sent) send_more();
        return Started{true, {}};
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

    void on_result(const Result& r) {
        for (auto& c : chunks_) {
            if (c.start != r.start || c.done || r.digits.size() != static_cast<std::size_t>(c.count)) continue;
            c.done = true;
            c.digits = r.digits;
            if (!r.host_name.empty()) hosts_.insert(r.host_name);
        }
        send_more();
    }

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

    bool finished() const {
        return started_ && std::ranges::all_of(chunks_, [](const Chunk& c) { return c.done; });
    }

    Status status() const {
        Status s;
        s.digits = params_.digits;
        s.chunks = static_cast<std::int64_t>(chunks_.size());
        s.in_flight = in_flight();
        s.resent = resent_;
        s.finished = finished();
        s.hex = "3.";
        bool contiguous = true;
        for (const auto& c : chunks_) {
            if (c.done) ++s.chunks_done;
            if (!c.done) contiguous = false;
            if (contiguous) {
                s.hex += c.digits;
                s.done += c.count;
            }
        }
        s.hosts.assign(hosts_.begin(), hosts_.end());
        return s;
    }

    // -- a worker ----------------------------------------------------------------------------

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

    void run(const std::string& lease) {
        lease_ = lease;
        if (task_.work_ms > 0) {
            (void)paglets::after_raw(task_.work_ms, "work-done", {});
        } else {
            compute();
        }
    }

    void compute() {
        Result r;
        r.start = task_.start;
        r.count = task_.count;
        r.attempt = task_.attempt;
        r.digits = pi_bbp::hex_digits(task_.start, task_.count);
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
    bool asking_ = false;
    Start params_;
    std::vector<Chunk> chunks_;
    std::set<std::string> hosts_;
    std::int64_t resent_ = 0;
    std::string host_;
    // A worker.
    bool worker_ = false;
    Task task_;
    paglets::Endpoint job_;
    std::string lease_;
};

}  // namespace

PAGLETS_PAGLET(Pi)
