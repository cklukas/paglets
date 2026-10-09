// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Mesh Benchmark (planning/cpp-demo-paglets.md, demo 12): which machines
// are fast for which kind of work. The paglet sends a clone to every host.
// Each clone asks the host's compute-slots for a slot (one benchmark at a
// time per host, and no other compute job beside it), then measures
// integer and floating-point speed and memory copying in its own memory,
// and writing and reading through its own storage (the host keeps it in
// the paglet's scratch directory). The parent measures the rest of the
// round trip and ranks the hosts.

#include "benchmark.schema.gen.hpp"

#include <paglets/patterns/fanout.hpp>
#include <paglets/patterns/services.hpp>
#include <paglets/services/compute_slots.gen.hpp>
#include <paglets/services/storage.gen.hpp>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace benchmark_msgs;
namespace svc = paglets::services;
namespace pt = paglets::patterns;
using paglets::Message;

constexpr std::size_t mb = 1024 * 1024;
constexpr std::size_t block = 256 * 1024;  // a message holds at most 1 MB

std::int64_t mono_ns() {
    return paglets::now_ns(paglets::abi::Clock::monotonic);
}

double seconds_since(std::int64_t start_ns) {
    return static_cast<double>(mono_ns() - start_ns) / 1e9;
}

class Benchmark : public paglets::Paglet {
public:
    Benchmark() {
        router().on<Bench>("start", [this](const Bench& b, Message& m) { start(b, m); });
        router().on("ranking", [this](Message& m) { (void)m.reply(ranking_); });
        router().on<svc::compute_slots::Decision>("compute.granted",
                                                  [this](const svc::compute_slots::Decision& d, Message&) { run(d.lease); });
        // Redirected while waiting: this host is the one to measure, so it runs anyway.
        router().on<svc::compute_slots::Decision>("compute.redirect",
                                                  [this](const svc::compute_slots::Decision&, Message&) { run({}); });
        // One test per handler: a handler has a time budget on every host.
        router().on("bench.step", [this](Message&) { step(); });
        fan_.attach(router(), [this](const pt::FanOut::Child& c) {
            HostResult r;
            if (!c.ok || !paglets::decode(c.result, r)) {
                ranking_.errors.push_back((c.host_name.empty() ? c.destination : c.host_name) + ": " +
                                          (c.error.empty() ? "no report" : c.error));
            } else {
                r.travel_ms = std::max<std::int64_t>(0, (mono_ns() - started_ns_) / 1'000'000 - r.bench_ms);
                ranking_.hosts.push_back(std::move(r));
            }
            if (fan_.finished() && ranking_.state == "running") {
                std::ranges::stable_sort(ranking_.hosts, [](const HostResult& a, const HostResult& b) { return a.int_mops > b.int_mops; });
                ranking_.state = "done";
            }
        });
    }

    void on_cloned(paglets::Started& s) override {
        if (s.caps.empty() || !s.decode_args(bench_)) {
            (void)paglets::dispose();
            return;
        }
        parent_ = paglets::Endpoint(s.caps[0].release());
        ask_for_slot();
    }

    void on_move_failed(const paglets::abi::MoveFailed& f) override {
        if (!f.clone.empty()) fan_.clone_failed(f);
    }

private:
    // -- the parent -----------------------------------------------------------------------

    void start(const Bench& b, Message& m) {
        if (ranking_.state == "running") return (void)m.reply(Started{false, "already running", 0});
        bench_ = b;
        ranking_ = Ranking{"running"};
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        auto go = [this, reply](std::vector<std::string> hosts) {
            started_ns_ = mono_ns();
            const auto sent = fan_.start(hosts, paglets::encode(bench_), bench_.timeout_ms);
            (void)paglets::reply_to(*reply, Started{sent > 0, sent > 0 ? "" : "no clone went out",
                                                    static_cast<std::int64_t>(hosts.size())});
        };
        if (!b.hosts.empty()) return go(b.hosts);
        auto mi = paglets::service("mesh-info");
        if (!mi) return go({""});
        svc::mesh_info::Client client{*mi};
        auto sent = client.landscape({}, [go](paglets::Result<svc::mesh_info::Landscape> l, Message&) {
            std::vector<std::string> hosts{""};
            if (l) {
                for (std::size_t i = 1; i < l->hosts.size(); ++i) hosts.push_back(l->hosts[i].host);
            }
            go(std::move(hosts));
        });
        if (!sent) go({""});
    }

    // -- a clone: a slot, then the measurements ------------------------------------------------

    void ask_for_slot() {
        auto slots = paglets::service("compute-slots");
        if (!slots) return run({});
        svc::compute_slots::Client client{*slots};
        svc::compute_slots::SlotRequest q;
        q.job = "benchmark";
        q.cores = 1;
        q.estimated_ms = 3 * bench_.budget_ms;
        auto sent = client.request_slot(q, [this](paglets::Result<svc::compute_slots::Decision> d, Message&) {
            if (!d) return run({});
            if (d->verdict == svc::compute_slots::Verdict::run_now) return run(d->lease);
            if (d->verdict != svc::compute_slots::Verdict::queued) run({});  // compute.granted comes later
        });
        if (!sent) run({});
    }

    void run(const std::string& lease) {
        if (running_) return;
        running_ = true;
        lease_ = lease;
        result_.slot = !lease.empty();
        bench_start_ns_ = mono_ns();
        next_step();
    }

    void next_step() {
        if (!paglets::after_raw(0, "bench.step", {})) step();
    }

    void step() {
        switch (step_++) {
            case 0: cpu_int(); return next_step();
            case 1: cpu_float(); return next_step();
            case 2: memory(); return next_step();
            default: break;
        }
        pt::here([this](paglets::Result<svc::mesh_info::Snapshot> s) {
            if (s) {
                result_.host_name = s->host_name;
                result_.os = s->os;
                result_.architecture = s->architecture;
                result_.cpus = s->cpus;
            }
            disk();
        });
    }

    void cpu_int() {
        const std::int64_t budget = bench_.budget_ms * 1'000'000;
        std::uint64_t x = 88172645463325252ull, acc = 0, iterations = 0;
        const auto t = mono_ns();
        while (mono_ns() - t < budget) {
            for (int i = 0; i < 10'000; ++i) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                acc += x & 0xff;
            }
            iterations += 10'000;
        }
        result_.int_mops = static_cast<double>(iterations) * 5 / seconds_since(t) / 1e6;  // 3 xor-shifts, an and, an add
        result_.check ^= acc;
    }

    void cpu_float() {
        const std::int64_t budget = bench_.budget_ms * 1'000'000;
        double zr = 0, zi = 0, sum = 0;
        std::uint64_t iterations = 0;
        const auto t = mono_ns();
        while (mono_ns() - t < budget) {
            for (int i = 0; i < 10'000; ++i) {
                const double nr = zr * zr - zi * zi - 0.4;  // z = z^2 + c, a bounded orbit
                zi = 2 * zr * zi + 0.3;
                zr = nr;
                sum += zr;
            }
            iterations += 10'000;
        }
        result_.float_mflops = static_cast<double>(iterations) * 8 / seconds_since(t) / 1e6;
        result_.check ^= static_cast<std::uint64_t>(sum);
    }

    void memory() {
        const std::size_t size = static_cast<std::size_t>(std::max<std::int64_t>(1, bench_.memory_mb)) * mb;
        std::vector<std::uint8_t> a(size, 1), b(size, 2);
        const std::int64_t budget = bench_.budget_ms * 1'000'000;
        std::uint64_t copied = 0;
        const auto t = mono_ns();
        while (mono_ns() - t < budget) {
            std::memcpy(b.data(), a.data(), size);
            a[copied % size] ^= 1;
            copied += size;
        }
        result_.memory_mbs = static_cast<double>(copied) / static_cast<double>(mb) / seconds_since(t);
        result_.check ^= b[size / 2];
    }

    // Writes and reads `disk_mb` MB in blocks of 256 KB through the paglet's storage.
    void disk() {
        pt::endpoint("storage", [this](paglets::Result<paglets::Endpoint> st) {
            if (!st) {
                result_.error = pt::error_text("storage", st.error());
                return finish();
            }
            storage_ = *st;
            block_.assign(block, 0x5a);
            disk_start_ns_ = mono_ns();
            write_block(0);
        });
    }

    std::int64_t blocks() const { return std::max<std::int64_t>(1, bench_.disk_mb) * static_cast<std::int64_t>(mb / block); }

    void write_block(std::int64_t i) {
        if (i >= blocks()) {
            result_.disk_write_mbs = static_cast<double>(blocks() * block) / static_cast<double>(mb) / seconds_since(disk_start_ns_);
            disk_start_ns_ = mono_ns();
            return read_block(0);
        }
        svc::storage::Client client{storage_};
        block_[0] = static_cast<std::uint8_t>(i);
        auto sent = client.put(svc::storage::PutRequest{"bench-" + std::to_string(i), block_},
                               [this, i](paglets::Result<svc::storage::Usage> r, Message&) {
                                   if (!r) {
                                       result_.error = pt::error_text("storage put", r.error());
                                       return clean_up(0);
                                   }
                                   write_block(i + 1);
                               });
        if (!sent) {
            result_.error = pt::error_text("storage put", sent.error());
            clean_up(0);
        }
    }

    void read_block(std::int64_t i) {
        if (i >= blocks()) {
            result_.disk_read_mbs = static_cast<double>(blocks() * block) / static_cast<double>(mb) / seconds_since(disk_start_ns_);
            return clean_up(0);
        }
        svc::storage::Client client{storage_};
        auto sent = client.get(svc::storage::GetRequest{"bench-" + std::to_string(i)},
                               [this, i](paglets::Result<svc::storage::GetReply> r, Message&) {
                                   if (!r) {
                                       result_.error = pt::error_text("storage get", r.error());
                                       return clean_up(0);
                                   }
                                   read_block(i + 1);
                               });
        if (!sent) {
            result_.error = pt::error_text("storage get", sent.error());
            clean_up(0);
        }
    }

    void clean_up(std::int64_t i) {
        if (i >= blocks()) return finish();
        svc::storage::Client client{storage_};
        auto sent = client.remove(svc::storage::DeleteRequest{"bench-" + std::to_string(i)},
                                  [this, i](paglets::Result<svc::storage::DeleteReply>, Message&) { clean_up(i + 1); });
        if (!sent) finish();
    }

    void finish() {
        block_.clear();
        result_.bench_ms = (mono_ns() - bench_start_ns_) / 1'000'000;
        // The slot is free again; the clone ends once its report is out.
        auto slots = paglets::service("compute-slots");
        if (!lease_.empty() && slots) {
            svc::compute_slots::Client client{*slots};
            (void)client.release_slot(svc::compute_slots::ReleaseRequest{lease_},
                                      [](paglets::Result<svc::compute_slots::ReleaseReply>, Message&) {});
        }
        pt::report(parent_, paglets::encode(result_), {}, [] { (void)paglets::dispose(); });
    }

    // Everywhere.
    Bench bench_;
    // The parent.
    pt::FanOut fan_;
    Ranking ranking_{"idle"};
    std::int64_t started_ns_ = 0;
    // A clone.
    paglets::Endpoint parent_;
    bool running_ = false;
    int step_ = 0;
    std::string lease_;
    HostResult result_;
    std::int64_t bench_start_ns_ = 0;
    std::int64_t disk_start_ns_ = 0;
    paglets::Endpoint storage_;
    std::vector<std::uint8_t> block_;
};

}  // namespace

PAGLETS_PAGLET(Benchmark)
