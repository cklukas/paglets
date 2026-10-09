// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Latency Map (planning/cpp-demo-paglets.md, demo 13): paglet-to-paglet
// messaging across the mesh. The paglet sends a probe (a clone) to every
// host; the probes tell it where they are, sending endpoints to themselves;
// it hands every probe the endpoints of the others; each probe pings the
// others and reports its row of round-trip times. The result is a host
// pair matrix.

#include "latency.schema.gen.hpp"

#include <paglets/patterns/fanout.hpp>

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace latency_msgs;
namespace svc = paglets::services;
namespace pt = paglets::patterns;
using paglets::Capability;
using paglets::Endpoint;
using paglets::Message;

double now_ms() {
    return static_cast<double>(paglets::now_ns(paglets::abi::Clock::monotonic)) / 1e6;
}

class Latency : public paglets::Paglet {
public:
    Latency() {
        // The parent.
        router().on<Probe>("start", [this](const Probe& p, Message& m) { start(p, m); });
        router().on("map", [this](Message& m) { (void)m.reply(map_); });
        router().on<Ready>("probe.ready", [this](const Ready& r, Message& m) { ready(r, m); });
        fan_.attach(router(), [this](const pt::FanOut::Child& c) {
            Row row;
            if (c.ok && paglets::decode(c.result, row)) {
                map_.rows.push_back(std::move(row));
            } else {
                map_.errors.push_back((c.host_name.empty() ? c.destination : c.host_name) + ": " + c.error);
            }
            if (fan_.finished()) finish();
        });
        // A probe.
        router().on("ping", [](Message& m) { (void)m.reply(); });
        router().on<Peers>("probe.peers", [this](const Peers& p, Message& m) { measure(p, m); });
        router().on("probe.done", [](Message&) { (void)paglets::dispose(); });
    }

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

    void on_move_failed(const paglets::abi::MoveFailed& f) override {
        if (!f.clone.empty()) fan_.clone_failed(f);
    }

private:
    // -- the parent -------------------------------------------------------------------

    void start(const Probe& p, Message& m) {
        map_ = LatencyMap{};
        probes_.clear();
        auto reply = std::make_shared<Capability>(m.defer_reply());
        auto go = [this, p, reply](std::vector<std::string> hosts) {
            const auto sent = fan_.start(hosts, paglets::encode(std::max<std::int64_t>(1, p.rounds)), p.timeout_ms);
            expected_ = sent;
            (void)paglets::reply_to(*reply,
                                    Started{sent > 0, sent > 0 ? "" : "no probe went out", static_cast<std::int64_t>(sent)});
        };
        if (!p.hosts.empty()) return go(p.hosts);
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

    void finish() {
        map_.finished = true;
        for (const auto& p : probes_) (void)p.endpoint.send("probe.done");
        std::ranges::sort(map_.rows, [](const Row& a, const Row& b) { return a.from < b.from; });
    }

    // -- a probe ----------------------------------------------------------------------

    void measure(const Peers& p, Message& m) {
        targets_.clear();
        for (std::size_t i = 0; i < p.names.size() && i < m.cap_count(); ++i) {
            targets_.push_back(Target{p.names[i], Endpoint(m.take_cap(i)), Cell{p.names[i], 0, 0, 0, {}}});
        }
        row_ = Row{name_, {}};
        ping(0, 0);
    }

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

    struct Probe_ {
        std::string name;
        Endpoint endpoint;
    };
    struct Target {
        std::string name;
        Endpoint endpoint;
        Cell cell;
    };

    // The parent.
    pt::FanOut fan_;
    LatencyMap map_;
    std::vector<Probe_> probes_;
    std::size_t expected_ = 0;
    // A probe.
    Endpoint parent_;
    std::string name_;
    std::int64_t rounds_ = 1;
    std::vector<Target> targets_;
    Row row_;
};

}  // namespace

PAGLETS_PAGLET(Latency)
