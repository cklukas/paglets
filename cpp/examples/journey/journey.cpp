// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Mesh Journey (planning/cpp-demo-paglets.md, demo 1): the "hello world" of
// paglets/cpp. The paglet visits every host of the mesh in turn and keeps a
// journal of where it was in an ordinary std::vector; memory images carry
// it from host to host without serialization code. At the end it comes
// home; the journal shows every host that can receive and run paglets.

#include "journey.schema.gen.hpp"

#include <paglets/patterns/services.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace {

using namespace journey_msgs;
namespace svc = paglets::services;
namespace pt = paglets::patterns;
using paglets::Message;

class Journey : public paglets::Paglet {
public:
    Journey() {
        router().on<Start>("start", [this](const Start& s, Message& m) { start(s, m); });
        router().on("journal", [this](Message& m) { (void)m.reply(journal_); });
    }

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

private:
    static std::int64_t now_ms() { return paglets::now_ns(paglets::abi::Clock::wall) / 1'000'000; }

    void start(const Start& s, Message& m) {
        if (journal_.state == "travelling") {
            (void)m.reply(Started{false, "already travelling", itinerary_});
            return;
        }
        journal_ = Journal{"travelling", {}, {}};
        next_ = 0;
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
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
        if (!sent) {
            journal_.state = "failed";
            (void)paglets::reply_to(*reply, Started{false, pt::error_text("mesh-info", sent.error()), {}});
        }
    }

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

    void go_on() {
        if (next_ >= itinerary_.size() && here_ == home_) {
            journal_.state = "home";  // nothing (more) to visit
            return;
        }
        std::string destination = next_ < itinerary_.size() ? itinerary_[next_++] : home_;
        left_ms_ = now_ms();
        if (auto moved = paglets::dispatch(destination); !moved) {
            if (destination == home_) {
                journal_.state = "failed";
                journal_.skipped.push_back("home: " + pt::error_text("dispatch", moved.error()));
                return;
            }
            journal_.skipped.push_back(destination + ": " + pt::error_text("dispatch", moved.error()));
            go_on();
        }
    }

    Journal journal_{"idle", {}, {}};
    std::vector<std::string> itinerary_;
    std::size_t next_ = 0;
    std::string home_;
    std::string here_;
    std::int64_t left_ms_ = 0;
};

}  // namespace

PAGLETS_PAGLET(Journey)
