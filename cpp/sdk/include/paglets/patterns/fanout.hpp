// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Patterns (WP18): mesh fan-out. A paglet sends a clone of itself to each
// of a set of hosts and collects one report from each; clones that do not
// report in time count as failed.
//
// The parent:
//
//     patterns::FanOut fan;  // a member
//     fan.attach(router(), [this](const patterns::FanOut::Child& c) { ... });
//     select_hosts(q, [this](Result<std::vector<std::string>> hosts) {
//         fan.start(*hosts, encode(work), 10'000);
//     });
//
// A clone (in on_cloned, with Started::caps[0], the parent's endpoint):
//
//     patterns::report(parent, encode(result));  // or report_error(parent, "why")
//
// Clones with a destination can only carry endpoints (ABI v1.1).

#pragma once

#include <paglets/patterns/services.hpp>
#include <paglets/patterns/task.hpp>

#include <functional>
#include <string>
#include <vector>

namespace paglets::patterns {

struct FanOutReport {
    std::string clone;  // the clone's paglet ID
    std::string host;   // key ID of the host it ran on
    std::string host_name;
    bool ok = true;
    std::string error;
    Bytes result;
};

inline void paglets_encode(msgpack::Writer& w, const FanOutReport& r) {
    w.write_map_header(6);
    abi::detail::put(w, "clone", r.clone);
    abi::detail::put(w, "host", r.host);
    abi::detail::put(w, "host_name", r.host_name);
    abi::detail::put(w, "ok", r.ok);
    abi::detail::put(w, "error", r.error);
    abi::detail::put(w, "result", r.result);
}

inline bool paglets_decode(msgpack::Reader& r, FanOutReport& x) {
    return abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "clone") return msgpack::read_value(r, x.clone);
        if (k == "host") return msgpack::read_value(r, x.host);
        if (k == "host_name") return msgpack::read_value(r, x.host_name);
        if (k == "ok") return msgpack::read_value(r, x.ok);
        if (k == "error") return msgpack::read_value(r, x.error);
        if (k == "result") return msgpack::read_value(r, x.result);
        return r.skip();
    });
}

inline constexpr std::string_view fanout_report = "fanout.report";
inline constexpr std::string_view fanout_timeout = "fanout.timeout";

class FanOut {
public:
    struct Child {
        std::string destination;  // as asked ("" : this host)
        std::string clone;
        std::string host;
        std::string host_name;
        bool done = false;
        bool ok = false;
        std::string error;
        Bytes result;
    };

    // Handles reports and the deadline; `on_child` runs for each child that
    // finished (reported or timed out).
    void attach(Router& router, std::function<void(const Child&)> on_child) {
        on_child_ = std::move(on_child);
        router.on<FanOutReport>(std::string(fanout_report),
                                [this](const FanOutReport& r, Message&) { received(r); });
        router.on(std::string(fanout_timeout), [this](Message&) { expire(); });
    }

    // Clones this paglet to every destination (a host name or key ID; ""
    // for this host) with `args` and this paglet's endpoint. Returns how
    // many clones went out; the others count as failed at once.
    std::size_t start(const std::vector<std::string>& destinations, const Bytes& args, std::int64_t timeout_ms) {
        children_.clear();
        std::size_t sent = 0;
        for (const auto& d : destinations) {
            Child c;
            c.destination = d;
            auto clone = clone_raw(args, {self()}, d.empty() ? std::nullopt : std::optional<std::string>(d));
            if (!clone) {
                c.done = true;
                c.error = error_text("clone", clone.error());
            } else {
                (void)clone->drop();  // the clone reports through the parent's endpoint
                ++sent;
            }
            children_.push_back(std::move(c));
        }
        deadline_ = unix_ms() + timeout_ms;
        if (sent > 0) (void)after_raw(timeout_ms, fanout_timeout, {});
        for (const auto& c : children_) {
            if (c.done && on_child_) on_child_(c);
        }
        return sent;
    }

    // A move that failed for a clone (Paglet::on_move_failed with a clone ID):
    // that child is over.
    void clone_failed(const abi::MoveFailed& f) {
        for (auto& c : children_) {
            if (c.done || c.destination != f.destination) continue;
            c.done = true;
            c.clone = f.clone;
            c.error = "clone did not arrive: " + f.reason;
            if (on_child_) on_child_(c);
            return;
        }
    }

    bool finished() const {
        return std::ranges::all_of(children_, [](const Child& c) { return c.done; });
    }
    std::size_t succeeded() const {
        return static_cast<std::size_t>(std::ranges::count_if(children_, [](const Child& c) { return c.done && c.ok; }));
    }
    const std::vector<Child>& children() const { return children_; }

private:
    void received(const FanOutReport& r) {
        // The first child still waiting for this destination (or any, if
        // the clone landed elsewhere) takes the report.
        Child* match = nullptr;
        for (auto& c : children_) {
            if (c.done) continue;
            if (c.destination == r.host || c.destination == r.host_name || (c.destination.empty() && !match)) {
                match = &c;
                break;
            }
        }
        if (match == nullptr) {
            for (auto& c : children_) {
                if (!c.done) {
                    match = &c;
                    break;
                }
            }
        }
        if (match == nullptr) return;
        match->done = true;
        match->ok = r.ok;
        match->clone = r.clone;
        match->host = r.host;
        match->host_name = r.host_name;
        match->error = r.error;
        match->result = r.result;
        if (on_child_) on_child_(*match);
    }

    void expire() {
        if (unix_ms() < deadline_) return;
        for (auto& c : children_) {
            if (c.done) continue;
            c.done = true;
            c.error = "no report in time";
            if (on_child_) on_child_(c);
        }
    }

    std::vector<Child> children_;
    std::int64_t deadline_ = 0;
    std::function<void(const Child&)> on_child_;
};

// The clone's side: reports to the parent (with the host it ran on).
inline void report(const Endpoint& parent, Bytes result, std::string error = {}) {
    here([parent, result = std::move(result), error = std::move(error)](Result<services::mesh_info::Snapshot> s) {
        FanOutReport r;
        if (auto me = self_info()) r.clone = me->id;
        if (s) {
            r.host = s->host;
            r.host_name = s->host_name;
        }
        r.ok = error.empty();
        r.error = error;
        r.result = result;
        (void)parent.send(fanout_report, r);
    });
}

inline void report_error(const Endpoint& parent, std::string error) {
    report(parent, {}, std::move(error));
}

// Hosts mesh-info selects for work (their key IDs, the best first).
inline void select_hosts(services::mesh_info::SelectRequest q,
                         std::function<void(Result<std::vector<std::string>>)> next) {
    auto mi = service("mesh-info");
    if (!mi) return next(std::unexpected(mi.error()));
    services::mesh_info::Client client{*mi};
    auto sent = client.select(q, [next](Result<services::mesh_info::Selection> r, Message&) {
        if (!r) return next(std::unexpected(r.error()));
        std::vector<std::string> hosts;
        for (const auto& h : r->hosts) hosts.push_back(h.host);
        next(std::move(hosts));
    });
    if (!sent) next(std::unexpected(sent.error()));
}

}  // namespace paglets::patterns
