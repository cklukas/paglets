// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Live conformance of the zombie protocol: the protocol steps that replicas
// recorded on their hosts (`zombies` system paglet, trace records) are
// checked against the specification (zombie.hpp):
//
// - every role change is one of allowed_transitions;
// - a replica's rank never falls, except when it restarts (it comes back
//   with its durable checkpoint);
// - an ended replica records nothing more;
// - at most one sink accepted a result, one more for every redirect.

#pragma once

#include <paglets/protocols/zombie/zombie.hpp>
#include <paglets/services/zombies.hpp>

#include <algorithm>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace paglets::protocols::zombie {

struct TraceReport {
    std::size_t records = 0;
    std::size_t replicas = 0;     // distinct replica paglets
    std::size_t hosts = 0;
    std::size_t takeovers = 0;    // role changes to main or delivering
    std::size_t restarts = 0;
    std::size_t accepted = 0;     // results accepted by a sink
    std::vector<std::string> violations;
    bool ok() const { return violations.empty(); }
};

template <class E, std::size_t N>
std::optional<E> enum_named(std::string_view name) {
    for (std::size_t i = 0; i < N; ++i) {
        if (to_string(static_cast<E>(i)) == name) return static_cast<E>(i);
    }
    return std::nullopt;
}

// `sink`: the group's current sink number (1 + redirects).
inline TraceReport check_traces(std::vector<services::zombies::TraceRecord> records, std::int64_t sink) {
    TraceReport rep;
    rep.records = records.size();
    std::set<std::string> hosts;
    std::ranges::stable_sort(records, [](const auto& a, const auto& b) {
        return std::tie(a.paglet, a.incarnation, a.seq) < std::tie(b.paglet, b.incarnation, b.seq);
    });
    std::string last_paglet;
    std::optional<Rank> rank;
    bool ended = false;
    std::uint32_t incarnation = 0;
    auto where = [](const services::zombies::TraceRecord& r) {
        return "replica " + std::to_string(r.replica) + " (" + r.paglet.substr(0, 8) + ", incarnation " +
               std::to_string(r.incarnation) + ", step " + std::to_string(r.seq) + ")";
    };
    for (const auto& r : records) {
        hosts.insert(r.host);
        if (r.paglet != last_paglet) {
            last_paglet = r.paglet;
            ++rep.replicas;
            rank.reset();
            ended = false;
            incarnation = r.incarnation;
        }
        if (ended) rep.violations.push_back(where(r) + ": a step after the replica ended");
        if (r.event == "accepted") {
            ++rep.accepted;
            continue;
        }
        if (r.event != "transition") continue;
        const auto from = enum_named<Role, 6>(r.from);
        const auto to = enum_named<Role, 6>(r.to);
        const auto cause = enum_named<Cause, 13>(r.cause);
        if (!from || !to || !cause || !is_allowed(*from, *cause, *to)) {
            rep.violations.push_back(where(r) + ": role change " + r.from + " --" + r.cause + "--> " + r.to +
                                     " is not in the specification");
            continue;
        }
        const Rank now{r.done, r.progress, r.term, r.ckpt, r.author};
        if (*cause == Cause::recovered || r.incarnation != incarnation) {
            ++rep.restarts;
            incarnation = r.incarnation;
        } else if (rank && now < *rank) {
            rep.violations.push_back(where(r) + ": its rank fell");
        }
        rank = now;
        if (*to == Role::main || *to == Role::delivering) ++rep.takeovers;
        if (*to == Role::ended) ended = true;
    }
    rep.hosts = hosts.size();
    if (static_cast<std::int64_t>(rep.accepted) > std::max<std::int64_t>(sink, 1)) {
        rep.violations.push_back(std::to_string(rep.accepted) + " results accepted, at most " +
                                 std::to_string(std::max<std::int64_t>(sink, 1)) + " allowed");
    }
    return rep;
}

}  // namespace paglets::protocols::zombie
