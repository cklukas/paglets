// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The zombie protocol (docs/zombie-protocol.md): the transition function
// and its encodings.

#include "test.hpp"

#include <paglets/protocols/zombie/zombie.hpp>

#include <algorithm>
#include <variant>

namespace z = paglets::protocols::zombie;

namespace {

template <class T>
std::vector<T> all(const z::Outputs& out) {
    std::vector<T> v;
    for (const auto& o : out) {
        if (const auto* t = std::get_if<T>(&o)) v.push_back(*t);
    }
    return v;
}

template <class T>
std::ptrdiff_t first(const z::Outputs& out) {
    for (std::size_t i = 0; i < out.size(); ++i) {
        if (std::holds_alternative<T>(out[i])) return static_cast<std::ptrdiff_t>(i);
    }
    return -1;
}

// Delivers every Send of `out` from `from` to the replicas in `group`.
void route(std::vector<z::State>& group, z::ReplicaId from, const z::Outputs& out, int depth = 0) {
    if (depth > 8) return;
    for (const auto& o : out) {
        if (const auto* s = std::get_if<z::Send>(&o)) {
            auto more = z::step(group[s->to], z::Received{from, s->message});
            route(group, s->to, more, depth + 1);
        }
    }
}

}  // namespace

PAGLETS_TEST("zombie: ranks order finished work first, then progress, reign, checkpoint, lower author") {
    using R = z::Rank;
    CHECK((R{true, 1, 1, 0, 0} > R{false, 99, 9, 9, 0}));
    CHECK((R{false, 5, 1, 0, 0} > R{false, 4, 9, 9, 0}));
    CHECK((R{false, 5, 2, 0, 1} > R{false, 5, 1, 7, 0}));
    CHECK((R{false, 5, 2, 3, 1} > R{false, 5, 2, 2, 0}));
    CHECK((R{false, 5, 2, 3, 0} > R{false, 5, 2, 3, 1}));
    CHECK((R{false, 5, 2, 3, 1} == R{false, 5, 2, 3, 1}));
}

PAGLETS_TEST("zombie: a new main is durable before its first beat") {
    auto s = z::initial(0, 3);
    const auto out = z::step(s, z::Start{true, {1, 2, 3}});
    CHECK(s.role == z::Role::main);
    CHECK_EQ(s.reign, 1u);
    REQUIRE(first<z::Persist>(out) >= 0);
    CHECK(first<z::Persist>(out) < first<z::Send>(out));
    CHECK_EQ(all<z::Send>(out).size(), 2u);  // a beat to each other replica
    CHECK_EQ(all<z::StartWork>(out).size(), 1u);
    const auto t = all<z::Transition>(out);
    REQUIRE(t.size() == 1);
    CHECK(z::is_allowed(t[0].from, t[0].cause, t[0].to));
}

PAGLETS_TEST("zombie: replicas take the main's checkpoint and journal, and hand over their own entries") {
    std::vector<z::State> g = {z::initial(0, 2), z::initial(1, 2)};
    route(g, 0, z::step(g[0], z::Start{true, {7}}));
    z::step(g[1], z::Start{false, {}});
    // The main works, observes and checkpoints.
    z::step(g[0], z::Worked{3, false, {9}});
    z::step(g[0], z::Observed{{'a'}});
    route(g, 0, z::step(g[0], z::Fired{z::Timer::checkpoint}));
    CHECK(g[1].role == z::Role::sleeping);
    CHECK(g[1].rank == g[0].rank);
    CHECK(g[1].work == z::Bytes{9});
    CHECK(g[1].journal == g[0].journal);
    // A replica holding an entry the main lacks hands it over on the next beat.
    g[1].journal.entries.emplace(z::EntryKey{1, 0, 1}, z::Bytes{'b'});
    route(g, 0, z::step(g[0], z::Fired{z::Timer::beat}));
    CHECK(g[0].journal.entries.contains(z::EntryKey{1, 0, 1}));
    CHECK(g[0].dirty);
}

PAGLETS_TEST("zombie: a replica that hears nobody becomes main with its checkpoint") {
    auto s = z::initial(2, 3);
    z::step(s, z::Start{false, {}});
    s.rank = z::Rank{false, 4, 1, 2, 0};
    s.work = {4};
    auto out = z::step(s, z::Fired{z::Timer::suspect});
    CHECK(s.role == z::Role::candidate);
    CHECK_EQ(all<z::Send>(out).size(), 2u);  // StatusReq to both others
    out = z::step(s, z::Fired{z::Timer::round});
    CHECK(s.role == z::Role::main);
    CHECK_EQ(s.rank.progress, 4);
    CHECK_EQ(s.rank.author, 2u);
    CHECK(s.rank.term > 1);
    const auto work = all<z::StartWork>(out);
    REQUIRE(work.size() == 1);
    CHECK(work[0].work == z::Bytes{4});
}

PAGLETS_TEST("zombie: the main behind steps down when two mains meet") {
    std::vector<z::State> g = {z::initial(0, 2), z::initial(1, 2)};
    z::step(g[0], z::Start{true, {}});
    // Replica 1 ran alone during a partition and got further.
    z::step(g[1], z::Start{false, {}});
    g[1].rank = z::Rank{false, 9, 1, 0, 0};
    z::step(g[1], z::Fired{z::Timer::suspect});
    z::step(g[1], z::Fired{z::Timer::round});
    REQUIRE(g[1].role == z::Role::main);
    // Replica 0's beat reaches replica 1: replica 1 is ahead and pauses;
    // its answer makes replica 0 step down.
    auto out = z::step(g[0], z::Fired{z::Timer::beat});
    route(g, 0, out);
    CHECK(g[0].role == z::Role::sleeping);
    CHECK_EQ(g[0].leader, 1u);
    CHECK(g[1].role == z::Role::main);
    CHECK_EQ(g[0].rank.progress, 9);  // took the winner's checkpoint
}

PAGLETS_TEST("zombie: the sink's answer ends the group, a tombstone ends the others") {
    auto s = z::initial(0, 2);
    z::step(s, z::Start{true, {}});
    auto out = z::step(s, z::Worked{1, true, {1}});
    CHECK(s.role == z::Role::delivering);
    CHECK_EQ(all<z::Deliver>(out).size(), 1u);
    out = z::step(s, z::SinkAnswer{1, false});
    CHECK(s.role == z::Role::ended);
    CHECK_EQ(all<z::Tombstone>(out).size(), 1u);
    CHECK_EQ(all<z::End>(out).size(), 1u);
    CHECK(z::step(s, z::Fired{z::Timer::beat}).empty());

    auto r = z::initial(1, 2);
    z::step(r, z::Start{false, {}});
    out = z::step(r, z::RegistryUpdate{true, 1});
    CHECK(r.role == z::Role::ended);
    CHECK_EQ(all<z::End>(out).size(), 1u);
}

PAGLETS_TEST("zombie: a restart raises the incarnation durably and listens first") {
    auto s = z::initial(0, 2);
    z::step(s, z::Start{true, {}});
    z::step(s, z::Observed{{'x'}});
    const auto out = z::step(s, z::Recovered{});
    CHECK(s.role == z::Role::recovering);
    CHECK_EQ(s.incarnation, 1u);
    CHECK_EQ(s.next_entry, 1u);
    CHECK(first<z::Persist>(out) >= 0);
    CHECK(s.armed(z::Timer::suspect));
    CHECK(!s.armed(z::Timer::beat));
}

PAGLETS_TEST("zombie: states and messages survive their encoding") {
    std::vector<z::State> g = {z::initial(0, 3), z::initial(1, 3), z::initial(2, 3)};
    route(g, 0, z::step(g[0], z::Start{true, {1, 2}}));
    z::step(g[0], z::Observed{{'o'}});
    z::step(g[2], z::Fired{z::Timer::suspect});
    for (const auto& s : g) {
        paglets::msgpack::Writer w;
        z::paglets_encode(w, s);
        paglets::msgpack::Reader r(w.bytes());
        z::State back;
        REQUIRE(z::paglets_decode(r, back));
        CHECK(back == s);
    }
    const std::vector<z::Message> messages = {
        z::Beat{3, z::Rank{false, 2, 3, 1, 0}, {{0, 0, 2}}},
        z::Ack{3, z::Rank{}, {}},
        z::CheckpointMsg{3, z::Rank{true, 1, 3, 4, 2}, z::Bytes{1, 2}, {z::Entry{{0, 1, 1}, {'e'}}}},
        z::Entries{0, {z::Entry{{1, 0, 2}, {}}}},
        z::StatusReq{5},
        z::Status{5, z::Role::candidate, z::Rank{false, 1, 1, 1, 1}},
        z::Takeover{6},
    };
    for (const auto& m : messages) {
        paglets::msgpack::Writer w;
        z::paglets_encode(w, m);
        paglets::msgpack::Reader r(w.bytes());
        z::Message back;
        REQUIRE(z::paglets_decode(r, back));
        CHECK(back == m);
    }
}
