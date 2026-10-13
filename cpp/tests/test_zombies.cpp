// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `zombies` system paglet (docs/zombie-protocol.md): one replica per
// group and host, tombstones and redirects that reach every host, traces
// collected from every host and checked against the specification.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <paglets/protocols/zombie/trace.hpp>
#include <paglets/services/zombies.hpp>
#include <paglets/wire/reflect.hpp>

using namespace paglets::test;
namespace zb = paglets::services::zombies;
namespace wire = paglets::wire;

namespace {

rt::PagletId start(Mesh& net, Mesh::Host& host, const std::string& module, char id) {
    auto created = host.node->create(module, net.passport(module, paglet_id(id)));
    REQUIRE_OK(created);
    return *created;
}

std::int32_t service_handle(Fixture& f, const rt::PagletId& paglet, std::string_view name) {
    auto info = dec<abi::SelfInfo>(f.call(paglet, "info").payload);
    for (const auto& [n, h] : info.services) {
        if (n == name) return h;
    }
    return 0;
}

template <class Reply, class Request>
Reply ask(Mesh& net, Fixture& f, const rt::PagletId& paglet, const std::string& op, const Request& q) {
    const auto before = f.journal(paglet).size();
    const auto h = service_handle(f, paglet, "zombies");
    REQUIRE(h > 0);
    REQUIRE(f.code(paglet, "request", Cmd{.handle = h, .name = op, .payload = wire::to_msgpack(q)}) > 0);
    std::string entry;
    REQUIRE(net.settle(
        [&] {
            const auto j = f.journal(paglet);
            for (std::size_t i = before; i < j.size(); ++i) {
                if (j[i].starts_with("reply:")) {
                    entry = j[i];
                    return true;
                }
            }
            return false;
        },
        400));
    REQUIRE(entry.starts_with("reply:ok:"));
    const std::string payload = entry.substr(9);
    Reply r{};
    REQUIRE(wire::from_msgpack(Bytes(payload.begin(), payload.end()), r));
    return r;
}

std::optional<zb::GroupInfo> group_on(Mesh::Host& h, const std::string& group) {
    for (auto& g : h.node->zombie_groups({})) {
        if (g.state.group == group) return g;
    }
    return std::nullopt;
}

zb::TraceRecord step(std::uint32_t replica, std::uint64_t seq, std::string from, std::string cause, std::string to,
                     std::int64_t progress) {
    zb::TraceRecord r;
    r.time_ms = 1000 + static_cast<std::int64_t>(seq);
    r.replica = replica;
    r.seq = seq;
    r.event = "transition";
    r.from = std::move(from);
    r.cause = std::move(cause);
    r.to = std::move(to);
    r.progress = progress;
    r.term = 1;
    return r;
}

const std::string group1 = "0123456789abcdef0123456789abcdef";
const std::string group2 = "fedcba9876543210fedcba9876543210";

}  // namespace

PAGLETS_TEST("zombies: one replica per host; a tombstone reaches every host and ends the group there") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    net.enroll_all();
    for (auto& h : net.hosts) h->node->set_zombie_gossip(50ms);
    const std::string module = a.f->module("conformance.wasm");
    (void)b.f->module("conformance.wasm");
    (void)c.f->module("conformance.wasm");
    const auto p1 = start(net, a, module, '1');
    const auto p2 = start(net, a, module, '2');
    const auto p3 = start(net, b, module, '3');

    auto j1 = ask<zb::JoinReply>(net, *a.f, p1, "join", zb::JoinRequest{group1, "host:b"});
    CHECK(j1.accepted);
    CHECK_EQ(j1.state.sink, 1);
    CHECK_EQ(j1.state.sink_spec, std::string("host:b"));
    // A second replica of the group on the same host is refused.
    auto j2 = ask<zb::JoinReply>(net, *a.f, p2, "join", zb::JoinRequest{group1, ""});
    CHECK(!j2.accepted);
    CHECK_EQ(j2.reason, std::string("another replica of the group runs on this host"));
    CHECK(ask<zb::JoinReply>(net, *b.f, p3, "join", zb::JoinRequest{group1, "host:b"}).accepted);

    // Replicas record their steps; the collection from every host conforms.
    ask<zb::Done>(net, *a.f, p1, "trace",
                  zb::TraceRequest{group1, {step(0, 0, "sleeping", "created", "main", 0),
                                            step(0, 1, "main", "done", "delivering", 5)}});
    ask<zb::Done>(net, *b.f, p3, "trace",
                  zb::TraceRequest{group1, {step(1, 0, "sleeping", "suspect", "candidate", 0)}});
    std::vector<zb::TraceRecord> records;
    std::thread collect([&] { records = a.node->zombie_traces(group1); });
    REQUIRE(net.settle([&] { return !records.empty(); }, 2000));
    collect.join();
    CHECK_EQ(records.size(), 3u);
    auto report = paglets::protocols::zombie::check_traces(records, 1);
    CHECK(report.ok());
    CHECK_EQ(report.hosts, 2u);
    CHECK_EQ(report.replicas, 2u);
    // A step outside the specification is found.
    records.push_back(step(1, 1, "sleeping", "done", "delivering", 1));
    CHECK(!paglets::protocols::zombie::check_traces(records, 1).ok());

    // Results go to the sink (host b), which accepts the first one only.
    auto d1 = ask<zb::DeliverReply>(net, *a.f, p1, "deliver", zb::DeliverRequest{group1, "log.md", {'o', 'n', 'e'}});
    CHECK(d1.accepted);
    CHECK_EQ(d1.host, key_id(b.key));
    auto d2 = ask<zb::DeliverReply>(net, *b.f, p3, "deliver", zb::DeliverRequest{group1, "log.md", {'t', 'w', 'o'}});
    CHECK(!d2.accepted);
    const auto stored = b.node->zombie_result(group1);
    CHECK(stored.found);
    CHECK(stored.data == Bytes({'o', 'n', 'e'}));
    CHECK_EQ(stored.from, key_id(a.key));
    CHECK(!a.node->zombie_result(group1).found);

    // The main delivered: its host records the tombstone, gossip spreads it,
    // and the replica on b hears it.
    auto ended = ask<zb::GroupState>(net, *a.f, p1, "ended", zb::EndedRequest{group1});
    CHECK(ended.ending == zb::Ending::delivered);
    REQUIRE(net.settle(
        [&] {
            auto gb = group_on(b, group1);
            auto gc = group_on(c, group1);
            return gb && gc && gb->state.ending == zb::Ending::delivered && gc->state.ending == zb::Ending::delivered;
        },
        2000));
    REQUIRE(net.settle([&] { return contains(b.f->journal(p3), "msg:zombies.update"); }, 2000));
    // A replica that comes later (it was cut off) learns that the group ended.
    const auto p4 = start(net, c, module, '4');
    auto j4 = ask<zb::JoinReply>(net, *c.f, p4, "join", zb::JoinRequest{group1, ""});
    CHECK(!j4.accepted);
    CHECK(j4.state.ending == zb::Ending::delivered);
}

PAGLETS_TEST("zombies: the owner redirects and cancels a group; every host follows, also one that was down") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    for (auto& h : net.hosts) h->node->set_zombie_gossip(50ms);
    const std::string module = a.f->module("conformance.wasm");
    (void)b.f->module("conformance.wasm");
    const auto p1 = start(net, a, module, '1');
    const auto p2 = start(net, b, module, '2');
    CHECK(ask<zb::JoinReply>(net, *a.f, p1, "join", zb::JoinRequest{group2, "sink-1"}).accepted);
    CHECK(ask<zb::JoinReply>(net, *b.f, p2, "join", zb::JoinRequest{group2, "sink-1"}).accepted);

    b.down = true;
    auto redirected = a.node->zombie_redirect(group2, {}, "sink-2");
    REQUIRE_OK(redirected);
    CHECK_EQ(redirected->sink, 2);
    CHECK(!a.node->zombie_redirect(group2, "someone-else", "sink-3").has_value());
    b.down = false;
    REQUIRE(net.settle([&] { return group_on(b, group2) && group_on(b, group2)->state.sink == 2; }, 2000));
    CHECK_EQ(group_on(b, group2)->state.sink_spec, std::string("sink-2"));
    REQUIRE(net.settle([&] { return contains(b.f->journal(p2), "msg:zombies.update"); }, 2000));

    auto cancelled = b.node->zombie_cancel(group2, {}, "no longer needed");
    REQUIRE_OK(cancelled);
    REQUIRE(net.settle(
        [&] { return group_on(a, group2) && group_on(a, group2)->state.ending == zb::Ending::cancelled; }, 2000));
    CHECK_EQ(group_on(a, group2)->state.reason, std::string("no longer needed"));
    CHECK_EQ(group_on(a, group2)->state.sink, 2);
}
