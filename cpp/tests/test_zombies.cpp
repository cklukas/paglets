// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `zombies` system paglet (docs/zombie-protocol.md): one replica per
// group and host, tombstones and redirects that reach every host, traces
// collected from every host and checked against the specification.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <atomic>
#include <cstdlib>
#include <fstream>
#include <thread>

#include <chronicle_msgs.hpp>
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

    // A host that knows a group from gossip before its replica joins still
    // takes the sink from the join.
    const std::string group3 = "00112233445566778899aabbccddeeff";
    const auto p5 = start(net, a, module, '5');
    const auto p6 = start(net, c, module, '6');
    zb::JoinRequest q3{group3, "host:a", 0};
    CHECK(ask<zb::JoinReply>(net, *a.f, p5, "join", q3).accepted);
    REQUIRE(net.settle([&] { return group_on(c, group3).has_value(); }, 2000));
    q3.replica = 2;
    auto j6 = ask<zb::JoinReply>(net, *c.f, p6, "join", q3);
    CHECK(j6.accepted);
    CHECK_EQ(j6.state.sink_spec, std::string("host:a"));

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

namespace {

void fast_mesh(Mesh& net, Mesh::Host& h) {
    h.node->set_zombie_gossip(100ms);
    h.node->set_compute_timing({.sample = 100ms, .gossip = 100ms, .ttl = 1500ms, .redirect_after = 1000ms});
    paglets::node::Node::LocationTiming lt;
    lt.heartbeat = 200ms;
    lt.host_timeout = 1000ms;
    h.node->set_location_timing(lt);
    (void)net;
}

}  // namespace

PAGLETS_TEST("example: the Mesh Chronicle outlives a partition and a restart of its main's host") {
    const auto dir =
        std::filesystem::temp_directory_path() / ("paglets-chronicle-" + std::to_string(std::random_device{}()));
    std::filesystem::remove_all(dir);
    {
        Mesh net;
        auto& a = net.add_host("a", {}, dir / "a");
        auto& b = net.add_host("b", {}, dir / "b");
        auto& c = net.add_host("c", {}, dir / "c");
        net.enroll_all();
        for (auto& h : net.hosts) fast_mesh(net, *h);
        const std::string module = a.f->module("chronicle.wasm");
        if (module.empty()) skip("chronicle.wasm is not built");
        // Every host has fresh snapshots of the others (the replicas go where mesh-info sees hosts).
        REQUIRE(net.settle([&] { return a.node->landscape().size() == 3u; }, 2000));
        const auto creator = start(net, a, module, '1');
        chronicle_msgs::Start s;
        s.sink = "host:c";
        s.lifetime_ms = 20'000;
        s.sample_ms = 1000;
        s.beat_ms = 200;
        s.suspect_ms = 1500;
        s.round_ms = 500;
        s.expand_ms = 1000;
        const auto started_reply = a.f->call(creator, "start", wire::to_msgpack(s));
        REQUIRE(started_reply.status == 0);
        chronicle_msgs::Started started;
        REQUIRE(wire::from_msgpack(started_reply.payload, started));
        const std::string group = started.group;
        REQUIRE(group.size() == 32u);
        // A replica on every host.
        REQUIRE(net.settle(
            [&] {
                for (auto& h : net.hosts) {
                    auto g = group_on(*h, group);
                    if (!g || g->paglet.empty()) return false;
                }
                return true;
            },
            4000));
        auto status_of = [&](Mesh::Host& h) {
            chronicle_msgs::Status st;
            auto g = group_on(h, group);
            if (!g || g->paglet.empty()) return st;
            auto r = h.f->call(g->paglet, "status");
            if (r.status == 0) (void)wire::from_msgpack(r.payload, st);
            return st;
        };
        // A host joins the mesh: the main makes a replica there.
        REQUIRE(net.settle([&] { return status_of(a).samples >= 2; }, 4000));
        auto& d = net.add_host("d", {}, dir / "d");
        net.admin_record("host-enroll", paglets::mesh::data::host_enroll(d.key, "d", {}));
        fast_mesh(net, d);
        (void)d.f->module("chronicle.wasm");
        REQUIRE(net.settle(
            [&] {
                auto g = group_on(d, group);
                return g && !g->paglet.empty();
            },
            6000));
        // A few samples, then the main's host is cut off: b, c and d elect a
        // new main while a works on alone.
        REQUIRE(net.settle([&] { return status_of(a).samples >= 5; }, 4000));
        a.down = true;
        REQUIRE(net.settle(
            [&] { return status_of(b).role == "main" || status_of(c).role == "main" || status_of(d).role == "main"; },
            4000));
        net.settle([] { return false; }, 1000);  // both sides work apart for a while
        // a restarts (its state is stored) and comes back into the mesh.
        net.restart(a);
        fast_mesh(net, a);
        a.down = false;
        // One main again, then the end: the result at the sink, every replica gone.
        REQUIRE(net.settle([&] { return c.node->zombie_result(group).found; }, 20000));
        const bool all_ended = net.settle(
            [&] {
                for (auto& h : net.hosts) {
                    auto g = group_on(*h, group);
                    if (!g || g->state.ending != zb::Ending::delivered || !g->paglet.empty()) return false;
                }
                return true;
            },
            4000);
        if (!all_ended) {
            for (auto& h : net.hosts) {
                auto g = group_on(*h, group);
                std::cerr << h->name << ": " << (g ? std::to_string(int(g->state.ending)) + " paglet " + g->paglet : "none");
                if (g && !g->paglet.empty()) {
                    auto st = status_of(*h);
                    std::cerr << " role " << st.role << " samples " << st.samples;
                    if (auto i = h->f->runtime->info(g->paglet)) std::cerr << " alive";
                }
                std::cerr << "\n";
            }
        }
        REQUIRE(all_ended);
        const auto result = c.node->zombie_result(group);
        const std::string log(result.data.begin(), result.data.end());
        if (const char* out = std::getenv("PAGLETS_CHRONICLE_LOG")) {
            std::ofstream(out) << log;  // to read the life log of the test run
        }
        CHECK(log.starts_with("# Mesh life log"));
        CHECK(log.find("took over as main") != std::string::npos);
        CHECK(log.find("disappeared") != std::string::npos);
        CHECK(log.find("is back") != std::string::npos);
        CHECK(log.find("partition: both sides had a main") != std::string::npos);
        CHECK(log.find("The work is done") != std::string::npos);
        CHECK(log.find("**d** joined the mesh and got a replica") != std::string::npos);
        CHECK(log.find("joined the mesh") == log.rfind("joined the mesh"));  // once
        // The protocol steps of every host conform to the specification.
        std::vector<zb::TraceRecord> records;
        std::atomic<bool> collected = false;
        std::thread collect([&] {
            records = c.node->zombie_traces(group);
            collected = true;
        });
        net.settle([&] { return collected.load(); }, 4000);
        collect.join();
        const auto report_ = paglets::protocols::zombie::check_traces(records, 1);
        for (const auto& v : report_.violations) paglets::test::record_failure(__FILE__, __LINE__, v);
        CHECK(report_.ok());
        CHECK(report_.takeovers >= 2u);
        CHECK_EQ(report_.accepted, 1u);
        report("chronicle", "hosts with a replica (one joined the mesh during the run)", 4);
        report("chronicle", "takeovers recorded by the hosts", report_.takeovers);
        report("chronicle", "protocol steps checked against the specification", report_.records);
        report("chronicle", "results accepted by the sink", report_.accepted);
        report("chronicle", "size of the life log", log.size(), "bytes");
    }
    std::filesystem::remove_all(dir);
}
