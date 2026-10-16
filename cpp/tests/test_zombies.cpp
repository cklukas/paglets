// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `zombies` system paglet (docs/zombie-protocol.md): one replica per
// group and host, tombstones and redirects that reach every host, traces
// collected from every host and checked against the specification.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <future>
#include <mutex>
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

template <class Request>
std::string ask_result(Mesh& net, Fixture& f, const rt::PagletId& paglet, const std::string& op, const Request& q) {
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
    return entry;
}

template <class Reply, class Request>
Reply ask(Mesh& net, Fixture& f, const rt::PagletId& paglet, const std::string& op, const Request& q) {
    const auto entry = ask_result(net, f, paglet, op, q);
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

class StartupLandscape final : public rt::SystemPaglet {
public:
    std::int32_t status = 0;
    bool empty = false;
    bool offers_zombies = true;
    std::string_view name() const override { return "mesh-info"; }
    std::vector<std::string> default_ops() const override { return {"landscape"}; }
    void handle(rt::SystemContext&, rt::ServiceCall& call) override {
        if (status != 0) {
            (void)call.reply(status);
            return;
        }
        ps::mesh_info::Snapshot host;
        host.host = std::string(64, '1');
        host.host_name = "startup-test";
        if (offers_zombies) host.offers.push_back({"zombies", {"join", "deliver"}, {}});
        (void)call.reply(0, wire::to_msgpack(ps::mesh_info::Landscape{empty ? std::vector<ps::mesh_info::Snapshot>{}
                                                                       : std::vector{host}}));
    }
};

class StartupRegistry final : public rt::SystemPaglet {
public:
    explicit StartupRegistry(bool hold = true) : hold_(hold) {}
    std::string_view name() const override { return "zombies"; }
    std::vector<std::string> default_ops() const override { return {"join", "peers", "trace", "state"}; }
    void handle(rt::SystemContext& ctx, rt::ServiceCall& call) override {
        if (call.name() == "join") {
            zb::JoinRequest request;
            if (!wire::from_msgpack(call.payload(), request)) {
                (void)call.reply(abi::malformed);
                return;
            }
            std::lock_guard lock(mu_);
            state_.group = request.group;
            state_.sink_spec = request.sink_spec;
            if (!hold_) {
                (void)call.reply(0, wire::to_msgpack(zb::JoinReply{false, "registry refused startup", state_}));
                return;
            }
            context_ = &ctx;
            pending_ = call.defer();
            cv_.notify_one();
        } else if (call.name() == "peers") {
            (void)call.reply(0, wire::to_msgpack(zb::Peers{}));
        } else {
            (void)call.reply(0, wire::to_msgpack(zb::Done{}));
        }
    }
    bool wait_pending() {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, 5s, [&] { return pending_.has_value(); });
    }
    void answer(bool accepted) {
        std::lock_guard lock(mu_);
        if (!pending_) return;
        (void)context_->reply(std::move(*pending_), 0,
            wire::to_msgpack(zb::JoinReply{accepted, accepted ? "" : "registry refused startup", state_}));
        pending_.reset();
    }
private:
    std::mutex mu_;
    bool hold_;
    std::condition_variable cv_;
    rt::SystemContext* context_ = nullptr;
    std::optional<rt::Cap> pending_;
    zb::GroupState state_;
};

}  // namespace

PAGLETS_TEST("example: Chronicle waits for registry admission before acknowledging startup") {
    Fixture f;
    REQUIRE_OK(f.runtime->add_system_paglet(std::make_shared<StartupLandscape>()));
    auto registry = std::make_shared<StartupRegistry>();
    REQUIRE_OK(f.runtime->add_system_paglet(registry));
    const auto id = f.create("chronicle.wasm");
    chronicle_msgs::Start request;
    request.sink = "host:startup-test";
    auto response = std::async(std::launch::async, [&] { return f.call(id, "start", wire::to_msgpack(request)); });
    struct Release {
        std::shared_ptr<StartupRegistry> registry;
        ~Release() { registry->answer(false); }
    } release{registry};
    REQUIRE(registry->wait_pending());
    CHECK(response.wait_for(50ms) == std::future_status::timeout);
    registry->answer(true);
    auto reply = response.get();
    REQUIRE(reply.status == 0);
    chronicle_msgs::Started started;
    REQUIRE(wire::from_msgpack(reply.payload, started));
    CHECK(started.group.size() == 32);
    CHECK(started.accepted);
    CHECK(started.reason.empty());
    REQUIRE(f.runtime->dispose(id));
    REQUIRE(f.runtime->wait_idle());
    CHECK(f.runtime->ending(id).has_value());
}

PAGLETS_TEST("example: Chronicle returns startup failures and notifies its owner") {
    // Missing service, refused lookup, no hosts, unsupported host, missing
    // registry and an asynchronous registry refusal must all reach the caller.
    for (bool notify : {false, true}) for (int failure = 0; failure != 6; ++failure) {
        Fixture f;
        std::shared_ptr<ps::SystemServices> services;
        if (notify) {
            auto installed = ps::install_system_services(*f.runtime, {});
            REQUIRE_OK(installed);
            services = *installed;
        }
        if (failure != 0) {
            auto landscape = std::make_shared<StartupLandscape>();
            landscape->status = failure == 1 ? abi::denied : 0;
            landscape->empty = failure == 2;
            landscape->offers_zombies = failure != 3;
            REQUIRE_OK(f.runtime->add_system_paglet(landscape));
        }
        if (failure != 4) REQUIRE_OK(f.runtime->add_system_paglet(std::make_shared<StartupRegistry>(false)));
        const auto id = f.create("chronicle.wasm");
        chronicle_msgs::Start request;
        request.sink = "host:startup-test";
        const auto reply = f.call(id, "start", wire::to_msgpack(request));
        REQUIRE(reply.status == 0);
        chronicle_msgs::Started started;
        REQUIRE(wire::from_msgpack(reply.payload, started));
        CHECK(!started.accepted);
        CHECK(!started.reason.empty());
        REQUIRE(f.runtime->wait_idle());
        CHECK(f.runtime->ending(id).has_value());
        if (notify) {
            const auto notifications = services->notifications("local");
            REQUIRE(notifications.size() == 1);
            CHECK(notifications.front().paglet == id);
            CHECK(notifications.front().level == ps::user_info::Level::error);
            CHECK(notifications.front().text == started.reason);
        }
    }
}

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
    std::atomic<bool> collected = false;
    std::thread collect([&] {
        records = a.node->zombie_traces(group1);
        collected = true;
    });
    REQUIRE(net.settle([&] { return collected.load(); }, 2000));
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

PAGLETS_TEST("zombies: full sink key IDs take precedence over conflicting host names") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& producer = a.key < b.key ? a : b;
    auto& sink = a.key < b.key ? b : a;
    // Deliberately make the earlier ledger entry's name look like the
    // other host's canonical identity. No key collision is required.
    producer.name = key_id(sink.key);
    net.enroll_all();
    const auto module = producer.f->module("conformance.wasm");
    const auto id = start(net, producer, module, 'a');
    REQUIRE(ask<zb::JoinReply>(net, *producer.f, id, "join",
                              zb::JoinRequest{group1, "host:" + key_id(sink.key)}).accepted);
    const auto reply = ask<zb::DeliverReply>(net, *producer.f, id, "deliver",
                                           zb::DeliverRequest{group1, "result.bin", {'o', 'k'}});
    CHECK(reply.accepted);
    CHECK_EQ(reply.host, key_id(sink.key));
    CHECK(sink.node->zombie_result(group1).found);
    CHECK(!producer.node->zombie_result(group1).found);
}

PAGLETS_TEST("zombies: ambiguous sink names and key prefixes refuse delivery") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& producer = a.key < b.key ? a : b;
    auto& sink = a.key < b.key ? b : a;
    producer.name = key_id(sink.key).substr(0, 8);
    net.enroll_all();
    const auto module = producer.f->module("conformance.wasm");
    const auto id = start(net, producer, module, 'b');
    REQUIRE(ask<zb::JoinReply>(net, *producer.f, id, "join",
                              zb::JoinRequest{group2, "host:" + producer.name}).accepted);
    const auto reply = ask_result(net, *producer.f, id, "deliver",
                                  zb::DeliverRequest{group2, "result.bin", {'n', 'o'}});
    CHECK(reply.starts_with("reply:invalid_argument:"));
    CHECK(!sink.node->zombie_result(group2).found);
    CHECK(!producer.node->zombie_result(group2).found);
    // An explicit owner redirect to the full key removes the ambiguity.
    REQUIRE_OK(producer.node->zombie_redirect(group2, {}, "host:" + key_id(sink.key)));
    const auto delivered = ask<zb::DeliverReply>(net, *producer.f, id, "deliver",
                                               zb::DeliverRequest{group2, "result.bin", {'o', 'k'}});
    CHECK(delivered.accepted);
    CHECK_EQ(delivered.host, key_id(sink.key));
    CHECK(sink.node->zombie_result(group2).found);
    CHECK(!producer.node->zombie_result(group2).found);
}

PAGLETS_TEST("zombies: unregistered sink key IDs do not resolve as host names") {
    Mesh net;
    auto& a = net.add_host("a");
    const auto absent = SigningKey::generate();
    a.name = key_id(absent.public_key());
    net.enroll_all();
    const auto id = start(net, a, a.f->module("conformance.wasm"), 'c');
    REQUIRE(ask<zb::JoinReply>(net, *a.f, id, "join", zb::JoinRequest{group1, "host:" + a.name}).accepted);
    CHECK(ask_result(net, *a.f, id, "deliver", zb::DeliverRequest{group1, "result.bin", {'n', 'o'}})
              .starts_with("reply:not_found:"));
    CHECK(!a.node->zombie_result(group1).found);
    for (const std::string& spec : {std::string("host:"), std::string("unsupported:a")}) {
        REQUIRE_OK(a.node->zombie_redirect(group1, {}, spec));
        CHECK(ask_result(net, *a.f, id, "deliver", zb::DeliverRequest{group1, "result.bin", {'n', 'o'}})
                  .starts_with("reply:invalid_argument:"));
        CHECK(!a.node->zombie_result(group1).found);
    }
    REQUIRE_OK(a.node->zombie_redirect(group1, {}, "host:" + key_id(a.key)));
    CHECK(ask<zb::DeliverReply>(net, *a.f, id, "deliver", zb::DeliverRequest{group1, "result.bin", {'o', 'k'}}).accepted);
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
        // Long enough that the main's host returns before the end, also on
        // slow machines (CI runners).
        s.lifetime_ms = 30'000;
        s.sample_ms = 1000;
        s.beat_ms = 200;
        s.suspect_ms = 1500;
        s.round_ms = 500;
        s.expand_ms = 1000;
        const auto started_reply = a.f->call(creator, "start", wire::to_msgpack(s));
        REQUIRE(started_reply.status == 0);
        chronicle_msgs::Started started;
        REQUIRE(wire::from_msgpack(started_reply.payload, started));
        REQUIRE(started.accepted);
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
        // Both sides work apart for a while (wall time: samples are timed).
        const auto apart_until = std::chrono::steady_clock::now() + std::chrono::milliseconds(2500);
        net.settle([&] { return std::chrono::steady_clock::now() >= apart_until; }, 100'000);
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

namespace {
struct ZombieTestDirectory {
    std::filesystem::path path = std::filesystem::temp_directory_path() /
        ("zombie-refusal-" + std::to_string(std::random_device{}()));
    ~ZombieTestDirectory() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};
std::string probe_start(Mesh::Host& h, const rt::PagletId& id, std::string sink, std::int64_t bytes, bool hold = true) {
    const auto r = h.f->cmd(id, "start", Cmd{.name = std::move(sink), .code = hold ? 1 : 0, .ms = bytes});
    REQUIRE(r.status == 0);
    return dec<std::string>(r.payload);
}
std::size_t probe_failures(Fixture& f) {
    std::lock_guard lock(f.log_mu);
    return static_cast<std::size_t>(std::ranges::count_if(f.log, [](const std::string& s) {
        return s.starts_with("probe-failure:");
    }));
}
}  // namespace

PAGLETS_TEST("zombies: oversized SDK results end the group as failed and notify every replica") {
    ZombieTestDirectory dir;
    Mesh net;
    auto& a = net.add_host("a", {}, dir.path / "a");
    auto& b = net.add_host("b", {}, dir.path / "b");
    net.enroll_all();
    for (auto& h : net.hosts) { fast_mesh(net, *h); h->node->set_zombie_gossip(20ms); }
    const auto module = a.f->module("zombie_probe.wasm");
    b.f->module("zombie_probe.wasm");
    const auto id = start(net, a, module, '8');
    const auto group = probe_start(a, id, "host:b", abi::max_document_size + 256);
    REQUIRE(net.settle([&] { auto g = group_on(b, group); return g && !g->paglet.empty(); }, 3000));
    REQUIRE(a.f->call(id, "finish").status == 0);
    REQUIRE(net.settle([&] {
        for (auto& h : net.hosts) {
            auto g = group_on(*h, group);
            if (!g || g->state.ending != zb::Ending::failed || !g->paglet.empty()) return false;
        }
        return true;
    }, 4000));
    for (auto& h : net.hosts) {
        auto g = group_on(*h, group);
        CHECK(g->state.reason.find("too_large") != std::string::npos);
        CHECK_EQ(probe_failures(*h->f), std::size_t{1});
        CHECK(!h->node->zombie_result(group).found);
    }
    net.restart(a);
    net.restart(b);
    for (auto& h : net.hosts) {
        auto g = group_on(*h, group);
        REQUIRE(g.has_value());
        CHECK(g->state.ending == zb::Ending::failed);
        CHECK(g->paglet.empty());
    }
}

PAGLETS_TEST("zombies: a refused result waits for durable failure recording before notification or disposal") {
    ZombieTestDirectory dir;
    Mesh net;
    auto& a = net.add_host("a", {}, dir.path / "a");
    net.enroll_all();
    fast_mesh(net, a);
    a.node->set_zombie_gossip(20ms);
    const auto id = start(net, a, a.f->module("zombie_probe.wasm"), '9');
    const auto group = probe_start(a, id, "host:a", abi::max_document_size + 256);
    REQUIRE(net.settle([&] { return group_on(a, group).has_value(); }, 1000));
    const auto blocked = a.state / "node/zombies.state.tmp";
    std::filesystem::create_directory(blocked);
    REQUIRE(a.f->call(id, "finish").status == 0);
    const auto until = std::chrono::steady_clock::now() + 300ms;
    net.settle([&] { return std::chrono::steady_clock::now() >= until; }, 1000);
    REQUIRE(a.f->runtime->info(id).has_value());
    CHECK(group_on(a, group)->state.ending == zb::Ending::none);
    CHECK_EQ(probe_failures(*a.f), std::size_t{0});
    const auto status = dec<std::vector<std::int64_t>>(a.f->call(id, "probe_status").payload);
    CHECK_EQ(status.at(2), 1);  // stopped regenerating and resending the impossible result
    std::filesystem::remove(blocked);
    REQUIRE(net.settle([&] { auto g = group_on(a, group); return g && g->state.ending == zb::Ending::failed && g->paglet.empty(); }, 3000));
    CHECK_EQ(probe_failures(*a.f), std::size_t{1});
    net.restart(a);
    REQUIRE(group_on(a, group).has_value());
    CHECK(group_on(a, group)->state.ending == zb::Ending::failed);
}

PAGLETS_TEST("zombies: a missing sink can be redirected without a false failure or lost result") {
    Mesh net;
    auto& a = net.add_host("a");
    net.enroll_all();
    fast_mesh(net, a);
    const auto id = start(net, a, a.f->module("zombie_probe.wasm"), 'a');
    const auto group = probe_start(a, id, "host:later", 16, false);
    REQUIRE(net.settle([&] {
        auto r = a.f->call(id, "probe_status");
        return r.status == 0 && dec<std::vector<std::int64_t>>(r.payload).at(2) >= 2;
    }, 2000));
    CHECK_EQ(probe_failures(*a.f), std::size_t{0});
    REQUIRE(a.node->zombie_redirect(group, {}, "host:a"));
    REQUIRE(net.settle([&] { auto g = group_on(a, group); return g && g->state.ending == zb::Ending::delivered && g->paglet.empty(); }, 3000));
    CHECK_EQ(probe_failures(*a.f), std::size_t{0});
    CHECK_EQ(a.node->zombie_result(group).data.size(), std::size_t{16});
}

PAGLETS_TEST("zombies: same-module impostors cannot forge replica beats or registry endings") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    for (auto& h : net.hosts) { fast_mesh(net, *h); h->node->set_zombie_gossip(20ms); }
    const auto module = a.f->module("zombie_probe.wasm");
    b.f->module("zombie_probe.wasm");
    const auto original = start(net, a, module, 'b');
    const auto group = probe_start(a, original, "host:a", 16);
    REQUIRE(net.settle([&] { auto g = group_on(b, group); return g && !g->paglet.empty(); }, 3000));
    const auto target = group_on(b, group)->paglet;
    const auto impostor = start(net, a, module, 'c');
    rt::Cap endpoint;
    endpoint.kind = rt::Cap::Kind::endpoint;
    endpoint.id = "impostor-endpoint";
    endpoint.target = target;
    endpoint.host = paglets::mesh::key_id(b.key);
    endpoint.ops = {"*"};
    const auto handle = a.f->runtime->add_capability(impostor, std::move(endpoint));
    REQUIRE(handle.has_value());
    namespace zp = paglets::protocols::zombie;
    const auto rid = zp::replica_id(paglets::mesh::key_id(a.key));
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    paglets::msgpack::write_value(w, rid);
    zp::Message forged = zp::Beat{100000, zp::Rank{true, 1, 100000, 1, rid}, {}, {rid}};
    zp::paglets_encode(w, forged);
    CHECK_EQ(a.f->code(impostor, "send", Cmd{.handle = *handle, .name = "zombie.msg", .payload = w.take()}), 0);
    const auto fake_end = wire::to_msgpack(zb::GroupState{group, zb::Ending::failed, "forged ending", 1, 1, "host:a"});
    CHECK_EQ(a.f->code(impostor, "send", Cmd{.handle = *handle, .name = std::string(zb::update_message), .payload = fake_end}), 0);
    const auto until = std::chrono::steady_clock::now() + 300ms;
    net.settle([&] { return std::chrono::steady_clock::now() >= until; }, 1000);
    REQUIRE(b.f->runtime->info(target).has_value());
    const auto status = dec<std::vector<std::int64_t>>(b.f->call(target, "probe_status").payload);
    CHECK(status.at(1) < 100000);
    CHECK_EQ(probe_failures(*b.f), std::size_t{0});
    CHECK(group_on(b, group)->state.ending == zb::Ending::none);
}

PAGLETS_TEST("zombies: a permanent remote refusal fails the group; duplicate acceptance is separate") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    for (auto& h : net.hosts) { fast_mesh(net, *h); h->node->set_zombie_gossip(20ms); }
    const auto module = a.f->module("zombie_probe.wasm");
    b.f->module("zombie_probe.wasm");
    const auto id = start(net, a, module, 'd');
    const auto group = probe_start(a, id, "host:b", 16);
    REQUIRE(net.settle([&] { auto g = group_on(b, group); return g && !g->paglet.empty(); }, 3000));
    net.filter = [&](const PublicKey& from, const PublicKey& to, Bytes& frame) {
        if (from != a.key || to != b.key || frame_type(frame) != "zb-deliver") return true;
        auto v = decode(frame);
        const Fields f{*v->as_map()};
        b.port.send(a.key, encode(Value(Map{{"t", Value("zb-accepted")}, {"r", Value(*f.integer("r"))},
            {"a", Value(false)}, {"e", Value(static_cast<std::int64_t>(abi::denied))}})));
        return false;
    };
    REQUIRE(a.f->call(id, "finish").status == 0);
    REQUIRE(net.settle([&] {
        for (auto& h : net.hosts) {
            auto g = group_on(*h, group);
            if (!g || g->state.ending != zb::Ending::failed || !g->paglet.empty()) return false;
        }
        return true;
    }, 4000));
    CHECK(group_on(a, group)->state.reason.find("denied") != std::string::npos);
    CHECK(!b.node->zombie_result(group).found);
    CHECK_EQ(probe_failures(*a.f), std::size_t{1});
    CHECK_EQ(probe_failures(*b.f), std::size_t{1});
}

PAGLETS_TEST("zombies: lost acceptance replies retry and accept the already-stored result without failure") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    for (auto& h : net.hosts) { fast_mesh(net, *h); h->node->set_zombie_gossip(20ms); }
    const auto module = a.f->module("zombie_probe.wasm");
    b.f->module("zombie_probe.wasm");
    const auto id = start(net, a, module, 'e');
    const auto group = probe_start(a, id, "host:b", 16);
    REQUIRE(net.settle([&] { auto g = group_on(b, group); return g && !g->paglet.empty(); }, 3000));
    bool dropped = false;
    bool duplicate = false;
    net.filter = [&](const PublicKey&, const PublicKey&, Bytes& frame) {
        if (frame_type(frame) != "zb-accepted") return true;
        auto v = decode(frame);
        const Fields f{*v->as_map()};
        if (!dropped) { dropped = true; return false; }
        const auto* accepted = f.get("a");
        if (accepted && accepted->as_bool() && !*accepted->as_bool()) duplicate = true;
        return true;
    };
    REQUIRE(a.f->call(id, "finish").status == 0);
    REQUIRE(net.settle([&] {
        auto g = group_on(a, group);
        return g && g->state.ending == zb::Ending::delivered && g->paglet.empty();
    }, 20000));
    CHECK(dropped);
    CHECK(duplicate);
    CHECK_EQ(b.node->zombie_result(group).data.size(), std::size_t{16});
    CHECK_EQ(probe_failures(*a.f), std::size_t{0});
}

PAGLETS_TEST("zombies: sink storage errors and oversized remote frames are refusals, not duplicate success") {
    ZombieTestDirectory dir;
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b", {}, dir.path / "b");
    net.enroll_all();
    std::int64_t error = 99;
    bool accepted = true;
    net.filter = [&](const PublicKey&, const PublicKey&, Bytes& frame) {
        if (frame_type(frame) != "zb-accepted") return true;
        auto v = decode(frame);
        const Fields f{*v->as_map()};
        error = f.integer("e").value_or(99);
        accepted = *f.get("a")->as_bool();
        return true;
    };
    auto deliver = [&](std::size_t size) {
        b.node->receive(a.key, encode(Value(Map{{"t", Value("zb-deliver")}, {"r", Value(std::int64_t{1})},
            {"g", Value(group1)}, {"s", Value(std::int64_t{1})}, {"n", Value("result.bin")},
            {"d", Value(Bytes(size, 'x'))}})));
        net.deliver();
    };
    const auto blocked = b.state / "node/zombies.state.tmp";
    std::filesystem::create_directory(blocked);
    deliver(16);
    CHECK_EQ(error, static_cast<std::int64_t>(abi::internal));
    CHECK(!accepted);
    CHECK(!b.node->zombie_result(group1).found);
    std::filesystem::remove(blocked);
    deliver(4 * 1024 * 1024 + 1);
    CHECK_EQ(error, static_cast<std::int64_t>(abi::quota));
    CHECK(!accepted);
    CHECK(!b.node->zombie_result(group1).found);
    deliver(16);
    CHECK_EQ(error, std::int64_t{0});
    CHECK(accepted);
    net.restart(b);
    CHECK(b.node->zombie_result(group1).found);
    net.enroll_all();
    error = 99;
    accepted = true;
    deliver(16);
    CHECK_EQ(error, std::int64_t{0});
    CHECK(!accepted);
}

PAGLETS_TEST("remaining: stale zombie callbacks cannot mutate a recovered lineage") {
    Mesh net;
    auto& a = net.add_host("a");
    net.enroll_all();
    fast_mesh(net, a);
    const auto id = start(net, a, a.f->module("zombie_probe.wasm"), 'a');
    auto begin = a.f->cmd(id, "start", Cmd{.name = "host:a", .code = 2, .ms = 16});
    REQUIRE(begin.status == 0);
    auto state = [&] { return dec<std::vector<std::int64_t>>(a.f->call(id, "step_work").payload); };
    REQUIRE(net.settle([&] { return state().at(1) == 1; }, 3000));
    REQUIRE(a.f->call(id, "recover_step").status == 0);
    REQUIRE(net.settle([&] { return state().at(1) >= 2; }, 3000));
    const auto before = state().at(0);
    REQUIRE(a.f->call(id, "release_old_step").status == 0);
    CHECK_EQ(state().at(0), before);
    REQUIRE(a.f->call(id, "release_old_step").status == 0);  // complete the current step
    CHECK_EQ(state().at(0), before + 100);
    REQUIRE(a.f->call(id, "repeat_last_step").status == 0);
    CHECK_EQ(state().at(0), before + 100);  // repeated callback has independent, still-valid storage
    REQUIRE(a.f->runtime->dispose(id));
}
