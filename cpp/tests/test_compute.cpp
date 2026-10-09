// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Mesh information and compute services (WP16, planning/cpp-compute.md).
// The exit: the pi compute example schedules across three hosts and
// survives the loss of one host mid-run.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <bbp.hpp>
#include <pi_msgs.hpp>
#include <paglets/services/compute_slots.hpp>
#include <paglets/services/mesh_info.hpp>
#include <paglets/wire/reflect.hpp>

using namespace paglets::test;
namespace mi = paglets::services::mesh_info;
namespace cs = paglets::services::compute_slots;
namespace wire = paglets::wire;

namespace {

void fast(Mesh& net) {
    for (auto& h : net.hosts) {
        h->node->set_compute_timing({.sample = 50ms, .gossip = 50ms, .ttl = 2000ms, .redirect_after = 0ms});
    }
}

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

// A request from a paglet to a service of its host; returns the reply.
template <class Reply, class Request>
Reply ask(Mesh& net, Fixture& f, const rt::PagletId& paglet, std::string_view service, const std::string& op,
          const Request& q) {
    const auto before = f.journal(paglet).size();
    const auto h = service_handle(f, paglet, service);
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

const mi::Snapshot* snapshot_of(const std::vector<mi::Snapshot>& all, const std::string& name) {
    for (const auto& s : all) {
        if (s.host_name == name) return &s;
    }
    return nullptr;
}

}  // namespace

PAGLETS_TEST("mesh-info: every host knows every host's snapshot; select prefers free slots and low load") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    net.enroll_all();
    fast(net);
    a.node->set_compute_slots(4);
    b.node->set_compute_slots(2);
    c.node->set_compute_slots(1);
    // (snapshots taken before the slot counts were set are replaced)
    REQUIRE(net.settle([&] {
        for (auto& h : net.hosts) {
            const auto all = h->node->landscape();
            const mi::Snapshot* sb = snapshot_of(all, "b");
            const mi::Snapshot* sc = snapshot_of(all, "c");
            if (all.size() != 3u || sb == nullptr || sb->slots != 2 || sc == nullptr || sc->slots != 1) return false;
        }
        return true;
    }));
    const auto all = a.node->landscape();
    CHECK(all.front().host_name == "a");  // this host first
    const mi::Snapshot* sb = snapshot_of(all, "b");
    REQUIRE(sb != nullptr);
    CHECK_EQ(sb->host, key_id(b.key));
    CHECK_EQ(sb->slots, 2);
    CHECK(sb->cpus > 0 && sb->memory_total > 0 && sb->observed_ms > 0);
    CHECK(!sb->os.empty());

    // Through the paglet-facing service.
    const std::string module = a.f->module("conformance.wasm");
    const auto p = start(net, a, module, '1');
    const auto landscape = ask<mi::Landscape>(net, *a.f, p, "mesh-info", "landscape", mi::LandscapeRequest{});
    CHECK_EQ(landscape.hosts.size(), 3u);
    mi::SelectRequest q;
    q.limit = 2;
    q.max_load_per_cpu = 1000;
    q.min_slots_free = 2;
    const auto selection = ask<mi::Selection>(net, *a.f, p, "mesh-info", "select", q);
    CHECK(!selection.fallback);
    REQUIRE(selection.hosts.size() == 2u);
    for (const auto& s : selection.hosts) CHECK(s.host_name == "a" || s.host_name == "b");
    // Nothing fits: the least loaded hosts all the same.
    q.min_slots_free = 100;
    const auto fallback = ask<mi::Selection>(net, *a.f, p, "mesh-info", "select", q);
    CHECK(fallback.fallback);
    CHECK_EQ(fallback.hosts.size(), 2u);

    // A host that goes away drops out once its snapshot is stale.
    c.down = true;
    REQUIRE(net.settle([&] { return a.node->landscape().size() == 2u; }, 2000));
}

PAGLETS_TEST("compute-slots: grants, a queue, wake-ups and redirects to hosts with free slots") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    fast(net);
    a.node->set_compute_slots(1);
    b.node->set_compute_slots(1);
    const std::string module = a.f->module("conformance.wasm");
    REQUIRE_OK(b.f->runtime->add_module_file(guest_path("conformance.wasm")));
    const auto busy_b = start(net, b, module, '2');
    REQUIRE(net.settle([&] { return a.node->landscape().size() == 2u; }));
    // b's slot is taken.
    auto d = ask<cs::Decision>(net, *b.f, busy_b, "compute-slots", "request_slot", cs::SlotRequest{"job", 1, 0});
    CHECK(d.verdict == cs::Verdict::run_now);
    REQUIRE(net.settle([&] {
        const auto all = a.node->landscape();
        const mi::Snapshot* sb = snapshot_of(all, "b");
        return sb != nullptr && sb->slots_free == 0;
    }));

    const auto p1 = start(net, a, module, '3');
    const auto p2 = start(net, a, module, '4');
    const auto p3 = start(net, a, module, '5');
    d = ask<cs::Decision>(net, *a.f, p1, "compute-slots", "request_slot", cs::SlotRequest{"job", 1, 0});
    CHECK(d.verdict == cs::Verdict::run_now);
    const std::string lease = d.lease;
    CHECK(!lease.empty());
    // Asking again keeps the lease.
    CHECK_EQ(ask<cs::Decision>(net, *a.f, p1, "compute-slots", "request_slot", cs::SlotRequest{"job", 1, 0}).lease,
             lease);
    d = ask<cs::Decision>(net, *a.f, p2, "compute-slots", "request_slot", cs::SlotRequest{"job", 1, 0});
    CHECK(d.verdict == cs::Verdict::queued);
    CHECK_EQ(d.position, 0);
    d = ask<cs::Decision>(net, *a.f, p3, "compute-slots", "request_slot", cs::SlotRequest{"job", 1, 0});
    CHECK(d.verdict == cs::Verdict::queued);
    CHECK_EQ(d.position, 1);
    // More cores than any host has: rejected.
    d = ask<cs::Decision>(net, *a.f, p3, "compute-slots", "request_slot", cs::SlotRequest{"job", 8, 0});
    CHECK(d.verdict == cs::Verdict::rejected);
    d = ask<cs::Decision>(net, *a.f, p3, "compute-slots", "request_slot", cs::SlotRequest{"job", 1, 0});
    CHECK(d.verdict == cs::Verdict::queued);
    auto status = a.node->compute_status();
    CHECK_EQ(status.self.slots, 1);
    CHECK_EQ(status.self.free, 0);
    CHECK_EQ(status.leases.size(), 1u);
    CHECK_EQ(status.queue.size(), 2u);

    // b's slot frees up: the second waiter goes there (the first stays,
    // it is next here).
    REQUIRE(ask<cs::ReleaseReply>(net, *b.f, busy_b, "compute-slots", "release_slot", cs::ReleaseRequest{d.lease})
                .released == false);  // not b's lease
    const auto b_status = b.node->compute_status();
    REQUIRE(b_status.leases.size() == 1u);
    CHECK(ask<cs::ReleaseReply>(net, *b.f, busy_b, "compute-slots", "release_slot",
                                cs::ReleaseRequest{b_status.leases[0].lease})
              .released);
    REQUIRE(net.settle([&] { return contains(a.f->journal(p3), "msg:compute.redirect"); }, 1000));
    CHECK(!contains(a.f->journal(p2), "msg:compute.redirect"));
    // a's lease ends: the first waiter gets the slot.
    CHECK(ask<cs::ReleaseReply>(net, *a.f, p1, "compute-slots", "release_slot", cs::ReleaseRequest{lease}).released);
    REQUIRE(net.settle([&] { return contains(a.f->journal(p2), "msg:compute.granted"); }, 400));
    status = a.node->compute_status();
    CHECK_EQ(status.leases.size(), 1u);
    CHECK(status.leases[0].paglet == p2);
    CHECK(status.queue.empty());
    // A paglet that ends gives its slot back.
    REQUIRE_OK(a.f->runtime->terminate(p2, "test"));
    REQUIRE(net.settle([&] { return a.node->compute_status().leases.empty(); }, 400));
}

PAGLETS_TEST("pi: the pi example computes across three hosts and survives the loss of one (WP16 exit)") {
    SigningKey admin = SigningKey::generate();
    SigningKey owner = SigningKey::generate();
    auto genesis = make_genesis("pi", {&admin}, {}, 1);
    REQUIRE_OK(genesis);
    std::vector<std::unique_ptr<NetHost>> hosts;
    for (int i = 0; i < 3; ++i) hosts.push_back(std::make_unique<NetHost>(*genesis));
    const char* names[] = {"a", "b", "c"};
    auto record = [&](const std::string& type, Map d) {
        auto r = hosts[0]->node->draft(type, std::move(d));
        REQUIRE_OK(r);
        r->sign(admin);
        for (auto& h : hosts) REQUIRE_OK(h->node->submit(*r));
    };
    for (int i = 0; i < 3; ++i) record("host-enroll", data::host_enroll(hosts[i]->node->host(), names[i], {}));
    record("owner-enroll", data::owner_enroll(owner.public_key(), "olga", {}));
    for (auto& h : hosts) {
        h->node->set_discovery_timing({.exchange = 200ms});
        h->node->set_location_timing({.heartbeat = 100ms,
                                      .host_timeout = 1500ms,
                                      .refresh = std::chrono::minutes(5),
                                      .record_ttl = std::chrono::hours(1),
                                      .lookup_timeout = 1000ms});
        h->node->set_compute_timing({.sample = 100ms, .gossip = 100ms, .ttl = 1500ms, .redirect_after = 200ms});
        h->node->set_compute_slots(2);
        h->node->set_move_timeout(2000ms);
    }
    for (int i = 1; i < 3; ++i) {
        auto contact = hosts[i]->transport->probe(hosts[0]->transport->url());
        REQUIRE_OK(contact);
        hosts[i]->node->joined(*contact);
    }
    auto eventually = [&](const std::function<bool()>& done, int rounds = 3000) {
        for (int i = 0; i < rounds; ++i) {
            if (done()) return true;
            for (auto& h : hosts) h->node->tick();
            std::this_thread::sleep_for(10ms);
        }
        return done();
    };
    REQUIRE(eventually([&] {
        for (auto& h : hosts) {
            if (h->node->landscape().size() != 3u) return false;
        }
        return true;
    }));

    auto& a = *hosts[0];
    const std::string module = a.f->module("pi.wasm");
    const std::int64_t now = unix_ms();
    auto passport = Passport::issue(owner, genesis->id(), *parse_key_id(module), paglet_id('7'), Value(), now - 1000,
                                    now + 600'000);
    REQUIRE_OK(passport);
    auto job = a.node->create(module, *passport);
    REQUIRE_OK(job);
    auto status = [&] {
        auto r = a.f->call(*job, "status");
        REQUIRE(r.status == 0);
        pi_msgs::Status s;
        REQUIRE(wire::from_msgpack(r.payload, s));
        return s;
    };
    pi_msgs::Start start{.digits = 1200, .chunk = 30, .max_in_flight = 6, .chunk_timeout_ms = 3000, .work_ms = 250};
    auto started = a.f->call(*job, "start", wire::to_msgpack(start));
    REQUIRE(started.status == 0);
    pi_msgs::Started ack;
    REQUIRE(wire::from_msgpack(started.payload, ack));
    REQUIRE(ack.accepted);

    // The chunks spread over the three hosts.
    REQUIRE(eventually([&] { return status().hosts.size() == 3u; }));
    const auto before = status();
    CHECK(!before.finished);

    // c goes away mid-run, with chunks of its own in flight.
    hosts[2]->f->runtime->shutdown(false);
    hosts[2]->transport->stop();
    hosts.pop_back();
    REQUIRE(eventually([&] { return status().finished; }, 6000));
    const auto done = status();
    CHECK_EQ(done.done, 1200);
    CHECK_EQ(done.chunks_done, 40);
    CHECK(done.resent > 0);  // c's chunks went out again
    // The digits are pi's.
    CHECK(done.hex.starts_with("3.243F6A8885A308D313198A2E03707344A4093822299F31D0082EFA98EC4E6C89"));
    CHECK(done.hex == "3." + pi_bbp::hex_digits(0, 1200));
}

PAGLETS_TEST("offers: hosts announce features; paglets find them and move to them by offer") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    net.enroll_all();
    fast(net);
    auto attr = [](std::string name, std::string text) { return mi::Attribute{std::move(name), std::move(text), 0, false}; };
    auto number = [](std::string name, double n) { return mi::Attribute{std::move(name), {}, n, true}; };
    b.node->set_offer(mi::Offer{"ai",
                                {"summarize", "classify"},
                                {attr("backend", "test"), attr("tasks", "summarize, classify"), number("context", 32000)}});
    c.node->set_offer(mi::Offer{"ai", {"summarize"}, {attr("backend", "test"), number("context", 8000)}});
    c.node->set_offer(mi::Offer{"web", {"fetch"}, {}});
    REQUIRE(net.settle([&] {
        return a.node->find_offers(mi::OffersRequest{.service = "ai"}).size() == 2u &&
               a.node->find_offers(mi::OffersRequest{.service = "web"}).size() == 1u;
    }));

    // A paglet on a asks mesh-info.
    const std::string module = a.f->module("conformance.wasm");
    const auto id = start(net, a, module, 'e');
    auto found = ask<mi::Offers>(net, *a.f, id, "mesh-info", "find_offers",
                                 mi::OffersRequest{.service = "ai", .op = "summarize", .prefer = "-context"});
    REQUIRE(found.matches.size() == 2u);
    CHECK(found.matches[0].host_name == "b");
    CHECK(found.matches[1].host_name == "c");
    CHECK((found.matches[0].offer.ops == std::vector<std::string>{"summarize", "classify"}));
    found = ask<mi::Offers>(net, *a.f, id, "mesh-info", "find_offers",
                            mi::OffersRequest{.service = "ai", .require = {{"context", ">=", {}, 16000}}});
    REQUIRE(found.matches.size() == 1u);
    CHECK(found.matches[0].host_name == "b");
    found = ask<mi::Offers>(net, *a.f, id, "mesh-info", "find_offers",
                            mi::OffersRequest{.service = "ai", .require = {{"tasks", "has", "classify", 0}}});
    REQUIRE(found.matches.size() == 1u);
    CHECK(found.matches[0].host_name == "b");
    CHECK(a.node->find_offers(mi::OffersRequest{.service = "ai", .op = "embed"}).empty());

    // A ticket names an offer: the paglet goes to the host offering it.
    REQUIRE_OK(a.node->dispatch(id, "offer:ai.classify"));
    REQUIRE(net.settle([&] { return b.f->runtime->info(id).has_value(); }, 400));

    // b withdraws its offer: nothing offers `classify` any more.
    b.node->withdraw_offer("ai");
    REQUIRE(net.settle([&] { return a.node->find_offers(mi::OffersRequest{.service = "ai"}).size() == 1u; }));
    CHECK(c.node->find_offers(mi::OffersRequest{.service = "ai", .op = "classify"}).empty());
    const auto failed = b.node->move_stats().moves_failed;
    REQUIRE_OK(b.node->dispatch(id, "offer:ai.classify"));
    REQUIRE(net.settle([&] { return b.node->move_stats().moves_failed == failed + 1; }, 400));
    REQUIRE_OK(b.node->dispatch(id, "offer:web"));
    REQUIRE(net.settle([&] { return c.f->runtime->info(id).has_value(); }, 400));
}
