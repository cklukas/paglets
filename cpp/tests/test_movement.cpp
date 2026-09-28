// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Moving paglets between hosts (WP12, planning/cpp-networking.md, section
// 6). The exit: dispatch and clone between two hosts; repeated moves
// transfer only changed pages; grants follow the paglet; failed transfers
// leave the paglet on the source.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <filesystem>
#include <fstream>
#include <random>

using namespace paglets::test;
namespace fs = std::filesystem;

namespace {

constexpr auto dispatch_op = static_cast<std::int32_t>(abi::LifecycleOp::dispatch);
constexpr auto clone_op = static_cast<std::int32_t>(abi::LifecycleOp::clone);

// A paglet of the enrolled owner, created from its passport on `host`.
rt::PagletId start(Mesh& net, Mesh::Host& host, const std::string& module, char id) {
    const auto p = net.passport(module, paglet_id(id));
    auto created = host.node->create(module, p);
    REQUIRE_OK(created);
    return *created;
}

std::int64_t count(Fixture& f, const rt::PagletId& id) {
    auto r = f.call(id, "count");
    if (r.status != 0) throw std::runtime_error("count: " + std::string(abi::error_name(r.status)));
    return dec<std::int64_t>(r.payload);
}

bool on(Mesh& net, Mesh::Host& host, const rt::PagletId& id, Mesh::Host& other) {
    return net.settle([&] { return host.f->runtime->info(id).has_value() && !other.f->runtime->info(id); }, 400);
}

}  // namespace

PAGLETS_TEST("movement: dispatch between hosts; repeated moves send only changed pages (WP12 exit)") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const std::string module = a.f->module("conformance.wasm");
    const auto id = start(net, a, module, '1');
    CHECK_EQ(count(*a.f, id), 1);
    CHECK_EQ(a.f->code(id, "ballast", Cmd{.ms = 1024 * 1024}), 1024 * 1024);  // 16 unchanging pages

    // The paglet dispatches itself to b (by host name).
    CHECK_EQ(a.f->code(id, "lifecycle", Cmd{.name = "b", .code = dispatch_op}), 0);
    REQUIRE(on(net, b, id, a));
    CHECK_EQ(count(*b.f, id), 2);  // its memory came along
    auto j = b.f->journal(id);
    CHECK(contains(j, "event:dispatching:b"));
    CHECK(contains(j, "event:arrived:" + key_id(a.key) + ":0"));
    CHECK(b.f->runtime->modules().contains(module));  // fetched from a on the way
    CHECK(b.node->passport(id).has_value());
    CHECK(!a.node->passport(id).has_value());
    const auto first = a.node->move_stats();
    CHECK_EQ(first.moves_out, 1u);
    CHECK(first.pages_sent > 0);
    CHECK_EQ(b.node->move_stats().pages_received, first.pages_sent);

    // Back to a (the host sends it): a still has the pages it sent, so only
    // pages that changed travel.
    REQUIRE_OK(b.node->dispatch(id, "a"));
    REQUIRE(on(net, a, id, b));
    CHECK_EQ(count(*a.f, id), 3);
    const auto back = b.node->move_stats();
    CHECK(back.pages_sent > 0);
    CHECK(back.pages_sent < first.pages_sent);
    CHECK(a.node->move_stats().pages_reused > 0);
    CHECK_EQ(a.node->move_stats().pages_reused + a.node->move_stats().pages_received, first.pages_sent);

    // And to b again: b kept the pages it received.
    REQUIRE_OK(a.node->dispatch(id, key_id(b.key)));
    REQUIRE(on(net, b, id, a));
    CHECK_EQ(count(*b.f, id), 4);
    CHECK(a.node->move_stats().pages_sent - first.pages_sent < first.pages_sent);
    CHECK_EQ(b.node->move_stats().moves_in, 2u);
}

PAGLETS_TEST("movement: clones on another host; messages between hosts") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const std::string module = a.f->module("conformance.wasm");
    const auto original = start(net, a, module, '2');
    CHECK_EQ(count(*a.f, original), 1);
    const auto h = static_cast<std::int32_t>(
        a.f->code(original, "lifecycle", Cmd{.name = "b", .payload = text("twin"), .code = clone_op}));
    REQUIRE(h > 1);
    const auto clone = a.f->target_of(original, h);
    REQUIRE(net.settle([&] { return b.f->runtime->info(clone).has_value(); }, 400));
    CHECK(contains(b.f->journal(clone), "event:cloned:twin"));
    CHECK_EQ(count(*b.f, clone), 2);
    CHECK_EQ(count(*a.f, original), 2);
    // The clone's passport is the original's, extended by a's host key.
    auto passport = b.node->passport(clone);
    REQUIRE(passport.has_value());
    REQUIRE(passport->links().size() == 1u);
    CHECK(passport->links()[0].kind == "clone" && passport->links()[0].host == a.key);
    // The original talks to its clone on b.
    CHECK(a.f->code(original, "request", Cmd{.handle = h, .name = "echo", .payload = text("hello b")}) > 0);
    REQUIRE(net.settle([&] { return contains(a.f->journal(original), "reply:ok:hello b"); }, 400));
    CHECK(net.sent["deliver"] >= 2);
    // The clone dispatches back to a; the original's endpoint still names b,
    // whose tombstone forwards.
    REQUIRE_OK(b.node->dispatch(clone, "a"));
    REQUIRE(on(net, a, clone, b));
    CHECK(a.f->code(original, "request", Cmd{.handle = h, .name = "echo", .payload = text("hello a")}) > 0);
    REQUIRE(net.settle([&] { return contains(a.f->journal(original), "reply:ok:hello a"); }, 400));
}

PAGLETS_TEST("movement: grants follow the paglet where they cover the destination") {
    const fs::path root = fs::temp_directory_path() / ("paglets-move-" + std::to_string(std::random_device{}()));
    fs::create_directories(root / "docs");
    std::ofstream(root / "docs" / "report.txt") << "numbers";
    ps::ServicesConfig sc;
    sc.roots["data"] = root;
    Mesh net;
    auto& a = net.add_host("a", sc);
    auto& b = net.add_host("b", sc);
    net.enroll_all();
    const std::string module = a.f->module("conformance.wasm");
    const auto id = start(net, a, module, '3');
    const Principal principal{id, net.owner.id(), module, "roaming"};
    const std::int64_t expires = unix_ms() + 600'000;
    // One grant for every host, one for a only (admins approve on a).
    net.admin_record("grant", data::grant(principal, Item{"files", {"read"}, "data", "docs"}, HostSelector{}, expires,
                                          std::nullopt, std::nullopt, std::nullopt));
    net.admin_record("grant",
                     data::grant(principal, Item{"files", {"read", "write"}, "data", ""}, HostSelector{{a.key}, {}},
                                 expires, std::nullopt, std::nullopt, std::nullopt));
    REQUIRE(net.settle([&] { return net.converged(); }));
    a.node->sync();
    REQUIRE(net.settle([&] {
        auto caps = a.f->runtime->capabilities(id);
        return std::ranges::count_if(caps, [](const auto& e) { return e.second.grant.has_value(); }) == 2;
    }));
    std::map<std::int32_t, std::string> granted;  // handle -> grant
    for (const auto& [handle, cap] : a.f->runtime->capabilities(id)) {
        if (cap.grant) granted[handle] = *cap.grant;
    }
    REQUIRE_OK(a.node->dispatch(id, "b"));
    REQUIRE(on(net, b, id, a));
    // On b: the grant for every host is back under the same handle; the
    // other one is reported lost.
    auto j = b.f->journal(id);
    CHECK(contains(j, "event:arrived:" + key_id(a.key) + ":1"));
    const auto caps = b.f->runtime->capabilities(id);
    int recreated = 0;
    for (const auto& [handle, grant] : granted) {
        auto it = std::ranges::find_if(caps, [&](const auto& e) { return e.first == handle; });
        if (it != caps.end()) {
            ++recreated;
            CHECK(it->second.grant == grant);
            CHECK(it->second.resource == "data/docs");
            CHECK(it->second.ops == std::vector<std::string>{"read"});
        }
    }
    CHECK_EQ(recreated, 1);
    CHECK(std::ranges::any_of(j, [](const std::string& e) { return e.starts_with("lost:handle "); }));
    std::error_code ec;
    fs::remove_all(root, ec);
}

PAGLETS_TEST("movement: failed transfers leave the paglet on the source") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    a.node->set_move_timeout(100ms);
    b.node->set_move_timeout(100ms);
    const std::string module = a.f->module("conformance.wasm");
    const auto id = start(net, a, module, '4');
    CHECK_EQ(count(*a.f, id), 1);
    // Reads the journal only once the move has failed (while a paglet moves,
    // requests to it wait, and this thread carries the frames).
    auto failed = [&](std::uint64_t n, const std::string& expected) {
        if (!net.settle([&] { return a.node->move_stats().moves_failed >= n; }, 400)) return false;
        // The paglet is here again; its report arrives with its next delivery.
        const auto until = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < until) {
            if (std::ranges::count_if(a.f->journal(id), [&](const std::string& e) {
                    return e.starts_with("move_failed:") && e.find(expected) != std::string::npos;
                }) > 0) {
                return true;
            }
            std::this_thread::sleep_for(10ms);
        }
        return false;
    };
    // No such host.
    CHECK_EQ(a.f->code(id, "lifecycle", Cmd{.name = "nowhere", .code = dispatch_op}), 0);
    CHECK(failed(1, "no host matches nowhere"));
    // A host that does not answer.
    net.filter = [&](const PublicKey&, const PublicKey& to, Bytes& frame) {
        return !(to == b.key && frame_type(frame) == "move-offer");
    };
    REQUIRE_OK(a.node->dispatch(id, "b"));
    CHECK(failed(2, "no answer in time"));
    // A page damaged on the way: b refuses the paglet.
    net.filter = [&](const PublicKey&, const PublicKey&, Bytes& frame) {
        if (frame_type(frame) != "move-pages") return true;
        auto v = decode(frame);
        Map m = *v->as_map();
        for (auto& [k, value] : m) {
            if (k != "p") continue;
            Array parts = *value.as_array();
            Array first = *parts[0].as_array();
            Bytes data = *first[1].as_bin();
            data[data.size() / 2] ^= 0x55;
            first[1] = Value(std::move(data));
            parts[0] = Value(std::move(first));
            value = Value(std::move(parts));
        }
        frame = encode(Value(std::move(m)));
        return true;
    };
    REQUIRE_OK(a.node->dispatch(id, "b"));
    CHECK(failed(3, "page"));
    net.filter = nullptr;
    // Through all of it the paglet stayed, with its state; b has nothing.
    CHECK_EQ(count(*a.f, id), 2);
    CHECK(!b.f->runtime->info(id).has_value());
    CHECK(a.node->passport(id).has_value());
    CHECK_EQ(a.node->move_stats().moves_failed, 3u);
    // Paglets without a passport cannot leave.
    auto local = a.f->runtime->create(module, rt::CreateOptions{{}, rt::TrustClass::roaming, net.owner.id()});
    REQUIRE_OK(local);
    REQUIRE_OK(a.node->dispatch(*local, "b"));
    REQUIRE(net.settle([&] { return a.node->move_stats().moves_failed >= 4; }, 400));
    CHECK(std::ranges::any_of(a.f->journal(*local),
                              [](const std::string& e) { return e.find("no passport") != std::string::npos; }));
    CHECK(!a.node->dispatch(id, "b?retries=zero"));  // malformed ticket
}

PAGLETS_TEST("movement: tickets choose among hosts, retry, and arrive inactive") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    net.enroll_all();
    net.admin_record("host-enroll", data::host_enroll(b.key, "b", {"gpu"}));
    net.admin_record("host-enroll", data::host_enroll(c.key, "c", {"gpu"}));
    REQUIRE(net.settle([&] { return net.converged(); }));
    a.node->set_move_timeout(100ms);
    const std::string module = a.f->module("conformance.wasm");
    const auto id = start(net, a, module, '5');
    CHECK_EQ(count(*a.f, id), 1);
    // b is silent; the ticket allows a second host with the label.
    net.filter = [&](const PublicKey&, const PublicKey& to, Bytes& frame) {
        return !(to == b.key && frame_type(frame) == "move-offer");
    };
    REQUIRE_OK(a.node->dispatch(id, "label:gpu?retries=2&arrival=inactive"));
    REQUIRE(on(net, c, id, a));
    net.filter = nullptr;
    // It arrived inactive: nothing ran until a message came.
    auto info = c.f->runtime->info(id);
    REQUIRE(info.has_value());
    CHECK(info->state == rt::PagletState::inactive);
    CHECK_EQ(info->handled, 0u);
    CHECK_EQ(count(*c.f, id), 2);
    auto j = c.f->journal(id);
    CHECK(contains(j, "event:arrived:" + key_id(a.key) + ":0"));
}

PAGLETS_TEST("movement: a lost commit is recovered by asking the source") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    b.node->set_move_timeout(100ms);
    const std::string module = a.f->module("conformance.wasm");
    const auto id = start(net, a, module, '6');
    bool dropped = false;
    net.filter = [&](const PublicKey&, const PublicKey&, Bytes& frame) {
        if (!dropped && frame_type(frame) == "move-commit") {
            dropped = true;
            return false;
        }
        return true;
    };
    REQUIRE_OK(a.node->dispatch(id, "b"));
    REQUIRE(on(net, b, id, a));
    CHECK(dropped);
    CHECK(net.sent["move-query"] >= 1);
    CHECK_EQ(count(*b.f, id), 1);
}

PAGLETS_TEST("movement over HTTPS channels: dispatch there and back") {
    SigningKey admin = SigningKey::generate();
    SigningKey owner = SigningKey::generate();
    auto genesis = make_genesis("network moves", {&admin}, {}, 1);
    REQUIRE_OK(genesis);
    NetHost a(*genesis);
    NetHost b(*genesis);
    a.node->add_seed(b.node->host());
    b.node->add_seed(a.node->host());
    a.transport->set_address(b.node->host(), b.transport->url());
    auto admin_record = [&](const std::string& type, Map d) {
        auto r = a.node->draft(type, std::move(d));
        REQUIRE_OK(r);
        r->sign(admin);
        REQUIRE_OK(a.node->submit(*r));
    };
    admin_record("host-enroll", data::host_enroll(a.node->host(), "a", {}));
    admin_record("host-enroll", data::host_enroll(b.node->host(), "b", {}));
    admin_record("owner-enroll", data::owner_enroll(owner.public_key(), "olga", {}));
    auto eventually = [&](const std::function<bool()>& done) {
        for (int i = 0; i < 1000; ++i) {
            if (done()) return true;
            a.node->tick();
            b.node->tick();
            std::this_thread::sleep_for(10ms);
        }
        return done();
    };
    REQUIRE(eventually([&] { return a.node->ledger_digest() == b.node->ledger_digest(); }));
    const std::string module = a.f->module("conformance.wasm");
    const std::int64_t now = unix_ms();
    auto passport = Passport::issue(owner, genesis->id(), *parse_key_id(module), paglet_id('7'), Value(), now - 1000,
                                    now + 600'000);
    REQUIRE_OK(passport);
    auto id = a.node->create(module, *passport);
    REQUIRE_OK(id);
    CHECK_EQ(count(*a.f, *id), 1);
    CHECK_EQ(a.f->code(*id, "ballast", Cmd{.ms = 1024 * 1024}), 1024 * 1024);
    REQUIRE_OK(a.node->dispatch(*id, "b"));
    REQUIRE(eventually([&] { return b.f->runtime->info(*id).has_value() && !a.f->runtime->info(*id); }));
    CHECK_EQ(count(*b.f, *id), 2);
    REQUIRE_OK(b.node->dispatch(*id, "a"));
    REQUIRE(eventually([&] { return a.f->runtime->info(*id).has_value() && !b.f->runtime->info(*id); }));
    CHECK_EQ(count(*a.f, *id), 3);
    CHECK(b.node->move_stats().pages_sent < a.node->move_stats().pages_sent);
}
