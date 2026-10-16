// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Moving paglets between hosts (WP12, planning/cpp-networking.md, section
// 6). The exit: dispatch and clone between two hosts; repeated moves
// transfer only changed pages; grants follow the paglet; failed transfers
// leave the paglet on the source.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <explorer_msgs.hpp>
#include <paglets/wire/reflect.hpp>

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

std::string move_failure(Mesh& net, Mesh::Host& host, const rt::PagletId& id) {
    REQUIRE(net.settle([&] { return host.node->move_stats().moves_failed > 0; }, 1500));
    std::string reason;
    REQUIRE(net.settle([&] {
        for (const auto& entry : host.f->journal(id)) {
            if (entry.starts_with("move_failed:")) {
                reason = entry.substr(std::string_view("move_failed:").size());
                return true;
            }
        }
        return false;
    }, 400));
    return reason;
}

}  // namespace

PAGLETS_TEST("movement addresses: canonical keys cannot be shadowed by host names") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    auto& alias = b.key < c.key ? b : c;
    auto& target = b.key < c.key ? c : b;
    alias.name = key_id(target.key);
    net.enroll_all();
    const auto id = start(net, a, a.f->module("conformance.wasm"), 'a');
    REQUIRE_OK(a.node->dispatch(id, key_id(target.key)));
    REQUIRE(on(net, target, id, a));
    CHECK(!alias.f->runtime->info(id).has_value());
}

PAGLETS_TEST("movement addresses: ambiguous names and prefixes never select a host") {
    for (bool prefix : {false, true}) {
        Mesh net;
        auto& a = net.add_host("a");
        auto& b = net.add_host("b");
        auto& c = net.add_host("c");
        b.name = prefix ? key_id(c.key).substr(0, 8) : "same-name";
        if (!prefix) c.name = b.name;
        net.enroll_all();
        const auto id = start(net, a, a.f->module("conformance.wasm"), 'b');
        REQUIRE_OK(a.node->dispatch(id, b.name));
        const auto why = move_failure(net, a, id);
        CHECK(why.find("ambiguous host address") != std::string::npos);
        CHECK(a.f->runtime->info(id).has_value());
        CHECK(!b.f->runtime->info(id).has_value());
        CHECK(!c.f->runtime->info(id).has_value());
    }
}

PAGLETS_TEST("movement addresses: unregistered keys cannot fall back to host names") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    const auto absent = SigningKey::generate();
    b.name = key_id(absent.public_key());
    net.enroll_all();
    const auto id = start(net, a, a.f->module("conformance.wasm"), 'c');
    REQUIRE_OK(a.node->dispatch(id, b.name));
    CHECK(!move_failure(net, a, id).empty());
    CHECK(a.f->runtime->info(id).has_value());
    CHECK(!b.f->runtime->info(id).has_value());
}

PAGLETS_TEST("movement diagnostics: excluded hosts remain in the final failed-move report") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    net.enroll_all();
    net.admin_record("policy-rule", data::policy_rule({"load on a and b", Decision::allow, "server-info", {"load"},
        Match{.hosts = HostSelector{{a.key, b.key}, {}}}, std::nullopt, std::nullopt, 0}));
    REQUIRE(net.settle([&] { return net.converged(); }));
    a.node->set_move_timeout(100ms);
    const auto module = a.f->module("conformance.wasm");
    const auto now = unix_ms();
    auto passport = Passport::issue(net.owner, net.genesis->id(), *parse_key_id(module), "diagnostic-manifest",
        manifest_value({Item{"server-info", {"load"}, {}, {}}}), now - 1000, now + 600000);
    REQUIRE_OK(passport);
    const auto id = a.node->create(module, *passport);
    REQUIRE_OK(id);
    net.filter = [&](const PublicKey&, const PublicKey& to, Bytes& frame) {
        return !(to == b.key && frame_type(frame) == "move-offer");
    };
    REQUIRE_OK(a.node->dispatch(*id, "any?retries=3"));
    const auto why = move_failure(net, a, *id);
    CHECK(why.find("no host took the paglet") != std::string::npos);
    CHECK(why.find("host c: the manifest's server-info access is denied there") != std::string::npos);
    CHECK(why.find("no answer in time") != std::string::npos);
    CHECK(a.f->runtime->info(*id).has_value());
    CHECK(!b.f->runtime->info(*id).has_value());
    CHECK(!c.f->runtime->info(*id).has_value());
}

PAGLETS_TEST("movement diagnostics: retry limits name the host that was not tried") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    net.enroll_all();
    a.node->set_move_timeout(100ms);
    std::vector<PublicKey> attempted;
    net.filter = [&](const PublicKey&, const PublicKey& to, Bytes& frame) {
        if (frame_type(frame) != "move-offer") return true;
        attempted.push_back(to);
        return false;
    };
    const auto id = start(net, a, a.f->module("conformance.wasm"), '8');
    REQUIRE_OK(a.node->dispatch(id, "any?retries=1"));
    const auto why = move_failure(net, a, id);
    REQUIRE(attempted.size() == 1u);
    const auto& skipped = attempted.front() == b.key ? c : b;
    CHECK(why.find("host " + skipped.name + ": not tried (retry limit 1)") != std::string::npos);
    CHECK(why.find("no answer in time") != std::string::npos);
    CHECK(a.f->runtime->info(id).has_value());
}

PAGLETS_TEST("movement diagnostics: oversized peer reasons are omitted within a bounded report") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const auto id = start(net, a, a.f->module("conformance.wasm"), '9');
    net.filter = [&](const PublicKey& from, const PublicKey& to, Bytes& frame) {
        if (from != a.key || to != b.key || frame_type(frame) != "move-offer") return true;
        const auto value = decode(frame);
        REQUIRE(value && value->as_map());
        const auto request = Fields{*value->as_map()}.integer("r");
        REQUIRE(request.has_value());
        a.node->receive(b.key, encode(Value(Map{{"t", Value("move-refused")}, {"r", Value(*request)},
                                               {"e", Value(std::string(20000, 'x'))}})));
        return false;
    };
    REQUIRE_OK(a.node->dispatch(id, "b"));
    const auto why = move_failure(net, a, id);
    CHECK(why.size() <= 8192u);
    CHECK(why.find("1 more host reason(s) omitted") != std::string::npos);
    CHECK(a.f->runtime->info(id).has_value());
}

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
    REQUIRE(net.settle([&] { return a.node->move_stats().arrival_acks == 1; }));
    const auto first = a.node->move_stats();
    CHECK_EQ(first.moves_out, 1u);
    CHECK(first.pages_sent > 0);
    CHECK(first.capture_ns > 0);
    CHECK(first.ready_ns > 0);
    CHECK(first.arrival_ack_ns >= first.capture_ns);
    CHECK_EQ(first.arrival_failures, 0u);
    CHECK(b.node->move_stats().prepare_ns > 0);
    CHECK(a.node->move_stats().destination_activation_ns > 0);
    CHECK(b.node->move_stats().commit_ns > 0);
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

PAGLETS_TEST("movement: arrival receipts authenticate their host and are counted only once") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    Bytes receipt;
    net.filter = [&](const PublicKey&, const PublicKey&, Bytes& frame) {
        if (frame_type(frame) == "move-arrived") { receipt = frame; return false; }
        return true;
    };
    const auto id = start(net, a, a.f->module("conformance.wasm"), '1');
    REQUIRE_OK(a.node->dispatch(id, "b"));
    REQUIRE(net.settle([&] { return !receipt.empty(); }, 1000));
    CHECK_EQ(a.node->move_stats().arrival_acks, 0u);
    a.node->receive(a.key, receipt);  // an enrolled host that is not the destination
    CHECK_EQ(a.node->move_stats().arrival_acks, 0u);
    a.node->receive(b.key, receipt);
    CHECK_EQ(a.node->move_stats().arrival_acks, 1u);
    a.node->receive(b.key, receipt);
    CHECK_EQ(a.node->move_stats().arrival_acks, 1u);
    CHECK_EQ(a.node->move_stats().arrival_failures, 0u);
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

PAGLETS_TEST("mesh followups: narrowed grant capabilities retain identity and revocation ancestry after moving") {
    const auto root = fs::temp_directory_path() / ("paglets-narrowed-move-" + std::to_string(std::random_device{}()));
    fs::create_directories(root / "docs" / "reports");
    ps::ServicesConfig sc;
    sc.roots["data"] = root;
    {
        Mesh net;
        auto& a = net.add_host("a", sc);
        auto& b = net.add_host("b", sc);
        net.enroll_all();
        const auto module = a.f->module("conformance.wasm");
        const auto id = start(net, a, module, '9');
        net.admin_record("grant", data::grant(Principal{id, net.owner.id(), module, "roaming"},
            Item{"files", {"read", "write"}, "data", ""}, HostSelector{}, unix_ms() + 600'000,
            std::nullopt, std::nullopt, std::nullopt));
        REQUIRE(net.settle([&] { return net.converged(); }));
        a.node->sync();
        REQUIRE(net.settle([&] {
            return std::ranges::any_of(a.f->runtime->capabilities(id), [](const auto& c) { return c.second.grant.has_value(); });
        }));
        const auto caps = a.f->runtime->capabilities(id);
        const auto grant = std::ranges::find_if(caps, [](const auto& c) { return c.second.grant.has_value(); });
        REQUIRE(grant != caps.end());
        const auto child = static_cast<std::int32_t>(a.f->code(id, "derive", Cmd{.handle = grant->first,
            .spec = enc(abi::DeriveSpec{.ops = std::vector<std::string>{"read"}, .expires_ms = 60'000,
                .uses = 3, .transferable = false, .badge = "reports", .path = "docs"})}));
        REQUIRE(child > 0);
        const auto leaf = static_cast<std::int32_t>(a.f->code(id, "derive", Cmd{.handle = child,
            .spec = enc(abi::DeriveSpec{.expires_ms = 30'000, .uses = 2, .path = "reports"})}));
        REQUIRE(leaf > 0);
        const auto before = a.f->runtime->capabilities(id);
        REQUIRE_OK(a.node->dispatch(id, "b"));
        REQUIRE(on(net, b, id, a));
        const auto after = b.f->runtime->capabilities(id);
        for (auto handle : {child, leaf}) {
            const auto old = std::ranges::find_if(before, [&](const auto& c) { return c.first == handle; });
            const auto now = std::ranges::find_if(after, [&](const auto& c) { return c.first == handle; });
            REQUIRE(old != before.end() && now != after.end());
            CHECK(now->second.resource == old->second.resource);
            CHECK(now->second.ops == std::vector<std::string>{"read"});
            CHECK(now->second.expires == old->second.expires);
            CHECK(now->second.uses_left == old->second.uses_left);
            CHECK(now->second.transferable == old->second.transferable);
            CHECK(now->second.badge == old->second.badge);
            CHECK(now->second.id == old->second.id);
            CHECK(now->second.lineage == old->second.lineage);
        }
        const auto ancestor = std::ranges::find_if(before, [&](const auto& c) { return c.first == child; });
        REQUIRE(ancestor != before.end());
        // Apply a known revocation on the destination. This checks preserved
        // identity/ancestry, not automatic replication of local revocations.
        b.f->runtime->revoke_capability(ancestor->second.id);
        for (auto handle : {child, leaf}) {
            const auto inspected = b.f->cmd(id, "inspect", Cmd{.handle = handle});
            REQUIRE(inspected.status == 0);
            CHECK(dec<abi::CapInfo>(inspected.payload).revoked);
            CHECK_EQ(b.f->code(id, "derive", Cmd{.handle = handle, .spec = enc(abi::DeriveSpec{})}), abi::revoked);
        }
        const auto parent = b.f->cmd(id, "inspect", Cmd{.handle = grant->first});
        REQUIRE(parent.status == 0);
        CHECK(!dec<abi::CapInfo>(parent.payload).revoked);
    }
    fs::remove_all(root);
}

PAGLETS_TEST("mesh followups: an out-of-scope grant resource is lost rather than widened on arrival") {
    const auto root = fs::temp_directory_path() / ("paglets-scope-move-" + std::to_string(std::random_device{}()));
    fs::create_directories(root / "docs");
    ps::ServicesConfig sc;
    sc.roots["data"] = root;
    {
        Mesh net;
        auto& a = net.add_host("a", sc);
        auto& b = net.add_host("b", sc);
        net.enroll_all();
        const auto module = a.f->module("conformance.wasm");
        const auto id = start(net, a, module, '8');
        net.admin_record("grant", data::grant(Principal{id, net.owner.id(), module, "roaming"},
            Item{"files", {"read"}, "data", "docs"}, HostSelector{}, unix_ms() + 600'000,
            std::nullopt, std::nullopt, std::nullopt));
        REQUIRE(net.settle([&] { return net.converged(); }));
        a.node->sync();
        REQUIRE(net.settle([&] {
            return std::ranges::any_of(a.f->runtime->capabilities(id), [](const auto& c) { return c.second.grant.has_value(); });
        }));
        const auto caps = a.f->runtime->capabilities(id);
        const auto grant = std::ranges::find_if(caps, [](const auto& c) { return c.second.grant.has_value(); });
        REQUIRE(grant != caps.end());
        // The host API can construct state a guest cannot derive. Arrival
        // must refuse such a mismatch instead of substituting the full grant.
        auto outside = grant->second;
        outside.resource = "data/docs-other";
        outside.id = "out-of-scope-test-cap";
        const auto invalid = a.f->runtime->add_capability(id, std::move(outside));
        REQUIRE_OK(invalid);
        REQUIRE_OK(a.node->dispatch(id, "b"));
        REQUIRE(on(net, b, id, a));
        const auto after = b.f->runtime->capabilities(id);
        CHECK(std::ranges::none_of(after, [&](const auto& c) { return c.first == *invalid; }));
        CHECK(std::ranges::any_of(after, [&](const auto& c) { return c.first == grant->first && c.second.resource == "data/docs"; }));
        CHECK(contains(b.f->journal(id), "event:arrived:" + key_id(a.key) + ":1"));
    }
    fs::remove_all(root);
}

PAGLETS_TEST("mesh followups: compressed page batches stay within one MB and preserve noisy memory") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const auto id = start(net, a, a.f->module("conformance.wasm"), '7');
    constexpr std::int64_t bytes = 20 * 65536;
    CHECK_EQ(a.f->code(id, "ballast", Cmd{.code = 1, .ms = bytes}), bytes);
    const auto before = dec<std::int64_t>(a.f->call(id, "ballast_digest").payload);
    std::vector<std::size_t> batches;
    net.filter = [&](const PublicKey&, const PublicKey&, Bytes& frame) {
        if (frame_type(frame) != "move-pages") return true;
        const auto decoded = decode(frame);
        REQUIRE_OK(decoded);
        const auto parts = Fields{*decoded->as_map()}.array("p");
        REQUIRE(parts != nullptr);
        std::size_t total = 0;
        for (const auto& part : *parts) {
            const auto items = part.as_array();
            REQUIRE(items != nullptr && items->size() == 2);
            REQUIRE((*items)[1].as_bin() != nullptr);
            total += (*items)[1].as_bin()->size();
        }
        batches.push_back(total);
        CHECK(total <= 1024u * 1024u);
        return true;
    };
    REQUIRE_OK(a.node->dispatch(id, "b"));
    // This sends over a MB of distinct pages, unlike the small/deduplicated
    // moves in on(). Allow instrumented worker startup and image restoration.
    REQUIRE(net.settle([&] { return b.f->runtime->info(id).has_value() && !a.f->runtime->info(id); }, 2500));
    REQUIRE(batches.size() >= 2);
    std::size_t total = 0;
    for (const auto size : batches) total += size;
    CHECK(total > 1024u * 1024u);
    CHECK_EQ(dec<std::int64_t>(b.f->call(id, "ballast_digest").payload), before);
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
    CHECK(failed(1, "no enrolled host matches nowhere"));
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

PAGLETS_TEST("movement: data residency keeps paglets that read a root where its content may go") {
    const fs::path root = fs::temp_directory_path() / ("paglets-residency-" + std::to_string(std::random_device{}()));
    fs::create_directories(root);
    std::ofstream(root / "patients.txt") << "confidential";
    ps::ServicesConfig sc;
    sc.roots["clinic"] = root;
    Mesh net;
    auto& a = net.add_host("a", sc);
    auto& b = net.add_host("b", sc);
    auto& c = net.add_host("c", sc);
    net.enroll_all();
    net.admin_record("host-enroll", data::host_enroll(a.key, "a", {"site"}));
    net.admin_record("host-enroll", data::host_enroll(b.key, "b", {"site"}));
    // The content of `clinic` goes only to hosts labelled `site`.
    net.admin_record("root-residency", data::root_residency("clinic", "hosts", HostSelector{{}, {"site"}}));
    REQUIRE(net.settle([&] { return net.converged(); }));
    REQUIRE(a.node->state().residency.contains("clinic"));

    const std::string module = a.f->module("explorer.wasm");
    const auto reader = start(net, a, module, '6');
    const auto idle = start(net, a, module, '7');
    // The reader reads a file of the root: it carries the root's mark.
    auto cap = a.services->directory_capability("clinic", "", {"read"});
    REQUIRE_OK(cap);
    auto dir = a.f->runtime->add_capability(reader, std::move(*cap));
    REQUIRE_OK(dir);
    auto r = a.f->runtime->call(reader, "read_file", paglets::wire::to_msgpack(explorer::Explore{*dir, "patients.txt"}), 20s);
    REQUIRE(r.status == 0);
    CHECK(dec<std::string>(r.payload) == "content:confidential");
    CHECK(a.f->runtime->marks(reader) == std::vector<std::string>{"root:clinic"});
    CHECK(a.f->runtime->marks(idle).empty());

    // Not to c (no label); the paglet that read nothing goes anywhere.
    REQUIRE_OK(a.node->dispatch(reader, "c"));
    REQUIRE(net.settle([&] { return a.node->move_stats().moves_failed == 1u; }, 400));
    CHECK(a.f->runtime->info(reader).has_value());
    REQUIRE_OK(a.node->dispatch(idle, "c"));
    REQUIRE(on(net, c, idle, a));
    // To b (labelled): the mark travels along.
    REQUIRE_OK(a.node->dispatch(reader, "label:site?retries=3"));
    REQUIRE(on(net, b, reader, a));
    CHECK(b.f->runtime->marks(reader) == std::vector<std::string>{"root:clinic"});

    // host-only: the content stays where it was read; the reader stays on b.
    net.admin_record("root-residency", data::root_residency("clinic", "host-only"));
    REQUIRE(net.settle([&] { return net.converged(); }));
    REQUIRE_OK(b.node->dispatch(reader, "a"));
    REQUIRE(net.settle([&] { return b.node->move_stats().moves_failed == 1u; }, 400));
    CHECK(b.f->runtime->info(reader).has_value());
    // none lifts the rule.
    net.admin_record("root-residency", data::root_residency("clinic", "none"));
    REQUIRE(net.settle([&] { return net.converged(); }));
    CHECK(!b.node->state().residency.contains("clinic"));
    REQUIRE_OK(b.node->dispatch(reader, "c"));
    REQUIRE(on(net, c, reader, b));
    std::error_code ec;
    fs::remove_all(root, ec);
}
