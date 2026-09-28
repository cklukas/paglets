// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Location and pinning (WP13, planning/cpp-location.md). The exit: a paglet
// moving continuously between hosts can be located and pinned, also after
// any one host is shut down; while pinned, its dispatch returns `pinned`
// until the pin ends.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <paglets/services/locator.hpp>
#include <paglets/wire/reflect.hpp>

#include <filesystem>
#include <random>

using namespace paglets::test;
namespace fs = std::filesystem;
namespace loc = paglets::services::locator;
namespace wire = paglets::wire;
using Located = paglets::node::Node::Located;

namespace {

constexpr auto dispatch_op = static_cast<std::int32_t>(abi::LifecycleOp::dispatch);
constexpr auto clone_op = static_cast<std::int32_t>(abi::LifecycleOp::clone);

rt::PagletId start(Mesh& net, Mesh::Host& host, const std::string& module, char id) {
    auto created = host.node->create(module, net.passport(module, paglet_id(id)));
    REQUIRE_OK(created);
    return *created;
}

// Short intervals, so that failures are noticed within a test.
void fast(Mesh& net) {
    for (auto& h : net.hosts) {
        h->node->set_location_timing({.heartbeat = 50ms,
                                      .host_timeout = 400ms,
                                      .refresh = std::chrono::minutes(5),
                                      .record_ttl = std::chrono::hours(1),
                                      .lookup_timeout = 300ms});
        h->node->set_move_timeout(600ms);
    }
}

Located locate(Mesh& net, Mesh::Host& from, const rt::PagletId& id, std::chrono::milliseconds pin_for = 0ms) {
    const auto l = from.node->locate(id, pin_for, "test");
    REQUIRE(net.settle([&] { return from.node->located(l)->state != Located::State::pending; }, 3000));
    return *from.node->located(l);
}

// The (up) host that holds a paglet, if any.
Mesh::Host* holder(Mesh& net, const rt::PagletId& id) {
    for (auto& h : net.hosts) {
        if (!h->down && h->f->runtime->info(id)) return h.get();
    }
    return nullptr;
}

std::int32_t service_handle(Fixture& f, const rt::PagletId& paglet, std::string_view name) {
    auto info = dec<abi::SelfInfo>(f.call(paglet, "info").payload);
    for (const auto& [n, h] : info.services) {
        if (n == name) return h;
    }
    return 0;
}

// The last reply in a journal: status name, payload, handles that came with it.
struct Answer {
    std::string status;
    Bytes payload;
    std::vector<std::int32_t> caps;
};

std::optional<Answer> last_reply(Fixture& f, const rt::PagletId& id) {
    const auto journal = f.journal(id);
    for (auto it = journal.rbegin(); it != journal.rend(); ++it) {
        if (!it->starts_with("reply:")) continue;
        std::string rest = it->substr(6);
        Answer a;
        const auto colon = rest.find(':');
        a.status = rest.substr(0, colon);
        rest = rest.substr(colon + 1);
        if (const auto c = rest.rfind(":caps="); c != std::string::npos) {
            std::string list = rest.substr(c + 6);
            rest = rest.substr(0, c);
            std::size_t start = 0;
            while (start < list.size()) {
                const auto comma = list.find(',', start);
                a.caps.push_back(std::stoi(list.substr(start, comma - start)));
                start = comma == std::string::npos ? list.size() : comma + 1;
            }
        }
        a.payload = Bytes(rest.begin(), rest.end());
        return a;
    }
    return std::nullopt;
}

// Sends a request from `paglet` to its locator and waits for the answer.
Answer ask_locator(Mesh& net, Fixture& f, const rt::PagletId& paglet, const std::string& op, Bytes payload,
                   std::vector<std::int32_t> lend) {
    const auto before = f.journal(paglet).size();
    const std::int32_t locator = service_handle(f, paglet, "locator");
    REQUIRE(locator > 0);
    const auto id = f.code(paglet, "request",
                           Cmd{.handle = locator, .name = op, .payload = std::move(payload), .lend = std::move(lend)});
    if (id < 0) return Answer{std::string(abi::error_name(static_cast<std::int32_t>(id))), {}, {}};
    REQUIRE(net.settle([&] { return f.journal(paglet).size() > before; }, 3000));
    auto a = last_reply(f, paglet);
    REQUIRE(a.has_value());
    return *a;
}

template <class T>
T decode_as(const Bytes& b) {
    T v{};
    REQUIRE(wire::from_msgpack(b, v));
    return v;
}

void allow_pins(Mesh& net, std::int64_t max_ms) {
    net.admin_record("policy-rule",
                     data::policy_rule({"pins", Decision::allow, "locator", {"pin"}, {}, std::nullopt, max_ms, 0}));
    REQUIRE(net.settle([&] { return net.converged(); }, 400));
}

// Records of a paglet on the hosts responsible for it, as `host` sees them.
int records_naming(Mesh& net, const std::vector<PublicKey>& responsible, const rt::PagletId& id, const PublicKey& host,
                   std::uint64_t moves) {
    int n = 0;
    for (const auto& k : responsible) {
        auto* h = net.find(k);
        if (h == nullptr || h->down) continue;
        auto r = h->node->location_record(id);
        if (r && r->host == host && r->moves == moves) ++n;
    }
    return n;
}

}  // namespace

PAGLETS_TEST("location: records on responsible hosts; every move updates a majority") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    auto& d = net.add_host("d");
    net.enroll_all();
    const std::string module = a.f->module("conformance.wasm");
    const auto id = start(net, a, module, '1');

    // Three of the four hosts keep its record; every host agrees which.
    const auto responsible = a.node->responsible(id);
    REQUIRE(responsible.size() == 3u);
    for (auto* h : {&b, &c, &d}) CHECK(h->node->responsible(id) == responsible);
    REQUIRE(net.settle([&] { return records_naming(net, responsible, id, a.key, 0) == 3; }));

    // A move commits only after a majority recorded the destination.
    int at_commit = -1;
    net.filter = [&](const PublicKey&, const PublicKey&, Bytes& frame) {
        if (at_commit < 0 && frame_type(frame) == "move-commit") {
            at_commit = records_naming(net, responsible, id, b.key, 1);
        }
        return true;
    };
    REQUIRE_OK(a.node->dispatch(id, "b"));
    REQUIRE(net.settle([&] { return b.f->runtime->info(id).has_value(); }, 400));
    CHECK(at_commit >= 2);
    REQUIRE(net.settle([&] { return records_naming(net, responsible, id, b.key, 1) == 3; }));

    // Any host finds it, with its move counter and the time of the move.
    for (auto* h : {&a, &b, &c, &d}) {
        const auto l = locate(net, *h, id);
        CHECK(l.state == Located::State::found);
        CHECK(l.host == b.key);
        CHECK_EQ(l.moves, 1u);
        CHECK(l.moved_ms > 0);
    }
    // A clone on another host gets its own record.
    const auto h = static_cast<std::int32_t>(b.f->code(id, "lifecycle", Cmd{.name = "c", .code = clone_op}));
    REQUIRE(h > 1);
    const auto clone = b.f->target_of(id, h);
    REQUIRE(net.settle([&] { return c.f->runtime->info(clone).has_value(); }, 400));
    const auto l = locate(net, d, clone);
    CHECK(l.state == Located::State::found && l.host == c.key);
}

PAGLETS_TEST("location: without records the mesh is asked who holds the paglet") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    auto& d = net.add_host("d");
    net.enroll_all();
    fast(net);
    const std::string module = a.f->module("conformance.wasm");
    // A paglet on a host that does not keep its record, whose records get lost.
    char which = 0;
    for (char ch : std::string("23456789abcdef")) {
        const auto r = a.node->responsible(paglet_id(ch));
        if (std::ranges::find(r, a.key) == r.end()) {
            which = ch;
            break;
        }
    }
    REQUIRE(which != 0);
    net.filter = [](const PublicKey&, const PublicKey&, Bytes& frame) { return frame_type(frame) != "loc-set"; };
    const auto id = start(net, a, module, which);
    net.settle([] { return false; }, 20);
    for (auto* h : {&b, &c, &d}) CHECK(!h->node->location_record(id).has_value());
    const auto l = locate(net, c, id);
    CHECK(l.state == Located::State::found);
    CHECK(l.host == a.key);
    CHECK(net.sent["loc-who"] > 0);
    // Nobody has an unknown paglet.
    const auto none = locate(net, c, paglet_id('f'));
    CHECK(none.state == Located::State::failed);
}

PAGLETS_TEST("location: a failed host's share moves to the next hosts; it catches up when it returns") {
    Mesh net;
    auto& a = net.add_host("a");
    net.add_host("b");
    net.add_host("c");
    net.add_host("d");
    net.add_host("e");
    net.enroll_all();
    fast(net);
    const std::string module = a.f->module("conformance.wasm");
    const auto id = start(net, a, module, '3');
    const auto before = a.node->responsible(id);
    REQUIRE(net.settle([&] { return records_naming(net, before, id, a.key, 0) == 3; }));
    // A responsible host other than a fails.
    Mesh::Host* failed = nullptr;
    for (const auto& k : before) {
        if (k != a.key) failed = net.find(k);
    }
    REQUIRE(failed != nullptr);
    failed->down = true;
    REQUIRE(net.settle(
        [&] {
            for (auto& h : net.hosts) {
                if (h->down) continue;
                const auto live = h->node->live_hosts();
                if (std::ranges::find(live, failed->key) != live.end()) return false;
            }
            return true;
        },
        2000));
    const auto after = a.node->responsible(id);
    CHECK(std::ranges::find(after, failed->key) == after.end());
    // The host that took over the share has the record.
    REQUIRE(net.settle([&] { return records_naming(net, after, id, a.key, 0) == 3; }));

    // Moves still commit, and every host that is up finds the paglet.
    REQUIRE_OK(a.node->dispatch(id, "any"));
    REQUIRE(net.settle([&] { return holder(net, id) != nullptr && holder(net, id) != &a; }, 2000));
    Mesh::Host* now = holder(net, id);
    for (auto& h : net.hosts) {
        if (h->down) continue;
        const auto l = locate(net, *h, id);
        CHECK(l.state == Located::State::found);
        CHECK(l.host == now->key);
    }
    // Back again: its stale record loses against the newer ones.
    failed->down = false;
    REQUIRE(net.settle(
        [&] {
            const auto live = a.node->live_hosts();
            return std::ranges::find(live, failed->key) != live.end() && failed->node->live_hosts().size() == 5u;
        },
        2000));
    const auto l = locate(net, *failed, id);
    CHECK(l.state == Located::State::found);
    CHECK(l.host == now->key);
}

PAGLETS_TEST("locator: paglets locate and pin paglets; a pinned paglet stays; release") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    allow_pins(net, 60'000);
    const std::string module = a.f->module("conformance.wasm");
    const auto seeker = start(net, a, module, '4');
    // The seeker's clone on b is the paglet it looks for.
    const auto h = static_cast<std::int32_t>(a.f->code(seeker, "lifecycle", Cmd{.name = "b", .code = clone_op}));
    REQUIRE(h > 1);
    const auto target = a.f->target_of(seeker, h);
    REQUIRE(net.settle([&] { return b.f->runtime->info(target).has_value(); }, 400));

    auto answer = ask_locator(net, *a.f, seeker, "locate", wire::to_msgpack(loc::LocateRequest{}), {h});
    REQUIRE(answer.status == "ok");
    const auto where = decode_as<loc::Location>(answer.payload);
    CHECK(where.paglet == target);
    CHECK(where.host == key_id(b.key));
    CHECK(where.host_name == "b");
    CHECK_EQ(where.moves, 1);

    // Pinned for at most what the policy allows.
    const std::int64_t asked_at = unix_ms();
    answer =
        ask_locator(net, *a.f, seeker, "locate_and_pin", wire::to_msgpack(loc::PinRequest{600'000, "report"}), {h});
    REQUIRE(answer.status == "ok");
    REQUIRE(answer.caps.size() == 1u);
    const auto pin = decode_as<loc::Pin>(answer.payload);
    CHECK(pin.location.host == key_id(b.key));
    CHECK(pin.expires_ms <= asked_at + 60'000 + 5'000);
    CHECK(pin.expires_ms > asked_at);
    CHECK(b.f->runtime->pinned_until(target) == pin.expires_ms);
    REQUIRE(b.node->pins().size() == 1u);
    CHECK(b.node->pins()[0].holder == seeker);
    CHECK(b.node->pins()[0].reason == "report");
    CHECK(dec<abi::SelfInfo>(b.f->call(target, "info").payload).pinned_until == pin.expires_ms);

    // While pinned it cannot leave: its own dispatch returns `pinned`, and
    // so does a dispatch asked for by its host.
    CHECK_EQ(b.f->code(target, "lifecycle", Cmd{.name = "a", .code = dispatch_op}),
             static_cast<std::int64_t>(abi::pinned));
    auto refused = b.node->dispatch(target, "a");
    REQUIRE(!refused);
    CHECK(refused.error() == "pinned");
    // Other work goes on.
    CHECK_EQ(b.f->code(target, "lifecycle", Cmd{.code = clone_op}) > 1, true);

    // The holder releases it through its pin capability.
    const std::int32_t pin_cap = answer.caps[0];
    answer = ask_locator(net, *a.f, seeker, "release", wire::to_msgpack(loc::ReleaseRequest{}), {pin_cap});
    REQUIRE(answer.status == "ok");
    CHECK(decode_as<loc::ReleaseReply>(answer.payload).released);
    CHECK(b.node->pins().empty());
    CHECK_EQ(b.f->runtime->pinned_until(target), 0);
    // The capability ended with the pin.
    answer = ask_locator(net, *a.f, seeker, "release", wire::to_msgpack(loc::ReleaseRequest{}), {pin_cap});
    CHECK(answer.status == "revoked");
    REQUIRE_OK(b.node->dispatch(target, "a"));
    REQUIRE(net.settle([&] { return a.f->runtime->info(target).has_value(); }, 400));

    // An endpoint without the rights: denied.
    const auto narrow = static_cast<std::int32_t>(
        a.f->code(seeker, "derive",
                  Cmd{.handle = h, .spec = abi::encode(abi::DeriveSpec{.ops = std::vector<std::string>{"echo"}})}));
    REQUIRE(narrow > 1);
    answer = ask_locator(net, *a.f, seeker, "locate", wire::to_msgpack(loc::LocateRequest{}), {narrow});
    CHECK(answer.status == "denied");
    // Nothing lent: invalid.
    answer = ask_locator(net, *a.f, seeker, "locate", wire::to_msgpack(loc::LocateRequest{}), {});
    CHECK(answer.status == "invalid_argument");
}

PAGLETS_TEST("locator: pins need the policy; admins end them; they survive restarts") {
    const fs::path dir = fs::temp_directory_path() / ("paglets-pins-" + std::to_string(std::random_device{}()));
    fs::remove_all(dir);
    {
        Mesh net;
        auto& a = net.add_host("a", {}, dir / "a");
        auto& b = net.add_host("b", {}, dir / "b");
        net.enroll_all();
        const std::string module = a.f->module("conformance.wasm");
        const auto seeker = start(net, a, module, '5');
        const auto h = static_cast<std::int32_t>(a.f->code(seeker, "lifecycle", Cmd{.name = "b", .code = clone_op}));
        const auto target = a.f->target_of(seeker, h);
        REQUIRE(net.settle([&] { return b.f->runtime->info(target).has_value(); }, 400));

        // No rule allows pins: denied.
        auto answer =
            ask_locator(net, *a.f, seeker, "locate_and_pin", wire::to_msgpack(loc::PinRequest{10'000, ""}), {h});
        CHECK(answer.status == "denied");
        allow_pins(net, 3'600'000);
        answer = ask_locator(net, *a.f, seeker, "locate_and_pin", wire::to_msgpack(loc::PinRequest{600'000, "a"}), {h});
        REQUIRE(answer.status == "ok");
        answer = ask_locator(net, *a.f, seeker, "locate_and_pin", wire::to_msgpack(loc::PinRequest{600'000, "b"}), {h});
        REQUIRE(answer.status == "ok");
        CHECK_EQ(b.node->pins().size(), 2u);

        // b restarts: the pins are still there, and still hold the paglet.
        net.restart(b);
        REQUIRE(net.settle([&] { return net.converged(); }, 400));
        REQUIRE(b.f->runtime->info(target).has_value());
        CHECK_EQ(b.node->pins().size(), 2u);
        CHECK(b.f->runtime->pinned_until(target) > unix_ms());
        CHECK(!b.node->dispatch(target, "a"));

        // An admin ends them (from any host: the locator finds the paglet).
        const auto forced = a.node->locate(target);
        REQUIRE(net.settle([&] { return a.node->located(forced)->state != Located::State::pending; }));
        CHECK_EQ(b.node->release_pins(target), 2u);
        CHECK(b.node->pins().empty());
        REQUIRE_OK(b.node->dispatch(target, "a"));
        REQUIRE(net.settle([&] { return a.f->runtime->info(target).has_value(); }, 400));
    }
    fs::remove_all(dir);
}

PAGLETS_TEST("location: messages follow a paglet whose host no longer remembers it") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    net.enroll_all();
    const std::string module = a.f->module("conformance.wasm");
    const auto sender = start(net, a, module, '6');
    const auto h = static_cast<std::int32_t>(a.f->code(sender, "lifecycle", Cmd{.name = "b", .code = clone_op}));
    const auto clone = a.f->target_of(sender, h);
    REQUIRE(net.settle([&] { return b.f->runtime->info(clone).has_value(); }, 400));
    // The clone moves on to c; b forgets where it went (no tombstone).
    REQUIRE_OK(b.node->dispatch(clone, "c"));
    REQUIRE(net.settle([&] { return c.f->runtime->info(clone).has_value(); }, 400));
    b.f->runtime->shutdown(false);
    net.restart(b);
    REQUIRE(net.settle([&] { return net.converged(); }, 400));
    CHECK(!b.f->runtime->location(clone).has_value());
    // The sender's endpoint still names b: b looks the clone up and forwards.
    CHECK(a.f->code(sender, "request", Cmd{.handle = h, .name = "echo", .payload = text("where are you")}) > 0);
    REQUIRE(net.settle([&] { return contains(a.f->journal(sender), "reply:ok:where are you"); }, 2000));
}

PAGLETS_TEST(
    "locator: a paglet moving continuously is located and pinned, also after a host is shut down (WP13 exit)") {
    Mesh net;
    auto& a = net.add_host("a");
    net.add_host("b");
    net.add_host("c");
    net.add_host("d");
    net.enroll_all();
    fast(net);
    allow_pins(net, 60'000);
    const std::string module = a.f->module("conformance.wasm");
    const auto seeker = start(net, a, module, '7');
    // The seeker's child roams: every 20 ms it dispatches itself to any host.
    const auto h = static_cast<std::int32_t>(a.f->code(seeker, "child", Cmd{}));
    REQUIRE(h > 1);
    const auto roamer = a.f->target_of(seeker, h);
    net.settle([] { return false; }, 5);  // its passport link
    CHECK_EQ(a.f->call(roamer, "roam", enc(Cmd{.name = "any", .ms = 20})).status, 0);

    std::int64_t hops_seen = 0;
    auto round = [&](Mesh::Host& from, bool through_seeker) {
        net.settle([] { return false; }, 60);  // it moves a few times
        std::string host;
        std::int32_t pin_cap = 0;
        if (through_seeker) {
            auto answer = ask_locator(net, *a.f, seeker, "locate_and_pin",
                                      wire::to_msgpack(loc::PinRequest{30'000, "exit"}), {h});
            REQUIRE(answer.status == "ok");
            REQUIRE(answer.caps.size() == 1u);
            host = decode_as<loc::Pin>(answer.payload).location.host;
            pin_cap = answer.caps[0];
        } else {
            const auto l = locate(net, from, roamer, 30'000ms);
            REQUIRE(l.state == Located::State::found);
            host = key_id(l.host);
        }
        // It is where the locator said, pinned, and stays there.
        auto* at = holder(net, roamer);
        REQUIRE(at != nullptr);
        CHECK(key_id(at->key) == host);
        CHECK(at->f->runtime->pinned_until(roamer) > unix_ms());
        net.settle([] { return false; }, 60);
        CHECK(holder(net, roamer) == at);
        CHECK(contains(at->f->journal(roamer), "roam:pinned"));
        hops_seen = dec<std::int64_t>(at->f->call(roamer, "hops").payload);
        // The pin ends; it moves on.
        if (through_seeker) {
            const auto answer =
                ask_locator(net, *a.f, seeker, "release", wire::to_msgpack(loc::ReleaseRequest{}), {pin_cap});
            CHECK(answer.status == "ok");
        } else {
            CHECK(at->node->release_pins(roamer) >= 1u);
        }
        REQUIRE(net.settle([&] { return holder(net, roamer) != at; }, 4000));
    };
    round(a, true);
    round(*net.hosts[2], false);

    // A host goes down (not a, where the seeker is, and not where the
    // paglet is: it is pinned meanwhile); preferably one that keeps its
    // record. The others notice and carry on without it.
    const auto hold = locate(net, a, roamer, 30'000ms);
    REQUIRE(hold.state == Located::State::found);
    Mesh::Host* failed = nullptr;
    for (const auto& k : a.node->responsible(roamer)) {
        if (k != a.key && k != hold.host) failed = net.find(k);
    }
    for (auto& x : net.hosts) {
        if (failed == nullptr && x.get() != &a && x->key != hold.host) failed = x.get();
    }
    REQUIRE(failed != nullptr);
    failed->down = true;
    REQUIRE(net.settle(
        [&] {
            for (auto& x : net.hosts) {
                if (x->down) continue;
                const auto live = x->node->live_hosts();
                if (std::ranges::find(live, failed->key) != live.end()) return false;
            }
            return true;
        },
        2000));
    CHECK(net.find(hold.host)->node->release_pins(roamer) >= 1u);
    std::size_t next = 0;
    for (int i = 0; i < 4; ++i) {
        if (i % 2 == 0) {
            round(a, true);
            continue;
        }
        while (net.hosts[next % net.hosts.size()]->down) ++next;
        round(*net.hosts[next++ % net.hosts.size()], false);
    }
    // It kept moving all along.
    CHECK(hops_seen >= 6);
}
