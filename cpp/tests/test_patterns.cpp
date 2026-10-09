// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The patterns library of the guest SDK (WP18, planning/cpp-patterns.md),
// through a paglet that uses each pattern on a mesh of three hosts.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <patterns_msgs.hpp>
#include <paglets/wire/reflect.hpp>

#include <filesystem>
#include <fstream>
#include <random>

using namespace paglets::test;
namespace wire = paglets::wire;
namespace fs = std::filesystem;
using namespace patterns_msgs;

namespace {

// The status of a patterns::Task, as the guest encodes it.
struct TaskStatus {
    std::string state;
    bool done = false;
    std::string error;
    std::int64_t started_ms = 0;
    std::int64_t completed_ms = 0;
    std::vector<std::uint8_t> result;
};

struct WaitRequest {
    std::int64_t timeout_ms = 0;
};

template <class Reply, class Request>
Reply ask(Fixture& f, const rt::PagletId& id, std::string_view op, const Request& q) {
    auto r = f.runtime->call(id, op, wire::to_msgpack(q), 20s);
    if (r.status != 0) throw std::runtime_error(std::string(op) + ": " + std::string(abi::error_name(r.status)));
    Reply reply{};
    if (!wire::from_msgpack(r.payload, reply)) throw std::runtime_error(std::string(op) + ": reply does not decode");
    return reply;
}

template <class Reply>
Reply ask(Fixture& f, const rt::PagletId& id, std::string_view op) {
    auto r = f.runtime->call(id, op, {}, 20s);
    if (r.status != 0) throw std::runtime_error(std::string(op) + ": " + std::string(abi::error_name(r.status)));
    Reply reply{};
    if (!wire::from_msgpack(r.payload, reply)) throw std::runtime_error(std::string(op) + ": reply does not decode");
    return reply;
}

fs::path temp_root(const std::string& name) {
    static std::mt19937_64 rng(std::random_device{}());
    auto dir = fs::temp_directory_path() / ("paglets-patterns-" + name + "-" + std::to_string(rng()));
    fs::create_directories(dir);
    return dir;
}

}  // namespace

PAGLETS_TEST("patterns: tasks, typed operations, fan-out, file mobility, locate and pin, notifications (WP18)") {
    const fs::path src = temp_root("src");
    const fs::path dst = temp_root("dst");
    std::ofstream(src / "letter.txt", std::ios::binary) << "dear c, a file travelled to you";
    ps::ServicesConfig sa, sc;
    sa.roots["src"] = src;
    sc.roots["dst"] = dst;
    Mesh net;
    auto& a = net.add_host("a", sa);
    auto& b = net.add_host("b");
    auto& c = net.add_host("c", sc);
    net.enroll_all();
    for (auto& h : net.hosts) {
        h->node->set_compute_timing({.sample = 50ms, .gossip = 50ms, .ttl = 2000ms, .redirect_after = 0ms});
    }
    net.admin_record("policy-rule", data::policy_rule({"read src", Decision::allow, "files", {"read"}, {},
                                                       Scope{std::vector<std::string>{"src"}, std::nullopt}, std::nullopt, 0}));
    net.admin_record("policy-rule", data::policy_rule({"write dst", Decision::allow, "files", {"write", "create"}, {},
                                                       Scope{std::vector<std::string>{"dst"}, std::nullopt}, std::nullopt, 0}));
    net.admin_record("policy-rule",
                     data::policy_rule({"pins", Decision::allow, "locator", {"pin"}, {}, std::nullopt, 600'000, 0}));
    REQUIRE(net.settle([&] { return net.converged() && a.node->landscape().size() == 3u; }));

    const std::string module = a.f->module("patterns_demo.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('5')));
    REQUIRE_OK(created);
    const auto id = *created;

    // A task: start answers at once; wait answers when it is done.
    auto st = ask<TaskStatus>(*a.f, id, "start", CountRequest{10, 300});
    CHECK_EQ(st.state, std::string("running"));
    CHECK(!st.done);
    st = ask<TaskStatus>(*a.f, id, "wait", WaitRequest{50});  // not done yet: the status as it is
    CHECK(!st.done);
    st = ask<TaskStatus>(*a.f, id, "wait", WaitRequest{10'000});
    CHECK_EQ(st.state, std::string("completed"));
    CountResult result;
    REQUIRE(wire::from_msgpack(st.result, result));
    CHECK_EQ(result.sum, 55);
    CHECK(st.completed_ms >= st.started_ms);
    st = ask<TaskStatus>(*a.f, id, "start", CountRequest{-1, 0});
    CHECK_EQ(st.state, std::string("failed"));
    CHECK_EQ(st.error, std::string("n is negative"));
    CHECK(ask<TaskStatus>(*a.f, id, "status").done);

    // A typed operation.
    CHECK_EQ(ask<AddReply>(*a.f, id, "add", AddRequest{2, 40}).sum, 42);

    // Fan-out: clones on b, c and here square the value; "nowhere" fails.
    (void)ask<SpreadStatus>(*a.f, id, "spread", SpreadRequest{{"b", "c", "", "nowhere"}, 7, 10'000});
    SpreadStatus spread;
    REQUIRE(net.settle(
        [&] {
            spread = ask<SpreadStatus>(*a.f, id, "spread_status");
            return spread.finished;
        },
        1000));
    CHECK_EQ(spread.succeeded, 3);
    CHECK(std::ranges::count(spread.results, std::string("b=49")) == 1);
    CHECK(std::ranges::count(spread.results, std::string("c=49")) == 1);
    CHECK(std::ranges::count(spread.results, std::string("a=49")) == 1);
    CHECK(std::ranges::any_of(spread.results, [](const std::string& r) { return r.starts_with("nowhere!"); }));

    // Locate and pin: while pinned, the paglet cannot leave.
    CHECK(ask<bool>(*a.f, id, "where"));
    std::string where;
    REQUIRE(net.settle(
        [&] {
            where = ask<std::string>(*a.f, id, "where_report");
            return !where.empty();
        },
        400));
    CHECK_EQ(where, std::string("a"));
    CHECK(ask<bool>(*a.f, id, "pin_self"));
    PinReport pinned;
    REQUIRE(net.settle(
        [&] {
            pinned = ask<PinReport>(*a.f, id, "pin_report");
            return pinned.done;
        },
        400));
    CHECK_EQ(pinned.host_name, std::string("a"));
    CHECK_EQ(pinned.dispatch_while_pinned, std::string("pinned"));
    REQUIRE(net.settle([&] { return a.f->runtime->pinned_until(id) == 0; }, 400));

    // Notifications reach the owner.
    CHECK(ask<bool>(*a.f, id, "notify", NotifyRequest{"patterns say hello"}));
    REQUIRE(net.settle([&] {
        const auto notes = a.services->notifications(net.owner.id());
        return std::ranges::any_of(notes, [](const auto& n) { return n.title == "patterns say hello"; });
    }));

    // Single-file mobility: a's file is put down on c.
    (void)ask<CarryStatus>(*a.f, id, "carry", CarryRequest{"src", "letter.txt", "c", "dst", "inbox/letter.txt"});
    REQUIRE(net.settle([&] { return c.f->runtime->info(id).has_value() && !a.f->runtime->info(id); }, 1000));
    CarryStatus carried;
    REQUIRE(net.settle(
        [&] {
            carried = ask<CarryStatus>(*c.f, id, "carry_status");
            return carried.state != "carrying";
        },
        400));
    if (carried.state != "delivered") std::cerr << "    carry: " << carried.error << "\n";
    CHECK_EQ(carried.state, std::string("delivered"));
    std::ifstream in(dst / "inbox" / "letter.txt", std::ios::binary);
    CHECK_EQ(std::string(std::istreambuf_iterator<char>(in), {}), std::string("dear c, a file travelled to you"));
    CHECK(c.f->runtime->marks(id) == std::vector<std::string>{"root:src"});  // it read a's root
    (void)b;

    std::error_code ec;
    fs::remove_all(src, ec);
    fs::remove_all(dst, ec);
}
