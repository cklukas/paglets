// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Example paglets (WP20, planning/cpp-demo-paglets.md) on meshes of several
// hosts.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <finder_msgs.hpp>
#include <benchmark_msgs.hpp>
#include <dupes_msgs.hpp>
#include <file_courier_msgs.hpp>
#include <log_scout_msgs.hpp>
#include <pi_msgs.hpp>
#include <bbp.hpp>
#include <tree_compare_msgs.hpp>
#include <guard_msgs.hpp>
#include <inventory_msgs.hpp>
#include <journey_msgs.hpp>
#include <latency_msgs.hpp>
#include <seek_msgs.hpp>
#include <paglets/sha256.hpp>
#include <paglets/services/storage.hpp>
#include <paglets/wire/reflect.hpp>

#include <chrono>
#include <filesystem>
#include <format>
#include <fstream>
#include <future>
#include <random>
#include <set>

using namespace paglets::test;
namespace wire = paglets::wire;

namespace {

void fast(Mesh& net) {
    for (auto& h : net.hosts) {
        h->node->set_compute_timing({.sample = 50ms, .gossip = 50ms, .ttl = 2000ms, .redirect_after = 0ms});
    }
}

std::filesystem::path temp_root(const std::string& name) {
    static std::mt19937_64 rng(std::random_device{}());
    auto dir = std::filesystem::temp_directory_path() / ("paglets-demo-" + name + "-" + std::to_string(rng()));
    std::filesystem::create_directories(dir);
    return dir;
}

void write_file(const std::filesystem::path& path, std::size_t size) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream(path, std::ios::binary) << std::string(size, 'x');
}

// The host that holds the paglet now, if it is settled on one.
Mesh::Host* holder(Mesh& net, const rt::PagletId& id) {
    for (auto& h : net.hosts) {
        if (h->f->runtime->info(id)) return h.get();
    }
    return nullptr;
}

}  // namespace

PAGLETS_TEST("example: Mesh Journey visits every host and comes home with its journal") {
    Mesh net;
    auto& home = net.add_host("home");
    net.add_host("north");
    net.add_host("south");
    net.add_host("east");
    net.enroll_all();
    fast(net);
    REQUIRE(net.settle([&] { return home.node->landscape().size() == 4u; }));
    const std::string module = home.f->module("journey.wasm");
    auto created = home.node->create(module, net.passport(module, paglet_id('1')));
    REQUIRE_OK(created);
    const auto id = *created;

    auto r = home.f->runtime->call(id, "start", wire::to_msgpack(journey_msgs::Start{{}, ""}), 20s);
    REQUIRE(r.status == 0);
    journey_msgs::Started started;
    REQUIRE(wire::from_msgpack(r.payload, started));
    CHECK(started.accepted);
    CHECK_EQ(started.itinerary.size(), 3u);

    // Calls only once it was away and is home again (a call to a paglet
    // that is leaving waits for frames this thread would have to carry).
    journey_msgs::Journal journal;
    auto back_home = [&] {
        bool away = false;
        return net.settle(
            [&] {
                Mesh::Host* h = holder(net, id);
                if (h != &home) {
                    away = true;
                    return false;
                }
                if (!away) return false;
                auto j = home.f->runtime->call(id, "journal", {}, 10s);
                return j.status == 0 && wire::from_msgpack(j.payload, journal) && journal.state != "travelling";
            },
            2000);
    };
    REQUIRE(back_home());
    CHECK_EQ(journal.state, std::string("home"));
    REQUIRE(journal.stops.size() == 5u);
    CHECK_EQ(journal.stops.front().host_name, std::string("home"));
    CHECK_EQ(journal.stops.back().host_name, std::string("home"));
    std::set<std::string> visited;
    for (std::size_t i = 1; i + 1 < journal.stops.size(); ++i) visited.insert(journal.stops[i].host_name);
    CHECK(visited == std::set<std::string>({"north", "south", "east"}));
    for (const auto& s : journal.stops) {
        CHECK(!s.os.empty());
        CHECK(s.arrived_ms > 0);
    }
    CHECK(journal.skipped.empty());
    report("journey", "hosts in the mesh", 4);
    report("journey", "hosts visited", visited.size());
    report("journey", "stops in the journal (with home twice)", journal.stops.size());
    report("journey", "journey time, start to home",
           journal.stops.back().arrived_ms - journal.stops.front().arrived_ms, "ms");

    // Asked for one known host and one that does not exist.
    r = home.f->runtime->call(id, "start", wire::to_msgpack(journey_msgs::Start{{"south", "atlantis"}, ""}), 20s);
    REQUIRE(r.status == 0);
    REQUIRE(wire::from_msgpack(r.payload, started));
    CHECK(started.itinerary.size() == 1u);
    REQUIRE(back_home());
    CHECK_EQ(journal.state, std::string("home"));
    REQUIRE(journal.stops.size() == 3u);
    CHECK_EQ(journal.stops[1].host_name, std::string("south"));
    REQUIRE(journal.skipped.size() == 1u);
    CHECK(journal.skipped[0].starts_with("atlantis"));
    report("journey", "unknown hosts skipped with a reason", journal.skipped.size());
}

PAGLETS_TEST("example: Mesh Journey stops after a host-independent dispatch refusal") {
    Mesh net;
    auto& home = net.add_host("home");
    net.add_host("north");
    net.add_host("south");
    net.enroll_all();
    fast(net);
    REQUIRE(net.settle([&] { return home.node->landscape().size() == 3u; }));
    const auto module = home.f->module("journey.wasm");
    auto created = home.node->create(module, net.passport(module, paglet_id('4')));
    REQUIRE_OK(created);
    home.f->runtime->wait_idle();
    REQUIRE_OK(home.f->runtime->pin(*created, "test-pin", unix_ms() + 60'000));
    auto r = home.f->runtime->call(*created, "start", wire::to_msgpack(journey_msgs::Start{{}, ""}), 10s);
    REQUIRE(r.status == 0);
    journey_msgs::Journal journal;
    REQUIRE(net.settle([&] {
        auto reply = home.f->runtime->call(*created, "journal", {}, 10s);
        return reply.status == 0 && wire::from_msgpack(reply.payload, journal) && journal.state == "failed";
    }));
    REQUIRE(journal.skipped.size() == 1u);
    CHECK(journal.skipped.front().find("pinned") != std::string::npos);
    CHECK_EQ(journal.stops.size(), 1u);
    CHECK(holder(net, *created) == &home);
}

PAGLETS_TEST("example: Ping Pong cleans up its child after a remote ping fails") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const auto module = a.f->module("ping_pong.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('5')));
    REQUIRE_OK(created);
    const auto id = *created;
    a.f->runtime->wait_idle();
    const auto caps = a.f->runtime->capabilities(id).size();
    auto running = std::async(std::launch::async, [&] {
        return a.f->runtime->call(id, "run", enc(std::uint32_t{0xffffffff}), 20s);
    });
    rt::PagletId child;
    REQUIRE(net.settle([&] {
        for (const auto& p : a.f->runtime->list()) {
            if (p.id != id && p.module == module) child = p.id;
        }
        return !child.empty();
    }));
    unsigned refused = 0;
    net.filter = [&](const PublicKey&, const PublicKey& to, Bytes& frame) {
        if (to != b.key || frame_type(frame) != "deliver") return true;
        auto v = decode(frame);
        auto bytes = Fields{*v->as_map()}.bin("m");
        if (!bytes) return true;
        auto m = rt::decode_remote(*bytes);
        if (m && m->target == child && m->kind == rt::RemoteMessage::Kind::request && m->name == "ping") {
            // A valid transport delivery whose receiver rejects the operation.
            // Leave the child alive so only the parent's cleanup can stop it.
            m->name = "unknown-ping";
            frame = encode(Value(Map{{"t", Value("deliver")}, {"m", Value(rt::encode_remote(*m))}}));
            ++refused;
        }
        return true;
    };
    REQUIRE_OK(a.node->dispatch(child, "b"));
    REQUIRE(net.settle([&] { return running.wait_for(0ms) == std::future_status::ready; }, 6000));
    CHECK_EQ(running.get().status, static_cast<std::int32_t>(abi::gone));
    CHECK(refused > 0);
    REQUIRE(net.settle([&] { return holder(net, child) == nullptr; }));
    a.f->runtime->wait_idle();
    CHECK_EQ(a.f->runtime->capabilities(id).size(), caps);
    // The same parent remains usable after the failed run.
    const auto again = a.f->runtime->call(id, "run", enc(std::uint32_t{3}), 10s);
    CHECK_EQ(again.status, 0);
    CHECK_EQ(dec<std::uint32_t>(again.payload), 3u);
    a.f->runtime->wait_idle();
    CHECK_EQ(a.f->runtime->capabilities(id).size(), caps);
}

PAGLETS_TEST("example: Mesh File Finder streams beyond the old result cap and cancels searches") {
    const auto root = temp_root("finder-stream");
    for (int i = 0; i < 10'250; ++i) write_file(root / (std::to_string(i) + ".csv"), 3);
    {
        ps::ServicesConfig config;
        config.roots["docs"] = root;
        Mesh net;
        auto& a = net.add_host("a", config);
        net.enroll_all();
        fast(net);
        net.admin_record("policy-rule", data::policy_rule({"read docs", Decision::allow, "files", {"read"}, Match{},
            Scope{std::vector<std::string>{"docs"}, std::nullopt}, std::nullopt, 0}));
        REQUIRE(net.settle([&] { return net.converged(); }));
        const auto module = a.f->module("finder.wasm");
        auto created = a.node->create(module, net.passport(module, paglet_id('2')));
        REQUIRE_OK(created);
        const auto id = *created;
        finder_msgs::Search q;
        q.root = "docs";
        q.hosts = {""};
        q.max_results = 3;
        auto start = [&] {
            auto r = a.f->runtime->call(id, "start", wire::to_msgpack(q), 10s);
            finder_msgs::Started s;
            REQUIRE(wire::from_msgpack(r.payload, s));
            return s;
        };
        REQUIRE(start().accepted);
        CHECK(!start().accepted);  // one search at a time
        finder_msgs::Findings result;
        bool saw_progress = false;
        REQUIRE(net.settle([&] {
            auto r = a.f->runtime->call(id, "findings", {}, 2s);
            if (r.status != 0 || !wire::from_msgpack(r.payload, result)) return false;
            saw_progress |= !result.progress.empty();
            return result.finished;
        }, 15000));
        CHECK(saw_progress);
        CHECK_EQ(result.files, 10'250);
        CHECK_EQ(result.bytes, 30'750);
        REQUIRE(result.hosts.size() == 1);
        CHECK(result.hosts[0].error.empty());
        CHECK_EQ(result.hosts[0].found.size(), 3u);
        CHECK(result.hosts[0].truncated);  // samples only; totals cover the whole tree
        CHECK_EQ(result.hosts[0].visited, 10'250u);
        REQUIRE(start().accepted);
        auto cancelled = a.f->runtime->call(id, "cancel", {}, 2s);
        REQUIRE(wire::from_msgpack(cancelled.payload, result));
        CHECK(result.finished);
        CHECK(result.cancelled);
        REQUIRE(net.settle([&] {
            const auto live = a.f->runtime->list();
            return std::ranges::none_of(live, [&](const auto& p) { return p.id != id && p.module == module; });
        }, 5000));
        q.timeout_ms = 1;
        REQUIRE(start().accepted);
        REQUIRE(net.settle([&] {
            auto r = a.f->runtime->call(id, "findings", {}, 2s);
            return wire::from_msgpack(r.payload, result) && result.finished;
        }, 5000));
        CHECK(!result.hosts.empty());
        CHECK(!result.hosts[0].error.empty());
    }
    std::filesystem::remove_all(root);
}

PAGLETS_TEST("example: Mesh File Finder and Storage Analyzer search every host's root") {
    namespace fs = std::filesystem;
    const fs::path ra = temp_root("a"), rb = temp_root("b"), rc = temp_root("c");
    write_file(ra / "report.csv", 1000);
    write_file(ra / "deep" / "data.csv", 5000);
    write_file(ra / "notes.txt", 10);
    write_file(rb / "big.csv", 20000);
    write_file(rc / "other.bin", 300);  // c lends no access: its root is not readable by policy
    ps::ServicesConfig sa, sb, sc;
    sa.roots["docs"] = ra;
    sb.roots["docs"] = rb;
    sc.roots["docs"] = rc;
    Mesh net;
    auto& a = net.add_host("a", sa);
    auto& b = net.add_host("b", sb);
    auto& c = net.add_host("c", sc);
    net.enroll_all();
    fast(net);
    net.admin_record("host-enroll", data::host_enroll(a.key, "a", {"open"}));
    net.admin_record("host-enroll", data::host_enroll(b.key, "b", {"open"}));
    Match open_hosts;
    open_hosts.hosts = HostSelector{{}, {"open"}};
    net.admin_record("policy-rule", data::policy_rule({"read docs", Decision::allow, "files", {"read"}, open_hosts,
                                                       Scope{std::vector<std::string>{"docs"}, std::nullopt},
                                                       std::nullopt, 0}));
    REQUIRE(net.settle([&] { return net.converged() && a.node->landscape().size() == 3u; }));

    const std::string module = a.f->module("finder.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('2')));
    REQUIRE_OK(created);
    const auto id = *created;
    finder_msgs::Search q;
    q.root = "docs";
    q.pattern = "**/*.csv";
    auto r = a.f->runtime->call(id, "start", wire::to_msgpack(q), 20s);
    REQUIRE(r.status == 0);
    finder_msgs::Started started;
    REQUIRE(wire::from_msgpack(r.payload, started));
    CHECK(started.accepted);
    CHECK_EQ(started.hosts, 3);

    finder_msgs::Findings f;
    REQUIRE(net.settle(
        [&] {
            auto x = a.f->runtime->call(id, "findings", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, f) && f.finished;
        },
        2000));
    REQUIRE(f.hosts.size() == 3u);
    CHECK_EQ(f.files, 3);
    CHECK_EQ(f.bytes, 26000);
    for (const auto& h : f.hosts) {
        if (h.host_name == "a") {
            CHECK(h.error.empty());
            CHECK_EQ(h.files, 2);
            REQUIRE(h.found.size() == 2u);
            CHECK_EQ(h.found[0].path, std::string("deep/data.csv"));
            REQUIRE(!h.largest.empty());
            CHECK_EQ(h.largest[0].size, 5000);
            REQUIRE(h.extensions.size() == 1u);
            CHECK_EQ(h.extensions[0].extension, std::string("csv"));
        } else if (h.host_name == "b") {
            CHECK_EQ(h.files, 1);
            CHECK_EQ(h.bytes, 20000);
        } else {
            CHECK_EQ(h.host_name, std::string("c"));
            CHECK(h.error.find("denied") != std::string::npos);  // the policy covers labelled hosts only
        }
    }
    report("finder", "hosts searched", f.hosts.size());
    report("finder", "matching files found", f.files);
    report("finder", "bytes in matching files", f.bytes);
    report("finder", "hosts that refused access (policy)", 1);
    std::error_code ec;
    for (const auto& p : {ra, rb, rc}) fs::remove_all(p, ec);
    (void)b;
    (void)c;
}

PAGLETS_TEST("example: Latency Map measures round trips between every pair of hosts") {
    Mesh net;
    auto& a = net.add_host("a");
    net.add_host("b");
    net.add_host("c");
    net.enroll_all();
    fast(net);
    REQUIRE(net.settle([&] { return a.node->landscape().size() == 3u; }));
    const std::string module = a.f->module("latency.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('3')));
    REQUIRE_OK(created);
    const auto id = *created;
    auto r = a.f->runtime->call(id, "start", wire::to_msgpack(latency_msgs::Probe{{}, 3, 30'000}), 20s);
    REQUIRE(r.status == 0);
    latency_msgs::Started started;
    REQUIRE(wire::from_msgpack(r.payload, started));
    CHECK(started.accepted);
    CHECK_EQ(started.probes, 3);
    latency_msgs::LatencyMap map;
    REQUIRE(net.settle(
        [&] {
            auto x = a.f->runtime->call(id, "map", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, map) && map.finished;
        },
        2000));
    CHECK(map.errors.empty());
    REQUIRE(map.rows.size() == 3u);
    std::vector<std::string> from;
    for (const auto& row : map.rows) {
        from.push_back(row.from);
        REQUIRE(row.cells.size() == 2u);  // every other host
        for (const auto& c : row.cells) {
            CHECK(c.error.empty());
            CHECK_EQ(c.replies, 3);
            CHECK(c.min_ms >= 0 && c.min_ms <= c.avg_ms);
            CHECK(c.to != row.from);
        }
    }
    CHECK(from == std::vector<std::string>({"a", "b", "c"}));
    double sum = 0;
    int pairs = 0;
    for (const auto& row : map.rows) {
        for (const auto& c : row.cells) {
            sum += c.avg_ms;
            ++pairs;
        }
    }
    report("latency", "host pairs measured", pairs);
    report("latency", "pings per pair", 3);
    report("latency", "mean round trip (in-memory test transport)", pairs ? sum / pairs : 0.0, "ms");
    // The probes end once the map is complete.
    REQUIRE(net.settle([&] {
        std::size_t paglets = 0;
        for (auto& h : net.hosts) paglets += h->f->runtime->list().size();
        std::size_t system = 0;
        for (auto& h : net.hosts) {
            for (const auto& p : h->f->runtime->list()) system += p.module.empty() ? 1 : 0;
        }
        return paglets - system == 1;  // only the parent
    }));
}

PAGLETS_TEST("example: Inventory Collector and Process Finder ask server-info on every host") {
    Mesh net;
    auto& a = net.add_host("a");
    net.add_host("b");
    net.add_host("c");
    net.enroll_all();
    fast(net);
    net.admin_record("policy-rule", data::policy_rule({"inventory", Decision::allow, "server-info",
                                                       {"summary", "load", "volumes", "processes"}, {}, std::nullopt,
                                                       std::nullopt, 0}));
    REQUIRE(net.settle([&] { return net.converged() && a.node->landscape().size() == 3u; }));
    const std::string module = a.f->module("inventory.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('4')));
    REQUIRE_OK(created);
    const auto id = *created;
    auto collect = [&](inventory_msgs::Collect q) {
        auto r = a.f->runtime->call(id, "start", wire::to_msgpack(q), 20s);
        REQUIRE(r.status == 0);
        inventory_msgs::Report report;
        REQUIRE(net.settle(
            [&] {
                auto x = a.f->runtime->call(id, "report", {}, 10s);
                return x.status == 0 && wire::from_msgpack(x.payload, report) && report.finished;
            },
            2000));
        REQUIRE(net.settle([&] {
            for (const auto& host : net.hosts)
                for (const auto& paglet : host->f->runtime->list())
                    if (paglet.module == module && paglet.id != id) return false;
            return true;
        }, 400));
        return report;
    };
    auto report = collect({});
    REQUIRE(report.hosts.size() == 3u);
    for (const auto& h : report.hosts) {
        CHECK(h.error.empty());
        CHECK(!h.os.empty());
        CHECK(h.cpus > 0);
        CHECK(h.memory_total > 0);
        CHECK(!h.volumes.empty());
        CHECK(h.processes_total > 0);
        CHECK(h.processes.size() <= 5u);
    }
    CHECK_EQ(report.hosts[0].host_name, std::string("a"));
    ::paglets::test::report("inventory", "hosts reporting", report.hosts.size());
    ::paglets::test::report("inventory", "volumes reported on the first host", report.hosts[0].volumes.size());
    ::paglets::test::report("inventory", "processes on the first host", report.hosts[0].processes_total);
    // Process Finder: the processes named like this test's.
    inventory_msgs::Collect find;
    find.process = "paglets_tests";
    report = collect(find);
    REQUIRE(report.hosts.size() == 3u);
    for (const auto& h : report.hosts) {
        REQUIRE(!h.processes.empty());  // the hosts share this process in the test
        CHECK(h.processes[0].name.find("paglets_tests") != std::string::npos);
    }
    ::paglets::test::report("inventory", "hosts where the process finder found the test process", report.hosts.size());
}

PAGLETS_TEST("example: Inventory Collector disposes clones after denied reports") {
    Mesh net;
    auto& a = net.add_host("a");
    net.add_host("b");
    net.enroll_all();
    fast(net);
    REQUIRE(net.settle([&] { return a.node->landscape().size() == 2u; }));
    const auto module = a.f->module("inventory.wasm");
    const auto created = a.node->create(module, net.passport(module, paglet_id('4')));
    REQUIRE_OK(created);
    const auto id = *created;
    CHECK(a.f->call(id, "start", wire::to_msgpack(inventory_msgs::Collect{})).status == 0);
    inventory_msgs::Report report;
    REQUIRE(net.settle([&] {
        const auto reply = a.f->call(id, "report");
        return reply.status == 0 && wire::from_msgpack(reply.payload, report) && report.finished;
    }, 2000));
    REQUIRE(report.hosts.size() == 2u);
    for (const auto& host : report.hosts) CHECK(!host.error.empty());
    REQUIRE(net.settle([&] {
        for (const auto& host : net.hosts)
            for (const auto& paglet : host->f->runtime->list())
                if (paglet.module == module && paglet.id != id) return false;
        return true;
    }, 400));
    CHECK(a.f->runtime->info(id).has_value());
}

PAGLETS_TEST("example: Hide and Seek finds and pins a paglet that keeps moving") {
    Mesh net;
    auto& a = net.add_host("a");
    net.add_host("b");
    net.add_host("c");
    net.enroll_all();
    fast(net);
    net.admin_record("policy-rule",
                     data::policy_rule({"pins", Decision::allow, "locator", {"pin"}, {}, std::nullopt, 600'000, 0}));
    REQUIRE(net.settle([&] { return net.converged() && a.node->landscape().size() == 3u; }));
    const std::string module = a.f->module("seek.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('5')));
    REQUIRE_OK(created);
    const auto id = *created;
    auto r = a.f->runtime->call(id, "start", wire::to_msgpack(seek_msgs::Game{4, 30, 300}), 20s);
    REQUIRE(r.status == 0);
    seek_msgs::Score score;
    REQUIRE(net.settle(
        [&] {
            auto x = a.f->runtime->call(id, "score", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, score) && score.state == "done";
        },
        3000));
    REQUIRE(score.catches.size() == 4u);
    for (std::size_t i = 0; i < score.catches.size(); ++i) {
        const auto& c = score.catches[i];
        CHECK(c.error.empty());
        CHECK(!c.host_name.empty());
        // It was caught again only after it had moved on.
        if (i > 0) CHECK(c.moves > score.catches[i - 1].moves);
    }
    CHECK(score.hider_moves >= 3);
    report("seek", "catches", score.catches.size());
    report("seek", "moves of the hider", score.hider_moves);
}

PAGLETS_TEST("example: Volume Guard sleeps between checks and alerts its owner") {
    Mesh net;
    auto& a = net.add_host("a");
    net.enroll_all();
    net.admin_record("policy-rule", data::policy_rule({"volumes", Decision::allow, "server-info", {"volumes"}, {},
                                                       std::nullopt, std::nullopt, 0}));
    REQUIRE(net.settle([&] { return net.converged(); }));
    const std::string module = a.f->module("guard.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('6')));
    REQUIRE_OK(created);
    const auto id = *created;
    // Every volume is "low" against 101%: alerts, once per volume.
    auto r = a.f->runtime->call(id, "start", wire::to_msgpack(guard_msgs::Watch{101, 50, 3}), 20s);
    REQUIRE(r.status == 0);
    // Undisturbed (a message would wake it as well), it sleeps and wakes.
    std::this_thread::sleep_for(1s);
    guard_msgs::Status st;
    REQUIRE(net.settle(
        [&] {
            auto x = a.f->runtime->call(id, "status", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, st) && st.state != "watching";
        },
        2000));
    CHECK_EQ(st.state, std::string("done"));
    CHECK_EQ(st.checks, 3);
    CHECK_EQ(st.sleeps, 2);  // inactive between the checks
    REQUIRE(!st.alerts.empty());
    std::set<std::string> mounts;
    for (const auto& alert : st.alerts) mounts.insert(alert.mount_point);
    CHECK_EQ(mounts.size(), st.alerts.size());  // each volume once
    REQUIRE(net.settle([&] {
        const auto notes = a.services->notifications(net.owner.id());
        return std::ranges::any_of(notes, [](const auto& n) { return n.title.starts_with("volume "); });
    }));
    report("guard", "checks", st.checks);
    report("guard", "times inactive between checks", st.sleeps);
    report("guard", "alerts (each volume once)", st.alerts.size());
}

PAGLETS_TEST("example: Duplicate Finder finds the same content on different hosts") {
    namespace fs = std::filesystem;
    const fs::path ra = temp_root("da"), rb = temp_root("db"), rc = temp_root("dc");
    auto write_text = [](const fs::path& p, const std::string& text) {
        fs::create_directories(p.parent_path());
        std::ofstream(p, std::ios::binary) << text;
    };
    const std::string report(5000, 'r');
    write_text(ra / "q3" / "report.txt", report);
    write_text(rb / "copy-of-report.txt", report);
    write_text(rc / "same-size-other.txt", std::string(5000, 'o'));  // same size, other content
    write_text(rc / "unique.txt", "only here");
    ps::ServicesConfig sa, sb, sc;
    sa.roots["docs"] = ra;
    sb.roots["docs"] = rb;
    sc.roots["docs"] = rc;
    Mesh net;
    auto& a = net.add_host("a", sa);
    net.add_host("b", sb);
    net.add_host("c", sc);
    net.enroll_all();
    fast(net);
    net.admin_record("policy-rule", data::policy_rule({"read docs", Decision::allow, "files", {"read"}, {},
                                                       Scope{std::vector<std::string>{"docs"}, std::nullopt},
                                                       std::nullopt, 0}));
    REQUIRE(net.settle([&] { return net.converged() && a.node->landscape().size() == 3u; }));
    const std::string module = a.f->module("dupes.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('7')));
    REQUIRE_OK(created);
    const auto id = *created;
    dupes_msgs::Search q;
    q.root = "docs";
    auto r = a.f->runtime->call(id, "start", wire::to_msgpack(q), 20s);
    REQUIRE(r.status == 0);
    dupes_msgs::Result result;
    REQUIRE(net.settle(
        [&] {
            auto x = a.f->runtime->call(id, "result", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, result) && result.state == "done";
        },
        3000));
    CHECK(result.errors.empty());
    CHECK_EQ(result.files, 4);
    CHECK_EQ(result.candidates, 3);
    REQUIRE(result.groups.size() == 1u);
    const auto& g = result.groups[0];
    CHECK_EQ(g.size, 5000);
    REQUIRE(g.copies.size() == 2u);
    CHECK_EQ(g.copies[0].host_name, std::string("a"));
    CHECK_EQ(g.copies[0].path, std::string("q3/report.txt"));
    CHECK_EQ(g.copies[1].host_name, std::string("b"));
    CHECK_EQ(result.wasted, 5000);
    CHECK_EQ(g.sha256, paglets::to_hex(paglets::sha256(text(report))));
    ::paglets::test::report("dupes", "files listed on three hosts", result.files);
    ::paglets::test::report("dupes", "files hashed (same size as another)", result.candidates);
    ::paglets::test::report("dupes", "groups of duplicates", result.groups.size());
    ::paglets::test::report("dupes", "bytes in extra copies", result.wasted);
    std::error_code ec;
    for (const auto& p : {ra, rb, rc}) fs::remove_all(p, ec);
}

PAGLETS_TEST("example: File Courier carries files of any size between hosts with checksums, within data residency") {
    namespace fs = std::filesystem;
    const fs::path data = temp_root("fc-data"), clinic = temp_root("fc-clinic"), inbox = temp_root("fc-inbox");
    auto write_text = [](const fs::path& p, const std::string& text) {
        fs::create_directories(p.parent_path());
        std::ofstream(p, std::ios::binary) << text;
    };
    auto read_text = [](const fs::path& p) {
        std::ifstream in(p, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(in), {});
    };
    std::string table;
    for (int i = 0; i < 400; ++i) table += std::to_string(i) + "," + std::to_string(i * i) + "\n";
    write_text(data / "results" / "squares.csv", table);
    write_text(data / "results" / "run-2" / "log.txt", "converged after 12 steps");
    // The content goes from host to host; the courier carries a capability.
    std::string big(24u * 1024 * 1024 + 123, '\0');
    for (std::size_t i = 0; i < big.size(); ++i) big[i] = static_cast<char>((i * 2654435761u) >> 13);
    write_text(data / "results" / "run-2" / "field.bin", big);
    write_text(data / "notes.md", "not carried");
    write_text(clinic / "results" / "patients.csv", "confidential");
    ps::ServicesConfig lab_sc, archive_sc;
    lab_sc.roots["data"] = data;
    lab_sc.roots["clinic"] = clinic;
    archive_sc.roots["inbox"] = inbox;
    Mesh net;
    auto& home = net.add_host("home");
    auto& lab = net.add_host("lab", lab_sc);
    auto& archive = net.add_host("archive", archive_sc);
    net.enroll_all();
    fast(net);
    net.admin_record("policy-rule", data::policy_rule({"read results", Decision::allow, "files", {"read"}, {},
                                                       Scope{std::vector<std::string>{"data", "clinic"}, std::nullopt},
                                                       std::nullopt, 0}));
    net.admin_record("policy-rule", data::policy_rule({"deliver", Decision::allow, "files", {"read", "write", "create"}, {},
                                                       Scope{std::vector<std::string>{"inbox"}, std::nullopt},
                                                       std::nullopt, 0}));
    net.admin_record("root-residency", data::root_residency("clinic", "host-only"));
    REQUIRE(net.settle([&] { return net.converged() && home.node->landscape().size() == 3u; }));
    const std::string module = home.f->module("file_courier.wasm");

    // Runs a courier from the home host until it is delivered, refused or
    // failed; returns its status and the host it ended on.
    auto run = [&](char tag, const std::string& root) {
        auto created = home.node->create(module, net.passport(module, paglet_id(tag)));
        REQUIRE_OK(created);
        const auto id = *created;
        file_courier_msgs::Start start;
        start.from_host = "lab";
        start.from_root = root;
        start.pattern = "results/**";
        start.to_host = "archive";
        start.to_root = "inbox";
        start.to_path = "from-lab";
        auto r = home.f->runtime->call(id, "start", wire::to_msgpack(start), 20s);
        REQUIRE(r.status == 0);
        file_courier_msgs::Started started;
        REQUIRE(wire::from_msgpack(r.payload, started));
        CHECK(started.accepted);
        file_courier_msgs::Status st;
        Mesh::Host* where = nullptr;
        REQUIRE(net.settle(
            [&] {
                Mesh::Host* h = holder(net, id);
                if (h == nullptr) return false;
                auto x = h->f->runtime->call(id, "status", {}, 10s);
                if (x.status != 0 || !wire::from_msgpack(x.payload, st)) return false;
                where = h;
                return st.state == "delivered" || st.state == "refused" || st.state == "failed";
            },
            3000));
        return std::pair{st, where};
    };

    const auto [done, at] = run('c', "data");
    if (done.state != "delivered") std::cerr << "    file courier: " << done.error << "\n";
    CHECK_EQ(done.state, std::string("delivered"));
    CHECK(at == &home);
    CHECK(done.hosts == std::vector<std::string>({"home", "lab", "archive", "home"}));
    REQUIRE(done.files.size() == 3u);
    for (const auto& f : done.files) CHECK(f.verified);
    CHECK_EQ(done.bytes, static_cast<std::int64_t>(table.size() + 24 + big.size()));
    CHECK_EQ(read_text(inbox / "from-lab" / "results" / "squares.csv"), table);
    CHECK_EQ(read_text(inbox / "from-lab" / "results" / "run-2" / "log.txt"), std::string("converged after 12 steps"));
    CHECK(read_text(inbox / "from-lab" / "results" / "run-2" / "field.bin") == big);
    // Saved files appear whole: no partial files are left.
    for (const auto& e : fs::recursive_directory_iterator(inbox)) {
        CHECK(!e.path().filename().string().starts_with(".paglets-save-"));
    }
    CHECK(!fs::exists(inbox / "from-lab" / "notes.md"));
    for (const auto& f : done.files) {
        if (f.path == "results/squares.csv") CHECK_EQ(f.sha256, paglets::to_hex(paglets::sha256(text(table))));
    }
    report("file_courier", "files carried", done.files.size());
    report("file_courier", "bytes carried", done.bytes);
    report("file_courier", "largest file transferred as an artifact", big.size() / (1024.0 * 1024.0), "MB");
    report("file_courier", "sharing (hashing) on the source host", done.share_ms, "ms");
    report("file_courier", "saving (transfer and check) on the destination", done.save_ms, "ms");
    if (done.save_ms > 0) {
        report("file_courier", "transfer rate (in-memory test transport)",
               done.bytes / (1024.0 * 1024.0) / (done.save_ms / 1000.0), "MB/s");
    }
    report("file_courier", "whole errand, start to receipt", done.total_ms, "ms");

    // Content of a host-only root may not leave the lab: the courier, which
    // listed the root, stays there, and nothing reaches the archive.
    const auto [refused, stayed] = run('d', "clinic");
    CHECK_EQ(refused.state, std::string("refused"));
    CHECK(refused.error.find("data residency") != std::string::npos);
    CHECK(stayed == &lab);
    CHECK(!fs::exists(inbox / "from-lab" / "results" / "patients.csv"));
    report("file_courier", "host-only root: refused, nothing delivered", "yes");

    // A host that is not in the mesh: the courier does not leave.
    auto created = home.node->create(module, net.passport(module, paglet_id('e')));
    REQUIRE_OK(created);
    file_courier_msgs::Start nowhere;
    nowhere.from_host = "lab";
    nowhere.from_root = "data";
    nowhere.to_host = "atlantis";
    nowhere.to_root = "inbox";
    auto r = home.f->runtime->call(*created, "start", wire::to_msgpack(nowhere), 20s);
    REQUIRE(r.status == 0);
    file_courier_msgs::Started started;
    REQUIRE(wire::from_msgpack(r.payload, started));
    CHECK(!started.accepted);
    CHECK(started.reason.find("atlantis") != std::string::npos);
    CHECK(home.f->runtime->info(*created).has_value());
    (void)archive;
    std::error_code ec;
    for (const auto& p : {data, clinic, inbox}) fs::remove_all(p, ec);
}

PAGLETS_TEST("example: Tree Compare finds missing and different files on several hosts") {
    namespace fs = std::filesystem;
    const fs::path ra = temp_root("ta"), rb = temp_root("tb"), rc = temp_root("tc");
    auto write_text = [](const fs::path& p, const std::string& text) {
        fs::create_directories(p.parent_path());
        std::ofstream(p, std::ios::binary) << text;
    };
    write_text(ra / "app.conf", "x=1");
    write_text(rb / "app.conf", "x=1");
    write_text(rc / "app.conf", "x=22");  // another size
    for (const auto& r : {ra, rb, rc}) write_text(r / "shared" / "readme.txt", "the same everywhere");
    write_text(ra / "data.bin", std::string(100, 'a'));
    write_text(rb / "data.bin", std::string(100, 'b'));  // same size, other content; none on c
    write_text(rb / "extra.txt", "only on b");
    ps::ServicesConfig sa, sb, sc;
    sa.roots["conf"] = ra;
    sb.roots["conf"] = rb;
    sc.roots["conf"] = rc;
    Mesh net;
    auto& a = net.add_host("a", sa);
    net.add_host("b", sb);
    net.add_host("c", sc);
    net.enroll_all();
    fast(net);
    net.admin_record("policy-rule", data::policy_rule({"read conf", Decision::allow, "files", {"read"}, {},
                                                       Scope{std::vector<std::string>{"conf"}, std::nullopt},
                                                       std::nullopt, 0}));
    REQUIRE(net.settle([&] { return net.converged() && a.node->landscape().size() == 3u; }));
    const std::string module = a.f->module("tree_compare.wasm");
    auto compare = [&](char tag, tree_compare_msgs::Compare q) {
        auto created = a.node->create(module, net.passport(module, paglet_id(tag)));
        REQUIRE_OK(created);
        const auto id = *created;
        auto r = a.f->runtime->call(id, "start", wire::to_msgpack(q), 20s);
        REQUIRE(r.status == 0);
        tree_compare_msgs::Started started;
        REQUIRE(wire::from_msgpack(r.payload, started));
        CHECK(started.accepted);
        tree_compare_msgs::Report report;
        REQUIRE(net.settle(
            [&] {
                auto x = a.f->runtime->call(id, "report", {}, 10s);
                return x.status == 0 && wire::from_msgpack(x.payload, report) && report.state == "done";
            },
            3000));
        return report;
    };
    auto find = [](const tree_compare_msgs::Report& r, const std::string& path) -> const tree_compare_msgs::Difference* {
        for (const auto& d : r.differences) {
            if (d.path == path) return &d;
        }
        return nullptr;
    };

    // Every host, sizes only.
    tree_compare_msgs::Compare q;
    q.root = "conf";
    const auto sizes = compare('a', q);
    CHECK(sizes.errors.empty());
    CHECK_EQ(sizes.hosts.size(), 3u);
    CHECK_EQ(sizes.files, 4);
    CHECK_EQ(sizes.same, 1);  // shared/readme.txt; data.bin differs only in content
    REQUIRE(sizes.differences.size() == 3u);
    const auto* conf = find(sizes, "app.conf");
    REQUIRE(conf != nullptr);
    CHECK_EQ(conf->kind, std::string("size"));
    CHECK_EQ(conf->copies.size(), 3u);
    const auto* bin = find(sizes, "data.bin");
    REQUIRE(bin != nullptr);
    CHECK_EQ(bin->kind, std::string("missing"));
    CHECK(bin->missing_on == std::vector<std::string>({"c"}));
    const auto* extra = find(sizes, "extra.txt");
    REQUIRE(extra != nullptr);
    CHECK(extra->missing_on == std::vector<std::string>({"a", "c"}));

    // This host and b, by content.
    q.hosts = {"", "b"};
    q.hashes = true;
    const auto content = compare('b', q);
    CHECK(content.errors.empty());
    CHECK(content.hosts == std::vector<std::string>({"a", "b"}));
    CHECK_EQ(content.same, 2);
    REQUIRE(content.differences.size() == 2u);
    const auto* bin2 = find(content, "data.bin");
    REQUIRE(bin2 != nullptr);
    CHECK_EQ(bin2->kind, std::string("content"));
    REQUIRE(bin2->copies.size() == 2u);
    CHECK_EQ(bin2->copies[0].sha256, paglets::to_hex(paglets::sha256(text(std::string(100, 'a')))));
    ::paglets::test::report("tree_compare", "hosts compared by size", sizes.hosts.size());
    ::paglets::test::report("tree_compare", "files compared", sizes.files);
    ::paglets::test::report("tree_compare", "differences by size (missing or other size)", sizes.differences.size());
    ::paglets::test::report("tree_compare", "differences by content (two hosts, SHA-256)", content.differences.size());
    std::error_code ec;
    for (const auto& p : {ra, rb, rc}) fs::remove_all(p, ec);
}

PAGLETS_TEST("example: Log Scout searches logs in a time window and follows them live") {
    namespace fs = std::filesystem;
    using namespace std::chrono;
    const fs::path r1 = temp_root("log1"), r2 = temp_root("log2");
    auto stamp = [](minutes ago) {
        return std::format("{:%FT%T}", floor<seconds>(system_clock::now() - ago));
    };
    auto write_text = [](const fs::path& p, const std::string& text, bool append = false) {
        fs::create_directories(p.parent_path());
        std::ofstream(p, std::ios::binary | (append ? std::ios::app : std::ios::trunc)) << text;
    };
    write_text(r1 / "app.log", stamp(120min) + " ERROR old failure\n" + stamp(5min) + " ERROR recent failure\n" +
                                   stamp(4min) + " WARN disk almost full\n" + stamp(3min) + " INFO all good\n");
    write_text(r1 / "old.log", "ERROR ancient\n");
    fs::last_write_time(r1 / "old.log", fs::file_time_type::clock::now() - 3h);
    write_text(r2 / "app.log", "error: connection refused\nwarning: slow response\nok\n");
    write_text(r2 / "sub" / "worker.log", stamp(1min) + " Error in worker\n");
    std::string noise;
    for (int i = 0; i < 200'000; ++i) noise += "noise line\n";
    write_text(r2 / "big.log", noise + "ERROR at the end\n");
    ps::ServicesConfig s1, s2;
    s1.roots["logs"] = r1;
    s2.roots["logs"] = r2;
    Mesh net;
    auto& home = net.add_host("home");
    net.add_host("web1", s1);
    net.add_host("web2", s2);
    net.enroll_all();
    fast(net);
    net.admin_record("policy-rule", data::policy_rule({"read logs", Decision::allow, "files", {"read"}, {},
                                                       Scope{std::vector<std::string>{"logs"}, std::nullopt},
                                                       std::nullopt, 0}));
    REQUIRE(net.settle([&] { return net.converged() && home.node->landscape().size() == 3u; }));
    const std::string module = home.f->module("log_scout.wasm");
    auto report_of = [&](const rt::PagletId& id) {
        log_scout_msgs::Report report;
        auto x = home.f->runtime->call(id, "report", {}, 10s);
        if (x.status != 0 || !wire::from_msgpack(x.payload, report)) report.state = "unknown";
        return report;
    };
    auto count = [](const log_scout_msgs::Report& r, const std::string& host, const std::string& pattern) {
        for (const auto& c : r.counts) {
            if (c.host_name == host && c.pattern == pattern) return c.matches;
        }
        return std::int64_t{-1};
    };
    auto launch = [&](char tag, const log_scout_msgs::Scout& q) {
        auto created = home.node->create(module, net.passport(module, paglet_id(tag)));
        REQUIRE_OK(created);
        auto r = home.f->runtime->call(*created, "start", wire::to_msgpack(q), 20s);
        REQUIRE(r.status == 0);
        log_scout_msgs::Started started;
        REQUIRE(wire::from_msgpack(r.payload, started));
        CHECK(started.accepted);
        return *created;
    };

    // The last hour on both hosts; only the end of each file is read.
    log_scout_msgs::Scout q;
    q.hosts = {"web1", "web2"};
    q.root = "logs";
    q.patterns = {"error", "warn"};
    q.tail_bytes = 64 * 1024;
    const auto id = launch('a', q);
    log_scout_msgs::Report report;
    REQUIRE(net.settle([&] { return (report = report_of(id)).state == "done"; }, 3000));
    CHECK(report.errors.empty());
    CHECK_EQ(count(report, "web1", "error"), 1);  // not the old failure, not old.log
    CHECK_EQ(count(report, "web1", "warn"), 1);
    CHECK_EQ(count(report, "web2", "error"), 3);  // connection refused, worker, the end of big.log
    CHECK_EQ(count(report, "web2", "warn"), 1);
    CHECK(report.bytes_read < 200 * 1024);  // big.log is 2.2 MB
    ::paglets::test::report("log_scout", "hosts searched", 2);
    ::paglets::test::report("log_scout", "bytes read of 2.2 MB of logs", report.bytes_read);
    bool end_seen = false;
    for (const auto& e : report.excerpts) end_seen = end_seen || (e.path == "big.log" && e.line == "ERROR at the end");
    CHECK(end_seen);

    // Live: the scout stays on web1 and reads only what is appended.
    log_scout_msgs::Scout live;
    live.hosts = {"web1"};
    live.root = "logs";
    live.patterns = {"error"};
    live.live = true;
    live.interval_ms = 100;
    const auto lid = launch('b', live);
    REQUIRE(net.settle([&] { return (report = report_of(lid)).state == "live"; }, 3000));
    CHECK_EQ(count(report, "web1", "error"), 1);
    write_text(r1 / "app.log", stamp(0min) + " ERROR new failure\n", true);
    const bool got = net.settle([&] { return (report = report_of(lid)).live_matches == 1; }, 3000);
    if (!got) {
        std::cerr << "    live: state " << report.state << ", batches " << report.live_batches << ", matches "
                  << report.live_matches << ", bytes " << report.bytes_read << "\n";
        for (auto& h : net.hosts) {
            for (const auto& p : h->f->runtime->list()) {
                if (!p.module.empty()) std::cerr << "    " << h->name << ": " << rt::to_string(p.state) << "\n";
            }
        }
    }
    REQUIRE(got);
    CHECK_EQ(count(report, "web1", "error"), 2);
    CHECK(report.excerpts.back().line.find("ERROR new failure") != std::string::npos);
    CHECK_EQ(report.excerpts.back().host_name, std::string("web1"));
    // A line still being written counts once it is complete.
    write_text(r1 / "app.log", "ERROR half", true);
    const auto batches = report.live_batches;
    REQUIRE(net.settle([&] { return (report = report_of(lid)).live_batches >= batches + 2; }, 3000));
    CHECK_EQ(report.live_matches, 1);
    write_text(r1 / "app.log", " written\n", true);
    REQUIRE(net.settle([&] { return (report = report_of(lid)).live_matches == 2; }, 3000));
    CHECK_EQ(report.excerpts.back().line, std::string("ERROR half written"));
    ::paglets::test::report("log_scout", "live checks while following", report.live_batches);
    ::paglets::test::report("log_scout", "new lines found live", report.live_matches);

    // After `stop` the scout ends at its next check.
    auto stopped = home.f->runtime->call(lid, "stop", {}, 10s);
    REQUIRE(stopped.status == 0);
    REQUIRE(net.settle([&] {
        std::size_t user = 0;
        for (auto& h : net.hosts) {
            for (const auto& p : h->f->runtime->list()) user += p.module.empty() ? 0 : 1;
        }
        return user == 2;  // the two paglets at home
    }, 3000));
    std::error_code ec;
    for (const auto& p : {r1, r2}) fs::remove_all(p, ec);
}

PAGLETS_TEST("example: Mesh Benchmark measures every host in a compute slot and ranks them") {
    Mesh net;
    auto& a = net.add_host("a");
    net.add_host("b");
    net.add_host("c");
    net.enroll_all();
    fast(net);
    for (auto& h : net.hosts) h->node->set_compute_slots(1);
    REQUIRE(net.settle([&] { return net.converged() && a.node->landscape().size() == 3u; }));
    const std::string module = a.f->module("benchmark.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('b')));
    REQUIRE_OK(created);
    const auto id = *created;
    benchmark_msgs::Bench q;
    q.budget_ms = 30;
    q.memory_mb = 2;
    q.disk_mb = 2;
    auto r = a.f->runtime->call(id, "start", wire::to_msgpack(q), 20s);
    REQUIRE(r.status == 0);
    benchmark_msgs::Started started;
    REQUIRE(wire::from_msgpack(r.payload, started));
    CHECK(started.accepted);
    CHECK_EQ(started.hosts, 3);
    benchmark_msgs::Ranking ranking;
    const bool done = net.settle(
        [&] {
            auto x = a.f->runtime->call(id, "ranking", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, ranking) && ranking.state == "done";
        },
        3000);
    if (!done) {
        std::cerr << "    ranking: " << ranking.state << ", " << ranking.hosts.size() << " hosts, "
                  << ranking.errors.size() << " errors\n";
        for (const auto& h : ranking.hosts) std::cerr << "    reported: " << h.host_name << " slot " << h.slot << " " << h.error << "\n";
        for (auto& h : net.hosts) {
            const auto m = h->node->move_stats();
            std::cerr << "    " << h->name << ": out " << m.moves_out << ", in " << m.moves_in << ", failed " << m.moves_failed << "\n";
        }
        for (auto& h : net.hosts) {
            for (const auto& p : h->f->runtime->list()) {
                if (!p.module.empty()) std::cerr << "    " << h->name << ": " << p.id.substr(0, 8) << " " << rt::to_string(p.state) << "\n";
            }
        }
    }
    REQUIRE(done);
    for (const auto& e : ranking.errors) std::cerr << "    benchmark: " << e << "\n";
    CHECK(ranking.errors.empty());
    REQUIRE(ranking.hosts.size() == 3u);
    std::set<std::string> names;
    for (std::size_t i = 0; i < ranking.hosts.size(); ++i) {
        const auto& h = ranking.hosts[i];
        if (!h.error.empty()) std::cerr << "    benchmark " << h.host_name << ": " << h.error << "\n";
        CHECK(h.error.empty());
        names.insert(h.host_name);
        CHECK(h.slot);  // in a compute slot of its own
        CHECK(h.int_mops > 0);
        CHECK(h.float_mflops > 0);
        CHECK(h.memory_mbs > 0);
        CHECK(h.disk_write_mbs > 0);
        CHECK(h.disk_read_mbs > 0);
        CHECK(h.bench_ms >= 3 * q.budget_ms);
        CHECK(!h.os.empty());
        if (i > 0) CHECK(h.int_mops <= ranking.hosts[i - 1].int_mops);
    }
    CHECK(names == std::set<std::string>({"a", "b", "c"}));
    report("benchmark", "hosts measured, each in a compute slot", ranking.hosts.size());
    report("benchmark", "integer speed of the fastest host (fast interpreter)", ranking.hosts[0].int_mops, "Mops");
    report("benchmark", "floating point of the fastest host", ranking.hosts[0].float_mflops, "Mflops");
    report("benchmark", "memory copying of the fastest host", ranking.hosts[0].memory_mbs, "MB/s");
    // The clones end after their report; their slots are free again.
    REQUIRE(net.settle([&] {
        std::size_t user = 0;
        for (auto& h : net.hosts) {
            for (const auto& p : h->f->runtime->list()) user += p.module.empty() ? 0 : 1;
        }
        return user == 1;
    }, 3000));
}

PAGLETS_TEST("example: Mesh Benchmark rejects missing and corrupt storage blocks") {
    class BrokenStorage final : public rt::SystemPaglet {
    public:
        explicit BrokenStorage(int mode) : mode_(mode) {}
        std::string_view name() const override { return "storage"; }
        std::vector<std::string> default_ops() const override { return {"put", "get", "delete"}; }
        void handle(rt::SystemContext&, rt::ServiceCall& call) override {
            namespace storage = paglets::services::storage;
            if (call.name() == "get") {
                storage::GetReply reply;
                reply.found = mode_ != 0;
                if (mode_ == 1) reply.value = {0};
                if (mode_ == 2) {
                    reply.value.assign(256 * 1024, 0x5a);
                    reply.value.front() = 0;
                    reply.value.back() = 0x00;
                }
                (void)call.reply(0, wire::to_msgpack(reply));
            } else if (call.name() == "put") {
                (void)call.reply(0, wire::to_msgpack(storage::Usage{}));
            } else {
                (void)call.reply(0, wire::to_msgpack(storage::DeleteReply{}));
            }
        }
    private:
        int mode_;
    };
    for (int mode = 0; mode != 3; ++mode) {
        Fixture f;
        REQUIRE_OK(f.runtime->add_system_paglet(std::make_shared<BrokenStorage>(mode)));
        const auto id = f.create("benchmark.wasm");
        benchmark_msgs::Bench input;
        input.budget_ms = 1;
        input.memory_mb = 1;
        input.disk_mb = 1;
        REQUIRE(f.call(id, "start", wire::to_msgpack(input)).status == 0);
        benchmark_msgs::Ranking ranking;
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            auto reply = f.call(id, "ranking");
            if (reply.status == 0 && wire::from_msgpack(reply.payload, ranking) && ranking.state == "done") break;
            std::this_thread::sleep_for(10ms);
        }
        REQUIRE(ranking.state == "done");
        REQUIRE(ranking.hosts.size() == 1u);
        CHECK(ranking.hosts.front().error == "storage get: missing or corrupted benchmark block");
        CHECK(ranking.hosts.front().disk_read_mbs == 0);
    }
}

PAGLETS_TEST("example: Mesh Benchmark compares the same job in the interpreter and AOT") {
    Fixture f;
    const auto id = f.create("benchmark.wasm");
    benchmark_msgs::JobBench q{3'000'000, 2};
    auto reply = f.runtime->call(id, "jobs", wire::to_msgpack(q), 30s);
    REQUIRE(reply.status == 0);
    benchmark_msgs::JobComparison interpreted;
    REQUIRE(wire::from_msgpack(reply.payload, interpreted));
    REQUIRE(interpreted.error.empty());
    REQUIRE(interpreted.samples.size() == 2u);
    CHECK(!interpreted.samples[0].aot);
    CHECK(interpreted.samples[0].check == interpreted.samples[1].check);
    report("benchmark_jobs", "integer job, interpreter", interpreted.samples[0].elapsed_ms, "ms");
#ifdef PAGLETS_TEST_WAMRC
    rt::Config c;
    c.aot_compiler = PAGLETS_TEST_WAMRC;
    Fixture compiled(c);
    const auto other = compiled.create("benchmark.wasm");
    REQUIRE(compiled.runtime->compile_job_module(compiled.runtime->info(other)->module, 300s));
    reply = compiled.runtime->call(other, "jobs", wire::to_msgpack(q), 30s);
    REQUIRE(reply.status == 0);
    benchmark_msgs::JobComparison native;
    REQUIRE(wire::from_msgpack(reply.payload, native));
    REQUIRE(native.error.empty());
    REQUIRE(native.samples.size() == 2u);
    CHECK(native.samples[0].aot);
    CHECK(native.samples[0].check == interpreted.samples[0].check);
    report("benchmark_jobs", "integer job, AOT", native.samples[0].elapsed_ms, "ms");
#endif
}

PAGLETS_TEST("example: Pi writes its digits to files in order and converts them into decimal digits") {
    namespace fs = std::filesystem;
    const fs::path results = temp_root("pi-results");
    ps::ServicesConfig home_sc;
    home_sc.roots["results"] = results;
    Mesh net;
    auto& home = net.add_host("home", home_sc);
    net.add_host("b");
    net.add_host("c");
    net.enroll_all();
    fast(net);
    net.admin_record("policy-rule", data::policy_rule({"results", Decision::allow, "files", {"read", "write", "create"}, {},
                                                       Scope{std::vector<std::string>{"results"}, std::nullopt},
                                                       std::nullopt, 0}));
    REQUIRE(net.settle([&] { return net.converged() && home.node->landscape().size() == 3u; }));
    const std::string module = home.f->module("pi.wasm");
    auto created = home.node->create(module, net.passport(module, paglet_id('9')));
    REQUIRE_OK(created);
    const auto id = *created;
    pi_msgs::Start start;
    start.digits = 3000;
    start.chunk = 100;
    start.max_in_flight = 6;
    start.output_root = "results";
    start.output_path = "run/pi";
    auto r = home.f->runtime->call(id, "start", wire::to_msgpack(start), 20s);
    REQUIRE(r.status == 0);
    pi_msgs::Started started;
    REQUIRE(wire::from_msgpack(r.payload, started));
    REQUIRE(started.accepted);
    pi_msgs::Status st;
    REQUIRE(net.settle(
        [&] {
            auto x = home.f->runtime->call(id, "status", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, st) && (st.state == "done" || st.state == "failed");
        },
        3000));
    if (st.state != "done") std::cerr << "    pi: " << st.error << "\n";
    REQUIRE(st.state == "done");
    auto read_text = [](const fs::path& p) {
        std::ifstream in(p, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(in), {});
    };
    // Every digit in the files, in order; the status shows the first.
    const std::string hex = read_text(results / "run" / "pi.hex");
    CHECK(hex == "3." + pi_bbp::hex_digits(0, 3000));
    const std::string decimal = read_text(results / "run" / "pi.txt");
    CHECK_EQ(st.decimal_done, 3610);  // 3000 * log10(16), less two
    CHECK_EQ(decimal.size(), std::size_t{3612});
    CHECK(decimal.starts_with("3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679"));
    CHECK(decimal == st.decimal);  // fewer than `shown` digits: all of them
    CHECK(st.files == std::vector<std::string>({"run/pi.hex", "run/pi.txt"}));
    report("pi", "hex digits written to a file in order", static_cast<std::int64_t>(hex.size() - 2));
    report("pi", "decimal digits written to a file", static_cast<std::int64_t>(decimal.size() - 2));
    // A conversion without files that would not fit a message is refused.
    auto big = home.node->create(module, net.passport(module, paglet_id('8')));
    REQUIRE_OK(big);
    pi_msgs::Start huge;
    huge.digits = 100'000'000;
    r = home.f->runtime->call(*big, "start", wire::to_msgpack(huge), 20s);
    REQUIRE(r.status == 0);
    REQUIRE(wire::from_msgpack(r.payload, started));
    CHECK(!started.accepted);
    std::error_code ec;
    fs::remove_all(results, ec);
}
