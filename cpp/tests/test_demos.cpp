// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Demo paglets (WP20, planning/cpp-demo-paglets.md) on meshes of several
// hosts.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <finder_msgs.hpp>
#include <dupes_msgs.hpp>
#include <guard_msgs.hpp>
#include <inventory_msgs.hpp>
#include <journey_msgs.hpp>
#include <latency_msgs.hpp>
#include <seek_msgs.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wire/reflect.hpp>

#include <filesystem>
#include <fstream>
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

PAGLETS_TEST("demo: Mesh Journey visits every host and comes home with its journal") {
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
}

PAGLETS_TEST("demo: Mesh File Finder and Storage Analyzer search every host's root") {
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
    std::error_code ec;
    for (const auto& p : {ra, rb, rc}) fs::remove_all(p, ec);
    (void)b;
    (void)c;
}

PAGLETS_TEST("demo: Latency Map measures round trips between every pair of hosts") {
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

PAGLETS_TEST("demo: Inventory Collector and Process Finder ask server-info on every host") {
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
    // Process Finder: the processes named like this test's.
    inventory_msgs::Collect find;
    find.process = "paglets_tests";
    report = collect(find);
    REQUIRE(report.hosts.size() == 3u);
    for (const auto& h : report.hosts) {
        REQUIRE(!h.processes.empty());  // the hosts share this process in the test
        CHECK(h.processes[0].name.find("paglets_tests") != std::string::npos);
    }
}

PAGLETS_TEST("demo: Hide and Seek finds and pins a paglet that keeps moving") {
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
    std::set<std::string> where;
    for (const auto& c : score.catches) {
        CHECK(c.error.empty());
        CHECK(!c.host_name.empty());
        where.insert(c.host_name);
    }
    CHECK(where.size() >= 2u);  // it was found on more than one host
    CHECK(score.hider_moves >= 3);
}

PAGLETS_TEST("demo: Volume Guard sleeps between checks and alerts its owner") {
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
}

PAGLETS_TEST("demo: Duplicate Finder finds the same content on different hosts") {
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
    std::error_code ec;
    for (const auto& p : {ra, rb, rc}) fs::remove_all(p, ec);
}
