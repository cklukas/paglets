// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Demo paglets (WP20, planning/cpp-demo-paglets.md) on meshes of several
// hosts.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <finder_msgs.hpp>
#include <benchmark_msgs.hpp>
#include <dupes_msgs.hpp>
#include <file_courier_msgs.hpp>
#include <log_scout_msgs.hpp>
#include <tree_compare_msgs.hpp>
#include <guard_msgs.hpp>
#include <inventory_msgs.hpp>
#include <journey_msgs.hpp>
#include <latency_msgs.hpp>
#include <seek_msgs.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wire/reflect.hpp>

#include <chrono>
#include <filesystem>
#include <format>
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
    for (std::size_t i = 0; i < score.catches.size(); ++i) {
        const auto& c = score.catches[i];
        CHECK(c.error.empty());
        CHECK(!c.host_name.empty());
        // It was caught again only after it had moved on.
        if (i > 0) CHECK(c.moves > score.catches[i - 1].moves);
    }
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

PAGLETS_TEST("demo: File Courier carries files between hosts with checksums, within data residency") {
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
    REQUIRE(done.files.size() == 2u);
    for (const auto& f : done.files) CHECK(f.verified);
    CHECK_EQ(done.bytes, static_cast<std::int64_t>(table.size() + 24));
    CHECK_EQ(read_text(inbox / "from-lab" / "results" / "squares.csv"), table);
    CHECK_EQ(read_text(inbox / "from-lab" / "results" / "run-2" / "log.txt"), std::string("converged after 12 steps"));
    CHECK(!fs::exists(inbox / "from-lab" / "notes.md"));
    for (const auto& f : done.files) {
        if (f.path == "results/squares.csv") CHECK_EQ(f.sha256, paglets::to_hex(paglets::sha256(text(table))));
    }

    // Content of a host-only root may not leave the lab: the courier stays
    // there, and nothing reaches the archive.
    const auto [refused, stayed] = run('d', "clinic");
    CHECK_EQ(refused.state, std::string("refused"));
    CHECK(refused.error.find("data residency") != std::string::npos);
    CHECK(stayed == &lab);
    CHECK(!fs::exists(inbox / "from-lab" / "results" / "patients.csv"));

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

PAGLETS_TEST("demo: Tree Compare finds missing and different files on several hosts") {
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
    std::error_code ec;
    for (const auto& p : {ra, rb, rc}) fs::remove_all(p, ec);
}

PAGLETS_TEST("demo: Log Scout searches logs in a time window and follows them live") {
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

PAGLETS_TEST("demo: Mesh Benchmark measures every host in a compute slot and ranks them") {
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
    // The clones end after their report; their slots are free again.
    REQUIRE(net.settle([&] {
        std::size_t user = 0;
        for (auto& h : net.hosts) {
            for (const auto& p : h->f->runtime->list()) user += p.module.empty() ? 0 : 1;
        }
        return user == 1;
    }, 3000));
}
