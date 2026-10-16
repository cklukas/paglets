// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "runtime_fixture.hpp"
#include "test.hpp"
#include "../host/src/services/services.hpp"

#include <explorer_msgs.hpp>
#include <paglets/wire/reflect.hpp>

#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <set>

using namespace paglets::test;
namespace ps = paglets::services;
namespace wire = paglets::wire;
namespace fs = std::filesystem;

namespace {

struct Gate {
    std::mutex mu;
    std::condition_variable cv;
    bool entered = false;
    bool released = false;
    void wait() {
        std::unique_lock lock(mu);
        entered = true;
        cv.notify_all();
        cv.wait_for(lock, 15s, [&] { return released; });
    }
    bool started() {
        std::unique_lock lock(mu);
        return cv.wait_for(lock, 5s, [&] { return entered; });
    }
    void open() { std::lock_guard lock(mu); released = true; cv.notify_all(); }
};
struct Release { Gate& gate; ~Release() { gate.open(); } };

struct Scans {
    fs::path root = fs::temp_directory_path() / ("paglets-scans-" + ps::impl::random_hex(8));
    std::unique_ptr<Fixture> f;
    rt::PagletId id;
    rt::Cap cap;
    std::int32_t dir = 0;

    explicit Scans(std::function<void()> slow = {}) {
        fs::create_directories(root);
        rt::Config config;
        config.threads = 1;  // a blocking service would freeze every request
        config.handler_budget = 5s;  // explicitly enable the host watchdog for this regression
        f = std::make_unique<Fixture>(config);
        REQUIRE_OK(f->runtime->add_system_paglet(std::make_shared<ps::impl::Directory>(fs::path{}, ps::ServicePolicy{})));
        REQUIRE_OK(f->runtime->add_system_paglet(std::make_shared<ps::impl::Files>(
            std::map<std::string, fs::path>{{"data", root}}, std::move(slow))));
        id = f->create("explorer.wasm");
        cap.kind = rt::Cap::Kind::resource;
        cap.target = "system.files";
        cap.resource_type = "dir";
        cap.resource = "data";
        cap.ops = {"read"};
        cap.id = ps::impl::random_hex(16);
        auto h = f->runtime->add_capability(id, cap);
        REQUIRE_OK(h);
        dir = *h;
    }
    ~Scans() { f.reset(); fs::remove_all(root); }

    explorer::ScanCommand command(std::string operation, auto request, bool lend = false) const {
        return {lend ? dir : 0, std::move(operation), wire::to_msgpack(request)};
    }
    explorer::ScanResult ask(const explorer::ScanCommand& q, const std::string& caller = {}) {
        auto r = f->runtime->call(caller.empty() ? id : caller, "scan_call", wire::to_msgpack(q), 10s);
        REQUIRE(r.status == abi::ok);
        explorer::ScanResult result;
        REQUIRE(wire::from_msgpack(r.payload, result));
        return result;
    }
    std::string start(ps::files::FindRequest query = {}, std::int64_t timeout = 30 * 60 * 1000) {
        const auto r = ask(command("scan_start", ps::files::ScanRequest{query, timeout}, true));
        REQUIRE(r.status == abi::ok);
        ps::files::ScanStarted started;
        REQUIRE(wire::from_msgpack(r.payload, started));
        return started.id;
    }
    template <class T> T value(const explorer::ScanResult& r) {
        REQUIRE(r.status == abi::ok);
        T v;
        REQUIRE(wire::from_msgpack(r.payload, v));
        return v;
    }
};

}  // namespace

PAGLETS_TEST("file tasks: bounded batches, progress through nonmatches, ownership and complete traversal") {
    Scans s;
    for (int i = 0; i < 430; ++i) std::ofstream(s.root / (std::to_string(i) + ".csv")) << "data";
    ps::files::FindRequest query;
    query.pattern = "**/*.csv";
    query.limit = 1;  // a streaming task must not truncate at the one-reply API's limit
    const auto id = s.start(query);
    const auto foreign = s.f->create("explorer.wasm");
    CHECK(s.ask(s.command("scan_next", ps::files::ScanId{id}), foreign).status == abi::denied);
    CHECK(s.ask(s.command("scan_cancel", ps::files::ScanId{id}), foreign).status == abi::denied);
    std::set<std::string> paths;
    std::uint64_t previous = 0;
    std::size_t batches = 0;
    while (true) {
        auto batch = s.value<ps::files::ScanBatch>(s.ask(s.command("scan_next", ps::files::ScanId{id})));
        CHECK(batch.entries.size() <= 128);
        CHECK(batch.completed >= previous);
        previous = batch.completed;
        for (const auto& e : batch.entries) CHECK(paths.insert(e.path).second);
        ++batches;
        if (batch.finished) { CHECK_EQ(batch.matched, 430u); break; }
        REQUIRE(batches < 20);
    }
    CHECK_EQ(paths.size(), 430u);
    CHECK(batches >= 4);
    CHECK_EQ(previous, 430u);
    CHECK(s.ask(s.command("scan_next", ps::files::ScanId{id})).status == abi::not_found);
    query.pattern = "**/*.none";
    const auto empty = s.start(query);
    auto no_matches = s.value<ps::files::ScanBatch>(s.ask(s.command("scan_next", ps::files::ScanId{empty})));
    CHECK(no_matches.entries.empty());
    CHECK_EQ(no_matches.completed, 430u);
    CHECK(no_matches.finished);
}

PAGLETS_TEST("file tasks: I/O longer than the five-second watchdog keeps a single lane responsive") {
    Gate gate;
    Scans s([&] { gate.wait(); });
    Release release{gate};
    std::ofstream(s.root / "file") << "data";
    const auto id = s.start();
    auto pending = s.f->runtime->request(s.id, "scan_call", wire::to_msgpack(s.command("scan_next", ps::files::ScanId{id})), 12s);
    REQUIRE(gate.started());
    CHECK(s.f->runtime->call(s.id, "info", {}, 1s).status == abi::ok);
    const auto began = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(5200ms);
    CHECK(s.f->runtime->call(s.id, "info", {}, 1s).status == abi::ok);
    CHECK(std::chrono::steady_clock::now() - began >= 5s);
    gate.open();
    REQUIRE(pending.wait_for(3s) == std::future_status::ready);
    const auto reply = pending.get();
    CHECK(reply.status == abi::ok);
    explorer::ScanResult result;
    REQUIRE(wire::from_msgpack(reply.payload, result));
    CHECK(s.value<ps::files::ScanBatch>(result).finished);
}

PAGLETS_TEST("file tasks: cancellation of busy work, duplicate pulls, deadlines, revocation and quota cleanup") {
    Gate gate;
    Scans s([&] { gate.wait(); });
    Release release{gate};
    const auto id = s.start();
    auto pending = s.f->runtime->request(s.id, "scan_call", wire::to_msgpack(s.command("scan_next", ps::files::ScanId{id})), 10s);
    REQUIRE(gate.started());
    CHECK(s.ask(s.command("scan_next", ps::files::ScanId{id})).status == abi::bad_state);
    CHECK(s.value<ps::files::ScanCancelled>(s.ask(s.command("scan_cancel", ps::files::ScanId{id}))).cancelled);
    CHECK(!s.value<ps::files::ScanCancelled>(s.ask(s.command("scan_cancel", ps::files::ScanId{id}))).cancelled);
    gate.open();
    REQUIRE(pending.wait_for(3s) == std::future_status::ready);
    explorer::ScanResult cancelled;
    REQUIRE(wire::from_msgpack(pending.get().payload, cancelled));
    CHECK(cancelled.status == abi::cancelled);
    const auto expired = s.start({}, 1);
    std::this_thread::sleep_for(5ms);
    CHECK(s.ask(s.command("scan_next", ps::files::ScanId{expired})).status == abi::timeout);
    std::vector<std::string> ids;
    for (int i = 0; i < 4; ++i) ids.push_back(s.start());
    CHECK(s.ask(s.command("scan_start", ps::files::ScanRequest{}, true)).status == abi::quota);
    for (const auto& scan : ids) CHECK(s.ask(s.command("scan_cancel", ps::files::ScanId{scan})).status == abi::ok);
    const auto revoked = s.start();
    s.f->runtime->revoke_capability(s.cap.id);
    CHECK(s.ask(s.command("scan_next", ps::files::ScanId{revoked})).status == abi::revoked);
}

PAGLETS_TEST("file tasks: common Task handle delivers progress, cancels once and cleans up when its paglet ends") {
    Scans s;
    for (int i = 0; i < 600; ++i) std::ofstream(s.root / std::to_string(i)) << "data";
    CHECK(s.f->runtime->call(s.id, "scan_task", wire::to_msgpack(explorer::Explore{s.dir, "**"}), 2s).status == abi::ok);
    explorer::TaskProbe probe;
    const auto until = std::chrono::steady_clock::now() + 5s;
    do {
        auto r = s.f->runtime->call(s.id, "task_status", {}, 2s);
        REQUIRE(wire::from_msgpack(r.payload, probe));
    } while (!probe.done && std::chrono::steady_clock::now() < until);
    CHECK(probe.done);
    CHECK(probe.state == "completed");
    CHECK(probe.updates >= 5);
    CHECK_EQ(probe.visited, 600);
    const auto abandoned = s.start();
    REQUIRE_OK(s.f->runtime->dispose(s.id));
    s.f->runtime->wait_idle();
    auto cancel = s.f->runtime->call("system.files", "scan_cancel", wire::to_msgpack(ps::files::ScanId{abandoned}), 2s);
    CHECK(cancel.status == abi::ok);
    ps::files::ScanCancelled result;
    REQUIRE(wire::from_msgpack(cancel.payload, result));
    CHECK(!result.cancelled);  // already reaped on paglet_ended
}

PAGLETS_TEST("file tasks: common TaskHandle cancels an in-flight service task") {
    Gate gate;
    Scans s([&] { gate.wait(); });
    Release release{gate};
    CHECK(s.f->runtime->call(s.id, "scan_task", wire::to_msgpack(explorer::Explore{s.dir, "**"}), 2s).status == abi::ok);
    REQUIRE(gate.started());
    CHECK(s.f->runtime->call(s.id, "task_cancel", {}, 1s).status == abi::ok);
    CHECK(s.f->runtime->call(s.id, "task_cancel", {}, 1s).status == abi::ok);
    explorer::TaskProbe probe;
    REQUIRE(wire::from_msgpack(s.f->runtime->call(s.id, "task_status", {}, 1s).payload, probe));
    CHECK(probe.done);
    CHECK(probe.state == "cancelled");
    CHECK(probe.error == abi::cancelled);
    gate.open();
}

PAGLETS_TEST("file tasks: long scan workflow reports progress while the agent stays responsive") {
    const bool extended = std::getenv("PAGLETS_TEST_LONG_SCANS") != nullptr;
    Scans s([&] { std::this_thread::sleep_for(extended ? 3000ms : 55ms); });
    for (int i = 0; i < 41; ++i) std::ofstream(s.root / std::to_string(i)) << "data";
    const auto began = std::chrono::steady_clock::now();
    REQUIRE(s.f->runtime->call(s.id, "scan_task", wire::to_msgpack(explorer::Explore{s.dir, "**"}), 2s).status == abi::ok);
    explorer::TaskProbe probe;
    const auto until = began + (extended ? 150s : 15s);
    do {
        REQUIRE(wire::from_msgpack(s.f->runtime->call(s.id, "task_status", {}, 1s).payload, probe));
        CHECK(s.f->runtime->call(s.id, "info", {}, 1s).status == abi::ok);
        if (probe.done) break;
        std::this_thread::sleep_for(100ms);
    } while (std::chrono::steady_clock::now() < until);
    CHECK(probe.state == "completed");
    CHECK(probe.done);
    CHECK(probe.updates > 1);
    CHECK_EQ(probe.visited, 41);
    if (extended) CHECK(std::chrono::steady_clock::now() - began >= 120s);
}
