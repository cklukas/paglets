// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Host runtime and paglet ABI v1 conformance (planning/cpp-abi-v1.md,
// section 11). Test names carry the conformance IDs.

#include "test.hpp"

#include <conformance_cmd.hpp>
#include <paglets/abi.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/wasm/binary.hpp>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <filesystem>
#include <mutex>
#include <random>
#include <thread>

#ifndef _WIN32
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace rt = paglets::runtime;
namespace pw = paglets::wasm;
namespace abi = paglets::abi;
using conformance::Cmd;
using namespace std::chrono_literals;

namespace {

using Bytes = std::vector<std::uint8_t>;

Bytes text(std::string_view s) {
    return Bytes(s.begin(), s.end());
}

template <class T>
Bytes enc(const T& v) {
    return abi::encode(v);
}

template <class T>
T dec(const Bytes& b) {
    T v{};
    if (!abi::decode(b, v)) throw std::runtime_error("reply does not decode");
    return v;
}

struct Fixture {
    explicit Fixture(rt::Config config = {}) {
        config.log = [this](const rt::LogRecord& r) {
            std::lock_guard lock(log_mu);
            if (std::getenv("PAGLETS_TEST_LOG") != nullptr) std::cerr << "    log: " << r.text << "\n";
            log.push_back(r.text);
        };
        if (config.threads == 2) config.threads = 3;
        // The whole suite also runs with worker processes (CTest unit_workers).
        if (const char* worker = std::getenv("PAGLETS_TEST_WORKER"); worker != nullptr && *worker != '\0') {
            config.worker_executable = worker;
        }
        runtime = std::make_unique<rt::Runtime>(std::move(config));
    }

    std::string module(const std::string& file) {
        const auto path = paglets::test::guest_path(file);
        if (path.empty()) paglets::test::skip(file + " not available");
        auto hash = runtime->add_module_file(path);
        if (!hash) throw std::runtime_error(hash.error());
        return *hash;
    }

    rt::PagletId create(const std::string& file, Bytes args = {}) {
        auto id = runtime->create(module(file), rt::CreateOptions{std::move(args)});
        if (!id) throw std::runtime_error(id.error());
        return *id;
    }

    rt::Reply call(const rt::PagletId& id, std::string_view name, Bytes payload = {}) {
        return runtime->call(id, name, std::move(payload), 10s);
    }

    rt::Reply cmd(const rt::PagletId& id, std::string_view name, const Cmd& c) { return call(id, name, enc(c)); }

    std::int64_t code(const rt::PagletId& id, std::string_view name, const Cmd& c) {
        auto r = cmd(id, name, c);
        if (r.status != 0) throw std::runtime_error("status " + std::string(abi::error_name(r.status)));
        return dec<std::int64_t>(r.payload);
    }

    std::vector<std::string> journal(const rt::PagletId& id) {
        runtime->wait_idle();
        auto r = call(id, "journal");
        if (r.status != 0) throw std::runtime_error("journal: " + std::string(abi::error_name(r.status)));
        return dec<std::vector<std::string>>(r.payload);
    }

    // The paglet ID behind an endpoint handle held by `holder`.
    rt::PagletId target_of(const rt::PagletId& holder, std::int32_t handle) {
        auto r = cmd(holder, "inspect", Cmd{.handle = handle});
        if (r.status != 0) throw std::runtime_error("inspect failed");
        const auto info = dec<abi::CapInfo>(r.payload);
        return info.target.substr(std::string("paglet:").size());
    }

    bool logged(std::string_view needle) {
        std::lock_guard lock(log_mu);
        return std::ranges::any_of(log, [&](const std::string& l) { return l.find(needle) != std::string::npos; });
    }

    std::mutex log_mu;
    std::vector<std::string> log;
    std::unique_ptr<rt::Runtime> runtime;
};

bool contains(const std::vector<std::string>& v, std::string_view s) {
    return std::ranges::find(v, s) != v.end();
}

std::ptrdiff_t index_of(const std::vector<std::string>& v, std::string_view s) {
    auto it = std::ranges::find(v, s);
    return it == v.end() ? -1 : it - v.begin();
}

pw::ModuleInfo paglet_info() {
    pw::ModuleInfo info;
    for (auto name : abi::required_exports) {
        info.exports.push_back(
            {std::string(name), name == "memory" ? pw::ExternKind::memory : pw::ExternKind::function, 0});
    }
    return info;
}

}  // namespace

// ---------------------------------------------------------------------------
// Module checks

PAGLETS_TEST("abi C01: a module without paglets_abi_v1 is rejected") {
    Fixture f;
    const auto path = paglets::test::guest_path("testbed.wasm");
    if (path.empty()) paglets::test::skip("testbed.wasm not available");
    auto r = f.runtime->add_module_file(path);
    REQUIRE(!r.has_value());
    CHECK(r.error().find("paglets_abi_v1") != std::string::npos);
}

PAGLETS_TEST("abi C02: a module declaring only unknown ABI versions is rejected") {
    auto info = paglet_info();
    std::erase_if(info.exports, [](const pw::Export& e) { return e.name == "paglets_abi_v1"; });
    info.exports.push_back({"paglets_abi_v7", pw::ExternKind::function, 0});
    auto r = rt::check_paglet_module(info, rt::TrustClass::roaming);
    REQUIRE(!r.has_value());
    CHECK(r.error().find("no supported paglet ABI version") != std::string::npos);
    CHECK(rt::check_paglet_module(paglet_info(), rt::TrustClass::roaming).has_value());
}

PAGLETS_TEST("abi C03: an unknown paglets import is rejected, naming the import") {
    auto info = paglet_info();
    info.imports.push_back({"paglets", "teleport", pw::ExternKind::function});
    auto r = rt::check_paglet_module(info, rt::TrustClass::roaming);
    REQUIRE(!r.has_value());
    CHECK(r.error().find("paglets.teleport") != std::string::npos);
}

PAGLETS_TEST("abi C04: roaming modules cannot import paglets_sys") {
    auto info = paglet_info();
    info.imports.push_back({"paglets_sys", "call", pw::ExternKind::function});
    CHECK(!rt::check_paglet_module(info, rt::TrustClass::roaming).has_value());
    CHECK(!rt::check_paglet_module(info, rt::TrustClass::resident).has_value());
    CHECK(rt::check_paglet_module(info, rt::TrustClass::system).has_value());
}

PAGLETS_TEST("abi C05: a module missing a required export is rejected") {
    auto info = paglet_info();
    std::erase_if(info.exports, [](const pw::Export& e) { return e.name == "paglets_on_message"; });
    auto r = rt::check_paglet_module(info, rt::TrustClass::roaming);
    REQUIRE(!r.has_value());
    CHECK(r.error().find("paglets_on_message") != std::string::npos);
}

// ---------------------------------------------------------------------------
// Delivery

PAGLETS_TEST("abi C06: created comes first, once, with args and caps") {
    Fixture f;
    auto id = f.create("conformance.wasm", text("hi"));
    CHECK_EQ(f.call(id, "echo").status, 0);
    auto j = f.journal(id);
    REQUIRE(!j.empty());
    CHECK_EQ(j[0], std::string("event:created:hi:0"));
    CHECK_EQ(std::ranges::count_if(j, [](const std::string& e) { return e.starts_with("event:created"); }), 1);
}

PAGLETS_TEST("abi C07: messages are delivered by priority, FIFO within a priority") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    auto busy = f.runtime->request(id, "busy", enc(Cmd{.ms = 300}));
    std::this_thread::sleep_for(100ms);  // the paglet is inside "busy" now
    REQUIRE(f.runtime->send(id, "a", {}, 1));
    REQUIRE(f.runtime->send(id, "b", {}, 5));
    REQUIRE(f.runtime->send(id, "c", {}, 1));
    REQUIRE(f.runtime->send(id, "d", {}, 7));
    CHECK_EQ(busy.get().status, 0);
    auto j = f.journal(id);
    const auto at = index_of(j, "msg:busy");
    REQUIRE(at >= 0 && j.size() >= static_cast<std::size_t>(at) + 5);
    CHECK_EQ(j[at + 1], std::string("msg:d"));
    CHECK_EQ(j[at + 2], std::string("msg:b"));
    CHECK_EQ(j[at + 3], std::string("msg:a"));
    CHECK_EQ(j[at + 4], std::string("msg:c"));
}

PAGLETS_TEST("abi C08: request and reply between paglets") {
    Fixture f;
    auto a = f.create("conformance.wasm");
    const auto child = f.code(a, "child", Cmd{});
    REQUIRE(child > 1);
    const auto correlation =
        f.code(a, "request", Cmd{.handle = static_cast<std::int32_t>(child), .name = "echo", .payload = text("ping")});
    CHECK(correlation > 0);
    auto j = f.journal(a);
    CHECK(contains(j, "reply:ok:ping"));
    // Host requests get the payload back as well.
    auto r = f.call(a, "echo", text("direct"));
    CHECK_EQ(r.status, 0);
    CHECK(r.payload == text("direct"));
}

PAGLETS_TEST("abi C09: an unhandled request is answered unknown_message") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(f.call(id, "no_such_message").status, static_cast<std::int32_t>(abi::unknown_message));
}

PAGLETS_TEST("abi C10: an error returned by the handler becomes the reply status") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(f.cmd(id, "fail", Cmd{.code = abi::denied}).status, static_cast<std::int32_t>(abi::denied));
}

PAGLETS_TEST("abi C11: a deferred reply reaches the requester") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    auto pending = f.runtime->request(id, "defer");
    f.runtime->wait_idle();
    CHECK_EQ(f.code(id, "answer", Cmd{.payload = text("late")}), 0);
    auto r = pending.get();
    CHECK_EQ(r.status, 0);
    CHECK(r.payload == text("late"));
}

PAGLETS_TEST("abi C12: request timeout; a later reply is discarded") {
    Fixture f;
    auto a = f.create("conformance.wasm");
    const auto child = static_cast<std::int32_t>(f.code(a, "child", Cmd{}));
    const auto b = f.target_of(a, child);
    CHECK(f.code(a, "request", Cmd{.handle = child, .name = "defer", .ms = 100}) > 0);
    std::this_thread::sleep_for(300ms);
    CHECK(contains(f.journal(a), "reply:timeout:"));
    CHECK_EQ(f.code(b, "answer", Cmd{.payload = text("too late")}), 0);
    auto j = f.journal(a);
    CHECK_EQ(std::ranges::count_if(j, [](const std::string& e) { return e.starts_with("reply:"); }), 1);
}

PAGLETS_TEST("abi C13: a dropped reply capability answers gone") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    auto pending = f.runtime->request(id, "defer");
    f.runtime->wait_idle();
    CHECK_EQ(dec<std::int32_t>(f.call(id, "drop_deferred").payload), 0);
    CHECK_EQ(pending.get().status, static_cast<std::int32_t>(abi::gone));
}

PAGLETS_TEST("abi C14: a trap fails only that paglet; its pending requests fail") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    auto other = f.create("hello.wasm");
    auto held = f.runtime->request(id, "defer");
    f.runtime->wait_idle();
    CHECK_EQ(f.call(id, "trap").status, static_cast<std::int32_t>(abi::failed));
    CHECK_EQ(held.get().status, static_cast<std::int32_t>(abi::failed));
    CHECK(!f.runtime->info(id).has_value());
    auto end = f.runtime->ending(id);
    REQUIRE(end.has_value());
    CHECK(end->failed);
    CHECK_EQ(f.call(id, "echo").status, static_cast<std::int32_t>(abi::not_found));
    CHECK_EQ(f.call(other, "greet", enc(std::string("x"))).status, 0);
}

PAGLETS_TEST("abi C15: a handler exceeding its time budget is terminated") {
    rt::Config c;
    c.handler_budget = 300ms;
    Fixture f(std::move(c));
    auto id = f.create("conformance.wasm");
    const auto t0 = std::chrono::steady_clock::now();
    CHECK_EQ(f.call(id, "spin").status, static_cast<std::int32_t>(abi::failed));
    CHECK(std::chrono::steady_clock::now() - t0 < 5s);
    auto end = f.runtime->ending(id);
    REQUIRE(end.has_value());
    CHECK(end->reason.find("time budget") != std::string::npos);
}

// ---------------------------------------------------------------------------
// Capabilities

PAGLETS_TEST("abi C16: sending an operation the endpoint does not allow is denied") {
    Fixture f;
    auto a = f.create("conformance.wasm");
    const auto child = static_cast<std::int32_t>(f.code(a, "child", Cmd{}));
    const auto echo_only = static_cast<std::int32_t>(f.code(
        a, "derive", Cmd{.handle = child, .spec = enc(abi::DeriveSpec{.ops = std::vector<std::string>{"echo"}})}));
    REQUIRE(echo_only > 0);
    CHECK_EQ(f.code(a, "send", Cmd{.handle = echo_only, .name = "count"}), static_cast<std::int64_t>(abi::denied));
    CHECK_EQ(f.code(a, "send", Cmd{.handle = echo_only, .name = "echo"}), 0);
}

PAGLETS_TEST("abi C17: expired and used-up endpoints") {
    Fixture f;
    auto a = f.create("conformance.wasm");
    const auto child = static_cast<std::int32_t>(f.code(a, "child", Cmd{}));
    const auto brief = static_cast<std::int32_t>(
        f.code(a, "derive", Cmd{.handle = child, .spec = enc(abi::DeriveSpec{.expires_ms = 50})}));
    const auto once =
        static_cast<std::int32_t>(f.code(a, "derive", Cmd{.handle = child, .spec = enc(abi::DeriveSpec{.uses = 1})}));
    REQUIRE(brief > 0 && once > 0);
    std::this_thread::sleep_for(100ms);
    CHECK_EQ(f.code(a, "send", Cmd{.handle = brief, .name = "echo"}), static_cast<std::int64_t>(abi::expired));
    CHECK_EQ(f.code(a, "send", Cmd{.handle = once, .name = "echo"}), 0);
    CHECK_EQ(f.code(a, "send", Cmd{.handle = once, .name = "echo"}), static_cast<std::int64_t>(abi::quota));
}

PAGLETS_TEST("abi C18: derive only shrinks rights") {
    Fixture f;
    auto a = f.create("conformance.wasm");
    const auto child = static_cast<std::int32_t>(f.code(a, "child", Cmd{}));
    auto derive = [&](std::int32_t from, abi::DeriveSpec spec) {
        return f.code(a, "derive", Cmd{.handle = from, .spec = enc(spec)});
    };
    const auto narrow = static_cast<std::int32_t>(
        derive(child, {.ops = std::vector<std::string>{"echo"}, .uses = 2, .transferable = false}));
    REQUIRE(narrow > 0);
    CHECK_EQ(derive(narrow, {.ops = std::vector<std::string>{"*"}}), static_cast<std::int64_t>(abi::denied));
    CHECK_EQ(derive(narrow, {.ops = std::vector<std::string>{"count"}}), static_cast<std::int64_t>(abi::denied));
    CHECK_EQ(derive(narrow, {.uses = 5}), static_cast<std::int64_t>(abi::denied));
    CHECK_EQ(derive(narrow, {.transferable = true}), static_cast<std::int64_t>(abi::denied));
    CHECK(derive(narrow, {.uses = 1}) > 0);
    const auto badged = static_cast<std::int32_t>(derive(child, {.badge = std::string("grant-7")}));
    CHECK_EQ(derive(badged, {.badge = std::string("other")}), static_cast<std::int64_t>(abi::denied));
}

PAGLETS_TEST("abi C19: capability transfer") {
    Fixture f;
    auto a = f.create("conformance.wasm");
    const auto child = static_cast<std::int32_t>(f.code(a, "child", Cmd{}));
    const auto b = f.target_of(a, child);
    const auto sealed = static_cast<std::int32_t>(
        f.code(a, "derive", Cmd{.handle = child, .spec = enc(abi::DeriveSpec{.transferable = false})}));
    CHECK_EQ(f.code(a, "send", Cmd{.handle = child, .name = "received_caps", .caps = {sealed}}),
             static_cast<std::int64_t>(abi::denied));
    const auto gift = static_cast<std::int32_t>(f.code(
        a, "derive", Cmd{.handle = child, .spec = enc(abi::DeriveSpec{.ops = std::vector<std::string>{"echo"}})}));
    CHECK_EQ(f.code(a, "send", Cmd{.handle = child, .name = "received_caps", .caps = {gift}}), 0);
    CHECK_EQ(f.cmd(a, "inspect", Cmd{.handle = gift}).status, static_cast<std::int32_t>(abi::bad_handle));
    CHECK(contains(f.journal(b), "caps:1"));
}

PAGLETS_TEST("abi C20: a child gets exactly the capabilities passed to it") {
    Fixture f;
    auto a = f.create("conformance.wasm");
    const auto sibling = static_cast<std::int32_t>(f.code(a, "child", Cmd{}));
    const auto kid = static_cast<std::int32_t>(f.code(a, "child", Cmd{.payload = text("kid"), .caps = {sibling}}));
    const auto k = f.target_of(a, kid);
    auto j = f.journal(k);
    REQUIRE(!j.empty());
    CHECK_EQ(j[0], std::string("event:created:kid:1"));
    auto caps = dec<std::vector<std::int32_t>>(f.call(k, "caps").payload);
    CHECK_EQ(caps.size(), std::size_t{2});  // itself and the sibling, no endpoint to the creator
    CHECK_EQ(caps[0], abi::self_handle);
}

PAGLETS_TEST("abi C21: cap_inspect and cap_list") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    auto self = dec<abi::CapInfo>(f.cmd(id, "inspect", Cmd{.handle = abi::self_handle}).payload);
    CHECK_EQ(self.kind, std::string("endpoint"));
    CHECK_EQ(self.target, "paglet:" + id);
    CHECK(self.ops == std::vector<std::string>{"*"});
    CHECK(self.transferable);
    CHECK_EQ(f.cmd(id, "inspect", Cmd{.handle = 999}).status, static_cast<std::int32_t>(abi::bad_handle));
    CHECK(dec<std::vector<std::int32_t>>(f.call(id, "caps").payload) == std::vector<std::int32_t>{1});
    CHECK_EQ(f.code(id, "drop", Cmd{.handle = abi::self_handle}), static_cast<std::int64_t>(abi::denied));
}

PAGLETS_TEST("abi C22: self_info") {
    Fixture f;
    const auto hash = f.module("conformance.wasm");
    auto id = f.create("conformance.wasm");
    auto info = dec<abi::SelfInfo>(f.call(id, "info").payload);
    CHECK_EQ(info.id, id);
    CHECK_EQ(info.module, hash);
    CHECK_EQ(info.trust, std::string("roaming"));
    CHECK_EQ(info.abi, 1u);
}

PAGLETS_TEST("abi C23: timers fire; dropped timers do not") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK(f.code(id, "timer", Cmd{.name = "tick", .ms = 50}) > 0);
    const auto cancelled = static_cast<std::int32_t>(f.code(id, "timer", Cmd{.name = "never", .ms = 150}));
    CHECK_EQ(f.code(id, "drop", Cmd{.handle = cancelled}), 0);
    std::this_thread::sleep_for(400ms);
    auto j = f.journal(id);
    CHECK(contains(j, "timer:tick"));
    CHECK(!contains(j, "timer:never"));
}

// ---------------------------------------------------------------------------
// Lifecycle

PAGLETS_TEST("abi C24: deactivate and activate keep the state") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 1);
    CHECK_EQ(f.code(id, "lifecycle", Cmd{.code = static_cast<std::int32_t>(abi::LifecycleOp::deactivate)}), 0);
    f.runtime->wait_idle();
    REQUIRE(f.runtime->info(id).has_value());
    CHECK(f.runtime->info(id)->state == rt::PagletState::inactive);
    CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 2);
    CHECK(f.runtime->info(id)->state == rt::PagletState::active);
    // The host can deactivate a paglet as well.
    REQUIRE(f.runtime->deactivate(id));
    f.runtime->wait_idle();
    CHECK(f.runtime->info(id)->state == rt::PagletState::inactive);
    CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 3);
    auto j = f.journal(id);
    CHECK_EQ(std::ranges::count(j, std::string("event:deactivating")), 2);
    CHECK_EQ(std::ranges::count(j, std::string("event:activated")), 2);
}

PAGLETS_TEST("abi C24b: deactivate with wake_after_ms activates without a message") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(f.code(id, "lifecycle", Cmd{.code = static_cast<std::int32_t>(abi::LifecycleOp::deactivate), .ms = 100}),
             0);
    f.runtime->wait_idle();
    CHECK(f.runtime->info(id)->state == rt::PagletState::inactive);
    std::this_thread::sleep_for(300ms);
    f.runtime->wait_idle();
    CHECK(f.runtime->info(id)->state == rt::PagletState::active);
}

PAGLETS_TEST("abi C25: clone copies the state and receives cloned") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 1);
    const auto h = static_cast<std::int32_t>(f.code(
        id, "lifecycle", Cmd{.payload = text("copy"), .code = static_cast<std::int32_t>(abi::LifecycleOp::clone)}));
    REQUIRE(h > 1);
    const auto clone = f.target_of(id, h);
    CHECK(clone != id);
    CHECK_EQ(dec<std::int64_t>(f.call(clone, "count").payload), 2);
    CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 2);
    auto j = f.journal(clone);
    CHECK(contains(j, "event:cloned:copy"));
    CHECK(contains(j, "event:created::0"));  // the original's history is part of the copy
    auto info = dec<abi::SelfInfo>(f.call(clone, "info").payload);
    CHECK_EQ(info.id, clone);
}

PAGLETS_TEST("abi C26: dispose") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(f.code(id, "lifecycle", Cmd{.code = static_cast<std::int32_t>(abi::LifecycleOp::dispose)}), 0);
    f.runtime->wait_idle();
    CHECK(!f.runtime->info(id).has_value());
    CHECK(f.logged("conformance paglet disposing"));
    auto end = f.runtime->ending(id);
    REQUIRE(end.has_value());
    CHECK(!end->failed);
    CHECK_EQ(f.runtime->send(id, "echo").error(), static_cast<std::int32_t>(abi::not_found));
    // Disposing through the host API.
    auto other = f.create("conformance.wasm");
    CHECK(f.runtime->dispose(other).has_value());
    f.runtime->wait_idle();
    CHECK(!f.runtime->info(other).has_value());
}

PAGLETS_TEST("abi: dispatch is unsupported before milestone M3") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(f.code(id, "lifecycle",
                    Cmd{.name = "elsewhere", .code = static_cast<std::int32_t>(abi::LifecycleOp::dispatch)}),
             static_cast<std::int64_t>(abi::unsupported));
}

// ---------------------------------------------------------------------------
// Robustness

PAGLETS_TEST("abi C27: out-of-bounds import arguments trap the paglet") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(f.call(id, "out_of_bounds").status, static_cast<std::int32_t>(abi::failed));
    CHECK(f.runtime->ending(id).has_value());
}

PAGLETS_TEST("abi C28: too large and malformed documents") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(f.code(id, "send", Cmd{.handle = abi::self_handle, .name = "echo", .payload = Bytes(1100 * 1024, 7)}),
             static_cast<std::int64_t>(abi::too_large));
    CHECK_EQ(dec<std::int32_t>(f.call(id, "malformed_send").payload), static_cast<std::int32_t>(abi::malformed));
}

PAGLETS_TEST("abi C29: too small output buffers are not written") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    auto r = dec<std::vector<std::int64_t>>(f.call(id, "small_buffer").payload);
    REQUIRE(r.size() == 2);
    CHECK(r[0] > 2);
    CHECK_EQ(r[1], 1);
}

PAGLETS_TEST("abi C30: reserved message names are refused") {
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(f.code(id, "send", Cmd{.handle = abi::self_handle, .name = "paglets.spoof"}),
             static_cast<std::int64_t>(abi::invalid_argument));
    CHECK_EQ(f.runtime->send(id, "paglets.spoof").error(), static_cast<std::int32_t>(abi::invalid_argument));
}

// ---------------------------------------------------------------------------
// Samples and persistence

PAGLETS_TEST("runtime: hello sample") {
    Fixture f;
    auto id = f.create("hello.wasm");
    auto r = f.call(id, "greet", enc(std::string("world")));
    CHECK_EQ(r.status, 0);
    CHECK_EQ(dec<std::string>(r.payload), std::string("hello, world (greeting 1)"));
    CHECK(f.logged("hello paglet created as " + id));
}

PAGLETS_TEST("runtime: ping-pong sample bounces between parent and child") {
    Fixture f;
    auto id = f.create("ping_pong.wasm");
    auto r = f.call(id, "run", enc(std::uint32_t{25}));
    CHECK_EQ(r.status, 0);
    CHECK_EQ(dec<std::uint32_t>(r.payload), 25u);
    f.runtime->wait_idle();
    CHECK_EQ(f.runtime->list().size(), std::size_t{1});  // the child disposed itself
}

PAGLETS_TEST("runtime: many paglets on several threads") {
    Fixture f;
    std::vector<rt::PagletId> ids;
    for (int i = 0; i < 40; ++i) ids.push_back(f.create("conformance.wasm"));
    std::vector<std::future<rt::Reply>> replies;
    for (int round = 0; round < 5; ++round) {
        for (const auto& id : ids) replies.push_back(f.runtime->request(id, "count"));
    }
    for (auto& r : replies) CHECK_EQ(r.get().status, 0);
    for (const auto& id : ids) CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 6);
}

PAGLETS_TEST("runtime: without a checkpoint, a crash loses the changes since the last image") {
    const auto dir =
        std::filesystem::temp_directory_path() / ("paglets-test-" + std::to_string(std::random_device{}()));
    std::filesystem::remove_all(dir);
    rt::PagletId id;
    {
        rt::Config c;
        c.state_dir = dir;
        c.checkpoint_interval = std::chrono::hours(1);  // only the first handler is followed by a checkpoint
        Fixture f(std::move(c));
        id = f.create("conformance.wasm");
        f.runtime->wait_idle();
        for (int i = 0; i < 3; ++i) f.call(id, "count");
        f.runtime->shutdown(false);
    }
    {
        rt::Config c;
        c.state_dir = dir;
        Fixture f(std::move(c));
        CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 1);
    }
    std::filesystem::remove_all(dir);
}

PAGLETS_TEST("runtime: paglets resume from their last image after a restart") {
    const auto dir =
        std::filesystem::temp_directory_path() / ("paglets-test-" + std::to_string(std::random_device{}()));
    std::filesystem::remove_all(dir);
    rt::PagletId id;
    rt::PagletId sleeper;
    {
        rt::Config c;
        c.state_dir = dir;
        c.checkpoint_interval = 0ms;
        Fixture f(std::move(c));
        id = f.create("conformance.wasm", text("durable"));
        for (int i = 0; i < 3; ++i) f.call(id, "count");
        CHECK(f.code(id, "timer", Cmd{.name = "after-restart", .ms = 400}) > 0);
        sleeper = f.create("conformance.wasm");
        f.call(sleeper, "count");
        REQUIRE(f.runtime->deactivate(sleeper));
        f.runtime->wait_idle();
        // Only the checkpoints remain, as after a killed host process.
        f.runtime->shutdown(false);
    }
    {
        rt::Config c;
        c.state_dir = dir;
        Fixture f(std::move(c));
        REQUIRE(f.runtime->info(id).has_value());
        REQUIRE(f.runtime->info(sleeper).has_value());
        CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 4);
        CHECK_EQ(dec<std::int64_t>(f.call(sleeper, "count").payload), 2);
        std::this_thread::sleep_for(600ms);
        auto j = f.journal(id);
        CHECK(contains(j, "event:created:durable:0"));
        CHECK(contains(j, "event:activated"));
        CHECK(contains(j, "timer:after-restart"));
    }
    std::filesystem::remove_all(dir);
}

#ifndef _WIN32
namespace {

// Kills a worker process and waits until it is gone (it is a child of this
// test process, which hosts the runtime).
void kill_and_reap(int pid) {
    ::kill(pid, SIGKILL);
    for (int i = 0; i < 500; ++i) {
        int status = 0;
        if (::waitpid(pid, &status, WNOHANG) == pid) return;
        std::this_thread::sleep_for(10ms);
    }
}

}  // namespace

PAGLETS_TEST("runtime: a killed worker process only affects its paglets") {
    if (std::getenv("PAGLETS_TEST_WORKER") == nullptr) paglets::test::skip("runs with worker processes only");
    const auto dir =
        std::filesystem::temp_directory_path() / ("paglets-test-" + std::to_string(std::random_device{}()));
    std::filesystem::remove_all(dir);
    {
        rt::Config c;
        c.state_dir = dir;
        c.checkpoint_interval = 0ms;
        c.threads = 1;  // one worker holds every paglet
        Fixture f(std::move(c));
        auto id = f.create("conformance.wasm");
        CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 1);
        CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 2);
        f.runtime->wait_idle();  // the checkpoint after the last call is written
        const auto workers = f.runtime->worker_processes();
        REQUIRE(workers.size() == 1);
        CHECK(workers[0] != ::getpid());
        kill_and_reap(workers[0]);
        // The next delivery notices the lost worker and resumes the paglet
        // from its last checkpoint in a new worker process.
        auto r = f.call(id, "count");
        CHECK_EQ(r.status, 0);
        CHECK_EQ(dec<std::int64_t>(r.payload), 3);
        const auto again = f.runtime->worker_processes();
        REQUIRE(again.size() == 1);
        CHECK(again[0] != workers[0]);
    }
    {
        rt::Config c;  // no state directory: nothing to resume from
        c.threads = 1;
        Fixture f(std::move(c));
        auto id = f.create("conformance.wasm");
        CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 1);
        f.runtime->wait_idle();
        kill_and_reap(f.runtime->worker_processes().at(0));
        CHECK_EQ(f.call(id, "count").status, static_cast<std::int32_t>(abi::failed));
        auto end = f.runtime->ending(id);
        REQUIRE(end.has_value());
        CHECK(end->failed);
        CHECK(end->reason.find("lost with its worker") != std::string::npos);
        // New paglets work in the restarted worker.
        auto fresh = f.create("conformance.wasm");
        CHECK_EQ(dec<std::int64_t>(f.call(fresh, "count").payload), 1);
    }
    std::filesystem::remove_all(dir);
}

PAGLETS_TEST("runtime: paglets run in worker processes when configured") {
    if (std::getenv("PAGLETS_TEST_WORKER") == nullptr) paglets::test::skip("runs with worker processes only");
    Fixture f;
    auto id = f.create("conformance.wasm");
    CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 1);
    CHECK(!f.runtime->worker_processes().empty());
}
#endif
