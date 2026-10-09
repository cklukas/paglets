// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Hardening (WP21, planning/cpp-hardening.md): limits that keep one owner's
// paglets from taking a host over. Clone bombs stop at the owner's paglet
// limit, also for paglets arriving from other hosts; pins per paglet are
// bounded; other owners' paglets go on.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <paglets/wire/reflect.hpp>

#include <algorithm>
#include <thread>

using namespace paglets::test;
namespace wire = paglets::wire;

namespace {

std::size_t paglets_of(Fixture& f, const std::string& owner) {
    std::size_t n = 0;
    for (const auto& p : f.runtime->list()) {
        if (!p.module.empty() && p.owner == owner) ++n;
    }
    return n;
}

}  // namespace

PAGLETS_TEST("hardening: a clone bomb stops at the owner's paglet limit; other owners go on") {
    rt::Config config;
    config.owner_paglet_limit = 40;
    Fixture f(std::move(config));
    const std::string bomb = f.module("bomb.wasm");
    auto first = f.runtime->create(bomb, rt::CreateOptions{.owner = "mallory"});
    REQUIRE_OK(first);
    // Every generation clones four times; the host refuses at the limit.
    REQUIRE(f.runtime->wait_idle());
    CHECK_EQ(paglets_of(f, "mallory"), 40u);
    std::int64_t refused = 0;
    for (const auto& p : f.runtime->list()) {
        if (p.owner != "mallory") continue;
        auto r = f.call(p.id, "refused");
        std::int64_t n = 0;
        REQUIRE(r.status == 0 && wire::from_msgpack(r.payload, n));
        refused += n;
    }
    CHECK(refused > 0);
    // The owner gets no more here, from the host either.
    auto more = f.runtime->create(bomb, rt::CreateOptions{.owner = "mallory"});
    REQUIRE(!more);
    CHECK(more.error().starts_with("quota"));
    // Another owner's paglets run as before.
    auto other = f.runtime->create(f.module("conformance.wasm"), rt::CreateOptions{.owner = "olga"});
    REQUIRE_OK(other);
    CHECK_EQ(f.call(*other, "echo", text("still here")).status, 0);
    // Room again once some end.
    for (const auto& p : f.runtime->list()) {
        if (p.owner == "mallory" && p.id != *first) {
            REQUIRE_OK(f.runtime->terminate(p.id, "test"));
            break;
        }
    }
    REQUIRE(f.runtime->wait_idle());
    CHECK_EQ(paglets_of(f, "mallory"), 39u);
    auto again = f.runtime->create(f.module("conformance.wasm"), rt::CreateOptions{.owner = "mallory"});
    CHECK(again.has_value());
}

PAGLETS_TEST("hardening: a paglet may hold a bounded number of pins; expired pins make room") {
    rt::Config config;
    config.pin_limit = 4;
    Fixture f(std::move(config));
    const auto id = f.create("conformance.wasm");
    REQUIRE(f.runtime->wait_idle());
    const std::int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
    REQUIRE_OK(f.runtime->pin(id, "short", now + 50));
    for (int i = 0; i < 3; ++i) REQUIRE_OK(f.runtime->pin(id, "pin-" + std::to_string(i), now + 60'000));
    auto full = f.runtime->pin(id, "one-more", now + 60'000);
    REQUIRE(!full);
    CHECK_EQ(full.error(), static_cast<std::int32_t>(abi::quota));
    // Renewing a pin it holds is fine.
    CHECK(f.runtime->pin(id, "pin-0", now + 120'000).has_value());
    // The short one expires: room for one more.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    CHECK(f.runtime->pin(id, "one-more", now + 60'000).has_value());
    CHECK(f.runtime->pinned_until(id) >= now + 60'000);
}

PAGLETS_TEST("hardening: a host refuses paglets arriving beyond their owner's limit; they stay where they were") {
    Mesh net;
    net.configure = [](const std::string& name, rt::Config& c) {
        if (name == "b") c.owner_paglet_limit = 1;
    };
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const std::string module = a.f->module("conformance.wasm");
    (void)b.f->module("conformance.wasm");
    // The owner's one paglet on b.
    auto resident = b.node->create(module, net.passport(module, paglet_id('1')));
    REQUIRE_OK(resident);
    auto traveller = a.node->create(module, net.passport(module, paglet_id('2')));
    REQUIRE_OK(traveller);
    REQUIRE_OK(a.node->dispatch(*traveller, "b"));
    // While it moves, requests to it wait (and this thread carries the frames):
    // the journal is read once the move has failed.
    REQUIRE(net.settle([&] { return a.node->move_stats().moves_failed >= 1; }, 2000));
    CHECK(a.f->runtime->info(*traveller).has_value());
    CHECK(!b.f->runtime->info(*traveller));
    bool quota = false;
    const auto until = std::chrono::steady_clock::now() + 10s;
    while (!quota && std::chrono::steady_clock::now() < until) {
        for (const auto& e : a.f->journal(*traveller)) quota = quota || (e.starts_with("move_failed:") && e.find("quota") != std::string::npos);
        if (!quota) std::this_thread::sleep_for(10ms);
    }
    CHECK(quota);
    // Once b has room, the move works.
    REQUIRE_OK(b.f->runtime->terminate(*resident, "test"));
    REQUIRE(b.f->runtime->wait_idle());
    REQUIRE_OK(a.node->dispatch(*traveller, "b"));
    REQUIRE(net.settle([&] { return b.f->runtime->info(*traveller).has_value() && !a.f->runtime->info(*traveller); }, 2000));
}
