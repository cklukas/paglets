// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <paglets/msgpack.hpp>
#include <paglets/wasm/engine.hpp>

#include <chrono>
#include <thread>

namespace pw = paglets::wasm;

namespace {

std::shared_ptr<pw::Module> load_guest(const std::string& file) {
    const auto path = paglets::test::guest_path(file);
    if (path.empty()) paglets::test::skip(file + " not available");
    auto bytes = pw::read_file(path);
    if (!bytes) throw std::runtime_error(bytes.error());
    auto module = pw::Module::load(std::move(*bytes), pw::ImportPolicy::standard());
    if (!module) throw std::runtime_error(module.error());
    return *module;
}

}  // namespace

PAGLETS_TEST("engine: call exports and keep state between calls") {
    auto module = load_guest("testbed.wasm");
    auto inst = pw::Instance::create(module);
    REQUIRE_OK(inst);
    auto r = (*inst)->call_i32("add", {40, 2});
    REQUIRE_OK(r);
    CHECK_EQ(*r, 42);
    (void)(*inst)->call_i32("add", {1, 1});
    auto calls = (*inst)->call_i32("calls");
    REQUIRE_OK(calls);
    CHECK_EQ(*calls, 2);
}

PAGLETS_TEST("engine: instances are isolated") {
    auto module = load_guest("testbed.wasm");
    auto a = pw::Instance::create(module);
    auto b = pw::Instance::create(module);
    REQUIRE_OK(a);
    REQUIRE_OK(b);
    (void)(*a)->call_i32("add", {1, 1});
    (void)(*a)->call_i32("add", {1, 1});
    CHECK_EQ(*(*a)->call_i32("calls"), 2);
    CHECK_EQ(*(*b)->call_i32("calls"), 0);
}

PAGLETS_TEST("engine: a trap fails the call, the instance stays usable") {
    auto module = load_guest("testbed.wasm");
    auto inst = pw::Instance::create(module);
    REQUIRE_OK(inst);
    auto trap = (*inst)->call_void("trap_now");
    REQUIRE(!trap.has_value());
    CHECK(trap.error().find("unreachable") != std::string::npos);
    CHECK_EQ(*(*inst)->call_i32("add", {2, 3}), 5);
}

PAGLETS_TEST("engine: stack exhaustion is a trap, not a crash") {
    auto module = load_guest("testbed.wasm");
    auto inst = pw::Instance::create(module, pw::Limits{.stack_size = 16 * 1024, .max_memory_pages = 64});
    REQUIRE_OK(inst);
    auto r = (*inst)->call_i32("recurse", {1'000'000});
    REQUIRE(!r.has_value());
    CHECK(r.error().find("stack") != std::string::npos);
}

PAGLETS_TEST("engine: memory limit refuses growth beyond max pages") {
    auto module = load_guest("testbed.wasm");
    auto inst = pw::Instance::create(module, pw::Limits{.max_memory_pages = 8});
    REQUIRE_OK(inst);
    const auto before = *(*inst)->call_i32("pages");
    CHECK(before <= 8);
    CHECK_EQ(*(*inst)->call_i32("grow", {static_cast<std::uint32_t>(8 - before)}), before);
    CHECK_EQ(*(*inst)->call_i32("grow", {1}), -1);
    CHECK_EQ(*(*inst)->call_i32("pages"), 8);
}

PAGLETS_TEST("engine: terminate stops an endless loop from another thread") {
    auto module = load_guest("testbed.wasm");
    auto inst = pw::Instance::create(module);
    REQUIRE_OK(inst);
    std::thread killer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        (*inst)->terminate();
    });
    const auto t0 = std::chrono::steady_clock::now();
    auto r = (*inst)->call_void("spin");
    const auto elapsed = std::chrono::steady_clock::now() - t0;
    killer.join();
    REQUIRE(!r.has_value());
    CHECK(r.error().find("terminated") != std::string::npos);
    CHECK(elapsed < std::chrono::seconds(5));
}

PAGLETS_TEST("engine: modules with forbidden imports are not loaded") {
    const auto path = paglets::test::guest_path("forbidden.wasm");
    if (path.empty()) paglets::test::skip("forbidden.wasm not available");
    auto bytes = pw::read_file(path);
    REQUIRE_OK(bytes);
    auto module = pw::Module::load(std::move(*bytes), pw::ImportPolicy::standard());
    REQUIRE(!module.has_value());
    CHECK(module.error().find("not allowed") != std::string::npos);
}

PAGLETS_TEST("engine: guest log reaches the host sink") {
    auto module = load_guest("counter.wasm");
    auto inst = pw::Instance::create(module);
    REQUIRE_OK(inst);
    std::string logged;
    (*inst)->set_log_sink([&](std::string_view text) { logged = text; });
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    w.write_str("log");
    w.write_str("hello from the guest");
    auto reply = (*inst)->send(w.bytes());
    REQUIRE_OK(reply);
    CHECK_EQ(logged, std::string("hello from the guest"));
}
