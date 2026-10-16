// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <paglets/msgpack.hpp>
#include <paglets/wasm/direct.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wasm/snapshot.hpp>

#include <chrono>
#include <filesystem>
#include <thread>

#if defined(__linux__) && defined(__aarch64__)
#include <atomic>
#include <barrier>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>
extern "C" void os_icache_flush(void*, std::size_t);
#endif

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

PAGLETS_TEST("engine: failed buffered file writes are reported") {
#if defined(__linux__)
    REQUIRE(std::filesystem::is_character_file("/dev/full"));
    const std::vector<std::uint8_t> small{1, 2, 3};
    CHECK(!pw::write_file("/dev/full", small).has_value());
#else
    paglets::test::skip("requires Linux /dev/full");
#endif
}

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

PAGLETS_TEST("engine: export signatures are checked before writing host result buffers") {
    // A valid multi-value Wasm function returning 32 i32 values. Calling
    // it through the old i32 wrapper could overwrite its 16-cell argv buffer.
    std::vector<std::uint8_t> bytes{0, 0x61, 0x73, 0x6d, 1, 0, 0, 0};
    auto section = [&](std::uint8_t id, const std::vector<std::uint8_t>& body) {
        REQUIRE(body.size() < 128);
        bytes.push_back(id);
        bytes.push_back(static_cast<std::uint8_t>(body.size()));
        bytes.insert(bytes.end(), body.begin(), body.end());
    };
    std::vector<std::uint8_t> type{1, 0x60, 0, 32};
    type.insert(type.end(), 32, 0x7f);
    section(1, type);
    section(3, {1, 0});
    section(7, {1, 4, 'm', 'a', 'n', 'y', 0, 0});
    std::vector<std::uint8_t> code{1, 66, 0};
    for (int i = 0; i < 32; ++i) code.insert(code.end(), {0x41, 0});
    code.push_back(0x0b);
    section(10, code);
    auto module = pw::Module::load(std::move(bytes), pw::ImportPolicy::standard());
    REQUIRE_OK(module);
    auto instance = pw::Instance::create(*module);
    REQUIRE_OK(instance);
    auto refused = (*instance)->call_i32("many");
    REQUIRE(!refused);
    CHECK(refused.error().find("signature mismatch") != std::string::npos);
    // An argument mismatch is refused before any guest side effect too.
    auto normal = pw::Instance::create(load_guest("testbed.wasm"));
    REQUIRE_OK(normal);
    CHECK(!(*normal)->call_i32("add", {1}));
    CHECK(!(*normal)->call_void("add", {1, 2}));
    CHECK_EQ(*(*normal)->call_i32("calls"), 0);
    CHECK_EQ(*(*normal)->call_i32("add", {40, 2}), 42);
}

PAGLETS_TEST("engine: unsupported globals are refused instead of captured as truncated integers") {
    // (global (export "ref") (mut externref) (ref.null extern))
    std::vector<std::uint8_t> bytes{0, 0x61, 0x73, 0x6d, 1, 0, 0, 0,
        6, 6, 1, 0x6f, 1, 0xd0, 0x6f, 0x0b, 7, 7, 1, 3, 'r', 'e', 'f', 3, 0};
    auto module = pw::Module::load(std::move(bytes), pw::ImportPolicy::standard());
    REQUIRE_OK(module);
    auto instance = pw::Instance::create(*module);
    REQUIRE_OK(instance);
    CHECK(!(*instance)->global_bits("ref"));
    CHECK(!(*instance)->set_global_bits("ref", 0));
    auto image = pw::capture(**instance);
    REQUIRE(!image);
    CHECK(image.error().find("numeric mutable globals") != std::string::npos);
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

PAGLETS_TEST("engine: termination before a call cannot be cleared by that call") {
    auto inst = pw::Instance::create(load_guest("testbed.wasm"));
    REQUIRE_OK(inst);
    (*inst)->terminate();
    auto r = (*inst)->call_i32("add", {2, 3});
    REQUIRE(!r.has_value());
    CHECK(r.error().find("terminated") != std::string::npos);
    r = (*inst)->call_i32("calls");
    CHECK(!r.has_value());
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
    pw::DirectHarness harness(**inst);
    REQUIRE_OK(harness.start());
    paglets::msgpack::Writer w;
    w.write_str("hello from the guest");
    auto reply = harness.call("log", w.bytes());
    REQUIRE_OK(reply);
    CHECK_EQ(reply->status, 0);
    CHECK_EQ(logged, std::string("hello from the guest"));
}

#if defined(__linux__) && defined(__aarch64__)
PAGLETS_TEST("engine: relocated instructions are visible on another ARM core") {
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    REQUIRE(sched_getaffinity(0, sizeof allowed, &allowed) == 0);
    std::vector<int> cpus;
    for (int i = 0; i < CPU_SETSIZE; ++i) if (CPU_ISSET(i, &allowed)) cpus.push_back(i);
    if (cpus.size() < 2) paglets::test::skip("two allowed CPU cores required");
    const auto size = static_cast<std::size_t>(getpagesize());
    void* memory = mmap(nullptr, size, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    REQUIRE(memory != MAP_FAILED);
    auto* code = static_cast<volatile std::uint32_t*>(memory);
    code[0] = 0x52800000;  // mov w0, #0
    code[1] = 0xd65f03c0;  // ret
    auto function = reinterpret_cast<unsigned (*)()>(memory);
    std::barrier phase(2);
    std::atomic<int> mismatches{0}, affinity_errors{0};
    auto pin = [&](int cpu) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        if (pthread_setaffinity_np(pthread_self(), sizeof set, &set)) ++affinity_errors;
    };
    std::thread writer([&] {
        pin(cpus[0]);
        for (unsigned i = 1; i <= 256; ++i) {
            code[0] = 0x52800000 | (i << 5);  // mov w0, #i
            os_icache_flush(memory, 8);
            phase.arrive_and_wait();
            phase.arrive_and_wait();
        }
    });
    std::thread reader([&] {
        pin(cpus[1]);
        for (unsigned i = 1; i <= 256; ++i) {
            phase.arrive_and_wait();
            // The receiving job thread also crosses a context boundary.
            // ISB alone cannot publish the writer's instruction bytes.
            asm volatile("isb" ::: "memory");
            if (function() != i) ++mismatches;
            phase.arrive_and_wait();
        }
    });
    writer.join();
    reader.join();
    munmap(memory, size);
    CHECK_EQ(affinity_errors.load(), 0);
    CHECK_EQ(mismatches.load(), 0);
}
#endif
