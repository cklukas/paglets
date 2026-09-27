// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The module store (WP11, planning/cpp-modules.md): content addressing and
// checks, the directory store, the cache of compiled modules, use counts,
// garbage collection, and how the runtime uses it.

#include "runtime_fixture.hpp"
#include "test.hpp"

#include <paglets/runtime/modules.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wasm/binary.hpp>

#include <filesystem>
#include <random>

namespace rt = paglets::runtime;
using namespace std::chrono_literals;
using namespace paglets::test;

namespace {

Bytes guest(const std::string& file) {
    const auto path = paglets::test::guest_path(file);
    if (path.empty()) paglets::test::skip(file + " not available");
    auto bytes = paglets::wasm::read_file(path);
    if (!bytes) throw std::runtime_error(bytes.error());
    return *bytes;
}

std::filesystem::path temp_dir() {
    auto dir = std::filesystem::temp_directory_path() / ("paglets-test-" + std::to_string(std::random_device{}()));
    std::filesystem::remove_all(dir);
    return dir;
}

bool eventually(const std::function<bool()>& condition, std::chrono::milliseconds timeout = 5000ms) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < until) {
        if (condition()) return true;
        std::this_thread::sleep_for(20ms);
    }
    return condition();
}

}  // namespace

PAGLETS_TEST("modules: the store checks modules and addresses them by hash") {
    rt::ModuleStore store(rt::ModuleStoreConfig{});
    const Bytes wasm = guest("conformance.wasm");
    auto hash = store.add(wasm);
    REQUIRE_OK(hash);
    CHECK_EQ(*hash, paglets::to_hex(paglets::sha256(wasm)));
    CHECK(store.contains(*hash));
    auto bytes = store.bytes(*hash);
    REQUIRE_OK(bytes);
    CHECK(*bytes == wasm);
    CHECK(store.info(*hash).has_value());
    CHECK_EQ(store.add(wasm).value(), *hash);  // present already
    CHECK_EQ(store.list().size(), 1u);

    CHECK(!store.add(text("not a module")).has_value());
    // A module from elsewhere must match the hash it was asked for.
    const paglets::Digest other = paglets::sha256(text("something else"));
    CHECK(!store.add_verified(other, wasm).has_value());
    CHECK(store.add_verified(paglets::sha256(wasm), wasm).has_value());
    CHECK(!store.contains(paglets::to_hex(other)));
}

PAGLETS_TEST("modules: a directory store verifies its files and keeps metadata") {
    const auto dir = temp_dir();
    const Bytes a = guest("conformance.wasm");
    const Bytes b = guest("hello.wasm");
    std::string ha;
    std::string hb;
    {
        rt::ModuleStore store(rt::ModuleStoreConfig{dir});
        ha = store.add(a).value();
        hb = store.add(b).value();
        CHECK(store.pin(ha));
    }
    CHECK(std::filesystem::exists(dir / (ha + ".wasm")));
    CHECK(std::filesystem::exists(dir / (ha + ".meta")));
    // A damaged file is not used.
    {
        Bytes damaged = b;
        damaged[damaged.size() / 2] ^= 0xff;
        REQUIRE(paglets::wasm::write_file((dir / (hb + ".wasm")).string(), damaged).has_value());
    }
    std::vector<std::string> warnings;
    rt::ModuleStore store(rt::ModuleStoreConfig{dir}, [&](const std::string& w) { warnings.push_back(w); });
    CHECK(store.contains(ha));
    CHECK(!store.contains(hb));
    CHECK_EQ(warnings.size(), 1u);
    auto entry = store.entry(ha);
    REQUIRE(entry.has_value());
    CHECK(entry->pinned);
    CHECK_EQ(entry->size, a.size());
    // Damage after opening is found when the module is read.
    {
        Bytes damaged = a;
        damaged[damaged.size() / 3] ^= 0xff;
        REQUIRE(paglets::wasm::write_file((dir / (ha + ".wasm")).string(), damaged).has_value());
    }
    CHECK(!store.bytes(ha).has_value());
    CHECK(!store.acquire(ha).has_value());
    std::filesystem::remove_all(dir);
}

PAGLETS_TEST("modules: compiled modules are shared and cached within a budget") {
    const Bytes a = guest("conformance.wasm");
    const Bytes b = guest("hello.wasm");
    rt::ModuleStore store(rt::ModuleStoreConfig{.dir = {}, .cache_bytes = std::max(a.size(), b.size())});
    const auto ha = store.add(a).value();
    const auto hb = store.add(b).value();

    auto first = store.acquire(ha);
    REQUIRE_OK(first);
    auto second = store.acquire(ha);
    REQUIRE_OK(second);
    CHECK(first->get() == second->get());
    CHECK_EQ(store.stats().loads, 1u);
    CHECK_EQ(store.stats().hits, 1u);

    // The budget holds one module: b's compiled form pushes a's out of the
    // cache, but a stays loaded while it is used.
    auto other = store.acquire(hb);
    REQUIRE_OK(other);
    CHECK_EQ(store.stats().evictions, 1u);
    CHECK(store.cached() == std::vector<std::string>{hb});
    CHECK(store.entry(ha)->loaded);
    first->reset();
    second->reset();
    CHECK(!store.entry(ha)->loaded);
    REQUIRE_OK(store.acquire(ha));
    CHECK_EQ(store.stats().loads, 3u);
    CHECK(!store.acquire(std::string(64, '0')).has_value());
}

PAGLETS_TEST("modules: garbage collection keeps used, pinned and recent modules") {
    const auto dir = temp_dir();
    rt::ModuleStore store(rt::ModuleStoreConfig{dir});
    const auto used = store.add(guest("conformance.wasm")).value();
    const auto pinned = store.add(guest("hello.wasm")).value();
    const auto unused = store.add(guest("counter.wasm")).value();
    REQUIRE(store.add_user(used));
    REQUIRE(store.pin(pinned));
    REQUIRE_OK(store.acquire(unused));  // cached compiled forms go with their module

    CHECK(store.collect(1h).removed.empty());  // all recent
    auto collected = store.collect(0ms);
    CHECK(collected.removed == std::vector<std::string>{unused});
    CHECK(collected.bytes > 0);
    CHECK(!std::filesystem::exists(dir / (unused + ".wasm")));
    CHECK(!std::filesystem::exists(dir / (unused + ".meta")));
    CHECK(store.cached().empty());

    store.remove_user(used);
    CHECK(store.collect(0ms).removed == std::vector<std::string>{used});
    CHECK(!store.add_user(used));
    CHECK(store.contains(pinned));
    REQUIRE(store.pin(pinned, false));
    CHECK_EQ(store.collect(0ms).removed.size(), 1u);
    CHECK(store.list().empty());
    std::filesystem::remove_all(dir);
}

PAGLETS_TEST("runtime: paglets load their module from the store while they run") {
    rt::Config c;
    c.module_cache_bytes = 0;  // nothing stays compiled without a user
    c.module_gc_interval = 0ms;
    Fixture f(std::move(c));
    const auto hash = f.module("conformance.wasm");
    auto id = f.runtime->create(hash);
    REQUIRE_OK(id);
    CHECK_EQ(f.call(*id, "count").status, 0);
    auto entry = f.runtime->modules().entry(hash);
    REQUIRE(entry.has_value());
    CHECK_EQ(entry->users, 1u);
    CHECK(entry->loaded);
    const bool workers = !f.runtime->worker_processes().empty();
    if (workers) CHECK(contains(f.runtime->worker_modules(), hash));

    // Inactive paglets hold no compiled module, neither here nor in a worker.
    REQUIRE(f.runtime->deactivate(*id).has_value());
    CHECK(eventually([&] { return !f.runtime->modules().entry(hash)->loaded; }));
    if (workers) CHECK(eventually([&] { return f.runtime->worker_modules().empty(); }));
    const auto loads = f.runtime->modules().stats().loads;
    CHECK_EQ(f.call(*id, "count").status, 0);
    CHECK_EQ(f.runtime->modules().stats().loads, loads + 1);

    // A module in use is not collected; after its last paglet it is.
    CHECK(f.runtime->modules().collect(0ms).removed.empty());
    REQUIRE(f.runtime->dispose(*id).has_value());
    f.runtime->wait_idle();
    CHECK_EQ(f.runtime->modules().entry(hash)->users, 0u);
    CHECK(f.runtime->modules().collect(0ms).removed == std::vector<std::string>{hash});
    CHECK(!f.runtime->create(hash).has_value());
}

PAGLETS_TEST("runtime: module use counts survive restarts; the host collects unused modules") {
    const auto dir = temp_dir();
    std::string hash;
    rt::PagletId id;
    {
        rt::Config c;
        c.state_dir = dir;
        Fixture f(std::move(c));
        hash = f.module("conformance.wasm");
        id = f.runtime->create(hash).value();
        CHECK_EQ(f.call(id, "count").status, 0);
    }
    CHECK(std::filesystem::exists(dir / "modules" / (hash + ".wasm")));
    rt::Config c;
    c.state_dir = dir;
    c.module_gc_grace = 0ms;
    c.module_gc_interval = 50ms;
    Fixture f(std::move(c));
    REQUIRE(f.runtime->info(id).has_value());
    CHECK_EQ(f.runtime->modules().entry(hash)->users, 1u);
    std::this_thread::sleep_for(150ms);  // several collections: the module is in use
    CHECK(f.runtime->modules().contains(hash));
    REQUIRE(f.runtime->dispose(id).has_value());
    f.runtime->wait_idle();
    CHECK(eventually([&] { return !f.runtime->modules().contains(hash); }));
    CHECK(!std::filesystem::exists(dir / "modules" / (hash + ".wasm")));
    CHECK(eventually([&] { return f.logged("module store: removed 1 unused modules"); }));
    f.runtime->shutdown();
    std::filesystem::remove_all(dir);
}

PAGLETS_TEST("runtime: the module admission check applies to every new paglet; terminate ends paglets") {
    Fixture f;
    const auto conformance = f.module("conformance.wasm");
    const auto hello = f.module("hello.wasm");
    f.runtime->set_module_admission([&](const std::string& module, rt::TrustClass trust) {
        if (module == hello) return std::expected<void, std::string>(std::unexpected(std::string("not here")));
        if (trust != rt::TrustClass::roaming) {
            return std::expected<void, std::string>(std::unexpected(std::string("roaming only")));
        }
        return std::expected<void, std::string>();
    });
    auto refused = f.runtime->create(hello);
    REQUIRE(!refused.has_value());
    CHECK_EQ(refused.error(), std::string("not here"));
    CHECK(!f.runtime->create(conformance, rt::CreateOptions{{}, rt::TrustClass::resident}).has_value());
    auto id = f.runtime->create(conformance);
    REQUIRE_OK(id);
    // Children of another module and clones pass the same check.
    CHECK_EQ(f.code(*id, "child", Cmd{.name = hello}), static_cast<std::int64_t>(abi::denied));
    CHECK(f.code(*id, "child", Cmd{}) > 0);
    CHECK(f.logged("child refused: not here"));

    // An idle paglet ends at once; a busy one when its handler is stopped.
    auto idle = f.runtime->create(conformance);
    REQUIRE_OK(idle);
    f.runtime->wait_idle();
    REQUIRE(f.runtime->terminate(*idle, "no longer wanted").has_value());
    auto ending = f.runtime->ending(*idle);
    REQUIRE(ending.has_value());
    CHECK(ending->failed);
    CHECK_EQ(ending->reason, std::string("no longer wanted"));
    auto busy = f.runtime->request(*id, "busy", enc(Cmd{.ms = 10'000}));
    std::this_thread::sleep_for(100ms);
    REQUIRE(f.runtime->terminate(*id, "stopped").has_value());
    CHECK_EQ(busy.get().status, static_cast<std::int32_t>(abi::failed));
    CHECK(eventually([&] { return f.runtime->ending(*id).has_value(); }));
    CHECK_EQ(f.runtime->ending(*id)->reason, std::string("stopped"));
    CHECK_EQ(f.runtime->terminate(*id, "again").error(), static_cast<std::int32_t>(abi::not_found));
    f.runtime->set_module_admission({});
    CHECK(f.runtime->create(hello).has_value());
}
