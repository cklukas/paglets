// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The host's module store (planning/cpp-modules.md): paglet modules by their
// SHA-256 hash, checked against the paglet ABI when added and against their
// hash whenever they are read; a cache of compiled (loaded) modules with a
// byte budget; use counts; and garbage collection of modules nobody uses.
//
// Compiled forms belong to this host's engine; they are never sent to other
// hosts. With WAMR's fast interpreter the compiled form is the interpreter's
// translated code in memory, so the cache lives in memory; modules that are
// in use (a placed paglet holds its module) stay loaded regardless of the
// budget.
//
// Thread-safe.

#pragma once

#include <paglets/sha256.hpp>
#include <paglets/wasm/binary.hpp>
#include <paglets/wasm/engine.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::runtime {

struct ModuleStoreConfig {
    // Directory of the store (the state directory's `modules`). Empty: the
    // modules are kept in memory.
    std::filesystem::path dir;
    // Compiled modules kept loaded while no paglet uses them, measured by
    // module size.
    std::size_t cache_bytes = 64u * 1024 * 1024;
};

struct ModuleEntry {
    std::string hash;  // hex SHA-256
    std::size_t size = 0;
    std::int64_t added_ms = 0;      // Unix milliseconds
    std::int64_t last_used_ms = 0;  // added, acquired, or its last user ended
    std::size_t users = 0;          // paglets on this host that run it
    bool pinned = false;            // kept by garbage collection
    bool loaded = false;            // a compiled form exists
};

struct ModuleCacheStats {
    std::uint64_t hits = 0;       // acquire found a compiled form
    std::uint64_t loads = 0;      // acquire compiled the module
    std::uint64_t evictions = 0;  // compiled forms dropped for the budget
    std::size_t cached_bytes = 0;
};

struct ModuleCollection {
    std::vector<std::string> removed;  // hashes
    std::size_t bytes = 0;
};

class ModuleStore {
public:
    using Warn = std::function<void(const std::string&)>;

    // Opens the store; with a directory, indexes the modules in it (files
    // that do not match their hash, or that are not valid paglet modules,
    // are reported and left out).
    explicit ModuleStore(ModuleStoreConfig config, Warn warn = {});
    ~ModuleStore();
    ModuleStore(const ModuleStore&) = delete;
    ModuleStore& operator=(const ModuleStore&) = delete;

    // Checks the module (Wasm structure, paglet ABI of a system paglet, the
    // widest import set) and stores it; returns its hex hash. Adding a module
    // that is present only refreshes its last use.
    std::expected<std::string, std::string> add(std::vector<std::uint8_t> wasm);
    // As add, but only if the bytes hash to `expected` (modules from other
    // hosts or sources).
    std::expected<std::string, std::string> add_verified(const Digest& expected, std::vector<std::uint8_t> wasm);

    bool contains(std::string_view hash) const;
    // The module's bytes, verified against its hash.
    std::expected<std::vector<std::uint8_t>, std::string> bytes(std::string_view hash) const;
    std::optional<wasm::ModuleInfo> info(std::string_view hash) const;
    std::optional<ModuleEntry> entry(std::string_view hash) const;
    std::vector<ModuleEntry> list() const;  // by hash

    // The compiled module, from the cache or compiled now. The caller keeps
    // it while it runs instances of it.
    std::expected<std::shared_ptr<wasm::Module>, std::string> acquire(std::string_view hash);
    // Hashes whose compiled form the cache keeps (without users).
    std::vector<std::string> cached() const;
    ModuleCacheStats stats() const;

    // Use counts: the runtime counts every paglet on this host (active or
    // not) as a user of its module. add_user fails for unknown modules.
    bool add_user(std::string_view hash);
    void remove_user(std::string_view hash);

    // Pinned modules are never collected (for example modules a host serves
    // to others, or keeps ready for arrivals).
    bool pin(std::string_view hash, bool pinned = true);

    // Removes every module without users that is not pinned and was not used
    // within `grace` (its stored bytes, metadata and compiled form).
    ModuleCollection collect(std::chrono::milliseconds grace);

    // Writes the last-use times (also done by collect and the destructor).
    void flush();

    const ModuleStoreConfig& config() const { return config_; }

private:
    struct Slot {
        ModuleEntry entry;
        wasm::ModuleInfo info;
        std::vector<std::uint8_t> bytes;  // in-memory stores only
        std::weak_ptr<wasm::Module> compiled;
        bool dirty = false;  // metadata not yet written
    };

    std::expected<std::string, std::string> insert(const Digest& hash, std::vector<std::uint8_t> wasm);
    std::expected<std::string, std::string> insert_locked(const Digest& hash, std::vector<std::uint8_t> wasm);
    ModuleCollection collect_locked(std::chrono::milliseconds grace);
    // Warnings are collected under mu_ and reported without it, so the
    // callback may take other locks.
    void report();
    std::expected<std::vector<std::uint8_t>, std::string> read(const std::string& hash, const Slot& slot) const;
    void write_meta(Slot& slot);
    void remember(const std::string& hash, std::shared_ptr<wasm::Module> module);  // mu_ held
    void trim();                                                                   // mu_ held
    std::filesystem::path wasm_path(const std::string& hash) const;
    std::filesystem::path meta_path(const std::string& hash) const;

    ModuleStoreConfig config_;
    Warn warn_;
    mutable std::mutex mu_;
    std::map<std::string, Slot, std::less<>> slots_;
    // Compiled forms kept for the budget, most recently used first.
    std::list<std::pair<std::string, std::shared_ptr<wasm::Module>>> lru_;
    std::size_t lru_bytes_ = 0;
    ModuleCacheStats stats_;
    std::vector<std::string> pending_;  // warnings to report
};

}  // namespace paglets::runtime
