// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/abi.hpp>
#include <paglets/runtime/modules.hpp>
#include <paglets/runtime/runtime.hpp>

#include <algorithm>
#include <system_error>

namespace paglets::runtime {

namespace {

namespace fs = std::filesystem;

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool is_hash(std::string_view s) {
    return s.size() == 64 && s.find_first_not_of("0123456789abcdef") == std::string_view::npos;
}

struct Meta {
    std::int64_t added = 0;
    std::int64_t last_used = 0;
    bool pinned = false;
};

std::vector<std::uint8_t> encode_meta(const Meta& m) {
    msgpack::Writer w;
    w.write_map_header(3);
    abi::detail::put(w, "added", m.added);
    abi::detail::put(w, "last_used", m.last_used);
    abi::detail::put(w, "pinned", m.pinned);
    return w.bytes();
}

bool decode_meta(std::span<const std::uint8_t> bytes, Meta& m) {
    msgpack::Reader r(bytes);
    return abi::detail::read_map(r,
                                 [&](std::string_view k) {
                                     if (k == "added") return msgpack::read_value(r, m.added);
                                     if (k == "last_used") return msgpack::read_value(r, m.last_used);
                                     if (k == "pinned") return msgpack::read_value(r, m.pinned);
                                     return r.skip();
                                 }) &&
           r.at_end();
}

std::expected<void, std::string> write_atomically(const fs::path& path, std::span<const std::uint8_t> data) {
    const fs::path tmp = path.string() + ".tmp";
    if (auto ok = wasm::write_file(tmp.string(), data); !ok) return ok;
    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (ec) return std::unexpected("cannot replace " + path.string() + ": " + ec.message());
    return {};
}

// Structure and ABI checks of a module from any source: valid Wasm, the
// paglet ABI with the widest import set (the trust class is checked when a
// paglet is created).
std::expected<wasm::ModuleInfo, std::string> check(std::span<const std::uint8_t> bytes) {
    auto info = wasm::parse_module(bytes);
    if (!info) return std::unexpected("invalid module: " + info.error());
    if (auto ok = check_paglet_module(*info, TrustClass::system); !ok) return std::unexpected(ok.error());
    if (auto ok = wasm::ImportPolicy::system().check(*info); !ok) return std::unexpected(ok.error());
    return std::move(*info);
}

}  // namespace

ModuleStore::ModuleStore(ModuleStoreConfig config, Warn warn) : config_(std::move(config)), warn_(std::move(warn)) {
    if (!warn_) warn_ = [](const std::string&) {};
    if (config_.dir.empty()) return;
    fs::create_directories(config_.dir);
    for (const auto& file : fs::directory_iterator(config_.dir)) {
        const fs::path& path = file.path();
        const std::string stem = path.stem().string();
        if (path.extension() == ".tmp") {
            std::error_code ec;
            fs::remove(path, ec);  // an interrupted write
            continue;
        }
        if (path.extension() != ".wasm") continue;
        if (!is_hash(stem)) {
            warn_("module store: unexpected file " + path.string());
            continue;
        }
        auto bytes = wasm::read_file(path.string());
        if (!bytes) {
            warn_("module store: " + bytes.error());
            continue;
        }
        if (to_hex(sha256(*bytes)) != stem) {
            warn_("module store: " + path.string() + " does not match its hash; ignored");
            continue;
        }
        auto info = check(*bytes);
        if (!info) {
            warn_("module store: " + path.string() + ": " + info.error());
            continue;
        }
        Slot slot;
        slot.entry.hash = stem;
        slot.entry.size = bytes->size();
        slot.info = std::move(*info);
        Meta meta;
        meta.added = meta.last_used = now_ms();
        bool have_meta = false;
        if (auto m = wasm::read_file(meta_path(stem).string()); m && decode_meta(*m, meta)) have_meta = true;
        slot.entry.added_ms = meta.added;
        slot.entry.last_used_ms = meta.last_used;
        slot.entry.pinned = meta.pinned;
        slot.dirty = !have_meta;
        slots_.emplace(stem, std::move(slot));
    }
}

ModuleStore::~ModuleStore() {
    flush();
}

fs::path ModuleStore::wasm_path(const std::string& hash) const {
    return config_.dir / (hash + ".wasm");
}

fs::path ModuleStore::meta_path(const std::string& hash) const {
    return config_.dir / (hash + ".meta");
}

std::expected<std::string, std::string> ModuleStore::add(std::vector<std::uint8_t> wasm) {
    const Digest hash = sha256(wasm);
    return insert(hash, std::move(wasm));
}

std::expected<std::string, std::string> ModuleStore::add_verified(const Digest& expected,
                                                                  std::vector<std::uint8_t> wasm) {
    if (sha256(wasm) != expected) {
        return std::unexpected("module does not match its hash " + to_hex(expected));
    }
    return insert(expected, std::move(wasm));
}

std::expected<std::string, std::string> ModuleStore::insert_locked(const Digest& digest,
                                                                   std::vector<std::uint8_t> wasm) {
    const std::string hash = to_hex(digest);
    {
        std::lock_guard lock(mu_);
        if (auto it = slots_.find(hash); it != slots_.end()) {
            it->second.entry.last_used_ms = now_ms();
            it->second.dirty = true;
            return hash;
        }
    }
    auto info = check(wasm);
    if (!info) return std::unexpected(info.error());
    Slot slot;
    slot.entry.hash = hash;
    slot.entry.size = wasm.size();
    slot.entry.added_ms = slot.entry.last_used_ms = now_ms();
    slot.info = std::move(*info);
    std::lock_guard lock(mu_);
    if (slots_.contains(hash)) return hash;  // added concurrently
    if (config_.dir.empty()) {
        slot.bytes = std::move(wasm);
    } else {
        if (auto ok = write_atomically(wasm_path(hash), wasm); !ok) return std::unexpected(ok.error());
        write_meta(slot);
    }
    slots_.emplace(hash, std::move(slot));
    return hash;
}

std::expected<std::string, std::string> ModuleStore::insert(const Digest& digest, std::vector<std::uint8_t> wasm) {
    auto r = insert_locked(digest, std::move(wasm));
    report();
    return r;
}

bool ModuleStore::contains(std::string_view hash) const {
    std::lock_guard lock(mu_);
    return slots_.contains(hash);
}

std::expected<std::vector<std::uint8_t>, std::string> ModuleStore::read(const std::string& hash,
                                                                        const Slot& slot) const {
    if (config_.dir.empty()) return slot.bytes;
    auto bytes = wasm::read_file(wasm_path(hash).string());
    if (!bytes) return std::unexpected(bytes.error());
    if (to_hex(sha256(*bytes)) != hash) return std::unexpected("module " + hash + " is damaged in the store");
    return std::move(*bytes);
}

std::expected<std::vector<std::uint8_t>, std::string> ModuleStore::bytes(std::string_view hash) const {
    std::lock_guard lock(mu_);
    auto it = slots_.find(hash);
    if (it == slots_.end()) return std::unexpected("unknown module " + std::string(hash));
    return read(it->first, it->second);
}

std::optional<wasm::ModuleInfo> ModuleStore::info(std::string_view hash) const {
    std::lock_guard lock(mu_);
    auto it = slots_.find(hash);
    if (it == slots_.end()) return std::nullopt;
    return it->second.info;
}

std::optional<ModuleEntry> ModuleStore::entry(std::string_view hash) const {
    std::lock_guard lock(mu_);
    auto it = slots_.find(hash);
    if (it == slots_.end()) return std::nullopt;
    ModuleEntry e = it->second.entry;
    e.loaded = !it->second.compiled.expired();
    return e;
}

std::vector<ModuleEntry> ModuleStore::list() const {
    std::lock_guard lock(mu_);
    std::vector<ModuleEntry> out;
    for (const auto& [hash, slot] : slots_) {
        out.push_back(slot.entry);
        out.back().loaded = !slot.compiled.expired();
    }
    return out;
}

std::expected<std::shared_ptr<wasm::Module>, std::string> ModuleStore::acquire(std::string_view hash_view) {
    const std::string hash(hash_view);
    std::vector<std::uint8_t> bytes;
    {
        std::lock_guard lock(mu_);
        auto it = slots_.find(hash);
        if (it == slots_.end()) return std::unexpected("unknown module " + hash);
        it->second.entry.last_used_ms = now_ms();
        it->second.dirty = true;
        if (auto m = it->second.compiled.lock()) {
            ++stats_.hits;
            remember(hash, m);
            return m;
        }
        auto read_bytes = read(hash, it->second);
        if (!read_bytes) return std::unexpected(read_bytes.error());
        bytes = std::move(*read_bytes);
    }
    // Compiling takes a while; other modules stay available meanwhile.
    auto module = wasm::Module::load(std::move(bytes), wasm::ImportPolicy::system());
    if (!module) return std::unexpected(module.error());
    std::lock_guard lock(mu_);
    auto it = slots_.find(hash);
    if (it == slots_.end()) return std::move(*module);  // collected meanwhile; the caller still runs it
    if (auto m = it->second.compiled.lock()) {
        ++stats_.hits;  // compiled concurrently: share that one
        remember(hash, m);
        return m;
    }
    ++stats_.loads;
    it->second.compiled = *module;
    remember(hash, *module);
    return std::move(*module);
}

void ModuleStore::remember(const std::string& hash, std::shared_ptr<wasm::Module> module) {
    if (auto it = std::ranges::find(lru_, hash, [](const auto& e) { return e.first; }); it != lru_.end()) {
        lru_.splice(lru_.begin(), lru_, it);
        return;
    }
    lru_bytes_ += module->size();
    lru_.emplace_front(hash, std::move(module));
    trim();
}

void ModuleStore::trim() {
    while (lru_bytes_ > config_.cache_bytes && !lru_.empty()) {
        lru_bytes_ -= lru_.back().second->size();
        lru_.pop_back();
        ++stats_.evictions;
    }
    stats_.cached_bytes = lru_bytes_;
}

std::vector<std::string> ModuleStore::cached() const {
    std::lock_guard lock(mu_);
    std::vector<std::string> out;
    for (const auto& [hash, module] : lru_) out.push_back(hash);
    return out;
}

ModuleCacheStats ModuleStore::stats() const {
    std::lock_guard lock(mu_);
    ModuleCacheStats s = stats_;
    s.cached_bytes = lru_bytes_;
    return s;
}

bool ModuleStore::add_user(std::string_view hash) {
    std::lock_guard lock(mu_);
    auto it = slots_.find(hash);
    if (it == slots_.end()) return false;
    ++it->second.entry.users;
    return true;
}

void ModuleStore::remove_user(std::string_view hash) {
    std::lock_guard lock(mu_);
    auto it = slots_.find(hash);
    if (it == slots_.end() || it->second.entry.users == 0) return;
    --it->second.entry.users;
    it->second.entry.last_used_ms = now_ms();
    it->second.dirty = true;
}

bool ModuleStore::pin(std::string_view hash, bool pinned) {
    {
        std::lock_guard lock(mu_);
        auto it = slots_.find(hash);
        if (it == slots_.end()) return false;
        it->second.entry.pinned = pinned;
        write_meta(it->second);
    }
    report();
    return true;
}

ModuleCollection ModuleStore::collect(std::chrono::milliseconds grace) {
    ModuleCollection out = collect_locked(grace);
    report();
    return out;
}

ModuleCollection ModuleStore::collect_locked(std::chrono::milliseconds grace) {
    ModuleCollection out;
    std::lock_guard lock(mu_);
    const std::int64_t now = now_ms();
    for (auto it = slots_.begin(); it != slots_.end();) {
        const ModuleEntry& e = it->second.entry;
        if (e.users > 0 || e.pinned || now - e.last_used_ms < grace.count()) {
            ++it;
            continue;
        }
        const std::string hash = it->first;
        if (!config_.dir.empty()) {
            std::error_code ec;
            fs::remove(wasm_path(hash), ec);
            if (ec) {
                pending_.push_back("module store: cannot remove " + wasm_path(hash).string() + ": " + ec.message());
                ++it;
                continue;
            }
            fs::remove(meta_path(hash), ec);
        }
        if (auto l = std::ranges::find(lru_, hash, [](const auto& p) { return p.first; }); l != lru_.end()) {
            lru_bytes_ -= l->second->size();
            lru_.erase(l);
        }
        out.removed.push_back(hash);
        out.bytes += e.size;
        it = slots_.erase(it);
    }
    stats_.cached_bytes = lru_bytes_;
    for (auto& [hash, slot] : slots_) {
        if (slot.dirty) write_meta(slot);
    }
    return out;
}

void ModuleStore::write_meta(Slot& slot) {
    slot.dirty = false;
    if (config_.dir.empty()) return;
    const Meta meta{slot.entry.added_ms, slot.entry.last_used_ms, slot.entry.pinned};
    if (auto ok = write_atomically(meta_path(slot.entry.hash), encode_meta(meta)); !ok) {
        pending_.push_back("module store: " + ok.error());
    }
}

void ModuleStore::report() {
    std::vector<std::string> pending;
    {
        std::lock_guard lock(mu_);
        pending.swap(pending_);
    }
    for (const auto& text : pending) warn_(text);
}

void ModuleStore::flush() {
    {
        std::lock_guard lock(mu_);
        for (auto& [hash, slot] : slots_) {
            if (slot.dirty) write_meta(slot);
        }
    }
    report();
}

}  // namespace paglets::runtime
