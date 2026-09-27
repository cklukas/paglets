// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "runtime_store.hpp"

#include <paglets/sha256.hpp>

#include <fstream>
#include <system_error>

namespace paglets::runtime {

namespace {

namespace fs = std::filesystem;
using abi::detail::put;
using abi::detail::put_opt;

constexpr std::string_view file_magic = "PGPAGLET1";

void encode_cap(msgpack::Writer& w, const Cap& c) {
    w.write_map_header(12);
    put(w, "kind", static_cast<std::int32_t>(c.kind));
    put(w, "target", c.target);
    put(w, "ops", c.ops);
    put(w, "transferable", c.transferable);
    put(w, "badge", c.badge);
    put(w, "expires", c.expires);
    put(w, "uses_left", c.uses_left);
    put(w, "correlation", c.correlation);
    put(w, "timer_id", c.timer_id);
    put(w, "fire_at", c.fire_at);
    w.write_str("message");
    abi::paglets_encode(w, c.message);
    put(w, "reserved", false);
}

bool decode_cap(msgpack::Reader& r, Cap& c) {
    return abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "kind") {
            std::int32_t kind = 0;
            if (!msgpack::read_value(r, kind) || kind < 0 || kind > 2) return false;
            c.kind = static_cast<Cap::Kind>(kind);
            return true;
        }
        if (k == "target") return msgpack::read_value(r, c.target);
        if (k == "ops") return msgpack::read_value(r, c.ops);
        if (k == "transferable") return msgpack::read_value(r, c.transferable);
        if (k == "badge") return msgpack::read_value(r, c.badge);
        if (k == "expires") return msgpack::read_value(r, c.expires);
        if (k == "uses_left") return msgpack::read_value(r, c.uses_left);
        if (k == "correlation") return msgpack::read_value(r, c.correlation);
        if (k == "timer_id") return msgpack::read_value(r, c.timer_id);
        if (k == "fire_at") return msgpack::read_value(r, c.fire_at);
        if (k == "message") return abi::paglets_decode(r, c.message);
        return r.skip();
    });
}

void encode_record(msgpack::Writer& w, const PagletRecord& p) {
    w.write_map_header(11);
    put(w, "id", p.id);
    put(w, "module", p.module);
    put(w, "trust", p.trust);
    put(w, "owner", p.owner);
    put(w, "started", p.started);
    put(w, "next_handle", p.next_handle);
    put(w, "next_correlation", p.next_correlation);
    put(w, "next_timer", p.next_timer);
    w.write_str("caps");
    w.write_array_header(p.caps.size());
    for (const auto& [h, cap] : p.caps) {
        w.write_array_header(2);
        w.write_int(h);
        encode_cap(w, cap);
    }
    put(w, "pending_requests", p.pending_requests);
    put(w, "checkpoint_ms", p.checkpoint_ms);
}

bool decode_record(msgpack::Reader& r, PagletRecord& p) {
    return abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "id") return msgpack::read_value(r, p.id);
        if (k == "module") return msgpack::read_value(r, p.module);
        if (k == "trust") return msgpack::read_value(r, p.trust);
        if (k == "owner") return msgpack::read_value(r, p.owner);
        if (k == "started") return msgpack::read_value(r, p.started);
        if (k == "next_handle") return msgpack::read_value(r, p.next_handle);
        if (k == "next_correlation") return msgpack::read_value(r, p.next_correlation);
        if (k == "next_timer") return msgpack::read_value(r, p.next_timer);
        if (k == "caps") {
            std::uint32_t n = 0;
            if (!r.read_array_header(n)) return false;
            for (std::uint32_t i = 0; i < n; ++i) {
                std::uint32_t pair = 0;
                std::int32_t h = 0;
                Cap cap;
                if (!r.read_array_header(pair) || pair != 2 || !msgpack::read_value(r, h) || !decode_cap(r, cap)) {
                    return false;
                }
                p.caps.emplace_back(h, std::move(cap));
            }
            return true;
        }
        if (k == "pending_requests") return msgpack::read_value(r, p.pending_requests);
        if (k == "checkpoint_ms") return msgpack::read_value(r, p.checkpoint_ms);
        return r.skip();
    });
}

// Writes to a temporary file and renames it over the target, so a crash
// leaves either the old or the new file.
std::expected<void, std::string> write_atomically(const fs::path& path, std::span<const std::uint8_t> data) {
    const fs::path tmp = path.string() + ".tmp";
    if (auto ok = wasm::write_file(tmp.string(), data); !ok) return ok;
    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (ec) return std::unexpected("cannot replace " + path.string() + ": " + ec.message());
    return {};
}

struct PagletFile {
    PagletRecord record;
    std::vector<std::uint8_t> image;
};

std::expected<PagletFile, std::string> read_paglet_file(const fs::path& path) {
    auto bytes = wasm::read_file(path.string());
    if (!bytes) return std::unexpected(bytes.error());
    msgpack::Reader r(*bytes);
    std::uint32_t n = 0;
    std::string_view magic;
    PagletFile f;
    if (!r.read_array_header(n) || n != 3 || !r.read_str(magic) || magic != file_magic || !decode_record(r, f.record) ||
        !r.read_bin(f.image) || !r.at_end()) {
        return std::unexpected("damaged paglet file " + path.string());
    }
    return f;
}

}  // namespace

Store::Store(fs::path root) : root_(std::move(root)) {
    fs::create_directories(root_ / "modules");
    fs::create_directories(root_ / "paglets");
}

fs::path Store::paglet_file(const std::string& id) const {
    return root_ / "paglets" / (id + ".paglet");
}

std::expected<void, std::string> Store::save_module(const std::string& hash, std::span<const std::uint8_t> bytes) {
    const fs::path path = root_ / "modules" / (hash + ".wasm");
    if (fs::exists(path)) return {};
    return write_atomically(path, bytes);
}

std::vector<std::shared_ptr<wasm::Module>> Store::load_modules(const Warn& warn) const {
    std::vector<std::shared_ptr<wasm::Module>> out;
    for (const auto& entry : fs::directory_iterator(root_ / "modules")) {
        if (entry.path().extension() != ".wasm") continue;
        auto bytes = wasm::read_file(entry.path().string());
        if (!bytes) {
            warn(bytes.error());
            continue;
        }
        if (to_hex(sha256(*bytes)) != entry.path().stem().string()) {
            warn("module file does not match its hash: " + entry.path().string());
            continue;
        }
        auto module = wasm::Module::load(std::move(*bytes), wasm::ImportPolicy::system());
        if (!module) {
            warn(module.error());
            continue;
        }
        out.push_back(std::move(*module));
    }
    return out;
}

std::expected<void, std::string> Store::save_paglet(const PagletRecord& record, const wasm::Snapshot& image) {
    msgpack::Writer w;
    w.write_array_header(3);
    w.write_str(file_magic);
    encode_record(w, record);
    w.write_bin(wasm::serialize(image));
    return write_atomically(paglet_file(record.id), w.bytes());
}

std::vector<PagletRecord> Store::load_paglets(const Warn& warn) const {
    std::vector<PagletRecord> out;
    for (const auto& entry : fs::directory_iterator(root_ / "paglets")) {
        if (entry.path().extension() != ".paglet") continue;
        auto f = read_paglet_file(entry.path());
        if (!f) {
            warn(f.error());
            continue;
        }
        out.push_back(std::move(f->record));
    }
    return out;
}

std::expected<wasm::Snapshot, std::string> Store::load_image(const std::string& id) const {
    auto f = read_paglet_file(paglet_file(id));
    if (!f) return std::unexpected(f.error());
    return wasm::deserialize(f->image);
}

void Store::remove_paglet(const std::string& id) {
    std::error_code ec;
    fs::remove(paglet_file(id), ec);
}

}  // namespace paglets::runtime
