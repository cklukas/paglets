// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "runtime_store.hpp"

#include <paglets/runtime/mobility.hpp>

#include <paglets/sha256.hpp>

#include <fstream>
#include <system_error>

namespace paglets::runtime {

namespace {

namespace fs = std::filesystem;
using abi::detail::put;
using abi::detail::put_opt;

constexpr std::string_view file_magic = "PGPAGLET1";

}  // namespace

void encode_cap(msgpack::Writer& w, const Cap& c) {
    w.write_map_header(18);
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
    put(w, "resource_type", c.resource_type);
    put(w, "resource", c.resource);
    put(w, "id", c.id);
    put(w, "lineage", c.lineage);
    put(w, "grant", c.grant);
    put(w, "host", c.host);
}

bool decode_cap(msgpack::Reader& r, Cap& c) {
    return abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "kind") {
            std::int32_t kind = 0;
            if (!msgpack::read_value(r, kind) || kind < 0 || kind > 3) return false;
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
        if (k == "resource_type") return msgpack::read_value(r, c.resource_type);
        if (k == "resource") return msgpack::read_value(r, c.resource);
        if (k == "id") return msgpack::read_value(r, c.id);
        if (k == "lineage") return msgpack::read_value(r, c.lineage);
        if (k == "grant") return msgpack::read_value(r, c.grant);
        if (k == "host") return msgpack::read_value(r, c.host);
        return r.skip();
    });
}

namespace {

void encode_record(msgpack::Writer& w, const PagletRecord& p) {
    w.write_map_header(13);
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
    w.write_str("services");
    w.write_array_header(p.services.size());
    for (const auto& [name, h] : p.services) {
        w.write_array_header(2);
        w.write_str(name);
        w.write_int(h);
    }
    put(w, "marks", p.marks);
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
            if (!r.read_array_header(n) || n > r.remaining()) return false;
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
        if (k == "marks") return msgpack::read_value(r, p.marks);
        if (k == "services") {
            std::uint32_t n = 0;
            if (!r.read_array_header(n) || n > r.remaining()) return false;
            for (std::uint32_t i = 0; i < n; ++i) {
                std::uint32_t pair = 0;
                std::string name;
                std::int32_t h = 0;
                if (!r.read_array_header(pair) || pair != 2 || !msgpack::read_value(r, name) ||
                    !msgpack::read_value(r, h)) {
                    return false;
                }
                p.services.emplace_back(std::move(name), h);
            }
            return true;
        }
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
    fs::create_directories(root_ / "paglets");
}

fs::path Store::paglet_file(const std::string& id) const {
    return root_ / "paglets" / (id + ".paglet");
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

std::expected<void, std::string> Store::save_revoked(const std::vector<std::string>& ids) {
    msgpack::Writer w;
    msgpack::write_value(w, ids);
    return write_atomically(root_ / "revoked", w.bytes());
}

std::vector<std::string> Store::load_revoked(const Warn& warn) const {
    const fs::path path = root_ / "revoked";
    std::error_code ec;
    if (!fs::exists(path, ec)) return {};
    auto bytes = wasm::read_file(path.string());
    std::vector<std::string> ids;
    if (!bytes) {
        warn(bytes.error());
        return ids;
    }
    msgpack::Reader r(*bytes);
    if (!msgpack::read_value(r, ids) || !r.at_end()) {
        warn("damaged file " + path.string());
        ids.clear();
    }
    return ids;
}

// -- travelling state (mobility.hpp) ---------------------------------------------

namespace {
constexpr std::int32_t state_version = 1;
}  // namespace

Bytes encode_state(const TravelState& s) {
    msgpack::Writer w;
    w.write_map_header(13);
    put(w, "v", state_version);
    put(w, "id", s.id);
    put(w, "module", s.module);
    put(w, "trust", s.trust);
    put(w, "owner", s.owner);
    put(w, "next_handle", s.next_handle);
    put(w, "next_correlation", s.next_correlation);
    put(w, "next_timer", s.next_timer);
    put(w, "checkpoint_ms", s.checkpoint_ms);
    w.write_str("services");
    w.write_array_header(s.services.size());
    for (const auto& [name, h] : s.services) {
        w.write_array_header(2);
        w.write_str(name);
        w.write_int(h);
    }
    w.write_str("caps");
    w.write_array_header(s.caps.size());
    for (const auto& [h, cap] : s.caps) {
        w.write_array_header(2);
        w.write_int(h);
        encode_cap(w, cap);
    }
    w.write_str("pending");
    w.write_array_header(s.pending.size());
    for (const auto& [correlation, ms] : s.pending) {
        w.write_array_header(2);
        w.write_uint(correlation);
        w.write_int(ms);
    }
    put(w, "marks", s.marks);
    return w.bytes();
}

std::expected<TravelState, std::string> decode_state(std::span<const std::uint8_t> bytes) {
    TravelState s;
    std::int32_t version = 0;
    msgpack::Reader r(bytes);
    const bool ok = abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "v") return msgpack::read_value(r, version);
        if (k == "id") return msgpack::read_value(r, s.id);
        if (k == "module") return msgpack::read_value(r, s.module);
        if (k == "trust") return msgpack::read_value(r, s.trust);
        if (k == "owner") return msgpack::read_value(r, s.owner);
        if (k == "next_handle") return msgpack::read_value(r, s.next_handle);
        if (k == "next_correlation") return msgpack::read_value(r, s.next_correlation);
        if (k == "next_timer") return msgpack::read_value(r, s.next_timer);
        if (k == "checkpoint_ms") return msgpack::read_value(r, s.checkpoint_ms);
        if (k == "marks") return msgpack::read_value(r, s.marks);
        auto pairs = [&](auto&& one) {
            std::uint32_t n = 0;
            if (!r.read_array_header(n) || n > r.remaining()) return false;
            for (std::uint32_t i = 0; i < n; ++i) {
                std::uint32_t pair = 0;
                if (!r.read_array_header(pair) || pair != 2 || !one()) return false;
            }
            return true;
        };
        if (k == "services") {
            return pairs([&] {
                std::string name;
                std::int32_t h = 0;
                if (!msgpack::read_value(r, name) || !msgpack::read_value(r, h)) return false;
                s.services.emplace_back(std::move(name), h);
                return true;
            });
        }
        if (k == "caps") {
            return pairs([&] {
                std::int32_t h = 0;
                Cap cap;
                if (!msgpack::read_value(r, h) || !decode_cap(r, cap)) return false;
                s.caps.emplace_back(h, std::move(cap));
                return true;
            });
        }
        if (k == "pending") {
            return pairs([&] {
                std::uint64_t correlation = 0;
                std::int64_t ms = 0;
                if (!msgpack::read_value(r, correlation) || !msgpack::read_value(r, ms)) return false;
                s.pending.emplace_back(correlation, ms);
                return true;
            });
        }
        return r.skip();
    });
    if (!ok || !r.at_end()) return std::unexpected(std::string("malformed paglet state"));
    if (version != state_version) return std::unexpected("paglet state version " + std::to_string(version));
    if (s.id.empty() || s.module.empty()) return std::unexpected(std::string("incomplete paglet state"));
    return s;
}

Bytes encode_caps(const std::vector<Cap>& caps) {
    msgpack::Writer w;
    w.write_array_header(caps.size());
    for (const auto& c : caps) encode_cap(w, c);
    return w.bytes();
}

std::expected<std::vector<Cap>, std::string> decode_caps(std::span<const std::uint8_t> bytes) {
    msgpack::Reader r(bytes);
    std::uint32_t n = 0;
    if (!r.read_array_header(n) || n > r.remaining()) return std::unexpected(std::string("malformed capabilities"));
    std::vector<Cap> out;
    for (std::uint32_t i = 0; i < n; ++i) {
        if (!decode_cap(r, out.emplace_back())) return std::unexpected(std::string("malformed capabilities"));
    }
    if (!r.at_end()) return std::unexpected(std::string("malformed capabilities"));
    return out;
}

Bytes encode_remote(const RemoteMessage& m) {
    msgpack::Writer w;
    w.write_map_header(16);
    put(w, "kind", static_cast<std::int32_t>(m.kind));
    put(w, "target", m.target);
    put(w, "host", m.host);
    put(w, "name", m.name);
    put(w, "payload", m.payload);
    put(w, "priority", m.priority);
    w.write_str("sender");
    if (m.sender) {
        abi::paglets_encode(w, *m.sender);
    } else {
        w.write_nil();
    }
    put(w, "badge", m.badge);
    w.write_str("caps");
    w.write_array_header(m.caps.size());
    for (const auto& c : m.caps) encode_cap(w, c);
    put(w, "requester", m.requester);
    put(w, "requester_host", m.requester_host);
    put(w, "correlation", m.correlation);
    put(w, "timeout_ms", m.timeout_ms);
    put(w, "status", m.status);
    put(w, "hops", m.hops);
    put(w, "v", std::int32_t{1});
    return w.bytes();
}

std::expected<RemoteMessage, std::string> decode_remote(std::span<const std::uint8_t> bytes) {
    RemoteMessage m;
    std::int32_t kind = -1;
    msgpack::Reader r(bytes);
    const bool ok = abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "kind") return msgpack::read_value(r, kind);
        if (k == "target") return msgpack::read_value(r, m.target);
        if (k == "host") return msgpack::read_value(r, m.host);
        if (k == "name") return msgpack::read_value(r, m.name);
        if (k == "payload") return msgpack::read_value(r, m.payload);
        if (k == "priority") return msgpack::read_value(r, m.priority);
        if (k == "sender") {
            if (r.peek() == msgpack::Kind::nil) return r.read_nil();
            abi::SenderRecord s;
            if (!abi::paglets_decode(r, s)) return false;
            m.sender = std::move(s);
            return true;
        }
        if (k == "badge") return msgpack::read_value(r, m.badge);
        if (k == "caps") {
            std::uint32_t n = 0;
            if (!r.read_array_header(n) || n > r.remaining()) return false;
            m.caps.clear();
            for (std::uint32_t i = 0; i < n; ++i) {
                if (!decode_cap(r, m.caps.emplace_back())) return false;
            }
            return true;
        }
        if (k == "requester") return msgpack::read_value(r, m.requester);
        if (k == "requester_host") return msgpack::read_value(r, m.requester_host);
        if (k == "correlation") return msgpack::read_value(r, m.correlation);
        if (k == "timeout_ms") return msgpack::read_value(r, m.timeout_ms);
        if (k == "status") return msgpack::read_value(r, m.status);
        if (k == "hops") return msgpack::read_value(r, m.hops);
        return r.skip();
    });
    if (!ok || !r.at_end() || kind < 0 || kind > 2) return std::unexpected(std::string("malformed remote message"));
    m.kind = static_cast<RemoteMessage::Kind>(kind);
    return m;
}

}  // namespace paglets::runtime
