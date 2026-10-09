// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Internal to the runtime: capability entries, the durable paglet record and
// the state directory (modules and paglets with their last memory image).

#pragma once

#include <paglets/abi.hpp>
#include <paglets/runtime/capability.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wasm/snapshot.hpp>

#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace paglets::runtime {

// What survives a restart besides the memory image.
struct PagletRecord {
    std::string id;
    std::string module;  // hex hash
    std::string trust;
    std::string owner;
    bool started = false;
    std::int32_t next_handle = 2;
    std::uint64_t next_correlation = 1;
    std::uint64_t next_timer = 1;
    std::vector<std::pair<std::int32_t, Cap>> caps;
    std::vector<std::uint64_t> pending_requests;
    std::int64_t checkpoint_ms = -1;                             // per-paglet checkpoint interval; -1: host default
    std::vector<std::pair<std::string, std::int32_t>> services;  // default service endpoints: name, handle
    std::vector<std::string> marks;                              // data residency marks
};

using Warn = std::function<void(const std::string&)>;

// Capability entries in MessagePack (stored records, travelling state).
void encode_cap(msgpack::Writer& w, const Cap& c);
bool decode_cap(msgpack::Reader& r, Cap& c);

// State directory layout:
//   modules/               the module store (modules.hpp)
//   paglets/<id>.paglet    record and last memory image, replaced atomically
//   storage/<id>/          durable storage of a paglet (see Runtime::storage_dir)
//   work/<id>/             scratch space, cleared when the host starts
class Store {
public:
    explicit Store(std::filesystem::path root);

    std::expected<void, std::string> save_paglet(const PagletRecord& record, const wasm::Snapshot& image);
    std::vector<PagletRecord> load_paglets(const Warn& warn) const;
    std::expected<wasm::Snapshot, std::string> load_image(const std::string& id) const;
    void remove_paglet(const std::string& id);

    // Capability IDs revoked on this host (grant revocations come from the
    // ledger and are not stored here).
    std::expected<void, std::string> save_revoked(const std::vector<std::string>& ids);
    std::vector<std::string> load_revoked(const Warn& warn) const;

    const std::filesystem::path& root() const { return root_; }

private:
    std::filesystem::path paglet_file(const std::string& id) const;

    std::filesystem::path root_;
};

}  // namespace paglets::runtime
