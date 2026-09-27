// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Internal to the runtime: capability entries, the durable paglet record and
// the state directory (modules and paglets with their last memory image).

#pragma once

#include <paglets/abi.hpp>
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

struct Cap {
    enum class Kind : std::int32_t { endpoint = 0, reply = 1, timer = 2 };
    Kind kind = Kind::endpoint;
    std::string target;  // endpoint: paglet ID; reply: requester (empty: the host); timer: owner
    std::vector<std::string> ops;
    bool transferable = true;
    std::optional<std::string> badge;
    std::optional<std::int64_t> expires;  // Unix milliseconds
    std::optional<std::int64_t> uses_left;
    std::uint64_t correlation = 0;  // reply
    std::uint64_t timer_id = 0;     // timer
    std::int64_t fire_at = 0;       // timer, Unix milliseconds
    abi::OutMessage message;        // timer: the message to deliver
};

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
    std::int64_t checkpoint_ms = -1;  // per-paglet checkpoint interval; -1: host default
};

using Warn = std::function<void(const std::string&)>;

// State directory layout:
//   modules/<hash>.wasm    paglet modules by SHA-256
//   paglets/<id>.paglet    record and last memory image, replaced atomically
//   storage/<id>/          durable storage of a paglet (see Runtime::storage_dir)
//   work/<id>/             scratch space, cleared when the host starts
class Store {
public:
    explicit Store(std::filesystem::path root);

    std::expected<void, std::string> save_module(const std::string& hash, std::span<const std::uint8_t> bytes);
    std::vector<std::shared_ptr<wasm::Module>> load_modules(const Warn& warn) const;

    std::expected<void, std::string> save_paglet(const PagletRecord& record, const wasm::Snapshot& image);
    std::vector<PagletRecord> load_paglets(const Warn& warn) const;
    std::expected<wasm::Snapshot, std::string> load_image(const std::string& id) const;
    void remove_paglet(const std::string& id);

    const std::filesystem::path& root() const { return root_; }

private:
    std::filesystem::path paglet_file(const std::string& id) const;

    std::filesystem::path root_;
};

}  // namespace paglets::runtime
