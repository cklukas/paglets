// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Memory images: capture and restore of a paglet instance at a quiescent
// point (between two handler calls). The image holds the complete linear
// memory (split into 64 KB pages, each hashed; zero pages are elided) and all
// mutable globals. It is bound to the exact module by its SHA-256 hash and is
// independent of the host CPU architecture and operating system.

#pragma once

#include <paglets/sha256.hpp>
#include <paglets/wasm/engine.hpp>

#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace paglets::wasm {

struct GlobalValue {
    std::string name;       // export name
    std::uint8_t type = 0;  // ValType byte
    std::uint64_t bits = 0;
};

struct PageEntry {
    bool zero = true;
    Digest hash{};  // unset for zero pages
};

struct Snapshot {
    Digest module_hash{};
    std::uint32_t page_count = 0;
    std::vector<GlobalValue> globals;
    std::vector<PageEntry> pages;    // page_count entries
    std::vector<std::uint8_t> data;  // non-zero pages, concatenated in order

    std::uint32_t data_pages() const { return static_cast<std::uint32_t>(data.size() / page_size); }
};

std::expected<Snapshot, std::string> capture(Instance& instance);

// Instantiates the module without running its initializer and installs the
// image. The module hash must match the image.
std::expected<std::unique_ptr<Instance>, std::string> restore(std::shared_ptr<Module> module, const Snapshot& snapshot,
                                                              const Limits& limits = {});

// Binary image format "PGIMG001" (little endian).
std::vector<std::uint8_t> serialize(const Snapshot& snapshot);
std::expected<Snapshot, std::string> deserialize(std::span<const std::uint8_t> bytes);

}  // namespace paglets::wasm
