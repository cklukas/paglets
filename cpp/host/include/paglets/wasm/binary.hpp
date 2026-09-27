// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Runtime-independent reader for the parts of a Wasm binary the host needs
// before running a module: imports (for import validation per trust class),
// defined globals (every mutable global must be exported so memory images can
// capture it), exports and memory limits.

#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::wasm {

enum class ExternKind : std::uint8_t { function = 0, table = 1, memory = 2, global = 3, tag = 4 };

enum class ValType : std::uint8_t {
    i32 = 0x7f,
    i64 = 0x7e,
    f32 = 0x7d,
    f64 = 0x7c,
    v128 = 0x7b,
    funcref = 0x70,
    externref = 0x6f,
};

std::string_view to_string(ExternKind kind);
std::string_view to_string(ValType type);

struct Import {
    std::string module;
    std::string name;
    ExternKind kind = ExternKind::function;
};

struct Export {
    std::string name;
    ExternKind kind = ExternKind::function;
    std::uint32_t index = 0;
};

struct Global {
    std::uint32_t index = 0;  // index in the global index space
    ValType type = ValType::i32;
    bool is_mutable = false;
    bool imported = false;
    std::optional<std::string> export_name;
};

struct MemoryLimits {
    std::uint64_t min_pages = 0;
    std::optional<std::uint64_t> max_pages;
    bool memory64 = false;
};

struct ModuleInfo {
    std::vector<Import> imports;
    std::vector<Export> exports;
    std::vector<Global> globals;  // imported and defined, in index order
    std::optional<MemoryLimits> memory;
    bool has_start_function = false;

    const Export* find_export(std::string_view name) const;
};

std::expected<ModuleInfo, std::string> parse_module(std::span<const std::uint8_t> bytes);

// Import allowlist. Entries are "module.name" or "module.*".
class ImportPolicy {
public:
    ImportPolicy() = default;
    explicit ImportPolicy(std::vector<std::string> allowed) : allowed_(std::move(allowed)) {}

    // Standard import set of roaming paglets in the M0 spike: the paglets
    // host API plus a minimal WASI subset (no files, no sockets).
    static ImportPolicy standard();

    bool allows(const Import& import) const;

    // Returns an error message naming every import that is not allowed.
    std::expected<void, std::string> check(const ModuleInfo& info) const;

private:
    std::vector<std::string> allowed_;
};

// Checks the memory-image requirement: every mutable defined global is
// exported, so a snapshot can read and restore it.
std::expected<void, std::string> check_snapshot_ready(const ModuleInfo& info);

}  // namespace paglets::wasm
