// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Thin C++ layer over the embedded WAMR runtime: modules, instances, limits,
// asynchronous termination and the paglet message ABI of the M0 spike
// (abi v0: paglets_alloc / paglets_handle / paglets_free, import paglets.log).

#pragma once

#include <paglets/sha256.hpp>
#include <paglets/wasm/binary.hpp>

#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// Opaque WAMR handles (from wasm_export.h), kept out of this header.
struct WASMModuleCommon;
struct WASMModuleInstanceCommon;
struct WASMExecEnv;

namespace paglets::wasm {

inline constexpr std::uint32_t page_size = 65536;

// Process-wide runtime. Initializes WAMR once and registers the paglets host
// functions; safe to call from any thread.
void ensure_runtime();

// Must be called once on every thread (other than the first) that executes
// Wasm code, before the first call.
void init_thread();

struct Limits {
    std::uint32_t stack_size = 64 * 1024;  // Wasm operand/call stack of the interpreter
    std::uint32_t max_memory_pages = 256;  // 16 MB linear memory
};

class Module {
public:
    // Parses, validates imports against the policy, and loads the module.
    static std::expected<std::shared_ptr<Module>, std::string> load(std::vector<std::uint8_t> bytes,
                                                                    const ImportPolicy& policy);
    ~Module();
    Module(const Module&) = delete;
    Module& operator=(const Module&) = delete;

    const ModuleInfo& info() const { return info_; }
    const Digest& hash() const { return hash_; }
    std::string hash_hex() const { return to_hex(hash_); }
    std::size_t size() const { return bytes_.size(); }
    WASMModuleCommon* handle() const { return module_; }

private:
    Module() = default;

    std::vector<std::uint8_t> bytes_;  // WAMR keeps pointers into this buffer
    ModuleInfo info_;
    Digest hash_{};
    WASMModuleCommon* module_ = nullptr;
};

using LogSink = std::function<void(std::string_view)>;

// Transparent string hash for lookups by std::string_view.
struct StringHash {
    using is_transparent = void;
    std::size_t operator()(std::string_view s) const { return std::hash<std::string_view>{}(s); }
};

class Instance {
public:
    // Instantiates the module. With run_initializer, the reactor initializer
    // (_initialize) runs; memory-image restore skips it.
    static std::expected<std::unique_ptr<Instance>, std::string> create(std::shared_ptr<Module> module,
                                                                        const Limits& limits = {},
                                                                        bool run_initializer = true);
    ~Instance();
    Instance(const Instance&) = delete;
    Instance& operator=(const Instance&) = delete;

    const std::shared_ptr<Module>& module() const { return module_; }
    const Limits& limits() const { return limits_; }

    // Calls an export with i32 arguments; result cells: 0 (void), 1 (i32) or 2 (i64).
    std::expected<std::uint64_t, std::string> call(std::string_view name, std::span<const std::uint32_t> args,
                                                   unsigned result_cells);
    std::expected<std::int32_t, std::string> call_i32(std::string_view name,
                                                      std::initializer_list<std::uint32_t> args = {});
    std::expected<void, std::string> call_void(std::string_view name, std::initializer_list<std::uint32_t> args = {});

    // ABI v0: copies the request into guest memory, calls paglets_handle and
    // returns a copy of the reply.
    std::expected<std::vector<std::uint8_t>, std::string> send(std::span<const std::uint8_t> request);

    // Makes a running call fail with "terminated"; callable from any thread.
    void terminate();

    // Linear memory.
    std::uint32_t page_count() const;
    std::span<std::uint8_t> memory();
    bool grow_to(std::uint32_t pages);

    // Exported globals, as raw 64-bit patterns (i32/f32 use the low 32 bits).
    std::expected<std::uint64_t, std::string> global_bits(std::string_view name) const;
    std::expected<void, std::string> set_global_bits(std::string_view name, std::uint64_t bits);

    void set_log_sink(LogSink sink) { log_sink_ = std::move(sink); }
    void log(std::string_view text) const;

private:
    Instance() = default;

    std::shared_ptr<Module> module_;
    Limits limits_;
    WASMModuleInstanceCommon* inst_ = nullptr;
    WASMExecEnv* exec_env_ = nullptr;
    std::unordered_map<std::string, void*, StringHash, std::equal_to<>> functions_;  // export lookup cache
    LogSink log_sink_;
};

std::expected<std::vector<std::uint8_t>, std::string> read_file(const std::string& path);
std::expected<void, std::string> write_file(const std::string& path, std::span<const std::uint8_t> data);

}  // namespace paglets::wasm
