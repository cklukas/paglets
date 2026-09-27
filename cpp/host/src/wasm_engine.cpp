// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/wasm/engine.hpp>

#include <wasm_export.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>

namespace paglets::wasm {

namespace {

// paglets.log(ptr, len): WAMR validates the (pointer, length) pair ("*~")
// against the instance memory before the call.
void native_log(wasm_exec_env_t env, const char* text, std::uint32_t len) {
    wasm_module_inst_t inst = wasm_runtime_get_module_inst(env);
    auto* self = static_cast<Instance*>(wasm_runtime_get_custom_data(inst));
    if (self != nullptr) {
        self->log(std::string_view(text, len));
    }
}

NativeSymbol paglets_natives[] = {
    {"log", reinterpret_cast<void*>(native_log), "(*~)", nullptr},
};

std::string error_text(const char* buf) {
    return std::string(buf[0] ? buf : "unknown error");
}

}  // namespace

void ensure_runtime() {
    static std::once_flag once;
    std::call_once(once, [] {
        RuntimeInitArgs args;
        std::memset(&args, 0, sizeof args);
        args.mem_alloc_type = Alloc_With_System_Allocator;
        if (!wasm_runtime_full_init(&args)) {
            std::cerr << "paglets: WAMR runtime initialization failed\n";
            std::abort();
        }
        wasm_runtime_set_log_level(WASM_LOG_LEVEL_ERROR);
        if (!wasm_runtime_register_natives("paglets", paglets_natives,
                                           sizeof paglets_natives / sizeof paglets_natives[0])) {
            std::cerr << "paglets: registering host functions failed\n";
            std::abort();
        }
    });
}

void init_thread() {
    ensure_runtime();
    if (!wasm_runtime_thread_env_inited()) {
        wasm_runtime_init_thread_env();
    }
}

// ---------------------------------------------------------------------------
// Module

std::expected<std::shared_ptr<Module>, std::string> Module::load(std::vector<std::uint8_t> bytes,
                                                                 const ImportPolicy& policy) {
    ensure_runtime();
    auto info = parse_module(bytes);
    if (!info) {
        return std::unexpected("invalid module: " + info.error());
    }
    if (auto ok = policy.check(*info); !ok) {
        return std::unexpected(ok.error());
    }

    std::shared_ptr<Module> m(new Module());
    m->hash_ = sha256(bytes);
    m->info_ = std::move(*info);
    m->bytes_ = std::move(bytes);

    char error[256] = {};
    m->module_ = wasm_runtime_load(m->bytes_.data(), static_cast<std::uint32_t>(m->bytes_.size()), error, sizeof error);
    if (m->module_ == nullptr) {
        return std::unexpected("WAMR load failed: " + error_text(error));
    }
    // No arguments, no environment, no preopened directories: file access
    // is granted later through capabilities, never ambiently.
    wasm_runtime_set_wasi_args(m->module_, nullptr, 0, nullptr, 0, nullptr, 0, nullptr, 0);
    return m;
}

Module::~Module() {
    if (module_ != nullptr) {
        wasm_runtime_unload(module_);
    }
}

// ---------------------------------------------------------------------------
// Instance

std::expected<std::unique_ptr<Instance>, std::string> Instance::create(std::shared_ptr<Module> module,
                                                                       const Limits& limits, bool run_initializer) {
    std::unique_ptr<Instance> self(new Instance());
    self->module_ = std::move(module);
    self->limits_ = limits;

    InstantiationArgs args{};
    args.default_stack_size = limits.stack_size;
    args.host_managed_heap_size = 0;
    args.max_memory_pages = limits.max_memory_pages;

    char error[256] = {};
    self->inst_ = wasm_runtime_instantiate_ex(self->module_->handle(), &args, error, sizeof error);
    if (self->inst_ == nullptr) {
        return std::unexpected("instantiation failed: " + error_text(error));
    }
    wasm_runtime_set_custom_data(self->inst_, self.get());

    self->exec_env_ = wasm_runtime_create_exec_env(self->inst_, limits.stack_size);
    if (self->exec_env_ == nullptr) {
        return std::unexpected("creating execution environment failed");
    }

    // WAMR itself runs _initialize during instantiation for modules that
    // import WASI (a second call traps in wasi-libc). Only modules without
    // WASI imports need the call from here. For memory-image restore the
    // initializer's effects are overwritten by the image anyway.
    const auto& imports = self->module_->info().imports;
    const bool wamr_ran_initializer =
        std::ranges::any_of(imports, [](const Import& i) { return i.module == "wasi_snapshot_preview1"; });
    if (run_initializer && !wamr_ran_initializer && self->module_->info().find_export("_initialize") != nullptr) {
        if (auto r = self->call_void("_initialize"); !r) {
            return std::unexpected("_initialize failed: " + r.error());
        }
    }
    return self;
}

Instance::~Instance() {
    if (exec_env_ != nullptr) {
        wasm_runtime_destroy_exec_env(exec_env_);
    }
    if (inst_ != nullptr) {
        wasm_runtime_deinstantiate(inst_);
    }
}

std::expected<std::uint64_t, std::string> Instance::call(std::string_view name, std::span<const std::uint32_t> args,
                                                         unsigned result_cells) {
    wasm_function_inst_t fn = nullptr;
    if (auto it = functions_.find(name); it != functions_.end()) {
        fn = it->second;
    } else {
        const std::string fname(name);
        fn = wasm_runtime_lookup_function(inst_, fname.c_str());
        if (fn == nullptr) {
            return std::unexpected("export not found: " + fname);
        }
        functions_.emplace(fname, fn);
    }
    std::array<std::uint32_t, 16> argv{};
    if (args.size() > argv.size()) {
        return std::unexpected("too many arguments");
    }
    std::copy(args.begin(), args.end(), argv.begin());

    wasm_runtime_clear_exception(inst_);
    if (!wasm_runtime_call_wasm(exec_env_, fn, static_cast<std::uint32_t>(args.size()), argv.data())) {
        const char* ex = wasm_runtime_get_exception(inst_);
        std::string message = ex != nullptr ? ex : "unknown trap";
        wasm_runtime_clear_exception(inst_);
        return std::unexpected(message);
    }
    switch (result_cells) {
        case 0: return 0;
        case 1: return argv[0];
        default: return std::uint64_t{argv[0]} | (std::uint64_t{argv[1]} << 32);
    }
}

std::expected<std::int32_t, std::string> Instance::call_i32(std::string_view name,
                                                            std::initializer_list<std::uint32_t> args) {
    auto r = call(name, std::span(args.begin(), args.size()), 1);
    if (!r) return std::unexpected(r.error());
    return static_cast<std::int32_t>(static_cast<std::uint32_t>(*r));
}

std::expected<void, std::string> Instance::call_void(std::string_view name, std::initializer_list<std::uint32_t> args) {
    auto r = call(name, std::span(args.begin(), args.size()), 0);
    if (!r) return std::unexpected(r.error());
    return {};
}

std::expected<std::vector<std::uint8_t>, std::string> Instance::send(std::span<const std::uint8_t> request) {
    const auto len = static_cast<std::uint32_t>(request.size());
    auto ptr = call_i32("paglets_alloc", {len});
    if (!ptr) return std::unexpected(ptr.error());
    if (*ptr == 0 && len > 0) return std::unexpected("guest allocation failed");
    const auto guest_ptr = static_cast<std::uint32_t>(*ptr);

    auto mem = memory();
    if (std::uint64_t{guest_ptr} + len > mem.size()) return std::unexpected("guest returned invalid buffer");
    std::memcpy(mem.data() + guest_ptr, request.data(), len);

    const std::array<std::uint32_t, 2> args{guest_ptr, len};
    auto packed = call("paglets_handle", args, 2);
    if (!packed) return std::unexpected(packed.error());
    if (auto f = call_void("paglets_free", {guest_ptr}); !f) return std::unexpected(f.error());

    const auto reply_ptr = static_cast<std::uint32_t>(*packed >> 32);
    const auto reply_len = static_cast<std::uint32_t>(*packed & 0xffffffffu);
    mem = memory();  // the handler may have grown memory
    if (std::uint64_t{reply_ptr} + reply_len > mem.size()) return std::unexpected("guest returned invalid reply");
    return std::vector<std::uint8_t>(mem.begin() + reply_ptr, mem.begin() + reply_ptr + reply_len);
}

void Instance::terminate() {
    wasm_runtime_terminate(inst_);
}

std::uint32_t Instance::page_count() const {
    wasm_memory_inst_t mem = wasm_runtime_get_default_memory(inst_);
    return mem != nullptr ? static_cast<std::uint32_t>(wasm_memory_get_cur_page_count(mem)) : 0;
}

std::span<std::uint8_t> Instance::memory() {
    wasm_memory_inst_t mem = wasm_runtime_get_default_memory(inst_);
    if (mem == nullptr) return {};
    auto* base = static_cast<std::uint8_t*>(wasm_memory_get_base_address(mem));
    const std::uint64_t size = wasm_memory_get_cur_page_count(mem) * wasm_memory_get_bytes_per_page(mem);
    return {base, static_cast<std::size_t>(size)};
}

bool Instance::grow_to(std::uint32_t pages) {
    wasm_memory_inst_t mem = wasm_runtime_get_default_memory(inst_);
    if (mem == nullptr) return pages == 0;
    const auto current = wasm_memory_get_cur_page_count(mem);
    if (pages <= current) return pages == current;
    return wasm_memory_enlarge(mem, pages - current);
}

std::expected<std::uint64_t, std::string> Instance::global_bits(std::string_view name) const {
    wasm_global_inst_t g{};
    const std::string n(name);
    if (!wasm_runtime_get_export_global_inst(inst_, n.c_str(), &g)) {
        return std::unexpected("exported global not found: " + n);
    }
    std::uint64_t bits = 0;
    const std::size_t width = (g.kind == WASM_I64 || g.kind == WASM_F64) ? 8 : 4;
    std::memcpy(&bits, g.global_data, width);
    return bits;
}

std::expected<void, std::string> Instance::set_global_bits(std::string_view name, std::uint64_t bits) {
    wasm_global_inst_t g{};
    const std::string n(name);
    if (!wasm_runtime_get_export_global_inst(inst_, n.c_str(), &g)) {
        return std::unexpected("exported global not found: " + n);
    }
    const std::size_t width = (g.kind == WASM_I64 || g.kind == WASM_F64) ? 8 : 4;
    std::memcpy(g.global_data, &bits, width);
    return {};
}

void Instance::log(std::string_view text) const {
    if (log_sink_) {
        log_sink_(text);
    }
}

// ---------------------------------------------------------------------------

std::expected<std::vector<std::uint8_t>, std::string> read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return std::unexpected("cannot open " + path);
    return std::vector<std::uint8_t>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

std::expected<void, std::string> write_file(const std::string& path, std::span<const std::uint8_t> data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) return std::unexpected("cannot write " + path);
    out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    if (!out) return std::unexpected("write failed: " + path);
    return {};
}

}  // namespace paglets::wasm
