// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/wasm/engine.hpp>

#include <paglets/abi.hpp>

#include <wasm_export.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <random>

// Not in wasm_export.h: copies the exception under the instance's exception
// lock (wasm_runtime_get_exception reads it without the lock, which races with
// wasm_runtime_terminate from another thread). The buffer holds 128 bytes.
extern "C" bool wasm_runtime_copy_exception(WASMModuleInstanceCommon* module_inst, char* exception_buf);

namespace paglets::wasm {

namespace {
constexpr std::size_t exception_buffer_size = 128;  // EXCEPTION_BUF_LEN
}  // namespace

namespace {

// Every native receives validated pointers: WAMR checks each "*~" pair
// (pointer, length) against the instance memory before the call and traps
// the guest otherwise.

Instance* instance_of(wasm_exec_env_t env) {
    return static_cast<Instance*>(wasm_runtime_get_custom_data(wasm_runtime_get_module_inst(env)));
}

HostImports* imports_of(wasm_exec_env_t env) {
    Instance* self = instance_of(env);
    return self != nullptr ? self->imports() : nullptr;
}

std::span<const std::uint8_t> bytes_of(const void* ptr, std::uint32_t len) {
    return {static_cast<const std::uint8_t*>(ptr), len};
}

bool document_too_large(std::uint32_t len) {
    return len > abi::max_document_size;
}

// Writes a document result into (out, cap) following the ABI's buffer
// protocol: the result is the document length; nothing is written if it
// does not fit.
std::int32_t write_document(const HostImports::Document& doc, void* out, std::uint32_t cap) {
    if (!doc) return doc.error();
    if (doc->size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) return abi::too_large;
    if (doc->size() <= cap && !doc->empty()) std::memcpy(out, doc->data(), doc->size());
    return static_cast<std::int32_t>(doc->size());
}

void native_log(wasm_exec_env_t env, std::int32_t level, const char* text, std::uint32_t len) {
    if (HostImports* h = imports_of(env)) {
        h->log(level, std::string_view(text, len));
    } else if (Instance* self = instance_of(env)) {
        self->log(std::string_view(text, len));
    }
}

std::int32_t native_self_info(wasm_exec_env_t env, void* out, std::uint32_t cap) {
    HostImports* h = imports_of(env);
    return h != nullptr ? write_document(h->self_info(), out, cap) : abi::unsupported;
}

std::int32_t native_send(wasm_exec_env_t env, std::int32_t endpoint, const void* msg, std::uint32_t len) {
    HostImports* h = imports_of(env);
    if (h == nullptr) return abi::unsupported;
    if (document_too_large(len)) return abi::too_large;
    return h->send(endpoint, bytes_of(msg, len));
}

std::int64_t native_request(wasm_exec_env_t env, std::int32_t endpoint, const void* msg, std::uint32_t len) {
    HostImports* h = imports_of(env);
    if (h == nullptr) return abi::unsupported;
    if (document_too_large(len)) return abi::too_large;
    return h->request(endpoint, bytes_of(msg, len));
}

std::int32_t native_reply(wasm_exec_env_t env, std::int32_t reply, const void* msg, std::uint32_t len) {
    HostImports* h = imports_of(env);
    if (h == nullptr) return abi::unsupported;
    if (document_too_large(len)) return abi::too_large;
    return h->reply(reply, bytes_of(msg, len));
}

std::int32_t native_cap_derive(wasm_exec_env_t env, std::int32_t handle, const void* spec, std::uint32_t len) {
    HostImports* h = imports_of(env);
    if (h == nullptr) return abi::unsupported;
    if (document_too_large(len)) return abi::too_large;
    return h->cap_derive(handle, bytes_of(spec, len));
}

std::int32_t native_cap_drop(wasm_exec_env_t env, std::int32_t handle) {
    HostImports* h = imports_of(env);
    return h != nullptr ? h->cap_drop(handle) : abi::unsupported;
}

std::int32_t native_cap_inspect(wasm_exec_env_t env, std::int32_t handle, void* out, std::uint32_t cap) {
    HostImports* h = imports_of(env);
    return h != nullptr ? write_document(h->cap_inspect(handle), out, cap) : abi::unsupported;
}

std::int32_t native_cap_list(wasm_exec_env_t env, void* out, std::uint32_t cap) {
    HostImports* h = imports_of(env);
    return h != nullptr ? write_document(h->cap_list(), out, cap) : abi::unsupported;
}

std::int32_t native_create_child(wasm_exec_env_t env, const void* spec, std::uint32_t len) {
    HostImports* h = imports_of(env);
    if (h == nullptr) return abi::unsupported;
    if (document_too_large(len)) return abi::too_large;
    return h->create_child(bytes_of(spec, len));
}

std::int32_t native_lifecycle(wasm_exec_env_t env, std::int32_t op, const void* arg, std::uint32_t len) {
    HostImports* h = imports_of(env);
    if (h == nullptr) return abi::unsupported;
    if (document_too_large(len)) return abi::too_large;
    return h->lifecycle(op, bytes_of(arg, len));
}

std::int32_t native_timer_set(wasm_exec_env_t env, std::int64_t delay_ms, const void* msg, std::uint32_t len) {
    HostImports* h = imports_of(env);
    if (h == nullptr) return abi::unsupported;
    if (document_too_large(len)) return abi::too_large;
    return h->timer_set(delay_ms, bytes_of(msg, len));
}

std::int64_t native_now(wasm_exec_env_t, std::int32_t clock) {
    using namespace std::chrono;
    if (clock == static_cast<std::int32_t>(abi::Clock::wall)) {
        return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
    }
    if (clock == static_cast<std::int32_t>(abi::Clock::monotonic)) {
        return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    }
    return abi::invalid_argument;
}

std::int32_t native_random(wasm_exec_env_t, void* out, std::uint32_t len) {
    fill_random(std::span<std::uint8_t>(static_cast<std::uint8_t*>(out), len));
    return abi::ok;
}

const NativeSymbol paglets_natives_template[] = {
    {"log", reinterpret_cast<void*>(native_log), "(i*~)", nullptr},
    {"self_info", reinterpret_cast<void*>(native_self_info), "(*~)i", nullptr},
    {"send", reinterpret_cast<void*>(native_send), "(i*~)i", nullptr},
    {"request", reinterpret_cast<void*>(native_request), "(i*~)I", nullptr},
    {"reply", reinterpret_cast<void*>(native_reply), "(i*~)i", nullptr},
    {"cap_derive", reinterpret_cast<void*>(native_cap_derive), "(i*~)i", nullptr},
    {"cap_drop", reinterpret_cast<void*>(native_cap_drop), "(i)i", nullptr},
    {"cap_inspect", reinterpret_cast<void*>(native_cap_inspect), "(i*~)i", nullptr},
    {"cap_list", reinterpret_cast<void*>(native_cap_list), "(*~)i", nullptr},
    {"create_child", reinterpret_cast<void*>(native_create_child), "(*~)i", nullptr},
    {"lifecycle", reinterpret_cast<void*>(native_lifecycle), "(i*~)i", nullptr},
    {"timer_set", reinterpret_cast<void*>(native_timer_set), "(I*~)i", nullptr},
    {"now", reinterpret_cast<void*>(native_now), "(i)I", nullptr},
    {"random", reinterpret_cast<void*>(native_random), "(*~)i", nullptr},
};
static_assert(std::size(paglets_natives_template) == std::size(abi::import_names),
              "every import of abi::import_names needs a native");

// WAMR sorts the array in place when registering it.
NativeSymbol paglets_natives[std::size(paglets_natives_template)];

// WASI fd_write for stdout and stderr, taking precedence over WAMR's libc-wasi
// (natives registered later are found first). No file descriptors are open
// besides the standard ones, so other descriptors are EBADF.
constexpr std::int32_t wasi_ebadf = 8;
constexpr std::int32_t wasi_efault = 21;

std::int32_t native_fd_write(wasm_exec_env_t env, std::int32_t fd, std::uint32_t iovs, std::uint32_t iovs_len,
                             std::uint32_t nwritten_ptr) {
    wasm_module_inst_t inst = wasm_runtime_get_module_inst(env);
    Instance* self = instance_of(env);
    if (fd != 1 && fd != 2) return wasi_ebadf;
    const std::uint64_t table = std::uint64_t{iovs_len} * 8;
    if (table > std::numeric_limits<std::uint32_t>::max() || !wasm_runtime_validate_app_addr(inst, iovs, table) ||
        !wasm_runtime_validate_app_addr(inst, nwritten_ptr, 4)) {
        return wasi_efault;
    }
    const auto* vec = static_cast<const std::uint8_t*>(wasm_runtime_addr_app_to_native(inst, iovs));
    std::uint32_t total = 0;
    for (std::uint32_t i = 0; i < iovs_len; ++i) {
        std::uint32_t buf = 0;
        std::uint32_t len = 0;
        std::memcpy(&buf, vec + 8 * i, 4);
        std::memcpy(&len, vec + 8 * i + 4, 4);
        if (len == 0) continue;
        if (!wasm_runtime_validate_app_addr(inst, buf, len)) return wasi_efault;
        const auto* data = static_cast<const char*>(wasm_runtime_addr_app_to_native(inst, buf));
        if (self != nullptr) self->write_output(fd, std::string_view(data, len));
        total += len;
    }
    std::memcpy(wasm_runtime_addr_app_to_native(inst, nwritten_ptr), &total, 4);
    return 0;
}

NativeSymbol wasi_natives[] = {
    {"fd_write", reinterpret_cast<void*>(native_fd_write), "(iiii)i", nullptr},
};

std::string error_text(const char* buf) {
    return std::string(buf[0] ? buf : "unknown error");
}

}  // namespace

void fill_random(std::span<std::uint8_t> out) {
    static thread_local std::random_device device;
    std::size_t i = 0;
    while (i < out.size()) {
        const auto word = device();
        for (std::size_t b = 0; b < sizeof word && i < out.size(); ++b, ++i) {
            out[i] = static_cast<std::uint8_t>(word >> (8 * b));
        }
    }
}

// ---------------------------------------------------------------------------
// HostImports defaults

void HostImports::log(std::int32_t, std::string_view text) {
    if (instance != nullptr) instance->log(text);
}
HostImports::Document HostImports::self_info() {
    return std::unexpected(abi::unsupported);
}
std::int32_t HostImports::send(std::int32_t, std::span<const std::uint8_t>) {
    return abi::unsupported;
}
std::int64_t HostImports::request(std::int32_t, std::span<const std::uint8_t>) {
    return abi::unsupported;
}
std::int32_t HostImports::reply(std::int32_t, std::span<const std::uint8_t>) {
    return abi::unsupported;
}
std::int32_t HostImports::cap_derive(std::int32_t, std::span<const std::uint8_t>) {
    return abi::unsupported;
}
std::int32_t HostImports::cap_drop(std::int32_t) {
    return abi::unsupported;
}
HostImports::Document HostImports::cap_inspect(std::int32_t) {
    return std::unexpected(abi::unsupported);
}
HostImports::Document HostImports::cap_list() {
    return std::unexpected(abi::unsupported);
}
std::int32_t HostImports::create_child(std::span<const std::uint8_t>) {
    return abi::unsupported;
}
std::int32_t HostImports::lifecycle(std::int32_t, std::span<const std::uint8_t>) {
    return abi::unsupported;
}
std::int32_t HostImports::timer_set(std::int64_t, std::span<const std::uint8_t>) {
    return abi::unsupported;
}

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
        std::copy(std::begin(paglets_natives_template), std::end(paglets_natives_template), paglets_natives);
        for (std::size_t i = 0; i < std::size(paglets_natives); ++i) {
            if (paglets_natives[i].symbol != abi::import_names[i]) {
                std::cerr << "paglets: native table does not match abi::import_names\n";
                std::abort();
            }
        }
        if (!wasm_runtime_register_natives("paglets", paglets_natives,
                                           sizeof paglets_natives / sizeof paglets_natives[0])) {
            std::cerr << "paglets: registering host functions failed\n";
            std::abort();
        }
        if (!wasm_runtime_register_natives("wasi_snapshot_preview1", wasi_natives, std::size(wasi_natives))) {
            std::cerr << "paglets: registering WASI output functions failed\n";
            std::abort();
        }
    });
}

namespace {
// wasm_runtime_thread_env_inited() only looks at the signal handler state in
// AOT builds, so the per-thread initialization is tracked here.
thread_local bool thread_env_ready = false;
}  // namespace

void init_thread() {
    ensure_runtime();
    if (!thread_env_ready) {
        if (!wasm_runtime_init_thread_env()) {
            std::cerr << "paglets: WAMR thread environment initialization failed\n";
            std::abort();
        }
        thread_env_ready = true;
    }
}

void deinit_thread() {
    if (thread_env_ready) {
        wasm_runtime_destroy_thread_env();
        thread_env_ready = false;
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
    m->original_ = bytes;
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
    self->exec_thread_ = std::this_thread::get_id();

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

    // An execution environment records the native stack of the thread that
    // first runs it; a paglet moved to another scheduler thread gets a new one.
    if (exec_thread_ != std::this_thread::get_id()) {
        wasm_runtime_destroy_exec_env(exec_env_);
        exec_env_ = wasm_runtime_create_exec_env(inst_, limits_.stack_size);
        if (exec_env_ == nullptr) return std::unexpected("creating execution environment failed");
        exec_thread_ = std::this_thread::get_id();
    }
    // Clearing searches every cluster of the thread manager; only do it
    // when there is something to clear.
    if (wasm_runtime_copy_exception(inst_, nullptr)) wasm_runtime_clear_exception(inst_);
    if (!wasm_runtime_call_wasm(exec_env_, fn, static_cast<std::uint32_t>(args.size()), argv.data())) {
        std::array<char, exception_buffer_size> ex{};
        std::string message = wasm_runtime_copy_exception(inst_, ex.data()) ? std::string(ex.data()) : "unknown trap";
        wasm_runtime_clear_exception(inst_);
        flush_output();
        return std::unexpected(message);
    }
    flush_output();
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

std::expected<std::int32_t, std::string> Instance::call_with_data(std::string_view name,
                                                                  std::span<const std::uint32_t> leading,
                                                                  std::span<const std::uint8_t> data) {
    const auto len = static_cast<std::uint32_t>(data.size());
    std::uint32_t guest_ptr = 0;
    if (len > 0) {
        auto ptr = call_i32("paglets_alloc", {len});
        if (!ptr) return std::unexpected(ptr.error());
        if (*ptr == 0) return std::unexpected("guest allocation failed");
        guest_ptr = static_cast<std::uint32_t>(*ptr);
        auto mem = memory();
        if (std::uint64_t{guest_ptr} + len > mem.size()) return std::unexpected("guest returned invalid buffer");
        std::memcpy(mem.data() + guest_ptr, data.data(), len);
    }
    std::array<std::uint32_t, 16> args{};
    if (leading.size() + 2 > args.size()) return std::unexpected("too many arguments");
    std::copy(leading.begin(), leading.end(), args.begin());
    args[leading.size()] = guest_ptr;
    args[leading.size() + 1] = len;
    auto result = call(name, std::span(args.data(), leading.size() + 2), 1);
    if (!result) return std::unexpected(result.error());
    if (len > 0) {
        if (auto f = call_void("paglets_free", {guest_ptr}); !f) return std::unexpected(f.error());
    }
    return static_cast<std::int32_t>(static_cast<std::uint32_t>(*result));
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

void Instance::write_output(int fd, std::string_view bytes) {
    constexpr std::size_t max_line = 4096;
    std::string& pending = output_[fd == 2 ? 1 : 0];
    for (char c : bytes) {
        if (c == '\n' || pending.size() >= max_line) {
            const std::string line = std::move(pending);
            pending.clear();
            const auto level = fd == 2 ? abi::LogLevel::warning : abi::LogLevel::info;
            if (imports_ != nullptr) {
                imports_->log(static_cast<std::int32_t>(level), line);
            } else {
                log(line);
            }
            if (c == '\n') continue;
        }
        pending.push_back(c);
    }
}

void Instance::flush_output() {
    for (int fd : {1, 2}) {
        if (!output_[fd - 1].empty()) write_output(fd, "\n");
    }
}

void Instance::set_imports(HostImports* imports) {
    imports_ = imports;
    if (imports_ != nullptr) imports_->instance = this;
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
