// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/wasm/binary.hpp>

#include <algorithm>
#include <cstring>

namespace paglets::wasm {

std::string_view to_string(ExternKind kind) {
    switch (kind) {
        case ExternKind::function: return "func";
        case ExternKind::table: return "table";
        case ExternKind::memory: return "memory";
        case ExternKind::global: return "global";
        case ExternKind::tag: return "tag";
    }
    return "?";
}

std::string_view to_string(ValType type) {
    switch (type) {
        case ValType::i32: return "i32";
        case ValType::i64: return "i64";
        case ValType::f32: return "f32";
        case ValType::f64: return "f64";
        case ValType::v128: return "v128";
        case ValType::funcref: return "funcref";
        case ValType::externref: return "externref";
    }
    return "?";
}

const Export* ModuleInfo::find_export(std::string_view name) const {
    for (const auto& e : exports) {
        if (e.name == name) {
            return &e;
        }
    }
    return nullptr;
}

namespace {

class Cursor {
public:
    Cursor(std::span<const std::uint8_t> data, std::string& error) : data_(data), error_(error) {}

    bool at_end() const { return pos_ >= data_.size(); }
    std::size_t pos() const { return pos_; }
    void seek(std::size_t p) { pos_ = p; }

    bool fail(std::string message) {
        if (error_.empty()) {
            error_ = std::move(message) + " at offset " + std::to_string(pos_);
        }
        return false;
    }

    bool byte(std::uint8_t& out) {
        if (at_end()) return fail("unexpected end of module");
        out = data_[pos_++];
        return true;
    }

    bool uleb(std::uint64_t& out, unsigned max_bits = 64) {
        out = 0;
        unsigned shift = 0;
        while (true) {
            std::uint8_t b = 0;
            if (!byte(b)) return false;
            if (shift >= max_bits) return fail("LEB128 value too long");
            out |= std::uint64_t{b & 0x7fu} << shift;
            if ((b & 0x80) == 0) return true;
            shift += 7;
        }
    }

    bool u32(std::uint32_t& out) {
        std::uint64_t v = 0;
        if (!uleb(v, 35) || v > 0xffffffffu) return fail("invalid u32");
        out = static_cast<std::uint32_t>(v);
        return true;
    }

    bool sleb_skip() {
        while (true) {
            std::uint8_t b = 0;
            if (!byte(b)) return false;
            if ((b & 0x80) == 0) return true;
        }
    }

    bool skip(std::size_t n) {
        if (pos_ + n > data_.size()) return fail("unexpected end of module");
        pos_ += n;
        return true;
    }

    bool name(std::string& out) {
        std::uint32_t n = 0;
        if (!u32(n) || pos_ + n > data_.size()) return fail("invalid name");
        out.assign(reinterpret_cast<const char*>(data_.data() + pos_), n);
        pos_ += n;
        return true;
    }

    bool valtype(ValType& out) {
        std::uint8_t b = 0;
        if (!byte(b)) return false;
        switch (b) {
            case 0x7f:
            case 0x7e:
            case 0x7d:
            case 0x7c:
            case 0x7b:
            case 0x70:
            case 0x6f: out = static_cast<ValType>(b); return true;
            default: return fail("unsupported value type 0x" + std::to_string(b));
        }
    }

    bool limits(MemoryLimits& out) {
        std::uint8_t flags = 0;
        if (!byte(flags)) return false;
        out.memory64 = (flags & 0x04) != 0;
        if (!uleb(out.min_pages)) return false;
        if (flags & 0x01) {
            std::uint64_t max = 0;
            if (!uleb(max)) return false;
            out.max_pages = max;
        }
        return true;
    }

    // Constant expression up to and including the final `end` (0x0b).
    bool skip_const_expr() {
        while (true) {
            std::uint8_t op = 0;
            if (!byte(op)) return false;
            switch (op) {
                case 0x0b: return true;  // end
                case 0x41:               // i32.const
                case 0x42:
                    if (!sleb_skip()) return false;
                    break;  // i64.const
                case 0x43:
                    if (!skip(4)) return false;
                    break;  // f32.const
                case 0x44:
                    if (!skip(8)) return false;
                    break;    // f64.const
                case 0x23:    // global.get
                case 0xd2: {  // ref.func
                    std::uint32_t idx = 0;
                    if (!u32(idx)) return false;
                    break;
                }
                case 0xd0:
                    if (!skip(1)) return false;
                    break;  // ref.null t
                case 0x6a:
                case 0x6b:
                case 0x6c:  // i32 add/sub/mul (extended const)
                case 0x7c:
                case 0x7d:
                case 0x7e: break;  // i64 add/sub/mul
                default: return fail("unsupported opcode in constant expression");
            }
        }
    }

private:
    std::span<const std::uint8_t> data_;
    std::string& error_;
    std::size_t pos_ = 0;
};

}  // namespace

std::expected<ModuleInfo, std::string> parse_module(std::span<const std::uint8_t> bytes) {
    static constexpr std::uint8_t header[] = {0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00};
    if (bytes.size() < sizeof header || std::memcmp(bytes.data(), header, sizeof header) != 0) {
        return std::unexpected("not a Wasm 1.0 binary module");
    }

    std::string error;
    Cursor c(bytes, error);
    c.seek(sizeof header);
    ModuleInfo info;

    while (!c.at_end()) {
        std::uint8_t id = 0;
        std::uint32_t size = 0;
        if (!c.byte(id) || !c.u32(size)) break;
        const std::size_t section_end = c.pos() + size;
        if (section_end > bytes.size()) {
            c.fail("section exceeds module size");
            break;
        }

        if (id == 2) {  // import
            std::uint32_t count = 0;
            if (!c.u32(count)) break;
            for (std::uint32_t i = 0; i < count && error.empty(); ++i) {
                Import imp;
                std::uint8_t kind = 0;
                if (!c.name(imp.module) || !c.name(imp.name) || !c.byte(kind)) break;
                imp.kind = static_cast<ExternKind>(kind);
                switch (kind) {
                    case 0: {
                        std::uint32_t type_index = 0;
                        c.u32(type_index);
                        break;
                    }
                    case 1: {
                        ValType elem{};
                        MemoryLimits lim;
                        if (c.valtype(elem)) c.limits(lim);
                        break;
                    }
                    case 2: {
                        MemoryLimits lim;
                        if (c.limits(lim)) info.memory = lim;
                        break;
                    }
                    case 3: {
                        Global g;
                        std::uint8_t mut = 0;
                        if (c.valtype(g.type) && c.byte(mut)) {
                            g.index = static_cast<std::uint32_t>(info.globals.size());
                            g.is_mutable = mut == 1;
                            g.imported = true;
                            info.globals.push_back(g);
                        }
                        break;
                    }
                    case 4: {
                        std::uint8_t attribute = 0;
                        std::uint32_t type_index = 0;
                        if (c.byte(attribute)) c.u32(type_index);
                        break;
                    }
                    default: c.fail("unknown import kind"); break;
                }
                info.imports.push_back(std::move(imp));
            }
        } else if (id == 5) {  // memory
            std::uint32_t count = 0;
            if (!c.u32(count)) break;
            for (std::uint32_t i = 0; i < count && error.empty(); ++i) {
                MemoryLimits lim;
                if (c.limits(lim) && !info.memory) info.memory = lim;
            }
        } else if (id == 6) {  // global
            std::uint32_t count = 0;
            if (!c.u32(count)) break;
            for (std::uint32_t i = 0; i < count && error.empty(); ++i) {
                Global g;
                std::uint8_t mut = 0;
                if (!c.valtype(g.type) || !c.byte(mut) || !c.skip_const_expr()) break;
                g.index = static_cast<std::uint32_t>(info.globals.size());
                g.is_mutable = mut == 1;
                info.globals.push_back(g);
            }
        } else if (id == 7) {  // export
            std::uint32_t count = 0;
            if (!c.u32(count)) break;
            for (std::uint32_t i = 0; i < count && error.empty(); ++i) {
                Export e;
                std::uint8_t kind = 0;
                if (!c.name(e.name) || !c.byte(kind) || !c.u32(e.index)) break;
                e.kind = static_cast<ExternKind>(kind);
                info.exports.push_back(std::move(e));
            }
        } else if (id == 8) {  // start
            info.has_start_function = true;
        }

        if (!error.empty()) break;
        c.seek(section_end);
    }

    if (!error.empty()) {
        return std::unexpected(error);
    }
    for (const auto& e : info.exports) {
        if (e.kind == ExternKind::global && e.index < info.globals.size()) {
            info.globals[e.index].export_name = e.name;
        }
    }
    return info;
}

ImportPolicy ImportPolicy::standard() {
    return ImportPolicy({
        "paglets.*",
        "wasi_snapshot_preview1.args_get",
        "wasi_snapshot_preview1.args_sizes_get",
        "wasi_snapshot_preview1.environ_get",
        "wasi_snapshot_preview1.environ_sizes_get",
        "wasi_snapshot_preview1.clock_res_get",
        "wasi_snapshot_preview1.clock_time_get",
        "wasi_snapshot_preview1.random_get",
        "wasi_snapshot_preview1.sched_yield",
        "wasi_snapshot_preview1.proc_exit",
        "wasi_snapshot_preview1.fd_write",
        "wasi_snapshot_preview1.fd_close",
        "wasi_snapshot_preview1.fd_seek",
        "wasi_snapshot_preview1.fd_fdstat_get",
        "wasi_snapshot_preview1.fd_prestat_get",
        "wasi_snapshot_preview1.fd_prestat_dir_name",
    });
}

bool ImportPolicy::allows(const Import& import) const {
    return std::ranges::any_of(allowed_, [&](const std::string& rule) {
        const auto dot = rule.find('.');
        if (dot == std::string::npos) return false;
        const std::string_view module(rule.data(), dot);
        const std::string_view name(rule.data() + dot + 1, rule.size() - dot - 1);
        return module == import.module && (name == "*" || name == import.name);
    });
}

std::expected<void, std::string> ImportPolicy::check(const ModuleInfo& info) const {
    std::string rejected;
    for (const auto& imp : info.imports) {
        if (!allows(imp)) {
            if (!rejected.empty()) rejected += ", ";
            rejected += imp.module + "." + imp.name + " (" + std::string(to_string(imp.kind)) + ")";
        }
    }
    if (!rejected.empty()) {
        return std::unexpected("imports not allowed for this trust class: " + rejected);
    }
    return {};
}

std::expected<void, std::string> check_snapshot_ready(const ModuleInfo& info) {
    std::string missing;
    for (const auto& g : info.globals) {
        if (g.is_mutable && !g.imported && !g.export_name) {
            if (!missing.empty()) missing += ", ";
            missing += "#" + std::to_string(g.index);
        }
    }
    if (!missing.empty()) {
        return std::unexpected("mutable globals must be exported for memory images; not exported: " + missing +
                               " (link guests with -Wl,--export=__stack_pointer)");
    }
    return {};
}

}  // namespace paglets::wasm
