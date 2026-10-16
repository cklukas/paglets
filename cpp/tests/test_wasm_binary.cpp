// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <paglets/runtime/modules.hpp>
#include <paglets/wasm/binary.hpp>
#include <paglets/wasm/engine.hpp>

#include <stdexcept>
#include <tuple>

namespace pw = paglets::wasm;

namespace {
using WasmBytes = std::vector<std::uint8_t>;

// Complete core Wasm modules, differing only in the kind of paglets.send.
// Defined functions have the required ABI signatures; no guest is executed.
WasmBytes import_kind_module(pw::ExternKind kind) {
    auto integer = [](WasmBytes& out, std::uint32_t value) {
        while (value >= 128) { out.push_back(static_cast<std::uint8_t>((value & 127) | 128)); value >>= 7; }
        out.push_back(static_cast<std::uint8_t>(value));
    };
    auto name = [&](WasmBytes& out, std::string_view value) {
        integer(out, static_cast<std::uint32_t>(value.size()));
        out.insert(out.end(), value.begin(), value.end());
    };
    WasmBytes module{0, 0x61, 0x73, 0x6d, 1, 0, 0, 0};
    auto section = [&](std::uint8_t id, const WasmBytes& data) {
        module.push_back(id); integer(module, static_cast<std::uint32_t>(data.size()));
        module.insert(module.end(), data.begin(), data.end());
    };
    section(1, {5, 0x60, 0, 0, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 1, 0x7f, 0,
                0x60, 3, 0x7f, 0x7f, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7f});
    WasmBytes imports{1};
    name(imports, "paglets"); name(imports, "send"); imports.push_back(static_cast<std::uint8_t>(kind));
    switch (kind) {
        case pw::ExternKind::function: imports.push_back(3); break;
        case pw::ExternKind::table: imports.insert(imports.end(), {0x70, 0, 1}); break;
        case pw::ExternKind::memory: imports.insert(imports.end(), {1, 1, 1}); break;
        case pw::ExternKind::global: imports.insert(imports.end(), {0x7f, 0}); break;
        default: throw std::runtime_error("unsupported test import kind");
    }
    section(2, imports);
    section(3, {5, 0, 1, 2, 3, 4});
    if (kind != pw::ExternKind::memory) section(5, {1, 1, 1, 1});
    WasmBytes exports{6};
    std::uint32_t index = kind == pw::ExternKind::function ? 1 : 0;
    for (const auto fn : {"paglets_abi_v1", "paglets_alloc", "paglets_free", "paglets_on_event", "paglets_on_message"}) {
        name(exports, fn); exports.push_back(0); integer(exports, index++);
    }
    name(exports, "memory"); exports.push_back(2); integer(exports, 0);
    section(7, exports);
    WasmBytes code{5};
    auto body = [&](std::initializer_list<std::uint8_t> bytes) {
        integer(code, static_cast<std::uint32_t>(bytes.size())); code.insert(code.end(), bytes);
    };
    body({0, 0x0b}); body({0, 0x41, 0, 0x0b}); body({0, 0x0b});
    body({0, 0x41, 0, 0x0b}); body({0, 0x41, 0, 0x0b});
    section(10, code);
    return module;
}
}  // namespace

PAGLETS_TEST("wasm imports: policies match function kinds as well as names") {
    for (const auto& [policy, module, fn] : std::vector<std::tuple<pw::ImportPolicy, std::string, std::string>>{
        {pw::ImportPolicy::standard(), "paglets", "send"},
        {pw::ImportPolicy::system(), "paglets_sys", "call"},
        {pw::ImportPolicy({"custom.*"}), "custom", "call"}}) {
        CHECK(policy.allows({module, fn, pw::ExternKind::function}));
        for (const auto kind : {pw::ExternKind::table, pw::ExternKind::memory, pw::ExternKind::global,
                               pw::ExternKind::tag, static_cast<pw::ExternKind>(99)}) {
            pw::ModuleInfo info;
            info.imports.push_back({module, fn, kind});
            CHECK(!policy.allows(info.imports.front()));
            const auto checked = policy.check(info);
            CHECK(!checked.has_value());
            if (!checked) CHECK(checked.error().find(module + "." + fn) != std::string::npos);
        }
    }
}

PAGLETS_TEST("wasm imports: module store refuses allowed-name non-function imports") {
    paglets::runtime::ModuleStore store(paglets::runtime::ModuleStoreConfig{});
    for (const auto kind : {pw::ExternKind::function, pw::ExternKind::table, pw::ExternKind::memory, pw::ExternKind::global}) {
        auto bytes = import_kind_module(kind);
        auto parsed = pw::parse_module(bytes);
        REQUIRE_OK(parsed);
        REQUIRE(parsed->imports.size() == 1u);
        CHECK(parsed->imports.front().kind == kind);
        CHECK(pw::check_snapshot_ready(*parsed).has_value());
        const auto admitted = store.add(std::move(bytes));
        if (kind == pw::ExternKind::function) {
            REQUIRE_OK(admitted);
        } else {
            CHECK(!admitted.has_value());
            if (!admitted) {
                CHECK(admitted.error().find("paglets.send") != std::string::npos);
                CHECK(admitted.error().find(pw::to_string(kind)) != std::string::npos);
            }
        }
    }
}

PAGLETS_TEST("wasm binary: snapshot checks decode table instructions without mistaking immediate bytes") {
    auto module = [](std::vector<std::uint8_t> instructions) {
        std::vector<std::uint8_t> bytes{0, 0x61, 0x73, 0x6d, 1, 0, 0, 0,
            1, 4, 1, 0x60, 0, 0, 3, 2, 1, 0, 4, 4, 1, 0x70, 0, 1};
        bytes.insert(bytes.end(), {10, static_cast<std::uint8_t>(instructions.size() + 4), 1,
            static_cast<std::uint8_t>(instructions.size() + 2), 0});
        bytes.insert(bytes.end(), instructions.begin(), instructions.end());
        bytes.push_back(0x0b);
        return bytes;
    };
    // 0x26 inside an integer constant is not table.set.
    auto safe = pw::parse_module(module({0x41, 0x26, 0x1a}));
    REQUIRE_OK(safe);
    CHECK(pw::check_snapshot_ready(*safe).has_value());
    for (const auto op : std::vector<std::vector<std::uint8_t>>{
        {0x26, 0}, {0xfc, 12, 0, 0}, {0xfc, 13, 0}, {0xfc, 14, 0, 0}, {0xfc, 15, 0}, {0xfc, 17, 0}}) {
        auto parsed = pw::parse_module(module(op));
        REQUIRE_OK(parsed);
        CHECK(parsed->mutable_tables);
        CHECK(!pw::check_snapshot_ready(*parsed));
    }
    auto unknown = pw::parse_module(module({0xfb, 0}));
    REQUIRE_OK(unknown);
    CHECK(!pw::check_snapshot_ready(*unknown));
    auto truncated = pw::parse_module(module({0x43}));
    REQUIRE_OK(truncated);
    CHECK(!pw::check_snapshot_ready(*truncated));
}

PAGLETS_TEST("wasm binary: rejects non-modules") {
    const std::vector<std::uint8_t> junk = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    CHECK(!pw::parse_module(junk).has_value());
}

PAGLETS_TEST("wasm binary: minimal hand-written module") {
    // (module (global $g (mut i32) (i32.const 7)) (global i64 (i64.const 1)) (export "g" (global 0)))
    const std::vector<std::uint8_t> bytes = {
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,  // header
        0x06, 0x0b, 0x02, 0x7f, 0x01, 0x41, 0x07, 0x0b,  // global section
        0x7e, 0x00, 0x42, 0x01, 0x0b,                    //
        0x07, 0x05, 0x01, 0x01, 'g',  0x03, 0x00,        // export section
    };
    auto info = pw::parse_module(bytes);
    REQUIRE_OK(info);
    REQUIRE(info->globals.size() == 2);
    CHECK(info->globals[0].is_mutable);
    CHECK(info->globals[0].export_name == std::optional<std::string>("g"));
    CHECK(!info->globals[1].is_mutable);
    CHECK(pw::check_snapshot_ready(*info).has_value());
}

PAGLETS_TEST("wasm binary: counter guest is memory-image ready") {
    const auto path = paglets::test::guest_path("counter.wasm");
    if (path.empty()) paglets::test::skip("counter.wasm not available");
    auto bytes = pw::read_file(path);
    REQUIRE_OK(bytes);
    auto info = pw::parse_module(*bytes);
    REQUIRE_OK(info);
    CHECK(info->find_export("paglets_on_message") != nullptr);
    CHECK(info->find_export("paglets_abi_v1") != nullptr);
    CHECK(info->find_export("_initialize") != nullptr);
    CHECK(pw::ImportPolicy::standard().check(*info).has_value());
    CHECK(pw::check_snapshot_ready(*info).has_value());
    int mutable_globals = 0;
    for (const auto& g : info->globals) mutable_globals += g.is_mutable ? 1 : 0;
    CHECK_EQ(mutable_globals, 1);  // __stack_pointer
}

PAGLETS_TEST("wasm binary: import policy names every forbidden import") {
    const auto path = paglets::test::guest_path("forbidden.wasm");
    if (path.empty()) paglets::test::skip("forbidden.wasm not available");
    auto bytes = pw::read_file(path);
    REQUIRE_OK(bytes);
    auto info = pw::parse_module(*bytes);
    REQUIRE_OK(info);
    auto check = pw::ImportPolicy::standard().check(*info);
    REQUIRE(!check.has_value());
    CHECK(check.error().find("wasi_snapshot_preview1.sock_accept") != std::string::npos);
    CHECK(check.error().find("env.system") != std::string::npos);
}
