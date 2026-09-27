// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <paglets/wasm/binary.hpp>
#include <paglets/wasm/engine.hpp>

namespace pw = paglets::wasm;

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
    CHECK(info->find_export("paglets_handle") != nullptr);
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
