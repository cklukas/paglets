// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <paglets/msgpack.hpp>

#include <algorithm>
#include <paglets/wasm/engine.hpp>
#include <paglets/wasm/snapshot.hpp>

namespace pw = paglets::wasm;

namespace {

std::shared_ptr<pw::Module> load_guest(const std::string& file) {
    const auto path = paglets::test::guest_path(file);
    if (path.empty()) paglets::test::skip(file + " not available");
    auto bytes = pw::read_file(path);
    if (!bytes) throw std::runtime_error(bytes.error());
    auto module = pw::Module::load(std::move(*bytes), pw::ImportPolicy::standard());
    if (!module) throw std::runtime_error(module.error());
    return *module;
}

std::vector<std::uint8_t> increment(std::int64_t by) {
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    w.write_str("increment");
    w.write_map_header(2);
    w.write_str("by");
    w.write_int(by);
    w.write_str("note");
    w.write_str("n" + std::to_string(by));
    return w.take();
}

std::vector<std::uint8_t> named(const std::string& name) {
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    w.write_str(name);
    w.write_nil();
    return w.take();
}

std::int64_t value_of(const std::vector<std::uint8_t>& reply) {
    paglets::msgpack::Reader r(reply);
    std::uint32_t parts = 0;
    std::string_view status;
    std::uint32_t n = 0;
    if (!r.read_array_header(parts) || !r.read_str(status) || status != "ok" || !r.read_map_header(n)) return -1;
    for (std::uint32_t i = 0; i < n; ++i) {
        std::string_view key;
        r.read_str(key);
        if (key == "value") {
            std::int64_t v = 0;
            r.read_i64(v);
            return v;
        }
        r.skip();
    }
    return -1;
}

}  // namespace

PAGLETS_TEST("snapshot: counter state survives capture, serialize and restore") {
    auto module = load_guest("counter.wasm");
    auto inst = pw::Instance::create(module);
    REQUIRE_OK(inst);
    for (int i = 1; i <= 10; ++i) REQUIRE_OK((*inst)->send(increment(i)));

    auto snap = pw::capture(**inst);
    REQUIRE_OK(snap);
    CHECK_EQ(snap->globals.size(), std::size_t{1});  // __stack_pointer

    const auto bytes = pw::serialize(*snap);
    auto loaded = pw::deserialize(bytes);
    REQUIRE_OK(loaded);

    auto restored = pw::restore(module, *loaded);
    REQUIRE_OK(restored);
    auto status = (*restored)->send(named("status"));
    REQUIRE_OK(status);
    CHECK_EQ(value_of(*status), 55);

    // Both copies continue independently (clone semantics).
    CHECK_EQ(value_of(*(*restored)->send(increment(100))), 155);
    CHECK_EQ(value_of(*(*inst)->send(increment(1))), 56);
}

PAGLETS_TEST("snapshot: heap growth is captured") {
    auto module = load_guest("counter.wasm");
    auto inst = pw::Instance::create(module);
    REQUIRE_OK(inst);
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    w.write_str("bloat");
    w.write_map_header(2);
    w.write_str("kilobytes");
    w.write_uint(1024);
    w.write_str("fill");
    w.write_uint(7);
    REQUIRE_OK((*inst)->send(w.bytes()));
    REQUIRE_OK((*inst)->send(increment(3)));

    auto snap = pw::capture(**inst);
    REQUIRE_OK(snap);
    CHECK(snap->page_count >= 16);
    CHECK(snap->data_pages() >= 16);
    auto restored = pw::restore(module, *snap);
    REQUIRE_OK(restored);
    CHECK_EQ(value_of(*(*restored)->send(named("status"))), 3);
    CHECK_EQ((*restored)->page_count(), snap->page_count);
}

PAGLETS_TEST("snapshot: untouched pages are elided and restored as zero") {
    auto module = load_guest("testbed.wasm");
    auto inst = pw::Instance::create(module);
    REQUIRE_OK(inst);
    const auto initial = *(*inst)->call_i32("pages");
    REQUIRE(*(*inst)->call_i32("grow", {20}) == initial);
    REQUIRE(*(*inst)->call_i32("add", {1, 2}) == 3);

    auto snap = pw::capture(**inst);
    REQUIRE_OK(snap);
    CHECK_EQ(snap->page_count, static_cast<std::uint32_t>(initial + 20));
    CHECK(snap->data_pages() <= static_cast<std::uint32_t>(initial));
    CHECK(pw::serialize(*snap).size() < std::size_t{snap->page_count} * pw::page_size / 4);

    auto restored = pw::restore(module, *snap);
    REQUIRE_OK(restored);
    CHECK_EQ(*(*restored)->call_i32("pages"), initial + 20);
    CHECK_EQ(*(*restored)->call_i32("calls"), 1);
    const auto mem = (*restored)->memory();
    CHECK(std::all_of(mem.end() - pw::page_size, mem.end(), [](std::uint8_t b) { return b == 0; }));
}

PAGLETS_TEST("snapshot: image of another module is refused") {
    auto counter = load_guest("counter.wasm");
    auto testbed = load_guest("testbed.wasm");
    auto inst = pw::Instance::create(counter);
    REQUIRE_OK(inst);
    auto snap = pw::capture(**inst);
    REQUIRE_OK(snap);
    auto wrong = pw::restore(testbed, *snap);
    REQUIRE(!wrong.has_value());
    CHECK(wrong.error().find("belongs to module") != std::string::npos);
}

PAGLETS_TEST("snapshot: corrupted image is detected") {
    auto module = load_guest("counter.wasm");
    auto inst = pw::Instance::create(module);
    REQUIRE_OK(inst);
    REQUIRE_OK((*inst)->send(increment(1)));
    auto snap = pw::capture(**inst);
    REQUIRE_OK(snap);
    auto bytes = pw::serialize(*snap);
    bytes[bytes.size() - 100] ^= 0x01;
    auto loaded = pw::deserialize(bytes);
    REQUIRE(!loaded.has_value());
    CHECK(loaded.error().find("hash") != std::string::npos);
}

PAGLETS_TEST("snapshot: memory limit applies on restore") {
    auto module = load_guest("counter.wasm");
    auto inst = pw::Instance::create(module);
    REQUIRE_OK(inst);
    auto snap = pw::capture(**inst);
    REQUIRE_OK(snap);
    auto small = pw::restore(module, *snap, pw::Limits{.max_memory_pages = 1});
    CHECK(!small.has_value());
}
