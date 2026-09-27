// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// WP2 exit test: a message struct is encoded on the host through C++26
// reflection, decoded in the Wasm guest by code generated with schema_gen
// (compiled by clang), re-encoded by the guest, and decoded again on the host.

#include "test.hpp"

#include <messages.hpp>
#include <paglets/wasm/direct.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/reflect.hpp>

using namespace counter_msgs;
namespace pw = paglets::wasm;
namespace wire = paglets::wire;

namespace {

struct Counter {
    std::unique_ptr<pw::Instance> instance;
    std::unique_ptr<pw::DirectHarness> harness;

    template <class Reply>
    bool call(std::string_view name, const std::vector<std::uint8_t>& payload, Reply& out) {
        auto r = harness->call(name, payload);
        if (!r || r->status != 0) return false;
        return wire::from_msgpack(r->payload, out);
    }
};

Counter counter_instance() {
    const auto path = paglets::test::guest_path("counter.wasm");
    if (path.empty()) paglets::test::skip("counter.wasm not available");
    auto bytes = pw::read_file(path);
    if (!bytes) throw std::runtime_error(bytes.error());
    auto module = pw::Module::load(std::move(*bytes), pw::ImportPolicy::standard());
    if (!module) throw std::runtime_error(module.error());
    auto inst = pw::Instance::create(*module);
    if (!inst) throw std::runtime_error(inst.error());
    Counter c{std::move(*inst), nullptr};
    c.harness = std::make_unique<pw::DirectHarness>(*c.instance);
    if (auto r = c.harness->start(); !r) throw std::runtime_error(r.error());
    return c;
}

}  // namespace

PAGLETS_TEST("roundtrip: host reflection -> guest generated code -> host") {
    auto inst = counter_instance();
    Echo in;
    in.flag = true;
    in.tiny = -128;
    in.medium = 2'000'000'000;
    in.big = 1ull << 63;
    in.ratio = -0.5f;
    in.precise = 1e-300;
    in.text = std::string(300, 'x');                    // str16 encoding
    in.blob = std::vector<std::uint8_t>(70'000, 0xab);  // bin32 encoding
    for (int i = 0; i < 20; ++i) in.items.push_back({i - 10, "item" + std::to_string(i), Mode::multiply});
    in.label = std::nullopt;
    in.nested = Status{7, 1, {"n"}, std::nullopt, 3};
    in.mode = Mode::multiply;

    Echo out;
    REQUIRE(inst.call("echo", wire::to_msgpack(in), out));

    // Byte-identical: the guest's generated encoder matches the host's
    // reflection encoder.
    CHECK(wire::to_msgpack(out) == wire::to_msgpack(in));
    CHECK(out.items.size() == 20 && out.items[19].note == "item19" && out.items[19].mode == Mode::multiply);
    CHECK(!out.label.has_value());
    CHECK(out.blob.size() == 70'000);
}

PAGLETS_TEST("roundtrip: typed increments and status") {
    auto inst = counter_instance();
    Status status;
    for (int i = 1; i <= 4; ++i) {
        REQUIRE(inst.call("increment", wire::to_msgpack(Increment{i, "step" + std::to_string(i), Mode::add}), status));
    }
    CHECK_EQ(status.value, 10);
    CHECK_EQ(status.history_size, 4u);
    CHECK(status.recent_notes == std::vector<std::string>({"step2", "step3", "step4"}));
    CHECK(status.average_step.has_value() && *status.average_step == 2.5);

    REQUIRE(inst.call("increment", wire::to_msgpack(Increment{3, "triple", Mode::multiply}), status));
    CHECK_EQ(status.value, 30);
}

PAGLETS_TEST("roundtrip: guest publishes the generated schema descriptor") {
    auto inst = counter_instance();
    std::string schema;
    REQUIRE(inst.call("schema", {}, schema));
    auto json = wire::Json::parse(schema);
    REQUIRE(json.has_value());
    CHECK(schema.find(R"({"name":"items","type":"list<Increment>"})") != std::string::npos);
    CHECK(schema.find(R"({"name":"nested","type":"optional<Status>"})") != std::string::npos);
    CHECK(schema.find(R"({"name":"Mode","kind":"enum","values":["add","multiply"]})") != std::string::npos);
}
