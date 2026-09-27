// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// WP2 exit test: a message struct is encoded on the host through C++26
// reflection, decoded in the Wasm guest by code generated with schema_gen
// (compiled by clang), re-encoded by the guest, and decoded again on the host.

#include "test.hpp"

#include <messages.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/reflect.hpp>

using namespace counter_msgs;
namespace pw = paglets::wasm;
namespace wire = paglets::wire;

namespace {

std::unique_ptr<pw::Instance> counter_instance() {
    const auto path = paglets::test::guest_path("counter.wasm");
    if (path.empty()) paglets::test::skip("counter.wasm not available");
    auto bytes = pw::read_file(path);
    if (!bytes) throw std::runtime_error(bytes.error());
    auto module = pw::Module::load(std::move(*bytes), pw::ImportPolicy::standard());
    if (!module) throw std::runtime_error(module.error());
    auto inst = pw::Instance::create(*module);
    if (!inst) throw std::runtime_error(inst.error());
    return std::move(*inst);
}

template <class Body>
std::vector<std::uint8_t> request(std::string_view name, const Body& body) {
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    w.write_str(name);
    wire::encode(w, body);
    return w.take();
}

template <class Reply>
bool decode_reply(const std::vector<std::uint8_t>& bytes, Reply& out) {
    paglets::msgpack::Reader r(bytes);
    std::uint32_t parts = 0;
    std::string_view status;
    return r.read_array_header(parts) && parts == 2 && r.read_str(status) && status == "ok" && wire::decode(r, out) &&
           r.ok() && r.at_end();
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

    const auto req = request("echo", in);
    auto reply = inst->send(req);
    REQUIRE_OK(reply);
    Echo out;
    REQUIRE(decode_reply(*reply, out));

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
        auto reply = inst->send(request("increment", Increment{i, "step" + std::to_string(i), Mode::add}));
        REQUIRE_OK(reply);
        REQUIRE(decode_reply(*reply, status));
    }
    CHECK_EQ(status.value, 10);
    CHECK_EQ(status.history_size, 4u);
    CHECK(status.recent_notes == std::vector<std::string>({"step2", "step3", "step4"}));
    CHECK(status.average_step.has_value() && *status.average_step == 2.5);

    auto reply = inst->send(request("increment", Increment{3, "triple", Mode::multiply}));
    REQUIRE_OK(reply);
    REQUIRE(decode_reply(*reply, status));
    CHECK_EQ(status.value, 30);
}

PAGLETS_TEST("roundtrip: guest publishes the generated schema descriptor") {
    auto inst = counter_instance();
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    w.write_str("schema");
    w.write_nil();
    auto reply = inst->send(w.bytes());
    REQUIRE_OK(reply);
    std::string schema;
    REQUIRE(decode_reply(*reply, schema));
    auto json = wire::Json::parse(schema);
    REQUIRE(json.has_value());
    CHECK(schema.find(R"({"name":"items","type":"list<Increment>"})") != std::string::npos);
    CHECK(schema.find(R"({"name":"nested","type":"optional<Status>"})") != std::string::npos);
    CHECK(schema.find(R"({"name":"Mode","kind":"enum","values":["add","multiply"]})") != std::string::npos);
}
