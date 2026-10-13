// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Interface descriptions (WP33): custom sections, checks, schema merges.

#include "test.hpp"

#include <paglets/wasm/binary.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/interface.hpp>

#include <string>

namespace wi = paglets::wire::interface;
using paglets::wire::Json;

#define CHECK_HAS_VALUE(r) CHECK((r).has_value())

namespace {

// A minimal module: header, one type section (no types).
std::vector<std::uint8_t> tiny_module() {
    return {0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00};
}

Json json(std::string_view text) {
    auto j = Json::parse(text);
    REQUIRE(j.has_value());
    return *j;
}

}  // namespace

PAGLETS_TEST("interface: custom sections are written, replaced and read") {
    const auto module = tiny_module();
    auto none = wi::read_custom_section(module, wi::section_name);
    REQUIRE_OK(none);
    CHECK(!none->has_value());
    auto once = wi::write_custom_section(module, wi::section_name, "{\"interface\":1}");
    REQUIRE_OK(once);
    auto twice = wi::write_custom_section(*once, wi::section_name, "{\"interface\":1,\"name\":\"x\"}");
    REQUIRE_OK(twice);
    auto read = wi::read_custom_section(*twice, wi::section_name);
    REQUIRE_OK(read);
    CHECK_EQ(**read, std::string("{\"interface\":1,\"name\":\"x\"}"));
    // One section only: the module grew by exactly one section.
    CHECK_EQ(twice->size(), module.size() + 3 + wi::section_name.size() + std::string_view(R"({"interface":1,"name":"x"})").size());
    CHECK(!wi::read_custom_section(std::vector<std::uint8_t>{1, 2, 3}, wi::section_name));
    std::vector<std::uint8_t> truncated = *once;
    truncated.pop_back();
    CHECK(!wi::read_custom_section(truncated, wi::section_name));
    // A long payload (multi-byte LEB128 sizes).
    const std::string big(70000, 'x');
    auto large = wi::write_custom_section(module, "big", big);
    REQUIRE_OK(large);
    CHECK_EQ(wi::read_custom_section(*large, "big")->value(), big);
}

PAGLETS_TEST("interface: a real module keeps working with its description") {
    const auto path = paglets::test::guest_path("counter.wasm");
    if (path.empty()) paglets::test::skip("counter.wasm not available");
    auto bytes = paglets::wasm::read_file(path);
    REQUIRE_OK(bytes);
    auto described = wi::write_custom_section(*bytes, wi::section_name, R"({"interface":1,"name":"counter"})");
    REQUIRE_OK(described);
    auto info = paglets::wasm::parse_module(*described);
    REQUIRE_OK(info);
    CHECK(info->find_export("paglets_on_message") != nullptr);
    auto d = wi::of_module(*described);
    REQUIRE_OK(d);
    REQUIRE(d->has_value());
    CHECK_EQ((*d)->find("name")->as_string(), std::string("counter"));
    auto plain = wi::of_module(*bytes);
    REQUIRE_OK(plain);
    CHECK(!plain->has_value());
}

PAGLETS_TEST("interface: descriptions are checked") {
    CHECK_HAS_VALUE(wi::validate(json(R"({"interface":1})")));
    CHECK_HAS_VALUE(wi::validate(json(R"({"interface":1,"name":"n","launch":{"type":"Args"},
        "messages":[{"name":"go","kind":"send","request":"list<optional<Args>>"},
                    {"name":"stop","request":"none","reply":"any","danger":true}],
        "types":[{"name":"Args","kind":"record","fields":[{"name":"n","type":"u32","doc":"how many"}]},
                 {"name":"Mode","kind":"enum","values":["a"]}]})")));
    auto refused = [](std::string_view text, std::string_view why) {
        auto r = wi::validate(json(text));
        CHECK(!r);
        if (!r) CHECK(r.error().find(why) != std::string::npos);
    };
    refused(R"({"interface":2})", "\"interface\" must be 1");
    refused(R"([])", "JSON object");
    refused(R"({"interface":1,"messages":[{"kind":"call"}]})", "without a name");
    refused(R"({"interface":1,"messages":[{"name":"a"},{"name":"a"}]})", "twice");
    refused(R"({"interface":1,"messages":[{"name":"a","kind":"poke"}]})", "kind");
    refused(R"({"interface":1,"messages":[{"name":"a","request":"Missing"}]})", "unknown type 'Missing'");
    refused(R"({"interface":1,"messages":[{"name":"paglets.x"}]})", "reserved");
    refused(R"({"interface":1,"launch":{"type":"list<Nope>"}})", "unknown type");
    refused(R"({"interface":1,"types":[{"name":"R","kind":"record","fields":[{"name":"f","type":"R2"}]}]})",
            "unknown type 'R2'");
    refused(R"({"interface":1,"types":[{"name":"E","kind":"enum","values":[]}]})", "no values");
    CHECK(!wi::parse("{not json"));
}

PAGLETS_TEST("interface: schema types merge in; contracts become descriptions") {
    const Json schema = json(R"({"namespace":"calc","types":[
        {"name":"AddRequest","kind":"record","fields":[{"name":"a","type":"i64"},{"name":"b","type":"i64"}]},
        {"name":"AddReply","kind":"record","fields":[{"name":"sum","type":"i64"}]}],
        "services":[{"name":"calc","operations":[{"name":"add","request":"AddRequest","reply":"AddReply"}]}]})");
    const Json own = json(R"({"interface":1,"messages":[{"name":"add","request":"AddRequest","reply":"AddReply"}],
        "types":[{"name":"AddReply","kind":"record","fields":[{"name":"sum","type":"i64","doc":"a+b"}]}]})");
    const Json merged = wi::merge_schema_types(own, schema);
    CHECK_HAS_VALUE(wi::validate(merged));
    const auto& types = merged.find("types")->as_array();
    REQUIRE(types.size() == 2u);
    // The description's own definition wins.
    CHECK(types[0].dump().find("a+b") != std::string::npos);

    const Json generated = wi::from_schema(schema, "calc_user");
    CHECK_HAS_VALUE(wi::validate(generated));
    CHECK_EQ(generated.find("name")->as_string(), std::string("calc_user"));
    const auto& messages = generated.find("messages")->as_array();
    REQUIRE(messages.size() == 1u);
    CHECK_EQ(messages[0].find("request")->as_string(), std::string("AddRequest"));
    CHECK_EQ(messages[0].find("kind")->as_string(), std::string("call"));
}

PAGLETS_TEST("interface: type names") {
    const std::vector<std::string> known{"R"};
    for (std::string_view t : {"bool", "i64", "u8", "f64", "string", "bytes", "any", "R", "list<R>",
                               "optional<list<string>>"}) {
        CHECK(wi::is_known_type(t, known));
    }
    for (std::string_view t : {"", "list<>", "list<S>", "optional<", "int", "none"}) {
        CHECK(!wi::is_known_type(t, known));
    }
}
