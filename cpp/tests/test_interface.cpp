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
    // A module built without a description.
    const auto path = paglets::test::guest_path("conformance.wasm");
    if (path.empty()) paglets::test::skip("conformance.wasm not available");
    auto bytes = paglets::wasm::read_file(path);
    REQUIRE_OK(bytes);
    auto described = wi::write_custom_section(*bytes, wi::section_name, R"({"interface":1,"name":"conformance"})");
    REQUIRE_OK(described);
    auto info = paglets::wasm::parse_module(*described);
    REQUIRE_OK(info);
    CHECK(info->find_export("paglets_on_message") != nullptr);
    auto d = wi::of_module(*described);
    REQUIRE_OK(d);
    REQUIRE(d->has_value());
    CHECK_EQ((*d)->find("name")->as_string(), std::string("conformance"));
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

PAGLETS_TEST("interface: the examples carry their descriptions") {
    for (const char* file : {"hello.wasm", "ping_pong.wasm", "counter.wasm", "pi.wasm", "inventory.wasm",
                             "chronicle.wasm", "calc_user.wasm"}) {
        const auto path = paglets::test::guest_path(file);
        if (path.empty()) paglets::test::skip(std::string(file) + " not available");
        auto bytes = paglets::wasm::read_file(path);
        REQUIRE_OK(bytes);
        auto d = wi::of_module(*bytes);
        REQUIRE_OK(d);
        REQUIRE(d->has_value());
        CHECK(!(*d)->find("messages")->as_array().empty());
    }
    // calc_user's description comes from its contract.
    auto calc = wi::of_module(*paglets::wasm::read_file(paglets::test::guest_path("calc_user.wasm")));
    REQUIRE_OK(calc);
    CHECK((**calc).dump().find("\"divide\"") != std::string::npos);
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
    std::string hostile = "i64";
    for (int i = 0; i < 4096; ++i) hostile = "optional<" + hostile + ">";
    CHECK(!wi::is_known_type(hostile, known));
}

PAGLETS_TEST("interface: examples and defaults are checked recursively with a useful path") {
    auto example = [](const std::string& type, const std::string& value) {
        return json("{\"interface\":1,\"messages\":[{\"name\":\"go\",\"request\":\"" + type + "\",\"example\":" + value + "}]}");
    };
    for (const auto& [type, value] : std::vector<std::pair<std::string, std::string>>{
        {"bool", "1"}, {"string", "false"}, {"i8", "-129"}, {"u8", "256"}, {"u64", "-1"},
        {"i64", "9223372036854775808"}, {"u64", "18446744073709551616"}, {"f32", "1e99"},
        {"bytes", R"({"$bytes":"0g"})"}, {"bytes", R"({"$bytes":"f"})"}, {"list<u8>", "[0,256]"},
        {"optional<string>", "12"}, {"none", "{}"}}) {
        auto r = wi::validate(example(type, value));
        CHECK(!r);
        if (!r) CHECK(r.error().find("messages.go.example") != std::string::npos);
    }
    for (const auto& [type, value] : std::vector<std::pair<std::string, std::string>>{
        {"i8", "-128"}, {"u8", "255"}, {"u64", "18446744073709551615"}, {"f32", "1.5"},
        {"bytes", R"({"$bytes":"00FF"})"}, {"optional<string>", "null"}, {"list<u8>", "[0,255]"}, {"any", "{}"}}) {
        CHECK_HAS_VALUE(wi::validate(example(type, value)));
    }
    auto bad = wi::validate(json(R"({"interface":1,"types":[
        {"name":"R","kind":"record","fields":[{"name":"items","type":"optional<list<u8>>","default":[0,256]}]}]})"));
    REQUIRE(!bad);
    CHECK(bad.error().find("types.R.items.default[1]") != std::string::npos);
    CHECK(!wi::validate(json(R"({"interface":1,"types":[{"name":"E","kind":"enum","values":["a","b"]}],
        "launch":{"type":"E","example":"c"}})")));
    CHECK(!wi::validate(json(R"({"interface":1,"types":[{"name":"R","kind":"record","fields":[]}],
        "launch":{"type":"R","example":{"unknown":0}}})")));
    CHECK(!wi::validate(json(R"({"interface":1,"types":[{"name":"E","kind":"enum","values":[1]}]})")));
    CHECK(!wi::validate(json(R"({"interface":1,"types":[{"name":"R","kind":"record","fields":[
        {"name":"x","type":"i64"},{"name":"x","type":"string"}]}]})")));
}

PAGLETS_TEST("interface: descriptions cannot silently replace a schema's wire shape") {
    const auto schema = json(R"({"types":[{"name":"R","kind":"record","fields":[
        {"name":"x","type":"i64"},{"name":"label","type":"string"}]},
        {"name":"E","kind":"enum","values":["a","b"]}]})");
    // Metadata and field order do not change the wire shape.
    CHECK_HAS_VALUE(wi::validate_schema_types(json(R"({"types":[{"name":"R","kind":"record","fields":[
        {"name":"label","type":"string","doc":"label"},{"name":"x","type":"i64","default":1}]}]})"), schema));
    for (const auto own : {
        R"({"types":[{"name":"R","kind":"record","fields":[{"name":"x","type":"i64"}]}]})",
        R"({"types":[{"name":"R","kind":"record","fields":[{"name":"x","type":"string"},{"name":"label","type":"string"}]}]})",
        R"({"types":[{"name":"R","kind":"record","fields":[{"name":"renamed","type":"i64"},{"name":"label","type":"string"}]}]})",
        R"({"types":[{"name":"R","kind":"enum","values":["a"]}]})",
        R"({"types":[{"name":"E","kind":"enum","values":["a","c"]}]})"}) {
        CHECK(!wi::validate_schema_types(json(own), schema));
    }
}
