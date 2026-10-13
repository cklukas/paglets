// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Session requests for tools (WP32, planning/cpp-web-ui.md): status with
// trust classes and host facts, one-way sends, interface descriptions,
// termination by admins.

#include "mesh_fixture.hpp"
#include "test.hpp"

#include <paglets/abi.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/interface.hpp>
#include <paglets/wire/json_msgpack.hpp>

using namespace paglets::test;
namespace wi = paglets::wire::interface;

#define CHECK_HAS_VALUE(r) CHECK((r).has_value())

namespace {

Map ask(paglets::node::Node& node, const SigningKey& who, std::string_view role, Map request) {
    auto v = decode(node.answer_session(who.public_key(), role, encode(Value(std::move(request)))));
    REQUIRE(v && v->as_map() != nullptr);
    return *v->as_map();
}

bool ok(const Map& m) {
    const Value* v = Fields{m}.get("ok");
    return v != nullptr && v->as_bool() != nullptr && *v->as_bool();
}

Bytes json_payload(std::string_view text) {
    auto j = paglets::wire::Json::parse(text);
    REQUIRE(j.has_value());
    return paglets::wire::json_to_msgpack(*j);
}

constexpr std::string_view counter_interface = R"({"interface":1,"name":"counter","summary":"Counts",
  "launch":{"type":"none"},
  "messages":[{"name":"increment","kind":"both","request":"Increment","reply":"Status"},
              {"name":"status","kind":"call","request":"none","reply":"Status"}],
  "types":[{"name":"Mode","kind":"enum","values":["add","multiply"]},
           {"name":"Increment","kind":"record","fields":[{"name":"by","type":"i64"},{"name":"note","type":"string"},
                                                        {"name":"mode","type":"Mode"}]},
           {"name":"Status","kind":"record","fields":[{"name":"value","type":"i64"}]}]})";

}  // namespace

PAGLETS_TEST("session: status, send, describe and terminate for tools") {
    Mesh m;
    auto& a = m.add_host("a");
    m.enroll_all();

    // The counter module with an interface description.
    const auto path = guest_path("counter.wasm");
    if (path.empty()) skip("counter.wasm not available");
    auto plain = paglets::wasm::read_file(path);
    REQUIRE_OK(plain);
    auto described = wi::write_custom_section(*plain, wi::section_name, counter_interface);
    REQUIRE_OK(described);
    const paglets::Digest hash = paglets::sha256(*described);
    const std::string id = paglet_id('7');
    const Passport passport = m.passport(paglets::to_hex(hash), id);
    Map launched = ask(*a.node, m.owner, "owner",
                       Map{{"t", Value("launch")},
                           {"passport", Value(passport.encode())},
                           {"module", Value(*described)},
                           {"args", Value(Bytes{})}});
    REQUIRE(ok(launched));
    CHECK(Fields{launched}.str("paglet") == std::optional<std::string>(id));

    // status lists the paglet with its trust class, the native system
    // paglets by name, and facts about the host.
    Map status = ask(*a.node, m.owner, "owner", Map{{"t", Value("status")}});
    REQUIRE(ok(status));
    const Fields sf{status};
    bool listed = false;
    for (const auto& p : *sf.array("paglets")) {
        const Array& e = *p.as_array();
        if (*e[0].as_str() == id) {
            listed = true;
            REQUIRE(e.size() == 5u);
            CHECK_EQ(*e[4].as_str(), std::string("roaming"));
        }
    }
    CHECK(listed);
    auto system = sf.strings("system");
    REQUIRE(system.has_value());
    CHECK(std::ranges::find(*system, std::string("system.mesh-info")) != system->end());
    CHECK(!sf.str("version").value_or("").empty());
    CHECK(!sf.str("os").value_or("").empty());
    CHECK(sf.integer("cpus").value_or(0) > 0);
    CHECK(sf.integer("uptime_s").has_value());

    // describe: of the paglet, of the module (also listed by `modules`).
    Map of_paglet = ask(*a.node, m.owner, "owner", Map{{"t", Value("describe")}, {"paglet", Value(id)}});
    REQUIRE(ok(of_paglet));
    auto text = Fields{of_paglet}.str("interface");
    REQUIRE(text.has_value());
    auto parsed = wi::parse(*text);
    REQUIRE_OK(parsed);
    CHECK_EQ(parsed->find("name")->as_string(), std::string("counter"));
    Map of_module =
        ask(*a.node, m.owner, "owner", Map{{"t", Value("describe")}, {"module", Value(paglets::to_hex(hash))}});
    CHECK(Fields{of_module}.str("interface") == text);
    Map modules = ask(*a.node, m.owner, "owner", Map{{"t", Value("modules")}});
    bool module_listed = false;
    for (const auto& e : *Fields{modules}.array("modules")) {
        const Fields mf{*e.as_map()};
        if (mf.str("hash") == std::optional<std::string>(paglets::to_hex(hash))) {
            module_listed = true;
            CHECK(mf.str("interface") == text);
        }
    }
    CHECK(module_listed);

    // A native system paglet describes its service.
    Map of_system =
        ask(*a.node, m.owner, "owner", Map{{"t", Value("describe")}, {"paglet", Value("system.server-info")}});
    REQUIRE(ok(of_system));
    auto system_interface = wi::parse(Fields{of_system}.str("interface").value_or(""));
    REQUIRE_OK(system_interface);
    CHECK(!system_interface->find("messages")->as_array().empty());

    // A one-way send, then a call sees its effect.
    Map sent = ask(*a.node, m.owner, "owner",
                   Map{{"t", Value("send")},
                       {"paglet", Value(id)},
                       {"name", Value("increment")},
                       {"payload", Value(json_payload(R"({"by":5,"note":"web","mode":"add"})"))},
                       {"priority", Value(std::int64_t{5})}});
    REQUIRE(ok(sent));
    Map called = ask(*a.node, m.owner, "owner",
                     Map{{"t", Value("call")}, {"paglet", Value(id)}, {"name", Value("status")}});
    REQUIRE(ok(called));
    auto reply = paglets::wire::msgpack_to_json(*Fields{called}.bin("payload"));
    REQUIRE(reply.has_value());
    CHECK(reply->dump().find("\"value\":5") != std::string::npos);

    // Another owner may not send; owners may not terminate; admins may.
    const SigningKey stranger = SigningKey::generate();
    CHECK(!ok(ask(*a.node, stranger, "owner",
                  Map{{"t", Value("send")}, {"paglet", Value(id)}, {"name", Value("status")}})));
    CHECK(!ok(ask(*a.node, m.owner, "owner", Map{{"t", Value("terminate")}, {"paglet", Value(id)}})));
    Map terminated = ask(*a.node, m.admin, "admin",
                         Map{{"t", Value("terminate")}, {"paglet", Value(id)}, {"reason", Value("test")}});
    REQUIRE(ok(terminated));
    CHECK(m.settle([&] { return !a.f->runtime->info(id).has_value(); }));
}
