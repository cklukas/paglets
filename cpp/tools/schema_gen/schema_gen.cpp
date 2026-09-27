// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Guest schema generator.
//
// Guest paglets are compiled by clang for wasm32-wasi, and clang does not
// implement C++26 reflection yet. This tool is compiled with GCC 16 per schema
// header: it includes the header (PAGLETS_SCHEMA_HEADER), reflects over the
// types in PAGLETS_SCHEMA_NAMESPACE, and emits plain C++ MessagePack
// encoders/decoders plus a JSON schema descriptor. The encoding is identical
// to the host's reflection codec (paglets/wire/reflect.hpp), the descriptor
// to paglets/wire/schema.hpp.
//
// For every service contract in the namespace (wire/schema.hpp) it also
// emits a typed client (`<Contract>Client`, or `Client` for a contract named
// `Contract`) and a function registering an implementation on a router
// (`serve_<contract>`, or `serve`), so guests call and offer services with
// the same declarations the host uses.
//
// Usage: schema_gen <out-header> <out-schema-json> <include-name>

#include PAGLETS_SCHEMA_HEADER

#include <paglets/wire/json.hpp>
#include <paglets/wire/reflect.hpp>
#include <paglets/wire/schema.hpp>

#include <fstream>
#include <iostream>
#include <meta>
#include <string>
#include <vector>

namespace {

struct Output {
    std::string forward;
    std::string code;
    std::string services;
};

template <std::meta::info T>
void emit_enum(Output& o) {
    const std::string name(std::meta::identifier_of(T));
    std::string to_name = "inline std::string_view paglets_enum_name(" + name + " v) {\n    switch (v) {\n";
    std::string from_name = "inline bool paglets_enum_from_name(std::string_view n, " + name + "& v) {\n";
    template for (constexpr auto e : std::define_static_array(std::meta::enumerators_of(T))) {
        const std::string en(std::meta::identifier_of(e));
        to_name += "        case " + name + "::" + en + ": return \"" + en + "\";\n";
        from_name += "    if (n == \"" + en + "\") { v = " + name + "::" + en + "; return true; }\n";
    }
    to_name += "    }\n    return {};\n}\n";
    from_name += "    return false;\n}\n";

    o.code += to_name + from_name;
    o.code += "inline void paglets_encode(paglets::msgpack::Writer& w, const " + name +
              "& v) { w.write_str(paglets_enum_name(v)); }\n";
    o.code += "inline bool paglets_decode(paglets::msgpack::Reader& r, " + name +
              "& v) {\n    std::string_view n;\n    return r.read_str(n) && paglets_enum_from_name(n, v);\n}\n\n";

    o.forward += "inline void paglets_encode(paglets::msgpack::Writer&, const " + name + "&);\n";
    o.forward += "inline bool paglets_decode(paglets::msgpack::Reader&, " + name + "&);\n";
}

template <std::meta::info T>
void emit_record(Output& o) {
    const std::string name(std::meta::identifier_of(T));
    constexpr std::size_t field_count =
        std::define_static_array(std::meta::nonstatic_data_members_of(T, std::meta::access_context::unchecked()))
            .size();

    std::string enc = "inline void paglets_encode(paglets::msgpack::Writer& w, const " + name + "& v) {\n";
    enc += "    w.write_map_header(" + std::to_string(field_count) + ");\n";
    std::string dec = "inline bool paglets_decode(paglets::msgpack::Reader& r, " + name + "& v) {\n";
    dec += "    std::uint32_t n = 0;\n    if (!r.read_map_header(n)) return false;\n";
    dec += "    for (std::uint32_t i = 0; i < n; ++i) {\n";
    dec += "        std::string_view key;\n        if (!r.read_str(key)) return false;\n        ";

    template for (constexpr auto f : std::define_static_array(
                      std::meta::nonstatic_data_members_of(T, std::meta::access_context::unchecked()))) {
        const std::string fname(std::meta::identifier_of(f));
        enc += "    w.write_str(\"" + fname + "\");\n";
        enc += "    paglets::msgpack::write_value(w, v." + fname + ");\n";
        dec += "if (key == \"" + fname + "\") {\n            if (!paglets::msgpack::read_value(r, v." + fname +
               ")) return false;\n        } else ";
    }
    enc += "}\n";
    dec += "if (!r.skip()) {\n            return false;\n        }\n    }\n    return true;\n}\n\n";

    o.code += enc + dec;
    o.forward += "inline void paglets_encode(paglets::msgpack::Writer&, const " + name + "&);\n";
    o.forward += "inline bool paglets_decode(paglets::msgpack::Reader&, " + name + "&);\n";
}

template <std::meta::info C>
void emit_contract(Output& o) {
    const std::string contract(std::meta::identifier_of(C));
    const std::string client = contract == "Contract" ? "Client" : contract + "Client";
    std::string serve_name = "serve";
    if (contract != "Contract") {
        serve_name += "_";
        for (char ch : contract) serve_name += static_cast<char>(ch >= 'A' && ch <= 'Z' ? ch - 'A' + 'a' : ch);
    }

    std::string cl = "// Typed client of the " + contract + " contract.\nstruct " + client + " {\n";
    cl += "    paglets::Endpoint endpoint;\n\n";
    std::string sv = "// Registers `impl` on `router` for the operations of " + contract +
                     "; impl.<op>(const Request&, paglets::Message&) returns paglets::Result<Reply>.\n"
                     "template <class Impl>\nvoid " +
                     serve_name + "(paglets::Router& router, Impl& impl) {\n";
    template for (constexpr auto op : std::define_static_array(paglets::wire::operations_of(C))) {
        const std::string name(std::meta::identifier_of(op));
        const std::string request(std::meta::identifier_of(paglets::wire::request_type(op)));
        const std::string reply(std::meta::identifier_of(paglets::wire::reply_type(op)));
        cl += "    // on_reply(paglets::Result<" + reply + ">, paglets::Message&)\n";
        cl += "    template <class F>\n    paglets::Result<std::uint64_t> " + name + "(const " + request +
              "& request, F&& on_reply, paglets::RequestOptions options = {}) const {\n";
        cl += "        return endpoint.request(\"" + name +
              "\", request, [g = std::forward<F>(on_reply)](paglets::Message& m) mutable {\n";
        cl += "            if (!m.ok()) {\n                g(paglets::Result<" + reply +
              ">(std::unexpected(m.status())), m);\n                return;\n            }\n";
        cl += "            " + reply + " reply{};\n";
        cl += "            if (!m.decode(reply)) {\n                g(paglets::Result<" + reply +
              ">(std::unexpected(paglets::abi::malformed)), m);\n                return;\n            }\n";
        cl += "            g(paglets::Result<" + reply +
              ">(std::move(reply)), m);\n        }, std::move(options));\n    }\n";
        sv += "    router.on<" + request + ">(\"" + name + "\", [&impl](const " + request +
              "& q, paglets::Message& m) -> std::int32_t {\n";
        sv += "        auto r = impl." + name + "(q, m);\n        if (!r) return r.error();\n";
        sv += "        if (m.reply_pending()) m.reply(*r);\n        return paglets::abi::handled;\n    });\n";
    }
    cl += "};\n\n";
    sv += "    router.on(\"describe\", [](paglets::Message& m) { m.reply(std::string(paglets_schema_json)); });\n";
    sv += "}\n\n";
    o.services += cl + sv;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "usage: schema_gen <out-header> <out-schema-json> <include-name>\n";
        return 2;
    }
    constexpr auto ns = ^^PAGLETS_SCHEMA_NAMESPACE;
    const std::string ns_name = std::define_static_string(paglets::wire::qualified_namespace(ns));

    Output o;
    template for (constexpr auto t : std::define_static_array(paglets::wire::schema_types_of(ns))) {
        if constexpr (std::meta::is_enum_type(t)) {
            emit_enum<t>(o);
        } else {
            emit_record<t>(o);
        }
    }
    template for (constexpr auto c : std::define_static_array(paglets::wire::contracts_of(ns))) {
        emit_contract<c>(o);
    }
    const std::string schema_json = paglets::wire::namespace_descriptor<ns>().dump();

    std::ofstream header(argv[1], std::ios::trunc);
    header << "// Generated by paglets schema_gen from " << argv[3] << ". Do not edit.\n"
           << "#pragma once\n\n"
           << "#include <paglets/msgpack.hpp>\n";
    if (!o.services.empty()) header << "#include <paglets/paglet.hpp>\n";
    header << "\n#include \"" << argv[3] << "\"\n\n"
           << "#include <cstdint>\n#include <string>\n#include <string_view>\n#include <utility>\n\n"
           << "namespace " << ns_name << " {\n\n"
           << o.forward << "\n"
           << o.code << "inline constexpr char paglets_schema_json[] = R\"PAGLETS(" << schema_json << ")PAGLETS\";\n\n"
           << o.services << "}  // namespace " << ns_name << "\n";

    std::ofstream json(argv[2], std::ios::trunc);
    json << schema_json << "\n";
    if (!header || !json) {
        std::cerr << "schema_gen: writing output failed\n";
        return 1;
    }
    return 0;
}
