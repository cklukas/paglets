// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Guest schema generator.
//
// Guest paglets are compiled by clang for wasm32-wasi, and clang does not
// implement C++26 reflection yet. This tool is compiled with GCC 16 per guest
// module: it includes the module's message header (PAGLETS_SCHEMA_HEADER),
// reflects over the types in PAGLETS_SCHEMA_NAMESPACE, and emits plain C++
// MessagePack encoders/decoders plus a JSON schema descriptor. The encoding is
// identical to the host's reflection codec (paglets/wire/reflect.hpp).
//
// Usage: schema_gen <out-header> <out-schema-json> <include-name>

#include PAGLETS_SCHEMA_HEADER

#include <paglets/wire/json.hpp>
#include <paglets/wire/reflect.hpp>

#include <fstream>
#include <iostream>
#include <meta>
#include <string>
#include <vector>

namespace {

consteval auto schema_types() {
    std::vector<std::meta::info> out;
    for (auto m : std::meta::members_of(^^PAGLETS_SCHEMA_NAMESPACE, std::meta::access_context::unchecked())) {
        if (std::meta::is_type(m) && (std::meta::is_enum_type(m) || std::meta::is_class_type(m))) {
            out.push_back(m);
        }
    }
    return std::define_static_array(out);
}

struct Output {
    std::string forward;
    std::string code;
    paglets::wire::Json::Array types;
};

template <std::meta::info T>
void emit_enum(Output& o) {
    const std::string name(std::meta::identifier_of(T));
    std::string to_name = "inline std::string_view paglets_enum_name(" + name + " v) {\n    switch (v) {\n";
    std::string from_name = "inline bool paglets_enum_from_name(std::string_view n, " + name + "& v) {\n";
    paglets::wire::Json::Array values;
    template for (constexpr auto e : std::define_static_array(std::meta::enumerators_of(T))) {
        const std::string en(std::meta::identifier_of(e));
        to_name += "        case " + name + "::" + en + ": return \"" + en + "\";\n";
        from_name += "    if (n == \"" + en + "\") { v = " + name + "::" + en + "; return true; }\n";
        values.emplace_back(en);
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

    paglets::wire::Json::Object desc;
    desc.emplace_back("name", paglets::wire::Json(name));
    desc.emplace_back("kind", paglets::wire::Json("enum"));
    desc.emplace_back("values", paglets::wire::Json(std::move(values)));
    o.types.emplace_back(std::move(desc));
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

    paglets::wire::Json::Array field_desc;
    template for (constexpr auto f : std::define_static_array(
                      std::meta::nonstatic_data_members_of(T, std::meta::access_context::unchecked()))) {
        const std::string fname(std::meta::identifier_of(f));
        enc += "    w.write_str(\"" + fname + "\");\n";
        enc += "    paglets::msgpack::write_value(w, v." + fname + ");\n";
        dec += "if (key == \"" + fname + "\") {\n            if (!paglets::msgpack::read_value(r, v." + fname +
               ")) return false;\n        } else ";

        constexpr const char* type_name =
            std::define_static_string(paglets::wire::schema_type_name(std::meta::type_of(f)));
        paglets::wire::Json::Object fd;
        fd.emplace_back("name", paglets::wire::Json(fname));
        fd.emplace_back("type", paglets::wire::Json(type_name));
        field_desc.emplace_back(std::move(fd));
    }
    enc += "}\n";
    dec += "if (!r.skip()) {\n            return false;\n        }\n    }\n    return true;\n}\n\n";

    o.code += enc + dec;
    o.forward += "inline void paglets_encode(paglets::msgpack::Writer&, const " + name + "&);\n";
    o.forward += "inline bool paglets_decode(paglets::msgpack::Reader&, " + name + "&);\n";

    paglets::wire::Json::Object desc;
    desc.emplace_back("name", paglets::wire::Json(name));
    desc.emplace_back("kind", paglets::wire::Json("record"));
    desc.emplace_back("fields", paglets::wire::Json(std::move(field_desc)));
    o.types.emplace_back(std::move(desc));
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "usage: schema_gen <out-header> <out-schema-json> <include-name>\n";
        return 2;
    }
    const std::string ns(std::meta::identifier_of(^^PAGLETS_SCHEMA_NAMESPACE));

    Output o;
    template for (constexpr auto t : schema_types()) {
        if constexpr (std::meta::is_enum_type(t)) {
            emit_enum<t>(o);
        } else {
            emit_record<t>(o);
        }
    }

    paglets::wire::Json::Object schema;
    schema.emplace_back("namespace", paglets::wire::Json(ns));
    schema.emplace_back("types", paglets::wire::Json(std::move(o.types)));
    const std::string schema_json = paglets::wire::Json(std::move(schema)).dump();

    std::ofstream header(argv[1], std::ios::trunc);
    header << "// Generated by paglets schema_gen from " << argv[3] << ". Do not edit.\n"
           << "#pragma once\n\n"
           << "#include <paglets/msgpack.hpp>\n\n"
           << "#include \"" << argv[3] << "\"\n\n"
           << "#include <cstdint>\n#include <string_view>\n\n"
           << "namespace " << ns << " {\n\n"
           << o.forward << "\n"
           << o.code << "inline constexpr char paglets_schema_json[] = R\"PAGLETS(" << schema_json << ")PAGLETS\";\n\n"
           << "}  // namespace " << ns << "\n";

    std::ofstream json(argv[2], std::ios::trunc);
    json << schema_json << "\n";
    if (!header || !json) {
        std::cerr << "schema_gen: writing output failed\n";
        return 1;
    }
    return 0;
}
