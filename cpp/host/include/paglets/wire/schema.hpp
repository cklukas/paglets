// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Schemas and service contracts by reflection (C++26, GCC 16).
//
// A schema namespace holds plain enums and structs (the wire types) and
// optionally service contracts. A contract is a struct with a static
// `service` name and one member function per operation, taking the request
// type and returning the reply type (declarations only):
//
//     namespace my_service {
//     struct AddRequest { std::int64_t a = 0; std::int64_t b = 0; };
//     struct AddReply { std::int64_t sum = 0; };
//     struct Contract {
//         static constexpr std::string_view service = "calc";
//         AddReply add(const AddRequest&);
//     };
//     }
//
// The host serves contracts with services/contract.hpp; the guest schema
// generator emits codecs, a typed client and a `serve` function for guests
// from the same declarations. Descriptors (JSON) are identical on both sides.

#pragma once

#include <paglets/wire/json.hpp>
#include <paglets/wire/reflect.hpp>

#include <meta>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::wire {

// Qualified name of a namespace, for example "paglets::services::files".
consteval std::string qualified_namespace(std::meta::info ns) {
    using namespace std::meta;
    std::string name(identifier_of(ns));
    for (auto p = parent_of(ns); p != ^^::; p = parent_of(p)) name = std::string(identifier_of(p)) + "::" + name;
    return name;
}

// True for a contract: a class with a static data member named `service`.
consteval bool is_contract(std::meta::info type) {
    using namespace std::meta;
    if (!is_class_type(type)) return false;
    for (auto m : members_of(type, access_context::unchecked())) {
        if (is_variable(m) && is_static_member(m) && has_identifier(m) && identifier_of(m) == "service") return true;
    }
    return false;
}

// The operations of a contract: its member functions other than special
// members, each with one parameter.
consteval std::vector<std::meta::info> operations_of(std::meta::info contract) {
    using namespace std::meta;
    std::vector<info> out;
    for (auto m : members_of(contract, access_context::unchecked())) {
        if (!is_function(m) || is_special_member_function(m) || is_static_member(m) || !has_identifier(m)) continue;
        if (parameters_of(m).size() != 1) throw "a contract operation takes exactly one request parameter";
        out.push_back(m);
    }
    return out;
}

consteval std::meta::info request_type(std::meta::info operation) {
    return std::meta::remove_cvref(std::meta::type_of(std::meta::parameters_of(operation)[0]));
}

consteval std::meta::info reply_type(std::meta::info operation) {
    return std::meta::remove_cvref(std::meta::return_type_of(operation));
}

// Wire types of a schema namespace (enums and records, not contracts), and
// its contracts.
consteval std::vector<std::meta::info> schema_types_of(std::meta::info ns) {
    using namespace std::meta;
    std::vector<info> out;
    for (auto m : members_of(ns, access_context::unchecked())) {
        if (!is_type(m) || !(is_enum_type(m) || is_class_type(m))) continue;
        if (is_contract(m)) continue;
        out.push_back(m);
    }
    return out;
}

consteval std::vector<std::meta::info> contracts_of(std::meta::info ns) {
    using namespace std::meta;
    std::vector<info> out;
    for (auto m : members_of(ns, access_context::unchecked())) {
        if (is_type(m) && is_contract(m)) out.push_back(m);
    }
    return out;
}

// JSON descriptor of one wire type.
template <std::meta::info T>
Json type_descriptor() {
    Json::Object desc;
    desc.emplace_back("name", Json(std::string(std::meta::identifier_of(T))));
    if constexpr (std::meta::is_enum_type(T)) {
        Json::Array values;
        template for (constexpr auto e : std::define_static_array(std::meta::enumerators_of(T))) {
            values.emplace_back(std::string(std::meta::identifier_of(e)));
        }
        desc.emplace_back("kind", Json("enum"));
        desc.emplace_back("values", Json(std::move(values)));
    } else {
        Json::Array fields;
        template for (constexpr auto f : std::define_static_array(
                          std::meta::nonstatic_data_members_of(T, std::meta::access_context::unchecked()))) {
            constexpr const char* type_name = std::define_static_string(schema_type_name(std::meta::type_of(f)));
            Json::Object fd;
            fd.emplace_back("name", Json(std::string(std::meta::identifier_of(f))));
            fd.emplace_back("type", Json(type_name));
            fields.emplace_back(std::move(fd));
        }
        desc.emplace_back("kind", Json("record"));
        desc.emplace_back("fields", Json(std::move(fields)));
    }
    return Json(std::move(desc));
}

// JSON descriptor of one contract: service name and operations.
template <std::meta::info C>
Json contract_descriptor() {
    Json::Array ops;
    template for (constexpr auto op : std::define_static_array(operations_of(C))) {
        constexpr const char* request = std::define_static_string(schema_type_name(request_type(op)));
        constexpr const char* reply = std::define_static_string(schema_type_name(reply_type(op)));
        Json::Object od;
        od.emplace_back("name", Json(std::string(std::meta::identifier_of(op))));
        od.emplace_back("request", Json(request));
        od.emplace_back("reply", Json(reply));
        ops.emplace_back(std::move(od));
    }
    using Contract = [:C:];
    Json::Object desc;
    desc.emplace_back("name", Json(std::string(Contract::service)));
    desc.emplace_back("operations", Json(std::move(ops)));
    return Json(std::move(desc));
}

// JSON descriptor of a schema namespace: {namespace, types, services}.
template <std::meta::info NS>
Json namespace_descriptor() {
    Json::Array types;
    template for (constexpr auto t : std::define_static_array(schema_types_of(NS))) {
        types.push_back(type_descriptor<t>());
    }
    Json::Array services;
    template for (constexpr auto c : std::define_static_array(contracts_of(NS))) {
        services.push_back(contract_descriptor<c>());
    }
    Json::Object schema;
    constexpr const char* ns_name = std::define_static_string(qualified_namespace(NS));
    schema.emplace_back("namespace", Json(ns_name));
    schema.emplace_back("types", Json(std::move(types)));
    schema.emplace_back("services", Json(std::move(services)));
    return Json(std::move(schema));
}

}  // namespace paglets::wire
