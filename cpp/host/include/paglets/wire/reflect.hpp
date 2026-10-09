// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Reflection-driven wire codecs (C++26, GCC 16 with -freflection).
//
// Plain structs, enums and standard containers are encoded without any
// hand-written serializer:
//   - structs become MessagePack maps / JSON objects keyed by field name,
//   - enums become their enumerator name,
//   - std::vector<T> becomes an array (std::vector<std::uint8_t> becomes bin),
//   - std::optional<T> becomes the value or nil/null.
//
// The encoding matches the code produced by the guest schema generator
// (tools/schema_gen), so host and guest interoperate byte for byte.

#pragma once

#include <paglets/msgpack.hpp>
#include <paglets/wire/json.hpp>

#include <charconv>
#include <cstdint>
#include <meta>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace paglets::wire {

namespace detail {

template <class T>
inline constexpr bool is_string_v = std::is_same_v<T, std::string>;

template <class T>
inline constexpr bool is_bytes_v = std::is_same_v<T, std::vector<std::uint8_t>>;

template <class T>
inline constexpr bool is_record_v =
    std::is_class_v<T> && !is_string_v<T> && !msgpack::is_vector<T>::value && !msgpack::is_optional<T>::value;

consteval auto fields_of(std::meta::info type) {
    return std::define_static_array(std::meta::nonstatic_data_members_of(type, std::meta::access_context::unchecked()));
}

consteval auto enumerators(std::meta::info type) {
    return std::define_static_array(std::meta::enumerators_of(type));
}

}  // namespace detail

// ---------------------------------------------------------------------------
// Enum names

template <class E>
    requires std::is_enum_v<E>
constexpr std::string_view enum_name(E value) {
    template for (constexpr auto e : detail::enumerators(^^E)) {
        if (value == [:e:]) {
            return std::meta::identifier_of(e);
        }
    }
    return {};
}

template <class E>
    requires std::is_enum_v<E>
constexpr bool enum_from_name(std::string_view name, E& out) {
    template for (constexpr auto e : detail::enumerators(^^E)) {
        if (name == std::meta::identifier_of(e)) {
            out = [:e:];
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// Canonical schema type names, identical on every platform and compiler
// (for example "i64", "list<string>", "optional<f64>", "Status").
// The fixed-width integer types are reflected through their global typedefs:
// libstdc++ declares std::int64_t and friends as using-declarations, which
// cannot be reflected.

consteval std::string schema_type_name(std::meta::info type) {
    using namespace std::meta;
    type = dealias(remove_cvref(type));
    if (type == dealias(^^bool)) return "bool";
    if (type == dealias(^^::int8_t)) return "i8";
    if (type == dealias(^^::int16_t)) return "i16";
    if (type == dealias(^^::int32_t)) return "i32";
    if (type == dealias(^^::int64_t)) return "i64";
    if (type == dealias(^^::uint8_t)) return "u8";
    if (type == dealias(^^::uint16_t)) return "u16";
    if (type == dealias(^^::uint32_t)) return "u32";
    if (type == dealias(^^::uint64_t)) return "u64";
    if (type == ^^float) return "f32";
    if (type == ^^double) return "f64";
    if (type == dealias(^^std::string)) return "string";
    if (has_template_arguments(type)) {
        const auto tmpl = template_of(type);
        const auto arg = template_arguments_of(type)[0];
        if (tmpl == ^^std::vector) {
            if (dealias(arg) == dealias(^^::uint8_t)) return "bytes";
            return "list<" + schema_type_name(arg) + ">";
        }
        if (tmpl == ^^std::optional) {
            return "optional<" + schema_type_name(arg) + ">";
        }
    }
    if (is_enum_type(type) || is_class_type(type)) {
        return std::string(identifier_of(type));
    }
    return "unsupported:" + std::string(display_string_of(type));
}

template <class T>
constexpr std::string_view schema_name() {
    constexpr const char* name = std::define_static_string(schema_type_name(^^T));
    return name;
}

// ---------------------------------------------------------------------------
// MessagePack

template <class T>
void encode(msgpack::Writer& w, const T& value) {
    if constexpr (std::is_enum_v<T>) {
        w.write_str(enum_name(value));
    } else if constexpr (detail::is_bytes_v<T>) {
        w.write_bin(value);
    } else if constexpr (msgpack::is_vector<T>::value) {
        w.write_array_header(value.size());
        for (const auto& item : value) {
            encode(w, item);
        }
    } else if constexpr (msgpack::is_optional<T>::value) {
        if (value) {
            encode(w, *value);
        } else {
            w.write_nil();
        }
    } else if constexpr (detail::is_record_v<T>) {
        constexpr std::size_t field_count = detail::fields_of(^^T).size();
        w.write_map_header(field_count);
        template for (constexpr auto f : detail::fields_of(^^T)) {
            w.write_str(std::meta::identifier_of(f));
            encode(w, value.[:f:]);
        }
    } else {
        msgpack::write_value(w, value);
    }
}

template <class T>
bool decode(msgpack::Reader& r, T& value) {
    if constexpr (std::is_enum_v<T>) {
        std::string_view name;
        return r.read_str(name) && enum_from_name(name, value);
    } else if constexpr (detail::is_bytes_v<T>) {
        return r.read_bin(value);
    } else if constexpr (msgpack::is_vector<T>::value) {
        std::uint32_t n = 0;
        if (!r.read_array_header(n)) {
            return false;
        }
        // Every item takes at least a byte (see msgpack::read_value).
        if (n > r.remaining()) return false;
        value.clear();
        value.reserve(std::min<std::uint32_t>(n, 64));
        for (std::uint32_t i = 0; i < n; ++i) {
            if (!decode(r, value.emplace_back())) {
                return false;
            }
        }
        return true;
    } else if constexpr (msgpack::is_optional<T>::value) {
        if (r.peek() == msgpack::Kind::nil) {
            value.reset();
            return r.read_nil();
        }
        typename T::value_type inner{};
        if (!decode(r, inner)) {
            return false;
        }
        value = std::move(inner);
        return true;
    } else if constexpr (detail::is_record_v<T>) {
        std::uint32_t n = 0;
        if (!r.read_map_header(n)) {
            return false;
        }
        for (std::uint32_t i = 0; i < n; ++i) {
            std::string_view key;
            if (!r.read_str(key)) {
                return false;
            }
            bool matched = false;
            template for (constexpr auto f : detail::fields_of(^^T)) {
                if (!matched && key == std::meta::identifier_of(f)) {
                    matched = true;
                    if (!decode(r, value.[:f:])) {
                        return false;
                    }
                }
            }
            if (!matched && !r.skip()) {  // unknown fields are ignored
                return false;
            }
        }
        return true;
    } else {
        return msgpack::read_value(r, value);
    }
}

template <class T>
std::vector<std::uint8_t> to_msgpack(const T& value) {
    msgpack::Writer w;
    encode(w, value);
    return w.take();
}

template <class T>
bool from_msgpack(std::span<const std::uint8_t> bytes, T& value) {
    msgpack::Reader r(bytes);
    return decode(r, value) && r.ok();
}

// ---------------------------------------------------------------------------
// JSON

template <class T>
Json to_json_value(const T& value) {
    if constexpr (std::is_enum_v<T>) {
        return Json(std::string(enum_name(value)));
    } else if constexpr (std::is_same_v<T, bool>) {
        return Json(value);
    } else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
        return Json::integer(static_cast<std::int64_t>(value));
    } else if constexpr (std::is_integral_v<T>) {
        return Json::unsigned_integer(static_cast<std::uint64_t>(value));
    } else if constexpr (std::is_floating_point_v<T>) {
        return Json::number(static_cast<double>(value));
    } else if constexpr (detail::is_string_v<T>) {
        return Json(value);
    } else if constexpr (msgpack::is_vector<T>::value) {
        Json::Array items;
        items.reserve(value.size());
        for (const auto& item : value) {
            items.push_back(to_json_value(item));
        }
        return Json(std::move(items));
    } else if constexpr (msgpack::is_optional<T>::value) {
        return value ? to_json_value(*value) : Json();
    } else {
        static_assert(detail::is_record_v<T>, "unsupported type for JSON encoding");
        Json::Object members;
        template for (constexpr auto f : detail::fields_of(^^T)) {
            members.emplace_back(std::string(std::meta::identifier_of(f)), to_json_value(value.[:f:]));
        }
        return Json(std::move(members));
    }
}

template <class T>
bool from_json_value(const Json& json, T& value) {
    if constexpr (std::is_enum_v<T>) {
        return json.is_string() && enum_from_name(json.as_string(), value);
    } else if constexpr (std::is_same_v<T, bool>) {
        if (!json.is_bool()) return false;
        value = json.as_bool();
        return true;
    } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
        return json.is_number() && json.number_as(value);
    } else if constexpr (detail::is_string_v<T>) {
        if (!json.is_string()) return false;
        value = json.as_string();
        return true;
    } else if constexpr (msgpack::is_vector<T>::value) {
        if (!json.is_array()) return false;
        value.clear();
        value.resize(json.as_array().size());
        for (std::size_t i = 0; i < value.size(); ++i) {
            if (!from_json_value(json.as_array()[i], value[i])) return false;
        }
        return true;
    } else if constexpr (msgpack::is_optional<T>::value) {
        if (json.is_null()) {
            value.reset();
            return true;
        }
        typename T::value_type inner{};
        if (!from_json_value(json, inner)) return false;
        value = std::move(inner);
        return true;
    } else {
        static_assert(detail::is_record_v<T>, "unsupported type for JSON decoding");
        if (!json.is_object()) return false;
        bool ok = true;
        template for (constexpr auto f : detail::fields_of(^^T)) {
            if (const Json* member = json.find(std::meta::identifier_of(f))) {
                ok = ok && from_json_value(*member, value.[:f:]);
            }
        }
        return ok;
    }
}

template <class T>
std::string to_json(const T& value) {
    return to_json_value(value).dump();
}

template <class T>
bool from_json(std::string_view text, T& value) {
    const std::optional<Json> json = Json::parse(text);
    return json && from_json_value(*json, value);
}

}  // namespace paglets::wire
