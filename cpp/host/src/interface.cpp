// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Interface descriptions of paglet modules (WP33): the custom section and the
// checks of its JSON.

#include <paglets/wire/interface.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>

namespace paglets::wire::interface {

namespace {

constexpr std::array<std::uint8_t, 8> wasm_header{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00};

// Reads an unsigned LEB128 value of at most 32 bits.
bool read_leb(std::span<const std::uint8_t> bytes, std::size_t& pos, std::uint32_t& out) {
    std::uint64_t value = 0;
    for (int shift = 0; shift < 35; shift += 7) {
        if (pos >= bytes.size()) return false;
        const std::uint8_t b = bytes[pos++];
        value |= static_cast<std::uint64_t>(b & 0x7f) << shift;
        if ((b & 0x80) == 0) {
            if (value > 0xffffffffu) return false;
            out = static_cast<std::uint32_t>(value);
            return true;
        }
    }
    return false;
}

void write_leb(std::vector<std::uint8_t>& out, std::uint32_t value) {
    do {
        std::uint8_t b = value & 0x7f;
        value >>= 7;
        if (value != 0) b |= 0x80;
        out.push_back(b);
    } while (value != 0);
}

struct Section {
    std::uint8_t id = 0;
    std::size_t start = 0;  // of the section id byte
    std::size_t end = 0;    // one past its content
    std::string name;       // custom sections
    std::size_t payload = 0;
};

std::expected<std::vector<Section>, std::string> sections_of(std::span<const std::uint8_t> wasm) {
    if (wasm.size() < wasm_header.size() || !std::equal(wasm_header.begin(), wasm_header.end(), wasm.begin())) {
        return std::unexpected(std::string("not a Wasm module (version 1)"));
    }
    std::vector<Section> out;
    std::size_t pos = wasm_header.size();
    while (pos < wasm.size()) {
        Section s;
        s.start = pos;
        s.id = wasm[pos++];
        std::uint32_t size = 0;
        if (!read_leb(wasm, pos, size) || size > wasm.size() - pos) {
            return std::unexpected(std::string("truncated Wasm section"));
        }
        s.end = pos + size;
        if (s.id == 0) {
            std::size_t p = pos;
            std::uint32_t name_size = 0;
            if (!read_leb(wasm, p, name_size) || name_size > s.end - p) {
                return std::unexpected(std::string("malformed custom section name"));
            }
            s.name.assign(reinterpret_cast<const char*>(wasm.data() + p), name_size);
            s.payload = p + name_size;
        }
        out.push_back(std::move(s));
        pos = out.back().end;
    }
    return out;
}

constexpr std::array<std::string_view, 14> builtin_types{"bool", "i8",  "i16", "i32",    "i64",   "u8",   "u16",
                                                         "u32",  "u64", "f32", "f64", "string", "bytes", "any"};

std::string str_member(const Json& j, std::string_view key) {
    const Json* v = j.find(key);
    return v != nullptr && v->is_string() ? v->as_string() : std::string();
}

// Rebuilds an object with `key` set to `value` (replaced or appended).
Json with_member(const Json& object, std::string_view key, Json value) {
    Json::Object members;
    bool set = false;
    for (std::size_t i = 0; i < object.keys().size(); ++i) {
        if (object.keys()[i] == key) {
            members.emplace_back(std::string(key), value);
            set = true;
        } else {
            members.emplace_back(object.keys()[i], object.as_array()[i]);
        }
    }
    if (!set) members.emplace_back(std::string(key), std::move(value));
    return Json(std::move(members));
}

// Values in descriptions are input documents too. Check the declared shape
// before a generated form or an example can submit malformed guest input.
std::expected<void, std::string> check_value(const Json& value, std::string_view type, const Json* types,
                                             const std::string& path, unsigned depth = 0) {
    auto mismatch = [&]() -> std::expected<void, std::string> {
        return std::unexpected(path + ": expected " + std::string(type));
    };
    if (depth > 128) return std::unexpected(path + ": value nesting exceeds 128");
    if (type == "any") return {};
    if (type == "none") return value.is_null() ? std::expected<void, std::string>{} : mismatch();
    if (type.starts_with("optional<") && type.ends_with('>')) {
        if (value.is_null()) return {};
        return check_value(value, type.substr(9, type.size() - 10), types, path, depth + 1);
    }
    if (type.starts_with("list<") && type.ends_with('>')) {
        if (!value.is_array()) return mismatch();
        std::size_t i = 0;
        for (const auto& item : value.as_array()) {
            if (auto ok = check_value(item, type.substr(5, type.size() - 6), types,
                                      path + "[" + std::to_string(i++) + "]", depth + 1); !ok) return ok;
        }
        return {};
    }
    if (type == "bool") return value.is_bool() ? std::expected<void, std::string>{} : mismatch();
    if (type == "string") return value.is_string() ? std::expected<void, std::string>{} : mismatch();
    if (type == "bytes") {
        const auto* hex = value.find("$bytes");
        if (!value.is_object() || value.keys().size() != 1 || !hex || !hex->is_string() ||
            hex->as_string().size() % 2 || hex->as_string().find_first_not_of("0123456789abcdefABCDEF") != std::string::npos)
            return mismatch();
        return {};
    }
    if (type == "f32" || type == "f64") {
        double n = 0;
        if (!value.number_as(n) || !std::isfinite(n) ||
            (type == "f32" && std::abs(n) > std::numeric_limits<float>::max())) return mismatch();
        return {};
    }
    if (type == "i8" || type == "i16" || type == "i32" || type == "i64") {
        std::int64_t n = 0;
        if (!value.number_as(n)) return mismatch();
        const unsigned bits = type == "i8" ? 8 : type == "i16" ? 16 : type == "i32" ? 32 : 64;
        if (bits < 64 && (n < -(std::int64_t{1} << (bits - 1)) || n >= (std::int64_t{1} << (bits - 1)))) return mismatch();
        return {};
    }
    if (type == "u8" || type == "u16" || type == "u32" || type == "u64") {
        std::uint64_t n = 0;
        if (!value.number_as(n)) return mismatch();
        const unsigned bits = type == "u8" ? 8 : type == "u16" ? 16 : type == "u32" ? 32 : 64;
        if (bits < 64 && n >= (std::uint64_t{1} << bits)) return mismatch();
        return {};
    }
    if (types) for (const auto& declaration : types->as_array()) {
        if (str_member(declaration, "name") != type) continue;
        if (str_member(declaration, "kind") == "enum") {
            if (!value.is_string()) return mismatch();
            for (const auto& v : declaration.find("values")->as_array())
                if (v.as_string() == value.as_string()) return {};
            return mismatch();
        }
        if (!value.is_object()) return mismatch();
        for (std::size_t i = 0; i < value.keys().size(); ++i) {
            const auto& name = value.keys()[i];
            const auto& fields = declaration.find("fields")->as_array();
            const auto it = std::ranges::find_if(fields, [&](const Json& f) { return str_member(f, "name") == name; });
            if (it == fields.end()) return std::unexpected(path + "." + name + ": unknown field");
            if (auto ok = check_value(value.as_array()[i], str_member(*it, "type"), types, path + "." + name, depth + 1); !ok) return ok;
        }
        return {};  // omitted fields use guest defaults
    }
    return mismatch();
}

}  // namespace

// -- custom sections -----------------------------------------------------------

std::expected<std::optional<std::string>, std::string> read_custom_section(std::span<const std::uint8_t> wasm,
                                                                           std::string_view name) {
    auto sections = sections_of(wasm);
    if (!sections) return std::unexpected(sections.error());
    for (const auto& s : *sections) {
        if (s.id == 0 && s.name == name) {
            return std::string(reinterpret_cast<const char*>(wasm.data() + s.payload), s.end - s.payload);
        }
    }
    return std::nullopt;
}

std::expected<std::vector<std::uint8_t>, std::string> write_custom_section(std::span<const std::uint8_t> wasm,
                                                                           std::string_view name,
                                                                           std::string_view payload) {
    auto sections = sections_of(wasm);
    if (!sections) return std::unexpected(sections.error());
    std::vector<std::uint8_t> out(wasm_header.begin(), wasm_header.end());
    for (const auto& s : *sections) {
        if (s.id == 0 && s.name == name) continue;
        out.insert(out.end(), wasm.begin() + static_cast<std::ptrdiff_t>(s.start),
                   wasm.begin() + static_cast<std::ptrdiff_t>(s.end));
    }
    std::vector<std::uint8_t> content;
    write_leb(content, static_cast<std::uint32_t>(name.size()));
    content.insert(content.end(), name.begin(), name.end());
    content.insert(content.end(), payload.begin(), payload.end());
    out.push_back(0);
    write_leb(out, static_cast<std::uint32_t>(content.size()));
    out.insert(out.end(), content.begin(), content.end());
    return out;
}

// -- descriptions --------------------------------------------------------------

bool is_known_type(std::string_view type, const std::vector<std::string>& known) {
    auto inner = [&](std::string_view prefix) -> std::optional<std::string_view> {
        if (type.size() > prefix.size() + 1 && type.starts_with(prefix) && type.back() == '>') {
            return type.substr(prefix.size(), type.size() - prefix.size() - 1);
        }
        return std::nullopt;
    };
    // Untrusted metadata can put thousands of wrappers inside one JSON
    // string. Peel them iteratively and bound nesting before rendering or
    // validating a value; do not consume one host stack frame per wrapper.
    for (unsigned depth = 0; depth <= 128; ++depth) {
        if (auto t = inner("list<")) { type = *t; continue; }
        if (auto t = inner("optional<")) { type = *t; continue; }
        if (std::ranges::find(builtin_types, type) != builtin_types.end()) return true;
        return std::ranges::find(known, type) != known.end();
    }
    return false;
}

std::expected<void, std::string> validate(const Json& d) {
    if (!d.is_object()) return std::unexpected(std::string("an interface description is a JSON object"));
    const Json* version = d.find("interface");
    std::int64_t v = 0;
    if (version == nullptr || !version->number_as(v) || v != format_version) {
        return std::unexpected("\"interface\" must be " + std::to_string(format_version));
    }
    for (const char* key : {"name", "summary"}) {
        if (const Json* s = d.find(key); s != nullptr && !s->is_string()) {
            return std::unexpected("\"" + std::string(key) + "\" is a string");
        }
    }

    // Types: records and enums with unique names.
    std::vector<std::string> known;
    const Json* types = d.find("types");
    if (types != nullptr) {
        if (!types->is_array()) return std::unexpected(std::string("\"types\" is an array"));
        for (const auto& t : types->as_array()) {
            const std::string name = str_member(t, "name");
            if (name.empty()) return std::unexpected(std::string("a type without a name"));
            if (std::ranges::find(builtin_types, name) != builtin_types.end() || name == "none")
                return std::unexpected("type " + name + ": built-in name is reserved");
            if (std::ranges::find(known, name) != known.end()) return std::unexpected("type " + name + " twice");
            known.push_back(name);
        }
        for (const auto& t : types->as_array()) {
            const std::string name = str_member(t, "name");
            const std::string kind = str_member(t, "kind");
            if (kind == "enum") {
                const Json* values = t.find("values");
                if (values == nullptr || !values->is_array() || values->as_array().empty()) {
                    return std::unexpected("enum " + name + " has no values");
                }
                std::vector<std::string> unique;
                for (const auto& value : values->as_array()) {
                    if (!value.is_string() || value.as_string().empty()) return std::unexpected("enum " + name + ": values are nonempty strings");
                    if (std::ranges::find(unique, value.as_string()) != unique.end()) return std::unexpected("enum " + name + ": duplicate value " + value.as_string());
                    unique.push_back(value.as_string());
                }
                continue;
            }
            if (kind != "record") return std::unexpected("type " + name + ": kind is record or enum");
            const Json* fields = t.find("fields");
            if (fields == nullptr || !fields->is_array()) return std::unexpected("record " + name + " has no fields");
            std::vector<std::string> field_names;
            for (const auto& f : fields->as_array()) {
                const std::string field = str_member(f, "name");
                const std::string type = str_member(f, "type");
                if (field.empty()) return std::unexpected("record " + name + ": a field without a name");
                if (std::ranges::find(field_names, field) != field_names.end()) return std::unexpected("record " + name + ": duplicate field " + field);
                field_names.push_back(field);
                if (!is_known_type(type, known)) {
                    return std::unexpected("record " + name + ", field " + field + ": unknown type '" + type + "'");
                }
            }
        }
    }

    auto check_type = [&](const Json& owner, std::string_view key, const std::string& where)
        -> std::expected<void, std::string> {
        const Json* t = owner.find(key);
        if (t == nullptr) return {};
        if (!t->is_string()) return std::unexpected(where + ": \"" + std::string(key) + "\" is a type name");
        if (t->as_string() == "none") return {};
        if (!is_known_type(t->as_string(), known)) {
            return std::unexpected(where + ": unknown type '" + t->as_string() + "'");
        }
        return {};
    };

    if (const Json* launch = d.find("launch")) {
        if (!launch->is_object()) return std::unexpected(std::string("\"launch\" is an object"));
        if (auto ok = check_type(*launch, "type", "launch"); !ok) return ok;
    }
    const Json* messages = d.find("messages");
    if (messages != nullptr) {
        if (!messages->is_array()) return std::unexpected(std::string("\"messages\" is an array"));
        std::vector<std::string> names;
        for (const auto& m : messages->as_array()) {
            const std::string name = str_member(m, "name");
            if (name.empty()) return std::unexpected(std::string("a message without a name"));
            if (name.starts_with("paglets.")) return std::unexpected("message " + name + ": the prefix is reserved");
            if (std::ranges::find(names, name) != names.end()) return std::unexpected("message " + name + " twice");
            names.push_back(name);
            const Json* kind = m.find("kind");
            if (kind != nullptr && (!kind->is_string() || (kind->as_string() != "call" &&
                                                           kind->as_string() != "send" &&
                                                           kind->as_string() != "both"))) {
                return std::unexpected("message " + name + ": kind is call, send or both");
            }
            if (auto ok = check_type(m, "request", "message " + name); !ok) return ok;
            if (auto ok = check_type(m, "reply", "message " + name); !ok) return ok;
            if (const Json* danger = m.find("danger"); danger != nullptr && !danger->is_bool()) {
                return std::unexpected("message " + name + ": danger is true or false");
            }
        }
    }
    // Validate values after all declarations have passed structural checks.
    if (types) for (const auto& t : types->as_array()) {
        if (str_member(t, "kind") != "record") continue;
        for (const auto& f : t.find("fields")->as_array()) {
            if (const auto* value = f.find("default")) {
                if (auto ok = check_value(*value, str_member(f, "type"), types,
                    "types." + str_member(t, "name") + "." + str_member(f, "name") + ".default"); !ok) return ok;
            }
        }
    }
    if (const auto* launch = d.find("launch")) {
        if (const auto* value = launch->find("example")) {
            const auto type = str_member(*launch, "type");
            if (auto ok = check_value(*value, type.empty() ? "none" : type, types, "launch.example"); !ok) return ok;
        }
    }
    if (messages) for (const auto& m : messages->as_array()) {
        if (const auto* value = m.find("example")) {
            const auto type = str_member(m, "request");
            if (auto ok = check_value(*value, type.empty() ? "none" : type, types,
                                      "messages." + str_member(m, "name") + ".example"); !ok) return ok;
        }
    }

    return {};
}

std::expected<Json, std::string> parse(std::string_view text) {
    auto j = Json::parse(text);
    if (!j) return std::unexpected(std::string("the interface description is not valid JSON"));
    if (auto ok = validate(*j); !ok) return std::unexpected(ok.error());
    return std::move(*j);
}

Json merge_schema_types(Json description, const Json& schema) {
    const Json* schema_types = schema.find("types");
    if (schema_types == nullptr || !schema_types->is_array()) return description;
    Json::Array types;
    std::vector<std::string> names;
    if (const Json* own = description.find("types"); own != nullptr && own->is_array()) {
        for (const auto& t : own->as_array()) {
            names.push_back(str_member(t, "name"));
            types.push_back(t);
        }
    }
    for (const auto& t : schema_types->as_array()) {
        const std::string name = str_member(t, "name");
        if (std::ranges::find(names, name) != names.end()) continue;
        names.push_back(name);
        types.push_back(t);
    }
    return with_member(description, "types", Json(std::move(types)));
}

std::expected<void, std::string> validate_schema_types(const Json& description, const Json& schema) {
    const auto* own = description.find("types");
    const auto* generated = schema.find("types");
    if (!own || !own->is_array() || !generated || !generated->is_array()) return {};
    for (const auto& type : own->as_array()) {
        const auto name = str_member(type, "name");
        const auto found = std::ranges::find_if(generated->as_array(), [&](const Json& t) { return str_member(t, "name") == name; });
        if (found == generated->as_array().end()) continue;
        const auto kind = str_member(type, "kind");
        if (kind != str_member(*found, "kind")) return std::unexpected("type " + name + ": kind differs from schema");
        if (kind == "record") {
            const auto* fields = type.find("fields");
            const auto* expected = found->find("fields");
            if (!fields || !fields->is_array() || !expected || !expected->is_array())
                return std::unexpected("record " + name + ": fields must match schema");
            if (fields->as_array().size() != expected->as_array().size())
                return std::unexpected("record " + name + ": field count differs from schema");
            std::vector<std::string> seen;
            for (const auto& field : fields->as_array()) {
                const auto field_name = str_member(field, "name");
                if (std::ranges::find(seen, field_name) != seen.end())
                    return std::unexpected("record " + name + ", field " + field_name + ": repeated field");
                seen.push_back(field_name);
                const auto match = std::ranges::find_if(expected->as_array(), [&](const Json& f) { return str_member(f, "name") == field_name; });
                if (match == expected->as_array().end() || str_member(field, "type") != str_member(*match, "type"))
                    return std::unexpected("record " + name + ", field " + field_name + ": differs from schema");
            }
        } else if (kind == "enum") {
            auto values = [](const Json& t) -> std::optional<std::vector<std::string>> {
                const auto* v = t.find("values");
                if (!v || !v->is_array()) return std::nullopt;
                std::vector<std::string> out;
                for (const auto& item : v->as_array()) {
                    if (!item.is_string()) return std::nullopt;
                    out.push_back(item.as_string());
                }
                std::ranges::sort(out);
                return out;
            };
            const auto actual = values(type), expected = values(*found);
            if (!actual || !expected || *actual != *expected) return std::unexpected("enum " + name + ": values differ from schema");
        }
    }
    return {};
}

Json from_schema(const Json& schema, std::string_view name) {
    Json::Array messages;
    if (const Json* services = schema.find("services"); services != nullptr && services->is_array()) {
        for (const auto& s : services->as_array()) {
            const Json* ops = s.find("operations");
            if (ops == nullptr || !ops->is_array()) continue;
            for (const auto& op : ops->as_array()) {
                Json::Object m;
                m.emplace_back("name", Json(str_member(op, "name")));
                m.emplace_back("kind", Json("call"));
                m.emplace_back("summary", Json("Operation " + str_member(op, "name") + " of service " +
                                               str_member(s, "name")));
                m.emplace_back("request", Json(str_member(op, "request")));
                m.emplace_back("reply", Json(str_member(op, "reply")));
                messages.emplace_back(std::move(m));
            }
        }
    }
    Json::Object d;
    d.emplace_back("interface", Json::integer(format_version));
    d.emplace_back("name", Json(std::string(name)));
    d.emplace_back("messages", Json(std::move(messages)));
    return merge_schema_types(Json(std::move(d)), schema);
}

std::expected<std::optional<Json>, std::string> of_module(std::span<const std::uint8_t> wasm) {
    auto text = read_custom_section(wasm, section_name);
    if (!text) return std::unexpected(text.error());
    if (!*text) return std::optional<Json>();
    auto j = parse(**text);
    if (!j) return std::unexpected(j.error());
    return std::optional<Json>(std::move(*j));
}

}  // namespace paglets::wire::interface
