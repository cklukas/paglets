// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Interface descriptions of paglet modules (WP33): the custom section and the
// checks of its JSON.

#include <paglets/wire/interface.hpp>

#include <algorithm>
#include <array>

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
    if (auto t = inner("list<")) return is_known_type(*t, known);
    if (auto t = inner("optional<")) return is_known_type(*t, known);
    if (std::ranges::find(builtin_types, type) != builtin_types.end()) return true;
    return std::ranges::find(known, type) != known.end();
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
                continue;
            }
            if (kind != "record") return std::unexpected("type " + name + ": kind is record or enum");
            const Json* fields = t.find("fields");
            if (fields == nullptr || !fields->is_array()) return std::unexpected("record " + name + " has no fields");
            for (const auto& f : fields->as_array()) {
                const std::string field = str_member(f, "name");
                const std::string type = str_member(f, "type");
                if (field.empty()) return std::unexpected("record " + name + ": a field without a name");
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
