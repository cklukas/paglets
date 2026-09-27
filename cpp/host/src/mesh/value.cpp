// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/mesh/value.hpp>

#include <paglets/msgpack.hpp>

#include <algorithm>

namespace paglets::mesh {

Value::Value(Map v) {
    // Sort by key; a later member replaces an earlier one with the same key.
    std::stable_sort(v.begin(), v.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    Map unique;
    unique.reserve(v.size());
    for (auto& member : v) {
        if (!unique.empty() && unique.back().first == member.first) {
            unique.back().second = std::move(member.second);
        } else {
            unique.push_back(std::move(member));
        }
    }
    v_ = std::move(unique);
}

const Value* Value::find(std::string_view key) const {
    const Map* m = as_map();
    if (m == nullptr) return nullptr;
    auto it = std::lower_bound(m->begin(), m->end(), key,
                               [](const auto& member, std::string_view k) { return member.first < k; });
    if (it == m->end() || it->first != key) return nullptr;
    return &it->second;
}

bool Value::operator==(const Value& other) const {
    return v_ == other.v_;
}

// ---------------------------------------------------------------------------

namespace {

void encode_into(msgpack::Writer& w, const Value& v) {
    if (v.is_nil()) {
        w.write_nil();
    } else if (const bool* b = v.as_bool()) {
        w.write_bool(*b);
    } else if (const std::int64_t* i = v.as_int()) {
        w.write_int(*i);
    } else if (const std::string* s = v.as_str()) {
        w.write_str(*s);
    } else if (const Bytes* bin = v.as_bin()) {
        w.write_bin(*bin);
    } else if (const Array* a = v.as_array()) {
        w.write_array_header(a->size());
        for (const auto& item : *a) encode_into(w, item);
    } else if (const Map* m = v.as_map()) {
        w.write_map_header(m->size());
        for (const auto& [key, item] : *m) {
            w.write_str(key);
            encode_into(w, item);
        }
    }
}

struct Decoder {
    msgpack::Reader r;
    int max_depth;
    std::string error;

    bool fail(std::string text) {
        if (error.empty()) error = std::move(text);
        return false;
    }

    bool value(Value& out, int depth) {
        if (depth > max_depth) return fail("nesting too deep");
        switch (r.peek()) {
            case msgpack::Kind::nil:
                if (!r.read_nil()) return fail("truncated");
                out = Value();
                return true;
            case msgpack::Kind::boolean: {
                bool b = false;
                if (!r.read_bool(b)) return fail("truncated");
                out = Value(b);
                return true;
            }
            case msgpack::Kind::integer: {
                std::int64_t i = 0;
                if (!r.read_i64(i)) return fail("integer out of range");
                out = Value(i);
                return true;
            }
            case msgpack::Kind::str: {
                std::string s;
                if (!r.read_str(s)) return fail("truncated");
                out = Value(std::move(s));
                return true;
            }
            case msgpack::Kind::bin: {
                Bytes b;
                if (!r.read_bin(b)) return fail("truncated");
                out = Value(std::move(b));
                return true;
            }
            case msgpack::Kind::array: {
                std::uint32_t n = 0;
                if (!r.read_array_header(n)) return fail("truncated");
                Array a;
                a.reserve(std::min<std::uint32_t>(n, 1024));
                for (std::uint32_t k = 0; k < n; ++k) {
                    Value item;
                    if (!value(item, depth + 1)) return false;
                    a.push_back(std::move(item));
                }
                out = Value(std::move(a));
                return true;
            }
            case msgpack::Kind::map: {
                std::uint32_t n = 0;
                if (!r.read_map_header(n)) return fail("truncated");
                Map m;
                m.reserve(std::min<std::uint32_t>(n, 1024));
                for (std::uint32_t k = 0; k < n; ++k) {
                    if (r.peek() != msgpack::Kind::str) return fail("map keys must be strings");
                    std::string key;
                    if (!r.read_str(key)) return fail("truncated");
                    if (!m.empty() && !(m.back().first < key)) return fail("map keys not sorted or repeated");
                    Value item;
                    if (!value(item, depth + 1)) return false;
                    m.emplace_back(std::move(key), std::move(item));
                }
                out = Value(std::move(m));
                return true;
            }
            default: return fail("unsupported type (floating point, extension or invalid byte)");
        }
    }
};

}  // namespace

Bytes encode(const Value& value) {
    msgpack::Writer w;
    encode_into(w, value);
    return w.take();
}

std::expected<Value, std::string> decode(std::span<const std::uint8_t> bytes, int max_depth) {
    Decoder d{msgpack::Reader(bytes), max_depth, {}};
    Value v;
    if (!d.value(v, 0)) return std::unexpected("not a canonical value: " + d.error);
    if (!d.r.at_end()) return std::unexpected(std::string("not a canonical value: trailing bytes"));
    // Integer and length headers must use their shortest form: the reader
    // accepts every form, so compare with the canonical encoding.
    const Bytes again = encode(v);
    if (!std::equal(again.begin(), again.end(), bytes.begin(), bytes.end())) {
        return std::unexpected(std::string("not a canonical value: non-minimal encoding"));
    }
    return v;
}

// ---------------------------------------------------------------------------

const Value* Fields::get(std::string_view key) const {
    auto it = std::lower_bound(map.begin(), map.end(), key,
                               [](const auto& member, std::string_view k) { return member.first < k; });
    if (it == map.end() || it->first != key) return nullptr;
    return &it->second;
}

std::optional<std::string> Fields::str(std::string_view key) const {
    const Value* v = get(key);
    if (v == nullptr || v->as_str() == nullptr) return std::nullopt;
    return *v->as_str();
}

std::optional<std::int64_t> Fields::integer(std::string_view key) const {
    const Value* v = get(key);
    if (v == nullptr || v->as_int() == nullptr) return std::nullopt;
    return *v->as_int();
}

std::optional<Bytes> Fields::bin(std::string_view key) const {
    const Value* v = get(key);
    if (v == nullptr || v->as_bin() == nullptr) return std::nullopt;
    return *v->as_bin();
}

const Array* Fields::array(std::string_view key) const {
    const Value* v = get(key);
    return v == nullptr ? nullptr : v->as_array();
}

const Map* Fields::submap(std::string_view key) const {
    const Value* v = get(key);
    return v == nullptr ? nullptr : v->as_map();
}

std::optional<std::vector<std::string>> Fields::strings(std::string_view key) const {
    const Array* a = array(key);
    if (a == nullptr) return std::nullopt;
    std::vector<std::string> out;
    for (const auto& item : *a) {
        if (item.as_str() == nullptr) return std::nullopt;
        out.push_back(*item.as_str());
    }
    return out;
}

}  // namespace paglets::mesh
