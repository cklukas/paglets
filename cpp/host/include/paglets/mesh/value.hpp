// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Canonical MessagePack values for signed data (ledger records, passports).
// A signature covers bytes, so every host must produce and accept exactly
// one encoding per value:
//
// - maps have string keys, sorted by their UTF-8 bytes, without duplicates;
// - integers are 64-bit signed values in the shortest form; non-negative
//   integers use the unsigned forms;
// - strings, binaries, arrays and maps use the shortest length header;
// - no floating point values and no extension types.
//
// decode() rejects any input that is not in this form, so a value that
// decodes re-encodes to the same bytes.

#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace paglets::mesh {

class Value;
using Array = std::vector<Value>;
// Map members. A Value made from a Map keeps them sorted by key; for
// duplicate keys the last member wins.
using Map = std::vector<std::pair<std::string, Value>>;
using Bytes = std::vector<std::uint8_t>;

class Value {
public:
    struct Nil {
        bool operator==(const Nil&) const = default;
    };

    Value() = default;
    Value(Nil) {}
    Value(bool v) : v_(v) {}
    Value(std::int64_t v) : v_(v) {}
    Value(int v) : v_(static_cast<std::int64_t>(v)) {}
    Value(std::string v) : v_(std::move(v)) {}
    Value(std::string_view v) : v_(std::string(v)) {}
    Value(const char* v) : v_(std::string(v)) {}
    Value(Bytes v) : v_(std::move(v)) {}
    Value(Array v) : v_(std::move(v)) {}
    Value(Map v);

    template <std::size_t N>
    static Value bin(const std::array<std::uint8_t, N>& a) {
        return Value(Bytes(a.begin(), a.end()));
    }

    bool is_nil() const { return std::holds_alternative<Nil>(v_); }
    const bool* as_bool() const { return std::get_if<bool>(&v_); }
    const std::int64_t* as_int() const { return std::get_if<std::int64_t>(&v_); }
    const std::string* as_str() const { return std::get_if<std::string>(&v_); }
    const Bytes* as_bin() const { return std::get_if<Bytes>(&v_); }
    const Array* as_array() const { return std::get_if<Array>(&v_); }
    const Map* as_map() const { return std::get_if<Map>(&v_); }

    // Map member lookup; nullptr if this is not a map or the key is absent.
    const Value* find(std::string_view key) const;

    bool operator==(const Value& other) const;

private:
    // std::vector allows the incomplete element type Value here.
    std::variant<Nil, bool, std::int64_t, std::string, Bytes, Array, Map> v_;
};

// Canonical encoding.
Bytes encode(const Value& value);
// Decodes a complete canonical document; rejects trailing bytes, nesting
// deeper than `max_depth` and anything not canonical.
std::expected<Value, std::string> decode(std::span<const std::uint8_t> bytes, int max_depth = 32);

// Typed access to map members, for record parsers.
struct Fields {
    const Map& map;

    const Value* get(std::string_view key) const;
    std::optional<std::string> str(std::string_view key) const;
    std::optional<std::int64_t> integer(std::string_view key) const;
    std::optional<Bytes> bin(std::string_view key) const;
    template <std::size_t N>
    std::optional<std::array<std::uint8_t, N>> fixed(std::string_view key) const {
        auto b = bin(key);
        if (!b || b->size() != N) return std::nullopt;
        std::array<std::uint8_t, N> out{};
        std::copy(b->begin(), b->end(), out.begin());
        return out;
    }
    const Array* array(std::string_view key) const;
    const Map* submap(std::string_view key) const;
    // A list of strings; nullopt if the member is absent or of another type.
    std::optional<std::vector<std::string>> strings(std::string_view key) const;
};

}  // namespace paglets::mesh
