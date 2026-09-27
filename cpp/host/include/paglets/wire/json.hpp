// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Small JSON value type with parser and serializer. Numbers keep their exact
// text so 64-bit integers survive a round trip unchanged.

#pragma once

#include <charconv>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace paglets::wire {

class Json {
public:
    enum class Type { null, boolean, number, string, array, object };
    using Array = std::vector<Json>;
    using Object = std::vector<std::pair<std::string, Json>>;

    Json() = default;
    explicit Json(bool b) : type_(Type::boolean), bool_(b) {}
    explicit Json(std::string s) : type_(Type::string), text_(std::move(s)) {}
    explicit Json(const char* s) : type_(Type::string), text_(s) {}
    explicit Json(Array items) : type_(Type::array), items_(std::move(items)) {}
    explicit Json(Object members);

    static Json integer(std::int64_t v);
    static Json unsigned_integer(std::uint64_t v);
    static Json number(double v);
    static Json number_token(std::string token);

    Type type() const { return type_; }
    bool is_null() const { return type_ == Type::null; }
    bool is_bool() const { return type_ == Type::boolean; }
    bool is_number() const { return type_ == Type::number; }
    bool is_string() const { return type_ == Type::string; }
    bool is_array() const { return type_ == Type::array; }
    bool is_object() const { return type_ == Type::object; }

    bool as_bool() const { return bool_; }
    const std::string& as_string() const { return text_; }
    const std::string& number_text() const { return text_; }
    const Array& as_array() const { return items_; }
    const std::vector<std::string>& keys() const { return keys_; }

    // Object member lookup; nullptr if absent or not an object.
    const Json* find(std::string_view key) const;

    // Converts a number to an arithmetic type; false on range or format errors.
    template <class T>
    bool number_as(T& out) const {
        if (type_ != Type::number) {
            return false;
        }
        const char* first = text_.data();
        const char* last = first + text_.size();
        if constexpr (std::is_floating_point_v<T>) {
            double d = 0;
            auto [p, ec] = std::from_chars(first, last, d);
            if (ec != std::errc() || p != last) return false;
            out = static_cast<T>(d);
            return true;
        } else {
            auto [p, ec] = std::from_chars(first, last, out);
            return ec == std::errc() && p == last;
        }
    }

    std::string dump() const;
    static std::optional<Json> parse(std::string_view text);

private:
    void dump_to(std::string& out) const;

    Type type_ = Type::null;
    bool bool_ = false;
    std::string text_;               // string value or number token
    Array items_;                    // array items, or object values
    std::vector<std::string> keys_;  // object keys, parallel to items_
};

}  // namespace paglets::wire
