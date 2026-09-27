// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Minimal MessagePack writer and reader shared by the host and by guest
// paglets. It deliberately has no dependencies beyond the standard library,
// uses no exceptions, and compiles as C++23 (guest, clang/wasi) and C++26
// (host, GCC 16).

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace paglets::msgpack {

class Writer {
public:
    // Documents are mostly small; one allocation up front avoids repeated
    // regrowth, which is expensive inside the Wasm interpreter.
    Writer() { buf_.reserve(256); }

    std::vector<std::uint8_t>& bytes() { return buf_; }
    const std::vector<std::uint8_t>& bytes() const { return buf_; }
    std::vector<std::uint8_t> take() { return std::move(buf_); }

    void write_nil() { put(0xc0); }
    void write_bool(bool v) { put(v ? 0xc3 : 0xc2); }

    void write_uint(std::uint64_t v) {
        if (v < 0x80) {
            put(static_cast<std::uint8_t>(v));
        } else if (v <= 0xff) {
            put(0xcc);
            put(static_cast<std::uint8_t>(v));
        } else if (v <= 0xffff) {
            put(0xcd);
            put_be(static_cast<std::uint16_t>(v));
        } else if (v <= 0xffffffffu) {
            put(0xce);
            put_be(static_cast<std::uint32_t>(v));
        } else {
            put(0xcf);
            put_be(v);
        }
    }

    void write_int(std::int64_t v) {
        if (v >= 0) {
            write_uint(static_cast<std::uint64_t>(v));
        } else if (v >= -32) {
            put(static_cast<std::uint8_t>(0xe0 | (v + 32)));
        } else if (v >= std::numeric_limits<std::int8_t>::min()) {
            put(0xd0);
            put(static_cast<std::uint8_t>(static_cast<std::int8_t>(v)));
        } else if (v >= std::numeric_limits<std::int16_t>::min()) {
            put(0xd1);
            put_be(static_cast<std::uint16_t>(static_cast<std::int16_t>(v)));
        } else if (v >= std::numeric_limits<std::int32_t>::min()) {
            put(0xd2);
            put_be(static_cast<std::uint32_t>(static_cast<std::int32_t>(v)));
        } else {
            put(0xd3);
            put_be(static_cast<std::uint64_t>(v));
        }
    }

    void write_f32(float v) {
        put(0xca);
        put_be(std::bit_cast<std::uint32_t>(v));
    }

    void write_f64(double v) {
        put(0xcb);
        put_be(std::bit_cast<std::uint64_t>(v));
    }

    void write_str(std::string_view s) {
        const auto n = s.size();
        if (n < 32) {
            put(static_cast<std::uint8_t>(0xa0 | n));
        } else if (n <= 0xff) {
            put(0xd9);
            put(static_cast<std::uint8_t>(n));
        } else if (n <= 0xffff) {
            put(0xda);
            put_be(static_cast<std::uint16_t>(n));
        } else {
            put(0xdb);
            put_be(static_cast<std::uint32_t>(n));
        }
        append(s.data(), n);
    }

    void write_bin(std::span<const std::uint8_t> b) {
        const auto n = b.size();
        if (n <= 0xff) {
            put(0xc4);
            put(static_cast<std::uint8_t>(n));
        } else if (n <= 0xffff) {
            put(0xc5);
            put_be(static_cast<std::uint16_t>(n));
        } else {
            put(0xc6);
            put_be(static_cast<std::uint32_t>(n));
        }
        append(b.data(), n);
    }

    void write_array_header(std::size_t n) {
        if (n < 16) {
            put(static_cast<std::uint8_t>(0x90 | n));
        } else if (n <= 0xffff) {
            put(0xdc);
            put_be(static_cast<std::uint16_t>(n));
        } else {
            put(0xdd);
            put_be(static_cast<std::uint32_t>(n));
        }
    }

    void write_map_header(std::size_t n) {
        if (n < 16) {
            put(static_cast<std::uint8_t>(0x80 | n));
        } else if (n <= 0xffff) {
            put(0xde);
            put_be(static_cast<std::uint16_t>(n));
        } else {
            put(0xdf);
            put_be(static_cast<std::uint32_t>(n));
        }
    }

private:
    void put(std::uint8_t b) { buf_.push_back(b); }
    void append(const void* p, std::size_t n) {
        const auto* b = static_cast<const std::uint8_t*>(p);
        buf_.insert(buf_.end(), b, b + n);
    }
    template <class U>
    void put_be(U v) {
        for (int shift = (sizeof(U) - 1) * 8; shift >= 0; shift -= 8) {
            put(static_cast<std::uint8_t>(v >> shift));
        }
    }

    std::vector<std::uint8_t> buf_;
};

enum class Kind { nil, boolean, integer, f32, f64, str, bin, array, map, invalid };

// Reader over a byte span. All read functions return false on type mismatch
// or truncated input and set a sticky error flag.
class Reader {
public:
    explicit Reader(std::span<const std::uint8_t> data) : data_(data) {}

    bool ok() const { return ok_; }
    bool at_end() const { return pos_ >= data_.size(); }
    std::size_t position() const { return pos_; }

    Kind peek() const {
        if (!ok_ || at_end()) {
            return Kind::invalid;
        }
        const std::uint8_t b = data_[pos_];
        if (b <= 0x7f || b >= 0xe0 || (b >= 0xcc && b <= 0xd3)) {
            return Kind::integer;
        }
        if ((b & 0xe0) == 0xa0 || (b >= 0xd9 && b <= 0xdb)) {
            return Kind::str;
        }
        if ((b & 0xf0) == 0x90 || b == 0xdc || b == 0xdd) {
            return Kind::array;
        }
        if ((b & 0xf0) == 0x80 || b == 0xde || b == 0xdf) {
            return Kind::map;
        }
        switch (b) {
            case 0xc0: return Kind::nil;
            case 0xc2:
            case 0xc3: return Kind::boolean;
            case 0xca: return Kind::f32;
            case 0xcb: return Kind::f64;
            case 0xc4:
            case 0xc5:
            case 0xc6: return Kind::bin;
            default: return Kind::invalid;
        }
    }

    bool read_nil() {
        if (peek() != Kind::nil) {
            return fail();
        }
        ++pos_;
        return true;
    }

    bool read_bool(bool& out) {
        if (peek() != Kind::boolean) {
            return fail();
        }
        out = data_[pos_++] == 0xc3;
        return true;
    }

    // Reads any integer encoding into a signed 64-bit value. Unsigned values
    // above INT64_MAX are rejected; use read_u64 for those.
    bool read_i64(std::int64_t& out) {
        bool negative = false;
        std::uint64_t magnitude = 0;
        if (!read_integer(negative, magnitude)) {
            return false;
        }
        if (negative) {
            out = static_cast<std::int64_t>(magnitude);
            return true;
        }
        if (magnitude > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
            return fail();
        }
        out = static_cast<std::int64_t>(magnitude);
        return true;
    }

    bool read_u64(std::uint64_t& out) {
        bool negative = false;
        std::uint64_t value = 0;
        if (!read_integer(negative, value) || negative) {
            return fail();
        }
        out = value;
        return true;
    }

    bool read_f64(double& out) {
        const Kind k = peek();
        if (k == Kind::f64) {
            ++pos_;
            std::uint64_t bits = 0;
            if (!get_be(bits)) {
                return false;
            }
            out = std::bit_cast<double>(bits);
            return true;
        }
        if (k == Kind::f32) {
            ++pos_;
            std::uint32_t bits = 0;
            if (!get_be(bits)) {
                return false;
            }
            out = std::bit_cast<float>(bits);
            return true;
        }
        if (k == Kind::integer) {
            std::int64_t i = 0;
            if (!read_i64(i)) {
                return false;
            }
            out = static_cast<double>(i);
            return true;
        }
        return fail();
    }

    bool read_str(std::string_view& out) {
        if (peek() != Kind::str) {
            return fail();
        }
        const std::uint8_t b = data_[pos_++];
        std::uint32_t n = 0;
        if ((b & 0xe0) == 0xa0) {
            n = b & 0x1f;
        } else if (b == 0xd9) {
            std::uint8_t v = 0;
            if (!get_be(v)) return false;
            n = v;
        } else if (b == 0xda) {
            std::uint16_t v = 0;
            if (!get_be(v)) return false;
            n = v;
        } else {
            if (!get_be(n)) return false;
        }
        if (!have(n)) {
            return fail();
        }
        out = std::string_view(reinterpret_cast<const char*>(data_.data() + pos_), n);
        pos_ += n;
        return true;
    }

    bool read_str(std::string& out) {
        std::string_view v;
        if (!read_str(v)) {
            return false;
        }
        out.assign(v);
        return true;
    }

    bool read_bin(std::vector<std::uint8_t>& out) {
        if (peek() != Kind::bin) {
            return fail();
        }
        const std::uint8_t b = data_[pos_++];
        std::uint32_t n = 0;
        if (b == 0xc4) {
            std::uint8_t v = 0;
            if (!get_be(v)) return false;
            n = v;
        } else if (b == 0xc5) {
            std::uint16_t v = 0;
            if (!get_be(v)) return false;
            n = v;
        } else {
            if (!get_be(n)) return false;
        }
        if (!have(n)) {
            return fail();
        }
        out.assign(data_.begin() + pos_, data_.begin() + pos_ + n);
        pos_ += n;
        return true;
    }

    bool read_array_header(std::uint32_t& n) { return read_container(Kind::array, 0x90, 0xdc, n); }
    bool read_map_header(std::uint32_t& n) { return read_container(Kind::map, 0x80, 0xde, n); }

    // Skips one complete value, including nested containers.
    bool skip() {
        switch (peek()) {
            case Kind::nil: return read_nil();
            case Kind::boolean: {
                bool b = false;
                return read_bool(b);
            }
            case Kind::integer: {
                bool neg = false;
                std::uint64_t v = 0;
                return read_integer(neg, v);
            }
            case Kind::f32:
            case Kind::f64: {
                double d = 0;
                return read_f64(d);
            }
            case Kind::str: {
                std::string_view s;
                return read_str(s);
            }
            case Kind::bin: {
                std::vector<std::uint8_t> b;
                return read_bin(b);
            }
            case Kind::array: {
                std::uint32_t n = 0;
                if (!read_array_header(n)) return false;
                for (std::uint32_t i = 0; i < n; ++i) {
                    if (!skip()) return false;
                }
                return true;
            }
            case Kind::map: {
                std::uint32_t n = 0;
                if (!read_map_header(n)) return false;
                for (std::uint32_t i = 0; i < 2 * n; ++i) {
                    if (!skip()) return false;
                }
                return true;
            }
            case Kind::invalid: return fail();
        }
        return fail();
    }

private:
    bool fail() {
        ok_ = false;
        return false;
    }
    bool have(std::size_t n) const { return pos_ + n <= data_.size(); }

    template <class U>
    bool get_be(U& out) {
        if (!have(sizeof(U))) {
            return fail();
        }
        U v = 0;
        for (std::size_t i = 0; i < sizeof(U); ++i) {
            v = static_cast<U>((v << 8) | data_[pos_ + i]);
        }
        pos_ += sizeof(U);
        out = v;
        return true;
    }

    bool read_integer(bool& negative, std::uint64_t& out) {
        if (peek() != Kind::integer) {
            return fail();
        }
        const std::uint8_t b = data_[pos_++];
        negative = false;
        if (b <= 0x7f) {
            out = b;
            return true;
        }
        if (b >= 0xe0) {
            negative = true;
            out = static_cast<std::uint64_t>(static_cast<std::int64_t>(static_cast<std::int8_t>(b)));
            return true;
        }
        switch (b) {
            case 0xcc: {
                std::uint8_t v = 0;
                if (!get_be(v)) return false;
                out = v;
                return true;
            }
            case 0xcd: {
                std::uint16_t v = 0;
                if (!get_be(v)) return false;
                out = v;
                return true;
            }
            case 0xce: {
                std::uint32_t v = 0;
                if (!get_be(v)) return false;
                out = v;
                return true;
            }
            case 0xcf: return get_be(out);
            case 0xd0: {
                std::uint8_t v = 0;
                if (!get_be(v)) return false;
                return signed_result(static_cast<std::int8_t>(v), negative, out);
            }
            case 0xd1: {
                std::uint16_t v = 0;
                if (!get_be(v)) return false;
                return signed_result(static_cast<std::int16_t>(v), negative, out);
            }
            case 0xd2: {
                std::uint32_t v = 0;
                if (!get_be(v)) return false;
                return signed_result(static_cast<std::int32_t>(v), negative, out);
            }
            case 0xd3: {
                std::uint64_t v = 0;
                if (!get_be(v)) return false;
                return signed_result(static_cast<std::int64_t>(v), negative, out);
            }
            default: return fail();
        }
    }

    static bool signed_result(std::int64_t v, bool& negative, std::uint64_t& out) {
        negative = v < 0;
        out = static_cast<std::uint64_t>(v);
        return true;
    }

    bool read_container(Kind kind, std::uint8_t fix_base, std::uint8_t first_wide, std::uint32_t& n) {
        if (peek() != kind) {
            return fail();
        }
        const std::uint8_t b = data_[pos_++];
        if ((b & 0xf0) == fix_base) {
            n = b & 0x0f;
            return true;
        }
        if (b == first_wide) {
            std::uint16_t v = 0;
            if (!get_be(v)) return false;
            n = v;
            return true;
        }
        return get_be(n);
    }

    std::span<const std::uint8_t> data_;
    std::size_t pos_ = 0;
    bool ok_ = true;
};

// ---------------------------------------------------------------------------
// Generic value helpers for standard types. Class and enum types are handed
// to paglets_encode / paglets_decode, found by argument-dependent lookup;
// generated schema code provides those for guest message types.

template <class T>
struct is_vector : std::false_type {};
template <class T, class A>
struct is_vector<std::vector<T, A>> : std::true_type {};

template <class T>
struct is_optional : std::false_type {};
template <class T>
struct is_optional<std::optional<T>> : std::true_type {};

template <class T>
void write_value(Writer& w, const T& v) {
    if constexpr (std::is_same_v<T, bool>) {
        w.write_bool(v);
    } else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
        w.write_int(v);
    } else if constexpr (std::is_integral_v<T>) {
        w.write_uint(v);
    } else if constexpr (std::is_same_v<T, float>) {
        w.write_f32(v);
    } else if constexpr (std::is_floating_point_v<T>) {
        w.write_f64(static_cast<double>(v));
    } else if constexpr (std::is_same_v<T, std::string>) {
        w.write_str(v);
    } else if constexpr (std::is_same_v<T, std::vector<std::uint8_t>>) {
        w.write_bin(v);
    } else if constexpr (is_vector<T>::value) {
        w.write_array_header(v.size());
        for (const auto& item : v) {
            write_value(w, item);
        }
    } else if constexpr (is_optional<T>::value) {
        if (v) {
            write_value(w, *v);
        } else {
            w.write_nil();
        }
    } else {
        paglets_encode(w, v);
    }
}

template <class T>
bool read_value(Reader& r, T& v) {
    if constexpr (std::is_same_v<T, bool>) {
        return r.read_bool(v);
    } else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
        std::int64_t i = 0;
        if (!r.read_i64(i) || i < std::numeric_limits<T>::min() || i > std::numeric_limits<T>::max()) {
            return false;
        }
        v = static_cast<T>(i);
        return true;
    } else if constexpr (std::is_integral_v<T>) {
        std::uint64_t u = 0;
        if (!r.read_u64(u) || u > std::numeric_limits<T>::max()) {
            return false;
        }
        v = static_cast<T>(u);
        return true;
    } else if constexpr (std::is_floating_point_v<T>) {
        double d = 0;
        if (!r.read_f64(d)) {
            return false;
        }
        v = static_cast<T>(d);
        return true;
    } else if constexpr (std::is_same_v<T, std::string>) {
        return r.read_str(v);
    } else if constexpr (std::is_same_v<T, std::vector<std::uint8_t>>) {
        return r.read_bin(v);
    } else if constexpr (is_vector<T>::value) {
        std::uint32_t n = 0;
        if (!r.read_array_header(n)) {
            return false;
        }
        v.clear();
        v.resize(n);
        for (auto& item : v) {
            if (!read_value(r, item)) {
                return false;
            }
        }
        return true;
    } else if constexpr (is_optional<T>::value) {
        if (r.peek() == Kind::nil) {
            v.reset();
            return r.read_nil();
        }
        typename T::value_type inner{};
        if (!read_value(r, inner)) {
            return false;
        }
        v = std::move(inner);
        return true;
    } else {
        return paglets_decode(r, v);
    }
}

}  // namespace paglets::msgpack
