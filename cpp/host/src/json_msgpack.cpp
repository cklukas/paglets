// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/wire/json_msgpack.hpp>

#include <paglets/msgpack.hpp>

namespace paglets::wire {

namespace {

void write(msgpack::Writer& w, const Json& v) {
    switch (v.type()) {
        case Json::Type::null: w.write_nil(); break;
        case Json::Type::boolean: w.write_bool(v.as_bool()); break;
        case Json::Type::number: {
            std::int64_t i = 0;
            std::uint64_t u = 0;
            double d = 0;
            if (v.number_as(i)) {
                w.write_int(i);
            } else if (v.number_as(u)) {
                w.write_uint(u);
            } else if (v.number_as(d)) {
                w.write_f64(d);
            } else {
                w.write_nil();
            }
            break;
        }
        case Json::Type::string: w.write_str(v.as_string()); break;
        case Json::Type::array:
            w.write_array_header(v.as_array().size());
            for (const auto& item : v.as_array()) write(w, item);
            break;
        case Json::Type::object:
            w.write_map_header(v.keys().size());
            for (const auto& key : v.keys()) {
                w.write_str(key);
                write(w, *v.find(key));
            }
            break;
    }
}

std::optional<Json> read(msgpack::Reader& r, int depth) {
    if (depth > 64) return std::nullopt;
    using msgpack::Kind;
    switch (r.peek()) {
        case Kind::nil:
            if (!r.read_nil()) return std::nullopt;
            return Json();
        case Kind::boolean: {
            bool b = false;
            if (!r.read_bool(b)) return std::nullopt;
            return Json(b);
        }
        case Kind::integer: {
            std::int64_t i = 0;
            msgpack::Reader probe = r;
            if (probe.read_i64(i)) {
                r = probe;
                return Json::integer(i);
            }
            std::uint64_t u = 0;
            if (!r.read_u64(u)) return std::nullopt;
            return Json::unsigned_integer(u);
        }
        case Kind::f32:
        case Kind::f64: {
            double d = 0;
            if (!r.read_f64(d)) return std::nullopt;
            return Json::number(d);
        }
        case Kind::str: {
            std::string s;
            if (!r.read_str(s)) return std::nullopt;
            return Json(std::move(s));
        }
        case Kind::bin: {
            std::vector<std::uint8_t> b;
            if (!r.read_bin(b)) return std::nullopt;
            Json::Array items;
            for (auto byte : b) items.push_back(Json::integer(byte));
            return Json(std::move(items));
        }
        case Kind::array: {
            std::uint32_t n = 0;
            if (!r.read_array_header(n)) return std::nullopt;
            Json::Array items;
            for (std::uint32_t i = 0; i < n; ++i) {
                auto item = read(r, depth + 1);
                if (!item) return std::nullopt;
                items.push_back(std::move(*item));
            }
            return Json(std::move(items));
        }
        case Kind::map: {
            std::uint32_t n = 0;
            if (!r.read_map_header(n)) return std::nullopt;
            Json::Object members;
            for (std::uint32_t i = 0; i < n; ++i) {
                std::string key;
                if (r.peek() == Kind::str) {
                    if (!r.read_str(key)) return std::nullopt;
                } else {
                    auto k = read(r, depth + 1);
                    if (!k) return std::nullopt;
                    key = k->dump();
                }
                auto value = read(r, depth + 1);
                if (!value) return std::nullopt;
                members.emplace_back(std::move(key), std::move(*value));
            }
            return Json(std::move(members));
        }
        default: return std::nullopt;
    }
}

}  // namespace

std::vector<std::uint8_t> json_to_msgpack(const Json& value) {
    msgpack::Writer w;
    write(w, value);
    return w.take();
}

std::optional<Json> msgpack_to_json(std::span<const std::uint8_t> bytes) {
    msgpack::Reader r(bytes);
    auto v = read(r, 0);
    if (!v || !r.ok() || !r.at_end()) return std::nullopt;
    return v;
}

}  // namespace paglets::wire
