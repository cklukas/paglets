// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/wire/json.hpp>

#include <array>
#include <cmath>
#include <cstdio>

namespace paglets::wire {

Json::Json(Object members) : type_(Type::object) {
    keys_.reserve(members.size());
    items_.reserve(members.size());
    for (auto& [key, value] : members) {
        keys_.push_back(std::move(key));
        items_.push_back(std::move(value));
    }
}

Json Json::integer(std::int64_t v) {
    return number_token(std::to_string(v));
}

Json Json::unsigned_integer(std::uint64_t v) {
    return number_token(std::to_string(v));
}

Json Json::number(double v) {
    if (!std::isfinite(v)) {
        return Json();  // JSON has no NaN/Infinity
    }
    std::array<char, 64> buf{};
    auto [p, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), v);
    return number_token(std::string(buf.data(), p));
}

Json Json::number_token(std::string token) {
    Json j;
    j.type_ = Type::number;
    j.text_ = std::move(token);
    return j;
}

const Json* Json::find(std::string_view key) const {
    if (type_ != Type::object) {
        return nullptr;
    }
    for (std::size_t i = 0; i < keys_.size(); ++i) {
        if (keys_[i] == key) {
            return &items_[i];
        }
    }
    return nullptr;
}

namespace {

void dump_string(std::string& out, std::string_view s) {
    out.push_back('"');
    for (const char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof buf, "\\u%04x", static_cast<unsigned>(c));
                    out += buf;
                } else {
                    out.push_back(c);
                }
        }
    }
    out.push_back('"');
}

class Parser {
public:
    explicit Parser(std::string_view text) : s_(text) {}

    std::optional<Json> parse_document() {
        auto v = parse_value(0);
        skip_ws();
        if (!v || pos_ != s_.size()) {
            return std::nullopt;
        }
        return v;
    }

private:
    static constexpr int max_depth = 256;

    void skip_ws() {
        while (pos_ < s_.size() && (s_[pos_] == ' ' || s_[pos_] == '\n' || s_[pos_] == '\r' || s_[pos_] == '\t')) {
            ++pos_;
        }
    }

    bool consume(std::string_view word) {
        if (s_.substr(pos_, word.size()) == word) {
            pos_ += word.size();
            return true;
        }
        return false;
    }

    std::optional<Json> parse_value(int depth) {
        if (depth > max_depth) {
            return std::nullopt;
        }
        skip_ws();
        if (pos_ >= s_.size()) {
            return std::nullopt;
        }
        const char c = s_[pos_];
        if (c == '{') return parse_object(depth);
        if (c == '[') return parse_array(depth);
        if (c == '"') {
            auto str = parse_string();
            if (!str) return std::nullopt;
            return Json(std::move(*str));
        }
        if (consume("true")) return Json(true);
        if (consume("false")) return Json(false);
        if (consume("null")) return Json();
        return parse_number();
    }

    std::optional<Json> parse_number() {
        const std::size_t start = pos_;
        if (pos_ < s_.size() && s_[pos_] == '-') ++pos_;
        bool digits = false;
        while (pos_ < s_.size()) {
            const char c = s_[pos_];
            if ((c >= '0' && c <= '9')) {
                digits = true;
            } else if (c != '.' && c != 'e' && c != 'E' && c != '+' && c != '-') {
                break;
            }
            ++pos_;
        }
        if (!digits) {
            return std::nullopt;
        }
        std::string token(s_.substr(start, pos_ - start));
        double check = 0;
        auto [p, ec] = std::from_chars(token.data(), token.data() + token.size(), check);
        if (ec != std::errc() && ec != std::errc::result_out_of_range) {
            return std::nullopt;
        }
        if (p != token.data() + token.size()) {
            return std::nullopt;
        }
        return Json::number_token(std::move(token));
    }

    static void append_utf8(std::string& out, std::uint32_t cp) {
        if (cp < 0x80) {
            out.push_back(static_cast<char>(cp));
        } else if (cp < 0x800) {
            out.push_back(static_cast<char>(0xc0 | (cp >> 6)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        } else if (cp < 0x10000) {
            out.push_back(static_cast<char>(0xe0 | (cp >> 12)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        } else {
            out.push_back(static_cast<char>(0xf0 | (cp >> 18)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3f)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        }
    }

    std::optional<std::uint32_t> parse_hex4() {
        if (pos_ + 4 > s_.size()) return std::nullopt;
        std::uint32_t v = 0;
        auto [p, ec] = std::from_chars(s_.data() + pos_, s_.data() + pos_ + 4, v, 16);
        if (ec != std::errc() || p != s_.data() + pos_ + 4) return std::nullopt;
        pos_ += 4;
        return v;
    }

    std::optional<std::string> parse_string() {
        ++pos_;  // opening quote
        std::string out;
        while (pos_ < s_.size()) {
            const char c = s_[pos_++];
            if (c == '"') {
                return out;
            }
            if (c != '\\') {
                out.push_back(c);
                continue;
            }
            if (pos_ >= s_.size()) return std::nullopt;
            const char e = s_[pos_++];
            switch (e) {
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                case '/': out.push_back('/'); break;
                case 'b': out.push_back('\b'); break;
                case 'f': out.push_back('\f'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                case 'u': {
                    auto cp = parse_hex4();
                    if (!cp) return std::nullopt;
                    std::uint32_t code = *cp;
                    if (code >= 0xd800 && code <= 0xdbff) {
                        if (!consume("\\u")) return std::nullopt;
                        auto low = parse_hex4();
                        if (!low || *low < 0xdc00 || *low > 0xdfff) return std::nullopt;
                        code = 0x10000 + ((code - 0xd800) << 10) + (*low - 0xdc00);
                    }
                    append_utf8(out, code);
                    break;
                }
                default: return std::nullopt;
            }
        }
        return std::nullopt;
    }

    std::optional<Json> parse_array(int depth) {
        ++pos_;
        Json::Array items;
        skip_ws();
        if (consume("]")) return Json(std::move(items));
        while (true) {
            auto v = parse_value(depth + 1);
            if (!v) return std::nullopt;
            items.push_back(std::move(*v));
            skip_ws();
            if (consume("]")) return Json(std::move(items));
            if (!consume(",")) return std::nullopt;
        }
    }

    std::optional<Json> parse_object(int depth) {
        ++pos_;
        Json::Object members;
        skip_ws();
        if (consume("}")) return Json(std::move(members));
        while (true) {
            skip_ws();
            if (pos_ >= s_.size() || s_[pos_] != '"') return std::nullopt;
            auto key = parse_string();
            if (!key) return std::nullopt;
            skip_ws();
            if (!consume(":")) return std::nullopt;
            auto v = parse_value(depth + 1);
            if (!v) return std::nullopt;
            members.emplace_back(std::move(*key), std::move(*v));
            skip_ws();
            if (consume("}")) return Json(std::move(members));
            if (!consume(",")) return std::nullopt;
        }
    }

    std::string_view s_;
    std::size_t pos_ = 0;
};

}  // namespace

void Json::dump_to(std::string& out) const {
    switch (type_) {
        case Type::null: out += "null"; break;
        case Type::boolean: out += bool_ ? "true" : "false"; break;
        case Type::number: out += text_; break;
        case Type::string: dump_string(out, text_); break;
        case Type::array:
            out.push_back('[');
            for (std::size_t i = 0; i < items_.size(); ++i) {
                if (i) out.push_back(',');
                items_[i].dump_to(out);
            }
            out.push_back(']');
            break;
        case Type::object:
            out.push_back('{');
            for (std::size_t i = 0; i < items_.size(); ++i) {
                if (i) out.push_back(',');
                dump_string(out, keys_[i]);
                out.push_back(':');
                items_[i].dump_to(out);
            }
            out.push_back('}');
            break;
    }
}

std::string Json::dump() const {
    std::string out;
    dump_to(out);
    return out;
}

std::optional<Json> Json::parse(std::string_view text) {
    return Parser(text).parse_document();
}

}  // namespace paglets::wire
