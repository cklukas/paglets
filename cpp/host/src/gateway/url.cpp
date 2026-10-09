// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// URLs and destinations of the gateway system paglets: parsing, resolving
// relative references, and telling internal addresses from public ones
// (planning/cpp-web-ai.md, section 2).

#include "gateway_impl.hpp"

#include <algorithm>
#include <cctype>
#include <array>
#include <charconv>
#include <cstring>

namespace paglets::gateway {

namespace {

std::string lower(std::string_view s) {
    std::string out(s);
    std::ranges::transform(out, out.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

std::optional<std::array<std::uint8_t, 4>> parse_ipv4(std::string_view s) {
    std::array<std::uint8_t, 4> out{};
    for (int i = 0; i < 4; ++i) {
        if (i > 0) {
            if (s.empty() || s.front() != '.') return std::nullopt;
            s.remove_prefix(1);
        }
        unsigned v = 0;
        auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
        if (ec != std::errc() || p == s.data() || v > 255 || (p - s.data() > 1 && s.front() == '0')) return std::nullopt;
        out[static_cast<std::size_t>(i)] = static_cast<std::uint8_t>(v);
        s.remove_prefix(static_cast<std::size_t>(p - s.data()));
    }
    if (!s.empty()) return std::nullopt;
    return out;
}

std::optional<std::array<std::uint8_t, 16>> parse_ipv6(std::string_view s) {
    // Groups before and after "::"; an IPv4 tail counts as two groups.
    std::array<std::uint16_t, 8> head{}, tail{};
    std::size_t nh = 0, nt = 0;
    bool gap = false;
    auto parse_groups = [](std::string_view part, std::array<std::uint16_t, 8>& out, std::size_t& n, bool last) {
        if (part.empty()) return true;
        while (true) {
            const auto colon = part.find(':');
            std::string_view g = part.substr(0, colon);
            if (colon == std::string_view::npos && last && g.find('.') != std::string_view::npos) {
                auto v4 = parse_ipv4(g);
                if (!v4 || n + 2 > 8) return false;
                out[n++] = static_cast<std::uint16_t>((*v4)[0] << 8 | (*v4)[1]);
                out[n++] = static_cast<std::uint16_t>((*v4)[2] << 8 | (*v4)[3]);
                return true;
            }
            if (g.empty() || g.size() > 4 || n >= 8) return false;
            unsigned v = 0;
            auto [p, ec] = std::from_chars(g.data(), g.data() + g.size(), v, 16);
            if (ec != std::errc() || p != g.data() + g.size()) return false;
            out[n++] = static_cast<std::uint16_t>(v);
            if (colon == std::string_view::npos) return true;
            part.remove_prefix(colon + 1);
        }
    };
    const auto dc = s.find("::");
    if (dc != std::string_view::npos) {
        gap = true;
        if (!parse_groups(s.substr(0, dc), head, nh, false) || !parse_groups(s.substr(dc + 2), tail, nt, true)) {
            return std::nullopt;
        }
        if (nh + nt > 7) return std::nullopt;
    } else if (!parse_groups(s, head, nh, true) || nh != 8) {
        return std::nullopt;
    }
    std::array<std::uint16_t, 8> groups{};
    for (std::size_t i = 0; i < nh; ++i) groups[i] = head[i];
    if (gap) {
        for (std::size_t i = 0; i < nt; ++i) groups[8 - nt + i] = tail[i];
    }
    std::array<std::uint8_t, 16> out{};
    for (std::size_t i = 0; i < 8; ++i) {
        out[2 * i] = static_cast<std::uint8_t>(groups[i] >> 8);
        out[2 * i + 1] = static_cast<std::uint8_t>(groups[i] & 0xff);
    }
    return out;
}

bool internal_v4(const std::array<std::uint8_t, 4>& a) {
    return a[0] == 0 || a[0] == 10 || a[0] == 127 ||                // this network, private, loopback
           (a[0] == 100 && (a[1] & 0xc0) == 64) ||                  // shared address space (100.64/10)
           (a[0] == 169 && a[1] == 254) ||                          // link-local
           (a[0] == 172 && (a[1] & 0xf0) == 16) ||                  // private
           (a[0] == 192 && a[1] == 0 && (a[2] == 0 || a[2] == 2)) ||  // IETF protocol, TEST-NET-1
           (a[0] == 192 && a[1] == 168) ||                          // private
           (a[0] == 198 && (a[1] & 0xfe) == 18) ||                  // benchmarking
           (a[0] == 198 && a[1] == 51 && a[2] == 100) ||            // TEST-NET-2
           (a[0] == 203 && a[1] == 0 && a[2] == 113) ||             // TEST-NET-3
           a[0] >= 224;                                             // multicast, reserved, broadcast
}

}  // namespace

bool internal_address(std::string_view address) {
    if (address.size() >= 2 && address.front() == '[' && address.back() == ']') address = address.substr(1, address.size() - 2);
    if (auto pct = address.find('%'); pct != std::string_view::npos) address = address.substr(0, pct);  // zone
    if (auto v4 = parse_ipv4(address)) return internal_v4(*v4);
    auto v6 = parse_ipv6(address);
    if (!v6) return true;  // not an address: never treated as public
    const auto& b = *v6;
    const bool zero_prefix = std::all_of(b.begin(), b.begin() + 10, [](std::uint8_t x) { return x == 0; });
    if (zero_prefix && b[10] == 0xff && b[11] == 0xff) return internal_v4({b[12], b[13], b[14], b[15]});  // mapped
    if (std::all_of(b.begin(), b.begin() + 12, [](std::uint8_t x) { return x == 0; })) return true;  // ::, ::1, compat
    if ((b[0] & 0xfe) == 0xfc) return true;                // unique local fc00::/7
    if (b[0] == 0xfe && (b[1] & 0xc0) == 0x80) return true;  // link-local fe80::/10
    if (b[0] == 0xfe && (b[1] & 0xc0) == 0xc0) return true;  // site-local (deprecated)
    if (b[0] == 0xff) return true;                         // multicast
    if (b[0] == 0x20 && b[1] == 0x01 && b[2] == 0x0d && b[3] == 0xb8) return true;  // documentation
    if (b[0] == 0x00 && b[1] == 0x64 && b[2] == 0xff && b[3] == 0x9b) {           // NAT64: the IPv4 inside
        return internal_v4({b[12], b[13], b[14], b[15]});
    }
    if (b[0] == 0x20 && b[1] == 0x02) return internal_v4({b[2], b[3], b[4], b[5]});  // 6to4
    return false;
}

std::optional<Url> parse_url(std::string_view text) {
    if (text.size() > 8192) return std::nullopt;
    for (char c : text) {
        if (static_cast<unsigned char>(c) <= 0x20 || c == 0x7f) return std::nullopt;
    }
    Url u;
    const auto colon = text.find("://");
    if (colon == std::string_view::npos) return std::nullopt;
    u.scheme = lower(text.substr(0, colon));
    if (u.scheme != "http" && u.scheme != "https") return std::nullopt;
    std::string_view rest = text.substr(colon + 3);
    const auto end = rest.find_first_of("/?#");
    std::string_view authority = rest.substr(0, end);
    rest = end == std::string_view::npos ? std::string_view() : rest.substr(end);
    if (authority.find('@') != std::string_view::npos) return std::nullopt;  // no credentials in URLs
    std::string_view port;
    if (authority.starts_with('[')) {
        const auto close = authority.find(']');
        if (close == std::string_view::npos) return std::nullopt;
        u.host = lower(authority.substr(0, close + 1));
        std::string_view after = authority.substr(close + 1);
        if (!after.empty()) {
            if (after.front() != ':') return std::nullopt;
            port = after.substr(1);
        }
        if (!parse_ipv6(std::string_view(u.host).substr(1, u.host.size() - 2))) return std::nullopt;
    } else {
        const auto c = authority.rfind(':');
        u.host = lower(authority.substr(0, c));
        if (c != std::string_view::npos) port = authority.substr(c + 1);
    }
    if (u.host.empty() || u.host.size() > 253) return std::nullopt;
    for (char c : u.host) {
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '.' || c == '-' || c == '_' || c == '[' || c == ']' ||
              c == ':')) {
            return std::nullopt;
        }
    }
    u.port = u.scheme == "https" ? 443 : 80;
    if (!port.empty()) {
        int p = 0;
        auto [ptr, ec] = std::from_chars(port.data(), port.data() + port.size(), p);
        if (ec != std::errc() || ptr != port.data() + port.size() || p < 1 || p > 65535) return std::nullopt;
        u.port = p;
    }
    if (auto hash = rest.find('#'); hash != std::string_view::npos) rest = rest.substr(0, hash);
    u.target = rest.empty() || rest.front() == '?' ? "/" + std::string(rest) : std::string(rest);
    const auto q = u.target.find('?');
    u.path = u.target.substr(0, q);
    return u;
}

std::string Url::origin() const {
    const bool default_port = (scheme == "https" && port == 443) || (scheme == "http" && port == 80);
    return scheme + "://" + host + (default_port ? "" : ":" + std::to_string(port));
}

std::string Url::text() const {
    return origin() + target;
}

std::string resolve_url(std::string_view base_text, std::string_view ref) {
    while (!ref.empty() && (ref.front() == ' ' || ref.front() == '\t' || ref.front() == '\n')) ref.remove_prefix(1);
    while (!ref.empty() && (ref.back() == ' ' || ref.back() == '\t' || ref.back() == '\n')) ref.remove_suffix(1);
    if (auto hash = ref.find('#'); hash != std::string_view::npos) ref = ref.substr(0, hash);
    if (ref.find("://") != std::string_view::npos) {
        auto u = parse_url(ref);
        return u ? u->text() : std::string();
    }
    auto base = parse_url(base_text);
    if (!base) return {};
    if (ref.starts_with("//")) {
        auto u = parse_url(base->scheme + ":" + std::string(ref));
        return u ? u->text() : std::string();
    }
    if (ref.empty()) return base->text();
    const std::string scheme_like(ref.substr(0, std::min<std::size_t>(ref.size(), 12)));
    if (scheme_like.find(':') != std::string::npos && scheme_like.find('/') == std::string::npos &&
        scheme_like.find('?') == std::string::npos) {
        return {};  // mailto:, javascript: and the like
    }
    std::string path;
    if (ref.front() == '/') {
        path = std::string(ref);
    } else if (ref.front() == '?') {
        path = base->path + std::string(ref);
    } else {
        const auto slash = base->path.rfind('/');
        path = base->path.substr(0, slash + 1) + std::string(ref);
    }
    // Removes dot segments (RFC 3986, 5.2.4) from the path part.
    const auto q = path.find('?');
    std::string query = q == std::string::npos ? std::string() : path.substr(q);
    std::string p = path.substr(0, q);
    std::vector<std::string> segments;
    std::size_t start = 1;
    while (start <= p.size()) {
        const auto next = p.find('/', start);
        std::string seg = p.substr(start, next == std::string::npos ? std::string::npos : next - start);
        const bool last = next == std::string::npos;
        if (seg == "..") {
            if (!segments.empty()) segments.pop_back();
            if (last) segments.emplace_back();
        } else if (seg == ".") {
            if (last) segments.emplace_back();
        } else {
            segments.push_back(seg);
        }
        if (last) break;
        start = next + 1;
    }
    std::string out;
    for (const auto& s : segments) out += "/" + s;
    if (out.empty()) out = "/";
    auto u = parse_url(base->origin() + out + query);
    return u ? u->text() : std::string();
}

std::string url_encode(std::string_view s) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            out += static_cast<char>(c);
        } else {
            out += '%';
            out += hex[c >> 4];
            out += hex[c & 15];
        }
    }
    return out;
}

}  // namespace paglets::gateway
