// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// HTTP requests and responses, access control and port choice (WP34).

#include <paglets/web/http.hpp>

#include <algorithm>
#include <charconv>

namespace paglets::web {

namespace {

char lower(char c) {
    return c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c;
}

std::string lowered(std::string_view s) {
    std::string out(s);
    for (char& c : out) c = lower(c);
    return out;
}

bool iequal(std::string_view a, std::string_view b) {
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](char x, char y) {
               return lower(x) == lower(y);
           });
}

bool is_token_char(char c) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) return true;
    return std::string_view("!#$%&'*+-.^_`|~").find(c) != std::string_view::npos;
}

std::string_view trim(std::string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
    return s;
}

int hex_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::unexpected<HttpError> error(int status, std::string message) {
    return std::unexpected(HttpError{status, std::move(message)});
}

}  // namespace

// -- requests ------------------------------------------------------------------

std::optional<std::string> percent_decode(std::string_view text, bool plus_is_space) {
    std::string out;
    out.reserve(text.size());
    for (std::size_t i = 0; i < text.size(); ++i) {
        const char c = text[i];
        if (c == '%') {
            if (i + 2 >= text.size()) return std::nullopt;
            const int hi = hex_value(text[i + 1]);
            const int lo = hex_value(text[i + 2]);
            if (hi < 0 || lo < 0) return std::nullopt;
            const char decoded = static_cast<char>(hi * 16 + lo);
            if (decoded == '\0') return std::nullopt;
            out += decoded;
            i += 2;
        } else if (c == '+' && plus_is_space) {
            out += ' ';
        } else {
            out += c;
        }
    }
    return out;
}

std::string Request::header(std::string_view name) const {
    for (const auto& [k, v] : headers) {
        if (iequal(k, name)) return v;
    }
    return {};
}

std::optional<std::string> Request::param(std::string_view name) const {
    std::string_view q = query;
    while (!q.empty()) {
        const std::size_t amp = q.find('&');
        const std::string_view pair = q.substr(0, amp);
        q = amp == std::string_view::npos ? std::string_view() : q.substr(amp + 1);
        const std::size_t eq = pair.find('=');
        auto key = percent_decode(pair.substr(0, eq), true);
        if (!key || *key != name) continue;
        if (eq == std::string_view::npos) return std::string();
        return percent_decode(pair.substr(eq + 1), true);
    }
    return std::nullopt;
}

std::optional<std::string> Request::cookie(std::string_view name) const {
    std::string_view c;
    std::string all = header("cookie");
    c = all;
    while (!c.empty()) {
        const std::size_t semi = c.find(';');
        const std::string_view part = trim(c.substr(0, semi));
        c = semi == std::string_view::npos ? std::string_view() : c.substr(semi + 1);
        const std::size_t eq = part.find('=');
        if (eq == std::string_view::npos) continue;
        if (part.substr(0, eq) == name) return std::string(part.substr(eq + 1));
    }
    return std::nullopt;
}

std::expected<Head, HttpError> parse_head(std::string_view head, const Limits& limits) {
    if (head.size() > limits.max_head) return error(431, "request header too large");
    for (char c : head) {
        const auto u = static_cast<unsigned char>(c);
        if ((u < 0x20 && c != '\r' && c != '\n' && c != '\t') || u == 0x7f) {
            return error(400, "control character in the request header");
        }
    }
    // Lines end in CRLF; bare CR or LF are refused.
    std::vector<std::string_view> lines;
    std::size_t pos = 0;
    while (pos <= head.size()) {
        const std::size_t end = head.find("\r\n", pos);
        const std::string_view line = head.substr(pos, end == std::string_view::npos ? head.size() - pos : end - pos);
        if (line.find('\r') != std::string_view::npos || line.find('\n') != std::string_view::npos) {
            return error(400, "bare CR or LF in the request header");
        }
        lines.push_back(line);
        if (end == std::string_view::npos) break;
        pos = end + 2;
    }
    if (lines.empty() || lines.front().empty()) return error(400, "no request line");
    if (lines.size() - 1 > limits.max_headers) return error(431, "too many headers");

    Head h;
    Request& r = h.request;
    const std::string_view line = lines.front();
    const std::size_t s1 = line.find(' ');
    const std::size_t s2 = s1 == std::string_view::npos ? s1 : line.find(' ', s1 + 1);
    if (s1 == std::string_view::npos || s2 == std::string_view::npos || line.find(' ', s2 + 1) != std::string_view::npos) {
        return error(400, "malformed request line");
    }
    r.method = std::string(line.substr(0, s1));
    r.target = std::string(line.substr(s1 + 1, s2 - s1 - 1));
    const std::string_view version = line.substr(s2 + 1);
    if (version != "HTTP/1.1" && version != "HTTP/1.0") return error(400, "unsupported HTTP version");
    for (char c : r.method) {
        if (!is_token_char(c)) return error(400, "malformed method");
    }
    if (r.method != "GET" && r.method != "HEAD" && r.method != "POST") return error(405, "method not allowed");
    if (r.target.empty() || r.target.front() != '/' || r.target.size() > 8192) return error(400, "malformed target");
    const std::size_t q = r.target.find('?');
    std::string_view raw_path = std::string_view(r.target).substr(0, q);
    if (const std::size_t hash = raw_path.find('#'); hash != std::string_view::npos) raw_path = raw_path.substr(0, hash);
    auto path = percent_decode(raw_path, false);
    if (!path) return error(400, "malformed path");
    r.path = std::move(*path);
    if (q != std::string::npos) {
        r.query = r.target.substr(q + 1);
        if (const std::size_t hash = r.query.find('#'); hash != std::string::npos) r.query.resize(hash);
    }

    std::optional<std::size_t> content_length;
    int hosts = 0;
    for (std::size_t i = 1; i < lines.size(); ++i) {
        const std::string_view l = lines[i];
        if (l.empty()) return error(400, "empty header line");
        if (l.front() == ' ' || l.front() == '\t') return error(400, "folded header");
        const std::size_t colon = l.find(':');
        if (colon == std::string_view::npos || colon == 0) return error(400, "malformed header");
        const std::string_view name = l.substr(0, colon);
        for (char c : name) {
            if (!is_token_char(c)) return error(400, "malformed header name");
        }
        const std::string_view value = trim(l.substr(colon + 1));
        const std::string key = lowered(name);
        if (key == "transfer-encoding") return error(501, "transfer encodings are not supported");
        if (key == "host") ++hosts;
        if (key == "content-length") {
            if (content_length) return error(400, "Content-Length twice");
            std::size_t n = 0;
            auto [p, ec] = std::from_chars(value.data(), value.data() + value.size(), n);
            if (value.empty() || ec != std::errc() || p != value.data() + value.size()) {
                return error(400, "malformed Content-Length");
            }
            if (n > limits.max_body) return error(413, "request body too large");
            content_length = n;
        }
        r.headers.emplace_back(key, std::string(value));
    }
    if (hosts != 1) return error(400, "exactly one Host header is required");
    h.content_length = content_length.value_or(0);
    if (h.content_length > 0 && r.method != "POST") return error(400, "a body is only accepted with POST");
    return h;
}

// -- responses -----------------------------------------------------------------

Response Response::text(int status, std::string body) {
    Response r;
    r.status = status;
    r.body = std::move(body);
    return r;
}

Response Response::json(int status, std::string body) {
    Response r;
    r.status = status;
    r.content_type = "application/json; charset=utf-8";
    r.body = std::move(body);
    return r;
}

Response Response::redirect(std::string location, int status) {
    Response r;
    r.status = status;
    r.headers.emplace_back("Location", std::move(location));
    return r;
}

std::string_view reason_phrase(int status) {
    switch (status) {
        case 200: return "OK";
        case 201: return "Created";
        case 204: return "No Content";
        case 303: return "See Other";
        case 304: return "Not Modified";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 408: return "Request Timeout";
        case 413: return "Content Too Large";
        case 415: return "Unsupported Media Type";
        case 422: return "Unprocessable Content";
        case 431: return "Request Header Fields Too Large";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 502: return "Bad Gateway";
        case 503: return "Service Unavailable";
        default: return "Status";
    }
}

std::string serialize(const Response& response, bool head_only) {
    std::string out = "HTTP/1.1 " + std::to_string(response.status) + " " + std::string(reason_phrase(response.status)) +
                      "\r\n";
    auto add = [&](std::string_view k, std::string_view v) {
        out += k;
        out += ": ";
        // Header values never carry line breaks.
        for (char c : v) out += (c == '\r' || c == '\n') ? ' ' : c;
        out += "\r\n";
    };
    add("Content-Type", response.content_type);
    add("Content-Length", std::to_string(response.body.size()));
    add("Connection", "close");
    add("Cache-Control", "no-store");
    add("X-Content-Type-Options", "nosniff");
    add("X-Frame-Options", "DENY");
    add("Referrer-Policy", "no-referrer");
    add("Cross-Origin-Opener-Policy", "same-origin");
    add("Cross-Origin-Resource-Policy", "same-origin");
    add("Content-Security-Policy",
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; "
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'");
    for (const auto& [k, v] : response.headers) add(k, v);
    out += "\r\n";
    if (!head_only) out += response.body;
    return out;
}

// -- ports ---------------------------------------------------------------------

std::vector<std::uint16_t> port_candidates(const PortChoice& choice) {
    // An explicit port is used as given (0: any free port), without search.
    if (choice.port) return {*choice.port};
    if (!choice.search) return {choice.first};
    std::vector<std::uint16_t> out;
    for (std::uint32_t p = choice.first; p <= choice.last; ++p) out.push_back(static_cast<std::uint16_t>(p));
    out.push_back(0);
    return out;
}

std::optional<std::pair<std::uint16_t, std::uint16_t>> parse_port_range(std::string_view text) {
    const std::size_t dash = text.find('-');
    if (dash == std::string_view::npos) return std::nullopt;
    auto number = [](std::string_view s) -> std::optional<std::uint16_t> {
        unsigned v = 0;
        auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
        if (s.empty() || ec != std::errc() || p != s.data() + s.size() || v == 0 || v > 65535) return std::nullopt;
        return static_cast<std::uint16_t>(v);
    };
    auto a = number(text.substr(0, dash));
    auto b = number(text.substr(dash + 1));
    if (!a || !b || *a > *b) return std::nullopt;
    return std::pair{*a, *b};
}

// -- access control ------------------------------------------------------------

bool equal_secret(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    unsigned char diff = 0;
    for (std::size_t i = 0; i < a.size(); ++i) diff |= static_cast<unsigned char>(a[i] ^ b[i]);
    return diff == 0;
}

Guard::Guard(std::string token, std::uint16_t port, bool allow_remote)
    : token_(std::move(token)), port_(port), allow_remote_(allow_remote) {}

bool Guard::authorized(const Request& request) const {
    if (auto c = request.cookie(cookie_name); c && equal_secret(*c, token_)) return true;
    const std::string auth = request.header("authorization");
    constexpr std::string_view bearer = "Bearer ";
    return auth.starts_with(bearer) && equal_secret(std::string_view(auth).substr(bearer.size()), token_);
}

std::optional<Response> Guard::check(const Request& request, bool api) const {
    const std::string host = request.header("host");
    if (!allow_remote_) {
        const std::string p = std::to_string(port_);
        if (host != "127.0.0.1:" + p && host != "localhost:" + p && host != "[::1]:" + p) {
            return Response::text(403, "forbidden: unexpected Host header\n");
        }
    }
    if (request.method == "POST") {
        const std::string origin = request.header("origin");
        if (!origin.empty() && origin != "http://" + host) return Response::text(403, "forbidden: cross-origin request\n");
        const std::string type = request.header("content-type");
        if (!type.starts_with("application/json")) {
            return Response::text(415, "requests that change something send JSON\n");
        }
        // Fetch metadata, where browsers send it.
        const std::string site = request.header("sec-fetch-site");
        if (!site.empty() && site != "same-origin" && site != "none") {
            return Response::text(403, "forbidden: cross-site request\n");
        }
    }
    if (api && !authorized(request)) {
        return Response::json(401, R"({"error":"not signed in: open the URL that paglets-web printed"})");
    }
    return std::nullopt;
}

std::optional<Response> Guard::exchange(const Request& request) const {
    auto t = request.param("t");
    if (!t || !equal_secret(*t, token_)) return std::nullopt;
    Response r = Response::redirect("/");
    r.headers.emplace_back("Set-Cookie", std::string(cookie_name) + "=" + token_ +
                                             "; Path=/; HttpOnly; SameSite=Strict");
    return r;
}

}  // namespace paglets::web
