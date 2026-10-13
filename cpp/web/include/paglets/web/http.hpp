// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// A small HTTP/1.1 server for local web UIs (WP34, planning/cpp-web-ui.md):
// sockets of the platform (POSIX, Winsock), no library. One request per
// connection (`Connection: close`), Content-Length bodies only, strict
// parsing with limits, an accept thread and a pool of workers with a bounded
// queue. Access control for UIs that act with the user's keys is in Guard:
// a per-start token exchanged for a cookie, Host and Origin checks.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace paglets::web {

// -- requests and responses ----------------------------------------------------

struct Limits {
    std::size_t max_head = 16 * 1024;  // request line and headers
    std::size_t max_headers = 64;
    std::size_t max_body = 1024 * 1024;
};

struct Request {
    std::string method;  // GET, HEAD, POST
    std::string target;  // as sent: path and query
    std::string path;    // decoded, without the query
    std::string query;   // raw, without '?'
    std::vector<std::pair<std::string, std::string>> headers;  // names in lower case
    std::string body;

    // The value of a header (name in any case); empty when absent.
    std::string header(std::string_view name) const;
    // A query parameter, decoded; nullopt when absent.
    std::optional<std::string> param(std::string_view name) const;
    // A cookie of the Cookie header; nullopt when absent.
    std::optional<std::string> cookie(std::string_view name) const;
};

// Parses the request line and headers (everything before the empty line,
// which `head` must not include). The body is read separately; its size is
// the Content-Length.
struct Head {
    Request request;
    std::size_t content_length = 0;
};
struct HttpError {
    int status = 400;  // 400, 405, 413, 431, 501
    std::string message;
};
std::expected<Head, HttpError> parse_head(std::string_view head, const Limits& limits = {});

// Percent-decoding for paths and query values ('+' is a space in queries).
std::optional<std::string> percent_decode(std::string_view text, bool plus_is_space);

struct Response {
    int status = 200;
    std::string content_type = "text/plain; charset=utf-8";
    std::string body;
    std::vector<std::pair<std::string, std::string>> headers;  // extra headers

    static Response text(int status, std::string body);
    static Response json(int status, std::string body);
    static Response redirect(std::string location, int status = 303);
};

std::string_view reason_phrase(int status);

// The whole response as sent: status line, headers (with the security
// headers of every response), body (omitted for HEAD).
std::string serialize(const Response& response, bool head_only);

// -- the server ----------------------------------------------------------------

struct ServerConfig {
    std::string address = "127.0.0.1";
    // Ports to try in this order; the first that binds is used (0: any).
    std::vector<std::uint16_t> ports{0};
    std::size_t workers = 4;
    std::size_t queue = 32;  // connections waiting for a worker
    std::chrono::milliseconds timeout{15000};  // per connection
    Limits limits;
};

using Handler = std::function<Response(const Request&)>;

class Server {
public:
    // Binds the first port of `config.ports` that is free.
    static std::expected<std::unique_ptr<Server>, std::string> bind(ServerConfig config);
    ~Server();
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    std::uint16_t port() const;
    const std::string& address() const;

    // Serves until stop(); called once.
    void serve(Handler handler);
    // From any thread (and signal-safe: it only sets a flag).
    void stop();

    struct Impl;

private:
    explicit Server(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

// The ports to try: an explicit port or no search gives exactly `port`;
// otherwise `port` (or the range's first), the rest of the range, then 0.
struct PortChoice {
    std::optional<std::uint16_t> port;  // given explicitly
    std::uint16_t first = 8470;         // the search range
    std::uint16_t last = 8499;
    bool search = true;
};
std::vector<std::uint16_t> port_candidates(const PortChoice& choice);

// Parses "A-B" (A <= B, both 1..65535).
std::optional<std::pair<std::uint16_t, std::uint16_t>> parse_port_range(std::string_view text);

// -- access control ------------------------------------------------------------

// For a server that acts for the person who started it:
//  - a token per start, in the URL the browser opens (`/?t=TOKEN`), is
//    exchanged once for an HttpOnly, SameSite=Strict cookie; API requests
//    need the cookie or `Authorization: Bearer TOKEN`;
//  - the Host header must name the server (no DNS rebinding) unless remote
//    access is allowed;
//  - requests that change something (POST) need a JSON body and, when they
//    carry an Origin, the server's own.
class Guard {
public:
    Guard(std::string token, std::uint16_t port, bool allow_remote = false);

    const std::string& token() const { return token_; }
    static constexpr std::string_view cookie_name = "paglets_web";

    // A response that refuses the request, or nullopt when it may proceed.
    // `api`: the route needs the token.
    std::optional<Response> check(const Request& request, bool api) const;
    // For `/?t=TOKEN`: a redirect that sets the cookie, or nullopt when the
    // request carries no (or a wrong) token.
    std::optional<Response> exchange(const Request& request) const;
    bool authorized(const Request& request) const;

private:
    std::string token_;
    std::uint16_t port_;
    bool allow_remote_;
};

// Constant-time comparison of secrets.
bool equal_secret(std::string_view a, std::string_view b);

// -- browser -------------------------------------------------------------------

// Opens `url` in the default browser (open, xdg-open, ShellExecute); false
// when no opener could be started.
bool open_browser(const std::string& url);

}  // namespace paglets::web
