// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The web UI's HTTP server (WP34): parsing and limits, access control, port
// search, requests over the loopback interface.

// As in test_gateway.cpp: one definition of cpp-httplib per binary.
#define CPPHTTPLIB_OPENSSL_SUPPORT

#include "test.hpp"

#include <paglets/web/http.hpp>

#include <httplib.h>

#include <thread>

namespace pw = paglets::web;
using namespace std::chrono_literals;

namespace {

std::expected<pw::Head, pw::HttpError> head(std::string_view text) {
    return pw::parse_head(text);
}

int status_of(std::string_view text) {
    auto h = head(text);
    return h ? 0 : h.error().status;
}

pw::Request request(std::string method, std::string host, std::vector<std::pair<std::string, std::string>> headers = {},
                    std::string target = "/api/mesh") {
    std::string text = method + " " + target + " HTTP/1.1\r\nHost: " + host;
    for (const auto& [k, v] : headers) text += "\r\n" + k + ": " + v;
    auto h = head(text);
    REQUIRE(h.has_value());
    return h->request;
}

// A server on a free loopback port, serving on a thread.
struct Running {
    std::unique_ptr<pw::Server> server;
    std::thread thread;

    explicit Running(pw::Handler handler, pw::ServerConfig config = {}) {
        auto s = pw::Server::bind(std::move(config));
        REQUIRE_OK(s);
        server = std::move(*s);
        thread = std::thread([this, h = std::move(handler)] { server->serve(h); });
    }
    ~Running() {
        server->stop();
        thread.join();
    }
};

}  // namespace

PAGLETS_TEST("http: request heads are parsed strictly") {
    auto h = head("GET /api/host/ab%20cd?x=1&name=a+b%21 HTTP/1.1\r\nHost: 127.0.0.1:8470\r\nX-Thing:  v \r\nCookie: a=1; paglets_web=tok");
    REQUIRE(h.has_value());
    CHECK_EQ(h->request.method, std::string("GET"));
    CHECK_EQ(h->request.path, std::string("/api/host/ab cd"));
    CHECK_EQ(h->request.query, std::string("x=1&name=a+b%21"));
    CHECK(h->request.param("name") == std::optional<std::string>("a b!"));
    CHECK(h->request.param("x") == std::optional<std::string>("1"));
    CHECK(!h->request.param("y").has_value());
    CHECK_EQ(h->request.header("x-thing"), std::string("v"));
    CHECK_EQ(h->request.header("X-THING"), std::string("v"));
    CHECK(h->request.cookie("paglets_web") == std::optional<std::string>("tok"));
    CHECK_EQ(h->content_length, 0u);

    auto post = head("POST /api/call HTTP/1.1\r\nHost: h\r\nContent-Length: 12");
    REQUIRE(post.has_value());
    CHECK_EQ(post->content_length, 12u);

    CHECK_EQ(status_of("GET / HTTP/1.1"), 400);                                 // no Host
    CHECK_EQ(status_of("GET / HTTP/1.1\r\nHost: a\r\nHost: b"), 400);          // two
    CHECK_EQ(status_of("DELETE / HTTP/1.1\r\nHost: a"), 405);
    CHECK_EQ(status_of("GET / HTTP/2\r\nHost: a"), 400);
    CHECK_EQ(status_of("GET  / HTTP/1.1\r\nHost: a"), 400);
    CHECK_EQ(status_of("GET relative HTTP/1.1\r\nHost: a"), 400);
    CHECK_EQ(status_of("GET / HTTP/1.1\nHost: a"), 400);                        // bare LF
    CHECK_EQ(status_of("GET / HTTP/1.1\r\nHost: a\r\n folded"), 400);
    CHECK_EQ(status_of("GET / HTTP/1.1\r\nHost: a\r\nBad Name: x"), 400);
    CHECK_EQ(status_of("GET / HTTP/1.1\r\nHost: a\r\nX: \x01"), 400);
    CHECK_EQ(status_of("POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked"), 501);
    CHECK_EQ(status_of("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nContent-Length: 1"), 400);
    CHECK_EQ(status_of("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: -1"), 400);
    CHECK_EQ(status_of("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 99999999"), 413);
    CHECK_EQ(status_of("GET / HTTP/1.1\r\nHost: a\r\nContent-Length: 3"), 400);  // bodies with POST only
    CHECK_EQ(status_of("GET /%zz HTTP/1.1\r\nHost: a"), 400);
    CHECK_EQ(status_of("GET /%00 HTTP/1.1\r\nHost: a"), 400);
    CHECK_EQ(status_of(std::string(20000, 'G')), 431);
    std::string many = "GET / HTTP/1.1\r\nHost: a";
    for (int i = 0; i < 70; ++i) many += "\r\nX" + std::to_string(i) + ": v";
    CHECK_EQ(status_of(many), 431);
}

PAGLETS_TEST("http: responses carry the security headers") {
    const std::string out = pw::serialize(pw::Response::json(200, "{}"), false);
    CHECK(out.starts_with("HTTP/1.1 200 OK\r\n"));
    for (std::string_view h : {"Content-Length: 2", "Connection: close", "X-Content-Type-Options: nosniff",
                               "X-Frame-Options: DENY", "Cache-Control: no-store", "script-src 'self'",
                               "frame-ancestors 'none'", "application/json"}) {
        CHECK(out.find(h) != std::string::npos);
    }
    CHECK(out.ends_with("\r\n\r\n{}"));
    CHECK(pw::serialize(pw::Response::json(200, "{}"), true).ends_with("\r\n\r\n"));
    pw::Response r = pw::Response::redirect("/x\r\nInjected: 1");
    CHECK(pw::serialize(r, false).find("\r\nInjected") == std::string::npos);
}

PAGLETS_TEST("http: the guard checks Host, Origin, JSON bodies and the token") {
    const pw::Guard g("secret-token", 8470);
    // Host must name this server.
    CHECK(!g.check(request("GET", "127.0.0.1:8470", {}, "/"), false).has_value());
    CHECK(!g.check(request("GET", "localhost:8470", {}, "/"), false).has_value());
    CHECK(!g.check(request("GET", "[::1]:8470", {}, "/"), false).has_value());
    CHECK_EQ(g.check(request("GET", "evil.example:8470", {}, "/"), false)->status, 403);
    CHECK_EQ(g.check(request("GET", "127.0.0.1:9999", {}, "/"), false)->status, 403);
    // The API needs the cookie or the bearer token.
    CHECK_EQ(g.check(request("GET", "127.0.0.1:8470"), true)->status, 401);
    CHECK(!g.check(request("GET", "127.0.0.1:8470", {{"Cookie", "paglets_web=secret-token"}}), true).has_value());
    CHECK(!g.check(request("GET", "127.0.0.1:8470", {{"Authorization", "Bearer secret-token"}}), true).has_value());
    CHECK_EQ(g.check(request("GET", "127.0.0.1:8470", {{"Cookie", "paglets_web=wrong-token!"}}), true)->status, 401);
    // POST: JSON, same origin.
    const std::vector<std::pair<std::string, std::string>> ok{{"Cookie", "paglets_web=secret-token"},
                                                              {"Content-Type", "application/json"},
                                                              {"Origin", "http://127.0.0.1:8470"}};
    CHECK(!g.check(request("POST", "127.0.0.1:8470", ok), true).has_value());
    auto with = [&](std::string k, std::string v) {
        auto h = ok;
        for (auto& [hk, hv] : h) {
            if (hk == k) hv = v;
        }
        if (std::ranges::find(h, k, &std::pair<std::string, std::string>::first) == h.end()) h.emplace_back(k, v);
        return h;
    };
    CHECK_EQ(g.check(request("POST", "127.0.0.1:8470", with("Origin", "http://evil.example")), true)->status, 403);
    CHECK_EQ(g.check(request("POST", "127.0.0.1:8470", with("Content-Type", "text/plain")), true)->status, 415);
    CHECK_EQ(g.check(request("POST", "127.0.0.1:8470", with("Sec-Fetch-Site", "cross-site")), true)->status, 403);
    // The token in the URL becomes a cookie.
    auto exchanged = g.exchange(request("GET", "127.0.0.1:8470", {}, "/?t=secret-token"));
    REQUIRE(exchanged.has_value());
    CHECK_EQ(exchanged->status, 303);
    bool cookie = false;
    for (const auto& [k, v] : exchanged->headers) {
        if (k == "Set-Cookie") {
            cookie = v.find("paglets_web=secret-token") != std::string::npos && v.find("HttpOnly") != std::string::npos &&
                     v.find("SameSite=Strict") != std::string::npos;
        }
    }
    CHECK(cookie);
    CHECK(!g.exchange(request("GET", "127.0.0.1:8470", {}, "/?t=nope")).has_value());
    // Remote access: any Host, the token still counts.
    const pw::Guard remote("secret-token", 8470, true);
    CHECK(!remote.check(request("GET", "mac.local:8470", {}, "/"), false).has_value());
    CHECK_EQ(remote.check(request("GET", "mac.local:8470"), true)->status, 401);
}

PAGLETS_TEST("http: port candidates and ranges") {
    pw::PortChoice c;
    auto all = pw::port_candidates(c);
    CHECK_EQ(all.size(), 31u);
    CHECK_EQ(all.front(), 8470);
    CHECK_EQ(all[29], 8499);
    CHECK_EQ(all.back(), 0);
    c.port = 9000;
    CHECK(pw::port_candidates(c) == std::vector<std::uint16_t>{9000});
    c.port.reset();
    c.search = false;
    CHECK(pw::port_candidates(c) == std::vector<std::uint16_t>{8470});
    CHECK(pw::parse_port_range("8000-8010") == (std::optional<std::pair<std::uint16_t, std::uint16_t>>{{8000, 8010}}));
    for (std::string_view bad : {"8010-8000", "0-10", "1-70000", "x-y", "8000", "-5"}) {
        CHECK(!pw::parse_port_range(bad).has_value());
    }
}

PAGLETS_TEST("http: the server answers over the loopback interface") {
    Running r([](const pw::Request& q) {
        if (q.path == "/fail") throw std::runtime_error("boom");
        return pw::Response::text(200, q.method + " " + q.path + " " + q.body);
    });
    const int port = r.server->port();
    CHECK(port > 0);
    httplib::Client client("127.0.0.1", port);
    client.set_read_timeout(5s);
    auto got = client.Get("/hello?x=1");
    REQUIRE(got);
    CHECK_EQ(got->status, 200);
    CHECK_EQ(got->body, std::string("GET /hello "));
    CHECK_EQ(got->get_header_value("X-Frame-Options"), std::string("DENY"));
    auto posted = client.Post("/echo", std::string(100000, 'x'), "application/json");
    REQUIRE(posted);
    CHECK_EQ(posted->body.size(), std::string("POST /echo ").size() + 100000);
    auto failed = client.Get("/fail");
    REQUIRE(failed);
    CHECK_EQ(failed->status, 500);
    auto removed = client.Delete("/x");
    REQUIRE(removed);
    CHECK_EQ(removed->status, 405);
    // Bodies above the limit are refused before they are read.
    auto big = client.Post("/echo", std::string(2 * 1024 * 1024, 'x'), "application/json");
    CHECK(!big || big->status == 413);
}

PAGLETS_TEST("http: a busy port is skipped, or refused when it was asked for") {
    Running first([](const pw::Request&) { return pw::Response::text(200, "first"); });
    const std::uint16_t busy = first.server->port();
    pw::ServerConfig only;
    only.ports = {busy};
    CHECK(!pw::Server::bind(only).has_value());
    pw::ServerConfig search;
    search.ports = {busy, 0};
    auto second = pw::Server::bind(search);
    REQUIRE_OK(second);
    CHECK((*second)->port() != busy);
    CHECK((*second)->port() > 0);
}
