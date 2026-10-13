// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-web end to end (WP36, planning/cpp-web-ui.md): two hosts over
// HTTPS, the web UI's model and JSON API over HTTP, as a browser and curl
// use them: the mesh with its counts, a launch with start arguments,
// call, send, describe, dispose; refusals without the token, from another
// origin and for another Host.

// As in test_gateway.cpp: one definition of cpp-httplib per binary.
#define CPPHTTPLIB_OPENSSL_SUPPORT

#include "mesh_fixture.hpp"
#include "test.hpp"

#include "app.hpp"

#include <httplib.h>
#include <paglets/wasm/engine.hpp>

#include <filesystem>
#include <random>
#include <thread>

using namespace paglets::test;
using namespace std::chrono_literals;
using paglets::wire::Json;
namespace pw = paglets::web;
namespace pc = paglets::client;

namespace {

Json parse(const std::string& text) {
    auto j = Json::parse(text);
    REQUIRE(j.has_value());
    return *j;
}

std::string member(const Json& j, std::string_view k) {
    const Json* v = j.find(k);
    return v != nullptr && v->is_string() ? v->as_string() : std::string();
}

std::int64_t number(const Json& j, std::string_view k) {
    const Json* v = j.find(k);
    std::int64_t n = 0;
    if (v != nullptr) (void)v->number_as(n);
    return n;
}

std::string base64(const std::vector<std::uint8_t>& bytes) {
    static constexpr char alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    std::size_t i = 0;
    for (; i + 2 < bytes.size(); i += 3) {
        const std::uint32_t v = (bytes[i] << 16) | (bytes[i + 1] << 8) | bytes[i + 2];
        for (int s = 18; s >= 0; s -= 6) out += alphabet[(v >> s) & 63];
    }
    if (i + 1 == bytes.size()) {
        const std::uint32_t v = bytes[i] << 16;
        out += alphabet[(v >> 18) & 63];
        out += alphabet[(v >> 12) & 63];
        out += "==";
    } else if (i + 2 == bytes.size()) {
        const std::uint32_t v = (bytes[i] << 16) | (bytes[i + 1] << 8);
        out += alphabet[(v >> 18) & 63];
        out += alphabet[(v >> 12) & 63];
        out += alphabet[(v >> 6) & 63];
        out += '=';
    }
    return out;
}

}  // namespace

PAGLETS_TEST("web: the UI shows the mesh, launches, calls and ends paglets") {
    const auto counter = guest_path("counter.wasm");
    if (counter.empty()) skip("counter.wasm not available");

    // Two hosts (a, b) that know each other's addresses; an owner, olga.
    SigningKey admin = SigningKey::generate();
    std::array<std::uint8_t, 32> seed{};
    std::random_device random;
    for (auto& b : seed) b = static_cast<std::uint8_t>(random());
    auto owner = SigningKey::from_seed(seed);
    REQUIRE_OK(owner);
    auto genesis = make_genesis("web", {&admin}, {}, 1);
    REQUIRE_OK(genesis);
    NetHost a(*genesis);
    NetHost b(*genesis);
    std::vector<Record> records;
    auto record = [&](const std::string& type, Map d) {
        auto r = a.node->draft(type, std::move(d));
        REQUIRE_OK(r);
        r->sign(admin);
        records.push_back(*r);
        REQUIRE_OK(a.node->submit(*r));
        REQUIRE_OK(b.node->submit(*r));
    };
    record("host-enroll", data::host_enroll(a.node->host(), "a", {"arm"}));
    record("host-enroll", data::host_enroll(b.node->host(), "b", {}));
    record("owner-enroll", data::owner_enroll(owner->public_key(), "olga", {}));
    a.transport->set_address(b.node->host(), b.transport->url());
    b.transport->set_address(a.node->host(), a.transport->url());
    a.node->add_seed(b.node->host());
    b.node->add_seed(a.node->host());
    auto known = [&] {
        for (const auto& h : a.node->hosts()) {
            if (h.key == b.node->host()) return h.online && !h.url.empty();
        }
        return false;
    };
    for (int i = 0; i < 1500 && !known(); ++i) {
        a.node->tick();
        b.node->tick();
        std::this_thread::sleep_for(10ms);
    }
    REQUIRE(known());

    // The owner's ledger copy and identity, as paglets-web opens them.
    auto ledger = Ledger::create(*genesis);
    REQUIRE_OK(ledger);
    for (const auto& r : records) REQUIRE_OK(ledger->add(r));
    auto key = SigningKey::from_seed(seed);
    REQUIRE_OK(key);
    pc::Identity identity{std::move(*ledger), std::move(*key), paglets::net::PeerRole::owner, "olga"};
    paglets::webapp::Options options;
    options.connect = a.transport->url();
    options.key_file = "olga.key";
    options.ledger_dir = "ledger";
    options.module_dir = std::filesystem::path(counter).parent_path();
    paglets::webapp::App app(options, std::move(identity));
    app.refresh();

    // The model: both hosts, a first (the entry host), with their status.
    const Json model = parse(app.model());
    CHECK_EQ(member(model, "error"), std::string());
    const auto& hosts = model.find("hosts")->as_array();
    REQUIRE(hosts.size() == 2u);
    CHECK_EQ(member(hosts[0], "name"), std::string("a"));
    CHECK(hosts[0].find("entry")->as_bool());
    CHECK_EQ(member(hosts[1], "name"), std::string("b"));
    const std::string b_key = member(hosts[1], "key");
    CHECK(!hosts[1].find("status")->is_null());
    CHECK(number(*hosts[1].find("counts"), "system") > 0);
    CHECK_EQ(number(*model.find("totals"), "hosts"), 2);
    CHECK(member(*hosts[0].find("shell"), "status").starts_with("paglets-host remote status --connect "));

    // The server, as paglets-web runs it.
    pw::ServerConfig config;
    auto server = pw::Server::bind(config);
    REQUIRE_OK(server);
    const int port = (*server)->port();
    const pw::Guard guard("test-token-0123456789", static_cast<std::uint16_t>(port));
    std::thread serving([&] { (*server)->serve([&](const pw::Request& r) { return paglets::webapp::route(app, guard, r); }); });
    struct Stop {
        pw::Server& s;
        std::thread& t;
        ~Stop() {
            s.stop();
            t.join();
        }
    } stop{**server, serving};

    httplib::Client web("127.0.0.1", port);
    web.set_read_timeout(60s);
    const httplib::Headers auth{{"Authorization", "Bearer test-token-0123456789"}};

    // The page itself needs no token; the API does.
    auto page = web.Get("/");
    REQUIRE(page);
    CHECK_EQ(page->status, 200);
    CHECK(page->body.find("/app.js") != std::string::npos);
    CHECK_EQ(web.Get("/app.js")->status, 200);
    CHECK_EQ(web.Get("/api/mesh")->status, 401);
    auto mesh = web.Get("/api/mesh", auth);
    REQUIRE(mesh);
    CHECK_EQ(mesh->status, 200);
    CHECK(mesh->body.find("\"name\":\"b\"") != std::string::npos);
    // The token in the link becomes a cookie that works for the API.
    auto signin = web.Get("/?t=test-token-0123456789");
    REQUIRE(signin);
    CHECK_EQ(signin->status, 303);
    const std::string cookie = signin->get_header_value("Set-Cookie");
    CHECK(cookie.find("HttpOnly") != std::string::npos);
    CHECK_EQ(web.Get("/api/mesh", httplib::Headers{{"Cookie", "paglets_web=test-token-0123456789"}})->status, 200);
    CHECK_EQ(web.Get("/?t=wrong")->status, 403);
    // Another Host (DNS rebinding) and another origin are refused.
    CHECK_EQ(web.Get("/api/mesh", httplib::Headers{{"Authorization", "Bearer test-token-0123456789"}, {"Host", "evil.example"}})->status,
             403);
    auto foreign = web.Post("/api/dispose", httplib::Headers{{"Authorization", "Bearer test-token-0123456789"}, {"Origin", "http://evil.example"}},
                            R"({"host":"x","paglet":"y"})", "application/json");
    REQUIRE(foreign);
    CHECK_EQ(foreign->status, 403);
    CHECK_EQ(web.Post("/api/dispose", auth, R"({"host":"x","paglet":"y"})", "text/plain")->status, 415);

    auto post = [&](const std::string& action, const std::string& body) {
        auto r = web.Post("/api/" + action, auth, body, "application/json");
        REQUIRE(r);
        return std::pair{r->status, parse(r->body)};
    };

    // Launch the counter on b from the module directory, with a start value.
    auto [launched_status, launched] =
        post("launch", R"({"host":")" + b_key + R"(","file":"counter.wasm","args":{"value":5}})");
    REQUIRE(launched_status == 200);
    const std::string paglet = member(launched, "paglet");
    CHECK_EQ(paglet.size(), 32u);
    CHECK(member(launched, "shell").find("remote launch --connect " + b.transport->url()) != std::string::npos);
    CHECK(member(launched, "shell").find(R"(--args '{"value":5}')") != std::string::npos);

    // Its interface, then a send and a call.
    auto described = web.Get("/api/paglet?host=" + b_key + "&id=" + paglet, auth);
    REQUIRE(described);
    CHECK_EQ(described->status, 200);
    const Json d = parse(described->body);
    CHECK_EQ(member(*d.find("interface"), "name"), std::string("counter"));
    auto [sent_status, sent] = post("send", R"({"host":")" + b_key + R"(","paglet":")" + paglet +
                                                R"(","name":"increment","payload":{"by":2,"note":"web"},"priority":5})");
    CHECK_EQ(sent_status, 200);
    CHECK(member(sent, "shell").find(R"(increment '{"by":2,"note":"web"}' --priority 5)") != std::string::npos);
    auto [called_status, called] =
        post("call", R"({"host":")" + b_key + R"(","paglet":")" + paglet + R"(","name":"status"})");
    CHECK_EQ(called_status, 200);
    CHECK_EQ(number(called, "status"), 0);
    CHECK_EQ(number(*called.find("reply"), "value"), 7);
    // A bytes field from {"$bytes": hex}.
    auto [echo_status, echo] = post("call", R"({"host":")" + b_key + R"(","paglet":")" + paglet +
                                                R"(","name":"echo","payload":{"blob":{"$bytes":"00ff"}}})");
    CHECK_EQ(echo_status, 200);
    CHECK(echo.find("reply")->dump().find(R"("blob":[0,255])") != std::string::npos);

    // The model counts it; the modules page lists the module on b.
    app.refresh();
    const Json after = parse(app.model());
    CHECK_EQ(number(*after.find("hosts")->as_array()[1].find("counts"), "user"), 1);
    auto modules = web.Get("/api/modules", auth);
    REQUIRE(modules);
    CHECK(modules->body.find("\"file\":\"counter.wasm\"") != std::string::npos);

    // A module uploaded from the browser (base64), pushed to b.
    auto hello = paglets::wasm::read_file(guest_path("hello.wasm"));
    REQUIRE_OK(hello);
    auto [pushed_status, pushed] =
        post("push-module", R"({"host":")" + b_key + R"(","name":"hello.wasm","data":")" + base64(*hello) + R"("})");
    CHECK_EQ(pushed_status, 200);
    CHECK_EQ(member(pushed, "hash"), paglets::to_hex(paglets::sha256(*hello)));
    CHECK(member(pushed, "shell").ends_with(" remote push-module --connect " + b.transport->url() +
                                            " --key olga.key --ledger ledger hello.wasm"));
    // No zombie groups in this mesh.
    auto zombies = web.Get("/api/zombies", auth);
    REQUIRE(zombies);
    CHECK_EQ(zombies->status, 200);
    CHECK(zombies->body.starts_with(R"({"groups":[],"errors":[])"));

    // An unknown action, a malformed body, then dispose.
    CHECK_EQ(post("explode", "{}").first, 404);
    CHECK_EQ(web.Post("/api/call", auth, "not json", "application/json")->status, 400);
    auto [disposed_status, disposed] = post("dispose", R"({"host":")" + b_key + R"(","paglet":")" + paglet + R"("})");
    CHECK_EQ(disposed_status, 200);
    CHECK_EQ(member(disposed, "shell"), "paglets-host remote dispose --connect " + b.transport->url() +
                                            " --key olga.key --ledger ledger " + paglet);
    auto [gone_status, gone] =
        post("call", R"({"host":")" + b_key + R"(","paglet":")" + paglet + R"(","name":"status"})");
    CHECK_EQ(gone_status, 422);
    CHECK(member(gone, "error").find("no paglet") != std::string::npos);
    app.stop();
}
