// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Gateway system paglets (WP17, planning/cpp-web-ai.md): destinations,
// URLs and text extraction; `web` and `ai` through paglets; the Download
// Courier exit (a paglet started on a host without internet access fetches
// a file through the only web host and brings it home; an intranet address
// is refused).

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <courier_msgs.hpp>
#include <digest_msgs.hpp>
#include <researcher_msgs.hpp>
#include <semantic_msgs.hpp>
#include <paglets/gateway/gateway.hpp>
#include <paglets/services/ai.hpp>
#include <paglets/services/web.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wire/reflect.hpp>

#include <filesystem>
#include <fstream>
#include <random>
#include <thread>

using namespace paglets::test;
namespace gw = paglets::gateway;
namespace wire = paglets::wire;
namespace web = paglets::services::web;
namespace ai = paglets::services::ai;

namespace {

// A small web site on 127.0.0.1 (an internal address).
struct Site {
    httplib::Server server;
    std::thread thread;
    int port = 0;
    std::string report = "quarterly numbers: 42\n";

    Site() {
        server.Get("/files/report.txt", [this](const httplib::Request&, httplib::Response& r) {
            r.set_content(report, "text/plain");
        });
        server.Get("/files/page.html", [](const httplib::Request&, httplib::Response& r) {
            r.set_content("<html><head><title>The &amp; Page</title><style>p{}</style></head><body><h1>Hello</h1>"
                          "<p>First <b>bold</b> line.</p><script>var x;</script><a href=\"report.txt\">Report</a>"
                          "<a href=\"/admin\">Admin</a></body></html>",
                          "text/html");
        });
        server.Get("/files/moved", [](const httplib::Request&, httplib::Response& r) {
            r.set_redirect("/files/report.txt");
        });
        server.Get("/files/to-admin", [](const httplib::Request&, httplib::Response& r) {
            r.set_redirect("/admin/secret");
        });
        server.Get("/files/big", [](const httplib::Request&, httplib::Response& r) {
            r.set_content(std::string(200 * 1024, 'x'), "application/octet-stream");
        });
        // A search backend (SearXNG's JSON format) that finds the site's pages.
        server.Get("/search", [this](const httplib::Request& q, httplib::Response& r) {
            const std::string query = q.get_param_value("q");
            r.set_content("{\"query\":\"" + query + "\",\"results\":[{\"title\":\"The report\",\"url\":\"" +
                              url("/files/report.txt") + "\",\"content\":\"numbers\"},{\"title\":\"\",\"url\":\"" +
                              url("/files/page.html") + "\",\"content\":\"a page\"}]}",
                          "application/json");
        });
        server.Get("/admin/secret", [](const httplib::Request&, httplib::Response& r) {
            r.set_content("intranet only", "text/plain");
        });
        port = server.bind_to_any_port("127.0.0.1");
        thread = std::thread([this] { server.listen_after_bind(); });
        server.wait_until_ready();
    }
    ~Site() {
        server.stop();
        thread.join();
    }
    std::string url(std::string_view path) const { return "http://127.0.0.1:" + std::to_string(port) + std::string(path); }
};

gw::GatewayConfig web_config(const Site& site) {
    gw::GatewayConfig c;
    c.web.enabled = true;
    c.web.internet = false;  // only the allowed intranet prefix
    c.web.internal = {site.url("/files/")};
    c.web.max_bytes = 64 * 1024;
    c.web.max_download_bytes = 1024 * 1024;
    c.web.timeout = std::chrono::milliseconds(5000);
    return c;
}

// A paglet's request to a system paglet through an endpoint capability;
// returns the journal entry of the reply ("reply:<status>:<payload>").
std::string ask_raw(Fixture& f, const rt::PagletId& paglet, std::int32_t handle, const std::string& op,
                    const Bytes& payload) {
    const auto before = f.journal(paglet).size();
    REQUIRE(f.code(paglet, "request", Cmd{.handle = handle, .name = op, .payload = payload, .ms = 20000}) > 0);
    const auto until = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < until) {
        const auto j = f.journal(paglet);
        for (std::size_t i = before; i < j.size(); ++i) {
            if (j[i].starts_with("reply:")) return j[i];
        }
        std::this_thread::sleep_for(10ms);
    }
    return "no reply";
}

template <class Reply>
Reply decode_ok(const std::string& entry) {
    REQUIRE(entry.starts_with("reply:ok:"));
    std::string payload = entry.substr(9);
    if (auto caps = payload.rfind(":caps="); caps != std::string::npos) payload.resize(caps);
    Reply r{};
    REQUIRE(wire::from_msgpack(Bytes(payload.begin(), payload.end()), r));
    return r;
}

std::int32_t endpoint_to(Fixture& f, const rt::PagletId& paglet, const std::string& service) {
    rt::Cap c;
    c.kind = rt::Cap::Kind::endpoint;
    c.target = rt::system_paglet_id(service);
    c.ops = {"*"};
    c.id = "test-endpoint-" + service;
    auto handle = f.runtime->add_capability(paglet, c);
    REQUIRE_OK(handle);
    return *handle;
}

}  // namespace

PAGLETS_TEST("gateway: internal addresses, URLs and readable text") {
    for (const char* a : {"127.0.0.1", "10.1.2.3", "172.16.0.1", "172.31.255.255", "192.168.1.1", "169.254.169.254",
                          "100.64.0.1", "0.0.0.0", "224.0.0.1", "255.255.255.255", "::1", "::", "fc00::1", "fd12::1",
                          "fe80::1", "::ffff:10.0.0.1", "::ffff:127.0.0.1", "64:ff9b::a00:1", "2002:a00:1::1",
                          "[::1]", "fe80::1%eth0", "not an address", "1.2.3"}) {
        if (!gw::internal_address(a)) std::cerr << "    public: " << a << "\n";
        CHECK(gw::internal_address(a));
    }
    for (const char* a : {"8.8.8.8", "1.1.1.1", "172.32.0.1", "100.128.0.1", "2001:4860:4860::8888", "::ffff:8.8.8.8",
                          "[2606:4700::1111]"}) {
        if (gw::internal_address(a)) std::cerr << "    internal: " << a << "\n";
        CHECK(!gw::internal_address(a));
    }
    CHECK(gw::resolve_url("https://example.com/a/b/c.html?x=1", "../d.txt") == "https://example.com/a/d.txt");
    CHECK(gw::resolve_url("https://example.com/a/b/", "/root?q") == "https://example.com/root?q");
    CHECK(gw::resolve_url("https://example.com/a/b", "//other.org/x") == "https://other.org/x");
    CHECK(gw::resolve_url("http://example.com:8080/a", "b#frag") == "http://example.com:8080/b");
    CHECK(gw::resolve_url("http://example.com/", "HTTP://Example.COM:80/x") == "http://example.com/x");
    CHECK(gw::resolve_url("http://example.com/", "mailto:someone@example.com").empty());
    CHECK(gw::resolve_url("http://example.com/", "javascript:alert(1)").empty());
    CHECK(gw::resolve_url("http://example.com/", "ftp://example.com/x").empty());
    CHECK(gw::resolve_url("http://example.com/", "http://user:secret@example.com/").empty());  // no credentials

    const auto e = gw::extract_html(
        "<html><head><title>A &amp; B</title><script>ignored()</script></head><body><h1>Head</h1>"
        "<p>One&nbsp;two <i>three</i>.</p><!-- hidden --><ul><li>x</li><li>y &#x263A;</li></ul>"
        "<a href='sub/page.html'>Sub page</a><a href=\"https://other.org/\">Other</a><style>.x{}</style></body></html>",
        "https://example.com/docs/index.html");
    CHECK_EQ(e.title, std::string("A & B"));
    CHECK(e.text.find("Head") != std::string::npos);
    CHECK(e.text.find("One two three.") != std::string::npos);
    CHECK(e.text.find("y \xe2\x98\xba") != std::string::npos);
    CHECK(e.text.find("ignored") == std::string::npos);
    CHECK(e.text.find("hidden") == std::string::npos);
    CHECK(e.text.find(".x{}") == std::string::npos);
    REQUIRE(e.links.size() == 2u);
    CHECK(e.links[0].first == "https://example.com/docs/sub/page.html");
    CHECK(e.links[0].second == "Sub page");
    CHECK(e.links[1].first == "https://other.org/");
}

PAGLETS_TEST("gateway: web fetches, extracts and downloads within its destinations") {
    Site site;
    Fixture f;
    auto services = ps::install_system_services(*f.runtime, {});
    REQUIRE_OK(services);
    std::vector<std::string> offered;
    auto config = web_config(site);
    config.offers = [&](const std::string& service, std::optional<paglets::services::mesh_info::Offer> o) {
        if (o) offered.push_back(service + ":" + std::to_string(o->ops.size()));
    };
    auto installed = gw::install_gateway(*f.runtime, *services, config);
    REQUIRE_OK(installed);
    CHECK(offered == std::vector<std::string>{"web:3"});

    const auto id = f.create("conformance.wasm");
    const auto h = endpoint_to(f, id, "web");
    const auto caps = decode_ok<web::Capabilities>(ask_raw(f, id, h, "capabilities", wire::to_msgpack(web::CapabilitiesRequest{})));
    CHECK(!caps.internet);
    CHECK(caps.internal == std::vector<std::string>{site.url("/files/")});

    // fetch, also after a redirect within the allowed prefix.
    auto r = decode_ok<web::FetchReply>(
        ask_raw(f, id, h, "fetch", wire::to_msgpack(web::FetchRequest{site.url("/files/moved"), "GET", 0})));
    CHECK_EQ(r.status, 200);
    CHECK(r.url == site.url("/files/report.txt"));
    CHECK(std::string(r.body.begin(), r.body.end()) == site.report);
    CHECK(r.content_type.starts_with("text/plain"));
    // Bodies are bounded.
    r = decode_ok<web::FetchReply>(
        ask_raw(f, id, h, "fetch", wire::to_msgpack(web::FetchRequest{site.url("/files/big"), "GET", 1000})));
    CHECK(r.truncated);
    CHECK_EQ(r.body.size(), 1000u);

    // Internal destinations outside the allowed prefix are refused, also
    // when a redirect leads there; so are other internal addresses.
    CHECK(ask_raw(f, id, h, "fetch", wire::to_msgpack(web::FetchRequest{site.url("/admin/secret"), "GET", 0}))
              .starts_with("reply:denied:"));
    CHECK(ask_raw(f, id, h, "fetch", wire::to_msgpack(web::FetchRequest{site.url("/files/to-admin"), "GET", 0}))
              .starts_with("reply:denied:"));
    CHECK(ask_raw(f, id, h, "fetch", wire::to_msgpack(web::FetchRequest{"http://10.11.12.13/", "GET", 0}))
              .starts_with("reply:denied:"));
    CHECK(ask_raw(f, id, h, "fetch", wire::to_msgpack(web::FetchRequest{"http://[::1]/", "GET", 0}))
              .starts_with("reply:denied:"));
    CHECK(ask_raw(f, id, h, "fetch", wire::to_msgpack(web::FetchRequest{"file:///etc/passwd", "GET", 0}))
              .starts_with("reply:invalid_argument:"));
    CHECK(ask_raw(f, id, h, "fetch", wire::to_msgpack(web::FetchRequest{site.url("/files/report.txt"), "POST", 0}))
              .starts_with("reply:invalid_argument:"));

    // Readable text and links.
    const auto page = decode_ok<web::ExtractReply>(
        ask_raw(f, id, h, "extract_text", wire::to_msgpack(web::ExtractRequest{site.url("/files/page.html")})));
    CHECK_EQ(page.title, std::string("The & Page"));
    CHECK(page.text.find("First bold line.") != std::string::npos);
    CHECK(page.text.find("var x") == std::string::npos);
    REQUIRE(page.links.size() == 2u);
    CHECK(page.links[0].url == site.url("/files/report.txt"));

    // A download becomes an artifact; a wrong expected hash is refused.
    const std::string hash = paglets::to_hex(paglets::sha256(text(site.report)));
    const auto entry = ask_raw(f, id, h, "download", wire::to_msgpack(web::DownloadRequest{site.url("/files/report.txt"), hash}));
    CHECK(entry.find(":caps=") != std::string::npos);
    const auto d = decode_ok<web::DownloadReply>(entry);
    CHECK_EQ(d.artifact, hash);
    CHECK_EQ(d.size, static_cast<std::int64_t>(site.report.size()));
    CHECK(ask_raw(f, id, h, "download",
                  wire::to_msgpack(web::DownloadRequest{site.url("/files/report.txt"), std::string(64, '0')}))
              .starts_with("reply:invalid_argument:"));
    CHECK(ask_raw(f, id, h, "search", wire::to_msgpack(web::SearchRequest{"paglets", 5})).starts_with("reply:failed:"));
}

PAGLETS_TEST("gateway: ai answers through its backend, with quotas and offers") {
    Fixture f;
    auto services = ps::install_system_services(*f.runtime, {});
    REQUIRE_OK(services);
    std::vector<std::string> offered;
    gw::GatewayConfig config;
    config.ai.enabled = true;
    config.ai.backend = "test";
    config.ai.max_requests_per_owner = 5;
    config.offers = [&](const std::string& service, std::optional<paglets::services::mesh_info::Offer> o) {
        offered.push_back(service + (o ? ":offered" : ":withdrawn"));
    };
    auto installed = gw::install_gateway(*f.runtime, *services, config);
    REQUIRE_OK(installed);
    CHECK(offered == std::vector<std::string>{"ai:offered"});

    const auto id = f.create("conformance.wasm");
    const auto h = endpoint_to(f, id, "ai");
    const auto caps = decode_ok<ai::Capabilities>(ask_raw(f, id, h, "capabilities", wire::to_msgpack(ai::CapabilitiesRequest{})));
    CHECK(caps.available);
    CHECK_EQ(caps.backend, std::string("test"));
    REQUIRE(caps.models.size() == 1u);

    const std::string doc = "The rocket launch was a success. Engineers praised the rocket and its engines.";
    const auto s = decode_ok<ai::TextReply>(ask_raw(f, id, h, "summarize", wire::to_msgpack(ai::SummarizeRequest{doc, 3, ""})));
    CHECK_EQ(s.text, std::string("The rocket launch ..."));
    CHECK_EQ(s.model, std::string("test"));
    const auto c = decode_ok<ai::ClassifyReply>(
        ask_raw(f, id, h, "classify", wire::to_msgpack(ai::ClassifyRequest{doc, {"cooking", "rocket", "sports"}, ""})));
    CHECK_EQ(c.label, std::string("rocket"));
    const auto x = decode_ok<ai::ExtractReply>(ask_raw(
        f, id, h, "extract",
        wire::to_msgpack(ai::ExtractRequest{"Invoice\nTotal: 12.50 EUR\nDue: tomorrow", {{"total", ""}, {"vendor", ""}}, ""})));
    REQUIRE(x.fields.size() == 2u);
    CHECK_EQ(x.fields[0].value, std::string("12.50 EUR"));
    CHECK(x.fields[1].value.empty());
    const auto v = decode_ok<ai::EmbedReply>(
        ask_raw(f, id, h, "embed", wire::to_msgpack(ai::EmbedRequest{{"rocket engines", "engines rocket", "soup"}, ""})));
    REQUIRE(v.vectors.size() == 3u);
    double same = 0, other = 0;
    for (std::size_t i = 0; i < 64; ++i) {
        same += v.vectors[0].values[i] * v.vectors[1].values[i];
        other += v.vectors[0].values[i] * v.vectors[2].values[i];
    }
    CHECK(same > 0.99);
    CHECK(other < 0.5);
    // An unknown model (not counted); the quota of the owner (5 per hour).
    CHECK(ask_raw(f, id, h, "generate", wire::to_msgpack(ai::GenerateRequest{"hi", "", "llama-9000"})).starts_with("reply:not_found:"));
    const auto g = decode_ok<ai::TextReply>(ask_raw(f, id, h, "generate", wire::to_msgpack(ai::GenerateRequest{"hi", "", ""})));
    CHECK_EQ(g.text, std::string("echo: hi"));
    CHECK(ask_raw(f, id, h, "generate", wire::to_msgpack(ai::GenerateRequest{"hi", "", ""})).starts_with("reply:quota:"));
}

PAGLETS_TEST("gateway: Download Courier fetches through the only web host and brings the file home (WP17 exit)") {
    Site site;
    Mesh net;
    auto& home = net.add_host("home");
    auto& gateway = net.add_host("gateway");
    net.add_host("plain");
    net.enroll_all();
    for (auto& h : net.hosts) {
        h->node->set_compute_timing({.sample = 50ms, .gossip = 50ms, .ttl = 2000ms, .redirect_after = 0ms});
    }
    // The gateway host offers `web`; the mesh policy lets paglets use it.
    auto config = web_config(site);
    config.offers = [&](const std::string&, std::optional<paglets::services::mesh_info::Offer> o) {
        if (o) gateway.node->set_offer(std::move(*o));
    };
    config.authorize = [&](const abi::SenderRecord& caller, std::string_view service, std::string_view op,
                           std::string_view root, std::string_view path) {
        return gateway.node->allows(caller, Item{std::string(service), {std::string(op)}, std::string(root), std::string(path)});
    };
    REQUIRE_OK(gw::install_gateway(*gateway.f->runtime, gateway.services, config));
    net.admin_record("policy-rule",
                     data::policy_rule({"downloads", Decision::allow, "web", {"download"}, {}, std::nullopt, std::nullopt, 0}));
    REQUIRE(net.settle([&] {
        return net.converged() &&
               home.node->find_offers(paglets::services::mesh_info::OffersRequest{.service = "web", .op = "download"}).size() == 1u;
    }));

    const std::string module = home.f->module("courier.wasm");
    auto run = [&](char tag, const std::string& url) {
        auto created = home.node->create(module, net.passport(module, paglet_id(tag)));
        REQUIRE_OK(created);
        const auto id = *created;
        auto started = home.f->runtime->call(id, "start", wire::to_msgpack(courier_msgs::Start{url, "", "offer:web.download"}), 20s);
        REQUIRE(started.status == 0);
        courier_msgs::Started s;
        REQUIRE(wire::from_msgpack(started.payload, s));
        CHECK(s.accepted);
        // Away and back.
        bool away = false;
        REQUIRE(net.settle(
            [&] {
                away = away || gateway.f->runtime->info(id).has_value();
                return away && home.f->runtime->info(id).has_value() && !gateway.f->runtime->info(id);
            },
            1000));
        courier_msgs::Status st;
        REQUIRE(net.settle(
            [&] {
                auto r = home.f->runtime->call(id, "status", {}, 10s);
                return r.status == 0 && wire::from_msgpack(r.payload, st) && (st.state == "delivered" || st.state == "failed");
            },
            400));
        return st;
    };

    const auto delivered = run('c', site.url("/files/report.txt"));
    if (delivered.state != "delivered") std::cerr << "    courier: " << delivered.error << "\n";
    CHECK_EQ(delivered.state, std::string("delivered"));
    CHECK_EQ(delivered.artifact, paglets::to_hex(paglets::sha256(text(site.report))));
    CHECK_EQ(delivered.size, static_cast<std::int64_t>(site.report.size()));
    CHECK(delivered.hosts == std::vector<std::string>({"home", "gateway", "home"}));
    const auto notes = home.services->notifications(net.owner.id());
    CHECK(std::ranges::any_of(notes, [](const auto& n) { return n.title.starts_with("courier delivered"); }));

    // An intranet address through `web` is refused; the courier comes home
    // and says so.
    const auto refused = run('d', site.url("/admin/secret"));
    CHECK_EQ(refused.state, std::string("failed"));
    CHECK(refused.error.find("denied") != std::string::npos);
    CHECK(refused.artifact.empty());
}

namespace {

std::filesystem::path temp_root(const std::string& name) {
    static std::mt19937_64 rng(std::random_device{}());
    auto dir = std::filesystem::temp_directory_path() / ("paglets-digest-" + name + "-" + std::to_string(rng()));
    std::filesystem::create_directories(dir);
    return dir;
}

void write_file(const std::filesystem::path& path, std::string_view text) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream(path, std::ios::binary) << text;
}

std::string read_file(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

}  // namespace

PAGLETS_TEST("gateway: AI Document Digest reads on two hosts, summarizes on the AI host, writes on a third (WP17 exit)") {
    namespace fs = std::filesystem;
    const fs::path linux_docs = temp_root("linux");
    const fs::path windows_docs = temp_root("windows");
    const fs::path windows_secret = temp_root("secret");
    const fs::path archive = temp_root("archive");
    write_file(linux_docs / "reports" / "launch.txt", "The rocket launch went well. All engines worked as planned.");
    write_file(linux_docs / "notes.md", "not a text file");
    write_file(windows_docs / "budget.txt", "The budget for next year grows by four percent. Travel is cut.");
    write_file(windows_secret / "salaries.txt", "Confidential salaries of the board.");
    ps::ServicesConfig linux_sc, windows_sc, archive_sc;
    linux_sc.roots["docs"] = linux_docs;
    windows_sc.roots["docs"] = windows_docs;
    windows_sc.roots["secret"] = windows_secret;
    archive_sc.roots["out"] = archive;

    Mesh net;
    auto& linux_host = net.add_host("linux", linux_sc);
    auto& windows_host = net.add_host("windows", windows_sc);
    auto& mac = net.add_host("mac");
    auto& archive_host = net.add_host("archive", archive_sc);
    net.enroll_all();
    for (auto& h : net.hosts) {
        h->node->set_compute_timing({.sample = 50ms, .gossip = 50ms, .ttl = 2000ms, .redirect_after = 0ms});
    }
    // Only the mac host offers `ai`.
    gw::GatewayConfig config;
    config.ai.enabled = true;
    config.ai.backend = "test";
    config.offers = [&](const std::string& service, std::optional<paglets::services::mesh_info::Offer> o) {
        if (o) {
            mac.node->set_offer(std::move(*o));
        } else {
            mac.node->withdraw_offer(service);
        }
    };
    config.authorize = [&](const abi::SenderRecord& caller, std::string_view service, std::string_view op,
                           std::string_view root, std::string_view path) {
        return mac.node->allows(caller, Item{std::string(service), {std::string(op)}, std::string(root), std::string(path)});
    };
    REQUIRE_OK(gw::install_gateway(*mac.f->runtime, mac.services, config));
    // The policy: documents may be read, digests written, texts summarized;
    // the secret root's content stays on its host.
    net.admin_record("policy-rule", data::policy_rule({"read documents", Decision::allow, "files", {"read"}, {},
                                                       Scope{std::vector<std::string>{"docs", "secret"}, std::nullopt},
                                                       std::nullopt, 0}));
    net.admin_record("policy-rule", data::policy_rule({"write digests", Decision::allow, "files", {"write", "create"}, {},
                                                       Scope{std::vector<std::string>{"out"}, std::nullopt}, std::nullopt, 0}));
    net.admin_record("policy-rule",
                     data::policy_rule({"summaries", Decision::allow, "ai", {"summarize"}, {}, std::nullopt, std::nullopt, 0}));
    net.admin_record("root-residency", data::root_residency("secret", "host-only"));
    REQUIRE(net.settle([&] {
        return net.converged() &&
               linux_host.node->find_offers(paglets::services::mesh_info::OffersRequest{.service = "ai"}).size() == 1u;
    }));

    const std::string module = linux_host.f->module("digest.wasm");
    // Runs a digest from the linux host until it is written, refused or failed;
    // returns the status and the host it ended on.
    auto run = [&](char tag, std::vector<digest_msgs::Source> sources) {
        auto created = linux_host.node->create(module, net.passport(module, paglet_id(tag)));
        REQUIRE_OK(created);
        const auto id = *created;
        digest_msgs::Start start;
        start.sources = std::move(sources);
        start.output = {"archive", "out", "digest.md"};
        start.max_words = 4;
        auto started = linux_host.f->runtime->call(id, "start", wire::to_msgpack(start), 20s);
        REQUIRE(started.status == 0);
        digest_msgs::Status st;
        Mesh::Host* where = nullptr;
        REQUIRE(net.settle(
            [&] {
                for (auto& h : net.hosts) {
                    if (!h->f->runtime->info(id)) continue;
                    auto r = h->f->runtime->call(id, "status", {}, 10s);
                    if (r.status != 0 || !wire::from_msgpack(r.payload, st)) return false;
                    where = h.get();
                    return st.state == "written" || st.state == "refused" || st.state == "failed";
                }
                return false;
            },
            2000));
        return std::pair{st, where};
    };

    const auto [written, at] = run('a', {{"linux", "docs", "**/*.txt"}, {"windows", "docs", "**/*.txt"}});
    if (written.state != "written") std::cerr << "    digest: " << written.error << "\n";
    CHECK_EQ(written.state, std::string("written"));
    CHECK(at == &archive_host);
    CHECK(written.hosts == std::vector<std::string>({"linux", "windows", "mac", "archive"}));
    REQUIRE(written.documents.size() == 2u);
    CHECK_EQ(written.documents[0].summary, std::string("The rocket launch went ..."));
    CHECK_EQ(written.documents[1].summary, std::string("The budget for next ..."));
    const std::string digest = read_file(archive / "digest.md");
    CHECK(digest.find("## linux: docs/reports/launch.txt") != std::string::npos);
    CHECK(digest.find("The budget for next ...") != std::string::npos);
    CHECK(digest.find("Summaries by test.") != std::string::npos);

    // With content of the host-only root in its memory, the paglet may not
    // go to the AI host: it stays on the windows host.
    const auto [refused, stayed] = run('b', {{"windows", "secret", "**/*.txt"}});
    CHECK_EQ(refused.state, std::string("refused"));
    CHECK(refused.error.find("data residency") != std::string::npos);
    CHECK(stayed == &windows_host);
    CHECK(refused.documents.size() == 1u);

    std::error_code ec;
    for (const auto& p : {linux_docs, windows_docs, windows_secret, archive}) fs::remove_all(p, ec);
}

PAGLETS_TEST("gateway: Semantic Mesh Search indexes documents on the AI host and answers by meaning") {
    namespace fs = std::filesystem;
    const fs::path ra = temp_root("sa"), rb = temp_root("sb");
    write_file(ra / "rockets.txt", "The rocket engines fired and the launch went to orbit.");
    write_file(ra / "soup.txt", "Simmer the vegetable soup with onions and garlic for an hour.");
    write_file(rb / "garden.txt", "Plant the tomatoes in spring and water the garden every morning.");
    ps::ServicesConfig sa, sb;
    sa.roots["docs"] = ra;
    sb.roots["docs"] = rb;
    Mesh net;
    auto& a = net.add_host("a", sa);
    net.add_host("b", sb);
    auto& mac = net.add_host("mac");
    net.enroll_all();
    for (auto& h : net.hosts) {
        h->node->set_compute_timing({.sample = 50ms, .gossip = 50ms, .ttl = 2000ms, .redirect_after = 0ms});
    }
    gw::GatewayConfig config;
    config.ai.enabled = true;
    config.ai.backend = "test";
    config.offers = [&](const std::string& service, std::optional<paglets::services::mesh_info::Offer> o) {
        if (o) {
            mac.node->set_offer(std::move(*o));
        } else {
            mac.node->withdraw_offer(service);
        }
    };
    config.authorize = [&](const abi::SenderRecord& caller, std::string_view service, std::string_view op,
                           std::string_view root, std::string_view path) {
        return mac.node->allows(caller, Item{std::string(service), {std::string(op)}, std::string(root), std::string(path)});
    };
    REQUIRE_OK(gw::install_gateway(*mac.f->runtime, mac.services, config));
    net.admin_record("policy-rule", data::policy_rule({"read docs", Decision::allow, "files", {"read"}, {},
                                                       Scope{std::vector<std::string>{"docs"}, std::nullopt},
                                                       std::nullopt, 0}));
    net.admin_record("policy-rule",
                     data::policy_rule({"embeddings", Decision::allow, "ai", {"embed"}, {}, std::nullopt, std::nullopt, 0}));
    REQUIRE(net.settle([&] {
        return net.converged() &&
               a.node->find_offers(paglets::services::mesh_info::OffersRequest{.service = "ai", .op = "embed"}).size() == 1u;
    }));
    const std::string module = a.f->module("semantic.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('8')));
    REQUIRE_OK(created);
    const auto id = *created;
    semantic_msgs::Index index;
    index.sources = {{"a", "docs", "**/*.txt"}, {"b", "docs", "**/*.txt"}};
    REQUIRE(a.f->runtime->call(id, "index", wire::to_msgpack(index), 20s).status == 0);
    semantic_msgs::Status st;
    REQUIRE(net.settle(
        [&] {
            if (!mac.f->runtime->info(id)) return false;  // the index is built on the AI host
            auto x = mac.f->runtime->call(id, "status", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, st) && (st.state == "ready" || st.state == "failed");
        },
        3000));
    if (st.state != "ready") std::cerr << "    semantic: " << st.error << "\n";
    REQUIRE(st.state == "ready");
    CHECK_EQ(st.documents, 3);
    CHECK_EQ(st.host_name, std::string("mac"));
    auto ask = [&](const std::string& text) {
        auto x = mac.f->runtime->call(id, "query", wire::to_msgpack(semantic_msgs::Query{text, 2}), 20s);
        REQUIRE(x.status == 0);
        semantic_msgs::Hits hits;
        REQUIRE(wire::from_msgpack(x.payload, hits));
        REQUIRE(hits.error.empty());
        REQUIRE(hits.hits.size() == 2u);
        return hits;
    };
    auto hits = ask("rocket engines launch");
    CHECK_EQ(hits.hits[0].path, std::string("docs/rockets.txt"));
    CHECK_EQ(hits.hits[0].host_name, std::string("a"));
    CHECK(hits.hits[0].score > hits.hits[1].score);
    hits = ask("water the tomatoes in the garden");
    CHECK_EQ(hits.hits[0].path, std::string("docs/garden.txt"));
    CHECK_EQ(hits.hits[0].host_name, std::string("b"));
    std::error_code ec;
    for (const auto& p : {ra, rb}) fs::remove_all(p, ec);
}

PAGLETS_TEST("gateway: Web Researcher searches and reads on the web host, summarizes on the AI host, comes home") {
    Site site;
    Mesh net;
    auto& home = net.add_host("home");
    auto& gateway = net.add_host("gateway");
    auto& mac = net.add_host("mac");
    net.enroll_all();
    for (auto& h : net.hosts) {
        h->node->set_compute_timing({.sample = 50ms, .gossip = 50ms, .ttl = 2000ms, .redirect_after = 0ms});
    }
    auto offers_of = [](Mesh::Host& h) {
        return [&h](const std::string& service, std::optional<paglets::services::mesh_info::Offer> o) {
            if (o) {
                h.node->set_offer(std::move(*o));
            } else {
                h.node->withdraw_offer(service);
            }
        };
    };
    auto authorize_on = [](Mesh::Host& h) {
        return [&h](const abi::SenderRecord& caller, std::string_view service, std::string_view op, std::string_view root,
                    std::string_view path) {
            return h.node->allows(caller, Item{std::string(service), {std::string(op)}, std::string(root), std::string(path)});
        };
    };
    auto web = web_config(site);
    web.web.search_url = site.url("/search?q={query}");
    web.offers = offers_of(gateway);
    web.authorize = authorize_on(gateway);
    REQUIRE_OK(gw::install_gateway(*gateway.f->runtime, gateway.services, web));
    gw::GatewayConfig ai;
    ai.ai.enabled = true;
    ai.ai.backend = "test";
    ai.offers = offers_of(mac);
    ai.authorize = authorize_on(mac);
    REQUIRE_OK(gw::install_gateway(*mac.f->runtime, mac.services, ai));
    net.admin_record("policy-rule", data::policy_rule({"web research", Decision::allow, "web", {"search", "extract_text"},
                                                       {}, std::nullopt, std::nullopt, 0}));
    net.admin_record("policy-rule",
                     data::policy_rule({"summaries", Decision::allow, "ai", {"summarize"}, {}, std::nullopt, std::nullopt, 0}));
    REQUIRE(net.settle([&] {
        return net.converged() &&
               home.node->find_offers(paglets::services::mesh_info::OffersRequest{.service = "web", .op = "search"}).size() == 1u &&
               home.node->find_offers(paglets::services::mesh_info::OffersRequest{.service = "ai"}).size() == 1u;
    }));
    const std::string module = home.f->module("researcher.wasm");
    auto created = home.node->create(module, net.passport(module, paglet_id('9')));
    REQUIRE_OK(created);
    const auto id = *created;
    researcher_msgs::Question q;
    q.question = "quarterly numbers";
    q.pages = 2;
    q.max_words = 3;
    REQUIRE(home.f->runtime->call(id, "start", wire::to_msgpack(q), 20s).status == 0);
    researcher_msgs::Report report;
    bool away = false;
    REQUIRE(net.settle(
        [&] {
            if (!home.f->runtime->info(id)) {
                away = true;
                return false;
            }
            if (!away) return false;
            auto x = home.f->runtime->call(id, "report", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, report) &&
                   (report.state == "done" || report.state == "failed");
        },
        3000));
    if (report.state != "done") std::cerr << "    researcher: " << report.error << "\n";
    CHECK_EQ(report.state, std::string("done"));
    CHECK(report.hosts == std::vector<std::string>({"home", "gateway", "mac", "home"}));
    REQUIRE(report.sources.size() == 2u);
    CHECK_EQ(report.sources[0].title, std::string("The report"));
    CHECK_EQ(report.sources[0].summary, std::string("quarterly numbers: 42"));  // three words: all of it
    CHECK_EQ(report.sources[1].summary, std::string("Hello First bold ..."));
}
