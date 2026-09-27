// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Native system paglets in the runtime (WP10): registration, default
// service endpoints, resource capabilities, lending, derive paths, and the
// revocation tree (planning/cpp-system-paglets.md).

#include "runtime_fixture.hpp"
#include "test.hpp"

#include <filesystem>
#include <random>

using namespace paglets::test;

namespace {

// A small service: `open` mints a `dir` capability, `read` and `write`
// check a lent one, `quiet` never answers.
struct Vault final : rt::SystemPaglet {
    std::mutex mu;
    std::vector<rt::PagletId> ended;
    std::optional<std::string> grant;

    std::string_view name() const override { return "vault"; }
    std::vector<std::string> default_ops() const override { return {"open", "read", "write", "quiet"}; }

    void handle(rt::SystemContext& ctx, rt::ServiceCall& call) override {
        if (call.name() == "open") {
            std::optional<std::string> g;
            {
                std::lock_guard lock(mu);
                g = grant;
            }
            (void)call.reply(0, text("opened"), {ctx.resource("dir", "root", {"read", "write"}, g)});
        } else if (call.name() == "read" || call.name() == "write") {
            if (call.lent().size() != 1) {
                (void)call.reply(abi::invalid_argument);
                return;
            }
            const rt::Cap& dir = call.lent().front();
            if (auto c = ctx.check(dir, "dir", call.name()); c != abi::ok) {
                (void)call.reply(c);
                return;
            }
            (void)call.reply(0, text(dir.resource));
        }
    }

    void paglet_ended(rt::SystemContext&, const rt::PagletId& id) override {
        std::lock_guard lock(mu);
        ended.push_back(id);
    }
};

struct SystemFixture : Fixture {
    explicit SystemFixture(rt::Config config = {}) : Fixture(std::move(config)) {
        vault = std::make_shared<Vault>();
        auto id = runtime->add_system_paglet(vault);
        REQUIRE_OK(id);
        CHECK(*id == "system.vault");
    }

    std::int32_t service_handle(const rt::PagletId& paglet, std::string_view name) {
        auto info = dec<abi::SelfInfo>(call(paglet, "info").payload);
        for (const auto& [n, h] : info.services) {
            if (n == name) return h;
        }
        return 0;
    }

    // Opens the vault from the paglet; returns the handle of the `dir`
    // capability that came with the reply.
    std::int32_t open(const rt::PagletId& paglet) {
        const std::int32_t vault_handle = service_handle(paglet, "vault");
        REQUIRE(vault_handle > 0);
        const auto before = journal(paglet).size();
        REQUIRE(code(paglet, "request", Cmd{.handle = vault_handle, .name = "open"}) > 0);
        const std::string prefix = "reply:ok:opened:caps=";
        const auto until = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < until) {
            auto j = journal(paglet);
            for (std::size_t i = before; i < j.size(); ++i) {
                if (j[i].starts_with(prefix)) return std::stoi(j[i].substr(prefix.size()));
            }
            std::this_thread::sleep_for(10ms);
        }
        REQUIRE(false);
        return 0;
    }

    // Lends `dir` to the vault's `op` and returns the reply entry.
    std::string use(const rt::PagletId& paglet, std::string_view op, std::int32_t dir) {
        const std::int32_t vault_handle = service_handle(paglet, "vault");
        const auto before = journal(paglet).size();
        const auto id = code(paglet, "request", Cmd{.handle = vault_handle, .name = std::string(op), .lend = {dir}});
        if (id < 0) return "error:" + std::string(abi::error_name(static_cast<std::int32_t>(id)));
        const auto until = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < until) {
            auto j = journal(paglet);
            for (std::size_t i = before; i < j.size(); ++i) {
                if (j[i].starts_with("reply:")) return j[i];
            }
            std::this_thread::sleep_for(10ms);
        }
        return "no reply";
    }

    abi::CapInfo inspect(const rt::PagletId& paglet, std::int32_t handle) {
        auto r = cmd(paglet, "inspect", Cmd{.handle = handle});
        if (r.status != 0) throw std::runtime_error("inspect: " + std::string(abi::error_name(r.status)));
        return dec<abi::CapInfo>(r.payload);
    }

    std::int64_t derive(const rt::PagletId& paglet, std::int32_t handle, const abi::DeriveSpec& spec) {
        return code(paglet, "derive", Cmd{.handle = handle, .spec = enc(spec)});
    }

    std::string cap_id(const rt::PagletId& paglet, std::int32_t handle) {
        for (const auto& [h, cap] : runtime->capabilities(paglet)) {
            if (h == handle) return cap.id;
        }
        return {};
    }

    std::shared_ptr<Vault> vault;
};

std::filesystem::path temp_state(const std::string& name) {
    static std::mt19937_64 rng(std::random_device{}());
    return std::filesystem::temp_directory_path() / ("paglets-system-" + name + "-" + std::to_string(rng()));
}

}  // namespace

PAGLETS_TEST("abi C31: system paglets: default service endpoints and self_info") {
    SystemFixture f;
    const auto id = f.create("conformance.wasm");
    auto info = dec<abi::SelfInfo>(f.call(id, "info").payload);
    CHECK(info.minor == abi::minor_version);
    REQUIRE(info.services.size() == 1);
    CHECK(info.services[0].first == "vault");
    const auto e = f.inspect(id, info.services[0].second);
    CHECK(e.kind == "endpoint");
    CHECK(e.target == "service:vault");
    CHECK(e.ops == std::vector<std::string>({"open", "read", "write", "quiet"}));
    CHECK(!e.transferable);
    CHECK(!f.runtime->dispose("system.vault"));
    CHECK(f.runtime->info("system.vault")->trust == rt::TrustClass::system);
    // Children get their own endpoints.
    const auto child_handle = static_cast<std::int32_t>(f.code(id, "child", Cmd{}));
    const auto child = f.target_of(id, child_handle);
    CHECK(f.service_handle(child, "vault") > 0);
    // A request the service does not answer is answered `internal`.
    CHECK(f.code(id, "request", Cmd{.handle = info.services[0].second, .name = "quiet"}) > 0);
    CHECK(journal_eventually(f, id, "reply:internal:"));
}

PAGLETS_TEST("abi C32/C33: system paglets: resource capabilities, lending and derive paths") {
    SystemFixture f;
    const auto id = f.create("conformance.wasm");
    const std::int32_t dir = f.open(id);
    const auto info = f.inspect(id, dir);
    CHECK(info.kind == "dir");
    CHECK(info.target == "vault:root");
    CHECK(info.ops == std::vector<std::string>({"read", "write"}));

    // Lent capabilities stay with the paglet.
    CHECK(f.use(id, "read", dir) == "reply:ok:root");
    CHECK(f.use(id, "write", dir) == "reply:ok:root");
    CHECK(f.inspect(id, dir).kind == "dir");

    // Narrower copies: fewer rights, a subdirectory.
    const auto sub = f.derive(id, dir, abi::DeriveSpec{.ops = std::vector<std::string>{"read"}, .path = "a/b"});
    REQUIRE(sub > 0);
    CHECK(f.inspect(id, static_cast<std::int32_t>(sub)).target == "vault:root/a/b");
    CHECK(f.use(id, "read", static_cast<std::int32_t>(sub)) == "reply:ok:root/a/b");
    CHECK(f.use(id, "write", static_cast<std::int32_t>(sub)) == "reply:denied:");
    CHECK_EQ(f.derive(id, static_cast<std::int32_t>(sub), abi::DeriveSpec{.ops = std::vector<std::string>{"write"}}),
             static_cast<std::int64_t>(abi::denied));
    for (const char* bad : {"..", "a/../b", "/abs", "a//b", "c:x", "a\\b", ""}) {
        CHECK_EQ(f.derive(id, dir, abi::DeriveSpec{.path = std::string(bad)}),
                 static_cast<std::int64_t>(abi::invalid_argument));
    }
    // Paths only narrow directories; lending only goes to system paglets.
    CHECK_EQ(f.derive(id, abi::self_handle, abi::DeriveSpec{.path = "x"}),
             static_cast<std::int64_t>(abi::invalid_argument));
    CHECK_EQ(f.code(id, "send", Cmd{.handle = abi::self_handle, .name = "echo", .lend = {dir}}),
             static_cast<std::int64_t>(abi::invalid_argument));
    // A limited number of uses counts lending.
    const auto once = f.derive(id, dir, abi::DeriveSpec{.uses = 1});
    CHECK(f.use(id, "read", static_cast<std::int32_t>(once)) == "reply:ok:root");
    CHECK(f.use(id, "read", static_cast<std::int32_t>(once)) == "error:quota");
}

PAGLETS_TEST("abi C34: system paglets: revoking a capability revokes everything derived from it") {
    SystemFixture f;
    const auto id = f.create("conformance.wasm");
    const std::int32_t dir = f.open(id);
    const auto sub = static_cast<std::int32_t>(f.derive(id, dir, abi::DeriveSpec{.path = "a"}));
    const auto subsub = static_cast<std::int32_t>(f.derive(id, sub, abi::DeriveSpec{.path = "b"}));
    const auto other = f.open(id);
    CHECK(f.use(id, "read", subsub) == "reply:ok:root/a/b");

    f.runtime->revoke_capability(f.cap_id(id, sub));
    CHECK(f.use(id, "read", subsub) == "error:revoked");
    CHECK(f.use(id, "read", sub) == "error:revoked");
    CHECK(f.inspect(id, subsub).revoked);
    CHECK_EQ(f.derive(id, subsub, abi::DeriveSpec{}), static_cast<std::int64_t>(abi::revoked));
    // The parent and unrelated capabilities are untouched.
    CHECK(f.use(id, "read", dir) == "reply:ok:root");
    CHECK(f.use(id, "read", other) == "reply:ok:root");

    // Grants: every capability from a revoked grant, however derived.
    {
        std::lock_guard lock(f.vault->mu);
        f.vault->grant = "grant-1";
    }
    const std::int32_t granted = f.open(id);
    const auto narrowed = static_cast<std::int32_t>(f.derive(id, granted, abi::DeriveSpec{.path = "x"}));
    CHECK(f.use(id, "read", narrowed) == "reply:ok:root/x");
    f.runtime->set_revoked_grants({"grant-1"});
    CHECK(f.use(id, "read", narrowed) == "error:revoked");
    CHECK(f.use(id, "read", granted) == "error:revoked");
    CHECK(f.use(id, "read", dir) == "reply:ok:root");
    f.runtime->set_revoked_grants({});
    CHECK(f.use(id, "read", narrowed) == "reply:ok:root/x");
}

PAGLETS_TEST("system paglets: told when paglets end") {
    SystemFixture f;
    const auto a = f.create("conformance.wasm");
    const auto b = f.create("conformance.wasm");
    REQUIRE_OK(f.runtime->dispose(a));
    CHECK(f.call(b, "trap").status == abi::failed);
    REQUIRE(f.runtime->wait_idle());
    std::lock_guard lock(f.vault->mu);
    auto ended = f.vault->ended;
    std::ranges::sort(ended);
    auto expected = std::vector<rt::PagletId>({a, b});
    std::ranges::sort(expected);
    CHECK(ended == expected);
}

PAGLETS_TEST("abi C35: system paglets: capabilities, service endpoints and revocations survive restarts") {
    const auto state = temp_state("restart");
    rt::PagletId id;
    std::int32_t dir = 0;
    std::int32_t sub = 0;
    {
        rt::Config config;
        config.state_dir = state;
        config.checkpoint_interval = 0ms;
        SystemFixture f(config);
        id = f.create("conformance.wasm");
        dir = f.open(id);
        sub = static_cast<std::int32_t>(f.derive(id, dir, abi::DeriveSpec{.path = "kept"}));
        f.runtime->revoke_capability(f.cap_id(id, dir));
        REQUIRE(f.runtime->wait_idle());
    }
    {
        rt::Config config;
        config.state_dir = state;
        SystemFixture f(config);
        CHECK(f.service_handle(id, "vault") > 0);
        CHECK(f.inspect(id, sub).target == "vault:root/kept");
        CHECK(f.use(id, "read", sub) == "error:revoked");
    }
    std::error_code ec;
    std::filesystem::remove_all(state, ec);
}
