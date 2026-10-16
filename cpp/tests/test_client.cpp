// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The mesh client library (WP31): shell commands for tools.

#include "test.hpp"

#include <paglets/client/client.hpp>
#include <paglets/mesh/passport.hpp>

#include <atomic>
#include <limits>

namespace pc = paglets::client;

PAGLETS_TEST("client: pin and passport durations are bounded before a request leaves") {
    namespace mesh = paglets::mesh;
    namespace net = paglets::net;
    auto admin = mesh::SigningKey::generate();
    auto owner = mesh::SigningKey::generate();
    auto key = mesh::SigningKey::generate();
    auto genesis = mesh::make_genesis("duration", {&admin}, {}, 1);
    REQUIRE_OK(genesis);
    auto ledger = mesh::Ledger::create(*genesis);
    REQUIRE_OK(ledger);
    std::atomic<std::int64_t> pin_ms{-1}, passport_ms{-1};
    std::atomic<unsigned> requests{0};
    net::Transport host(key, ledger->mesh(), {},
        [](const net::Identity&) -> std::expected<void, std::string> { return {}; },
        [&](const net::Identity&, mesh::Bytes bytes) {
            ++requests;
            auto decoded = mesh::decode(bytes);
            if (!decoded || !decoded->as_map()) return std::vector<mesh::Bytes>{};
            const mesh::Fields fields{*decoded->as_map()};
            if (fields.str("t") == "pin") pin_ms = fields.integer("duration_ms").value_or(-1);
            if (fields.str("t") == "launch") {
                auto data = fields.bin("passport");
                if (data) {
                    auto passport = mesh::Passport::decode(*data);
                    if (passport) passport_ms = passport->root().expires - passport->root().issued;
                }
            }
            return std::vector<mesh::Bytes>{mesh::encode(mesh::Value(mesh::Map{
                {"ok", mesh::Value(true)}, {"paglet", mesh::Value(std::string(32, 'a'))},
                {"host", mesh::Value::bin(key.public_key())}, {"host_name", mesh::Value("fixture")},
                {"moves", mesh::Value(0)}, {"pin", mesh::Value("pin-1")},
                {"until", mesh::Value(mesh::unix_ms() + 60'000)}}))};
        });
    REQUIRE_OK(host.start());
    pc::Identity identity{std::move(*ledger), std::move(owner), net::PeerRole::owner, "test"};
    auto session = pc::Session::open(host.url(), identity, key.public_key());
    REQUIRE_OK(session);
    REQUIRE_OK(session->pin("demo", 1, "ordinary"));
    CHECK_EQ(pin_ms.load(), 60'000);
    REQUIRE_OK(session->pin("demo", std::numeric_limits<std::int64_t>::max(), "large"));
    CHECK_EQ(pin_ms.load(), 86'400'000);
    REQUIRE_OK(session->launch(identity, {}, {}, {}, "normal", 24));
    CHECK_EQ(passport_ms.load(), 24LL * 3'600'000 + 60'000);
    const auto sent = requests.load();
    // This value wraps to a short positive duration if multiplied unchecked.
    constexpr auto wraps = static_cast<std::int64_t>(std::numeric_limits<std::uint64_t>::max() / 3'600'000 + 1);
    for (const auto hours : {std::int64_t{0}, std::int64_t{-1}, wraps, std::numeric_limits<std::int64_t>::max()}) {
        CHECK(!session->launch(identity, {}, {}, {}, "invalid", hours));
    }
    CHECK_EQ(requests.load(), sent);
}

PAGLETS_TEST("client: shell arguments are quoted only when needed") {
    CHECK_EQ(pc::shell_quote("plain-arg_1.2/x:y=z"), std::string("plain-arg_1.2/x:y=z"));
    CHECK_EQ(pc::shell_quote(""), std::string("''"));
    CHECK_EQ(pc::shell_quote("two words"), std::string("'two words'"));
    CHECK_EQ(pc::shell_quote(R"({"by":1})"), std::string(R"('{"by":1}')"));
    CHECK_EQ(pc::shell_quote("it's"), std::string(R"('it'\''s')"));
    CHECK_EQ(pc::shell_quote("$HOME"), std::string("'$HOME'"));
}

PAGLETS_TEST("client: remote commands carry the connection") {
    pc::ShellContext c;
    c.connect = "https://mac.local:7443";
    c.key_file = "keys/me.key";
    c.ledger_dir = "my ledger";
    CHECK_EQ(pc::remote_command(c, "call", {"abc", "increment", R"({"by":2})"}),
             std::string("paglets-host remote call --connect https://mac.local:7443 --key keys/me.key "
                         "--ledger 'my ledger' abc increment '{\"by\":2}'"));
    c.host_key = "00ff";
    CHECK_EQ(pc::remote_command(c, "hosts"),
             std::string("paglets-host remote hosts --connect https://mac.local:7443 --key keys/me.key "
                         "--ledger 'my ledger' --host-key 00ff"));
}

PAGLETS_TEST("client: an explicit host pin cannot override a recorded revocation") {
    namespace mesh = paglets::mesh;
    namespace net = paglets::net;
    auto admin = mesh::SigningKey::generate();
    auto owner = mesh::SigningKey::generate();
    auto key = mesh::SigningKey::generate();
    auto genesis = mesh::make_genesis("pin", {&admin}, {}, 1);
    REQUIRE_OK(genesis);
    auto ledger = mesh::Ledger::create(*genesis);
    REQUIRE_OK(ledger);
    net::Transport host(key, ledger->mesh(), {},
        [](const net::Identity&) -> std::expected<void, std::string> { return {}; }, {});
    REQUIRE_OK(host.start());
    pc::Identity identity{std::move(*ledger), std::move(owner), net::PeerRole::owner, "test"};
    REQUIRE(!identity.ledger.state().is_host(key.public_key()));
    // An explicit pin still supports bootstrapping an unenrolled host.
    REQUIRE_OK(pc::Session::open(host.url(), identity, key.public_key()));
    auto revoke = identity.ledger.draft("revoke", mesh::data::revoke_key(key.public_key(), "compromised"));
    REQUIRE_OK(revoke);
    revoke->sign(admin);
    REQUIRE_OK(identity.ledger.add(*revoke));
    REQUIRE(identity.ledger.state().revoked_keys.contains(key.public_key()));
    CHECK(!pc::Session::open(host.url(), identity, key.public_key()));
}
