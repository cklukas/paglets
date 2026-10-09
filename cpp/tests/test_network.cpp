// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Channels over HTTPS (WP12, planning/cpp-networking.md, section 3): hosts
// exchange frames through the channels they open to each other's servers,
// CLI sessions ask and get answers, strangers are refused; TLS certificates
// are checked only against a configured CA file.

#include "test.hpp"
#include "tls_fixture.hpp"

#include <paglets/net/transport.hpp>
#include <paglets/sha256.hpp>

#include <chrono>
#include <filesystem>
#include <map>
#include <random>
#include <mutex>
#include <thread>

using namespace paglets::net;
using namespace paglets::test;
namespace mesh = paglets::mesh;
using namespace std::chrono_literals;

namespace {

const mesh::RecordId mesh_id = paglets::sha256(Bytes{'m'});

// A host with a transport that accepts the listed keys and records frames.
struct Host {
    mesh::SigningKey key = mesh::SigningKey::generate();
    std::set<mesh::PublicKey> allowed;
    std::mutex mu;
    std::vector<std::pair<Identity, Bytes>> frames;
    std::vector<std::string> log;
    std::unique_ptr<Transport> transport;

    explicit Host(TransportConfig config = {}) {
        config.log = [this](const std::string& text) {
            std::lock_guard lock(mu);
            log.push_back(text);
        };
        transport = std::make_unique<Transport>(
            key, mesh_id, config,
            [this](const Identity& peer) -> std::expected<void, std::string> {
                std::lock_guard lock(mu);
                if (peer.role == PeerRole::host && allowed.contains(peer.key)) return {};
                if (peer.role == PeerRole::owner) return {};  // CLI sessions of owners
                return std::unexpected(std::string("not a known host"));
            },
            [this](const Identity& from, Bytes frame) -> std::vector<Bytes> {
                std::lock_guard lock(mu);
                frames.emplace_back(from, frame);
                if (from.role == PeerRole::owner) {  // answer CLI sessions: echo reversed
                    std::ranges::reverse(frame);
                    return {frame};
                }
                return {};
            });
        REQUIRE_OK(transport->start());
    }

    std::size_t count() {
        std::lock_guard lock(mu);
        return frames.size();
    }
    bool wait_for(std::size_t n, std::chrono::milliseconds timeout = 10s) {
        const auto until = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < until) {
            if (count() >= n) return true;
            std::this_thread::sleep_for(5ms);
        }
        return count() >= n;
    }
};

void introduce(Host& a, Host& b) {
    a.allowed.insert(b.key.public_key());
    b.allowed.insert(a.key.public_key());
    a.transport->set_address(b.key.public_key(), b.transport->url());
    b.transport->set_address(a.key.public_key(), a.transport->url());
}

Bytes frame_of(std::uint32_t n, std::size_t size = 16) {
    Bytes f(size);
    for (std::size_t i = 0; i < size; ++i) f[i] = static_cast<std::uint8_t>(n + i);
    f[0] = static_cast<std::uint8_t>(n);
    f[1] = static_cast<std::uint8_t>(n >> 8);
    return f;
}

}  // namespace

PAGLETS_TEST("network: hosts exchange frames in order over HTTPS channels") {
    Host a;
    Host b;
    introduce(a, b);
    CHECK(a.transport->url().starts_with("https://127.0.0.1:"));
    for (std::uint32_t i = 0; i < 300; ++i) a.transport->send(b.key.public_key(), frame_of(i));
    const Bytes big = frame_of(7, 3u * 1024 * 1024);
    a.transport->send(b.key.public_key(), big);
    b.transport->send(a.key.public_key(), frame_of(1));
    REQUIRE(b.wait_for(301));
    REQUIRE(a.wait_for(1));
    CHECK(a.transport->flush());
    {
        std::lock_guard lock(b.mu);
        for (std::uint32_t i = 0; i < 300; ++i) {
            CHECK(b.frames[i].first == (Identity{a.key.public_key(), PeerRole::host}));
            CHECK(b.frames[i].second == frame_of(i));
        }
        CHECK(b.frames[300].second == big);
    }
    CHECK_EQ(a.transport->stats().frames_sent, 301u);
    CHECK_EQ(a.transport->stats().channels_opened, 1u);    // one channel carries everything
    CHECK_EQ(b.transport->stats().channels_accepted, 1u);  // a's channel; b sends through its own
    CHECK_EQ(a.transport->stats().channels_accepted, 1u);

    // A host that only knows its seed becomes reachable: its channel
    // announces its address.
    Host c;
    c.allowed.insert(a.key.public_key());
    a.allowed.insert(c.key.public_key());
    c.transport->set_address(a.key.public_key(), a.transport->url());
    c.transport->send(a.key.public_key(), frame_of(9));
    REQUIRE(a.wait_for(2));
    CHECK(a.transport->address(c.key.public_key()) == c.transport->url());
    a.transport->send(c.key.public_key(), frame_of(10));
    REQUIRE(c.wait_for(1));
}

PAGLETS_TEST("network: strangers are refused; unknown addresses drop frames") {
    Host a;
    Host b;
    Host stranger;
    introduce(a, b);
    stranger.transport->set_address(b.key.public_key(), b.transport->url());
    stranger.allowed.insert(b.key.public_key());
    stranger.transport->send(b.key.public_key(), frame_of(1));
    CHECK(stranger.transport->flush());
    CHECK_EQ(stranger.transport->stats().frames_dropped, 1u);
    CHECK_EQ(b.transport->stats().handshakes_refused, 1u);
    CHECK_EQ(b.count(), 0u);
    // A peer that answers at an address with another key is not trusted either.
    a.transport->set_address(stranger.key.public_key(), b.transport->url());
    a.transport->send(stranger.key.public_key(), frame_of(2));
    CHECK(a.transport->flush());
    CHECK_EQ(a.transport->stats().frames_dropped, 1u);
    // No address, no delivery.
    a.transport->send(mesh::SigningKey::generate().public_key(), frame_of(3));
    CHECK(a.transport->flush());
    CHECK_EQ(a.transport->stats().frames_dropped, 2u);
    CHECK_EQ(b.count(), 0u);
}

PAGLETS_TEST("network: a restarted host gets a new channel") {
    Host a;
    auto b = std::make_unique<Host>();
    introduce(a, *b);
    a.transport->send(b->key.public_key(), frame_of(1));
    REQUIRE(b->wait_for(1));
    // b restarts with the same key on another port: a's session is unknown
    // there, so a opens a new channel and nothing is lost.
    TransportConfig same;
    auto key = std::move(b->key);
    b.reset();
    Host restarted;
    restarted.key = std::move(key);
    restarted.transport = std::make_unique<Transport>(
        restarted.key, mesh_id, TransportConfig{},
        [&](const Identity&) -> std::expected<void, std::string> { return {}; },
        [&](const Identity& from, Bytes frame) -> std::vector<Bytes> {
            std::lock_guard lock(restarted.mu);
            restarted.frames.emplace_back(from, std::move(frame));
            return {};
        });
    REQUIRE_OK(restarted.transport->start());
    a.transport->set_address(restarted.key.public_key(), restarted.transport->url());
    a.transport->send(restarted.key.public_key(), frame_of(2));
    REQUIRE(restarted.wait_for(1));
    CHECK_EQ(a.transport->stats().channels_opened, 2u);
}

PAGLETS_TEST("network: CLI sessions ask and get answers; the server's identity is checked") {
    Host host;
    const mesh::SigningKey owner = mesh::SigningKey::generate();
    auto expect_host = [&](const Identity& server) -> std::expected<void, std::string> {
        if (server.key != host.key.public_key()) return std::unexpected(std::string("unexpected server"));
        return {};
    };
    auto session = ClientSession::open(host.transport->url(), owner, PeerRole::owner, mesh_id, expect_host);
    REQUIRE_OK(session);
    CHECK(session->server() == (Identity{host.key.public_key(), PeerRole::host}));
    auto answer = session->exchange({Bytes{1, 2, 3}, Bytes{4, 5}});
    REQUIRE_OK(answer);
    CHECK_EQ(answer->size(), 2u);
    CHECK((*answer)[0] == (Bytes{3, 2, 1}));
    CHECK((*answer)[1] == (Bytes{5, 4}));
    // Admin sessions are not accepted by this host, and the server must be
    // the host the client expects.
    const mesh::SigningKey admin = mesh::SigningKey::generate();
    CHECK(!ClientSession::open(host.transport->url(), admin, PeerRole::admin, mesh_id, expect_host));
    auto other = [](const Identity&) -> std::expected<void, std::string> {
        return std::unexpected(std::string("not the host I want"));
    };
    CHECK(!ClientSession::open(host.transport->url(), owner, PeerRole::owner, mesh_id, other));
    // Another mesh fails the handshake.
    CHECK(!ClientSession::open(host.transport->url(), owner, PeerRole::owner, paglets::sha256(Bytes{'x'}), {}));
}

PAGLETS_TEST("network: CLI sessions wait read_timeout for answers that take longer than the timeout") {
    // A host that answers after 2 s (as a host answers a call when the
    // paglet replied).
    const mesh::SigningKey key = mesh::SigningKey::generate();
    Transport slow(
        key, mesh_id, TransportConfig{}, [](const Identity&) -> std::expected<void, std::string> { return {}; },
        [](const Identity&, Bytes frame) -> std::vector<Bytes> {
            std::this_thread::sleep_for(2s);
            return {frame};
        });
    REQUIRE_OK(slow.start());
    const mesh::SigningKey owner = mesh::SigningKey::generate();
    TransportConfig client;
    client.timeout = 1s;
    auto impatient = ClientSession::open(slow.url(), owner, PeerRole::owner, mesh_id, {}, client);
    REQUIRE_OK(impatient);
    CHECK(!impatient->exchange({Bytes{1}}));  // gave up after 1 s: the answer is lost
    client.read_timeout = 5s;
    auto patient = ClientSession::open(slow.url(), owner, PeerRole::owner, mesh_id, {}, client);
    REQUIRE_OK(patient);
    auto answer = patient->exchange({Bytes{2}});
    REQUIRE_OK(answer);
    CHECK(*answer == std::vector<Bytes>{Bytes{2}});
}

PAGLETS_TEST("network: self-signed certificates") {
    auto c = self_signed_certificate("paglets test");
    REQUIRE_OK(c);
    CHECK(c->cert_pem.starts_with("-----BEGIN CERTIFICATE-----"));
    CHECK(c->key_pem.find("PRIVATE KEY") != std::string::npos);
}

PAGLETS_TEST("network: TLS certificates are checked against a CA file; chains are sent along") {
    namespace fs = std::filesystem;
    const fs::path dir = fs::temp_directory_path() / ("paglets-tls-" + std::to_string(std::random_device{}()));
    fs::create_directories(dir);
    const TestCert root = make_test_cert("test root", nullptr, true);
    const TestCert intermediate = make_test_cert("test intermediate", &root, true);
    const TestCert other_root = make_test_cert("other root", nullptr, true);
    const fs::path root_file = write_test_file(dir / "root.pem", root.cert);
    // A host serving `cert` (followed by `chain`), checking peers against
    // the test root when `check`.
    auto config = [&](const std::string& name, const TestCert& cert, const std::string& chain, bool check) {
        TransportConfig c;
        c.tls_cert = write_test_file(dir / (name + ".crt.pem"), cert.cert + chain);
        c.tls_key = write_test_file(dir / (name + ".key.pem"), cert.key);
        if (check) c.tls_ca = root_file;
        return c;
    };
    auto logged = [](Host& h, std::string_view text) {
        std::lock_guard lock(h.mu);
        return std::ranges::any_of(h.log, [&](const std::string& l) { return l.find(text) != std::string::npos; });
    };

    // a's certificate comes from the root, b's from an intermediate CA: b
    // sends the intermediate along, so a root CA file is enough for both.
    Host a(config("a", make_test_cert("a", &root, false), "", true));
    Host b(config("b", make_test_cert("b", &intermediate, false), intermediate.cert, true));
    introduce(a, b);
    a.transport->send(b.key.public_key(), frame_of(1));
    b.transport->send(a.key.public_key(), frame_of(2));
    REQUIRE(b.wait_for(1));
    REQUIRE(a.wait_for(1));

    // Hosts that check refuse a certificate of another CA, and say why; the
    // host itself does not check, so its own frames still arrive.
    Host c(config("c", make_test_cert("c", &other_root, false), "", false));
    introduce(a, c);
    a.transport->send(c.key.public_key(), frame_of(3));
    CHECK(a.transport->flush(10s));
    CHECK_EQ(c.count(), 0u);
    CHECK_EQ(a.transport->stats().frames_dropped, 1u);
    CHECK(logged(a, "SSL server verification failed (unable to get local issuer certificate)"));
    c.transport->send(a.key.public_key(), frame_of(4));
    REQUIRE(a.wait_for(2));

    // An expired certificate: its host warns at start, hosts that check refuse it.
    Host d(config("d", make_test_cert("d", &root, false, -10L * 24 * 3600, -24L * 3600), "", false));
    CHECK(logged(d, "expired on"));
    introduce(a, d);
    a.transport->send(d.key.public_key(), frame_of(5));
    CHECK(a.transport->flush(10s));
    CHECK_EQ(d.count(), 0u);
    CHECK(logged(a, "(certificate has expired)"));

    // CLI sessions do not check certificates: the Noise channel checks the host.
    const mesh::SigningKey owner = mesh::SigningKey::generate();
    CHECK(ClientSession::open(c.transport->url(), owner, PeerRole::owner, mesh_id, {}).has_value());

    // Files that cannot be used are refused at start, not at every connection.
    auto start_error = [&](TransportConfig cfg) {
        Transport t(a.key, mesh_id, std::move(cfg), {}, {});
        auto started = t.start();
        return started ? std::string() : started.error();
    };
    TransportConfig bad;
    bad.tls_ca = dir / "missing.pem";
    CHECK(start_error(bad).find("cannot read the TLS CA file") != std::string::npos);
    bad.listen = false;  // hosts without an inbound port check it too
    CHECK(start_error(bad).find("cannot read the TLS CA file") != std::string::npos);
    bad = {};
    bad.tls_ca = dir / "a.key.pem";
    CHECK(start_error(bad).find("holds no PEM certificate") != std::string::npos);
    bad = config("e", make_test_cert("e", &root, false), "", false);
    bad.tls_key = dir / "b.key.pem";
    CHECK(start_error(bad).find("does not belong to the TLS certificate") != std::string::npos);
    bad.tls_key.clear();
    CHECK(start_error(bad).find("needs both") != std::string::npos);
    fs::remove_all(dir);
}

PAGLETS_TEST("network: failed deliveries are logged once, summarized, and when they recover") {
    TransportConfig quick;
    quick.failure_log_interval = 300ms;
    Host a(quick);
    Host b;
    introduce(a, b);
    auto count_logged = [&](std::string_view text) {
        std::lock_guard lock(a.mu);
        return std::ranges::count_if(a.log, [&](const std::string& l) { return l.find(text) != std::string::npos; });
    };
    const auto b_key = b.key.public_key();
    // b's address leads nowhere first.
    a.transport->set_address(b_key, "https://127.0.0.1:1");
    CHECK(!a.transport->send_failure(b_key));
    for (std::uint32_t i = 0; i < 5; ++i) {
        a.transport->send(b_key, frame_of(i));
        CHECK(a.transport->flush(10s));
    }
    CHECK_EQ(a.transport->stats().frames_dropped, 5u);
    CHECK_EQ(count_logged("dropped:"), 1);  // the first failure only
    REQUIRE(a.transport->send_failure(b_key).has_value());
    CHECK(a.transport->send_failure(b_key)->find("opening a channel to https://127.0.0.1:1") != std::string::npos);
    // Repeats are summarized at most every interval.
    std::this_thread::sleep_for(350ms);
    a.transport->send(b_key, frame_of(5));
    CHECK(a.transport->flush(10s));
    CHECK_EQ(count_logged("still dropped (5 more times"), 1);
    // A new reason is logged at once.
    a.transport->set_address(b_key, "https://127.0.0.1:2");
    a.transport->send(b_key, frame_of(6));
    CHECK(a.transport->flush(10s));
    CHECK_EQ(count_logged("dropped: opening a channel to https://127.0.0.1:2"), 1);
    // The right address: delivered again, and logged.
    a.transport->set_address(b_key, b.transport->url());
    a.transport->send(b_key, frame_of(7));
    REQUIRE(b.wait_for(1));
    CHECK(a.transport->flush(10s));
    CHECK(!a.transport->send_failure(b_key));
    CHECK_EQ(count_logged("frames to " + mesh::key_id(b_key).substr(0, 16) + " delivered again"), 1);
}

PAGLETS_TEST("network: hosts of another mesh protocol are refused; probe finds a host by its address") {
    Host a;
    TransportConfig other;
    other.protocol = mesh_protocol + 1;
    Host b(other);
    introduce(a, b);
    a.transport->send(b.key.public_key(), frame_of(1));
    b.transport->send(a.key.public_key(), frame_of(2));
    CHECK(a.transport->flush(10s));
    CHECK(b.transport->flush(10s));
    CHECK_EQ(a.count(), 0u);
    CHECK_EQ(b.count(), 0u);
    CHECK_EQ(a.transport->stats().frames_dropped, 1u);
    CHECK_EQ(b.transport->stats().frames_dropped, 1u);
    CHECK(std::ranges::any_of(
        a.log, [](const std::string& l) { return l.find("incompatible mesh protocol") != std::string::npos; }));

    // A bootstrap contact: only its address is known.
    Host c;
    c.allowed.insert(a.key.public_key());
    a.allowed.insert(c.key.public_key());
    auto found = c.transport->probe(a.transport->url());
    REQUIRE_OK(found);
    CHECK(*found == a.key.public_key());
    CHECK(c.transport->address(a.key.public_key()) == a.transport->url());
    c.transport->send(a.key.public_key(), frame_of(3));
    REQUIRE(a.wait_for(1));
    // A host it does not accept, or one of the other protocol, is no contact.
    CHECK(!c.transport->probe(b.transport->url()));
    c.allowed.insert(b.key.public_key());
    b.allowed.insert(c.key.public_key());
    auto refused = c.transport->probe(b.transport->url());
    REQUIRE(!refused);
    CHECK(refused.error().find("incompatible") != std::string::npos);
}
