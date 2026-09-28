// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Noise XX (WP12): the Noise_XX_25519_ChaChaPoly_SHA256 test vector of the
// cacophony suite, and channels between identity keys.

#include "test.hpp"

#include <paglets/mesh/crypto.hpp>
#include <paglets/net/channel.hpp>
#include <paglets/net/noise.hpp>
#include <paglets/sha256.hpp>

#include <string>

using namespace paglets::net;
namespace mesh = paglets::mesh;

namespace {

Bytes unhex(std::string_view hex) {
    Bytes out;
    for (std::size_t i = 0; i + 1 < hex.size(); i += 2) {
        out.push_back(static_cast<std::uint8_t>(std::stoi(std::string(hex.substr(i, 2)), nullptr, 16)));
    }
    return out;
}

std::string hex(std::span<const std::uint8_t> b) {
    return paglets::to_hex(b);
}

KeyPair key(std::string_view secret_hex) {
    const Bytes s = unhex(secret_hex);
    return KeyPair::from_secret(std::span<const std::uint8_t, 32>(s.data(), 32));
}

// Runs a channel handshake between two sides; returns both channels or the
// first error.
std::expected<std::pair<Channel, Channel>, std::string> connect(ChannelHandshake& a, ChannelHandshake& b) {
    ChannelHandshake* sides[2] = {&a, &b};
    for (int turn = 0; !a.done() || !b.done(); ++turn) {
        ChannelHandshake& writer = *sides[turn % 2];
        ChannelHandshake& reader = *sides[(turn + 1) % 2];
        auto m = writer.next_message();
        if (!m) return std::unexpected(m.error());
        if (auto ok = reader.receive(*m); !ok) return std::unexpected(ok.error());
    }
    auto ca = a.finish();
    auto cb = b.finish();
    if (!ca) return std::unexpected(ca.error());
    if (!cb) return std::unexpected(cb.error());
    return std::pair<Channel, Channel>{std::move(*ca), std::move(*cb)};
}

// Sends a frame through a channel pair and returns what arrived.
std::optional<Bytes> transfer(Channel& from, Channel& to, std::span<const std::uint8_t> frame) {
    std::optional<Bytes> out;
    for (const auto& m : from.seal(frame)) {
        auto r = to.open(m);
        if (!r) return std::nullopt;
        if (*r) out = std::move(**r);
    }
    return out;
}

const mesh::RecordId mesh_a = paglets::sha256(Bytes{'a'});
const mesh::RecordId mesh_b = paglets::sha256(Bytes{'b'});

}  // namespace

PAGLETS_TEST("noise: Noise_XX_25519_ChaChaPoly_SHA256 test vector (cacophony)") {
    // From the cacophony test vectors as distributed with the snow library
    // (tests/vectors/cacophony.txt, MIT license).
    const Bytes prologue = unhex("4a6f686e2047616c74");
    Handshake init(Handshake::Role::initiator, key("e61ef9919cde45dd5f82166404bd08e38bceb5dfdfded0a34c8df7ed542214d1"),
                   prologue, key("893e28b9dc6ca8d611ab664754b8ceb7bac5117349a4439a6b0569da977c464a"));
    Handshake resp(Handshake::Role::responder, key("4a3acbfdb163dec651dfa3194dece676d437029c62a408b4c5ea9114246e4893"),
                   prologue, key("bbdb4cdbd309f1a1f2e1456967fe288cadd6f712d65dc7b7793d5e63da6b375b"));
    const std::pair<std::string_view, std::string_view> messages[] = {
        {"4c756477696720766f6e204d69736573",
         "ca35def5ae56cec33dc2036731ab14896bc4c75dbb07a61f879f8e3afa4c79444c756477696720766f6e204d69736573"},
        {"4d757272617920526f746862617264",
         "95ebc60d2b1fa672c1f46a8aa265ef51bfe38e7ccb39ec5be34069f14480884381cbad1f276e038c48378ffce2b65285e08d6b68aaa36"
         "29a5a8639392490e5b9bd5269c2f1e4f488ed8831161f19b7815528f8982ffe09be9b5c412f8a0db50f8814c7194e83f23dbd8d162c"
         "9326ad"},
        {"462e20412e20486179656b",
         "c7195ffacac1307ff99046f219750fc47693e23c3cb08b89c2af808b444850a80ae475b9df0f169ae80a89be0865b57f58c9fea0d4ec8"
         "2"
         "a286427402f113e4b6ae769a1d95941d49b25030"},
        {"4361726c204d656e676572", "96763ed773f8e47bb3712f0e29b3060ffc956ffc146cee53d5e1df"},
        {"4a65616e2d426170746973746520536179", "3e40f15f6f3a46ae446b253bf8b1d9ffb6ed9b174d272328ff91a7e2e5c79c07f5"},
        {"457567656e2042f6686d20766f6e2042617765726b",
         "eb3f3515110702e047a6c9da4478b6ead94873c11c0f2d710ddb3f09fce024b3a58502ae3f"},
    };
    // Handshake: initiator, responder, initiator.
    for (int i = 0; i < 3; ++i) {
        Handshake& writer = i % 2 == 0 ? init : resp;
        Handshake& reader = i % 2 == 0 ? resp : init;
        auto m = writer.write_message(unhex(messages[i].first));
        REQUIRE_OK(m);
        CHECK_EQ(hex(*m), std::string(messages[i].second));
        auto payload = reader.read_message(*m);
        REQUIRE_OK(payload);
        CHECK(*payload == unhex(messages[i].first));
    }
    REQUIRE(init.finished() && resp.finished());
    CHECK_EQ(hex(init.handshake_hash()),
             std::string("c8e5f64e846193be2a834104c2a009868d6c9f3bd3c186299888b488b2f1f58e"));
    CHECK(init.handshake_hash() == resp.handshake_hash());
    CHECK(*init.remote_static() == key("4a3acbfdb163dec651dfa3194dece676d437029c62a408b4c5ea9114246e4893").public_key);
    // Transport messages continue to alternate: responder, initiator, responder.
    auto si = init.split();
    auto sr = resp.split();
    REQUIRE_OK(si);
    REQUIRE_OK(sr);
    for (int i = 3; i < 6; ++i) {
        Handshake::Split& writer = i % 2 == 0 ? *si : *sr;
        Handshake::Split& reader = i % 2 == 0 ? *sr : *si;
        const Bytes c = writer.send.encrypt({}, unhex(messages[i].first));
        CHECK_EQ(hex(c), std::string(messages[i].second));
        auto p = reader.receive.decrypt({}, c);
        REQUIRE_OK(p);
        CHECK(*p == unhex(messages[i].first));
    }
    CHECK(!init.write_message({}).has_value());  // a finished handshake writes nothing
}

PAGLETS_TEST("noise: identity keys and their X25519 forms agree") {
    const mesh::SigningKey k = mesh::SigningKey::generate();
    Secret32 secret;
    k.x25519_secret(secret.bytes());
    const auto pair = KeyPair::from_secret(secret.bytes());
    auto converted = mesh::x25519_public(k.public_key());
    REQUIRE(converted.has_value());
    CHECK(*converted == pair.public_key);
}

PAGLETS_TEST("channels: identities, frames of any size, tampering and replays") {
    const mesh::SigningKey host_a = mesh::SigningKey::generate();
    const mesh::SigningKey host_b = mesh::SigningKey::generate();
    std::vector<Identity> seen;
    auto accept = [&](const Identity& id) -> std::expected<void, std::string> {
        seen.push_back(id);
        return {};
    };
    ChannelHandshake a(Handshake::Role::initiator, host_a, PeerRole::host, mesh_a, accept);
    ChannelHandshake b(Handshake::Role::responder, host_b, PeerRole::host, mesh_a, accept);
    auto channels = connect(a, b);
    REQUIRE_OK(channels);
    auto& [ca, cb] = *channels;
    CHECK(ca.peer() == (Identity{host_b.public_key(), PeerRole::host}));
    CHECK(cb.peer() == (Identity{host_a.public_key(), PeerRole::host}));
    CHECK(ca.binding() == cb.binding());
    CHECK_EQ(seen.size(), 2u);

    const Bytes small = {1, 2, 3};
    Bytes large(300'000);
    for (std::size_t i = 0; i < large.size(); ++i) large[i] = static_cast<std::uint8_t>(i * 7);
    CHECK(transfer(ca, cb, small) == small);
    CHECK(transfer(ca, cb, Bytes{}) == Bytes{});
    auto parts_large = cb.seal(large);
    CHECK_EQ(parts_large.size(), 5u);  // 300 000 bytes in parts of 65 518
    std::optional<Bytes> received;
    for (const auto& m : parts_large) {
        auto r = ca.open(m);
        REQUIRE_OK(r);
        if (*r) received = std::move(**r);
    }
    CHECK(received == large);

    // A replayed or altered message breaks the channel.
    auto parts = ca.seal(small);
    REQUIRE(cb.open(parts[0]).has_value());
    CHECK(!cb.open(parts[0]).has_value());
    CHECK(cb.broken());
    auto altered = cb.seal(small);
    altered[0][5] ^= 1;
    CHECK(!ca.open(altered[0]).has_value());
}

PAGLETS_TEST("channels: refused peers, other meshes and oversized frames") {
    const mesh::SigningKey host = mesh::SigningKey::generate();
    const mesh::SigningKey owner = mesh::SigningKey::generate();
    // The host accepts only hosts; the owner's session is refused after its
    // identity arrives (third message).
    auto hosts_only = [](const Identity& id) -> std::expected<void, std::string> {
        if (id.role != PeerRole::host) return std::unexpected(std::string("hosts only"));
        return {};
    };
    ChannelHandshake cli(Handshake::Role::initiator, owner, PeerRole::owner, mesh_a, {});
    ChannelHandshake server(Handshake::Role::responder, host, PeerRole::host, mesh_a, hosts_only);
    auto refused = connect(cli, server);
    REQUIRE(!refused.has_value());
    CHECK_EQ(refused.error(), std::string("hosts only"));

    // Different meshes fail to agree on keys.
    ChannelHandshake x(Handshake::Role::initiator, owner, PeerRole::owner, mesh_a, {});
    ChannelHandshake y(Handshake::Role::responder, host, PeerRole::host, mesh_b, {});
    CHECK(!connect(x, y).has_value());

    // Frames over the receiver's limit are refused.
    ChannelHandshake p(Handshake::Role::initiator, owner, PeerRole::owner, mesh_a, {});
    ChannelHandshake q(Handshake::Role::responder, host, PeerRole::host, mesh_a, {}, 100'000);
    auto channels = connect(p, q);
    REQUIRE_OK(channels);
    CHECK(transfer(channels->first, channels->second, Bytes(100'000)).has_value());
    CHECK(!transfer(channels->first, channels->second, Bytes(100'001)).has_value());
    CHECK(channels->second.broken());
}
