// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/abi.hpp>
#include <paglets/mesh/value.hpp>
#include <paglets/net/channel.hpp>

#include <algorithm>

namespace paglets::net {

namespace {

constexpr std::string_view prologue_text = "paglets channel v1";
constexpr std::int64_t payload_version = 1;
constexpr std::uint8_t flag_last = 1;
// Frame bytes per Noise message: the flag byte and the tag take the rest.
constexpr std::size_t part_bytes = noise_max_message - noise_tag - 1;

Bytes prologue(const mesh::RecordId& mesh_id) {
    Bytes p(prologue_text.begin(), prologue_text.end());
    p.push_back(0);
    p.insert(p.end(), mesh_id.begin(), mesh_id.end());
    return p;
}

KeyPair static_key_of(const mesh::SigningKey& identity) {
    Secret32 secret;
    identity.x25519_secret(secret.bytes());
    return KeyPair::from_secret(secret.bytes());
}

}  // namespace

std::string_view to_string(PeerRole role) {
    switch (role) {
        case PeerRole::host: return "host";
        case PeerRole::admin: return "admin";
        case PeerRole::owner: return "owner";
    }
    return "host";
}

std::optional<PeerRole> parse_peer_role(std::string_view text) {
    if (text == "host") return PeerRole::host;
    if (text == "admin") return PeerRole::admin;
    if (text == "owner") return PeerRole::owner;
    return std::nullopt;
}

// -- Channel ------------------------------------------------------------------------

Channel::Channel(Handshake::Split ciphers, Identity peer, std::array<std::uint8_t, 32> binding, std::size_t max_frame)
    : send_(std::move(ciphers.send)),
      receive_(std::move(ciphers.receive)),
      peer_(peer),
      binding_(binding),
      max_frame_(max_frame) {}

std::vector<Bytes> Channel::seal(std::span<const std::uint8_t> frame) {
    std::vector<Bytes> out;
    std::size_t at = 0;
    Bytes plain;
    do {
        const std::size_t n = std::min(part_bytes, frame.size() - at);
        plain.assign(1, at + n == frame.size() ? flag_last : 0);
        plain.insert(plain.end(), frame.begin() + static_cast<std::ptrdiff_t>(at),
                     frame.begin() + static_cast<std::ptrdiff_t>(at + n));
        out.push_back(send_.encrypt({}, plain));
        at += n;
    } while (at < frame.size());
    return out;
}

std::expected<std::optional<Bytes>, std::string> Channel::open(std::span<const std::uint8_t> message) {
    if (broken_) return std::unexpected(std::string("channel broken"));
    auto fail = [this](std::string why) -> std::expected<std::optional<Bytes>, std::string> {
        broken_ = true;
        partial_.clear();
        return std::unexpected(std::move(why));
    };
    if (message.size() > noise_max_message) return fail("channel message too large");
    auto plain = receive_.decrypt({}, message);
    if (!plain) return fail(plain.error());
    if (plain->empty() || (*plain)[0] > flag_last) return fail("malformed channel message");
    if (partial_.size() + plain->size() - 1 > max_frame_) return fail("frame too large");
    partial_.insert(partial_.end(), plain->begin() + 1, plain->end());
    if ((*plain)[0] != flag_last) return std::optional<Bytes>();
    return std::optional<Bytes>(std::exchange(partial_, {}));
}

// -- ChannelHandshake -------------------------------------------------------------

ChannelHandshake::ChannelHandshake(Handshake::Role role, const mesh::SigningKey& identity, PeerRole my_role,
                                   const mesh::RecordId& mesh_id, AcceptPeer accept, std::size_t max_frame,
                                   std::string my_url, std::int64_t protocol)
    : handshake_(role, static_key_of(identity), prologue(mesh_id)),
      role_(role),
      me_{identity.public_key(), my_role, std::move(my_url), protocol, abi::version, abi::minor_version},
      accept_(std::move(accept)),
      max_frame_(max_frame) {}

Bytes ChannelHandshake::my_payload() const {
    mesh::Map m{{"v", mesh::Value(payload_version)},
                {"key", mesh::Value::bin(me_.key)},
                {"role", mesh::Value(to_string(me_.role))},
                {"proto", mesh::Value(me_.protocol)}};
    if (me_.role == PeerRole::host) {
        m.emplace_back("abi", mesh::Value(mesh::Array{mesh::Value(static_cast<std::int64_t>(me_.abi_major)),
                                                      mesh::Value(static_cast<std::int64_t>(me_.abi_minor))}));
    }
    if (!me_.url.empty()) m.emplace_back("url", mesh::Value(me_.url));
    return mesh::encode(mesh::Value(std::move(m)));
}

std::expected<Bytes, std::string> ChannelHandshake::next_message() {
    // The initiator's first message goes out before any key is agreed: no
    // identity in it. The responder sends its identity in the second, the
    // initiator in the third message.
    const bool first = role_ == Handshake::Role::initiator && !handshake_.remote_static();
    return handshake_.write_message(first ? Bytes{} : my_payload());
}

std::expected<void, std::string> ChannelHandshake::receive(std::span<const std::uint8_t> message) {
    auto payload = handshake_.read_message(message);
    if (!payload) return std::unexpected(payload.error());
    if (!handshake_.remote_static()) {
        // The initiator's first message carries no payload.
        if (!payload->empty()) return std::unexpected(std::string("unexpected handshake payload"));
        return {};
    }
    return check_peer(*payload);
}

std::expected<void, std::string> ChannelHandshake::check_peer(std::span<const std::uint8_t> payload) {
    auto v = mesh::decode(payload);
    if (!v || v->as_map() == nullptr) return std::unexpected(std::string("malformed handshake payload"));
    const mesh::Fields f{*v->as_map()};
    auto version = f.integer("v");
    auto key = f.fixed<32>("key");
    auto role_text = f.str("role");
    auto role = role_text ? parse_peer_role(*role_text) : std::nullopt;
    std::optional<std::string> url = std::string();
    if (f.get("url") != nullptr) url = f.str("url");
    if (!version || *version != payload_version || !key || !role || !url || url->size() > 512) {
        return std::unexpected(std::string("malformed handshake payload"));
    }
    // The identity key must be the one behind the static key the peer proved.
    auto x = mesh::x25519_public(*key);
    if (!x || *x != *handshake_.remote_static()) {
        return std::unexpected(std::string("the peer's identity key does not match its channel key"));
    }
    Identity peer{*key, *role, std::move(*url)};
    peer.protocol = f.integer("proto").value_or(1);
    if (const mesh::Array* a = f.array("abi"); a != nullptr && a->size() == 2 && (*a)[0].as_int() && (*a)[1].as_int()) {
        peer.abi_major = static_cast<std::uint32_t>(std::clamp<std::int64_t>(*(*a)[0].as_int(), 0, 1'000'000));
        peer.abi_minor = static_cast<std::uint32_t>(std::clamp<std::int64_t>(*(*a)[1].as_int(), 0, 1'000'000));
    }
    // The compatibility gate: one mesh protocol; hosts run the same ABI.
    if (peer.protocol != me_.protocol) {
        return std::unexpected("incompatible mesh protocol " + std::to_string(peer.protocol) + " (this side speaks " +
                               std::to_string(me_.protocol) + ")");
    }
    if (peer.role == PeerRole::host && me_.role == PeerRole::host && peer.abi_major != me_.abi_major) {
        return std::unexpected("incompatible paglet ABI " + std::to_string(peer.abi_major) + " (this host runs " +
                               std::to_string(me_.abi_major) + ")");
    }
    if (accept_) {
        if (auto ok = accept_(peer); !ok) return std::unexpected(ok.error());
    }
    peer_ = peer;
    return {};
}

std::expected<Channel, std::string> ChannelHandshake::finish() {
    if (!done() || !peer_) return std::unexpected(std::string("the handshake is not finished"));
    auto split = handshake_.split();
    if (!split) return std::unexpected(split.error());
    return Channel(std::move(*split), *peer_, handshake_.handshake_hash(), max_frame_);
}

}  // namespace paglets::net
