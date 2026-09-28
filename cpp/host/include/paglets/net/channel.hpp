// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// End-to-end channels (planning/cpp-networking.md, section 2): a Noise XX
// handshake between two identity keys of a mesh, then encrypted frames.
//
// - The static Noise keys are the X25519 forms of the peers' Ed25519
//   identity keys (host, admin or owner keys). Each side sends its Ed25519
//   key and role in its (encrypted) handshake payload; the other side checks
//   that it converts to the static key the handshake proved, so a channel
//   peer is an identity key, which the caller then checks against the
//   ledger (an enrolled host, an admin, an owner).
// - The prologue binds the channel to the protocol version and the mesh:
//   peers of different meshes fail the handshake.
// - Frames of any size (up to a limit) are split into Noise messages; each
//   carries a flag byte (1: last part of the frame).

#pragma once

#include <paglets/mesh/crypto.hpp>
#include <paglets/mesh/record.hpp>
#include <paglets/net/noise.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::net {

// The role an identity claims in a channel: hosts connect to hosts; CLI
// sessions connect with admin or owner keys.
enum class PeerRole { host, admin, owner };
std::string_view to_string(PeerRole role);
std::optional<PeerRole> parse_peer_role(std::string_view text);

struct Identity {
    mesh::PublicKey key{};
    PeerRole role = PeerRole::host;
    // Where the peer says it can be reached (hosts; may be empty). Advisory:
    // identities compare by key and role.
    std::string url;
    bool operator==(const Identity& o) const { return key == o.key && role == o.role; }
};

// Decides whether the proven peer may use the channel (for example: an
// enrolled host of the ledger); returns why not otherwise.
using AcceptPeer = std::function<std::expected<void, std::string>(const Identity&)>;

// Largest frame a channel accepts by default.
inline constexpr std::size_t channel_max_frame = 64u * 1024 * 1024;

class Channel {
public:
    // Encrypts a frame into Noise messages.
    std::vector<Bytes> seal(std::span<const std::uint8_t> frame);
    // Decrypts the next Noise message from the peer; returns the frame when
    // its last part arrived. Any error ends the channel.
    std::expected<std::optional<Bytes>, std::string> open(std::span<const std::uint8_t> message);

    const Identity& peer() const { return peer_; }
    const std::array<std::uint8_t, 32>& binding() const { return binding_; }  // handshake hash
    bool broken() const { return broken_; }

private:
    friend class ChannelHandshake;
    Channel(Handshake::Split ciphers, Identity peer, std::array<std::uint8_t, 32> binding, std::size_t max_frame);

    CipherState send_;
    CipherState receive_;
    Identity peer_;
    std::array<std::uint8_t, 32> binding_{};
    std::size_t max_frame_;
    Bytes partial_;
    bool broken_ = false;
};

// The handshake of a channel. The initiator writes first; each side
// alternates next_message() and receive() until done().
class ChannelHandshake {
public:
    ChannelHandshake(Handshake::Role role, const mesh::SigningKey& identity, PeerRole my_role,
                     const mesh::RecordId& mesh_id, AcceptPeer accept, std::size_t max_frame = channel_max_frame,
                     std::string my_url = {});

    std::expected<Bytes, std::string> next_message();
    std::expected<void, std::string> receive(std::span<const std::uint8_t> message);
    bool my_turn() const { return handshake_.my_turn(); }
    bool done() const { return handshake_.finished(); }
    // After done(): the channel.
    std::expected<Channel, std::string> finish();

private:
    std::expected<void, std::string> check_peer(std::span<const std::uint8_t> payload);
    Bytes my_payload() const;

    Handshake handshake_;
    Handshake::Role role_;
    Identity me_;
    AcceptPeer accept_;
    std::size_t max_frame_;
    std::optional<Identity> peer_;
};

}  // namespace paglets::net
