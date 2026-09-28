// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The Noise protocol framework (revision 34), pattern XX with X25519,
// ChaCha20-Poly1305 and SHA-256: Noise_XX_25519_ChaChaPoly_SHA256, built on
// libsodium. Channels between hosts and CLI sessions run it on top of the
// HTTPS transport (planning/cpp-networking.md, section 2):
//
//   -> e
//   <- e, ee, s, es
//   -> s, se
//
// Both sides prove possession of their static keys (the X25519 forms of
// their Ed25519 identity keys); ephemeral keys give forward secrecy. After
// the handshake, split() yields the two transport cipher states.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::net {

using Bytes = std::vector<std::uint8_t>;
using Key32 = std::array<std::uint8_t, 32>;

inline constexpr std::string_view noise_protocol = "Noise_XX_25519_ChaChaPoly_SHA256";
// Largest Noise message (the specification's limit).
inline constexpr std::size_t noise_max_message = 65535;
inline constexpr std::size_t noise_tag = 16;

// A 32-byte secret in locked, guarded memory (sodium_malloc).
class Secret32 {
public:
    Secret32();
    ~Secret32();
    Secret32(Secret32&& other) noexcept;
    Secret32& operator=(Secret32&& other) noexcept;
    Secret32(const Secret32&) = delete;
    Secret32& operator=(const Secret32&) = delete;

    std::span<std::uint8_t, 32> bytes() { return std::span<std::uint8_t, 32>(data_, 32); }
    std::span<const std::uint8_t, 32> bytes() const { return std::span<const std::uint8_t, 32>(data_, 32); }

private:
    std::uint8_t* data_ = nullptr;
};

// An X25519 key pair.
struct KeyPair {
    Secret32 secret;
    Key32 public_key{};

    static KeyPair generate();
    static KeyPair from_secret(std::span<const std::uint8_t, 32> secret);
};

// CipherState: ChaCha20-Poly1305 (IETF) with a 64-bit counter nonce.
class CipherState {
public:
    void initialize(std::span<const std::uint8_t, 32> key);
    bool has_key() const { return has_key_; }
    Bytes encrypt(std::span<const std::uint8_t> ad, std::span<const std::uint8_t> plaintext);
    std::expected<Bytes, std::string> decrypt(std::span<const std::uint8_t> ad,
                                              std::span<const std::uint8_t> ciphertext);
    std::uint64_t nonce() const { return nonce_; }

private:
    Secret32 key_;
    bool has_key_ = false;
    std::uint64_t nonce_ = 0;
};

class Handshake {
public:
    enum class Role { initiator, responder };

    // `ephemeral` is for test vectors only; normally a fresh key is made.
    Handshake(Role role, KeyPair static_key, std::span<const std::uint8_t> prologue,
              std::optional<KeyPair> ephemeral = std::nullopt);

    // The next handshake message with `payload`, or the payload of the
    // peer's next message. Calls must follow the pattern's order.
    std::expected<Bytes, std::string> write_message(std::span<const std::uint8_t> payload);
    std::expected<Bytes, std::string> read_message(std::span<const std::uint8_t> message);

    bool finished() const { return step_ == 3; }
    bool my_turn() const;  // is the next message ours to write?
    // The peer's static key (known after its `s` token) and the handshake
    // hash (channel binding; final once finished).
    const std::optional<Key32>& remote_static() const { return rs_; }
    const std::array<std::uint8_t, 32>& handshake_hash() const { return h_; }

    // After the handshake: the cipher states for sending and receiving.
    struct Split {
        CipherState send;
        CipherState receive;
    };
    std::expected<Split, std::string> split();

private:
    void mix_hash(std::span<const std::uint8_t> data);
    void mix_key(std::span<const std::uint8_t> input);
    Bytes encrypt_and_hash(std::span<const std::uint8_t> plaintext);
    std::expected<Bytes, std::string> decrypt_and_hash(std::span<const std::uint8_t> ciphertext);
    std::expected<void, std::string> dh_mix(const Secret32& secret, const Key32& remote);

    Role role_;
    KeyPair s_;
    std::optional<KeyPair> e_;
    std::optional<KeyPair> test_ephemeral_;
    std::optional<Key32> re_;
    std::optional<Key32> rs_;
    std::array<std::uint8_t, 32> h_{};
    Secret32 ck_;
    CipherState cipher_;
    int step_ = 0;  // messages exchanged
    bool failed_ = false;
};

}  // namespace paglets::net
