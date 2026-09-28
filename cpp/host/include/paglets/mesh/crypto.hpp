// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Keys of the mesh (planning/cpp-security-and-communication.md, section 3.1):
// Ed25519 key pairs for admins, hosts and owners, built on libsodium.
// Private keys never leave the machine that made them; admin and owner key
// files are encrypted with a key derived from a local passphrase (Argon2id,
// XChaCha20-Poly1305), host key files are protected by file permissions.

#pragma once

#include <array>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::mesh {

using Bytes = std::vector<std::uint8_t>;
using PublicKey = std::array<std::uint8_t, 32>;
using Signature = std::array<std::uint8_t, 64>;

// Initializes libsodium once; safe from any thread.
void ensure_crypto();

// Hex form of a public key: the key ID used in records.
std::string key_id(const PublicKey& key);
std::optional<PublicKey> parse_key_id(std::string_view id);

bool verify(const PublicKey& key, std::span<const std::uint8_t> message, const Signature& signature);

// The X25519 form of an Ed25519 public key; nullopt for keys that are not
// valid curve points.
std::optional<std::array<std::uint8_t, 32>> x25519_public(const PublicKey& key);

// Signer keys sign modules (module_trust.hpp); like admin and owner keys they
// are always encrypted.
enum class KeyRole { admin, owner, host, signer };
std::string_view to_string(KeyRole role);
std::optional<KeyRole> parse_key_role(std::string_view text);

// An Ed25519 signing key. The secret stays in memory the library locks and
// wipes; the object is movable, not copyable.
class SigningKey {
public:
    static SigningKey generate();
    static std::expected<SigningKey, std::string> from_seed(std::span<const std::uint8_t> seed);

    SigningKey(SigningKey&& other) noexcept;
    SigningKey& operator=(SigningKey&& other) noexcept;
    SigningKey(const SigningKey&) = delete;
    SigningKey& operator=(const SigningKey&) = delete;
    ~SigningKey();

    const PublicKey& public_key() const { return public_; }
    std::string id() const { return key_id(public_); }
    Signature sign(std::span<const std::uint8_t> message) const;
    // The X25519 form of the secret key, for key exchange in channels
    // (planning/cpp-networking.md). The caller keeps it in locked memory.
    void x25519_secret(std::span<std::uint8_t, 32> out) const;

private:
    friend std::expected<void, std::string> save_key(const std::filesystem::path& path, const SigningKey& key,
                                                     KeyRole role, std::string_view name,
                                                     std::optional<std::string_view> passphrase, bool fast_kdf);
    SigningKey();
    std::uint8_t* secret_ = nullptr;  // 64 bytes (seed and public key), sodium_malloc
    PublicKey public_{};
};

struct KeyInfo {
    KeyRole role = KeyRole::owner;
    std::string name;
    PublicKey public_key{};
    bool encrypted = false;
};

// Key files. Admin and owner keys need a passphrase (Argon2id with the
// "moderate" limits unless `fast_kdf` is set, which tests use); host keys
// are stored unencrypted with owner-only permissions.
std::expected<void, std::string> save_key(const std::filesystem::path& path, const SigningKey& key, KeyRole role,
                                          std::string_view name, std::optional<std::string_view> passphrase,
                                          bool fast_kdf = false);
std::expected<SigningKey, std::string> load_key(const std::filesystem::path& path,
                                                std::optional<std::string_view> passphrase);
std::expected<KeyInfo, std::string> read_key_info(const std::filesystem::path& path);

// Secure random bytes.
void random_bytes(std::span<std::uint8_t> out);

}  // namespace paglets::mesh
