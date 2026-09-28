// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/net/noise.hpp>

#include <sodium.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

namespace paglets::net {

namespace {

void ensure_sodium() {
    static const int ok = sodium_init();
    if (ok < 0) throw std::runtime_error("libsodium initialization failed");
}

using Hash = std::array<std::uint8_t, 32>;

void hmac(std::span<const std::uint8_t> key, std::initializer_list<std::span<const std::uint8_t>> data,
          std::span<std::uint8_t, 32> out) {
    crypto_auth_hmacsha256_state st;
    crypto_auth_hmacsha256_init(&st, key.data(), key.size());
    for (const auto& d : data) crypto_auth_hmacsha256_update(&st, d.data(), d.size());
    crypto_auth_hmacsha256_final(&st, out.data());
    sodium_memzero(&st, sizeof st);
}

// HKDF of the Noise specification (section 4.3) with two outputs.
void hkdf2(std::span<const std::uint8_t, 32> chaining_key, std::span<const std::uint8_t> input,
           std::span<std::uint8_t, 32> out1, std::span<std::uint8_t, 32> out2) {
    Secret32 temp;
    hmac(chaining_key, {input}, temp.bytes());
    const std::uint8_t one = 1;
    const std::uint8_t two = 2;
    hmac(temp.bytes(), {std::span<const std::uint8_t>(&one, 1)}, out1);
    hmac(temp.bytes(), {std::span<const std::uint8_t>(out1), std::span<const std::uint8_t>(&two, 1)}, out2);
}

std::array<std::uint8_t, crypto_aead_chacha20poly1305_ietf_NPUBBYTES> nonce_bytes(std::uint64_t n) {
    std::array<std::uint8_t, crypto_aead_chacha20poly1305_ietf_NPUBBYTES> nonce{};
    for (int i = 0; i < 8; ++i) nonce[4 + i] = static_cast<std::uint8_t>(n >> (8 * i));
    return nonce;
}

}  // namespace

// -- Secret32 -------------------------------------------------------------------

Secret32::Secret32() {
    ensure_sodium();
    data_ = static_cast<std::uint8_t*>(sodium_malloc(32));
    if (data_ == nullptr) throw std::bad_alloc();
    sodium_memzero(data_, 32);
}

Secret32::~Secret32() {
    if (data_ != nullptr) sodium_free(data_);
}

Secret32::Secret32(Secret32&& other) noexcept : data_(std::exchange(other.data_, nullptr)) {}

Secret32& Secret32::operator=(Secret32&& other) noexcept {
    if (this != &other) {
        if (data_ != nullptr) sodium_free(data_);
        data_ = std::exchange(other.data_, nullptr);
    }
    return *this;
}

// -- KeyPair --------------------------------------------------------------------

KeyPair KeyPair::generate() {
    ensure_sodium();
    KeyPair k;
    randombytes_buf(k.secret.bytes().data(), 32);
    crypto_scalarmult_base(k.public_key.data(), k.secret.bytes().data());
    return k;
}

KeyPair KeyPair::from_secret(std::span<const std::uint8_t, 32> secret) {
    ensure_sodium();
    KeyPair k;
    std::copy(secret.begin(), secret.end(), k.secret.bytes().begin());
    crypto_scalarmult_base(k.public_key.data(), k.secret.bytes().data());
    return k;
}

// -- CipherState ------------------------------------------------------------------

void CipherState::initialize(std::span<const std::uint8_t, 32> key) {
    std::copy(key.begin(), key.end(), key_.bytes().begin());
    has_key_ = true;
    nonce_ = 0;
}

Bytes CipherState::encrypt(std::span<const std::uint8_t> ad, std::span<const std::uint8_t> plaintext) {
    if (!has_key_) return Bytes(plaintext.begin(), plaintext.end());
    // The maximum nonce is reserved; 2^64 - 1 messages per channel are never reached.
    if (nonce_ == std::numeric_limits<std::uint64_t>::max()) throw std::runtime_error("channel nonces exhausted");
    Bytes out(plaintext.size() + noise_tag);
    unsigned long long len = 0;
    const auto nonce = nonce_bytes(nonce_++);
    crypto_aead_chacha20poly1305_ietf_encrypt(out.data(), &len, plaintext.data(), plaintext.size(), ad.data(),
                                              ad.size(), nullptr, nonce.data(), key_.bytes().data());
    out.resize(static_cast<std::size_t>(len));
    return out;
}

std::expected<Bytes, std::string> CipherState::decrypt(std::span<const std::uint8_t> ad,
                                                       std::span<const std::uint8_t> ciphertext) {
    if (!has_key_) return Bytes(ciphertext.begin(), ciphertext.end());
    if (ciphertext.size() < noise_tag) return std::unexpected(std::string("message too short"));
    if (nonce_ == std::numeric_limits<std::uint64_t>::max()) return std::unexpected(std::string("nonces exhausted"));
    Bytes out(ciphertext.size() - noise_tag);
    unsigned long long len = 0;
    const auto nonce = nonce_bytes(nonce_);
    if (crypto_aead_chacha20poly1305_ietf_decrypt(out.data(), &len, nullptr, ciphertext.data(), ciphertext.size(),
                                                  ad.data(), ad.size(), nonce.data(), key_.bytes().data()) != 0) {
        return std::unexpected(std::string("message authentication failed"));
    }
    ++nonce_;  // only after a successful decryption
    out.resize(static_cast<std::size_t>(len));
    return out;
}

// -- Handshake ----------------------------------------------------------------------

Handshake::Handshake(Role role, KeyPair static_key, std::span<const std::uint8_t> prologue,
                     std::optional<KeyPair> ephemeral)
    : role_(role), s_(std::move(static_key)), test_ephemeral_(std::move(ephemeral)) {
    ensure_sodium();
    // InitializeSymmetric: the protocol name fits the hash length exactly.
    static_assert(noise_protocol.size() == 32);
    std::copy(noise_protocol.begin(), noise_protocol.end(), h_.begin());
    std::copy(h_.begin(), h_.end(), ck_.bytes().begin());
    mix_hash(prologue);
}

bool Handshake::my_turn() const {
    return !finished() && ((step_ % 2 == 0) == (role_ == Role::initiator));
}

void Handshake::mix_hash(std::span<const std::uint8_t> data) {
    crypto_hash_sha256_state st;
    crypto_hash_sha256_init(&st);
    crypto_hash_sha256_update(&st, h_.data(), h_.size());
    crypto_hash_sha256_update(&st, data.data(), data.size());
    crypto_hash_sha256_final(&st, h_.data());
}

void Handshake::mix_key(std::span<const std::uint8_t> input) {
    Secret32 ck;
    Secret32 temp_k;
    hkdf2(ck_.bytes(), input, ck.bytes(), temp_k.bytes());
    ck_ = std::move(ck);
    cipher_.initialize(temp_k.bytes());
}

Bytes Handshake::encrypt_and_hash(std::span<const std::uint8_t> plaintext) {
    Bytes c = cipher_.encrypt(h_, plaintext);
    mix_hash(c);
    return c;
}

std::expected<Bytes, std::string> Handshake::decrypt_and_hash(std::span<const std::uint8_t> ciphertext) {
    auto p = cipher_.decrypt(h_, ciphertext);
    if (!p) return p;
    mix_hash(ciphertext);
    return p;
}

std::expected<void, std::string> Handshake::dh_mix(const Secret32& secret, const Key32& remote) {
    Secret32 shared;
    // libsodium refuses low-order points (an all-zero result).
    if (crypto_scalarmult(shared.bytes().data(), secret.bytes().data(), remote.data()) != 0) {
        return std::unexpected(std::string("invalid public key"));
    }
    mix_key(shared.bytes());
    return {};
}

std::expected<Bytes, std::string> Handshake::write_message(std::span<const std::uint8_t> payload) {
    if (failed_) return std::unexpected(std::string("handshake failed"));
    if (!my_turn()) return std::unexpected(std::string("not our turn in the handshake"));
    auto fail = [this](std::string why) -> std::expected<Bytes, std::string> {
        failed_ = true;
        return std::unexpected(std::move(why));
    };
    Bytes out;
    auto append = [&out](std::span<const std::uint8_t> b) { out.insert(out.end(), b.begin(), b.end()); };
    auto write_e = [&] {
        e_ = test_ephemeral_ ? std::move(*test_ephemeral_) : KeyPair::generate();
        test_ephemeral_.reset();
        append(e_->public_key);
        mix_hash(e_->public_key);
    };
    switch (step_) {
        case 0:  // -> e
            write_e();
            break;
        case 1:  // <- e, ee, s, es
            write_e();
            if (auto ok = dh_mix(e_->secret, *re_); !ok) return fail(ok.error());
            append(encrypt_and_hash(s_.public_key));
            if (auto ok = dh_mix(s_.secret, *re_); !ok) return fail(ok.error());
            break;
        case 2:  // -> s, se
            append(encrypt_and_hash(s_.public_key));
            if (auto ok = dh_mix(s_.secret, *re_); !ok) return fail(ok.error());
            break;
        default: return fail("handshake finished");
    }
    append(encrypt_and_hash(payload));
    if (out.size() > noise_max_message) return fail("handshake message too large");
    ++step_;
    return out;
}

std::expected<Bytes, std::string> Handshake::read_message(std::span<const std::uint8_t> message) {
    if (failed_) return std::unexpected(std::string("handshake failed"));
    if (finished() || my_turn()) return std::unexpected(std::string("not the peer's turn in the handshake"));
    auto fail = [this](std::string why) -> std::expected<Bytes, std::string> {
        failed_ = true;
        return std::unexpected(std::move(why));
    };
    if (message.size() > noise_max_message) return fail("handshake message too large");
    std::size_t at = 0;
    auto take = [&](std::size_t n) -> std::optional<std::span<const std::uint8_t>> {
        if (message.size() - at < n) return std::nullopt;
        auto part = message.subspan(at, n);
        at += n;
        return part;
    };
    auto read_e = [&]() -> bool {
        auto e = take(32);
        if (!e) return false;
        Key32 re{};
        std::copy(e->begin(), e->end(), re.begin());
        re_ = re;
        mix_hash(re);
        return true;
    };
    auto read_s = [&]() -> std::expected<void, std::string> {
        auto s = take(32 + (cipher_.has_key() ? noise_tag : 0));
        if (!s) return std::unexpected(std::string("handshake message too short"));
        auto plain = decrypt_and_hash(*s);
        if (!plain) return std::unexpected(plain.error());
        Key32 rs{};
        std::copy(plain->begin(), plain->end(), rs.begin());
        rs_ = rs;
        return {};
    };
    switch (step_) {
        case 0:  // -> e (we are the responder)
            if (!read_e()) return fail("handshake message too short");
            break;
        case 1:  // <- e, ee, s, es (we are the initiator)
            if (!read_e()) return fail("handshake message too short");
            if (auto ok = dh_mix(e_->secret, *re_); !ok) return fail(ok.error());
            if (auto ok = read_s(); !ok) return fail(ok.error());
            if (auto ok = dh_mix(e_->secret, *rs_); !ok) return fail(ok.error());
            break;
        case 2:  // -> s, se (we are the responder)
            if (auto ok = read_s(); !ok) return fail(ok.error());
            if (auto ok = dh_mix(e_->secret, *rs_); !ok) return fail(ok.error());
            break;
        default: return fail("handshake finished");
    }
    auto payload = decrypt_and_hash(message.subspan(at));
    if (!payload) return fail(payload.error());
    ++step_;
    return payload;
}

std::expected<Handshake::Split, std::string> Handshake::split() {
    if (!finished() || failed_) return std::unexpected(std::string("the handshake is not finished"));
    Secret32 k1;
    Secret32 k2;
    hkdf2(ck_.bytes(), {}, k1.bytes(), k2.bytes());
    Split out;
    if (role_ == Role::initiator) {
        out.send.initialize(k1.bytes());
        out.receive.initialize(k2.bytes());
    } else {
        out.send.initialize(k2.bytes());
        out.receive.initialize(k1.bytes());
    }
    // The handshake cannot be used any more.
    failed_ = true;
    return out;
}

}  // namespace paglets::net
