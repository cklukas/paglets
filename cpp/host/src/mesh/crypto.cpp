// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/mesh/crypto.hpp>

#include <paglets/msgpack.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wasm/engine.hpp>

#include <sodium.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <utility>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace paglets::mesh {

namespace {

constexpr std::string_view key_format = "paglets-key-1";
constexpr std::string_view protection_none = "none";
constexpr std::string_view protection_argon2 = "argon2id-xchacha20poly1305";

// The associated data binds an encrypted seed to its public key and role.
Bytes associated_data(const PublicKey& key, KeyRole role) {
    Bytes ad(key.begin(), key.end());
    const auto r = to_string(role);
    ad.insert(ad.end(), r.begin(), r.end());
    return ad;
}

struct KeyFileContent {
    std::string format;
    KeyRole role = KeyRole::owner;
    std::string name;
    PublicKey public_key{};
    std::string protection;
    Bytes salt;
    std::uint64_t opslimit = 0;
    std::uint64_t memlimit = 0;
    Bytes nonce;
    Bytes secret;  // the 32-byte seed, encrypted or plain
};

std::expected<KeyFileContent, std::string> parse_key_file(const std::filesystem::path& path) {
    auto bytes = wasm::read_file(path.string());
    if (!bytes) return std::unexpected(bytes.error());
    msgpack::Reader r(*bytes);
    KeyFileContent k;
    std::uint32_t n = 0;
    if (!r.read_map_header(n)) return std::unexpected("not a key file: " + path.string());
    std::string role;
    Bytes public_key;
    for (std::uint32_t i = 0; i < n; ++i) {
        std::string_view key;
        if (!r.read_str(key)) return std::unexpected("damaged key file: " + path.string());
        bool ok = true;
        if (key == "format") {
            ok = msgpack::read_value(r, k.format);
        } else if (key == "role") {
            ok = msgpack::read_value(r, role);
        } else if (key == "name") {
            ok = msgpack::read_value(r, k.name);
        } else if (key == "public") {
            ok = msgpack::read_value(r, public_key);
        } else if (key == "protection") {
            ok = msgpack::read_value(r, k.protection);
        } else if (key == "salt") {
            ok = msgpack::read_value(r, k.salt);
        } else if (key == "opslimit") {
            ok = msgpack::read_value(r, k.opslimit);
        } else if (key == "memlimit") {
            ok = msgpack::read_value(r, k.memlimit);
        } else if (key == "nonce") {
            ok = msgpack::read_value(r, k.nonce);
        } else if (key == "secret") {
            ok = msgpack::read_value(r, k.secret);
        } else {
            ok = r.skip();
        }
        if (!ok) return std::unexpected("damaged key file: " + path.string());
    }
    auto parsed_role = parse_key_role(role);
    if (k.format != key_format || !parsed_role || public_key.size() != k.public_key.size()) {
        return std::unexpected("unsupported key file: " + path.string());
    }
    k.role = *parsed_role;
    std::copy(public_key.begin(), public_key.end(), k.public_key.begin());
    return k;
}

std::expected<void, std::string> write_private_file(const std::filesystem::path& path,
                                                    std::span<const std::uint8_t> data) {
    std::error_code ec;
    if (std::filesystem::exists(path, ec)) return std::unexpected("refusing to overwrite " + path.string());
    if (path.has_parent_path()) std::filesystem::create_directories(path.parent_path(), ec);
#ifdef _WIN32
    // The user's profile directories are private to the user by default.
    return wasm::write_file(path.string(), data);
#else
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) return std::unexpected("cannot create " + path.string() + ": " + std::strerror(errno));
    std::size_t done = 0;
    while (done < data.size()) {
        const ssize_t w = ::write(fd, data.data() + done, data.size() - done);
        if (w < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            return std::unexpected("cannot write " + path.string() + ": " + std::strerror(errno));
        }
        done += static_cast<std::size_t>(w);
    }
    if (::fsync(fd) != 0 || ::close(fd) != 0) return std::unexpected("cannot write " + path.string());
    return {};
#endif
}

}  // namespace

void ensure_crypto() {
    static std::once_flag once;
    std::call_once(once, [] {
        if (sodium_init() < 0) {
            std::cerr << "paglets: libsodium initialization failed\n";
            std::abort();
        }
    });
}

void random_bytes(std::span<std::uint8_t> out) {
    ensure_crypto();
    randombytes_buf(out.data(), out.size());
}

std::string key_id(const PublicKey& key) {
    return to_hex(std::span<const std::uint8_t>(key));
}

std::optional<PublicKey> parse_key_id(std::string_view id) {
    if (id.size() != 64) return std::nullopt;
    PublicKey key{};
    for (std::size_t i = 0; i < key.size(); ++i) {
        unsigned v = 0;
        for (int j = 0; j < 2; ++j) {
            const char c = id[2 * i + static_cast<std::size_t>(j)];
            v <<= 4;
            if (c >= '0' && c <= '9') {
                v |= static_cast<unsigned>(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                v |= static_cast<unsigned>(c - 'a' + 10);
            } else {
                return std::nullopt;
            }
        }
        key[i] = static_cast<std::uint8_t>(v);
    }
    return key;
}

bool verify(const PublicKey& key, std::span<const std::uint8_t> message, const Signature& signature) {
    ensure_crypto();
    return crypto_sign_verify_detached(signature.data(), message.data(), message.size(), key.data()) == 0;
}

// ---------------------------------------------------------------------------

SigningKey::SigningKey() {
    ensure_crypto();
    secret_ = static_cast<std::uint8_t*>(sodium_malloc(crypto_sign_SECRETKEYBYTES));
    if (secret_ == nullptr) {
        std::cerr << "paglets: cannot allocate protected key memory\n";
        std::abort();
    }
}

SigningKey::SigningKey(SigningKey&& other) noexcept
    : secret_(std::exchange(other.secret_, nullptr)), public_(other.public_) {}

SigningKey& SigningKey::operator=(SigningKey&& other) noexcept {
    if (this != &other) {
        if (secret_ != nullptr) sodium_free(secret_);
        secret_ = std::exchange(other.secret_, nullptr);
        public_ = other.public_;
    }
    return *this;
}

SigningKey::~SigningKey() {
    if (secret_ != nullptr) sodium_free(secret_);  // wipes the memory
}

SigningKey SigningKey::generate() {
    SigningKey k;
    crypto_sign_keypair(k.public_.data(), k.secret_);
    return k;
}

std::expected<SigningKey, std::string> SigningKey::from_seed(std::span<const std::uint8_t> seed) {
    if (seed.size() != crypto_sign_SEEDBYTES) return std::unexpected(std::string("a key seed has 32 bytes"));
    SigningKey k;
    crypto_sign_seed_keypair(k.public_.data(), k.secret_, seed.data());
    return k;
}

Signature SigningKey::sign(std::span<const std::uint8_t> message) const {
    Signature s{};
    crypto_sign_detached(s.data(), nullptr, message.data(), message.size(), secret_);
    return s;
}

// ---------------------------------------------------------------------------

std::string_view to_string(KeyRole role) {
    switch (role) {
        case KeyRole::admin: return "admin";
        case KeyRole::owner: return "owner";
        case KeyRole::host: return "host";
        case KeyRole::signer: return "signer";
    }
    return "owner";
}

std::optional<KeyRole> parse_key_role(std::string_view text) {
    if (text == "admin") return KeyRole::admin;
    if (text == "owner") return KeyRole::owner;
    if (text == "host") return KeyRole::host;
    if (text == "signer") return KeyRole::signer;
    return std::nullopt;
}

namespace {

// Memory from sodium_malloc for the seed and the key-encryption key: locked,
// guarded and wiped on release.
class SecretBuffer {
public:
    explicit SecretBuffer(std::size_t size) : size_(size) {
        data_ = static_cast<std::uint8_t*>(sodium_malloc(size));
        if (data_ == nullptr) {
            std::cerr << "paglets: cannot allocate protected key memory\n";
            std::abort();
        }
    }
    SecretBuffer(const SecretBuffer&) = delete;
    SecretBuffer& operator=(const SecretBuffer&) = delete;
    ~SecretBuffer() { sodium_free(data_); }
    std::uint8_t* data() { return data_; }
    std::size_t size() const { return size_; }

private:
    std::uint8_t* data_ = nullptr;
    std::size_t size_ = 0;
};

// Upper bounds for key files read from disk, so a crafted file cannot make
// the key derivation take arbitrary time or memory.
constexpr std::uint64_t max_opslimit = crypto_pwhash_OPSLIMIT_SENSITIVE;
constexpr std::uint64_t max_memlimit = crypto_pwhash_MEMLIMIT_SENSITIVE;

std::expected<void, std::string> derive_key(SecretBuffer& out, std::string_view passphrase,
                                            std::span<const std::uint8_t> salt, std::uint64_t opslimit,
                                            std::uint64_t memlimit) {
    if (crypto_pwhash(out.data(), out.size(), passphrase.data(), passphrase.size(), salt.data(), opslimit,
                      static_cast<std::size_t>(memlimit), crypto_pwhash_ALG_ARGON2ID13) != 0) {
        return std::unexpected(std::string("key derivation ran out of memory"));
    }
    return {};
}

}  // namespace

std::expected<void, std::string> save_key(const std::filesystem::path& path, const SigningKey& key, KeyRole role,
                                          std::string_view name, std::optional<std::string_view> passphrase,
                                          bool fast_kdf) {
    ensure_crypto();
    const bool encrypt = passphrase && !passphrase->empty();
    if (role != KeyRole::host && !encrypt) {
        return std::unexpected(std::string("admin, owner and signer keys need a passphrase"));
    }
    msgpack::Writer w;
    w.write_map_header(encrypt ? 10 : 6);
    w.write_str("format");
    w.write_str(key_format);
    w.write_str("role");
    w.write_str(to_string(role));
    w.write_str("name");
    w.write_str(name);
    w.write_str("public");
    w.write_bin(key.public_);
    w.write_str("protection");
    w.write_str(encrypt ? protection_argon2 : protection_none);
    // libsodium keeps seed || public key as the secret key.
    const std::span<const std::uint8_t> seed(key.secret_, crypto_sign_SEEDBYTES);
    if (encrypt) {
        std::array<std::uint8_t, crypto_pwhash_SALTBYTES> salt{};
        std::array<std::uint8_t, crypto_aead_xchacha20poly1305_ietf_NPUBBYTES> nonce{};
        random_bytes(salt);
        random_bytes(nonce);
        const std::uint64_t opslimit = fast_kdf ? crypto_pwhash_OPSLIMIT_INTERACTIVE : crypto_pwhash_OPSLIMIT_MODERATE;
        const std::uint64_t memlimit = fast_kdf ? crypto_pwhash_MEMLIMIT_INTERACTIVE : crypto_pwhash_MEMLIMIT_MODERATE;
        SecretBuffer kek(crypto_aead_xchacha20poly1305_ietf_KEYBYTES);
        if (auto r = derive_key(kek, *passphrase, salt, opslimit, memlimit); !r) return r;
        const Bytes ad = associated_data(key.public_, role);
        Bytes sealed(seed.size() + crypto_aead_xchacha20poly1305_ietf_ABYTES);
        unsigned long long sealed_len = 0;
        crypto_aead_xchacha20poly1305_ietf_encrypt(sealed.data(), &sealed_len, seed.data(), seed.size(), ad.data(),
                                                   ad.size(), nullptr, nonce.data(), kek.data());
        sealed.resize(static_cast<std::size_t>(sealed_len));
        w.write_str("salt");
        w.write_bin(salt);
        w.write_str("opslimit");
        w.write_uint(opslimit);
        w.write_str("memlimit");
        w.write_uint(memlimit);
        w.write_str("nonce");
        w.write_bin(nonce);
        w.write_str("secret");
        w.write_bin(sealed);
    } else {
        w.write_str("secret");
        w.write_bin(seed);
    }
    Bytes file = w.take();
    auto written = write_private_file(path, file);
    sodium_memzero(file.data(), file.size());
    return written;
}

std::expected<SigningKey, std::string> load_key(const std::filesystem::path& path,
                                                std::optional<std::string_view> passphrase) {
    ensure_crypto();
    auto k = parse_key_file(path);
    if (!k) return std::unexpected(k.error());
    SecretBuffer seed(crypto_sign_SEEDBYTES);
    if (k->protection == protection_none) {
        if (k->role != KeyRole::host) {
            sodium_memzero(k->secret.data(), k->secret.size());
            return std::unexpected("refusing an unencrypted " + std::string(to_string(k->role)) +
                                   " key: " + path.string());
        }
        if (k->secret.size() != seed.size()) return std::unexpected("damaged key file: " + path.string());
        std::memcpy(seed.data(), k->secret.data(), seed.size());
        sodium_memzero(k->secret.data(), k->secret.size());
    } else if (k->protection == protection_argon2) {
        if (!passphrase || passphrase->empty()) {
            return std::unexpected("the key in " + path.string() + " needs its passphrase");
        }
        if (k->salt.size() != crypto_pwhash_SALTBYTES ||
            k->nonce.size() != crypto_aead_xchacha20poly1305_ietf_NPUBBYTES ||
            k->secret.size() != seed.size() + crypto_aead_xchacha20poly1305_ietf_ABYTES ||
            k->opslimit < crypto_pwhash_OPSLIMIT_MIN || k->opslimit > max_opslimit ||
            k->memlimit < crypto_pwhash_MEMLIMIT_MIN || k->memlimit > max_memlimit) {
            return std::unexpected("damaged key file: " + path.string());
        }
        SecretBuffer kek(crypto_aead_xchacha20poly1305_ietf_KEYBYTES);
        if (auto r = derive_key(kek, *passphrase, k->salt, k->opslimit, k->memlimit); !r) {
            return std::unexpected(r.error());
        }
        const Bytes ad = associated_data(k->public_key, k->role);
        unsigned long long seed_len = 0;
        if (crypto_aead_xchacha20poly1305_ietf_decrypt(seed.data(), &seed_len, nullptr, k->secret.data(),
                                                       k->secret.size(), ad.data(), ad.size(), k->nonce.data(),
                                                       kek.data()) != 0 ||
            seed_len != seed.size()) {
            return std::unexpected("wrong passphrase or damaged key file: " + path.string());
        }
    } else {
        return std::unexpected("unsupported key protection in " + path.string());
    }
    auto key = SigningKey::from_seed(std::span<const std::uint8_t>(seed.data(), seed.size()));
    if (!key) return key;
    if (key->public_key() != k->public_key) {
        return std::unexpected("the public key does not match the private key in " + path.string());
    }
    return key;
}

std::expected<KeyInfo, std::string> read_key_info(const std::filesystem::path& path) {
    auto k = parse_key_file(path);
    if (!k) return std::unexpected(k.error());
    sodium_memzero(k->secret.data(), k->secret.size());
    return KeyInfo{k->role, k->name, k->public_key, k->protection != protection_none};
}

}  // namespace paglets::mesh
