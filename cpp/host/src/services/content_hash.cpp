// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "content_hash.hpp"

#include <openssl/evp.h>

#include <stdexcept>
#include <utility>

namespace paglets::services {

ContentHash::ContentHash() : ctx_(EVP_MD_CTX_new()) {
    if (ctx_ == nullptr || EVP_DigestInit_ex(static_cast<EVP_MD_CTX*>(ctx_), EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("SHA-256 unavailable");
    }
}

ContentHash::~ContentHash() {
    EVP_MD_CTX_free(static_cast<EVP_MD_CTX*>(ctx_));
}

ContentHash::ContentHash(ContentHash&& other) noexcept : ctx_(std::exchange(other.ctx_, nullptr)) {}

ContentHash& ContentHash::operator=(ContentHash&& other) noexcept {
    std::swap(ctx_, other.ctx_);
    return *this;
}

void ContentHash::update(std::span<const std::uint8_t> data) {
    if (!data.empty()) EVP_DigestUpdate(static_cast<EVP_MD_CTX*>(ctx_), data.data(), data.size());
}

Digest ContentHash::finish() {
    Digest d{};
    unsigned int n = 0;
    EVP_DigestFinal_ex(static_cast<EVP_MD_CTX*>(ctx_), d.data(), &n);
    return d;
}

}  // namespace paglets::services
