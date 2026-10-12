// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// SHA-256 of large content in the host (files shared with `files.share` or
// hashed with `files.hash`, content arriving from other hosts): OpenSSL's
// implementation, which uses the CPU's SHA instructions where it has them.
// paglets::Sha256 stays the portable one that guests share.

#pragma once

#include <paglets/sha256.hpp>

#include <span>

namespace paglets::services {

class ContentHash {
public:
    ContentHash();
    ~ContentHash();
    ContentHash(ContentHash&& other) noexcept;
    ContentHash& operator=(ContentHash&& other) noexcept;
    ContentHash(const ContentHash&) = delete;
    ContentHash& operator=(const ContentHash&) = delete;

    void update(std::span<const std::uint8_t> data);
    Digest finish();

private:
    void* ctx_ = nullptr;  // EVP_MD_CTX
};

}  // namespace paglets::services
