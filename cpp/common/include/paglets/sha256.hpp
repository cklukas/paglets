// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// SHA-256 (FIPS 180-4) for module content addressing and image page hashes.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace paglets {

using Digest = std::array<std::uint8_t, 32>;

class Sha256 {
public:
    Sha256();
    void update(std::span<const std::uint8_t> data);
    Digest finish();

private:
    void block(const std::uint8_t* p);

    std::array<std::uint32_t, 8> h_{};
    std::array<std::uint8_t, 64> buf_{};
    std::size_t buf_len_ = 0;
    std::uint64_t total_ = 0;
};

Digest sha256(std::span<const std::uint8_t> data);
std::string to_hex(std::span<const std::uint8_t> bytes);

}  // namespace paglets
