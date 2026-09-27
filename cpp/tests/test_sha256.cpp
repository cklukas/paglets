// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <paglets/sha256.hpp>

#include <string_view>
#include <vector>

namespace {

std::string hex_of(std::string_view s) {
    return paglets::to_hex(paglets::sha256({reinterpret_cast<const std::uint8_t*>(s.data()), s.size()}));
}

}  // namespace

PAGLETS_TEST("sha256 known vectors") {
    CHECK_EQ(hex_of(""), std::string("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
    CHECK_EQ(hex_of("abc"), std::string("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
    CHECK_EQ(hex_of("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
             std::string("248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"));
}

PAGLETS_TEST("sha256 incremental equals one-shot") {
    std::vector<std::uint8_t> data(200'003);
    for (std::size_t i = 0; i < data.size(); ++i) data[i] = static_cast<std::uint8_t>(i * 31 + 7);
    paglets::Sha256 s;
    for (std::size_t off = 0; off < data.size(); off += 1000) {
        const std::size_t n = std::min<std::size_t>(1000 - (off % 7), data.size() - off);
        s.update({data.data() + off, n});
        off -= 1000 - n;
    }
    CHECK(s.finish() == paglets::sha256(data));
}
