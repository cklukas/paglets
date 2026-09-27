// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <paglets/msgpack.hpp>

#include <cstdint>
#include <limits>

using paglets::msgpack::Reader;
using paglets::msgpack::Writer;

PAGLETS_TEST("msgpack integer boundaries round trip") {
    const std::int64_t values[] = {0,
                                   1,
                                   127,
                                   128,
                                   255,
                                   256,
                                   65535,
                                   65536,
                                   -1,
                                   -32,
                                   -33,
                                   -128,
                                   -129,
                                   -32768,
                                   -32769,
                                   std::numeric_limits<std::int32_t>::min(),
                                   std::numeric_limits<std::int64_t>::min(),
                                   std::numeric_limits<std::int64_t>::max()};
    for (const auto v : values) {
        Writer w;
        w.write_int(v);
        Reader r(w.bytes());
        std::int64_t out = 0;
        CHECK(r.read_i64(out));
        CHECK_EQ(out, v);
        CHECK(r.at_end());
    }
    Writer w;
    w.write_uint(std::numeric_limits<std::uint64_t>::max());
    Reader r(w.bytes());
    std::int64_t as_signed = 0;
    CHECK(!r.read_i64(as_signed));  // does not fit
    Reader r2(w.bytes());
    std::uint64_t u = 0;
    CHECK(r2.read_u64(u));
    CHECK_EQ(u, std::numeric_limits<std::uint64_t>::max());
}

PAGLETS_TEST("msgpack encodes compactly per the specification") {
    Writer w;
    w.write_int(-1);
    w.write_uint(200);
    w.write_str("hi");
    w.write_map_header(1);
    const std::vector<std::uint8_t> expected = {0xff, 0xcc, 0xc8, 0xa2, 'h', 'i', 0x81};
    CHECK(w.bytes() == expected);
}

PAGLETS_TEST("msgpack skip and truncation") {
    Writer w;
    w.write_map_header(2);
    w.write_str("a");
    w.write_array_header(3);
    w.write_f64(1.5);
    w.write_bin(std::vector<std::uint8_t>{1, 2, 3});
    w.write_nil();
    w.write_str("b");
    w.write_bool(true);
    w.write_int(42);

    Reader r(w.bytes());
    CHECK(r.skip());
    std::int64_t v = 0;
    CHECK(r.read_i64(v));
    CHECK_EQ(v, 42);

    auto truncated = w.bytes();
    truncated.resize(truncated.size() - 3);
    Reader t(truncated);
    CHECK(!t.skip());
    CHECK(!t.ok());
}
