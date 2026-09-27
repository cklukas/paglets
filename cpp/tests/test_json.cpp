// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <paglets/wire/json.hpp>

using paglets::wire::Json;

PAGLETS_TEST("json parse and dump round trip") {
    const std::string text = R"({"a":1,"b":[true,false,null],"c":"x\"y\\z\n","d":-12.5e3,"e":{}})";
    auto j = Json::parse(text);
    REQUIRE(j.has_value());
    CHECK_EQ(j->dump(), text);
    const Json* a = j->find("a");
    REQUIRE(a != nullptr);
    std::int64_t v = 0;
    CHECK(a->number_as(v));
    CHECK_EQ(v, 1);
}

PAGLETS_TEST("json keeps 64-bit integers exact") {
    auto j = Json::parse("18446744073709551615");
    REQUIRE(j.has_value());
    std::uint64_t u = 0;
    CHECK(j->number_as(u));
    CHECK_EQ(u, 18446744073709551615ull);
}

PAGLETS_TEST("json unicode escapes") {
    auto j = Json::parse(R"("ä😀")");
    REQUIRE(j.has_value());
    CHECK_EQ(j->as_string(), std::string("\xc3\xa4\xf0\x9f\x98\x80"));
}

PAGLETS_TEST("json rejects malformed input") {
    CHECK(!Json::parse("{").has_value());
    CHECK(!Json::parse("[1,]").has_value());
    CHECK(!Json::parse("\"abc").has_value());
    CHECK(!Json::parse("tru").has_value());
    CHECK(!Json::parse("1 2").has_value());
    CHECK(!Json::parse("-").has_value());
}
