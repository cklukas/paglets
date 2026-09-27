// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `artifacts` system paglet (planning/cpp-system-paglets.md):
// a content-addressed blob store. `put` stores data and replies with an
// `artifact` capability (rights `read`) as the first capability of the
// reply; `get` and `stat` take a lent artifact capability. Artifacts are
// identified by the SHA-256 of their content (hex), so capabilities to them
// stay meaningful wherever they are passed.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::artifacts {

// At most 4 MB per request (larger artifacts arrive with WP11 streaming).
struct PutRequest {
    std::vector<std::uint8_t> data;
    std::string media_type;  // informational, for example "text/csv"
};

struct Info {
    std::string hash;  // hex SHA-256
    std::uint64_t size = 0;
    std::string media_type;
    std::int64_t created_ms = 0;
};

struct GetRequest {
    std::uint64_t offset = 0;
    std::optional<std::uint64_t> length;
};

struct GetReply {
    std::vector<std::uint8_t> data;
    std::uint64_t size = 0;
    bool eof = false;
};

struct StatRequest {};

struct Contract {
    static constexpr std::string_view service = "artifacts";
    Info put(const PutRequest&);
    GetReply get(const GetRequest&);
    Info stat(const StatRequest&);
};

}  // namespace paglets::services::artifacts
