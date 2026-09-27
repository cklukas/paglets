// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `storage` system paglet (planning/cpp-system-paglets.md):
// persistent key/value storage of the calling paglet, within a quota. Every
// paglet has its own space (the host identifies the caller by its sender
// record), which survives restarts of the host and ends with the paglet.
// Keys are 1 to 128 characters from [A-Za-z0-9._-]; keys are
// case-sensitive on every platform.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::storage {

struct GetRequest {
    std::string key;
};

struct GetReply {
    bool found = false;
    std::vector<std::uint8_t> value;
};

struct PutRequest {
    std::string key;
    std::vector<std::uint8_t> value;
};

struct Usage {
    std::uint64_t used = 0;   // bytes of all values
    std::uint64_t quota = 0;  // bytes
    std::uint32_t keys = 0;
};

struct DeleteRequest {
    std::string key;
};

struct DeleteReply {
    bool existed = false;
};

struct ListRequest {
    std::string prefix;
};

struct ListReply {
    std::vector<std::string> keys;  // sorted
    Usage usage;
};

struct Contract {
    static constexpr std::string_view service = "storage";
    GetReply get(const GetRequest&);
    Usage put(const PutRequest&);  // quota when the value does not fit
    DeleteReply remove(const DeleteRequest&);
    ListReply list(const ListRequest&);
};

}  // namespace paglets::services::storage
