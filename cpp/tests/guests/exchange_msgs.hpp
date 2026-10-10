// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the content exchange test paglet
// (planning/cpp-content-exchange.md).

#pragma once

#include <cstdint>
#include <string>

namespace exchange_msgs {

// `share` (root, path), `save` (root, path), `put` (text), `read` (up to
// `length` bytes from `offset`), `go` (host): each answers at once; the
// outcome of the last one is in `outcome`.
struct Step {
    std::string root;
    std::string path;
    std::string text;
    std::string host;
    std::uint64_t offset = 0;
    std::uint64_t length = 0;
};

struct Outcome {
    std::string step;  // the last step
    bool done = false;
    std::int32_t status = 0;  // 0 or an abi::Error
    std::string hash;
    std::uint64_t size = 0;
    std::string data;  // read: the bytes
};

}  // namespace exchange_msgs
