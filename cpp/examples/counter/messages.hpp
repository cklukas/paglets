// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Message types of the counter example paglet. Plain structs: the host encodes
// them through C++26 reflection, the guest through code generated from this
// header by schema_gen.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace counter_msgs {

enum class Mode { add, multiply };

struct Increment {
    std::int64_t by = 1;
    std::string note;
    Mode mode = Mode::add;
};

struct Status {
    std::int64_t value = 0;
    std::uint32_t history_size = 0;
    std::vector<std::string> recent_notes;
    std::optional<double> average_step;
    std::uint32_t memory_pages = 0;
};

// Exercises every supported field type; the guest echoes it unchanged.
struct Echo {
    bool flag = false;
    std::int8_t tiny = 0;
    std::int32_t medium = 0;
    std::uint64_t big = 0;
    float ratio = 0.0f;
    double precise = 0.0;
    std::string text;
    std::vector<std::uint8_t> blob;
    std::vector<Increment> items;
    std::optional<std::string> label;
    std::optional<Status> nested;
    Mode mode = Mode::add;
};

// Allocates and keeps the given amount of memory (image size experiments).
struct Bloat {
    std::uint32_t kilobytes = 0;
    std::uint8_t fill = 0;
};

}  // namespace counter_msgs
