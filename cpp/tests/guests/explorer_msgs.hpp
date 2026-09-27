// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the explorer test guest (tests/test_services.cpp), which uses
// every standard system paglet through the generated service clients.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace explorer {

// Finds files below a lent directory, reads the first match, and reports
// the host's summary, load, volumes and processes (the WP10 exit).
struct Explore {
    std::int32_t dir = 0;
    std::string pattern;
};

struct Report {
    std::string os;
    std::string host_name;
    std::string architecture;
    std::uint32_t cpu_count = 0;
    std::uint64_t memory_total = 0;
    std::vector<std::string> found;
    std::string first_content;
    double cpu_percent = -1;
    std::uint32_t cores = 0;
    std::uint64_t memory_available = 0;
    std::uint32_t volumes = 0;
    std::uint64_t largest_volume = 0;
    std::uint64_t largest_volume_free = 0;
    std::uint32_t processes = 0;
    std::uint32_t processes_listed = 0;
    std::string error;  // "<step>:<status>" on failure
};

// Writes, lists, moves and deletes below a lent directory.
struct FileOps {
    std::int32_t dir = 0;
};

struct Store {
    std::string key;
    std::vector<std::uint8_t> value;
};

struct Name {
    std::string name;
    bool is_public = false;
};

struct Text {
    std::string text;
};

// Asks the grants service for access.
struct Access {
    std::string service;
    std::vector<std::string> ops;
    std::string root;
    std::string path;
    std::int64_t duration_ms = 3'600'000;
};

}  // namespace explorer
