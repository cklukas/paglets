// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Mesh File Finder and Storage Analyzer demos
// (planning/cpp-demo-paglets.md, demos 2 and 3).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace finder_msgs {

// `start`: look below the named root `root` on `hosts` (empty: every host
// mesh-info knows) for files matching `pattern`.
struct Search {
    std::string root;
    std::string pattern = "**";
    std::vector<std::string> hosts;
    std::int64_t min_size = 0;
    std::int64_t max_results = 100;  // files listed per host
    std::int64_t timeout_ms = 20'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

struct Found {
    std::string path;
    std::int64_t size = 0;
    std::int64_t modified_ms = 0;
};

struct ExtensionTotal {
    std::string extension;  // "" for files without one
    std::int64_t files = 0;
    std::int64_t bytes = 0;
};

// What one host has (the storage analysis: counts, sizes, the largest
// files, totals by extension).
struct HostFindings {
    std::string host;
    std::string host_name;
    std::string error;
    std::int64_t files = 0;
    std::int64_t bytes = 0;
    bool truncated = false;
    std::vector<Found> found;    // the first max_results, by path
    std::vector<Found> largest;  // the five largest
    std::vector<ExtensionTotal> extensions;
};

struct Findings {
    bool finished = false;
    std::vector<HostFindings> hosts;
    std::int64_t files = 0;
    std::int64_t bytes = 0;
};

}  // namespace finder_msgs
