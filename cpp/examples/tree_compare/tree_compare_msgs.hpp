// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Tree Compare demo (planning/cpp-demo-paglets.md, demo 6).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace tree_compare_msgs {

// `start`: compare the files matching `pattern` below `root`/`path` on
// `hosts` (names or key IDs; empty: every host mesh-info knows).
// `hashes`: compare content by SHA-256 too, not only sizes.
struct Compare {
    std::vector<std::string> hosts;
    std::string root;
    std::string path;
    std::string pattern = "**";
    bool hashes = false;
    std::int64_t max_file_bytes = 64 * 1024 * 1024;  // larger files are not hashed
    std::int64_t timeout_ms = 20'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

// A clone's report: the tree on its host.
struct TreeEntry {
    std::string path;
    std::int64_t size = 0;
    std::int64_t modified_ms = 0;
    std::string sha256;  // with `hashes`
};

struct HostTree {
    std::vector<TreeEntry> entries;
    bool truncated = false;
};

// One host's copy of a file that differs.
struct Copy {
    std::string host_name;
    std::int64_t size = 0;
    std::int64_t modified_ms = 0;
    std::string sha256;
};

struct Difference {
    std::string path;
    std::string kind;                     // missing, size, content
    std::vector<std::string> missing_on;  // missing: the hosts without it
    std::vector<Copy> copies;             // the hosts that have it
};

// `report`: the comparison, once every host answered (or timed out).
struct Report {
    std::string state;               // idle, scanning, done
    std::vector<std::string> hosts;  // compared, in the order asked
    std::int64_t files = 0;          // distinct paths
    std::int64_t same = 0;           // paths equal on every host
    std::vector<Difference> differences;
    std::vector<std::string> errors;  // hosts that could not be scanned
    bool truncated = false;
};

}  // namespace tree_compare_msgs
