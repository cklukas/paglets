// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Duplicate Finder demo (planning/cpp-demo-paglets.md, demo 4).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dupes_msgs {

// `start`: files of at least `min_size` bytes below the root `root` on every
// host (or `hosts`) that have the same content somewhere in the mesh.
struct Search {
    std::string root;
    std::string pattern = "**";
    std::vector<std::string> hosts;
    std::int64_t min_size = 1;
    std::int64_t timeout_ms = 30'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// Phase 1, what a host has (path and size), and phase 2, the hashes of the
// files whose size occurs more than once in the mesh.
struct Phase {
    std::int64_t phase = 1;
    Search search;
    std::vector<std::int64_t> sizes;  // phase 2: the sizes to hash
};

struct File {
    std::string host_name;
    std::string path;
    std::int64_t size = 0;
    std::string sha256;  // phase 2
};

struct HostFiles {
    std::vector<File> files;
};

struct Group {
    std::string sha256;
    std::int64_t size = 0;
    std::vector<File> copies;
};

struct Result {
    std::string state;  // idle, sizes, hashing, done
    std::int64_t files = 0;
    std::int64_t candidates = 0;
    std::vector<Group> groups;  // the largest waste first
    std::int64_t wasted = 0;    // bytes in copies beyond the first
    std::vector<std::string> errors;
};

}  // namespace dupes_msgs
