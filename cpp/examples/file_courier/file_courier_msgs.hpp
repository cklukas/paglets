// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the File Courier demo (planning/cpp-demo-paglets.md, demo 5).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace file_courier_msgs {

// `start`: carry the files matching `pattern` below the root `from_root` of
// `from_host` to the directory `to_path` below the root `to_root` of
// `to_host` (hosts by name or key ID), then come home with a receipt.
struct Start {
    std::string from_host;
    std::string from_root;
    std::string pattern = "**";
    std::string to_host;
    std::string to_root;
    std::string to_path;  // "" : the root itself
    std::int64_t max_files = 100;
    std::int64_t max_bytes = 16 * 1024 * 1024;  // all files together
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// One file of the receipt.
struct Item {
    std::string path;  // below the source root; the same below `to_path`
    std::int64_t size = 0;
    std::string sha256;     // hex, of the content read on the source host
    bool verified = false;  // read back on the destination host with the same hash
};

// `status`: the errand and, once delivered, the receipt.
struct Status {
    std::string state;  // idle, picking-up, delivering, returning, delivered, refused, failed
    std::vector<Item> files;
    std::int64_t bytes = 0;
    std::vector<std::string> hosts;  // names of the hosts it was on, in order
    std::string error;
};

}  // namespace file_courier_msgs
