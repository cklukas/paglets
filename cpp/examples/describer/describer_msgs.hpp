// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Image Describer demo (planning/cpp-demo-paglets.md, demo 18).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace describer_msgs {

// Where to look for images: a host (name or key ID), a named root on it and
// a file pattern; files ending in .png, .jpg or .jpeg count.
struct Source {
    std::string host;
    std::string root;
    std::string pattern = "**";
};

// Where the catalogue goes: a JSON file below a named root of a host.
struct Output {
    std::string host;
    std::string root;
    std::string path = "images.json";
};

// `start`: collect the images, describe them on a host whose `ai` offer can
// see (vision=yes, describe_image), write the catalogue.
struct Start {
    std::vector<Source> sources;
    Output output;
    std::string model;  // empty: the AI host's default
    std::int64_t max_images = 50;
    std::int64_t max_image_bytes = 8 * 1024 * 1024;
    std::int64_t max_total_bytes = 64 * 1024 * 1024;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

struct Image {
    std::string host;
    std::string path;  // root/path on that host
    std::int64_t size = 0;
    std::string caption;
    std::vector<std::string> tags;
};

struct Status {
    std::string state;  // idle, collecting, describing, writing, written, refused, failed
    std::vector<Image> images;
    std::vector<std::string> skipped;  // too large, unreadable
    std::vector<std::string> hosts;    // names of the hosts it was on, in order
    std::string ai_host;               // the host that described the images
    std::string model;
    std::string error;
};

}  // namespace describer_msgs
