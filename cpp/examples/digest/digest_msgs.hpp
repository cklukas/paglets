// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the AI Document Digest demo (planning/cpp-web-ai.md, section 5).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace digest_msgs {

// Where to look for documents: a host (name or key ID), a named root on it
// and a file pattern.
struct Source {
    std::string host;
    std::string root;
    std::string pattern = "**/*.txt";
};

// Where the digest goes: a file below a named root of a host.
struct Output {
    std::string host;
    std::string root;
    std::string path = "digest.md";
};

// `start`: collect the documents, summarize them where `ai` is offered,
// write the digest.
struct Start {
    std::vector<Source> sources;
    Output output;
    std::string ai = "offer:ai.summarize";  // the transfer ticket to the AI host
    std::int64_t max_files = 20;
    std::int64_t max_words = 60;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

struct Document {
    std::string host;
    std::string path;
    std::int64_t size = 0;
    std::string summary;
};

struct Status {
    std::string state;  // idle, collecting, summarizing, writing, written, refused, failed
    std::vector<Document> documents;
    std::vector<std::string> hosts;  // names of the hosts it was on, in order
    std::string model;
    std::string error;
};

}  // namespace digest_msgs
