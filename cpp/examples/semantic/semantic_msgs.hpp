// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Semantic Mesh Search demo (planning/cpp-demo-paglets.md,
// demo 17).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace semantic_msgs {

struct Source {
    std::string host;
    std::string root;
    std::string pattern = "**/*.txt";
};

// `index`: read the documents of `sources`, move to a host that offers
// `ai.embed`, and keep their embeddings in memory there.
struct Index {
    std::vector<Source> sources;
    std::string ai = "offer:ai.embed";
    std::int64_t max_files = 200;
};

// `query`: the documents closest in meaning to `text`.
struct Query {
    std::string text;
    std::int64_t limit = 3;
};

struct Hit {
    std::string host_name;
    std::string path;
    double score = 0;  // cosine similarity
    std::string excerpt;
};

struct Hits {
    std::vector<Hit> hits;
    std::string error;
};

struct Status {
    std::string state;  // idle, collecting, embedding, ready, failed
    std::int64_t documents = 0;
    std::string host_name;  // where the index is
    std::string model;
    std::string error;
};

}  // namespace semantic_msgs
