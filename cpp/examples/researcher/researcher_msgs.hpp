// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Web Researcher demo (planning/cpp-demo-paglets.md, demo 21).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace researcher_msgs {

// `start`: search the web for `question` on a host offering `web.search`,
// read the first `pages` results there, summarize them on a host offering
// `ai.summarize`, and come home with the report.
struct Question {
    std::string question;
    std::int64_t pages = 3;
    std::int64_t max_words = 60;
    std::string web = "offer:web.search";
    std::string ai = "offer:ai.summarize";
};

struct Source {
    std::string title;
    std::string url;
    std::string summary;
};

struct Report {
    std::string state;  // idle, searching, summarizing, returning, done, failed
    std::string question;
    std::vector<Source> sources;
    std::vector<std::string> hosts;  // where it went
    std::string error;
};

}  // namespace researcher_msgs
