// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Log Explainer demo (planning/cpp-demo-paglets.md, demo 19).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace explainer_msgs {

// `start`: let a Log Scout (a child from `scout_module`, hex hash) search
// the logs, group the lines it found, explain each group on a host that
// offers `ai`, and come home with the explanations.
struct Explain {
    std::vector<std::string> hosts;  // where to search; empty: every host
    std::string root;
    std::string files = "**/*.log";
    std::vector<std::string> patterns = {"error", "fail"};
    std::int64_t window_ms = 3'600'000;
    std::string scout_module;
    std::string via = "offer:ai.generate";
    std::vector<std::string> labels = {"disk", "network", "permission", "configuration", "memory", "other"};
    std::int64_t max_groups = 10;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// A group of similar lines.
struct Group {
    std::string shape;  // the line with numbers, IDs and quoted text replaced by #
    std::string example;
    std::int64_t count = 0;
    std::vector<std::string> hosts;
    std::string label;  // ai.classify, one of the labels
    std::string explanation;  // ai.generate
};

struct Status {
    std::string state;  // idle, scouting, explaining, returning, explained, failed
    std::int64_t lines = 0;
    std::vector<Group> groups;  // the largest first
    std::vector<std::string> hosts;  // names of the hosts it was on, in order
    std::string ai_host;
    std::string model;
    std::string error;
};

// The Log Scout's messages (log_scout_msgs, the fields used here; the
// encodings are maps by field name, so the others keep their defaults).
struct ScoutStart {
    std::vector<std::string> hosts;
    std::string root;
    std::string files;
    std::vector<std::string> patterns;
    std::int64_t window_ms = 0;
    std::int64_t max_excerpts = 0;
};

struct ScoutStarted {
    bool accepted = false;
    std::string reason;
};

struct ScoutExcerpt {
    std::string host_name;
    std::string path;
    std::string line;
};

struct ScoutReport {
    std::string state;
    std::vector<ScoutExcerpt> excerpts;
    std::vector<std::string> errors;
};

}  // namespace explainer_msgs
