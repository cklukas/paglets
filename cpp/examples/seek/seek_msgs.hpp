// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Hide and Seek demo (planning/cpp-demo-paglets.md, demo 15).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace seek_msgs {

// `start`: the seeker creates a hider that keeps moving to other hosts
// every `hop_ms`; then, `rounds` times, it finds the hider, pins it,
// checks it stays, and lets it go.
struct Game {
    std::int64_t rounds = 5;
    std::int64_t hop_ms = 30;
    std::int64_t pause_ms = 300;  // between rounds: the hider moves on
};

struct Catch {
    std::string host_name;   // where the locator found it
    std::int64_t moves = 0;  // how often it had moved by then
    std::string error;
};

struct Score {
    std::string state;  // idle, seeking, done
    std::vector<Catch> catches;
    std::int64_t hider_moves = 0;  // as the hider counted, at the end
};

}  // namespace seek_msgs
