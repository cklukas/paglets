// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Volume Guard demo (planning/cpp-demo-paglets.md, demo 9).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace guard_msgs {

// `start`: check the host's volumes every `interval_ms`, `checks` times
// (0: until stopped); alert when a volume has less than `min_free_percent`
// of its space available.
struct Watch {
    double min_free_percent = 10;
    std::int64_t interval_ms = 60'000;
    std::int64_t checks = 0;
};

struct Alert {
    std::string mount_point;
    double free_percent = 0;
    std::int64_t at_ms = 0;
};

struct Status {
    std::string state;  // idle, watching, done, failed
    std::int64_t checks = 0;
    std::int64_t sleeps = 0;  // times it went inactive until the next check
    std::vector<Alert> alerts;
    std::string error;
};

}  // namespace guard_msgs
