// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Mesh Journey demo (planning/cpp-demo-paglets.md, demo 1).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace journey_msgs {

// `start`: visit `hosts` in turn (host names or key IDs; empty: every host
// mesh-info knows, or those with `label`), then come home.
struct Start {
    std::vector<std::string> hosts;
    std::string label;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::vector<std::string> itinerary;  // key IDs, in order
};

// One stop of the journey.
struct Stop {
    std::string host;  // key ID
    std::string host_name;
    std::string os;
    std::string architecture;
    double load_per_cpu = 0;
    std::int64_t arrived_ms = 0;   // Unix milliseconds, this host's clock
    std::int64_t transfer_ms = 0;  // from leaving the previous host (clocks may differ)
};

struct Journal {
    std::string state;  // idle, travelling, home, failed
    std::vector<Stop> stops;
    std::vector<std::string> skipped;  // hosts it could not reach, with the reason
};

}  // namespace journey_msgs
