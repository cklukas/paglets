// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "platform.hpp"

#include <algorithm>

namespace paglets::platform {

double cpu_percent(const CpuTimes& before, const CpuTimes& after) {
    if (after.total <= before.total || after.busy < before.busy) return 0.0;
    const double busy = static_cast<double>(after.busy - before.busy);
    const double total = static_cast<double>(after.total - before.total);
    return std::clamp(100.0 * busy / total, 0.0, 100.0);
}

}  // namespace paglets::platform
