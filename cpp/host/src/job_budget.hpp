// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>

namespace paglets::runtime::job_detail {

// -1 is unlimited, 0 selects the default, positive values are milliseconds.
// -2 denotes invalid configuration/input. A positive owner ceiling applies
// to unlimited requests too; there is no framework-imposed duration ceiling.
constexpr std::int64_t budget(std::int64_t requested, std::int64_t fallback, std::int64_t ceiling) {
    if (requested < -1 || fallback == 0 || fallback < -1 || ceiling == 0 || ceiling < -1) return -2;
    auto effective = requested == 0 ? fallback : requested;
    if (ceiling > 0 && (effective == -1 || effective > ceiling)) effective = ceiling;
    return effective;
}

template <class Clock>
typename Clock::time_point deadline(typename Clock::time_point now, std::int64_t ms) {
    if (ms == -1) return Clock::time_point::max();
    const auto available = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::time_point::max() - now).count();
    return now + std::chrono::duration_cast<typename Clock::duration>(std::chrono::milliseconds(std::min(ms, available)));
}

}  // namespace paglets::runtime::job_detail
