// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#pragma once

#include <cstdint>
#include <limits>

namespace paglets::time_detail {

// Timestamp fields are signed Unix milliseconds. Keep an out-of-range
// deadline at the nearest representable endpoint instead of wrapping it.
constexpr std::int64_t saturating_add(std::int64_t time, std::int64_t duration) noexcept {
    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();
    constexpr auto minimum = std::numeric_limits<std::int64_t>::min();
    if (duration > 0 && time > maximum - duration) return maximum;
    if (duration < 0 && time < minimum - duration) return minimum;
    return time + duration;
}

}  // namespace paglets::time_detail
