// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#pragma once

#include <cstdint>

namespace paglets {

// Current resident set size of this process in bytes (0 if unknown).
std::uint64_t resident_bytes();

}  // namespace paglets
