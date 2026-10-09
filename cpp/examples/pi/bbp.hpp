// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Hexadecimal digits of pi at any position (Bailey-Borwein-Plouffe): every
// chunk of digits can be computed on its own, which makes pi a good chunked
// compute job. Header only: the pi paglet and the host tests share it.

#pragma once

#include <cmath>
#include <cstdint>
#include <string>

namespace pi_bbp {

// 16^e mod m (m > 0).
inline double pow16_mod(std::int64_t e, double m) {
    if (m == 1.0) return 0.0;
    double result = 1.0;
    double base = std::fmod(16.0, m);
    while (e > 0) {
        if (e & 1) result = std::fmod(result * base, m);
        base = std::fmod(base * base, m);
        e >>= 1;
    }
    return result;
}

// The fractional part of 16^n * sum_k 1/(16^k (8k+j)).
inline double series(int j, std::int64_t n) {
    double s = 0.0;
    for (std::int64_t k = 0; k <= n; ++k) {
        const double denominator = 8.0 * static_cast<double>(k) + j;
        s += pow16_mod(n - k, denominator) / denominator;
        s -= std::floor(s);
    }
    for (std::int64_t k = n + 1; k <= n + 100; ++k) {
        const double term = std::pow(16.0, static_cast<double>(n - k)) / (8.0 * static_cast<double>(k) + j);
        if (term < 1e-17) break;
        s += term;
        s -= std::floor(s);
    }
    return s;
}

// Hex digits of pi after the point, starting at `position` (0: the first
// digit after "3."), 6 at a time (well within double precision).
inline std::string hex_digits(std::int64_t position, std::int64_t count) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(static_cast<std::size_t>(count));
    for (std::int64_t at = position; at < position + count; at += 6) {
        double x = 4.0 * series(1, at) - 2.0 * series(4, at) - series(5, at) - series(6, at);
        x -= std::floor(x);
        for (int i = 0; i < 6 && at + i < position + count; ++i) {
            x *= 16.0;
            const int digit = static_cast<int>(x);
            out.push_back(hex[digit]);
            x -= digit;
        }
    }
    return out;
}

}  // namespace pi_bbp
