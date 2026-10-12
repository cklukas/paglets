// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Hexadecimal digits of pi at any position (Bailey-Borwein-Plouffe): every
// chunk of digits can be computed on its own, which makes pi a good chunked
// compute job. Header only: the pi paglet and the host tests share it.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>

namespace pi_bbp {

// 16^e mod m (m > 0), in integers: m < 2^32, so products fit 64 bits (exact,
// and far faster than fmod on doubles, which Wasm has no instruction for).
inline std::uint64_t pow16_mod(std::int64_t e, std::uint64_t m) {
    if (m == 1) return 0;
    std::uint64_t result = 1;
    std::uint64_t base = 16 % m;
    for (auto x = static_cast<std::uint64_t>(e); x > 0; x >>= 1) {
        if (x & 1) result = result * base % m;
        base = base * base % m;
    }
    return result;
}

// The fractional part of sum_{k0 <= k < k1} 16^(n-k) / (8k+j), for k1 <= n+1
// (the terms with a whole part; only their fraction counts).
inline double series_head(int j, std::int64_t n, std::int64_t k0, std::int64_t k1) {
    double s = 0.0;
    for (std::int64_t k = k0; k < k1; ++k) {
        const auto denominator = static_cast<std::uint64_t>(8 * k + j);
        s += static_cast<double>(pow16_mod(n - k, denominator)) / static_cast<double>(denominator);
        s -= std::floor(s);
    }
    return s;
}

// The rest, k > n: terms below one that shrink by 16 each.
inline double series_tail(int j, std::int64_t n) {
    double s = 0.0;
    for (std::int64_t k = n + 1; k <= n + 100; ++k) {
        const double term = std::pow(16.0, static_cast<double>(n - k)) / (8.0 * static_cast<double>(k) + j);
        if (term < 1e-17) break;
        s += term;
    }
    return s - std::floor(s);
}

// --8<-- [start:digits]
// Up to six hex digits of pi after the point, starting at `position` (0: the
// first digit after "3."), from
//   pi = sum_k 16^-k (4/(8k+1) - 2/(8k+4) - 1/(8k+5) - 1/(8k+6)).
// The work grows with the position: about 4 * position terms. step()
// computes at most `terms` of them, so a paglet can spread a block over
// several handler calls (the parts of a sum add up modulo 1).
class Block {
public:
    Block() = default;
    Block(std::int64_t position, int count) : at_(position), count_(count) {}

    // True when the digits are there.
    bool step(std::int64_t terms) {
        static constexpr int js[4] = {1, 4, 5, 6};
        while (series_ < 4 && terms > 0) {
            const std::int64_t end = std::min(at_ + 1, k_ + terms);
            part_[series_] += series_head(js[series_], at_, k_, end);
            part_[series_] -= std::floor(part_[series_]);
            terms -= end - k_;
            k_ = end;
            if (k_ == at_ + 1) {
                part_[series_] += series_tail(js[series_], at_);
                ++series_;
                k_ = 0;
            }
        }
        return series_ == 4;
    }

    std::string digits() const {
        static constexpr char hex[] = "0123456789ABCDEF";
        double x = 4.0 * part_[0] - 2.0 * part_[1] - part_[2] - part_[3];
        x -= std::floor(x);
        std::string out;
        for (int i = 0; i < count_; ++i) {
            x *= 16.0;
            const int digit = static_cast<int>(x);
            out.push_back(hex[digit]);
            x -= digit;
        }
        return out;
    }

private:
    std::int64_t at_ = 0;
    int count_ = 0;
    int series_ = 0;
    std::int64_t k_ = 0;
    double part_[4] = {0, 0, 0, 0};
};
// --8<-- [end:digits]

// `count` hex digits from `position`, six at a time (well within the
// precision of a double).
inline std::string hex_digits(std::int64_t position, std::int64_t count) {
    std::string out;
    out.reserve(static_cast<std::size_t>(count));
    for (std::int64_t at = position; at < position + count; at += 6) {
        Block b(at, static_cast<int>(std::min<std::int64_t>(6, position + count - at)));
        while (!b.step(1'000'000)) {
        }
        out += b.digits();
    }
    return out;
}

}  // namespace pi_bbp
