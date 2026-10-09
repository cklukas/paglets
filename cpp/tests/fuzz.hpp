// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// A small mutation fuzzer for the decoders (WP21, planning/cpp-hardening.md).
// Each target gets valid seed inputs; the engine mutates them (bit flips,
// interesting bytes, insertions, deletions, splices, length fields) and
// feeds the results to the target for a time budget. Targets must neither
// crash nor trip a sanitizer, whatever the input; they may also check
// properties (a decoded value encodes back to what decodes the same).
//
// PAGLETS_FUZZ_SECONDS (default 2) sets the budget per target,
// PAGLETS_FUZZ_SEED the random seed (default: random, printed), and
// PAGLETS_FUZZ_REPLAY=FILE runs a target on one saved input. The input
// that crashes a target is saved as fuzz-crash-<target>.bin first.

#pragma once

#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <span>
#include <string>
#include <vector>

namespace paglets::fuzz {

using Bytes = std::vector<std::uint8_t>;

namespace detail {

inline const std::uint8_t* current_data = nullptr;
inline std::size_t current_size = 0;
inline char crash_file[256] = {};

// Saves the input being run (async-signal-safe enough for a crash report).
inline void save_current() {
    if (current_data == nullptr || crash_file[0] == '\0') return;
    if (std::FILE* f = std::fopen(crash_file, "wb")) {
        std::fwrite(current_data, 1, current_size, f);
        std::fclose(f);
    }
}

extern "C" inline void on_crash_signal(int sig) {
    save_current();
    std::signal(sig, SIG_DFL);
    std::raise(sig);
}

}  // namespace detail

}  // namespace paglets::fuzz

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
// Sanitizers call this before they abort.
extern "C" void __sanitizer_set_death_callback(void (*callback)());
#define PAGLETS_FUZZ_DEATH_CALLBACK 1
#endif

namespace paglets::fuzz {

struct Stats {
    std::uint64_t runs = 0;
    std::uint64_t seed = 0;
};

class Mutator {
public:
    explicit Mutator(std::uint64_t seed) : rng_(seed) {}

    Bytes mutate(const Bytes& in, const std::vector<Bytes>& pool) {
        Bytes out = in;
        const int rounds = 1 + static_cast<int>(below(4));
        for (int i = 0; i < rounds; ++i) step(out, pool);
        if (out.size() > 1 << 20) out.resize(1 << 20);
        return out;
    }

    std::uint64_t below(std::uint64_t n) { return n == 0 ? 0 : rng_() % n; }

private:
    void step(Bytes& b, const std::vector<Bytes>& pool) {
        static constexpr std::uint8_t interesting[] = {0x00, 0x01, 0x7f, 0x80, 0xff, 0xc0, 0xc1, 0xc2, 0xc3, 0xcc,
                                                       0xcd, 0xce, 0xcf, 0xd0, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde,
                                                       0xdf, 0x90, 0x9f, 0x80, 0x8f, 0xa0, 0xbf, 0xc4, 0xc5, 0xc6};
        switch (below(b.empty() ? 3 : 9)) {
            case 0: {  // insert random bytes
                const std::size_t at = static_cast<std::size_t>(below(b.size() + 1));
                const std::size_t n = 1 + static_cast<std::size_t>(below(8));
                Bytes ins(n);
                for (auto& x : ins) x = static_cast<std::uint8_t>(below(256));
                b.insert(b.begin() + static_cast<std::ptrdiff_t>(at), ins.begin(), ins.end());
                break;
            }
            case 1: {  // insert an interesting byte
                const std::size_t at = static_cast<std::size_t>(below(b.size() + 1));
                b.insert(b.begin() + static_cast<std::ptrdiff_t>(at), interesting[below(std::size(interesting))]);
                break;
            }
            case 2: {  // splice with another input
                if (pool.empty()) break;
                const Bytes& other = pool[static_cast<std::size_t>(below(pool.size()))];
                if (other.empty()) break;
                const std::size_t from = static_cast<std::size_t>(below(other.size()));
                const std::size_t n = 1 + static_cast<std::size_t>(below(other.size() - from));
                const std::size_t at = static_cast<std::size_t>(below(b.size() + 1));
                b.insert(b.begin() + static_cast<std::ptrdiff_t>(at), other.begin() + static_cast<std::ptrdiff_t>(from),
                         other.begin() + static_cast<std::ptrdiff_t>(from + n));
                break;
            }
            case 3:
            case 4: {  // flip a bit
                const std::size_t at = static_cast<std::size_t>(below(b.size()));
                b[at] ^= static_cast<std::uint8_t>(1u << below(8));
                break;
            }
            case 5: {  // replace with an interesting byte
                b[static_cast<std::size_t>(below(b.size()))] = interesting[below(std::size(interesting))];
                break;
            }
            case 6: {  // delete a range
                const std::size_t at = static_cast<std::size_t>(below(b.size()));
                const std::size_t n = 1 + static_cast<std::size_t>(below(std::min<std::size_t>(16, b.size() - at)));
                b.erase(b.begin() + static_cast<std::ptrdiff_t>(at), b.begin() + static_cast<std::ptrdiff_t>(at + n));
                break;
            }
            case 7: {  // duplicate a range
                const std::size_t at = static_cast<std::size_t>(below(b.size()));
                const std::size_t n = 1 + static_cast<std::size_t>(below(std::min<std::size_t>(32, b.size() - at)));
                Bytes chunk(b.begin() + static_cast<std::ptrdiff_t>(at), b.begin() + static_cast<std::ptrdiff_t>(at + n));
                b.insert(b.begin() + static_cast<std::ptrdiff_t>(at), chunk.begin(), chunk.end());
                break;
            }
            default: {  // a big-endian length-like value
                if (b.size() < 4) break;
                const std::size_t at = static_cast<std::size_t>(below(b.size() - 3));
                static constexpr std::uint32_t values[] = {0, 1, 0x7f, 0x80, 0xff, 0x100, 0xffff, 0x10000,
                                                           0x7fffffff, 0x80000000, 0xffffffff};
                const std::uint32_t v = values[below(std::size(values))];
                b[at] = static_cast<std::uint8_t>(v >> 24);
                b[at + 1] = static_cast<std::uint8_t>(v >> 16);
                b[at + 2] = static_cast<std::uint8_t>(v >> 8);
                b[at + 3] = static_cast<std::uint8_t>(v);
                break;
            }
        }
    }

    std::mt19937_64 rng_;
};

inline double budget_seconds() {
    if (const char* s = std::getenv("PAGLETS_FUZZ_SECONDS")) return std::max(0.01, std::atof(s));
    return 2.0;
}

// Runs `target` on the seeds and their mutations until the budget is used.
inline Stats run(const std::string& name, const std::vector<Bytes>& seeds,
                 const std::function<void(std::span<const std::uint8_t>)>& target) {
    std::snprintf(detail::crash_file, sizeof(detail::crash_file), "fuzz-crash-%s.bin", name.c_str());
#ifdef PAGLETS_FUZZ_DEATH_CALLBACK
    __sanitizer_set_death_callback(detail::save_current);
#endif
    std::signal(SIGSEGV, detail::on_crash_signal);
    std::signal(SIGABRT, detail::on_crash_signal);
    std::signal(SIGFPE, detail::on_crash_signal);
#ifdef SIGBUS
    std::signal(SIGBUS, detail::on_crash_signal);
#endif
    auto feed = [&](const Bytes& input) {
        detail::current_data = input.data();
        detail::current_size = input.size();
        target(input);
    };
    Stats stats;
    if (const char* replay = std::getenv("PAGLETS_FUZZ_REPLAY")) {
        std::ifstream in(replay, std::ios::binary);
        const Bytes input((std::istreambuf_iterator<char>(in)), {});
        feed(input);
        stats.runs = 1;
        return stats;
    }
    stats.seed = std::random_device{}();
    if (const char* s = std::getenv("PAGLETS_FUZZ_SEED")) stats.seed = std::strtoull(s, nullptr, 10);
    Mutator m(stats.seed);
    std::vector<Bytes> pool = seeds;
    for (const auto& s : seeds) {
        feed(s);
        ++stats.runs;
    }
    const auto until = std::chrono::steady_clock::now() + std::chrono::duration<double>(budget_seconds());
    while (std::chrono::steady_clock::now() < until && !pool.empty()) {
        for (int i = 0; i < 64; ++i) {
            const Bytes& base = pool[static_cast<std::size_t>(m.below(pool.size()))];
            Bytes input = m.mutate(base, pool);
            feed(input);
            ++stats.runs;
            // Keep some mutants as new starting points (a crude stand-in
            // for coverage feedback).
            if (m.below(16) == 0 && pool.size() < 4096) pool.push_back(std::move(input));
        }
    }
    detail::current_data = nullptr;
    std::signal(SIGSEGV, SIG_DFL);
    std::signal(SIGABRT, SIG_DFL);
    std::signal(SIGFPE, SIG_DFL);
#ifdef SIGBUS
    std::signal(SIGBUS, SIG_DFL);
#endif
    std::cerr << "    fuzz " << name << ": " << stats.runs << " runs, seed " << stats.seed << "\n";
    return stats;
}

}  // namespace paglets::fuzz
