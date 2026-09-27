// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Minimal test harness: PAGLETS_TEST registers a case, CHECK records
// failures, REQUIRE stops the case, skip() marks it as skipped.

#pragma once

#include <functional>
#include <sstream>
#include <string>
#include <vector>

namespace paglets::test {

struct Case {
    const char* name;
    std::function<void()> body;
};

std::vector<Case>& registry();
void record_failure(const char* file, int line, const std::string& message);
void skip(const std::string& reason);
const std::string& guest_dir();
std::string guest_path(const std::string& file);  // empty if the guest is unavailable

struct Registrar {
    Registrar(const char* name, std::function<void()> body) { registry().push_back({name, std::move(body)}); }
};

struct RequireFailed {};
struct Skipped {
    std::string reason;
};

}  // namespace paglets::test

#define PAGLETS_CONCAT2(a, b) a##b
#define PAGLETS_CONCAT(a, b) PAGLETS_CONCAT2(a, b)
#define PAGLETS_TEST(name)                                                                                            \
    static void PAGLETS_CONCAT(test_fn_, __LINE__)();                                                                 \
    static ::paglets::test::Registrar PAGLETS_CONCAT(test_reg_, __LINE__)(name, &PAGLETS_CONCAT(test_fn_, __LINE__)); \
    static void PAGLETS_CONCAT(test_fn_, __LINE__)()

#define CHECK(expr)                                                                           \
    do {                                                                                      \
        if (!(expr)) ::paglets::test::record_failure(__FILE__, __LINE__, "CHECK(" #expr ")"); \
    } while (0)

#define REQUIRE(expr)                                                                  \
    do {                                                                               \
        if (!(expr)) {                                                                 \
            ::paglets::test::record_failure(__FILE__, __LINE__, "REQUIRE(" #expr ")"); \
            throw ::paglets::test::RequireFailed{};                                    \
        }                                                                              \
    } while (0)

// For std::expected results: fails with the contained error message.
#define REQUIRE_OK(result)                                                                                            \
    do {                                                                                                              \
        if (!(result)) {                                                                                              \
            ::paglets::test::record_failure(__FILE__, __LINE__, std::string(#result " failed: ") + (result).error()); \
            throw ::paglets::test::RequireFailed{};                                                                   \
        }                                                                                                             \
    } while (0)

#define CHECK_EQ(a, b)                                                      \
    do {                                                                    \
        const auto va_ = (a);                                               \
        const auto vb_ = (b);                                               \
        if (!(va_ == vb_)) {                                                \
            std::ostringstream os_;                                         \
            os_ << "CHECK_EQ(" #a ", " #b "): " << va_ << " != " << vb_;    \
            ::paglets::test::record_failure(__FILE__, __LINE__, os_.str()); \
        }                                                                   \
    } while (0)
