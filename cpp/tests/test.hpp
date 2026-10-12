// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Minimal test harness: PAGLETS_TEST registers a case, CHECK records
// failures, REQUIRE stops the case, skip() marks it as skipped.

#pragma once

#include <functional>
#include <sstream>
#include <string>
#include <type_traits>
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

// Results for the documentation (docs/examples/, rendered by
// paglets-docs-sync): with PAGLETS_RESULTS_DIR set, a test that reports to
// `topic` is recorded in <dir>/<topic>.json after it ran, with its outcome,
// duration and the values it reported (`name` empty: only the outcome).
void report(const std::string& topic, const std::string& name = {}, const std::string& value = {},
            const std::string& unit = {});
template <class T>
    requires std::is_arithmetic_v<T>
void report(const std::string& topic, const std::string& name, T value, const std::string& unit = {}) {
    std::ostringstream os;
    os.precision(4);
    os << std::fixed << value;
    std::string text = os.str();
    // Integral values and round numbers without trailing zeros.
    if (text.find('.') != std::string::npos) {
        while (text.back() == '0') text.pop_back();
        if (text.back() == '.') text.pop_back();
    }
    report(topic, name, text, unit);
}

struct Registrar {
    Registrar(const char* name, std::function<void()> body) { registry().push_back({name, std::move(body)}); }
};

// Text of an std::expected error: strings as they are, error codes as numbers.
template <class E>
std::string describe_error(const E& error) {
    if constexpr (std::is_convertible_v<const E&, std::string>) {
        return std::string(error);
    } else {
        std::ostringstream os;
        os << "error code " << error;
        return os.str();
    }
}

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
#define REQUIRE_OK(result)                                                                             \
    do {                                                                                               \
        if (!(result)) {                                                                               \
            ::paglets::test::record_failure(                                                           \
                __FILE__, __LINE__,                                                                    \
                std::string(#result " failed: ") + ::paglets::test::describe_error((result).error())); \
            throw ::paglets::test::RequireFailed{};                                                    \
        }                                                                                              \
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
