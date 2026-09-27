// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <filesystem>
#include <iostream>

namespace paglets::test {

namespace {
int failures_in_case = 0;
std::string g_guest_dir;
}  // namespace

std::vector<Case>& registry() {
    static std::vector<Case> cases;
    return cases;
}

void record_failure(const char* file, int line, const std::string& message) {
    ++failures_in_case;
    std::cerr << "    " << file << ":" << line << ": " << message << "\n";
}

void skip(const std::string& reason) {
    throw Skipped{reason};
}

const std::string& guest_dir() {
    return g_guest_dir;
}

std::string guest_path(const std::string& file) {
    if (g_guest_dir.empty()) return {};
    const auto p = std::filesystem::path(g_guest_dir) / file;
    return std::filesystem::exists(p) ? p.string() : std::string{};
}

}  // namespace paglets::test

int main(int argc, char** argv) {
    using namespace paglets::test;
    if (argc > 1) g_guest_dir = argv[1];

    int passed = 0;
    int failed = 0;
    int skipped = 0;
    for (const Case& c : registry()) {
        failures_in_case = 0;
        std::string status;
        try {
            c.body();
            status = failures_in_case == 0 ? "ok" : "FAILED";
        } catch (const RequireFailed&) {
            status = "FAILED";
        } catch (const Skipped& s) {
            status = "skipped (" + s.reason + ")";
            ++skipped;
            std::cout << "[skip] " << c.name << ": " << s.reason << "\n";
            continue;
        } catch (const std::exception& e) {
            record_failure("exception", 0, e.what());
            status = "FAILED";
        }
        if (failures_in_case == 0) {
            ++passed;
            std::cout << "[ ok ] " << c.name << "\n";
        } else {
            ++failed;
            std::cout << "[FAIL] " << c.name << "\n";
        }
    }
    std::cout << passed << " passed, " << failed << " failed, " << skipped << " skipped\n";
    return failed == 0 ? 0 : 1;
}
