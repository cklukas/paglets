// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <paglets/wire/json.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <cmath>
#include <map>

namespace paglets::test {

namespace {
int failures_in_case = 0;
std::string g_guest_dir;

struct Reported {
    std::string name;
    std::string value;
    std::string unit;
};
std::map<std::string, std::vector<Reported>> reports;  // of the running case, by topic

#ifndef PAGLETS_TEST_PLATFORM
#define PAGLETS_TEST_PLATFORM "unknown"
#endif
#ifndef PAGLETS_TEST_BUILD
#define PAGLETS_TEST_BUILD ""
#endif

using paglets::wire::Json;

Json text(std::string s) {
    return Json(std::move(s));
}

// Adds or replaces the case's entry (by test and mode) in <dir>/<topic>.json.
void write_reports(const std::string& test, const std::string& outcome, double seconds) {
    const char* dir = std::getenv("PAGLETS_RESULTS_DIR");
    if (dir == nullptr || *dir == 0 || reports.empty()) return;
    const std::string mode = std::getenv("PAGLETS_TEST_WORKER") ? "worker processes" : "in-process";
    const auto today = std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now());
    const std::string date = std::format("{:%Y-%m-%d}", std::chrono::year_month_day{today});
    for (const auto& [topic, values] : reports) {
        const auto path = std::filesystem::path(dir) / (topic + ".json");
        Json::Array tests;
        if (std::ifstream in{path}) {
            const std::string old((std::istreambuf_iterator<char>(in)), {});
            if (auto j = Json::parse(old); j && j->find("tests") && j->find("tests")->is_array()) {
                for (const auto& t : j->find("tests")->as_array()) {
                    const Json* n = t.find("test");
                    const Json* m = t.find("mode");
                    if (n && m && n->as_string() == test && m->as_string() == mode) continue;
                    tests.push_back(t);
                }
            }
        }
        Json::Array list;
        for (const auto& v : values) {
            if (v.name.empty()) continue;
            list.emplace_back(Json::Object{{"name", text(v.name)}, {"value", text(v.value)}, {"unit", text(v.unit)}});
        }
        tests.emplace_back(Json::Object{{"test", text(test)},
                                        {"mode", text(mode)},
                                        {"outcome", text(outcome)},
                                        {"seconds", Json::number(std::round(seconds * 10) / 10)},
                                        {"platform", text(PAGLETS_TEST_PLATFORM)},
                                        {"build", text(PAGLETS_TEST_BUILD)},
                                        {"date", text(date)},
                                        {"values", Json(std::move(list))}});
        std::filesystem::create_directories(path.parent_path());
        std::ofstream(path) << Json(Json::Object{{"topic", text(topic)}, {"tests", Json(std::move(tests))}}).dump()
                            << "\n";
    }
}
}  // namespace

std::vector<Case>& registry() {
    static std::vector<Case> cases;
    return cases;
}

void record_failure(const char* file, int line, const std::string& message) {
    ++failures_in_case;
    std::cerr << "    " << file << ":" << line << ": " << message << "\n";
}

void report(const std::string& topic, const std::string& name, const std::string& value, const std::string& unit) {
    reports[topic].push_back({name, value, unit});
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
    // An optional second argument runs only the cases whose names contain it.
    const std::string only = argc > 2 ? argv[2] : "";

    int passed = 0;
    int failed = 0;
    int skipped = 0;
    for (const Case& c : registry()) {
        if (!only.empty() && std::string_view(c.name).find(only) == std::string_view::npos) continue;
        failures_in_case = 0;
        reports.clear();
        const auto began = std::chrono::steady_clock::now();
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
        write_reports(c.name, failures_in_case == 0 ? "passed" : "failed",
                      std::chrono::duration<double>(std::chrono::steady_clock::now() - began).count());
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
