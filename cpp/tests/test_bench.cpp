// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Benchmarks (WP20, planning/cpp-benchmarks.md): activation latency,
// message throughput, move latency, image transfer with and without page
// reuse, memory per paglet. In CI they run briefly (as smoke tests, with
// the suite's in-process and worker modes); PAGLETS_BENCH=1 runs them at
// full size, and PAGLETS_BENCH_JSON=FILE appends the results to a JSON
// lines file.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include "../host/src/process_stats.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <numeric>

using namespace paglets::test;

namespace {

bool full() {
    const char* v = std::getenv("PAGLETS_BENCH");
    return v != nullptr && *v != '\0' && std::string_view(v) != "0";
}

int scaled(int ci, int full_size) {
    return full() ? full_size : ci;
}

std::string mode() {
    const char* w = std::getenv("PAGLETS_TEST_WORKER");
    return w != nullptr && *w != '\0' ? "workers" : "in-process";
}

using Clock = std::chrono::steady_clock;

double ms_since(Clock::time_point t) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t).count();
}

struct Sample {
    double median = 0;
    double p90 = 0;
    double min = 0;
};

Sample summarize(std::vector<double> v) {
    if (v.empty()) return {};
    std::ranges::sort(v);
    return Sample{v[v.size() / 2], v[std::min(v.size() - 1, v.size() * 9 / 10)], v.front()};
}

void report(const std::string& name, const std::vector<std::pair<std::string, double>>& values) {
    std::cerr << "    bench " << name << " (" << mode() << "):";
    for (const auto& [k, v] : values) std::cerr << " " << k << "=" << v;
    std::cerr << "\n";
    if (const char* file = std::getenv("PAGLETS_BENCH_JSON")) {
        std::ofstream out(file, std::ios::app);
        out << "{\"bench\":\"" << name << "\",\"mode\":\"" << mode() << "\"";
        for (const auto& [k, v] : values) out << ",\"" << k << "\":" << v;
        out << "}\n";
    }
}

constexpr auto deactivate_op = static_cast<std::int32_t>(abi::LifecycleOp::deactivate);

}  // namespace

PAGLETS_TEST("bench: messages to an active paglet and activations of an inactive one") {
    Fixture f;
    const auto id = f.create("conformance.wasm");
    REQUIRE((f.call(id, "count").status) == (0));
    // Request and reply to an active paglet.
    const int calls = scaled(200, 20'000);
    std::vector<double> latency;
    latency.reserve(static_cast<std::size_t>(calls));
    const auto start = Clock::now();
    for (int i = 0; i < calls; ++i) {
        const auto t = Clock::now();
        REQUIRE((f.runtime->call(id, "echo", text("x"), 10s).status) == (0));
        latency.push_back(ms_since(t));
    }
    const double seconds = ms_since(start) / 1000;
    const auto s = summarize(latency);
    report("request-reply", {{"calls", calls}, {"per_s", calls / seconds}, {"median_ms", s.median}, {"p90_ms", s.p90}});
    CHECK(s.median > 0);

    // Activation: the paglet is inactive (its image stored), a message wakes it.
    const int wakes = scaled(10, 200);
    std::vector<double> activation;
    for (int i = 0; i < wakes; ++i) {
        REQUIRE((f.code(id, "lifecycle", Cmd{.code = deactivate_op})) == (0));
        REQUIRE(f.runtime->wait_idle());
        auto info = f.runtime->info(id);
        REQUIRE(info.has_value());
        if (info->state != rt::PagletState::inactive) continue;
        const auto t = Clock::now();
        REQUIRE((f.runtime->call(id, "echo", text("wake"), 10s).status) == (0));
        activation.push_back(ms_since(t));
    }
    REQUIRE(!activation.empty());
    const auto a = summarize(activation);
    report("activation", {{"wakes", static_cast<double>(activation.size())}, {"median_ms", a.median}, {"p90_ms", a.p90}});
}

PAGLETS_TEST("bench: memory per paglet") {
    Fixture f;
    const auto module = f.module("counter.wasm");
    const int n = scaled(20, 500);
    REQUIRE(f.runtime->create(module, rt::CreateOptions{}).has_value());  // the module compiled and cached
    const std::uint64_t before = paglets::resident_bytes();
    if (before == 0) skip("resident memory is not known on this platform");
    for (int i = 0; i < n; ++i) REQUIRE(f.runtime->create(module, rt::CreateOptions{}).has_value());
    REQUIRE(f.runtime->wait_idle());
    const std::uint64_t after = paglets::resident_bytes();
    const double per = static_cast<double>(after > before ? after - before : 0) / n;
    report("memory-per-paglet", {{"paglets", n}, {"bytes", per}, {"KB", per / 1024}});
    CHECK(after >= before);
}

PAGLETS_TEST("bench: moves between hosts and page reuse") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const std::string module = a.f->module("conformance.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('b')));
    REQUIRE_OK(created);
    const auto id = *created;
    REQUIRE((a.f->code(id, "ballast", Cmd{.ms = 4 * 1024 * 1024})) == (4 * 1024 * 1024));  // 64 pages that stay the same
    const int moves = scaled(6, 100);
    std::vector<double> latency;
    std::uint64_t first_pages = 0;
    std::uint64_t later_pages = 0;
    Mesh::Host* at = &a;
    for (int i = 0; i < moves; ++i) {
        Mesh::Host* to = at == &a ? &b : &a;
        const auto sent_before = at->node->move_stats().pages_sent;
        const auto t = Clock::now();
        REQUIRE_OK(at->node->dispatch(id, to == &a ? "a" : "b"));
        REQUIRE(net.settle([&] { return to->f->runtime->info(id).has_value() && !at->f->runtime->info(id); }, 2000));
        latency.push_back(ms_since(t));
        const auto pages = at->node->move_stats().pages_sent - sent_before;
        if (i == 0) {
            first_pages = pages;
        } else if (i >= 2) {
            later_pages += pages;  // both hosts have seen the image by now
        }
        at = to;
    }
    const auto s = summarize(latency);
    const double later = moves > 2 ? static_cast<double>(later_pages) / (moves - 2) : 0;
    const auto stats_a = a.node->move_stats();
    const auto stats_b = b.node->move_stats();
    report("move", {{"moves", moves},
                    {"median_ms", s.median},
                    {"p90_ms", s.p90},
                    {"first_move_pages", static_cast<double>(first_pages)},
                    {"later_move_pages", later},
                    {"pages_reused", static_cast<double>(stats_a.pages_reused + stats_b.pages_reused)},
                    {"bytes_sent", static_cast<double>(stats_a.bytes_sent + stats_b.bytes_sent)}});
    CHECK(first_pages >= 64);
    CHECK(later < static_cast<double>(first_pages) / 4);  // page reuse: only changed pages travel
}
