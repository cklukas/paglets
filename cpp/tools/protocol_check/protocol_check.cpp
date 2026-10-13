// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-protocol-check: checks a protocol of the mesh against its
// properties (docs/zombie-protocol.md): every reachable state of a small
// world with faults (paglets/verify/explorer.hpp), and timed random runs
// with virtual clocks (paglets/protocols/zombie/sim.hpp).
//
// Usage: paglets-protocol-check [--protocol zombie] [--suite NAME | world options]
//                               [--simulate] [--seeds N] [--results FILE] [--quiet]
//   --suite quick|full     a set of worlds (quick: the ctest, full: the documentation)
//   world options          --replicas N --work N --observations N --crashes N --cuts N
//                          --drops N --duplicates N --sink-downs N --cancels N
//                          --redirects N --early-timeouts N --in-flight N --max-states N
//   --simulate             timed random runs (with --suite: after the worlds)
//   --seeds N              runs of the simulation (default 1000)
//   --trace SEED           one simulated run, with its role changes and faults
//   --results FILE         record the outcome (docs/results/TOPIC.json format)
// Exit status: 0 if every property holds, 1 if not, 2 for usage errors.

#include <paglets/protocols/zombie/model.hpp>
#include <paglets/protocols/zombie/sim.hpp>

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace zombie = paglets::protocols::zombie;
namespace verify = paglets::verify;

namespace {

struct World {
    std::string name;
    zombie::ModelConfig config;
    std::size_t max_states = 60'000'000;
    std::size_t max_depth = 0;  // bounded: every state within this many steps (safety only)
};

std::string describe(const zombie::ModelConfig& c) {
    std::ostringstream os;
    os << c.replicas << " replicas, work " << c.work << ", " << c.observations << " observation(s); faults: " << c.crashes
       << " crash, " << c.cuts << " cut, " << c.drops << " loss, " << c.duplicates << " duplicate, " << c.sink_downs
       << " sink down, " << c.cancels << " cancel, " << c.redirects << " redirect, " << c.early_timeouts << " early timeout; " << c.in_flight
       << " messages in flight per channel";
    return os.str();
}

std::vector<World> suite(const std::string& name) {
    std::vector<World> worlds;
    auto add = [&](std::string n, auto&& tweak) {
        World w;
        w.name = std::move(n);
        tweak(w.config);
        worlds.push_back(std::move(w));
    };
    auto none = [](zombie::ModelConfig& c) {
        c.crashes = c.cuts = c.drops = c.duplicates = c.early_timeouts = 0;
    };
    if (name == "quick") {
        // Two replicas with one kind of fault each (as the ctest).
        const char* names[] = {"crash", "cut", "loss", "duplicate", "early timeout"};
        for (int fault = 0; fault < 5; ++fault) {
            add(std::string("two replicas: ") + names[fault], [&](auto& c) {
                none(c);
                c.replicas = 2;
                c.crashes = fault == 0;
                c.cuts = fault == 1;
                c.drops = fault == 2;
                c.duplicates = fault == 3;
                c.early_timeouts = fault == 4;
            });
        }
        return worlds;
    }
    if (name == "full") {
        auto two = [&](std::string n, auto&& faults) {
            add("two replicas: " + n, [&](auto& c) {
                none(c);
                c.replicas = 2;
                faults(c);
            });
        };
        two("crash, cut", [](auto& c) { c.crashes = c.cuts = 1; });
        two("crash, loss", [](auto& c) { c.crashes = c.drops = 1; });
        two("crash, duplicate", [](auto& c) { c.crashes = c.duplicates = 1; });
        two("crash, early timeout", [](auto& c) { c.crashes = c.early_timeouts = 1; });
        two("two crashes", [](auto& c) { c.crashes = 2; });
        two("cut, loss, early timeout", [](auto& c) { c.cuts = c.drops = c.early_timeouts = 1; });
        two("crash, cancel", [](auto& c) { c.crashes = c.cancels = 1; });
        two("sink down, redirect", [](auto& c) {
            c.sink_downs = 1;
            c.redirects = 1;
        });
        // Three replicas: every state within a bounded number of steps
        // (safety only; the complete graph is too large).
        for (const auto& [n, crashes, cuts] : {std::tuple{"crash, cut", 1u, 1u}, std::tuple{"two crashes", 2u, 0u}}) {
            add(std::string("three replicas: ") + n, [&](auto& c) {
                none(c);
                c.replicas = 3;
                c.crashes = crashes;
                c.cuts = cuts;
                c.in_flight = 2;
            });
            worlds.back().max_depth = 11;
        }
        return worlds;
    }
    return {};
}

std::string json_text(std::string_view s) {
    std::string out = "\"";
    for (char ch : s) {
        if (ch == '"' || ch == '\\') {
            out += '\\';
            out += ch;
        } else if (static_cast<unsigned char>(ch) < 0x20) {
            out += ' ';
        } else {
            out += ch;
        }
    }
    return out + "\"";
}

struct Recorded {
    std::string test;
    std::string mode;
    bool passed = false;
    double seconds = 0;
    std::vector<std::tuple<std::string, std::string, std::string>> values;
};

std::string today() {
    const std::time_t t = std::time(nullptr);
    char buf[16];
    std::strftime(buf, sizeof buf, "%Y-%m-%d", std::localtime(&t));
    return buf;
}

std::string platform() {
#if defined(__APPLE__)
    std::string os = "Darwin";
#elif defined(_WIN32)
    std::string os = "Windows";
#else
    std::string os = "Linux";
#endif
#if defined(__aarch64__)
    return os + " arm64";
#elif defined(__x86_64__)
    return os + " x86_64";
#else
    return os;
#endif
}

void write_results(const std::string& file, const std::vector<Recorded>& tests) {
    std::ofstream out(file);
    out << "{\"topic\":\"zombie_protocol\",\"tests\":[";
    for (std::size_t i = 0; i < tests.size(); ++i) {
        const auto& t = tests[i];
        if (i) out << ',';
        std::ostringstream secs;
        secs.precision(1);
        secs << std::fixed << t.seconds;
        out << "{\"test\":" << json_text(t.test) << ",\"mode\":" << json_text(t.mode)
            << ",\"outcome\":" << json_text(t.passed ? "passed" : "failed") << ",\"seconds\":" << secs.str()
            << ",\"platform\":" << json_text(platform()) << ",\"build\":\"Release\",\"date\":" << json_text(today())
            << ",\"values\":[";
        for (std::size_t k = 0; k < t.values.size(); ++k) {
            const auto& [name, value, unit] = t.values[k];
            if (k) out << ',';
            out << "{\"name\":" << json_text(name) << ",\"value\":" << json_text(value) << ",\"unit\":" << json_text(unit)
                << '}';
        }
        out << "]}";
    }
    out << "]}\n";
}

int usage() {
    std::cerr << "usage: paglets-protocol-check [--protocol zombie] [--suite quick|full | world options]\n"
                 "                              [--simulate] [--seeds N] [--results FILE] [--quiet]\n";
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    std::optional<std::string> suite_name, results;
    World custom;
    custom.name = "custom world";
    bool simulate = false, quiet = false;
    std::uint64_t seeds = 1000;
    std::optional<std::uint64_t> trace_seed;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto value = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc) return std::nullopt;
            return std::string(argv[++i]);
        };
        auto number = [&](auto& target) {
            auto v = value();
            if (!v) return false;
            target = static_cast<std::remove_reference_t<decltype(target)>>(std::stoull(*v));
            return true;
        };
        auto& c = custom.config;
        bool ok = true;
        if (a == "--protocol") {
            auto v = value();
            ok = v && *v == "zombie";
        } else if (a == "--suite") {
            suite_name = value();
            ok = suite_name.has_value();
        } else if (a == "--results") {
            results = value();
            ok = results.has_value();
        } else if (a == "--simulate") {
            simulate = true;
        } else if (a == "--quiet") {
            quiet = true;
        } else if (a == "--trace") {
            auto v = value();
            ok = v.has_value();
            if (ok) trace_seed = std::stoull(*v);
        } else if (a == "--seeds") {
            ok = number(seeds);
        } else if (a == "--replicas") {
            ok = number(c.replicas);
        } else if (a == "--work") {
            ok = number(c.work);
        } else if (a == "--observations") {
            ok = number(c.observations);
        } else if (a == "--crashes") {
            ok = number(c.crashes);
        } else if (a == "--cuts") {
            ok = number(c.cuts);
        } else if (a == "--drops") {
            ok = number(c.drops);
        } else if (a == "--duplicates") {
            ok = number(c.duplicates);
        } else if (a == "--sink-downs") {
            ok = number(c.sink_downs);
        } else if (a == "--cancels") {
            ok = number(c.cancels);
        } else if (a == "--redirects") {
            ok = number(c.redirects);
        } else if (a == "--early-timeouts") {
            ok = number(c.early_timeouts);
        } else if (a == "--in-flight") {
            ok = number(c.in_flight);
        } else if (a == "--max-states") {
            ok = number(custom.max_states);
        } else {
            ok = false;
        }
        if (!ok) return usage();
    }

    std::vector<Recorded> recorded;
    bool all_ok = true;

    if (trace_seed) {
        zombie::SimConfig sc;
        sc.trace = true;
        const auto run = zombie::Simulation(sc, *trace_seed).run();
        for (const auto& line : run.trace) std::cout << line << "\n";
        return run.failures.empty() ? 0 : 1;
    }
    if (!simulate || suite_name) {
        std::vector<World> worlds = suite_name ? suite(*suite_name) : std::vector<World>{custom};
        if (worlds.empty()) return usage();
        std::set<std::tuple<zombie::Role, zombie::Cause, zombie::Role>> covered;
        for (const auto& w : worlds) {
            zombie::Model model(w.config);
            verify::Limits limits;
            limits.max_states = w.max_states;
            limits.max_depth = w.max_depth;
            const auto report = verify::explore(model, limits);
            for (const auto& t : model.transitions_seen()) covered.insert(t);
            // A bounded world is complete when every level up to its bound was explored.
            const bool complete = report.complete || (w.max_depth && report.depth >= w.max_depth &&
                                                      report.states < w.max_states);
            const bool ok = report.violations.empty() && complete;
            all_ok &= ok;
            std::cout << (ok ? "ok    " : "FAIL  ") << w.name << " (" << describe(w.config) << ")\n"
                      << "      " << report.states << " states, " << report.edges << " steps, depth " << report.depth
                      << ", " << report.seconds << " s"
                      << (w.max_depth ? ", bounded to " + std::to_string(w.max_depth) + " steps (safety only)"
                                      : (report.complete ? "" : ", INCOMPLETE (limit)"))
                      << "\n";
            for (const auto& v : report.violations) {
                std::cout << "      violated " << v.kind << " " << v.property << " after " << v.trace.size()
                          << " steps:\n";
                if (!quiet) {
                    for (const auto& s : v.trace) std::cout << "        " << s << "\n";
                }
            }
            Recorded rec;
            rec.test = "zombie protocol, exhaustive: " + w.name;
            rec.mode = w.max_depth ? "model checker, bounded" : "model checker";
            rec.passed = ok;
            rec.seconds = report.seconds;
            rec.values.emplace_back("reachable states", std::to_string(report.states), "");
            rec.values.emplace_back("steps", std::to_string(report.edges), "");
            rec.values.emplace_back(w.max_depth ? "explored to (bounded)" : "longest shortest path",
                                    std::to_string(report.depth), "steps");
            rec.values.emplace_back("properties", std::to_string(report.invariants_checked.size() + report.goals_checked.size()),
                                    "");
            recorded.push_back(rec);
        }
        // The table of role changes: every entry must occur somewhere.
        std::size_t missing = 0;
        for (const auto& a : zombie::allowed_transitions) {
            if (!covered.contains({a.from, a.cause, a.to})) {
                ++missing;
                if (!quiet || suite_name)
                    std::cout << "      not reached: " << zombie::to_string(a.from) << " --" << zombie::to_string(a.cause)
                              << "--> " << zombie::to_string(a.to) << "\n";
            }
        }
        std::cout << "role changes: " << covered.size() << " of " << zombie::allowed_transitions.size()
                  << " in the table reached\n";
        if (suite_name && *suite_name == "full" && missing) all_ok = false;
    }
    if (simulate) {
        zombie::SimConfig sc;
        const auto t0 = std::chrono::steady_clock::now();
        const auto summary = zombie::simulate_many(sc, seeds);
        Recorded rec;
        rec.test = "zombie protocol: " + std::to_string(seeds) + " timed random runs with faults";
        rec.mode = "simulation";
        rec.passed = summary.failures.empty();
        rec.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        std::cout << summary.text();
        for (const auto& [n, v, u] : summary.values()) rec.values.emplace_back(n, v, u);
        recorded.push_back(rec);
        all_ok &= rec.passed;
    }
    if (results) write_results(*results, recorded);
    return all_ok ? 0 : 1;
}
