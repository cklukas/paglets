// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets::verify: an explicit-state model checker for protocols written as
// pure transition functions (for example paglets/protocols/zombie).
//
// A model describes a finite world: its initial state, the successors of a
// state (each labelled, and marked as a fault or not), a canonical encoding
// (equal encodings mean equal states), invariants, and goals. explore()
// visits every reachable state breadth-first and checks
//
// - invariants on every reachable state (safety); a violation comes with
//   the shortest trace that leads to it;
// - goals on the graph of fault-free steps: from every reachable state, a
//   state that satisfies the goal is reachable without further faults. On a
//   finite graph this means no dead end and no trap: whatever the faults
//   did, once they stop, the goal stays reachable; under any scheduler that
//   picks every enabled step with positive probability, it is reached with
//   probability 1.
//
// States are identified by a 128-bit hash of their encoding (hash
// compaction; a collision needs about 2^64 states). The states still to be
// expanded are kept as encodings; successors() and the properties must be
// safe to call from several threads at once.
//
// A model M provides:
//   using State = ...;
//   struct Label { ... };                       // what a step did
//   State initial() const;
//   void successors(const State&, const std::function<void(State, Label, bool fault)>&) const;
//   std::string encode(const State&) const;     // canonical
//   State decode(const std::string&) const;     // the state of an encoding
//   std::string describe(const Label&) const;
//   std::vector<Property<State>> invariants() const;
//   std::vector<Property<State>> goals() const;

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace paglets::verify {

template <class State>
struct Property {
    std::string name;
    std::string description;
    std::function<bool(const State&)> holds;
};

struct Limits {
    std::size_t max_states = 20'000'000;  // stop (incomplete) beyond this
    std::size_t max_depth = 0;            // 0: no limit
    unsigned threads = 0;                 // 0: one per hardware thread
    // Called after every level: states so far, states of the next level.
    std::function<void(std::size_t, std::size_t)> progress;
};

struct Violation {
    std::string property;
    std::string kind;                 // "invariant" or "goal"
    std::vector<std::string> trace;   // steps from the initial state
};

struct Report {
    std::size_t states = 0;
    std::size_t edges = 0;
    std::size_t fault_free_edges = 0;
    std::size_t depth = 0;
    bool complete = true;  // false if a limit stopped the search
    double seconds = 0;
    std::vector<std::string> invariants_checked;
    std::vector<std::string> goals_checked;
    std::vector<Violation> violations;
    bool ok() const { return complete && violations.empty(); }
};

namespace detail {

struct Hash128 {
    std::uint64_t a = 0, b = 0;
    bool operator==(const Hash128&) const = default;
};
struct Hash128Hasher {
    std::size_t operator()(const Hash128& h) const { return static_cast<std::size_t>(h.a ^ (h.b * 0x9e3779b97f4a7c15ull)); }
};

inline std::uint64_t mix(std::uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebull;
    x ^= x >> 31;
    return x;
}

inline Hash128 hash128(std::string_view s) {
    std::uint64_t a = 0x243f6a8885a308d3ull ^ s.size(), b = 0x13198a2e03707344ull + s.size();
    std::size_t i = 0;
    for (; i + 8 <= s.size(); i += 8) {
        std::uint64_t w = 0;
        std::memcpy(&w, s.data() + i, 8);
        a = mix(a ^ w) + 0x9e3779b97f4a7c15ull;
        b = mix(b + w * 0xc2b2ae3d27d4eb4full) ^ a;
    }
    std::uint64_t w = 0;
    std::memcpy(&w, s.data() + i, s.size() - i);
    a = mix(a ^ w ^ 0xa0761d6478bd642full);
    b = mix(b ^ (w + 0xe7037ed1a0b428dbull) ^ a);
    return {a, b};
}

}  // namespace detail

template <class M>
Report explore(const M& model, Limits limits = {}) {
    using State = typename M::State;
    using Label = typename M::Label;
    const auto t0 = std::chrono::steady_clock::now();
    Report report;
    const auto invariants = model.invariants();
    const auto goals = model.goals();
    for (const auto& p : invariants) report.invariants_checked.push_back(p.name);
    for (const auto& p : goals) report.goals_checked.push_back(p.name);

    std::unordered_map<detail::Hash128, std::uint32_t, detail::Hash128Hasher> index;
    struct Parent {
        std::uint32_t state;
        std::uint32_t label;  // into labels
    };
    std::vector<Parent> parents;
    std::vector<std::string> labels;  // distinct step descriptions
    std::unordered_map<std::string, std::uint32_t> label_ids;
    std::vector<std::uint32_t> depth;
    // Fault-free edges (from, to), for the goals.
    std::vector<std::pair<std::uint32_t, std::uint32_t>> edges;
    // goal_hit[g][i]: state i satisfies goal g.
    std::vector<std::vector<bool>> goal_hit(goals.size());

    auto trace_to = [&](std::uint32_t i) {
        std::vector<std::string> trace;
        while (i != 0) {
            trace.push_back(labels[parents[i].label]);
            i = parents[i].state;
        }
        return std::vector<std::string>(trace.rbegin(), trace.rend());
    };

    // What a worker found for one step.
    struct Found {
        detail::Hash128 hash;
        std::uint32_t from;
        bool fault;
        bool known;               // in the index before this level
        std::string encoding;     // for unknown states
        std::string label;        // for unknown states
        std::uint64_t invariants; // bit set: violated
        std::uint64_t goals;      // bit set: satisfied
    };
    auto evaluate = [&](const State& s, std::uint64_t& bad, std::uint64_t& good) {
        bad = good = 0;
        for (std::size_t p = 0; p < invariants.size(); ++p) {
            if (!invariants[p].holds(s)) bad |= 1ull << p;
        }
        for (std::size_t g = 0; g < goals.size(); ++g) {
            if (goals[g].holds(s)) good |= 1ull << g;
        }
    };

    std::vector<bool> violated(invariants.size(), false);
    auto record = [&](std::uint32_t i, std::uint64_t bad, std::uint64_t good) {
        for (std::size_t p = 0; p < invariants.size(); ++p) {
            if (!violated[p] && ((bad >> p) & 1)) {
                violated[p] = true;
                report.violations.push_back({invariants[p].name, "invariant", trace_to(i)});
            }
        }
        for (std::size_t g = 0; g < goals.size(); ++g) goal_hit[g].push_back((good >> g) & 1);
    };

    // The search goes level by level; the states of a level are expanded by
    // all threads, the results merged in a fixed order (the outcome does
    // not depend on the number of threads).
    std::vector<std::pair<std::string, std::uint32_t>> level;
    {
        State s0 = model.initial();
        std::string e = model.encode(s0);
        index.emplace(detail::hash128(e), 0);
        parents.push_back({0, 0});
        depth.push_back(0);
        labels.emplace_back("(initial)");
        std::uint64_t bad = 0, good = 0;
        evaluate(s0, bad, good);
        record(0, bad, good);
        level.emplace_back(std::move(e), 0);
    }
    const unsigned threads = std::max(1u, limits.threads ? limits.threads : std::thread::hardware_concurrency());
    bool stop = false;
    while (!level.empty() && !stop) {
        if (limits.max_depth && depth[level.front().second] >= limits.max_depth) {
            report.complete = false;
            break;
        }
        const std::size_t chunks = std::min<std::size_t>(level.size(), threads * 8);
        std::vector<std::vector<Found>> found(chunks);
        std::atomic<std::size_t> next_chunk{0};
        auto work = [&] {
            for (;;) {
                const std::size_t c = next_chunk.fetch_add(1);
                if (c >= chunks) return;
                const std::size_t lo = level.size() * c / chunks, hi = level.size() * (c + 1) / chunks;
                auto& out = found[c];
                for (std::size_t k = lo; k < hi; ++k) {
                    const State s = model.decode(level[k].first);
                    const std::uint32_t from = level[k].second;
                    model.successors(s, [&](State next, Label label, bool fault) {
                        Found f;
                        f.encoding = model.encode(next);
                        f.hash = detail::hash128(f.encoding);
                        f.from = from;
                        f.fault = fault;
                        f.known = index.contains(f.hash);  // read-only during the level
                        if (f.known) {
                            f.encoding.clear();
                            f.encoding.shrink_to_fit();
                        } else {
                            f.label = model.describe(label);
                            evaluate(next, f.invariants, f.goals);
                        }
                        out.push_back(std::move(f));
                    });
                }
            }
        };
        {
            std::vector<std::thread> pool;
            for (unsigned t = 1; t < threads; ++t) pool.emplace_back(work);
            work();
            for (auto& t : pool) t.join();
        }
        std::vector<std::pair<std::string, std::uint32_t>> next_level;
        for (auto& chunk : found) {
            for (auto& f : chunk) {
                ++report.edges;
                auto [it, fresh] = index.try_emplace(f.hash, static_cast<std::uint32_t>(parents.size()));
                const std::uint32_t j = it->second;
                if (fresh) {
                    if (parents.size() >= limits.max_states) {
                        index.erase(it);
                        report.complete = false;
                        stop = true;
                        continue;
                    }
                    auto [lit, added] = label_ids.try_emplace(std::move(f.label), static_cast<std::uint32_t>(labels.size()));
                    if (added) labels.push_back(lit->first);
                    parents.push_back({f.from, lit->second});
                    depth.push_back(depth[f.from] + 1);
                    report.depth = std::max<std::size_t>(report.depth, depth[f.from] + 1);
                    record(j, f.invariants, f.goals);
                    next_level.emplace_back(std::move(f.encoding), j);
                }
                if (!f.fault && j != f.from) edges.emplace_back(f.from, j);
            }
            chunk.clear();
            chunk.shrink_to_fit();
        }
        level = std::move(next_level);
        if (limits.progress) limits.progress(parents.size(), level.size());
    }
    report.states = parents.size();
    report.fault_free_edges = edges.size();

    // Goals: backward reachability from the goal states over fault-free edges.
    if (!goals.empty() && report.complete) {
        // Reverse adjacency in compressed form.
        std::vector<std::uint32_t> start(report.states + 1, 0);
        for (const auto& e : edges) ++start[e.second + 1];
        for (std::size_t k = 1; k < start.size(); ++k) start[k] += start[k - 1];
        std::vector<std::uint32_t> preds(edges.size());
        {
            auto fill = start;
            for (const auto& e : edges) preds[fill[e.second]++] = e.first;
        }
        edges.clear();
        edges.shrink_to_fit();
        for (std::size_t g = 0; g < goals.size(); ++g) {
            std::vector<bool> reach = goal_hit[g];
            std::vector<std::uint32_t> queue;
            for (std::uint32_t k = 0; k < report.states; ++k) {
                if (reach[k]) queue.push_back(k);
            }
            while (!queue.empty()) {
                const auto k = queue.back();
                queue.pop_back();
                for (auto p = start[k]; p < start[k + 1]; ++p) {
                    if (!reach[preds[p]]) {
                        reach[preds[p]] = true;
                        queue.push_back(preds[p]);
                    }
                }
            }
            // The shallowest state that cannot reach the goal.
            std::uint32_t worst = 0;
            bool found_bad = false;
            for (std::uint32_t k = 0; k < report.states; ++k) {
                if (!reach[k] && (!found_bad || depth[k] < depth[worst])) {
                    worst = k;
                    found_bad = true;
                }
            }
            if (found_bad) report.violations.push_back({goals[g].name, "goal", trace_to(worst)});
        }
    }
    report.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    return report;
}

}  // namespace paglets::verify
