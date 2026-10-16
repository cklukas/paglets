// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets::verify on the zombie protocol: small worlds explored
// exhaustively, and timed runs with faults (docs/zombie-protocol.md).

#include "test.hpp"

#include <paglets/protocols/zombie/model.hpp>
#include <paglets/protocols/zombie/sim.hpp>

namespace z = paglets::protocols::zombie;

PAGLETS_TEST("verify: missing or disconnected role-change reports are rejected") {
    CHECK(z::role_changes_match(z::Role::main, z::Role::main, {}));
    CHECK(!z::role_changes_match(z::Role::main, z::Role::ended, {}));
    CHECK(!z::role_changes_match(z::Role::main, z::Role::ended,
        {z::Transition{z::Role::sleeping, z::Role::ended, z::Cause::tomb}}));
    CHECK(!z::role_changes_match(z::Role::main, z::Role::ended,
        {z::Transition{z::Role::main, z::Role::delivering, z::Cause::done}}));
    CHECK(z::role_changes_match(z::Role::main, z::Role::ended,
        {z::Transition{z::Role::main, z::Role::delivering, z::Cause::done},
         z::Transition{z::Role::delivering, z::Role::ended, z::Cause::delivered}}));
    CHECK(z::role_changes_match(z::Role::recovering, z::Role::recovering,
        {z::Transition{z::Role::recovering, z::Role::recovering, z::Cause::recovered}}));
}

PAGLETS_TEST("zombie: small worlds are explored completely without a violated property") {
    // Two replicas with each kind of fault; the documentation records the
    // larger worlds (paglets-protocol-check --suite full).
    for (int fault = 0; fault < 6; ++fault) {
        z::ModelConfig c;
        c.replicas = 2;
        c.crashes = fault == 0;
        c.cuts = fault == 1;
        c.drops = fault == 2;
        c.duplicates = fault == 3;
        c.early_timeouts = fault == 4;
        if (fault == 5) {  // a host that joins later, and a crash
            c.late_hosts = 1;
            c.crashes = 1;
        }
        z::Model model(c);
        const auto report = paglets::verify::explore(model);
        CHECK(report.complete);
        for (const auto& v : report.violations) {
            std::string trace;
            for (const auto& s : v.trace) trace += "\n    " + s;
            paglets::test::record_failure(__FILE__, __LINE__, v.kind + " " + v.property + " violated:" + trace);
        }
        CHECK(report.states > 1000);
    }
}

PAGLETS_TEST("zombie: timed runs with faults deliver one result and end") {
    const auto summary = z::simulate_many(z::SimConfig{}, 200);
    for (const auto& [seed, what] : summary.failures) {
        paglets::test::record_failure(__FILE__, __LINE__, "seed " + std::to_string(seed) + ": " + what);
    }
    CHECK(summary.takeovers > 0);
    CHECK(summary.takeover_max_ms <= z::takeover_bound(z::SimConfig{}));
}

namespace {

// A finite fork whose terminal state is 4. The goal can deliberately be
// unreachable, so merely evaluating its predicate cannot claim recovery.
struct RecoveryEvidenceModel {
    using State = int;
    using Label = int;
    int goal = 9;
    State initial() const { return 0; }
    void successors(State s, const std::function<void(State, Label, bool)>& add) const {
        if (s == 0) for (int n : {1, 2, 3}) add(n, n, false);
        else if (s < 4) add(4, 4, false);
    }
    std::string encode(State s) const { return std::to_string(s); }
    State decode(const std::string& s) const { return std::stoi(s); }
    std::string describe(Label n) const { return "step " + std::to_string(n); }
    std::vector<paglets::verify::Property<State>> invariants() const {
        return {{"nonnegative", "states are nonnegative", [](State s) { return s >= 0; }}};
    }
    std::vector<paglets::verify::Property<State>> goals() const {
        return {{"terminal goal", "the requested terminal state remains reachable", [g = goal](State s) { return s == g; }}};
    }
};

}  // namespace

PAGLETS_TEST("verify: bounded and state-limited worlds record only safety checks, not declared recovery goals") {
    for (unsigned threads : {1u, 3u}) {
        for (bool depth_bound : {false, true}) {
            paglets::verify::Limits limits;
            limits.threads = threads;
            if (depth_bound) limits.max_depth = 1;
            else limits.max_states = 2;
            const auto report = paglets::verify::explore(RecoveryEvidenceModel{}, limits);
            CHECK(!report.complete);
            CHECK(!report.ok());
            CHECK(report.violations.empty());
            CHECK_EQ(report.invariants_checked.size(), std::size_t{1});
            CHECK_EQ(report.goals_declared.size(), std::size_t{1});
            CHECK(report.goals_checked.empty());
        }
    }
}

PAGLETS_TEST("verify: complete worlds record both successful and violated recovery checks") {
    for (int goal : {4, 9}) {
        const auto report = paglets::verify::explore(RecoveryEvidenceModel{goal}, {.threads = 3});
        REQUIRE(report.complete);
        CHECK_EQ(report.states, std::size_t{5});
        CHECK_EQ(report.goals_declared.size(), std::size_t{1});
        CHECK_EQ(report.goals_checked.size(), std::size_t{1});
        CHECK_EQ(report.goals_checked.front(), std::string("terminal goal"));
        if (goal == 4) CHECK(report.ok());
        else {
            CHECK(!report.ok());
            REQUIRE(report.violations.size() == 1);
            CHECK_EQ(report.violations.front().kind, std::string("goal"));
        }
    }
}
