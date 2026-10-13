// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets::verify on the zombie protocol: small worlds explored
// exhaustively, and timed runs with faults (docs/zombie-protocol.md).

#include "test.hpp"

#include <paglets/protocols/zombie/model.hpp>
#include <paglets/protocols/zombie/sim.hpp>

namespace z = paglets::protocols::zombie;

PAGLETS_TEST("zombie: small worlds are explored completely without a violated property") {
    // Two replicas with each kind of fault; the documentation records the
    // larger worlds (paglets-protocol-check --suite full).
    for (int fault = 0; fault < 5; ++fault) {
        z::ModelConfig c;
        c.replicas = 2;
        c.crashes = fault == 0;
        c.cuts = fault == 1;
        c.drops = fault == 2;
        c.duplicates = fault == 3;
        c.early_timeouts = fault == 4;
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
