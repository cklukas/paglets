// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Compute jobs (ABI v1.3, planning/cpp-compute-jobs.md), on one host and in
// a mesh: the jobs guest submits job functions of its own module and
// answers with their outcome.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"

#include <jobs_msgs.hpp>
#include <paglets/services/system_services.hpp>
#include <paglets/wire/reflect.hpp>

#include <filesystem>
#include <future>
#include <limits>

namespace {

using namespace paglets::test;
using jobs_test::Outcome;
using jobs_test::Run;

template <class T>
Bytes body(const T& v) {
    paglets::msgpack::Writer w;
    paglets::msgpack::write_value(w, v);
    return w.take();
}

template <class T>
T value_of(const Bytes& b) {
    T v{};
    paglets::msgpack::Reader r(b);
    if (!paglets::msgpack::read_value(r, v)) throw std::runtime_error("does not decode");
    return v;
}

Outcome outcome(const rt::Reply& r) {
    if (r.status != 0) throw std::runtime_error("reply status " + std::string(abi::error_name(r.status)));
    return value_of<Outcome>(r.payload);
}

Run job(std::string function, Bytes input = {}, std::int64_t max_ms = 0) {
    Run r;
    r.function = std::move(function);
    r.input = std::move(input);
    r.max_ms = max_ms;
    return r;
}

Outcome run(Fixture& f, const rt::PagletId& id, const Run& r, std::string_view how = "run") {
    return outcome(f.runtime->call(id, how, body(r), 60s));
}

std::string state_of(Fixture& f, const rt::PagletId& id) {
    auto r = f.call(id, "status");
    return r.status == 0 ? value_of<abi::JobStatus>(r.payload).state : std::string("?");
}

bool eventually(const std::function<bool()>& cond, std::chrono::milliseconds timeout = 10s) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < until) {
        if (cond()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return cond();
}

}  // namespace

PAGLETS_TEST("memory defaults: unknown requirements grow without the former paglet and job caps") {
    Fixture f;
    const auto id = f.create("jobs.wasm");
    auto check = [](const std::vector<std::int64_t>& probe, std::int64_t ceiling) {
        REQUIRE(probe.size() == 5u);
        CHECK(probe[0] > 0 && probe[0] < 256);  // initially below the former 16 MB paglet cap
        CHECK_EQ(probe[1], probe[0]);
        CHECK_EQ(probe[2], ceiling);
        CHECK_EQ(probe[3], 0);  // the old default ceiling no longer stops growth
        CHECK_EQ(probe[4], 1);  // first and last new bytes are writable
    };
    const auto paglet = f.call(id, "memory-probe", body(std::int64_t{4096}));
    REQUIRE(paglet.status == abi::ok);
    check(value_of<std::vector<std::int64_t>>(paglet.payload), 4096);
    const auto task = run(f, id, job("memory", body(std::int64_t{16384})));
    REQUIRE(task.status == abi::ok);
    check(value_of<std::vector<std::int64_t>>(task.output), 16384);
}

PAGLETS_TEST("resource declarations: explicit 2 GB jobs work and owner memory policy rejects before execution") {
    {
        Fixture f;
        const auto id = f.create("jobs.wasm");
        auto request = job("memory", body(std::int64_t{32768}));
        request.memory_mb = 2048;
        const auto result = run(f, id, request);
        REQUIRE(result.status == abi::ok);
        const auto probe = value_of<std::vector<std::int64_t>>(result.output);
        REQUIRE(probe.size() == 5u);
        CHECK_EQ(probe[2], 32768);
        CHECK_EQ(probe[3], 1);  // declared ceiling, not the old 1 GB framework maximum
        CHECK_EQ(probe[4], 1);
    }
    {
        rt::Config config;
        config.job_max_memory_mb = 64;
        Fixture f(config);
        const auto id = f.create("jobs.wasm");
        auto request = job("sum", {}, 1000);
        request.memory_mb = 128;
        CHECK_EQ(run(f, id, request).status, abi::quota);
        CHECK(f.runtime->jobs().empty());
        request.memory_mb = 32;
        CHECK_EQ(run(f, id, request).status, abi::ok);
    }
    {
        rt::Config config;
        config.require_job_resources = true;
        Fixture f(config);
        const auto id = f.create("jobs.wasm");
        CHECK_EQ(run(f, id, job("sum")).status, abi::denied);
        auto request = job("sum", {}, 1000);
        request.memory_mb = 16;
        CHECK_EQ(run(f, id, request).status, abi::ok);
    }
    {
        rt::Config config;
        config.limits.max_memory_pages = 512;  // operator allows 32 MB
        Fixture f(config);
        const auto module = f.module("memory_declared.wasm");
        auto denied = f.runtime->create(module);
        CHECK(!denied);
        CHECK(denied.error().find("declared memory") != std::string::npos);
    }
    {
        rt::Config config;
        config.require_paglet_memory = true;
        Fixture f(config);
        const auto id = f.create("memory_declared.wasm");
        CHECK(f.runtime->call(id, "greet", body(std::string("declared")), 2s).status == abi::ok);
    }
}

PAGLETS_TEST("resource declarations: unknown, unlimited and week-long job budgets have no hidden cap") {
    rt::Config config;
    config.job_slots = 1;
    Fixture f(config);
    const auto id = f.create("jobs.wasm");
    REQUIRE(f.runtime->reserve_compute_slots(1));
    for (const auto budget : {std::int64_t{0}, std::int64_t{-1}, std::int64_t{7LL * 24 * 60 * 60 * 1000}}) {
        auto pending = f.runtime->request(id, "run", body(job("sum", {}, budget)), 10s);
        REQUIRE(eventually([&] { return f.runtime->jobs_queued() == 1; }));
        const auto jobs = f.runtime->jobs();
        REQUIRE(jobs.size() == 1);
        CHECK_EQ(jobs[0].max_ms, budget == 0 ? -1 : budget);
        CHECK_EQ(jobs[0].memory_pages, 65536u);
        CHECK_EQ(value_of<std::int32_t>(f.call(id, "cancel").payload), 0);
        CHECK_EQ(outcome(pending.get()).status, abi::cancelled);
    }
    f.runtime->release_compute_slots(1);
    CHECK_EQ(run(f, id, job("sum", {}, -2)).status, abi::invalid_argument);
}

PAGLETS_TEST("jobs: a job runs a function of the module and answers with its output") {
    report("jobs");
    Fixture f;
    const auto id = f.create("jobs.wasm");
    const Outcome o = run(f, id, job("sum", Bytes{1, 2, 3, 250}));
    CHECK_EQ(o.status, 0);
    CHECK_EQ(value_of<std::int64_t>(o.output), 256);
    CHECK(!o.host.empty());
    // Unknown functions and traps fail.
    CHECK_EQ(run(f, id, job("nothing")).status, abi::not_found);
    const Outcome trapped = run(f, id, job("trap"));
    CHECK_EQ(trapped.status, abi::failed);
    CHECK(!trapped.error.empty());
}

PAGLETS_TEST("jobs: inline submission is bounded and extreme requested duration cannot overflow") {
    Fixture f;
    const auto id = f.create("jobs.wasm");
    CHECK_EQ(value_of<std::int32_t>(f.call(id, "oversized").payload), abi::too_large);
    CHECK_EQ(run(f, id, job("sum", {}, std::numeric_limits<std::int64_t>::max())).status, abi::ok);
}

PAGLETS_TEST("jobs: the reply deadline cancels queued work rather than leaving an orphan") {
    rt::Config c;
    c.job_slots = 1;
    c.job_max_time = 1ms;
    Fixture f(c);
    const auto id = f.create("jobs.wasm");
    REQUIRE(f.runtime->reserve_compute_slots(1));
    auto pending = f.runtime->request(id, "run", body(job("sum", {}, 1)), 90s);
    REQUIRE(eventually([&] { return f.runtime->jobs_queued() == 1u; }));
    // The reply deadline includes the configured running limit plus one
    // minute of grace. No kernel is running to trigger the pool's watchdog.
    REQUIRE(f.runtime->wait_jobs_idle(65s));
    CHECK_EQ(outcome(pending.get()).status, abi::timeout);
    f.runtime->release_compute_slots(1);
    CHECK(f.runtime->jobs().empty());
}

PAGLETS_TEST("jobs: the paglet keeps handling messages while its job runs") {
    Fixture f;
    const auto id = f.create("jobs.wasm");
    // Longer than a handler may take (5 s budget is far away; 1.5 s shows it).
    auto pending = f.runtime->request(id, "run", body(job("spin", body(std::int64_t{1500}))), 60s);
    REQUIRE(eventually([&] { return state_of(f, id) == "running"; }));
    const auto t0 = std::chrono::steady_clock::now();
    auto pong = f.call(id, "ping");
    CHECK_EQ(pong.status, 0);
    CHECK(std::chrono::steady_clock::now() - t0 < 500ms);
    const Outcome o = outcome(pending.get());
    CHECK_EQ(o.status, 0);
    CHECK(o.elapsed_ms >= 1400);
    CHECK(value_of<std::int64_t>(o.output) > 0);
}

PAGLETS_TEST("jobs: compute and service work share the Task control lifecycle") {
    Fixture f;
    const auto id = f.create("jobs.wasm");
    struct Status { std::string state; bool done = false; std::int32_t error = 0; std::int64_t completed = 0; };
    auto status = [&] {
        Status s;
        REQUIRE(paglets::wire::from_msgpack(f.call(id, "task_status").payload, s));
        return s;
    };
    auto pending = f.runtime->request(id, "run", body(job("spin", body(std::int64_t{60000}))), 60s);
    REQUIRE(eventually([&] { return status().state == "running"; }));
    CHECK_EQ(value_of<std::int32_t>(f.call(id, "task_cancel").payload), 0);
    CHECK_EQ(outcome(pending.get()).status, abi::cancelled);
    CHECK(status().done);
    CHECK(status().state == "cancelled");
    CHECK_EQ(status().error, abi::cancelled);
    CHECK_EQ(value_of<std::int32_t>(f.call(id, "task_cancel").payload), 0);
    CHECK_EQ(run(f, id, job("sum", Bytes{1, 2, 3})).status, 0);
    CHECK(status().state == "completed");
    CHECK(status().done);
    CHECK_EQ(run(f, id, job("trap")).status, abi::failed);
    CHECK(status().state == "failed");
}

PAGLETS_TEST("jobs: cancel, wall time and code that ignores cancellation") {
    Fixture f;
    const auto id = f.create("jobs.wasm");
    {
        auto pending = f.runtime->request(id, "run", body(job("spin", body(std::int64_t{60000}))), 60s);
        REQUIRE(eventually([&] { return state_of(f, id) == "running"; }));
        CHECK_EQ(value_of<std::int32_t>(f.call(id, "cancel").payload), 0);
        CHECK_EQ(outcome(pending.get()).status, abi::cancelled);
    }
    CHECK_EQ(run(f, id, job("spin", body(std::int64_t{60000}), 300)).status, abi::timeout);
    // The engine stops it (no check of `cancelled` in its loop).
    const auto t0 = std::chrono::steady_clock::now();
    CHECK_EQ(run(f, id, job("stubborn", {}, 300)).status, abi::timeout);
    CHECK(std::chrono::steady_clock::now() - t0 < 10s);
    {
        auto pending = f.runtime->request(id, "run", body(job("stubborn")), 60s);
        REQUIRE(eventually([&] { return state_of(f, id) == "running"; }));
        CHECK_EQ(value_of<std::int32_t>(f.call(id, "cancel").payload), 0);
        CHECK_EQ(outcome(pending.get()).status, abi::cancelled);
    }
    CHECK(f.runtime->wait_jobs_idle(10s));
    CHECK(f.runtime->jobs().empty());
}

PAGLETS_TEST("jobs: shutdown stops a job even when cancellation lands before its guest call") {
    for (int attempt = 0; attempt < 8; ++attempt) {
        Fixture f;
        const auto id = f.create("jobs.wasm");
        auto pending = f.runtime->request(id, "run", body(job("stubborn")), 60s);
        REQUIRE(eventually([&] { return !f.runtime->jobs().empty(); }));
        const auto began = std::chrono::steady_clock::now();
        f.runtime->shutdown();
        CHECK(std::chrono::steady_clock::now() - began < 5s);
        CHECK_EQ(pending.get().status, abi::gone);
    }
}

PAGLETS_TEST("jobs: slots admit jobs, the others queue in order") {
    Fixture f;
    f.runtime->set_job_slots(1);
    CHECK_EQ(f.runtime->job_slots(), 1u);
    const auto a = f.create("jobs.wasm");
    const auto b = f.create("jobs.wasm");
    auto first = f.runtime->request(a, "run", body(job("spin", body(std::int64_t{600}))), 60s);
    REQUIRE(eventually([&] { return f.runtime->job_cores_running() == 1; }));
    auto second = f.runtime->request(b, "run", body(job("spin", body(std::int64_t{100}))), 60s);
    REQUIRE(eventually([&] { return f.runtime->jobs_queued() == 1; }));
    CHECK_EQ(state_of(f, b), "queued");
    const auto jobs = f.runtime->jobs();
    CHECK_EQ(jobs.size(), 2u);
    CHECK_EQ(outcome(first.get()).status, 0);
    CHECK_EQ(outcome(second.get()).status, 0);
    // More cores than the host has: quota.
    Run wide = job("sum");
    wide.cores = 2;
    CHECK_EQ(run(f, a, wide).status, abi::quota);
}

PAGLETS_TEST("jobs: cancelling a queued large head admits the smaller job behind it") {
    Fixture f;
    f.runtime->set_job_slots(2);
    REQUIRE(f.runtime->reserve_compute_slots(1));
    const auto head = f.create("jobs.wasm");
    const auto tail = f.create("jobs.wasm");
    Run wide = job("sum");
    wide.cores = 2;
    auto first = f.runtime->request(head, "run", body(wide), 10s);
    REQUIRE(eventually([&] { return f.runtime->jobs_queued() == 1u; }));
    const auto id = f.runtime->jobs().front().id;
    auto last = f.runtime->request(tail, "run", body(job("sum")), 10s);
    REQUIRE(eventually([&] { return f.runtime->jobs_queued() == 2u; }));
    REQUIRE(f.runtime->cancel_job(id));
    REQUIRE(eventually([&] { return first.wait_for(0s) == std::future_status::ready &&
                                   last.wait_for(0s) == std::future_status::ready; }));
    CHECK_EQ(outcome(first.get()).status, abi::cancelled);
    CHECK_EQ(outcome(last.get()).status, abi::ok);
    f.runtime->release_compute_slots(1);
}

PAGLETS_TEST("jobs: reducing capacity ends impossible queued jobs with quota") {
    Fixture f;
    f.runtime->set_job_slots(2);
    REQUIRE(f.runtime->reserve_compute_slots(1));
    const auto head = f.create("jobs.wasm");
    const auto tail = f.create("jobs.wasm");
    Run wide = job("sum");
    wide.cores = 2;
    auto first = f.runtime->request(head, "run", body(wide), 10s);
    REQUIRE(eventually([&] { return f.runtime->jobs_queued() == 1u; }));
    auto last = f.runtime->request(tail, "run", body(job("sum")), 10s);
    REQUIRE(eventually([&] { return f.runtime->jobs_queued() == 2u; }));
    f.runtime->set_job_slots(1);
    REQUIRE(eventually([&] { return first.wait_for(0s) == std::future_status::ready; }));
    CHECK_EQ(outcome(first.get()).status, abi::quota);
    CHECK(last.wait_for(0s) != std::future_status::ready);
    f.runtime->release_compute_slots(1);
    REQUIRE(eventually([&] { return last.wait_for(0s) == std::future_status::ready; }));
    CHECK_EQ(outcome(last.get()).status, abi::ok);
}

PAGLETS_TEST("jobs: later work uses only surplus beyond an older external waiter's needs") {
    Fixture f;
    f.runtime->set_job_slots(3);
    const auto queued_at = std::chrono::steady_clock::now();
    f.runtime->set_compute_waiter(2, queued_at);
    const auto id = f.create("jobs.wasm");
    CHECK_EQ(run(f, id, job("sum")).status, abi::ok);
    REQUIRE(f.runtime->reserve_compute_slots_for(2, queued_at));
    f.runtime->set_compute_waiter(0);
    f.runtime->release_compute_slots(2);
}

PAGLETS_TEST("jobs: a deactivated paglet is activated by the result") {
    Fixture f;
    const auto id = f.create("jobs.wasm");
    auto pending = f.runtime->request(id, "run", body(job("spin", body(std::int64_t{500}))), 60s);
    REQUIRE(eventually([&] { return state_of(f, id) == "running"; }));
    REQUIRE(f.call(id, "deactivate").status == 0);
    REQUIRE(eventually([&] { return f.runtime->info(id)->state == rt::PagletState::inactive; }));
    CHECK_EQ(outcome(pending.get()).status, 0);
    CHECK_EQ(value_of<Outcome>(f.call(id, "last").payload).status, 0);
}

PAGLETS_TEST("jobs: co_await a job and a request in a coroutine") {
    Fixture f;
    const auto id = f.create("jobs.wasm");
    const Outcome o = run(f, id, job("sum", Bytes{5, 6}), "coro");
    CHECK_EQ(o.status, 0);
    CHECK_EQ(value_of<std::int64_t>(o.output), 11);
    CHECK_EQ(run(f, id, job("spin", body(std::int64_t{60000}), 200), "coro").status, abi::timeout);
}

PAGLETS_TEST("jobs: large outputs come as artifacts, which are inputs of other jobs") {
    Fixture f;
    auto services = paglets::services::install_system_services(*f.runtime, {});
    REQUIRE_OK(services);
    const auto id = f.create("jobs.wasm");
    constexpr std::int64_t size = 3 * 1024 * 1024;
    const Outcome o = run(f, id, job("big", body(size)));
    REQUIRE(o.status == 0);
    CHECK(o.artifact);
    CHECK(o.output.empty());
    const Outcome chained = run(f, id, job("sum"), "chain");
    REQUIRE(chained.status == 0);
    std::int64_t expected = 0;
    for (std::int64_t i = 0; i < size; ++i) expected += i % 251;
    CHECK_EQ(value_of<std::int64_t>(chained.output), expected);
}

PAGLETS_TEST("jobs: without artifacts, a large output fails") {
    Fixture f;
    const auto id = f.create("jobs.wasm");
    CHECK_EQ(run(f, id, job("big", body(std::int64_t{2 * 1024 * 1024}))).status, abi::too_large);
}

PAGLETS_TEST("jobs: after a restart, the paglet hears `gone` for its pending job") {
    std::random_device random;
    const auto dir = std::filesystem::temp_directory_path() / ("paglets-jobs-" + std::to_string(random()));
    std::filesystem::remove_all(dir);
    rt::PagletId id;
    {
        rt::Config c;
        c.state_dir = dir;
        Fixture f(c);
        id = f.create("jobs.wasm");
        auto pending = f.runtime->request(id, "run", body(job("spin", body(std::int64_t{60000}))), 60s);
        REQUIRE(eventually([&] { return state_of(f, id) == "running"; }));
        f.runtime->shutdown();
        CHECK_EQ(pending.get().status, abi::gone);
    }
    {
        rt::Config c;
        c.state_dir = dir;
        Fixture f(c);
        f.module("jobs.wasm");
        REQUIRE(eventually([&] {
            auto r = f.call(id, "last");
            return r.status == 0 && value_of<Outcome>(r.payload).status == abi::gone &&
                   value_of<Outcome>(r.payload).recovered_before_reply;
        }));
        CHECK(f.runtime->jobs().empty());
    }
    std::filesystem::remove_all(dir);
}

PAGLETS_TEST("jobs: a paglet that ends cancels its jobs") {
    Fixture f;
    const auto id = f.create("jobs.wasm");
    auto pending = f.runtime->request(id, "run", body(job("spin", body(std::int64_t{60000}))), 60s);
    REQUIRE(eventually([&] { return f.runtime->job_cores_running() == 1; }));
    REQUIRE(f.runtime->dispose(id));
    CHECK(f.runtime->wait_jobs_idle(10s));
    CHECK(f.runtime->jobs().empty());
}

#ifdef PAGLETS_TEST_WAMRC
PAGLETS_TEST("jobs: modules compiled ahead of time run faster, with the same results") {
    const std::filesystem::path compiler = PAGLETS_TEST_WAMRC;
    if (!std::filesystem::exists(compiler)) skip("no paglets-wamrc");
    rt::Config c;
    c.aot_compiler = compiler;
    const auto diagnostics = std::filesystem::temp_directory_path() /
        ("paglets-jobs-aot-" + std::to_string(std::random_device{}()));
    c.aot_cache = diagnostics;  // retain the compiled artifact if this test throws
    Fixture f(c);
    if (!f.runtime->job_aot()) skip("no AOT in this build");
    const auto id = f.create("jobs.wasm");
    const auto module = f.runtime->info(id)->module;
    // The first job runs in the interpreter while the module compiles.
    Run work = job("mix", body(std::int64_t{30'000'000}));
    const Outcome interpreted = run(f, id, work);
    REQUIRE(interpreted.status == 0);
    REQUIRE(f.runtime->compile_job_module(module, 300s));
    const Outcome compiled = run(f, id, work);
    if (compiled.status != 0) throw std::runtime_error("compiled job status " + std::to_string(compiled.status) + ": " +
        compiled.error + "; cache: " + diagnostics.string());
    REQUIRE(compiled.status == 0);
    CHECK(compiled.aot);
    CHECK(compiled.output == interpreted.output);
    // AOT instances also start small and grow only when the job asks.
    const auto memory = run(f, id, job("memory", body(std::int64_t{16384})));
    REQUIRE(memory.status == abi::ok);
    CHECK(memory.aot);
    const auto pages = value_of<std::vector<std::int64_t>>(memory.output);
    REQUIRE(pages.size() == 5u);
    CHECK(pages[0] > 0 && pages[0] < 256);
    CHECK_EQ(pages[1], pages[0]);
    CHECK_EQ(pages[2], 16384);
    CHECK_EQ(pages[3], 0);  // no implicit 1 GB ceiling in AOT either
    CHECK_EQ(pages[4], 1);
    report("jobs", "mix, 30 million rounds, interpreter", interpreted.elapsed_ms, "ms");
    report("jobs", "mix, 30 million rounds, ahead-of-time compiled", compiled.elapsed_ms, "ms");
    CHECK(compiled.elapsed_ms * 2 < interpreted.elapsed_ms);
    // Each submission creates and tears down a fresh instance. Exercise
    // repeated AOT placement with diagnostics for the intermittent rpi4 failure.
    for (int i = 0; i < 64; ++i) {
        const auto repeated = run(f, id, work);
        if (repeated.status != 0) throw std::runtime_error("compiled job repeat " + std::to_string(i) +
            " status " + std::to_string(repeated.status) + ": " + repeated.error + "; cache: " + diagnostics.string());
        CHECK(repeated.aot);
        CHECK(repeated.output == interpreted.output);
    }
    // Compiled code stops too: wall time and cancellation.
    CHECK_EQ(run(f, id, job("stubborn", {}, 300)).status, abi::timeout);
    auto pending = f.runtime->request(id, "run", body(job("stubborn")), 60s);
    REQUIRE(eventually([&] { return state_of(f, id) == "running"; }));
    CHECK_EQ(value_of<std::int32_t>(f.call(id, "cancel").payload), 0);
    CHECK_EQ(outcome(pending.get()).status, abi::cancelled);
    f.runtime->shutdown();
    std::filesystem::remove_all(diagnostics);
}
#endif

// -- remote jobs ----------------------------------------------------------------------------

namespace {

void fast(Mesh& net) {
    for (auto& h : net.hosts) {
        h->node->set_compute_timing({.sample = 50ms, .gossip = 50ms, .ttl = 2000ms, .redirect_after = 0ms});
    }
}

std::future<rt::Reply> submit(Mesh::Host& h, const rt::PagletId& id, const Run& r, std::string_view how = "run") {
    return h.f->runtime->request(id, how, body(r), 120s);
}

Outcome wait(Mesh& net, std::future<rt::Reply>& pending, int rounds = 10000) {
    REQUIRE(net.settle([&] { return pending.wait_for(0s) == std::future_status::ready; }, rounds));
    return outcome(pending.get());
}

Outcome run_in(Mesh& net, Mesh::Host& h, const rt::PagletId& id, const Run& r, std::string_view how = "run") {
    auto pending = submit(h, id, r, how);
    return wait(net, pending);
}

Run on(Run r, std::vector<std::string> hosts) {
    r.hosts = std::move(hosts);
    return r;
}

struct JobMesh {
    Mesh net;
    Mesh::Host* a = nullptr;
    Mesh::Host* b = nullptr;
    Mesh::Host* c = nullptr;
    rt::PagletId paglet;

    JobMesh() {
        a = &net.add_host("a");
        b = &net.add_host("b");
        c = &net.add_host("c");
        net.enroll_all();
        fast(net);
        for (auto& h : net.hosts) h->node->set_compute_slots(2);
        REQUIRE(net.settle([&] { return net.converged() && a->node->landscape().size() == 3u; }, 2000));
        const std::string module = a->f->module("jobs.wasm");
        auto id = a->node->create(module, net.passport(module, paglet_id('1')));
        REQUIRE_OK(id);
        paglet = *id;
    }
};

}  // namespace

PAGLETS_TEST("jobs: a successful move cancels work or forwards its completed result") {
    for (bool finish : {false, true}) {
        JobMesh m;
        Run work = job("spin", body(std::int64_t{1500}));
        work.finish = finish;
        auto pending = submit(*m.a, m.paglet, work);
        REQUIRE(m.net.settle([&] { return !m.a->f->runtime->jobs().empty(); }, 2000));
        REQUIRE_OK(m.a->node->dispatch(m.paglet, "b"));
        REQUIRE(m.net.settle([&] { return m.b->f->runtime->info(m.paglet).has_value(); }, 2000));
        const Outcome result = wait(m.net, pending);
        CHECK_EQ(result.status, finish ? abi::ok : abi::cancelled);
        CHECK(m.a->f->runtime->wait_jobs_idle(10s));
    }
}

PAGLETS_TEST("jobs: remote jobs run on a named host, or where the mesh has free slots") {
    report("jobs");
    JobMesh m;
    const Outcome on_b = run_in(m.net, *m.a, m.paglet, on(job("sum", Bytes{7, 8}), {"b"}));
    REQUIRE(on_b.status == 0);
    CHECK_EQ(value_of<std::int64_t>(on_b.output), 15);
    CHECK_EQ(on_b.host, key_id(m.b->key));
    const Outcome on_c = run_in(m.net, *m.a, m.paglet, on(job("sum", Bytes{1}), {key_id(m.c->key)}));
    CHECK_EQ(on_c.host, key_id(m.c->key));
    // a's slots are busy: "any" picks another host.
    auto busy1 = submit(*m.a, m.paglet, job("spin", body(std::int64_t{3000})));
    REQUIRE(m.net.settle([&] { return m.a->f->runtime->job_cores_running() == 1; }, 2000));
    const auto other = m.a->node->create(m.a->f->module("jobs.wasm"), m.net.passport(m.a->f->module("jobs.wasm"), paglet_id('2')));
    REQUIRE_OK(other);
    auto busy2 = submit(*m.a, *other, job("spin", body(std::int64_t{3000})));
    REQUIRE(m.net.settle([&] { return m.a->f->runtime->job_cores_running() == 2; }, 2000));
    const auto third = m.a->node->create(m.a->f->module("jobs.wasm"), m.net.passport(m.a->f->module("jobs.wasm"), paglet_id('3')));
    REQUIRE_OK(third);
    const Outcome any = run_in(m.net, *m.a, *third, on(job("sum", Bytes{2}), {"any"}));
    REQUIRE(any.status == 0);
    CHECK(any.host == key_id(m.b->key) || any.host == key_id(m.c->key));
    CHECK_EQ(wait(m.net, busy1).status, 0);
    CHECK_EQ(wait(m.net, busy2).status, 0);
    // A host that is not in the mesh: nowhere to run.
    CHECK_EQ(run_in(m.net, *m.a, m.paglet, on(job("sum"), {"nowhere"})).status, abi::not_found);
}

PAGLETS_TEST("jobs: ambiguous names and unregistered keys cannot choose an execution host") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    b.name = c.name = "same-name";
    const auto absent = SigningKey::generate();
    a.name = key_id(absent.public_key());
    net.enroll_all();
    fast(net);
    for (auto& h : net.hosts) h->node->set_compute_slots(2);
    REQUIRE(net.settle([&] { return net.converged() && a.node->landscape().size() == 3u; }, 2000));
    const auto module = a.f->module("jobs.wasm");
    const auto id = a.node->create(module, net.passport(module, paglet_id('a')));
    REQUIRE_OK(id);
    for (const auto& address : {b.name, a.name}) {
        CHECK_EQ(run_in(net, a, *id, on(job("sum", Bytes{7, 8}), {address})).status, abi::not_found);
    }
    // An explicit list can still contain another valid destination.
    const auto result = run_in(net, a, *id, on(job("sum", Bytes{7, 8}), {b.name, a.name, key_id(c.key)}));
    CHECK_EQ(result.status, abi::ok);
    CHECK_EQ(result.host, key_id(c.key));
    CHECK_EQ(value_of<std::int64_t>(result.output), 15);
}

PAGLETS_TEST("jobs: losing the execution host reports gone") {
    JobMesh m;
    auto pending = submit(*m.a, m.paglet, on(job("spin", body(std::int64_t{60000})), {"b"}));
    REQUIRE(m.net.settle([&] { return !m.b->f->runtime->jobs().empty(); }, 2000));
    m.b->down = true;
    m.b->f->runtime->shutdown();
    CHECK_EQ(wait(m.net, pending).status, abi::gone);
}

PAGLETS_TEST("jobs: a remote job reports progress and can be cancelled") {
    JobMesh m;
    auto pending = submit(*m.a, m.paglet, on(job("spin", body(std::int64_t{60000})), {"b"}));
    REQUIRE(m.net.settle([&] { return !m.b->f->runtime->jobs().empty(); }, 3000));
    // Its status comes from b.
    std::optional<abi::JobStatus> st;
    REQUIRE(m.net.settle(
        [&] {
            auto r = m.a->f->runtime->request(m.paglet, "status", {}, 10s);
            m.net.deliver();
            if (r.wait_for(2s) != std::future_status::ready) return false;
            auto reply = r.get();
            if (reply.status != 0) return false;
            st = value_of<abi::JobStatus>(reply.payload);
            return st->state == "running" && st->total > 0;
        },
        3000));
    CHECK_EQ(st->host, key_id(m.b->key));
    auto cancel = m.a->f->runtime->request(m.paglet, "cancel", {}, 10s);
    const Outcome o = wait(m.net, pending);
    CHECK_EQ(o.status, abi::cancelled);
    REQUIRE(m.net.settle([&] { return m.b->f->runtime->jobs().empty(); }, 2000));
    (void)cancel.get();
}

PAGLETS_TEST("jobs: the mesh policy caps a job's wall time or refuses jobs") {
    JobMesh m;
    m.net.admin_record("policy-rule", data::policy_rule({"short jobs", Decision::allow, "compute-jobs", {"submit"}, {},
                                                         std::nullopt, std::int64_t{300}, 0}));
    REQUIRE(m.net.settle([&] { return m.net.converged(); }, 2000));
    m.net.settle([] { return false; }, 20);
    CHECK_EQ(run_in(m.net, *m.a, m.paglet, job("spin", body(std::int64_t{60000}))).status, abi::timeout);
    CHECK_EQ(run_in(m.net, *m.a, m.paglet, on(job("spin", body(std::int64_t{60000})), {"b"})).status, abi::timeout);
    CHECK_EQ(run_in(m.net, *m.a, m.paglet, job("spin", body(std::int64_t{60000}), -1)).status, abi::timeout);
    CHECK_EQ(run_in(m.net, *m.a, m.paglet, on(job("spin", body(std::int64_t{60000}), -1), {"b"})).status, abi::timeout);
    m.net.admin_record("policy-rule", data::policy_rule({"no jobs", Decision::deny, "compute-jobs", {"submit"}, {},
                                                         std::nullopt, std::nullopt, 10}));
    REQUIRE(m.net.settle([&] { return m.net.converged(); }, 2000));
    m.net.settle([] { return false; }, 20);
    CHECK_EQ(run_in(m.net, *m.a, m.paglet, job("sum")).status, abi::denied);
}

PAGLETS_TEST("jobs: approvals match owner, module and trust on the execution host") {
    for (int mismatch = 0; mismatch != 3; ++mismatch) {
        JobMesh m;
        m.net.admin_record("policy-rule", data::policy_rule({"ask for jobs", Decision::ask,
            "compute-jobs", {"submit"}, {}, {}, {}, 0}));
        const Principal actual{m.paglet, m.net.owner.id(), m.a->f->module("jobs.wasm"), "roaming"};
        auto wrong = actual;
        if (mismatch == 0) wrong.owner = SigningKey::generate().id();
        if (mismatch == 1) wrong.module = std::string(64, 'f');
        if (mismatch == 2) wrong.trust = "resident";
        const Item item{"compute-jobs", {"submit"}, {}, {}};
        auto approve = [&](const Principal& principal, HostSelector hosts) {
            m.net.admin_record("grant", data::grant(principal, item, hosts, unix_ms() + 60'000, {}, {}, {}));
            REQUIRE(m.net.settle([&] { return m.net.converged(); }, 2000));
            m.net.settle([] { return false; }, 20);
        };
        approve(wrong, {});
        CHECK_EQ(run_in(m.net, *m.a, m.paglet, job("sum")).status, abi::denied);
        approve(actual, HostSelector{{m.a->key}, {}});
        CHECK_EQ(run_in(m.net, *m.a, m.paglet, job("sum")).status, abi::ok);
        CHECK_EQ(run_in(m.net, *m.a, m.paglet, on(job("sum"), {"b"})).status, abi::denied);
        approve(actual, HostSelector{{m.b->key}, {}});
        CHECK_EQ(run_in(m.net, *m.a, m.paglet, on(job("sum"), {"b"})).status, abi::ok);
    }
}

PAGLETS_TEST("jobs: result provenance follows a paglet that moves during remote work") {
    JobMesh m;
    m.net.admin_record("root-residency", data::root_residency("restricted", "hosts",
                                      HostSelector{{m.a->key, m.b->key, m.c->key}, {}}));
    REQUIRE(m.net.settle([&] { return m.net.converged(); }, 2000));
    auto cap = m.a->services->store_artifact(body(std::int64_t{1500}), "application/octet-stream", {"root:restricted"});
    REQUIRE_OK(cap);
    auto handle = m.a->f->runtime->add_capability(m.paglet, *cap);
    REQUIRE_OK(handle);
    CHECK_EQ(value_of<std::int32_t>(m.a->f->call(m.paglet, "input_artifact", body(*handle)).payload), 0);
    Run work = on(job("spin"), {"b"});
    work.finish = true;
    auto pending = submit(*m.a, m.paglet, work, "chain");
    REQUIRE(m.net.settle([&] { return !m.b->f->runtime->jobs().empty(); }, 2000));
    REQUIRE_OK(m.a->node->dispatch(m.paglet, "c"));
    REQUIRE(m.net.settle([&] { return m.c->f->runtime->info(m.paglet).has_value(); }, 2000));
    REQUIRE(wait(m.net, pending).status == 0);
    CHECK(m.c->f->runtime->marks(m.paglet) == std::vector<std::string>{"root:restricted"});
}

PAGLETS_TEST("jobs: artifact input residency survives remote computation and both result forms") {
    for (bool large : {false, true}) {
        JobMesh m;
        m.net.admin_record("root-residency", data::root_residency("restricted", "hosts",
                                          HostSelector{{m.a->key, m.b->key, m.c->key}, {}}));
        REQUIRE(m.net.settle([&] { return m.net.converged(); }, 2000));
        constexpr std::int64_t size = 3 * 1024 * 1024;
        const Bytes input = large ? body(size) : Bytes{10, 20};
        auto cap = m.a->services->store_artifact(input, "application/octet-stream", {"root:restricted"});
        REQUIRE_OK(cap);
        auto handle = m.a->f->runtime->add_capability(m.paglet, *cap);
        REQUIRE_OK(handle);
        CHECK(m.a->f->runtime->marks(m.paglet).empty());
        CHECK_EQ(value_of<std::int32_t>(m.a->f->call(m.paglet, "input_artifact", body(*handle)).payload), 0);
        const Outcome result = run_in(m.net, *m.a, m.paglet, on(job(large ? "big" : "sum"), {"b"}), "chain");
        REQUIRE(result.status == 0);
        CHECK(result.artifact == large);
        CHECK(m.a->f->runtime->marks(m.paglet) == std::vector<std::string>{"root:restricted"});
        if (large) {
            Bytes expected(static_cast<std::size_t>(size));
            for (std::size_t i = 0; i < expected.size(); ++i) expected[i] = static_cast<std::uint8_t>(i % 251);
            const auto hash = paglets::to_hex(paglets::sha256(expected));
            const auto stored = m.b->services->served_content(hash);
            REQUIRE(stored.has_value());
            CHECK(stored->marks == std::vector<std::string>{"root:restricted"});
        } else {
            CHECK_EQ(value_of<std::int64_t>(result.output), 30);
        }
    }
}

PAGLETS_TEST("jobs: a large output on one host is the input of a job on another") {
    JobMesh m;
    constexpr std::int64_t size = 3 * 1024 * 1024;
    const Outcome o = run_in(m.net, *m.a, m.paglet, on(job("big", body(size)), {"b"}));
    REQUIRE(o.status == 0);
    CHECK(o.artifact);
    CHECK_EQ(o.host, key_id(m.b->key));
    const Outcome chained = run_in(m.net, *m.a, m.paglet, on(job("sum"), {"c"}), "chain");
    REQUIRE(chained.status == 0);
    CHECK_EQ(chained.host, key_id(m.c->key));
    std::int64_t expected = 0;
    for (std::int64_t i = 0; i < size; ++i) expected += i % 251;
    CHECK_EQ(value_of<std::int64_t>(chained.output), expected);
}
