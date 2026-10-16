// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Compute jobs, for the runtime and mesh tests: job functions of every kind
// (quick, long, stubborn, failing, large output) and a paglet that submits
// them and answers with their outcome.

#include "jobs_msgs.hpp"

#include <paglets/compute.hpp>
#include <paglets/paglet.hpp>
#include <paglets/async.hpp>

#include <cstdint>
#include <memory>
#include <optional>

namespace compute = paglets::compute;

namespace {

std::int64_t now_ms() {
    return paglets::now_ns(paglets::abi::Clock::monotonic) / 1'000'000;
}

// The sum of the input bytes.
void sum(compute::JobContext& job) {
    auto input = job.input();
    if (!input) return job.fail(input.error());
    std::int64_t total = 0;
    for (auto b : *input) total += b;
    (void)job.output(total);
}
PAGLETS_JOB(sum)

// Probe real memory growth without touching the existing allocator's heap.
// Return the small initial size, growth outcome, final size and limit check.
std::vector<std::int64_t> memory_probe(std::int64_t target) {
    const auto before = __builtin_wasm_memory_size(0);
    if (target <= static_cast<std::int64_t>(before)) return {};
    const auto grown = __builtin_wasm_memory_grow(0, static_cast<std::size_t>(target) - before);
    const auto after = __builtin_wasm_memory_size(0);
    bool content = false;
    if (grown != static_cast<std::size_t>(-1)) {
        auto* first = reinterpret_cast<volatile std::uint8_t*>(before * 65536);
        auto* last = reinterpret_cast<volatile std::uint8_t*>(after * 65536 - 1);
        *first = 67;
        *last = 211;
        content = *first == 67 && *last == 211;
    }
    const auto beyond = __builtin_wasm_memory_grow(0, 1);
    return {static_cast<std::int64_t>(before), static_cast<std::int64_t>(grown),
            static_cast<std::int64_t>(after), beyond == static_cast<std::size_t>(-1), content};
}

void memory(compute::JobContext& job) {
    std::int64_t target = 0;
    if (!job.input(target)) return job.fail(paglets::abi::malformed);
    (void)job.output(memory_probe(target));
}
PAGLETS_JOB(memory)

// Runs for the given milliseconds, unless cancelled; reports progress.
void spin(compute::JobContext& job) {
    std::int64_t ms = 0;
    if (!job.input(ms)) return job.fail(paglets::abi::malformed);
    const std::int64_t until = now_ms() + ms;
    std::int64_t rounds = 0;
    while (now_ms() < until) {
        if (job.cancelled()) return job.fail(paglets::abi::cancelled);
        job.progress(ms - (until - now_ms()), ms);
        ++rounds;
    }
    (void)job.output(rounds);
}
PAGLETS_JOB(spin)

// Never looks at cancellation: only the engine can stop it.
void stubborn(compute::JobContext&) {
    volatile std::uint64_t x = 1;
    for (;;) x = x * 6364136223846793005ULL + 1442695040888963407ULL;
}
PAGLETS_JOB(stubborn)

void trap(compute::JobContext&) {
    __builtin_trap();
}
PAGLETS_JOB(trap)

// An output of the given size (bytes i % 251).
void big(compute::JobContext& job) {
    std::int64_t n = 0;
    if (!job.input(n)) return job.fail(paglets::abi::malformed);
    std::vector<std::uint8_t> chunk(64 * 1024);
    for (std::int64_t at = 0; at < n;) {
        const auto len = static_cast<std::size_t>(std::min<std::int64_t>(n - at, chunk.size()));
        for (std::size_t i = 0; i < len; ++i) chunk[i] = static_cast<std::uint8_t>((at + i) % 251);
        if (auto w = job.write(std::span(chunk.data(), len)); !w) return job.fail(w.error());
        at += len;
    }
}
PAGLETS_JOB(big)

// CPU work: `n` rounds of a mixing function (for interpreter vs AOT).
void mix(compute::JobContext& job) {
    std::int64_t n = 0;
    if (!job.input(n)) return job.fail(paglets::abi::malformed);
    std::uint64_t x = 0x9e3779b97f4a7c15ULL;
    for (std::int64_t i = 0; i < n; ++i) {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 29;
        x += static_cast<std::uint64_t>(i);
        if ((i & 0xffffff) == 0 && job.cancelled()) return job.fail(paglets::abi::cancelled);
    }
    (void)job.output(static_cast<std::int64_t>(x >> 1));
}
PAGLETS_JOB(mix)

}  // namespace

class Jobs : public paglets::Paglet {
public:
    Jobs() {
        router().on<std::int64_t>("memory-probe", [](std::int64_t target, paglets::Message& m) {
            (void)m.reply(memory_probe(target));
        });
        router().on<jobs_test::Run>("run", [this](const jobs_test::Run& r, paglets::Message& m) { submit(r, m, false); });
        // The input is the artifact the last job made.
        router().on<jobs_test::Run>("chain", [this](const jobs_test::Run& r, paglets::Message& m) { submit(r, m, true); });
        router().on<jobs_test::Run>("coro", [this](const jobs_test::Run& r, paglets::Message& m) {
            awaited(r, m.defer_reply());
        });
        router().on("cancel", [this](paglets::Message& m) {
            auto r = last_.cancel();
            (void)m.reply(static_cast<std::int32_t>(r ? 0 : r.error()));
        });
        router().on("status", [this](paglets::Message& m) {
            auto s = last_.status();
            if (!s) {
                (void)m.reply(paglets::abi::JobStatus{"error " + std::to_string(s.error())});
                return;
            }
            (void)m.reply(*s);
        });
        router().on("ping", [](paglets::Message& m) { (void)m.reply(std::string("pong")); });
        router().on("oversized", [](paglets::Message& m) {
            auto submitted = compute::submit_raw("sum", paglets::Bytes(paglets::abi::max_document_size), {}, {});
            (void)m.reply(static_cast<std::int32_t>(submitted ? 0 : submitted.error()));
        });
        router().on("last", [this](paglets::Message& m) { (void)m.reply(last_outcome_); });
        router().on("input_artifact", [this](paglets::Message& m) {
            std::int32_t handle = 0;
            if (!m.decode(handle)) return;
            if (artifact_) (void)artifact_->drop();
            artifact_ = paglets::Capability(handle);
            (void)m.reply(std::int32_t{0});
        });
        router().on("move", [](paglets::Message& m) {
            std::string to;
            if (!m.decode(to)) return;
            (void)m.reply(static_cast<std::int32_t>(paglets::dispatch(to) ? 0 : -1));
        });
        router().on("deactivate", [](paglets::Message& m) {
            (void)m.reply(std::int32_t{0});
            (void)paglets::deactivate();
        });
    }

    void on_created(paglets::Started&) override { (void)paglets::set_restart(true, true); }
    void on_recovered(const paglets::abi::Recovered&) override { recovered_ = true; }

private:
    static compute::Options options_of(const jobs_test::Run& r) {
        compute::Options o;
        o.cores = r.cores;
        o.max_ms = r.max_ms;
        o.hosts = r.hosts;
        o.finish_on_departure = r.finish;
        return o;
    }

    static jobs_test::Outcome outcome_of(const paglets::Result<compute::Output>& r, paglets::Message& reply) {
        jobs_test::Outcome o;
        if (!r) {
            o.status = r.error();
            auto p = reply.payload();
            o.error.assign(p.begin(), p.end());
            return o;
        }
        o.output = r->data;
        o.host = r->info.host;
        o.aot = r->info.aot;
        o.elapsed_ms = r->info.elapsed_ms;
        o.artifact = r->artifact.has_value();
        return o;
    }

    void submit(const jobs_test::Run& r, paglets::Message& m, bool chain) {
        compute::Options o = options_of(r);
        if (chain) {
            if (!artifact_) {
                (void)m.reply(jobs_test::Outcome{paglets::abi::not_found});
                return;
            }
            o.input_artifact = *artifact_;
        }
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        auto job = compute::submit_raw(
            r.function, r.input,
            [this, reply](paglets::Result<compute::Output> result, paglets::Message& msg) {
                jobs_test::Outcome out = outcome_of(result, msg);
                if (result && result->artifact) artifact_ = *result->artifact;
                out.recovered_before_reply = recovered_;
                last_outcome_ = out;
                (void)paglets::reply_to(*reply, out);
            },
            std::move(o));
        if (!job) {
            (void)paglets::reply_to(*reply, jobs_test::Outcome{job.error()});
            return;
        }
        last_ = *job;
    }

    paglets::Async awaited(jobs_test::Run r, paglets::Capability reply) {
        auto result = co_await compute::run_raw(r.function, r.input, options_of(r), &last_);
        jobs_test::Outcome out;
        if (!result) {
            out.status = result.error();
        } else {
            out.output = result->data;
            out.host = result->info.host;
            out.aot = result->info.aot;
            // A second step after the first: the paglet asks itself.
            auto pong = co_await paglets::ask_raw(paglets::self(), "ping");
            if (!pong.ok()) out.status = pong.status;
        }
        (void)paglets::reply_to(reply, out);
    }

    bool recovered_ = false;
    compute::Job last_;
    jobs_test::Outcome last_outcome_{-1};
    std::optional<paglets::Capability> artifact_;
};

PAGLETS_PAGLET(Jobs)
