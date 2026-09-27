// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-spike: milestone M0 feasibility tool.
//
//   paglets-spike info <module.wasm>
//   paglets-spike image-save <counter.wasm> <image> [--increments N] [--bloat KB]
//   paglets-spike image-resume <counter.wasm> <image> [--increments N] [--expect VALUE]
//   paglets-spike bench <testbed.wasm> <counter.wasm> [--instances N]
//   paglets-spike runtime <counter.wasm> <ping_pong.wasm> [--threads N]
//   paglets-spike call-bench <module.wasm> <message> [--count N]
//
// image-save and image-resume run in different processes (and, for the
// cross-architecture check, on different hosts): a counter paglet is driven,
// captured as a memory image, and resumed elsewhere with its state intact.
// The counter protocol is written by hand here so the tool also builds without
// C++26 reflection.

#include "process_stats.hpp"

#include <paglets/abi.hpp>
#include <paglets/msgpack.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/wasm/direct.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wasm/snapshot.hpp>

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace pw = paglets::wasm;
using Clock = std::chrono::steady_clock;

namespace {

struct Args {
    std::vector<std::string> positional;
    std::map<std::string, std::string> options;

    long long number(const std::string& key, long long fallback) const {
        auto it = options.find(key);
        return it == options.end() ? fallback : std::atoll(it->second.c_str());
    }
    bool has(const std::string& key) const { return options.contains(key); }
};

Args parse_args(int argc, char** argv) {
    Args a;
    for (int i = 2; i < argc; ++i) {
        std::string s = argv[i];
        if (s.starts_with("--") && i + 1 < argc) {
            a.options[s.substr(2)] = argv[++i];
        } else {
            a.positional.push_back(s);
        }
    }
    return a;
}

int fail(const std::string& message) {
    std::cerr << "paglets-spike: " << message << "\n";
    return 1;
}

double ms_since(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

std::shared_ptr<pw::Module> load_module(const std::string& path) {
    auto bytes = pw::read_file(path);
    if (!bytes) throw std::runtime_error(bytes.error());
    auto module = pw::Module::load(std::move(*bytes), pw::ImportPolicy::standard());
    if (!module) throw std::runtime_error(module.error());
    return *module;
}

// --- counter protocol (ABI v1 requests; bodies are MessagePack maps) -------

struct Request {
    std::string name;
    std::vector<std::uint8_t> payload;
};

Request increment_request(std::int64_t by, const std::string& note) {
    paglets::msgpack::Writer w;
    w.write_map_header(3);
    w.write_str("by");
    w.write_int(by);
    w.write_str("note");
    w.write_str(note);
    w.write_str("mode");
    w.write_str("add");
    return {"increment", w.take()};
}

Request simple_request(const std::string& name) {
    return {name, {}};
}

Request bloat_request(std::uint32_t kilobytes) {
    paglets::msgpack::Writer w;
    w.write_map_header(2);
    w.write_str("kilobytes");
    w.write_uint(kilobytes);
    w.write_str("fill");
    w.write_uint(0x5a);
    return {"bloat", w.take()};
}

struct CounterStatus {
    std::int64_t value = 0;
    std::uint32_t history_size = 0;
    std::uint32_t memory_pages = 0;
};

CounterStatus parse_status(const pw::DirectHarness::Reply& reply) {
    if (reply.status != 0) {
        throw std::runtime_error("paglet error: " + std::string(paglets::abi::error_name(reply.status)));
    }
    paglets::msgpack::Reader r(reply.payload);
    CounterStatus s;
    std::uint32_t n = 0;
    if (!r.read_map_header(n)) throw std::runtime_error("malformed status");
    for (std::uint32_t i = 0; i < n; ++i) {
        std::string_view key;
        r.read_str(key);
        std::uint64_t u = 0;
        if (key == "value") {
            r.read_i64(s.value);
        } else if (key == "history_size") {
            r.read_u64(u);
            s.history_size = static_cast<std::uint32_t>(u);
        } else if (key == "memory_pages") {
            r.read_u64(u);
            s.memory_pages = static_cast<std::uint32_t>(u);
        } else {
            r.skip();
        }
    }
    if (!r.ok()) throw std::runtime_error("malformed status");
    return s;
}

// A counter instance driven through the paglet ABI.
struct Driven {
    std::unique_ptr<pw::Instance> instance;
    std::unique_ptr<pw::DirectHarness> harness;
};

Driven drive(std::unique_ptr<pw::Instance> instance, bool start) {
    Driven d{std::move(instance), nullptr};
    d.harness = std::make_unique<pw::DirectHarness>(*d.instance);
    if (start) {
        if (auto r = d.harness->start(); !r) throw std::runtime_error(r.error());
    }
    return d;
}

CounterStatus send(Driven& d, const Request& request) {
    auto reply = d.harness->call(request.name, request.payload);
    if (!reply) throw std::runtime_error(reply.error());
    return parse_status(*reply);
}

void print_image_stats(const pw::Snapshot& snap, std::size_t file_size) {
    std::uint32_t zero = 0;
    for (const auto& p : snap.pages) zero += p.zero ? 1 : 0;
    std::cout << "  image: " << snap.page_count << " pages (" << snap.page_count * 64 << " KB memory), " << zero
              << " zero pages elided, " << snap.globals.size() << " globals, file " << file_size / 1024 << " KB\n";
}

// --- commands ---------------------------------------------------------------

int cmd_info(const Args& a) {
    if (a.positional.size() != 1) return fail("usage: info <module.wasm>");
    auto bytes = pw::read_file(a.positional[0]);
    if (!bytes) return fail(bytes.error());
    auto info = pw::parse_module(*bytes);
    if (!info) return fail(info.error());
    std::cout << "module " << a.positional[0] << "\n  sha256: " << paglets::to_hex(paglets::sha256(*bytes))
              << "\n  size:   " << bytes->size() << " bytes\n  imports:\n";
    for (const auto& i : info->imports) {
        std::cout << "    " << i.module << "." << i.name << " (" << pw::to_string(i.kind) << ")\n";
    }
    std::cout << "  exports:\n";
    for (const auto& e : info->exports) {
        std::cout << "    " << e.name << " (" << pw::to_string(e.kind) << ")\n";
    }
    std::cout << "  globals:\n";
    for (const auto& g : info->globals) {
        std::cout << "    #" << g.index << " " << pw::to_string(g.type) << (g.is_mutable ? " mut" : "")
                  << (g.export_name ? " export=" + *g.export_name : "") << "\n";
    }
    if (info->memory) {
        std::cout << "  memory: min " << info->memory->min_pages << " pages"
                  << (info->memory->max_pages ? ", max " + std::to_string(*info->memory->max_pages) : "") << "\n";
    }
    const auto policy = pw::ImportPolicy::standard().check(*info);
    std::cout << "  standard import policy: " << (policy ? "ok" : policy.error()) << "\n";
    const auto ready = pw::check_snapshot_ready(*info);
    std::cout << "  memory-image ready:     " << (ready ? "ok" : ready.error()) << "\n";
    return 0;
}

int cmd_image_save(const Args& a) {
    if (a.positional.size() != 2) return fail("usage: image-save <counter.wasm> <image> [--increments N] [--bloat KB]");
    auto module = load_module(a.positional[0]);
    auto inst = pw::Instance::create(module);
    if (!inst) return fail(inst.error());
    Driven ctr = drive(std::move(*inst), true);

    const auto increments = a.number("increments", 5);
    CounterStatus s;
    for (long long i = 1; i <= increments; ++i) {
        s = send(ctr, increment_request(i, "step " + std::to_string(i)));
    }
    if (a.has("bloat")) {
        s = send(ctr, bloat_request(static_cast<std::uint32_t>(a.number("bloat", 0))));
    }

    const auto t0 = Clock::now();
    auto snap = pw::capture(*ctr.instance);
    if (!snap) return fail(snap.error());
    const double capture_ms = ms_since(t0);
    const auto bytes = pw::serialize(*snap);
    if (auto w = pw::write_file(a.positional[1], bytes); !w) return fail(w.error());

    std::cout << "saved counter: value " << s.value << ", history " << s.history_size << "\n";
    print_image_stats(*snap, bytes.size());
    std::cout << "  capture: " << std::fixed << std::setprecision(3) << capture_ms << " ms\n";
    return 0;
}

int cmd_image_resume(const Args& a) {
    if (a.positional.size() != 2) {
        return fail("usage: image-resume <counter.wasm> <image> [--increments N] [--expect VALUE]");
    }
    auto module = load_module(a.positional[0]);
    auto bytes = pw::read_file(a.positional[1]);
    if (!bytes) return fail(bytes.error());
    auto snap = pw::deserialize(*bytes);
    if (!snap) return fail(snap.error());

    const auto t0 = Clock::now();
    auto inst = pw::restore(module, *snap);
    if (!inst) return fail(inst.error());
    const double restore_ms = ms_since(t0);

    Driven ctr = drive(std::move(*inst), false);
    CounterStatus s = send(ctr, simple_request("status"));
    std::cout << "resumed counter: value " << s.value << ", history " << s.history_size << "\n";
    const auto increments = a.number("increments", 0);
    for (long long i = 1; i <= increments; ++i) {
        s = send(ctr, increment_request(i, "after resume " + std::to_string(i)));
    }
    if (increments > 0) {
        std::cout << "after " << increments << " more increments: value " << s.value << ", history " << s.history_size
                  << "\n";
    }
    print_image_stats(*snap, bytes->size());
    std::cout << "  restore: " << std::fixed << std::setprecision(3) << restore_ms << " ms\n";

    if (a.has("expect") && s.value != a.number("expect", 0)) {
        return fail("expected value " + a.options.at("expect") + ", got " + std::to_string(s.value));
    }
    return 0;
}

int cmd_bench(const Args& a) {
    if (a.positional.size() != 2) return fail("usage: bench <testbed.wasm> <counter.wasm> [--instances N]");
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "runtime: " << PAGLETS_WAMR_TAG << " (" << PAGLETS_WAMR_MODE << ")\n";

    auto t0 = Clock::now();
    auto testbed = load_module(a.positional[0]);
    auto counter = load_module(a.positional[1]);
    std::cout << "module load (testbed " << testbed->size() / 1024 << " KB, counter " << counter->size() / 1024
              << " KB): " << ms_since(t0) << " ms\n";

    // Instantiation latency (counter, including its C++ initializer).
    constexpr int rounds = 200;
    t0 = Clock::now();
    for (int i = 0; i < rounds; ++i) {
        auto inst = pw::Instance::create(counter);
        if (!inst) return fail(inst.error());
    }
    std::cout << "instantiate + _initialize + destroy (counter): " << ms_since(t0) * 1000.0 / rounds << " us\n";

    // Call overhead.
    auto tb = pw::Instance::create(testbed);
    if (!tb) return fail(tb.error());
    constexpr int calls = 1'000'000;
    t0 = Clock::now();
    for (int i = 0; i < calls; ++i) {
        auto r = (*tb)->call_i32("add", {1, 2});
        if (!r) return fail(r.error());
    }
    std::cout << "host->guest call (add): " << ms_since(t0) * 1e6 / calls << " ns\n";

    // Message round trip through the paglet ABI (request, reply).
    auto ctr_inst = pw::Instance::create(counter);
    if (!ctr_inst) return fail(ctr_inst.error());
    Driven ctr = drive(std::move(*ctr_inst), true);
    constexpr int messages = 100'000;
    t0 = Clock::now();
    for (int i = 0; i < messages; ++i) {
        auto r = ctr.harness->call("status");
        if (!r) return fail(r.error());
    }
    std::cout << "message round trip (status, MessagePack): " << ms_since(t0) * 1e3 / messages << " us\n";

    // Memory image capture and restore.
    for (int i = 1; i <= 100; ++i) send(ctr, increment_request(i, "bench"));
    t0 = Clock::now();
    auto snap = pw::capture(*ctr.instance);
    if (!snap) return fail(snap.error());
    const double cap = ms_since(t0);
    t0 = Clock::now();
    auto restored = pw::restore(counter, *snap);
    if (!restored) return fail(restored.error());
    const double res = ms_since(t0);
    std::cout << "memory image: " << snap->page_count << " pages, " << snap->data_pages() << " non-zero; capture "
              << cap << " ms, restore " << res << " ms\n";

    // Terminating a running endless loop from another thread.
    auto spinner = pw::Instance::create(testbed);
    if (!spinner) return fail(spinner.error());
    Clock::time_point requested;
    std::thread killer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        requested = Clock::now();
        (*spinner)->terminate();
    });
    auto spin = (*spinner)->call_void("spin");
    const double stop_ms = ms_since(requested);
    killer.join();
    std::cout << "terminate endless loop: " << (spin ? "NOT stopped" : "stopped (" + spin.error() + ")") << " after "
              << stop_ms << " ms\n";

    // Memory per instance.
    const auto count = static_cast<int>(a.number("instances", 1000));
    std::vector<Driven> many;
    many.reserve(static_cast<std::size_t>(count));
    const auto rss0 = paglets::resident_bytes();
    t0 = Clock::now();
    for (int i = 0; i < count; ++i) {
        auto inst = pw::Instance::create(counter);
        if (!inst) return fail("instance " + std::to_string(i) + ": " + inst.error());
        Driven d = drive(std::move(*inst), true);
        send(d, increment_request(1, "x"));
        many.push_back(std::move(d));
    }
    const double create_ms = ms_since(t0);
    const auto rss1 = paglets::resident_bytes();
    std::cout << count << " live counter instances: " << create_ms << " ms, resident +" << (rss1 - rss0) / 1024 / 1024
              << " MB (" << (rss1 - rss0) / 1024 / count << " KB per instance)\n";
    return 0;
}

// Measurements of the M1 runtime: scheduling, request/reply between the
// host and paglets and between paglets, lifecycle operations.
int cmd_runtime(const Args& a) {
    namespace rt = paglets::runtime;
    if (a.positional.size() != 2) return fail("usage: runtime <counter.wasm> <ping_pong.wasm> [--threads N]");
    std::cout << std::fixed << std::setprecision(2);
    rt::Config config;
    config.threads = static_cast<unsigned>(a.number("threads", 4));
    rt::Runtime runtime(config);
    auto counter = runtime.add_module_file(a.positional[0]);
    auto ping_pong = runtime.add_module_file(a.positional[1]);
    if (!counter) return fail(counter.error());
    if (!ping_pong) return fail(ping_pong.error());
    std::cout << "runtime: " << config.threads << " scheduler threads, " << PAGLETS_WAMR_MODE << "\n";

    const auto status = simple_request("status");
    auto t0 = Clock::now();
    constexpr int creations = 200;
    std::vector<rt::PagletId> ids;
    for (int i = 0; i < creations; ++i) {
        auto id = runtime.create(*counter);
        if (!id) return fail(id.error());
        ids.push_back(*id);
        if (runtime.call(*id, status.name).status != 0) return fail("status failed");
    }
    std::cout << "create + first request (counter): " << ms_since(t0) * 1000.0 / creations << " us\n";

    constexpr int sequential = 20'000;
    t0 = Clock::now();
    for (int i = 0; i < sequential; ++i) {
        if (runtime.call(ids[0], status.name).status != 0) return fail("status failed");
    }
    std::cout << "host request/reply, sequential: " << ms_since(t0) * 1000.0 / sequential << " us\n";

    constexpr int per_paglet = 100;
    std::vector<std::future<rt::Reply>> pending;
    pending.reserve(ids.size() * per_paglet);
    t0 = Clock::now();
    for (int round = 0; round < per_paglet; ++round) {
        for (const auto& id : ids) pending.push_back(runtime.request(id, status.name));
    }
    for (auto& f : pending) {
        if (f.get().status != 0) return fail("status failed");
    }
    const double parallel_ms = ms_since(t0);
    std::cout << "host requests over " << ids.size() << " paglets: " << pending.size() * 1000.0 / parallel_ms
              << " requests/s\n";

    auto pp = runtime.create(*ping_pong);
    if (!pp) return fail(pp.error());
    constexpr std::uint32_t rounds = 20'000;
    paglets::msgpack::Writer w;
    w.write_uint(rounds);
    t0 = Clock::now();
    auto reply = runtime.call(*pp, "run", w.take(), std::chrono::minutes(5));
    const double pp_ms = ms_since(t0);
    if (reply.status != 0) return fail("ping-pong failed");
    std::cout << "paglet-to-paglet request/reply (ping-pong): " << pp_ms * 1000.0 / rounds << " us per round trip\n";

    constexpr int cycles = 200;
    t0 = Clock::now();
    for (int i = 0; i < cycles; ++i) {
        if (!runtime.deactivate(ids[1])) return fail("deactivate failed");
        if (runtime.call(ids[1], status.name).status != 0) return fail("status failed");
    }
    std::cout << "deactivate + activate by message (memory image in memory): " << ms_since(t0) * 1000.0 / cycles
              << " us\n";
    return 0;
}

// Time of one request through the paglet ABI, without the runtime.
int cmd_call_bench(const Args& a) {
    if (a.positional.size() != 2) return fail("usage: call-bench <module.wasm> <message> [--count N]");
    auto module = load_module(a.positional[0]);
    auto inst = pw::Instance::create(module);
    if (!inst) return fail(inst.error());
    Driven d = drive(std::move(*inst), true);
    const auto count = a.number("count", 100'000);
    auto t0 = Clock::now();
    for (long long i = 0; i < count; ++i) {
        auto r = d.harness->call(a.positional[1]);
        if (!r) return fail(r.error());
    }
    std::cout << std::fixed << std::setprecision(2) << a.positional[1] << ": "
              << ms_since(t0) * 1000.0 / static_cast<double>(count) << " us per request\n";
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: paglets-spike info|image-save|image-resume|bench|runtime ...\n";
        return 2;
    }
    const std::string cmd = argv[1];
    const Args args = parse_args(argc, argv);
    try {
        if (cmd == "info") return cmd_info(args);
        if (cmd == "image-save") return cmd_image_save(args);
        if (cmd == "image-resume") return cmd_image_resume(args);
        if (cmd == "bench") return cmd_bench(args);
        if (cmd == "runtime") return cmd_runtime(args);
        if (cmd == "call-bench") return cmd_call_bench(args);
    } catch (const std::exception& e) {
        return fail(e.what());
    }
    return fail("unknown command " + cmd);
}
