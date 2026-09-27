// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-spike: milestone M0 feasibility tool.
//
//   paglets-spike info <module.wasm>
//   paglets-spike image-save <counter.wasm> <image> [--increments N] [--bloat KB]
//   paglets-spike image-resume <counter.wasm> <image> [--increments N] [--expect VALUE]
//   paglets-spike bench <testbed.wasm> <counter.wasm> [--instances N]
//
// image-save and image-resume run in different processes (and, for the
// cross-architecture check, on different hosts): a counter paglet is driven,
// captured as a memory image, and resumed elsewhere with its state intact.
// The counter protocol is written by hand here so the tool also builds without
// C++26 reflection.

#include "process_stats.hpp"

#include <paglets/msgpack.hpp>
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

// --- counter protocol (ABI v0: [name, body] -> [status, body]) ------------

std::vector<std::uint8_t> increment_request(std::int64_t by, const std::string& note) {
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    w.write_str("increment");
    w.write_map_header(3);
    w.write_str("by");
    w.write_int(by);
    w.write_str("note");
    w.write_str(note);
    w.write_str("mode");
    w.write_str("add");
    return w.take();
}

std::vector<std::uint8_t> simple_request(const std::string& name) {
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    w.write_str(name);
    w.write_nil();
    return w.take();
}

std::vector<std::uint8_t> bloat_request(std::uint32_t kilobytes) {
    paglets::msgpack::Writer w;
    w.write_array_header(2);
    w.write_str("bloat");
    w.write_map_header(2);
    w.write_str("kilobytes");
    w.write_uint(kilobytes);
    w.write_str("fill");
    w.write_uint(0x5a);
    return w.take();
}

struct CounterStatus {
    std::int64_t value = 0;
    std::uint32_t history_size = 0;
    std::uint32_t memory_pages = 0;
};

CounterStatus parse_status(const std::vector<std::uint8_t>& reply) {
    paglets::msgpack::Reader r(reply);
    std::uint32_t parts = 0;
    std::string_view status;
    if (!r.read_array_header(parts) || parts != 2 || !r.read_str(status)) {
        throw std::runtime_error("malformed reply");
    }
    if (status != "ok") {
        std::string message;
        r.read_str(message);
        throw std::runtime_error("paglet error: " + message);
    }
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

CounterStatus send(pw::Instance& inst, const std::vector<std::uint8_t>& request) {
    auto reply = inst.send(request);
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

    const auto increments = a.number("increments", 5);
    CounterStatus s;
    for (long long i = 1; i <= increments; ++i) {
        s = send(**inst, increment_request(i, "step " + std::to_string(i)));
    }
    if (a.has("bloat")) {
        s = send(**inst, bloat_request(static_cast<std::uint32_t>(a.number("bloat", 0))));
    }

    const auto t0 = Clock::now();
    auto snap = pw::capture(**inst);
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

    CounterStatus s = send(**inst, simple_request("status"));
    std::cout << "resumed counter: value " << s.value << ", history " << s.history_size << "\n";
    const auto increments = a.number("increments", 0);
    for (long long i = 1; i <= increments; ++i) {
        s = send(**inst, increment_request(i, "after resume " + std::to_string(i)));
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

    // Message round trip through ABI v0.
    auto ctr = pw::Instance::create(counter);
    if (!ctr) return fail(ctr.error());
    const auto status = simple_request("status");
    constexpr int messages = 100'000;
    t0 = Clock::now();
    for (int i = 0; i < messages; ++i) {
        auto r = (*ctr)->send(status);
        if (!r) return fail(r.error());
    }
    std::cout << "message round trip (status, MessagePack): " << ms_since(t0) * 1e3 / messages << " us\n";

    // Memory image capture and restore.
    for (int i = 1; i <= 100; ++i) send(**ctr, increment_request(i, "bench"));
    t0 = Clock::now();
    auto snap = pw::capture(**ctr);
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
    std::vector<std::unique_ptr<pw::Instance>> many;
    many.reserve(static_cast<std::size_t>(count));
    const auto rss0 = paglets::resident_bytes();
    t0 = Clock::now();
    for (int i = 0; i < count; ++i) {
        auto inst = pw::Instance::create(counter);
        if (!inst) return fail("instance " + std::to_string(i) + ": " + inst.error());
        send(**inst, increment_request(1, "x"));
        many.push_back(std::move(*inst));
    }
    const double create_ms = ms_since(t0);
    const auto rss1 = paglets::resident_bytes();
    std::cout << count << " live counter instances: " << create_ms << " ms, resident +" << (rss1 - rss0) / 1024 / 1024
              << " MB (" << (rss1 - rss0) / 1024 / count << " KB per instance)\n";
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: paglets-spike info|image-save|image-resume|bench ...\n";
        return 2;
    }
    const std::string cmd = argv[1];
    const Args args = parse_args(argc, argv);
    try {
        if (cmd == "info") return cmd_info(args);
        if (cmd == "image-save") return cmd_image_save(args);
        if (cmd == "image-resume") return cmd_image_resume(args);
        if (cmd == "bench") return cmd_bench(args);
    } catch (const std::exception& e) {
        return fail(e.what());
    }
    return fail("unknown command " + cmd);
}
