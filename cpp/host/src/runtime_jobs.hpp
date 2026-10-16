// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Internal to the runtime: compute jobs (ABI v1.3, planning/cpp-compute-jobs.md).
//
// A job is a call of a job function of a module (paglets_on_job) in a fresh
// instance of its own, with an input, producing an output. Jobs run in this
// pool, away from the scheduler lanes: each running job has a thread and an
// executor of its own (a worker process with worker processes, else the
// host process). The pool admits jobs by slots (cores), queues the others
// in order, enforces wall time and memory, and stops jobs on cancellation.
//
// Ahead-of-time compilation: with a compiler (paglets-wamrc), the first job
// of a module starts compiling it for this CPU in the background; later jobs
// of the module run the compiled code. Compiled code is cached by module
// hash in the cache directory. Jobs never move, so native code is safe here,
// while paglets stay in the interpreter (their images move between hosts).

#pragma once

#include "runtime_exec.hpp"

#include <paglets/abi.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/wasm/engine.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace paglets::runtime {

struct JobRequest {
    PagletId paglet;             // the requester
    std::string requester_host;  // key ID of its host; empty: this host
    std::uint64_t correlation = 0;
    std::string owner;
    std::string module;  // hex hash
    std::string function;
    Bytes input;
    std::int64_t cores = 1;
    std::int64_t max_ms = 0;
    std::uint32_t memory_pages = 0;
    std::vector<std::string> marks;
};

struct JobOutcome {
    std::uint64_t id = 0;
    PagletId paglet;
    std::string requester_host;
    std::uint64_t correlation = 0;
    std::string module;       // hex hash
    std::int32_t status = 0;  // abi::ok or an abi::Error
    Bytes output;
    std::string error;
    std::int64_t elapsed_ms = 0;
    std::int64_t queued_ms = 0;
    bool aot = false;
    std::vector<std::string> marks;
};

class JobPool {
public:
    struct Settings {
        unsigned slots = 0;  // cores for jobs; 0: the CPUs of the machine
        std::filesystem::path worker;  // paglets-worker; empty: jobs run in this process
        bool sandbox = true;
        std::int64_t default_max_ms = -1;
        std::int64_t cap_max_ms = -1;
        std::uint32_t default_pages = 0;  // unknown: grow within the host/wasm32 allowance
        std::uint32_t max_pages = 65536;  // wasm32, 4 GB; not a preallocation
        std::uint32_t stack_size = 256 * 1024;
        std::size_t owner_limit = 256;  // queued and running jobs per owner
        std::filesystem::path aot_compiler;  // empty: no AOT compilation
        std::filesystem::path aot_cache;     // compiled modules; empty: a temporary directory
        bool hw_bound_check = true;          // the engine relies on guard pages
        std::string host_name;
        // Loads a module by hash (on the job's thread: it may compile).
        std::function<std::expected<std::shared_ptr<wasm::Module>, std::string>(const std::string&)> acquire;
    };
    using Done = std::function<void(JobOutcome)>;
    using Log = std::function<void(std::int32_t level, const std::string&)>;

    JobPool(Settings settings, Done done, Log log);
    ~JobPool();
    JobPool(const JobPool&) = delete;
    JobPool& operator=(const JobPool&) = delete;

    // Queues a job (it runs when its cores are free); returns its ID, or an
    // abi::Error (quota: more cores than the pool has, or the owner's limit).
    std::expected<std::uint64_t, std::int32_t> submit(JobRequest request);
    // Stops a job (queued or running); its outcome is `status`.
    bool cancel(std::uint64_t id, std::int32_t status = abi::cancelled);
    std::optional<JobInfo> info(std::uint64_t id) const;
    std::vector<JobInfo> list() const;

    void set_slots(unsigned slots);
    bool reserve_slots(std::int64_t cores, std::chrono::steady_clock::time_point queued_at);
    void set_waiter(std::int64_t cores, std::chrono::steady_clock::time_point queued_at);
    void release_slots(std::int64_t cores);
    unsigned slots() const;
    std::int64_t cores_running() const;
    std::size_t queued() const;
    // Waits until no job is queued or running (tests); false on timeout.
    bool wait_idle(std::chrono::milliseconds timeout);
    // Compiles the module now unless it is compiled or compiling (tests,
    // benchmarks); true once compiled code is in the cache.
    bool compile_now(const std::shared_ptr<wasm::Module>& module, std::chrono::milliseconds timeout);
    bool aot_available() const;
    void shutdown();

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace paglets::runtime
