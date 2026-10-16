// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "runtime_jobs.hpp"

#include "ipc.hpp"
#include "job_budget.hpp"

#include <paglets/msgpack.hpp>
#include <paglets/sha256.hpp>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <system_error>

#ifndef _WIN32
#include <csignal>
#endif

namespace paglets::runtime {

namespace {

using Clock = std::chrono::steady_clock;
namespace fs = std::filesystem;

std::int64_t ms_between(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(b - a).count();
}

// The host side of a job instance's imports: its input, its output, its
// progress and whether it should stop.
class JobImports final : public wasm::HostImports {
public:
    JobImports(const Bytes& input, std::size_t max_output, std::function<void(std::int32_t, std::string_view)> log)
        : input_(input), max_output_(max_output), log_(std::move(log)) {}

    void log(std::int32_t level, std::string_view text) override { log_(level, text); }
    std::int32_t job_input(std::int64_t offset, std::span<std::uint8_t> out) override {
        if (offset < 0 || static_cast<std::uint64_t>(offset) > input_.size()) return abi::invalid_argument;
        const std::size_t n = std::min<std::size_t>(out.size(), input_.size() - static_cast<std::size_t>(offset));
        std::copy_n(input_.begin() + offset, n, out.begin());
        return static_cast<std::int32_t>(n);
    }
    std::int32_t job_output(std::span<const std::uint8_t> data) override {
        if (output.size() + data.size() > max_output_) return abi::too_large;
        output.insert(output.end(), data.begin(), data.end());
        return abi::ok;
    }
    std::int32_t job_progress(std::int64_t d, std::int64_t t) override {
        done = d;
        total = t;
        return abi::ok;
    }
    std::int32_t job_cancelled() override { return stop.load() ? 1 : 0; }

    Bytes output;
    std::atomic<std::int64_t> done{0};
    std::atomic<std::int64_t> total{0};
    std::atomic<bool> stop{false};

private:
    const Bytes& input_;
    std::size_t max_output_;
    std::function<void(std::int32_t, std::string_view)> log_;
};

}  // namespace

struct JobPool::Impl {
    struct Job {
        std::uint64_t id = 0;
        JobRequest request;
        Clock::time_point submitted{};
        Clock::time_point started{};
        Clock::time_point deadline{};
        bool running = false;
        bool aot = false;
        std::int32_t stop_status = 0;  // set: the job is being stopped with this outcome
        Clock::time_point stop_at{};
        JobImports* imports = nullptr;
        Placed* placed = nullptr;
        std::optional<int> worker_pid;
        std::thread thread;
    };

    Settings settings;
    Done done;
    Log log;
    mutable std::mutex mu;
    std::condition_variable cv;
    std::map<std::uint64_t, std::unique_ptr<Job>> jobs;
    std::deque<std::uint64_t> queue;
    std::uint64_t next_id = 1;
    std::int64_t cores_used = 0;
    std::int64_t cores_reserved = 0;  // leases issued by compute-slots
    std::int64_t waiter_cores = 0;
    Clock::time_point waiter_since{};
    bool stopping = false;
    std::thread watchdog;
    std::vector<std::thread> finished;  // threads of ended jobs, joined by the watchdog
    std::vector<JobOutcome> outcomes;   // of jobs cancelled while queued, reported by the watchdog

    // Ahead-of-time compilation.
    std::map<std::string, std::shared_ptr<wasm::Module>> aot_modules;  // by module hash
    std::set<std::string> compiling;
    std::set<std::string> aot_failed;
    std::vector<std::thread> compilers;
    fs::path cache_dir;
    std::string aot_tag;
    bool own_cache = false;

    Impl(Settings s, Done d, Log l) : settings(std::move(s)), done(std::move(d)), log(std::move(l)) {
        if (settings.slots == 0) settings.slots = std::max(1u, std::thread::hardware_concurrency());
        if (!settings.aot_compiler.empty() && wasm::Module::aot_supported()) {
            std::error_code ec;
            cache_dir = settings.aot_cache;
            if (cache_dir.empty()) {
                std::array<std::uint8_t, 8> suffix{};
                wasm::fill_random(suffix);
                cache_dir = fs::temp_directory_path(ec) / ("paglets-aot-" + to_hex(std::span<const std::uint8_t>(suffix)));
                own_cache = true;
            }
            fs::create_directories(cache_dir, ec);
            // Compiled code depends on the compiler, its options and the
            // engine's bound checks: they name the cache entries.
            std::string what = settings.aot_compiler.string() + "|" + aot_flags_text();
            if (auto t = fs::last_write_time(settings.aot_compiler, ec); !ec) {
                what += "|" + std::to_string(t.time_since_epoch().count());
            }
            if (auto sz = fs::file_size(settings.aot_compiler, ec); !ec) what += "|" + std::to_string(sz);
            const auto digest = sha256(std::span(reinterpret_cast<const std::uint8_t*>(what.data()), what.size()));
            aot_tag = to_hex(std::span<const std::uint8_t>(digest.data(), 6));
        }
        watchdog = std::thread([this] { watch(); });
    }

    ~Impl() { stop_all(); }

    std::vector<std::string> aot_flags() const {
        // Keep explicit bounds checks in jobs on every platform. WAMR's
        // guard-page AOT path can mis-handle a job that grows its memory
        // (large Pi conversion reproduces traps/bad_alloc on macOS).
        // The options are part of the cache key, so older code is not reused.
        return {"--opt-level=3", "--disable-simd", "--enable-multi-thread", "--bounds-checks=1"};
    }
    std::string aot_flags_text() const {
        std::string t;
        for (const auto& f : aot_flags()) t += f + " ";
        return t;
    }
    bool aot_enabled() const { return !aot_tag.empty(); }

    void stop_all() {
        std::vector<std::thread> threads;
        {
            std::unique_lock lock(mu);
            if (stopping) return;
            stopping = true;
            for (auto& [id, j] : jobs) {
                if (j->running) request_stop(*j, abi::gone);
            }
            cv.notify_all();
        }
        if (watchdog.joinable()) watchdog.join();
        {
            std::unique_lock lock(mu);
            for (auto& [id, j] : jobs) {
                if (j->thread.joinable()) threads.push_back(std::move(j->thread));
            }
            for (auto& t : finished) threads.push_back(std::move(t));
            finished.clear();
            for (auto& t : compilers) threads.push_back(std::move(t));
            compilers.clear();
        }
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }
        if (own_cache) {
            std::error_code ec;
            fs::remove_all(cache_dir, ec);
        }
    }

    // mu held.
    void request_stop(Job& j, std::int32_t status) {
        if (j.stop_status != 0) return;
        j.stop_status = status;
        j.stop_at = Clock::now();
        if (j.imports != nullptr) j.imports->stop = true;
        if (j.placed != nullptr) j.placed->terminate();
    }

    // mu held; callbacks are deferred to the watchdog's unlocked report phase.
    void end_queued(std::uint64_t id, std::int32_t status) {
        const auto& j = *jobs.at(id);
        JobOutcome out;
        out.id = id;
        out.paglet = j.request.paglet;
        out.requester_host = j.request.requester_host;
        out.correlation = j.request.correlation;
        out.module = j.request.module;
        out.status = status;
        out.error = std::string(abi::error_name(status));
        out.queued_ms = ms_between(j.submitted, Clock::now());
        outcomes.push_back(std::move(out));
        std::erase(queue, id);
        jobs.erase(id);
    }

    // mu held: starts the jobs at the head of the queue that fit.
    void start_ready() {
        while (!queue.empty() && !stopping) {
            auto it = jobs.find(queue.front());
            if (it == jobs.end()) {
                queue.pop_front();
                continue;
            }
            Job& j = *it->second;
            const auto free_after = static_cast<std::int64_t>(settings.slots) - cores_used - cores_reserved - j.request.cores;
            if (free_after < 0) return;
            // Native FIFO wins equal timestamps. An older lease head owns
            // its required capacity; later jobs may use only the surplus.
            if (waiter_cores > 0 && waiter_cores <= settings.slots && waiter_since < j.submitted &&
                free_after < waiter_cores) return;
            queue.pop_front();
            cores_used += j.request.cores;
            j.running = true;
            j.started = Clock::now();
            j.deadline = job_detail::deadline<Clock>(j.started, j.request.max_ms);
            j.thread = std::thread([this, id = j.id] { run(id); });
        }
    }

    std::shared_ptr<wasm::Module> compiled(const std::shared_ptr<wasm::Module>& m) {
        if (!aot_enabled()) return nullptr;
        const std::string hash = m->hash_hex();
        std::lock_guard lock(mu);
        if (auto it = aot_modules.find(hash); it != aot_modules.end()) return it->second;
        if (!compiling.contains(hash) && !aot_failed.contains(hash) && !stopping) {
            compiling.insert(hash);
            compilers.emplace_back([this, m] { compile(m); });
        }
        return nullptr;
    }

    fs::path cache_file(const std::string& hash) const { return cache_dir / (hash + "-" + aot_tag + ".aot"); }

    // Compiles a module with the host's compiler into the cache (or loads
    // what an earlier run compiled), on a thread of its own.
    void compile(const std::shared_ptr<wasm::Module> m) {
        const std::string hash = m->hash_hex();
        const fs::path out = cache_file(hash);
        auto finish = [&](std::shared_ptr<wasm::Module> loaded, const std::string& why) {
            if (!loaded) log(2, "jobs: module " + hash.substr(0, 16) + " runs in the interpreter: " + why);
            std::lock_guard lock(mu);
            compiling.erase(hash);
            if (loaded) {
                aot_modules[hash] = std::move(loaded);
            } else {
                aot_failed.insert(hash);
            }
            cv.notify_all();
        };
        auto load = [&]() -> std::expected<std::shared_ptr<wasm::Module>, std::string> {
            auto bytes = wasm::read_file(out.string());
            if (!bytes) return std::unexpected(bytes.error());
            Bytes wasm_bytes(m->bytes().begin(), m->bytes().end());
            return wasm::Module::load_aot(std::move(wasm_bytes), std::move(*bytes), wasm::ImportPolicy::system());
        };
        std::error_code ec;
        if (fs::exists(out, ec)) {
            if (auto loaded = load()) return finish(*loaded, {});
            fs::remove(out, ec);
        }
        const fs::path in = cache_dir / (hash + "-" + aot_tag + ".wasm.tmp");
        const fs::path tmp = cache_dir / (hash + "-" + aot_tag + ".aot.tmp");
        if (auto w = wasm::write_file(in.string(), m->bytes()); !w) return finish(nullptr, w.error());
#ifdef _WIN32
        return finish(nullptr, "no AOT compilation on this platform");
#else
        // The compiler's own output goes nowhere: a shell redirects it.
        std::vector<std::string> args = {"-c", "exec \"$0\" \"$@\" >/dev/null 2>&1", settings.aot_compiler.string()};
        for (const auto& f : aot_flags()) args.push_back(f);
        args.push_back("-o");
        args.push_back(tmp.string());
        args.push_back(in.string());
        const auto began = Clock::now();
        auto process = ipc::Process::spawn("/bin/sh", args, {});
        if (!process) {
            fs::remove(in, ec);
            return finish(nullptr, process.error());
        }
        while (!process->exited()) {
            {
                std::lock_guard lock(mu);
                if (stopping) break;
            }
            if (Clock::now() - began > std::chrono::minutes(10)) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        process->stop(std::chrono::milliseconds(0));
        fs::remove(in, ec);
        if (!fs::exists(tmp, ec)) return finish(nullptr, "the compiler made no code");
        fs::rename(tmp, out, ec);
        if (ec) return finish(nullptr, ec.message());
        auto loaded = load();
        if (!loaded) return finish(nullptr, loaded.error());
        log(1, "jobs: module " + hash.substr(0, 16) + " compiled for this CPU in " +
                   std::to_string(ms_between(began, Clock::now())) + " ms");
        finish(*loaded, {});
#endif
    }

    void run(std::uint64_t id) {
        Job* job = nullptr;
        {
            std::lock_guard lock(mu);
            job = jobs.at(id).get();
        }
        const JobRequest& rq = job->request;
        std::string failure;
        std::shared_ptr<wasm::Module> module;
        if (auto m = settings.acquire(rq.module)) {
            module = std::move(*m);
        } else {
            failure = m.error();
        }
        std::shared_ptr<wasm::Module> native = module ? compiled(module) : nullptr;
        const bool aot = native != nullptr;
        if (native) module = native;
        const bool in_process = module && settings.worker.empty();
        std::unique_ptr<Executor> executor;
        if (in_process) {
            executor = make_in_process_executor();
            wasm::init_thread();
        } else if (module) {
            auto w = make_worker_executor(settings.worker, settings.sandbox,
                                          [this](const std::string& text) { log(2, "jobs: " + text); });
            if (w) {
                executor = std::move(*w);
            } else {
                failure = w.error();
            }
        }
        const std::size_t max_output = std::size_t{settings.max_pages} * wasm::page_size;
        JobImports imports(rq.input, max_output, [this, id](std::int32_t level, std::string_view text) {
            log(level, "job " + std::to_string(id) + ": " + std::string(text));
        });
        std::expected<std::int32_t, std::string> result = std::unexpected(failure);
        if (executor) {
            wasm::Limits limits;
            limits.stack_size = settings.stack_size;
            limits.max_memory_pages = rq.memory_pages;
            std::unique_ptr<Placed> placed;
            {
                std::lock_guard lock(mu);
                job->aot = aot;
                job->imports = &imports;
                if (job->stop_status != 0) imports.stop = true;
            }
            auto p = executor->place("job-" + std::to_string(id), module, limits, nullptr, imports);
            if (!p) {
                result = std::unexpected(p.error());
            } else {
                placed = std::move(*p);
                {
                    std::lock_guard lock(mu);
                    job->placed = placed.get();
                    job->worker_pid = executor->process_id();
                    if (job->stop_status != 0) placed->terminate();
                }
                const Bytes call = abi::encode(abi::JobCall{rq.function, static_cast<std::int64_t>(rq.input.size())});
                result = placed->call(abi::job_export, {}, call);
                std::lock_guard lock(mu);
                job->placed = nullptr;
            }
            {
                std::lock_guard lock(mu);
                job->imports = nullptr;
            }
            placed.reset();
            executor.reset();
        }
        if (in_process) wasm::deinit_thread();

        JobOutcome out;
        out.id = id;
        out.paglet = rq.paglet;
        out.requester_host = rq.requester_host;
        out.correlation = rq.correlation;
        out.module = rq.module;
        out.aot = aot;
        out.marks = rq.marks;
        {
            std::lock_guard lock(mu);
            const auto now = Clock::now();
            out.elapsed_ms = ms_between(job->started, now);
            out.queued_ms = ms_between(job->submitted, job->started);
            if (job->stop_status != 0) {
                out.status = job->stop_status;
                out.error = job->stop_status == abi::timeout ? "the job ran longer than its "
                                                              + std::to_string(rq.max_ms) + " ms"
                                                            : std::string(abi::error_name(job->stop_status));
            } else if (!result) {
                out.status = abi::failed;
                out.error = result.error();
            } else if (*result != 0) {
                out.status = *result < 0 ? *result : abi::failed;
                out.error = std::string(abi::error_name(out.status));
            } else {
                out.output = std::move(imports.output);
            }
            cores_used -= rq.cores;
            if (job->thread.joinable()) finished.push_back(std::move(job->thread));
            jobs.erase(id);
            start_ready();
            cv.notify_all();
        }
        done(std::move(out));
    }

    // Enforces wall time, makes stopping jobs stop, and joins the threads of
    // ended jobs.
    void watch() {
        std::unique_lock lock(mu);
        // Shutdown still needs the worker kill fallback and the completed
        // thread joins. Queued jobs never start once stopping is set.
        while (!stopping || std::ranges::any_of(jobs, [](const auto& e) { return e.second->running; })) {
            cv.wait_for(lock, std::chrono::milliseconds(50));
            const auto now = Clock::now();
            for (auto& [id, j] : jobs) {
                if (!j->running) continue;
                if (j->stop_status == 0 && j->request.max_ms != -1 && now >= j->deadline) request_stop(*j, abi::timeout);
                // Code that does not check for termination (it must not
                // happen, but a worker can be killed): after 3 s, the
                // worker process goes.
                if (j->stop_status != 0 && now - j->stop_at > std::chrono::seconds(3) && j->worker_pid) {
#ifndef _WIN32
                    ::kill(*j->worker_pid, SIGKILL);
#endif
                    j->worker_pid.reset();
                }
            }
            std::vector<std::thread> ended = std::move(finished);
            finished.clear();
            std::vector<JobOutcome> reports = std::move(outcomes);
            outcomes.clear();
            lock.unlock();
            for (auto& t : ended) {
                if (t.joinable()) t.join();
            }
            for (auto& o : reports) done(std::move(o));
            lock.lock();
        }
    }

    JobInfo info_of(const Job& j) const {
        JobInfo i;
        i.id = j.id;
        i.paglet = j.request.paglet;
        i.requester_host = j.request.requester_host;
        i.correlation = j.request.correlation;
        i.owner = j.request.owner;
        i.module = j.request.module;
        i.function = j.request.function;
        i.state = j.running ? "running" : "queued";
        i.cores = j.request.cores;
        const auto now = Clock::now();
        i.queued_ms = ms_between(j.submitted, j.running ? j.started : now);
        i.elapsed_ms = j.running ? ms_between(j.started, now) : 0;
        if (j.imports != nullptr) {
            i.done = j.imports->done;
            i.total = j.imports->total;
        }
        i.aot = j.aot;
        i.max_ms = j.request.max_ms;
        i.memory_pages = j.request.memory_pages;
        return i;
    }
};

JobPool::JobPool(Settings settings, Done done, Log log)
    : impl_(std::make_unique<Impl>(std::move(settings), std::move(done), std::move(log))) {}

JobPool::~JobPool() = default;

std::expected<std::uint64_t, std::int32_t> JobPool::submit(JobRequest request) {
    std::lock_guard lock(impl_->mu);
    if (impl_->stopping) return std::unexpected(abi::gone);
    if (request.cores < 1) request.cores = 1;
    if (request.cores > static_cast<std::int64_t>(impl_->settings.slots)) return std::unexpected(abi::quota);
    const std::size_t of_owner = std::ranges::count_if(
        impl_->jobs, [&](const auto& e) { return e.second->request.owner == request.owner; });
    if (of_owner >= impl_->settings.owner_limit) return std::unexpected(abi::quota);
    request.max_ms = job_detail::budget(request.max_ms, impl_->settings.default_max_ms, impl_->settings.cap_max_ms);
    if (request.max_ms < -1) return std::unexpected(abi::invalid_argument);
    if (request.memory_pages == 0) request.memory_pages = impl_->settings.default_pages == 0 ? impl_->settings.max_pages : impl_->settings.default_pages;
    if (request.memory_pages > 65536 || request.memory_pages == 0) return std::unexpected(abi::invalid_argument);
    if (request.memory_pages > impl_->settings.max_pages) return std::unexpected(abi::quota);
    auto job = std::make_unique<Impl::Job>();
    job->id = impl_->next_id++;
    job->request = std::move(request);
    job->submitted = Clock::now();
    const auto id = job->id;
    impl_->jobs.emplace(id, std::move(job));
    impl_->queue.push_back(id);
    impl_->start_ready();
    return id;
}

// The outcome of a cancelled job is reported later, on another thread (the
// caller may hold locks the report needs).
bool JobPool::cancel(std::uint64_t id, std::int32_t status) {
    {
        std::lock_guard lock(impl_->mu);
        auto it = impl_->jobs.find(id);
        if (it == impl_->jobs.end()) return false;
        auto& j = *it->second;
        if (j.running) {
            impl_->request_stop(j, status);
            impl_->cv.notify_all();
            return true;
        }
        // Queued: it ends here.
        impl_->end_queued(id, status);
        impl_->start_ready();
        impl_->cv.notify_all();
    }
    return true;
}

std::optional<JobInfo> JobPool::info(std::uint64_t id) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->jobs.find(id);
    if (it == impl_->jobs.end()) return std::nullopt;
    return impl_->info_of(*it->second);
}

std::vector<JobInfo> JobPool::list() const {
    std::lock_guard lock(impl_->mu);
    std::vector<JobInfo> out;
    for (const auto& [id, j] : impl_->jobs) out.push_back(impl_->info_of(*j));
    return out;
}

void JobPool::set_slots(unsigned slots) {
    std::lock_guard lock(impl_->mu);
    impl_->settings.slots = std::max(1u, slots);
    const auto pending = impl_->queue;
    for (auto id : pending) {
        if (impl_->jobs.at(id)->request.cores > impl_->settings.slots) impl_->end_queued(id, abi::quota);
    }
    impl_->start_ready();
    impl_->cv.notify_all();
}

unsigned JobPool::slots() const {
    std::lock_guard lock(impl_->mu);
    return impl_->settings.slots;
}

bool JobPool::reserve_slots(std::int64_t cores, Clock::time_point queued_at) {
    std::lock_guard lock(impl_->mu);
    const auto free = static_cast<std::int64_t>(impl_->settings.slots) - impl_->cores_used - impl_->cores_reserved;
    if (impl_->stopping || cores < 1 || cores > free) return false;
    const auto free_after = free - cores;
    if (!impl_->queue.empty()) {
        const auto& head = *impl_->jobs.at(impl_->queue.front());
        if (head.submitted <= queued_at && free_after < head.request.cores) return false;
    }
    if (impl_->waiter_cores > 0 && impl_->waiter_cores <= impl_->settings.slots &&
        impl_->waiter_since < queued_at && free_after < impl_->waiter_cores) return false;
    impl_->cores_reserved += cores;
    return true;
}

void JobPool::set_waiter(std::int64_t cores, Clock::time_point queued_at) {
    std::lock_guard lock(impl_->mu);
    impl_->waiter_cores = std::max<std::int64_t>(0, cores);
    impl_->waiter_since = queued_at;
    impl_->start_ready();
}

void JobPool::release_slots(std::int64_t cores) {
    std::lock_guard lock(impl_->mu);
    impl_->cores_reserved -= std::clamp<std::int64_t>(cores, 0, impl_->cores_reserved);
    impl_->start_ready();
}

std::int64_t JobPool::cores_running() const {
    std::lock_guard lock(impl_->mu);
    return impl_->cores_used;
}

std::size_t JobPool::queued() const {
    std::lock_guard lock(impl_->mu);
    return impl_->queue.size();
}

bool JobPool::wait_idle(std::chrono::milliseconds timeout) {
    std::unique_lock lock(impl_->mu);
    return impl_->cv.wait_for(lock, timeout, [&] { return impl_->jobs.empty(); });
}

bool JobPool::compile_now(const std::shared_ptr<wasm::Module>& module, std::chrono::milliseconds timeout) {
    if (!impl_->aot_enabled()) return false;
    (void)impl_->compiled(module);
    const std::string hash = module->hash_hex();
    std::unique_lock lock(impl_->mu);
    impl_->cv.wait_for(lock, timeout, [&] { return !impl_->compiling.contains(hash); });
    return impl_->aot_modules.contains(hash);
}

bool JobPool::aot_available() const {
    return impl_->aot_enabled();
}

void JobPool::shutdown() {
    impl_->stop_all();
}

}  // namespace paglets::runtime
