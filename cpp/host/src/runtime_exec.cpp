// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "runtime_exec.hpp"

#include "ipc.hpp"
#include "shm_ring.hpp"
#include "sandbox.hpp"

#include <paglets/abi.hpp>
#include <paglets/msgpack.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#ifndef _WIN32
#include <csignal>
#endif

namespace paglets::runtime {

namespace {

using Bytes = std::vector<std::uint8_t>;

// ---------------------------------------------------------------------------
// In the host process

class InProcessPlaced final : public Placed {
public:
    explicit InProcessPlaced(std::unique_ptr<wasm::Instance> instance) : instance_(std::move(instance)) {}

    std::expected<std::int32_t, std::string> call(std::string_view export_name, std::span<const std::uint32_t> lead,
                                                  std::span<const std::uint8_t> data) override {
        return instance_->call_with_data(export_name, lead, data);
    }
    std::expected<wasm::Snapshot, std::string> capture() override { return wasm::capture(*instance_); }
    void terminate() override { instance_->terminate(); }
    bool lost() override { return false; }

private:
    std::unique_ptr<wasm::Instance> instance_;
};

class InProcessExecutor final : public Executor {
public:
    std::expected<std::unique_ptr<Placed>, std::string> place(const std::string&,
                                                              const std::shared_ptr<wasm::Module>& module,
                                                              const wasm::Limits& limits, const wasm::Snapshot* image,
                                                              wasm::HostImports& imports) override {
        auto inst = image != nullptr ? wasm::restore(module, *image, limits) : wasm::Instance::create(module, limits);
        if (!inst) return std::unexpected(inst.error());
        (*inst)->set_imports(&imports);
        return std::make_unique<InProcessPlaced>(std::move(*inst));
    }
};

// ---------------------------------------------------------------------------
// Frames

// Reads the [0, ...] / [1, error] status of a worker answer.
std::expected<void, std::string> answer_status(msgpack::Reader& r, std::uint32_t& n) {
    std::int32_t tag = 0;
    if (!r.read_array_header(n) || n < 1 || !msgpack::read_value(r, tag)) {
        return std::unexpected(std::string("worker: malformed answer"));
    }
    if (tag == 1) {
        std::string error;
        msgpack::read_value(r, error);
        return std::unexpected(error);
    }
    if (tag != 0) return std::unexpected(std::string("worker: unexpected answer"));
    return {};
}

Bytes ok_frame() {
    msgpack::Writer w;
    w.write_array_header(1);
    w.write_int(0);
    return w.take();
}

Bytes error_frame(const std::string& error) {
    msgpack::Writer w;
    w.write_array_header(2);
    w.write_int(1);
    w.write_str(error);
    return w.take();
}

// Executes an import frame from the worker against the host imports and
// returns the answer [result, document?].
Bytes serve_import(wasm::HostImports& h, msgpack::Reader& r) {
    std::string name;
    std::int64_t a = 0;
    std::int64_t b = 0;
    Bytes data;
    std::int64_t result = abi::malformed;
    std::optional<Bytes> doc;
    if (msgpack::read_value(r, name) && msgpack::read_value(r, a) && msgpack::read_value(r, b) &&
        msgpack::read_value(r, data)) {
        const auto h32 = static_cast<std::int32_t>(a);
        auto document = [&](wasm::HostImports::Document d) {
            if (d) {
                result = 0;
                doc = std::move(*d);
            } else {
                result = d.error();
            }
        };
        if (name == "log") {
            h.log(h32, std::string_view(reinterpret_cast<const char*>(data.data()), data.size()));
            result = 0;
        } else if (name == "self_info") {
            document(h.self_info());
        } else if (name == "send") {
            result = h.send(h32, data);
        } else if (name == "request") {
            result = h.request(h32, data);
        } else if (name == "reply") {
            result = h.reply(h32, data);
        } else if (name == "cap_derive") {
            result = h.cap_derive(h32, data);
        } else if (name == "cap_drop") {
            result = h.cap_drop(h32);
        } else if (name == "cap_inspect") {
            document(h.cap_inspect(h32));
        } else if (name == "cap_list") {
            document(h.cap_list());
        } else if (name == "create_child") {
            result = h.create_child(data);
        } else if (name == "lifecycle") {
            result = h.lifecycle(h32, data);
        } else if (name == "timer_set") {
            result = h.timer_set(a, data);
        } else {
            result = abi::unsupported;
        }
    }
    msgpack::Writer w;
    w.write_array_header(2);
    w.write_int(result);
    msgpack::write_value(w, doc);
    return w.take();
}

// ---------------------------------------------------------------------------
// Worker processes (control side)

class WorkerExecutor;

class WorkerPlaced final : public Placed {
public:
    WorkerPlaced(WorkerExecutor& worker, std::string paglet, std::uint64_t generation, wasm::HostImports& imports)
        : worker_(worker), paglet_(std::move(paglet)), generation_(generation), imports_(imports) {}
    ~WorkerPlaced() override;

    std::expected<std::int32_t, std::string> call(std::string_view export_name, std::span<const std::uint32_t> lead,
                                                  std::span<const std::uint8_t> data) override;
    std::expected<wasm::Snapshot, std::string> capture() override;
    void terminate() override;
    bool lost() override;

private:
    WorkerExecutor& worker_;
    std::string paglet_;
    std::uint64_t generation_;
    wasm::HostImports& imports_;
};

class WorkerExecutor final : public Executor {
public:
    WorkerExecutor(std::filesystem::path executable, bool sandbox, std::function<void(const std::string&)> warn)
        : executable_(std::move(executable)), sandbox_(sandbox), warn_(std::move(warn)) {}

    ~WorkerExecutor() override {
        std::lock_guard lock(mu_);
        stop("host stopping");
    }

    std::expected<std::unique_ptr<Placed>, std::string> place(const std::string& paglet,
                                                              const std::shared_ptr<wasm::Module>& module,
                                                              const wasm::Limits& limits, const wasm::Snapshot* image,
                                                              wasm::HostImports& imports) override {
        std::lock_guard lock(mu_);
        if (auto ok = ensure_started(); !ok) return std::unexpected(ok.error());
        const std::string hash = module->hash_hex();
        if (!loaded_.contains(hash)) {
            msgpack::Writer w;
            w.write_array_header(3);
            w.write_int(static_cast<std::int32_t>(ipc::Op::load_module));
            w.write_str(hash);
            w.write_bin(module->bytes());
            if (auto r = round_trip(w.bytes()); !r) return std::unexpected(r.error());
            loaded_.insert(hash);
        }
        msgpack::Writer w;
        w.write_array_header(6);
        w.write_int(static_cast<std::int32_t>(ipc::Op::create));
        w.write_str(paglet);
        w.write_str(hash);
        w.write_uint(limits.stack_size);
        w.write_uint(limits.max_memory_pages);
        if (image != nullptr) {
            w.write_bin(wasm::serialize(*image));
        } else {
            w.write_nil();
        }
        if (auto r = round_trip(w.bytes()); !r) return std::unexpected(r.error());
        return std::make_unique<WorkerPlaced>(*this, paglet, generation_, imports);
    }

    std::expected<std::int32_t, std::string> call(std::uint64_t generation, const std::string& paglet,
                                                  std::string_view export_name, std::span<const std::uint32_t> lead,
                                                  std::span<const std::uint8_t> data, wasm::HostImports& imports) {
        std::lock_guard lock(mu_);
        if (generation != generation_ || !alive_) return std::unexpected(std::string("worker process ended"));
        msgpack::Writer w;
        w.write_array_header(5);
        w.write_int(static_cast<std::int32_t>(ipc::Op::call));
        w.write_str(paglet);
        w.write_str(export_name);
        w.write_array_header(lead.size());
        for (auto v : lead) w.write_uint(v);
        w.write_bin(data);
        if (auto s = main_.send(w.bytes()); !s) return fail(s.error());
        while (true) {
            auto frame = main_.receive();
            if (!frame) return fail(frame.error());
            msgpack::Reader r(*frame);
            std::uint32_t n = 0;
            std::int32_t tag = 0;
            if (!r.read_array_header(n) || !msgpack::read_value(r, tag)) return fail("worker: malformed frame");
            if (tag == ipc::import_marker) {
                const Bytes answer = serve_import(imports, r);
                if (auto s = main_.send(answer); !s) return fail(s.error());
                continue;
            }
            if (tag == ipc::notify_marker) {
                (void)serve_import(imports, r);  // no answer
                continue;
            }
            if (tag == 1) {
                std::string error;
                msgpack::read_value(r, error);
                return std::unexpected(error);
            }
            std::int32_t result = 0;
            if (tag != 0 || !msgpack::read_value(r, result)) return fail("worker: malformed result");
            return result;
        }
    }

    std::expected<wasm::Snapshot, std::string> capture(std::uint64_t generation, const std::string& paglet) {
        std::lock_guard lock(mu_);
        if (generation != generation_ || !alive_) return std::unexpected(std::string("worker process ended"));
        msgpack::Writer w;
        w.write_array_header(2);
        w.write_int(static_cast<std::int32_t>(ipc::Op::capture));
        w.write_str(paglet);
        if (auto s = main_.send(w.bytes()); !s) return fail(s.error());
        auto frame = main_.receive();
        if (!frame) return fail(frame.error());
        msgpack::Reader r(*frame);
        std::uint32_t n = 0;
        if (auto ok = answer_status(r, n); !ok) return std::unexpected(ok.error());
        Bytes image;
        if (!msgpack::read_value(r, image)) return fail("worker: malformed image");
        return wasm::deserialize(image);
    }

    void destroy(std::uint64_t generation, const std::string& paglet) {
        std::lock_guard lock(mu_);
        if (generation != generation_ || !alive_) return;
        msgpack::Writer w;
        w.write_array_header(2);
        w.write_int(static_cast<std::int32_t>(ipc::Op::destroy));
        w.write_str(paglet);
        (void)round_trip(w.bytes());
    }

    void terminate(std::uint64_t generation, const std::string& paglet) {
        std::lock_guard lock(control_mu_);
        if (generation != generation_.load()) return;
        msgpack::Writer w;
        w.write_array_header(2);
        w.write_int(static_cast<std::int32_t>(ipc::Op::terminate));
        w.write_str(paglet);
        (void)control_.send(w.bytes());
    }

    // Notices a worker process that ended while it was idle: between calls
    // the worker sends nothing, so a readable channel means it closed. (The
    // process may take a while to become a zombie; its sockets close first.)
    bool lost(std::uint64_t generation) {
        std::lock_guard lock(mu_);
        if (alive_ && main_.closed_by_peer()) (void)fail("the process ended");
        return generation != generation_.load();
    }

    std::optional<int> process_id() const override {
        const int pid = pid_.load();
        return pid > 0 ? std::optional<int>(pid) : std::nullopt;
    }

private:
    std::expected<void, std::string> ensure_started() {
        if (alive_) return {};
        const std::size_t capacity = ipc::default_ring_capacity;
        auto memory = ipc::SharedMemory::create(ipc::ring_region_size(capacity));
        if (!memory) return std::unexpected(memory.error());
        auto doorbell = ipc::duplex_pair();
        if (!doorbell) return std::unexpected(doorbell.error());
        auto control = ipc::duplex_pair();
        if (!control) {
            ipc::close_ends(doorbell->host);
            ipc::close_ends(doorbell->child);
            return std::unexpected(control.error());
        }
        const std::intptr_t memory_handle = memory->handle();
        // The rings' control blocks exist before the worker attaches.
        ipc::RingChannel channel(std::move(*memory), capacity, doorbell->host, true);
        auto job = ipc::create_worker_job();
        // The worker gets: doorbell read and write ends, the control read
        // end and the ring memory.
        const std::vector<std::intptr_t> handles = {doorbell->child.read, doorbell->child.write, control->child.read,
                                                    memory_handle};
        const auto values = ipc::Process::child_values(handles);
        std::vector<std::string> args;
        for (auto v : values) args.push_back(std::to_string(v));
        args.push_back(std::to_string(capacity));
        if (!sandbox_) args.emplace_back("--no-sandbox");
        auto process = job ? ipc::Process::spawn(executable_, args, handles, *job)
                           : std::expected<ipc::Process, std::string>(std::unexpected(job.error()));
        ipc::close_ends(doorbell->child);
        ipc::close_ends(control->child);
        if (!process) {
            if (job && *job >= 0) ipc::close_handle(*job);
            ipc::close_ends(control->host);
            return std::unexpected("cannot start worker " + executable_.string() + ": " + process.error());
        }
        process_ = std::move(*process);
        job_ = *job;
        pid_ = process_.pid();
        main_ = std::move(channel);
        {
            std::lock_guard lock(control_mu_);
            control_ = ipc::Channel(control->host);
        }
        loaded_.clear();
        alive_ = true;
        return {};
    }

    std::expected<void, std::string> round_trip(std::span<const std::uint8_t> request) {
        if (auto s = main_.send(request); !s) return std::unexpected(fail(s.error()).error());
        auto frame = main_.receive();
        if (!frame) return std::unexpected(fail(frame.error()).error());
        msgpack::Reader r(*frame);
        std::uint32_t n = 0;
        return answer_status(r, n);
    }

    // The worker is unusable: stop it; every instance in it is lost.
    std::unexpected<std::string> fail(const std::string& reason) {
        stop(reason);
        if (warn_) warn_("worker process ended: " + reason);
        return std::unexpected("worker process ended: " + reason);
    }

    void stop(const std::string&) {
        if (!alive_) return;
        alive_ = false;
        ++generation_;
        main_.close();
        {
            std::lock_guard lock(control_mu_);
            control_.close();
        }
        pid_ = -1;
        // Closing the channels ends a healthy worker; a stuck one is killed.
        process_.stop(std::chrono::milliseconds(100));
        if (job_ >= 0) {
            ipc::close_handle(job_);  // kills anything left in the job
            job_ = -1;
        }
    }

    std::filesystem::path executable_;
    bool sandbox_ = true;
    std::function<void(const std::string&)> warn_;
    std::mutex mu_;          // main channel and process state
    std::mutex control_mu_;  // control channel (terminate from the watchdog)
    ipc::RingChannel main_;  // calls and imports
    ipc::Channel control_;   // terminations
    ipc::Process process_;
    std::intptr_t job_ = -1;  // Windows job object of the worker
    std::atomic<int> pid_{-1};
    bool alive_ = false;
    std::atomic<std::uint64_t> generation_{0};
    std::set<std::string> loaded_;
};

WorkerPlaced::~WorkerPlaced() {
    worker_.destroy(generation_, paglet_);
}

std::expected<std::int32_t, std::string> WorkerPlaced::call(std::string_view export_name,
                                                            std::span<const std::uint32_t> lead,
                                                            std::span<const std::uint8_t> data) {
    return worker_.call(generation_, paglet_, export_name, lead, data, imports_);
}

std::expected<wasm::Snapshot, std::string> WorkerPlaced::capture() {
    return worker_.capture(generation_, paglet_);
}

void WorkerPlaced::terminate() {
    worker_.terminate(generation_, paglet_);
}

bool WorkerPlaced::lost() {
    return worker_.lost(generation_);
}

// ---------------------------------------------------------------------------
// Worker process (worker side)

// Host imports of an instance in the worker: forwarded to the control
// process over the main channel, which is inside a call at that moment.
class ForwardedImports final : public wasm::HostImports {
public:
    explicit ForwardedImports(ipc::RingChannel& channel) : channel_(channel) {}

    // No result: sent without waiting for an answer.
    void log(std::int32_t level, std::string_view text) override {
        msgpack::Writer w;
        w.write_array_header(5);
        w.write_int(ipc::notify_marker);
        w.write_str("log");
        w.write_int(level);
        w.write_int(0);
        w.write_bin({reinterpret_cast<const std::uint8_t*>(text.data()), text.size()});
        if (!channel_.send(w.bytes())) std::_Exit(3);  // the host is gone
    }
    Document self_info() override { return document("self_info", 0, 0, {}); }
    std::int32_t send(std::int32_t endpoint, std::span<const std::uint8_t> msg) override {
        return result32(forward("send", endpoint, 0, msg));
    }
    std::int64_t request(std::int32_t endpoint, std::span<const std::uint8_t> msg) override {
        auto r = forward("request", endpoint, 0, msg);
        return r ? r->first : std::int64_t{abi::internal};
    }
    std::int32_t reply(std::int32_t handle, std::span<const std::uint8_t> msg) override {
        return result32(forward("reply", handle, 0, msg));
    }
    std::int32_t cap_derive(std::int32_t handle, std::span<const std::uint8_t> spec) override {
        return result32(forward("cap_derive", handle, 0, spec));
    }
    std::int32_t cap_drop(std::int32_t handle) override { return result32(forward("cap_drop", handle, 0, {})); }
    Document cap_inspect(std::int32_t handle) override { return document("cap_inspect", handle, 0, {}); }
    Document cap_list() override { return document("cap_list", 0, 0, {}); }
    std::int32_t create_child(std::span<const std::uint8_t> spec) override {
        return result32(forward("create_child", 0, 0, spec));
    }
    std::int32_t lifecycle(std::int32_t op, std::span<const std::uint8_t> arg) override {
        return result32(forward("lifecycle", op, 0, arg));
    }
    std::int32_t timer_set(std::int64_t delay_ms, std::span<const std::uint8_t> msg) override {
        return result32(forward("timer_set", delay_ms, 0, msg));
    }

private:
    using Answer = std::pair<std::int64_t, std::optional<Bytes>>;

    std::optional<Answer> forward(std::string_view name, std::int64_t a, std::int64_t b,
                                  std::span<const std::uint8_t> data) {
        msgpack::Writer w;
        w.write_array_header(5);
        w.write_int(ipc::import_marker);
        w.write_str(name);
        w.write_int(a);
        w.write_int(b);
        w.write_bin(data);
        if (!channel_.send(w.bytes())) std::_Exit(3);  // the host is gone
        auto frame = channel_.receive();
        if (!frame) std::_Exit(3);
        msgpack::Reader r(*frame);
        std::uint32_t n = 0;
        Answer answer;
        if (!r.read_array_header(n) || n != 2 || !msgpack::read_value(r, answer.first) ||
            !msgpack::read_value(r, answer.second)) {
            return std::nullopt;
        }
        return answer;
    }

    static std::int32_t result32(const std::optional<Answer>& a) {
        return a ? static_cast<std::int32_t>(a->first) : abi::internal;
    }

    Document document(std::string_view name, std::int64_t a, std::int64_t b, std::span<const std::uint8_t> data) {
        auto r = forward(name, a, b, data);
        if (!r) return std::unexpected(abi::internal);
        if (r->first != 0) return std::unexpected(static_cast<std::int32_t>(r->first));
        return r->second ? std::move(*r->second) : Bytes{};
    }

    ipc::RingChannel& channel_;
};

struct WorkerInstance {
    std::unique_ptr<ForwardedImports> imports;
    std::unique_ptr<wasm::Instance> instance;
};

}  // namespace

std::unique_ptr<Executor> make_in_process_executor() {
    return std::make_unique<InProcessExecutor>();
}

std::expected<std::unique_ptr<Executor>, std::string> make_worker_executor(
    std::filesystem::path executable, bool sandbox, std::function<void(const std::string&)> warn) {
    if (!std::filesystem::exists(executable)) {
        return std::unexpected("worker executable not found: " + executable.string());
    }
    return std::make_unique<WorkerExecutor>(std::move(executable), sandbox, std::move(warn));
}

int run_worker(ipc::Ends doorbell, ipc::Ends control_ends, std::intptr_t shm_handle, std::size_t ring_capacity,
               bool sandbox) {
#ifndef _WIN32
    ::signal(SIGPIPE, SIG_IGN);
#endif
    wasm::ensure_runtime();
    auto memory = ipc::SharedMemory::attach(shm_handle, ipc::ring_region_size(ring_capacity));
    if (!memory) {
        std::cerr << "paglets-worker: " << memory.error() << "\n";
        return 2;
    }
    if (sandbox) {
        // Everything that opens files lazily is initialized first.
        std::uint8_t warm[8];
        wasm::fill_random(warm);
        if (auto ok = sandbox_worker(); !ok) {
            std::cerr << "paglets-worker: running without OS sandbox: " << ok.error() << "\n";
        }
    }
    ipc::RingChannel main(std::move(*memory), ring_capacity, doorbell, false);
    ipc::Channel control(control_ends);
    std::mutex mu;
    std::map<std::string, WorkerInstance> instances;
    std::map<std::string, std::shared_ptr<wasm::Module>> modules;

    // Terminations arrive while the main thread is inside a call.
    std::thread watcher([&] {
        while (true) {
            auto frame = control.receive();
            if (!frame) return;
            msgpack::Reader r(*frame);
            std::uint32_t n = 0;
            std::int32_t op = 0;
            std::string paglet;
            if (!r.read_array_header(n) || !msgpack::read_value(r, op) || !msgpack::read_value(r, paglet)) continue;
            if (op != static_cast<std::int32_t>(ipc::Op::terminate)) continue;
            std::lock_guard lock(mu);
            if (auto it = instances.find(paglet); it != instances.end()) it->second.instance->terminate();
        }
    });
    watcher.detach();

    while (true) {
        auto frame = main.receive();
        if (!frame) break;  // the host closed the channel
        msgpack::Reader r(*frame);
        std::uint32_t n = 0;
        std::int32_t op = 0;
        if (!r.read_array_header(n) || !msgpack::read_value(r, op)) break;
        Bytes answer;
        switch (static_cast<ipc::Op>(op)) {
            case ipc::Op::load_module: {
                std::string hash;
                Bytes bytes;
                if (!msgpack::read_value(r, hash) || !msgpack::read_value(r, bytes)) {
                    answer = error_frame("malformed load_module");
                    break;
                }
                auto module = wasm::Module::load(std::move(bytes), wasm::ImportPolicy::system());
                if (!module) {
                    answer = error_frame(module.error());
                } else if ((*module)->hash_hex() != hash) {
                    answer = error_frame("module hash mismatch");
                } else {
                    modules[hash] = std::move(*module);
                    answer = ok_frame();
                }
                break;
            }
            case ipc::Op::create: {
                std::string paglet;
                std::string hash;
                wasm::Limits limits;
                std::optional<Bytes> image;
                if (!msgpack::read_value(r, paglet) || !msgpack::read_value(r, hash) ||
                    !msgpack::read_value(r, limits.stack_size) || !msgpack::read_value(r, limits.max_memory_pages) ||
                    !msgpack::read_value(r, image)) {
                    answer = error_frame("malformed create");
                    break;
                }
                auto module = modules.find(hash);
                if (module == modules.end()) {
                    answer = error_frame("module not loaded in the worker");
                    break;
                }
                std::expected<std::unique_ptr<wasm::Instance>, std::string> inst = std::unexpected(std::string());
                if (image) {
                    auto snap = wasm::deserialize(*image);
                    if (!snap) {
                        answer = error_frame(snap.error());
                        break;
                    }
                    inst = wasm::restore(module->second, *snap, limits);
                } else {
                    inst = wasm::Instance::create(module->second, limits);
                }
                if (!inst) {
                    answer = error_frame(inst.error());
                    break;
                }
                WorkerInstance wi{std::make_unique<ForwardedImports>(main), std::move(*inst)};
                wi.instance->set_imports(wi.imports.get());
                std::lock_guard lock(mu);
                instances[paglet] = std::move(wi);
                answer = ok_frame();
                break;
            }
            case ipc::Op::call: {
                std::string paglet;
                std::string export_name;
                std::vector<std::uint32_t> lead;
                Bytes data;
                if (!msgpack::read_value(r, paglet) || !msgpack::read_value(r, export_name) ||
                    !msgpack::read_value(r, lead) || !msgpack::read_value(r, data)) {
                    answer = error_frame("malformed call");
                    break;
                }
                wasm::Instance* inst = nullptr;
                {
                    std::lock_guard lock(mu);
                    if (auto it = instances.find(paglet); it != instances.end()) inst = it->second.instance.get();
                }
                if (inst == nullptr) {
                    answer = error_frame("no such instance in the worker");
                    break;
                }
                auto result = inst->call_with_data(export_name, lead, data);
                if (!result) {
                    answer = error_frame(result.error());
                } else {
                    msgpack::Writer w;
                    w.write_array_header(2);
                    w.write_int(0);
                    w.write_int(*result);
                    answer = w.take();
                }
                break;
            }
            case ipc::Op::capture: {
                std::string paglet;
                if (!msgpack::read_value(r, paglet)) {
                    answer = error_frame("malformed capture");
                    break;
                }
                std::lock_guard lock(mu);
                auto it = instances.find(paglet);
                if (it == instances.end()) {
                    answer = error_frame("no such instance in the worker");
                    break;
                }
                auto snap = wasm::capture(*it->second.instance);
                if (!snap) {
                    answer = error_frame(snap.error());
                    break;
                }
                msgpack::Writer w;
                w.write_array_header(2);
                w.write_int(0);
                w.write_bin(wasm::serialize(*snap));
                answer = w.take();
                break;
            }
            case ipc::Op::destroy: {
                std::string paglet;
                msgpack::read_value(r, paglet);
                std::lock_guard lock(mu);
                instances.erase(paglet);
                answer = ok_frame();
                break;
            }
            default: answer = error_frame("unknown operation"); break;
        }
        if (!main.send(answer)) break;
    }
    // Leave without running destructors: the watcher thread may still block
    // in receive, and the host no longer needs anything from this process.
    std::_Exit(0);
}

}  // namespace paglets::runtime
