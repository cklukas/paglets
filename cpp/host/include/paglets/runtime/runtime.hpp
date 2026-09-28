// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Single-host paglet runtime (milestone M1): paglet registry and lifecycle,
// priority mailboxes with serial handling per paglet, a scheduler thread
// pool, capability tables, request/reply with one-shot reply capabilities,
// timers, handler time budgets, and memory images for deactivation, clones
// and checkpoints, with recovery after a restart. Paglets run in worker
// processes (one per scheduler lane) or, without a worker executable, in the
// host process (plan, section 3.4).

#pragma once

#include <paglets/runtime/capability.hpp>
#include <paglets/runtime/mobility.hpp>
#include <paglets/runtime/modules.hpp>
#include <paglets/runtime/system.hpp>
#include <paglets/wasm/engine.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::runtime {

using Bytes = std::vector<std::uint8_t>;
using PagletId = std::string;

enum class TrustClass { roaming, resident, system };
std::string_view to_string(TrustClass trust);

enum class PagletState { active, inactive };
std::string_view to_string(PagletState state);

struct LogRecord {
    std::int32_t level = 1;  // abi::LogLevel
    PagletId paglet;         // empty for host messages
    std::string text;
};
using LogFunction = std::function<void(const LogRecord&)>;

struct Config {
    std::string host_name = "local";
    unsigned threads = 2;  // scheduler lanes; a paglet stays on one lane while active
    // Durable state (modules, images, paglet records). Empty: in memory only.
    std::filesystem::path state_dir;
    // Worker process executable (paglets-worker): one worker per lane runs
    // the paglet instances. Empty: paglets run in the host process.
    std::filesystem::path worker_executable;
    // OS sandbox for worker processes (Linux: seccomp; elsewhere not yet).
    bool sandbox_workers = true;
    wasm::Limits limits{};
    std::chrono::milliseconds handler_budget{5000};
    // With a state directory: checkpoint an active paglet after a handler
    // when its last image is older than this (0: after every handler).
    std::chrono::milliseconds checkpoint_interval{1000};
    std::size_t mailbox_limit = 1024;
    std::size_t cap_limit = 1024;
    std::size_t timer_limit = 64;
    std::size_t spawn_limit_per_call = 16;
    // Module store (modules.hpp): compiled modules kept without users, and
    // garbage collection of modules without users that were not used within
    // the grace period, every interval (0: only on request).
    std::size_t module_cache_bytes = 64u * 1024 * 1024;
    // How long this host remembers where a departed paglet went, to forward
    // messages to it (planning/cpp-networking.md, section 4).
    std::chrono::milliseconds tombstone_ttl{std::chrono::minutes(10)};
    std::chrono::milliseconds module_gc_grace{std::chrono::hours(24)};
    std::chrono::milliseconds module_gc_interval{std::chrono::minutes(10)};
    LogFunction log;  // default: stderr
};

struct CreateOptions {
    Bytes args;
    TrustClass trust = TrustClass::roaming;
    std::string owner = "local";
    // A paglet ID chosen in advance (32 lowercase hex digits), for example
    // one a passport or an early grant approval names. Default: a new one.
    std::optional<PagletId> id;
    // Checkpoint policy of this paglet (default: Config::checkpoint_interval);
    // children and clones inherit it.
    std::optional<std::chrono::milliseconds> checkpoint_interval;
};

struct Reply {
    std::int32_t status = 0;  // 0 or an abi::Error
    Bytes payload;
};

struct PagletInfo {
    PagletId id;
    std::string module;  // hex SHA-256
    TrustClass trust = TrustClass::roaming;
    std::string owner;
    PagletState state = PagletState::inactive;
    std::size_t mailbox = 0;
    std::size_t caps = 0;
    std::uint64_t handled = 0;  // handler calls
    int lane = -1;              // scheduler lane while placed, -1 otherwise
};

// Why a paglet ended.
struct Ending {
    PagletId id;
    bool failed = false;  // trapped or exceeded its budget; otherwise disposed
    std::string reason;
};

class Runtime {
public:
    explicit Runtime(Config config);
    ~Runtime();
    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;

    const Config& config() const;

    // Checks the module against the paglet ABI and stores it; returns its
    // hex hash. Adding the same module again is a no-op.
    std::expected<std::string, std::string> add_module(Bytes wasm);
    std::expected<std::string, std::string> add_module_file(const std::filesystem::path& path);
    // The module store (with the state directory: `modules/` in it). Every
    // paglet on this host counts as a user of its module.
    ModuleStore& modules();
    // Hashes of the modules compiled in worker processes (diagnostics).
    std::vector<std::string> worker_modules() const;

    // Creates a paglet; `created` is its first delivery.
    std::expected<PagletId, std::string> create(std::string_view module, CreateOptions options = {});

    // Messages from the host itself (sender "host"). Errors are abi::Error codes.
    std::expected<void, std::int32_t> send(const PagletId& to, std::string_view name, Bytes payload = {},
                                           std::int32_t priority = 3);
    std::future<Reply> request(const PagletId& to, std::string_view name, Bytes payload = {},
                               std::chrono::milliseconds timeout = std::chrono::milliseconds(30000));
    // Blocking request.
    Reply call(const PagletId& to, std::string_view name, Bytes payload = {},
               std::chrono::milliseconds timeout = std::chrono::milliseconds(30000));

    std::expected<void, std::int32_t> deactivate(const PagletId& id);
    std::expected<void, std::int32_t> dispose(const PagletId& id);
    // Ends a paglet as failed without running it again (for example when its
    // module loses the mesh's trust); a handler in progress is terminated.
    std::expected<void, std::int32_t> terminate(const PagletId& id, std::string reason);

    // Decides whether a module may run as a trust class; a mesh host installs
    // the ledger's module trust (planning/cpp-modules.md). Consulted whenever
    // a paglet is created: by create(), and when paglets create children
    // (roaming) and clones (their own class). Called with the runtime's lock
    // held, so it must not call the runtime.
    using ModuleAdmission =
        std::function<std::expected<void, std::string>(const std::string& module, TrustClass trust)>;
    void set_module_admission(ModuleAdmission admission);

    // -- movement between hosts (mobility.hpp, planning/cpp-networking.md) --

    // Enables remote endpoints, dispatch and clones for other hosts. Without
    // hooks, dispatch answers `unsupported`.
    void set_mobility(MobilityHooks hooks);
    // A message from another host. Returns abi::ok, or why it was refused
    // (requests are answered then).
    std::int32_t deliver_remote(RemoteMessage message);
    // The outcome of a departure: the host that has the paglet now, or
    // nullopt if it stays here (it then receives `paglets.move_failed`).
    void finish_departure(std::uint64_t move, std::optional<std::string> host, std::string reason = {});
    // An arriving paglet, created without running (messages to it wait).
    // Returns the capabilities that could not be re-created here, as
    // descriptions for its `arrived` event.
    std::expected<std::vector<std::string>, std::string> prepare_arrival(Arrival arrival, const RecreateCap& recreate);
    // `activate` false: the paglet stays inactive until a message arrives
    // (it then receives `arrived` first).
    std::expected<void, std::string> commit_arrival(const PagletId& id, bool activate = true);
    void abort_arrival(const PagletId& id);
    // Asks a paglet to move (an admin or tool on this host); it dispatches
    // after its current handler, receiving `dispatching` first.
    std::expected<void, std::int32_t> dispatch(const PagletId& id, std::string destination);
    // Where a paglet that left this host went (key ID of the host), while
    // this host remembers.
    std::optional<std::string> location(const PagletId& id) const;
    // Children and local clones created since the last call, for passport
    // links (the mesh takes them regularly; at most 10 000 are kept).
    std::vector<Spawn> take_spawns();

    // Paglets of prepared, uncommitted arrivals are not listed.
    std::optional<PagletInfo> info(const PagletId& id) const;
    std::vector<PagletInfo> list() const;
    std::optional<Ending> ending(const PagletId& id) const;

    // Host directories of a paglet, created on first use and removed when
    // the paglet ends: durable storage (needs a state directory; survives
    // restarts) and scratch space (the whole scratch root is cleared when
    // the host starts). Paglets reach them through system paglets.
    std::expected<std::filesystem::path, std::int32_t> storage_dir(const PagletId& id);
    std::expected<std::filesystem::path, std::int32_t> scratch_dir(const PagletId& id);

    // Process IDs of the running worker processes (none in-process).
    std::vector<int> worker_processes() const;

    // Registers a native system paglet (system.hpp) under `system.<name>`.
    // Paglets created afterwards receive its default endpoint, if it has
    // one; register system paglets before creating paglets.
    std::expected<PagletId, std::string> add_system_paglet(std::shared_ptr<SystemPaglet> paglet);
    std::optional<PagletId> system_paglet(std::string_view name) const;
    std::shared_ptr<SystemPaglet> system_paglet_object(std::string_view name) const;
    std::vector<std::string> system_paglet_names() const;  // sorted

    // Capabilities of a paglet (tests, tools and the host's grant handling).
    std::vector<std::pair<std::int32_t, Cap>> capabilities(const PagletId& id) const;
    // Adds a capability to a paglet's table and returns its handle.
    std::expected<std::int32_t, std::int32_t> add_capability(const PagletId& id, Cap cap);

    // Revocation (planning/cpp-security-and-communication.md, section 4.5).
    // Revoking a capability ID invalidates it and everything derived from
    // it, in every table (stored with the state directory). Grants are
    // revoked in the ledger; the host passes the current set.
    void revoke_capability(const std::string& id);
    void set_revoked_grants(std::set<std::string, std::less<>> grants);

    // Waits until no paglet has pending deliveries or is running (timers
    // that have not fired yet do not count). Returns false on timeout.
    bool wait_idle(std::chrono::milliseconds timeout = std::chrono::milliseconds(30000));

    // Stops the scheduler: running handlers finish; queued messages are
    // lost. With a state directory and `save_state`, the latest state of
    // every active paglet is stored; without it only the last checkpoints
    // remain, as after a crash. Called by the destructor (saving state).
    void shutdown(bool save_state = true);

    struct Impl;  // internal

private:
    std::unique_ptr<Impl> impl_;
};

// Static checks of an ABI v1 paglet module (planning/cpp-abi-v1.md,
// sections 1 to 3): version marker, required exports, import set of the
// trust class, exported mutable globals.
std::expected<void, std::string> check_paglet_module(const wasm::ModuleInfo& info, TrustClass trust);

}  // namespace paglets::runtime
