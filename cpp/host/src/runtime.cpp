// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/runtime/mobility.hpp>
#include <paglets/runtime/modules.hpp>
#include <paglets/runtime/runtime.hpp>

#include "runtime_exec.hpp"
#include "runtime_jobs.hpp"
#include "runtime_store.hpp"

#include <paglets/abi.hpp>
#include <paglets/runtime/system.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wasm/snapshot.hpp>

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>

namespace paglets::runtime {

namespace {

using SteadyClock = std::chrono::steady_clock;

std::int64_t wall_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::chrono::milliseconds bounded_delay(std::int64_t ms) {
    const auto limit = std::chrono::duration_cast<std::chrono::milliseconds>(SteadyClock::duration::max() / 4);
    return std::chrono::milliseconds(std::clamp<std::int64_t>(ms, 0, limit.count()));
}

std::chrono::milliseconds delay_until(std::int64_t deadline) {
    const auto now = wall_ms();
    if (deadline <= now) return std::chrono::milliseconds(0);
    // Unsigned subtraction also handles extreme persisted values without
    // signed overflow, before narrowing to the scheduler's clock horizon.
    const auto remaining = static_cast<std::uint64_t>(deadline) - static_cast<std::uint64_t>(now);
    const auto limit = bounded_delay(std::numeric_limits<std::int64_t>::max()).count();
    return std::chrono::milliseconds(std::min(remaining, static_cast<std::uint64_t>(limit)));
}

std::int64_t wall_deadline(std::int64_t ms) {
    const auto now = wall_ms();
    return now + bounded_delay(ms).count();
}

PagletId new_paglet_id() {
    std::array<std::uint8_t, 16> bytes{};
    wasm::fill_random(bytes);
    return to_hex(std::span<const std::uint8_t>(bytes));
}

// IDs of endpoint and resource capabilities (the revocation tree).
std::string new_cap_id() {
    std::array<std::uint8_t, 16> bytes{};
    wasm::fill_random(bytes);
    return to_hex(std::span<const std::uint8_t>(bytes));
}

// A relative path below a directory capability: segments separated by `/`,
// none of them empty, `.`, `..` or containing `\` or `:`.
std::optional<std::string> normalize_relative(std::string_view path) {
    std::string out;
    std::size_t start = 0;
    while (start <= path.size()) {
        const std::size_t end = std::min(path.find('/', start), path.size());
        const std::string_view seg = path.substr(start, end - start);
        if (seg.empty() || seg == "." || seg == ".." || seg.find_first_of("\\:") != std::string_view::npos ||
            seg.find('\0') != std::string_view::npos) {
            return std::nullopt;
        }
        if (!out.empty()) out += '/';
        out += seg;
        start = end + 1;
    }
    return out;
}

// Priority of lifecycle deliveries: above every message priority, so they
// run before messages that were queued earlier.
constexpr std::int32_t control_priority = abi::max_priority + 1;
constexpr std::int32_t max_forwards = 4;

bool op_allowed(const std::vector<std::string>& ops, std::string_view name) {
    return std::ranges::any_of(ops, [&](const std::string& op) { return op == "*" || op == name; });
}

}  // namespace

std::string_view to_string(TrustClass trust) {
    switch (trust) {
        case TrustClass::roaming: return "roaming";
        case TrustClass::resident: return "resident";
        case TrustClass::system: return "system";
    }
    return "roaming";
}

std::string_view to_string(PagletState state) {
    return state == PagletState::active ? "active" : "inactive";
}

std::expected<void, std::string> check_paglet_module(const wasm::ModuleInfo& info, TrustClass trust) {
    // Version marker: the highest supported version wins; v1 is the only one.
    bool has_v1 = false;
    bool has_other = false;
    for (const auto& e : info.exports) {
        if (e.name == abi::version_marker && e.kind == wasm::ExternKind::function) has_v1 = true;
        if (e.name.starts_with("paglets_abi_v") && e.name != abi::version_marker) has_other = true;
    }
    if (!has_v1) {
        return std::unexpected(has_other ? "module declares no supported paglet ABI version (host supports v1)"
                                         : "module is not a paglet: export paglets_abi_v1 missing");
    }
    for (const auto name : abi::required_exports) {
        const auto* e = info.find_export(name);
        const auto expected = name == "memory" ? wasm::ExternKind::memory : wasm::ExternKind::function;
        if (e == nullptr || e->kind != expected) {
            return std::unexpected("paglet module lacks the required export or has the wrong kind: " + std::string(name));
        }
    }
    const auto policy = trust == TrustClass::system ? wasm::ImportPolicy::system() : wasm::ImportPolicy::standard();
    if (auto ok = policy.check(info); !ok) return std::unexpected(ok.error());
    if (auto ok = wasm::check_snapshot_ready(info); !ok) return std::unexpected(ok.error());
    return {};
}

// ---------------------------------------------------------------------------
// Internal state

namespace {

struct Envelope {
    // `ended` only goes to system paglets: a paglet (ended_id) ended.
    enum class Type { start, message, wake, deactivate, dispose, ended, move };
    Type type = Type::message;
    abi::EventKind event = abi::EventKind::created;  // start
    Bytes args;                                      // start
    std::optional<std::string> original;             // start (clones)
    abi::Delivered delivered;                        // message; caps are installed on delivery
    std::vector<Cap> caps;                           // transferred capabilities
    std::vector<std::string> residency_marks;        // host-controlled result provenance
    std::optional<RemoteMessage::FailureRoute> failure;  // asynchronous sends held during a move
    std::optional<Cap> reply_cap;                    // requests
    std::int64_t deadline_ms = 0;
    std::optional<SteadyClock::time_point> expires_at;
    std::int64_t wake_at_ms = 0;  // fences a queued wake against a later sleep
    std::vector<Cap> lent;                           // system paglets: capabilities lent for this message
    PagletId ended_id;                               // ended
    std::optional<abi::ArrivedEvent> arrived;        // start (arrivals)
    std::string destination;                         // move (Runtime::dispatch)
};

struct PendingClone {
    PagletId id;
    Bytes args;
    std::vector<Cap> caps;
    std::optional<std::string> destination;  // a clone for another host
};

struct PagletRec {
    PagletId id;
    std::string module;                    // hex hash; empty for native system paglets
    std::shared_ptr<wasm::Module> loaded;  // the compiled module while an instance exists
    std::shared_ptr<SystemPaglet> native;
    std::unique_ptr<SystemContext> context;
    TrustClass trust = TrustClass::roaming;
    std::string owner;

    std::unique_ptr<Placed> instance;     // in this process or in the lane's worker
    std::optional<wasm::Snapshot> image;  // memory-only images and clone seeds
    bool image_on_disk = false;
    bool started = false;         // `created` / `cloned` delivered
    bool awaiting_image = false;  // a clone before its image exists

    std::map<std::int32_t, Cap> caps;
    std::int32_t next_handle = 2;

    std::map<std::pair<std::int32_t, std::uint64_t>, Envelope> mailbox;  // (-priority, seq)
    std::uint64_t next_seq = 0;
    bool running = false;
    bool queued = false;
    int lane = -1;       // scheduler lane while placed (active or about to be activated)
    int lane_hint = -1;  // lane of a paglet it talks to (its creator, or a recent sender)

    std::set<std::uint64_t> pending_requests;
    // Compute jobs it waits for, by the correlation of their result: the
    // job on this host's pool, or a remote one (job_status answers from
    // `status`, which the mesh updates).
    struct PendingJob {
        std::uint64_t id = 0;  // in the job pool; 0 for a remote job
        bool remote = false;
        bool finish = false;  // on departure: finish (the result follows it), else cancel
        abi::JobStatus status;
    };
    std::map<std::uint64_t, PendingJob> jobs;
    std::uint64_t next_correlation = 1;
    std::uint64_t next_timer = 1;

    // Lifecycle operations requested during the current handler call.
    std::optional<abi::LifecycleOp> pending_end;
    bool in_end_hook = false;  // no nested lifecycle operations or spawns
    std::optional<std::int64_t> wake_after_ms;
    std::int64_t wake_at_ms = 0;
    std::vector<PendingClone> clones;
    std::size_t spawned_this_call = 0;

    bool budget_exceeded = false;
    std::optional<std::string> kill_reason;   // Runtime::terminate while running
    SteadyClock::time_point call_deadline{};  // of the guest call in progress
    std::uint64_t handled = 0;
    SteadyClock::time_point last_checkpoint{};
    std::optional<std::chrono::milliseconds> checkpoint_interval;  // per paglet; default from the config
    bool checkpoint_due = false;                                   // no stored image yet
    bool checkpoint_requested = false;  // explicit request in the current handler
    std::unique_ptr<wasm::HostImports> imports;
    std::vector<std::pair<std::string, std::int32_t>> services;  // default service endpoints

    // Movement (mobility.hpp).
    std::optional<std::string> dispatch_to;  // destination of a pending dispatch
    bool moving = false;                     // departed, outcome pending: messages are held
    bool dispatch_pending = false;           // accepted, including a queued host request and its hook
    bool arriving = false;                   // prepared here, not yet committed
    bool dormant = false;                    // arrived inactive: runs when a message comes
    std::optional<Envelope> arrival_start;   // its first delivery, on commit
    std::optional<std::pair<std::string, std::uint64_t>> arrival_receipt;
    std::uint64_t arrival_activation_ns = 0;
    // Pins (the mesh's locator): pin ID -> end (Unix milliseconds). While
    // one has not ended the paglet stays on this host.
    std::map<std::string, std::int64_t> pins;
    // Data residency marks: kept for good, inherited by children and clones,
    // carried along when it moves (planning/cpp-residency.md).
    std::set<std::string> marks;
    // Restart policy (abi::RestartArg): without `restart` the paglet is
    // never stored and ends with its host process.
    bool restart = true;
    bool notify_recovered = false;
    std::int64_t image_ms = 0;  // when its last image was stored (Unix milliseconds)

    // The end of the last pin, or 0 when the paglet is not pinned.
    std::int64_t pinned_until() {
        const std::int64_t now = wall_ms();
        std::erase_if(pins, [&](const auto& p) { return p.second <= now; });
        std::int64_t until = 0;
        for (const auto& [pin, end] : pins) until = std::max(until, end);
        return until;
    }

    const std::string& module_hash() const { return module; }
    abi::SenderRecord sender(const std::string& host) const {
        return {id, owner, std::string(to_string(trust)), module_hash(), host};
    }
};

struct Deadline {
    enum class Type { timer, request_timeout, wake };
    Type type = Type::timer;
    PagletId paglet;  // empty: the host (external requests)
    std::uint64_t id = 0;
};

}  // namespace

struct Runtime::Impl {
    explicit Impl(Config c) : config(std::move(c)) {}

    Config config;
    mutable std::mutex mu;
    std::condition_variable idle_cv;
    std::condition_variable clock_cv;
    bool stopping = false;

    std::unique_ptr<ModuleStore> modules;
    Runtime::ModuleAdmission admission;

    // Movement between hosts (mobility.hpp).
    MobilityHooks mobility;
    struct Tombstone {
        std::string host;
        SteadyClock::time_point expires;
    };
    std::map<PagletId, Tombstone> tombstones;  // paglets that left, and where to
    struct Moving {
        PagletId paglet;  // the departing paglet, or the original of a clone
        PagletId clone;   // clones only
        std::string destination;

        std::set<std::uint64_t> captured_ready;
    };
    std::map<std::uint64_t, Moving> departures;
    std::uint64_t next_move = 1;
    std::vector<Departure> outgoing;  // for the depart hook, outside the lock
    std::deque<Spawn> spawns;         // children and local clones, for the mesh

    void record_spawn(const PagletId& parent, const PagletId& child, std::string kind, const std::string& module) {
        if (spawns.size() >= 10000) spawns.pop_front();
        spawns.push_back(Spawn{parent, child, std::move(kind), module});
    }
    SteadyClock::time_point next_module_gc{};
    std::map<PagletId, std::unique_ptr<PagletRec>> paglets;
    std::map<PagletId, Ending> endings;
    // A lane is one scheduler thread with its own ready queue. A paglet stays
    // on the lane it was activated on until its instance is released, so its
    // WAMR execution environment never changes threads.
    struct Lane {
        std::deque<PagletId> ready;
        std::condition_variable cv;
        std::size_t placed = 0;
        std::unique_ptr<Executor> executor;
        std::thread thread;
    };
    bool uses_workers = false;
    std::vector<std::unique_ptr<Lane>> lanes;
    std::size_t running_count = 0;

    std::multimap<SteadyClock::time_point, Deadline> deadlines;
    std::set<PagletRec*> in_call;  // guest calls in progress, checked by the clock thread

    std::map<std::uint64_t, std::promise<Reply>> external;  // host requests by correlation
    std::uint64_t next_external = 1;

    // Native system paglets by service name (IDs `system.<name>`).
    std::map<std::string, PagletId, std::less<>> system_paglets;
    // Revocation (capability IDs revoked here, grants revoked in the ledger).
    std::set<std::string, std::less<>> revoked_ids;
    std::set<std::string, std::less<>> revoked_grants;

    std::optional<Store> store;

    // Compute jobs (runtime_jobs.hpp).
    std::unique_ptr<JobPool> job_pool;
    JobHooks job_hooks;
    JobArtifacts job_artifacts;
    // Calls that must not run under `mu` (hooks into the mesh and the
    // services), made in order on a thread of their own.
    std::deque<std::function<void()>> deferred;
    std::condition_variable deferred_cv;
    std::thread deferred_thread;
    bool deferred_stop = false;
    bool deferred_running = false;
    void defer(std::function<void()> f) {  // mu held
        deferred.push_back(std::move(f));
        deferred_cv.notify_all();
    }
    void run_deferred();
    void job_done(JobOutcome outcome);
    void cancel_jobs(PagletRec& rec, std::int32_t status, bool departing);
    std::expected<std::uint64_t, std::int32_t> start_job(IncomingJob job);
    void start_job_from_artifact(IncomingJob job);
    std::filesystem::path storage_root;  // empty without a state directory
    std::filesystem::path work_root;
    bool own_work_root = false;  // a temporary root, removed at shutdown

    void remove_directories(const PagletId& id) {
        std::error_code ec;
        if (!storage_root.empty()) std::filesystem::remove_all(storage_root / id, ec);
        std::filesystem::remove_all(work_root / id, ec);
    }
    std::thread clock;

    // -- helpers (mu held) --------------------------------------------------

    void log(std::int32_t level, const PagletId& paglet, std::string text) const {
        LogRecord r{level, paglet, std::move(text)};
        if (config.log) {
            config.log(r);
        } else {
            std::cerr << "[" << (paglet.empty() ? std::string("host") : paglet.substr(0, 8)) << "] " << r.text << "\n";
        }
    }

    PagletRec* find(const PagletId& id) {
        auto it = paglets.find(id);
        return it == paglets.end() ? nullptr : it->second.get();
    }

    const std::string& identity_host() const { return mobility.host_id.empty() ? config.host_name : mobility.host_id; }
    abi::SenderRecord host_sender(std::string_view owner) const {
        return {"host", std::string(owner), "host", "", identity_host()};
    }

    bool is_system(const PagletId& id) const {
        auto it = paglets.find(id);
        return it != paglets.end() && it->second->native != nullptr;
    }

    // Where a paglet that is not here lives: the capability's host hint, else
    // where it went. Empty if unknown.
    std::string remote_host(const PagletId& id, const std::string& hint) {
        if (!hint.empty() && hint != mobility.host_id) return hint;
        auto t = tombstones.find(id);
        if (t == tombstones.end()) return {};
        if (SteadyClock::now() >= t->second.expires) {
            tombstones.erase(t);
            return {};
        }
        return t->second.host;
    }

    // A capability as it leaves this host: endpoints and reply capabilities
    // name the host of their target.
    Cap portable(Cap c) {
        if (c.kind != Cap::Kind::endpoint && c.kind != Cap::Kind::reply) return c;
        if (c.kind == Cap::Kind::reply && c.target.empty()) {
            if (c.host.empty()) c.host = mobility.host_id;  // the host itself asked
            return c;
        }
        if (paglets.contains(c.target)) {
            c.host = is_system(c.target) ? std::string() : mobility.host_id;
        } else if (c.host.empty()) {
            c.host = remote_host(c.target, {});
        }
        return c;
    }

    // Capabilities a message to another host may carry: endpoints to
    // paglets, and artifacts that name the host holding their content.
    bool portable_cap(const Cap& cap) const {
        return (cap.kind == Cap::Kind::endpoint && !cap.target.starts_with("system.")) || mesh_artifact(cap);
    }

    bool portable_handles(const PagletRec& rec, const std::vector<std::int32_t>& handles) const {
        return std::ranges::all_of(handles, [&](std::int32_t h) {
            auto it = rec.caps.find(h);
            return it == rec.caps.end() || portable_cap(it->second);
        });
    }

    std::vector<Cap> portable_caps(std::vector<Cap> caps) {
        std::vector<Cap> out;
        for (auto& c : caps) {
            if (portable_cap(c)) {
                out.push_back(portable(std::move(c)));
            } else {
                drop_cap(std::move(c), abi::gone);
            }
        }
        return out;
    }

    void send_remote(RemoteMessage m) {
        if (m.kind == RemoteMessage::Kind::request && m.timeout_ms > 0 && !m.expires_at)
            m.expires_at = SteadyClock::now() + bounded_delay(m.timeout_ms);
        if (mobility.deliver) mobility.deliver(std::move(m));
    }

    void move_failed(PagletRec& rec, std::string destination, std::string reason, std::string clone) {
        if (clone.empty() && mobility.moved) mobility.moved(rec.id, std::nullopt, reason);
        Envelope env;
        env.delivered.kind = abi::MessageKind::message;
        env.delivered.name = std::string(abi::move_failed_message);
        env.delivered.payload =
            abi::encode(abi::MoveFailed{std::move(destination), std::move(reason), std::move(clone)});
        env.delivered.priority = abi::max_priority;
        enqueue(rec, std::move(env), abi::max_priority);
    }

    // The travelling state of a paglet (mu held).
    struct ReadyCapture {
        std::vector<ReadyMessage> messages;
        std::set<std::uint64_t> sequences;
    };
    static bool host_report(std::string_view name) {
        return name == abi::undelivered_message || name == abi::recovered_message || name == abi::move_failed_message;
    }
    std::expected<ReadyCapture, std::string> capture_ready(const PagletRec& rec) {
        ReadyCapture captured;
        std::size_t bytes = 0;
        for (const auto& [key, env] : rec.mailbox) {
            if (env.type != Envelope::Type::message) continue;
            const auto kind = env.delivered.kind;
            if (kind != abi::MessageKind::reply && kind != abi::MessageKind::timer &&
                !(kind == abi::MessageKind::message && host_report(env.delivered.name))) continue;
            if (env.delivered.payload.size() > config.move_ready_bytes - bytes)
                return std::unexpected("ready notifications exceed the move byte budget");
            ReadyMessage message{env.delivered, {}, env.residency_marks};
            message.delivered.caps.clear();
            message.delivered.reply = 0;
            if (std::ranges::all_of(env.caps, [&](const Cap& c) { return portable_cap(c); })) {
                for (const auto& cap : env.caps) message.caps.push_back(portable(cap));
            } else {
                message.delivered.payload.clear();
                message.delivered.status = abi::unsupported;
            }
            const auto size = encode_ready(message).size() + 5;  // outer binary header
            if (size > config.move_ready_bytes - bytes)
                return std::unexpected("ready notifications exceed the move byte budget");
            bytes += size;
            captured.sequences.insert(key.second);
            captured.messages.push_back(std::move(message));
        }
        return captured;
    }

    TravelState state_of(const PagletRec& rec) {
        TravelState st;
        st.id = rec.id;
        st.module = rec.module;
        st.trust = std::string(to_string(rec.trust));
        st.owner = rec.owner;
        st.next_handle = rec.next_handle;
        st.next_correlation = rec.next_correlation;
        st.next_timer = rec.next_timer;
        st.checkpoint_ms = rec.checkpoint_interval ? rec.checkpoint_interval->count() : -1;
        st.services = rec.services;
        st.marks.assign(rec.marks.begin(), rec.marks.end());
        st.restart = rec.restart;
        st.notify_recovered = rec.notify_recovered;
        st.wake_at_ms = rec.wake_at_ms;
        for (const auto& [h, c] : rec.caps) st.caps.emplace_back(h, portable(c));
        const auto now = SteadyClock::now();
        const auto captured_ms = wall_ms();
        for (const auto& [at, d] : deadlines) {
            if (d.type == Deadline::Type::request_timeout && d.paglet == rec.id &&
                rec.pending_requests.contains(d.id)) {
                st.pending.emplace_back(
                    d.id,
                    std::max<std::int64_t>(0, std::chrono::duration_cast<std::chrono::milliseconds>(at - now).count()));
            }
        }
        if (!st.pending.empty()) st.pending_captured_ms = captured_ms;
        return st;
    }

    bool revoked(const Cap& c) const {
        if (c.grant && revoked_grants.contains(*c.grant)) return true;
        if (!c.id.empty() && revoked_ids.contains(c.id)) return true;
        return std::ranges::any_of(c.lineage, [&](const std::string& id) { return revoked_ids.contains(id); });
    }

    // abi::ok, revoked or expired.
    std::int32_t usable(const Cap& c) const {
        if (revoked(c)) return abi::revoked;
        if (c.expires && wall_ms() >= *c.expires) return abi::expired;
        return abi::ok;
    }

    void schedule(SteadyClock::time_point at, Deadline d) {
        const bool earliest = deadlines.empty() || at < deadlines.begin()->first;
        deadlines.emplace(at, std::move(d));
        if (earliest) clock_cv.notify_all();
    }

    void schedule_wake(PagletRec& rec) {
        if (rec.wake_at_ms <= 0) return;
        const auto delay = delay_until(rec.wake_at_ms);
        schedule(SteadyClock::now() + delay,
                 Deadline{Deadline::Type::wake, rec.id, static_cast<std::uint64_t>(rec.wake_at_ms)});
    }

    void make_ready(PagletRec& rec) {
        if (rec.running || rec.queued || rec.awaiting_image || rec.moving || rec.arriving || rec.mailbox.empty()) {
            return;
        }
        if (rec.dormant && rec.mailbox.size() < 2) return;  // only its `arrived` event waits
        if (lanes.empty()) return;  // recovery, before the scheduler starts: readied then
        if (rec.lane < 0) {
            // Place next to the paglet it talks to, unless that lane is
            // clearly busier than the least loaded one; otherwise on the
            // least loaded lane.
            std::size_t best = 0;
            for (std::size_t i = 1; i < lanes.size(); ++i) {
                if (lanes[i]->placed < lanes[best]->placed) best = i;
            }
            if (rec.lane_hint >= 0 && static_cast<std::size_t>(rec.lane_hint) < lanes.size()) {
                const std::size_t slack = 1 + lanes[best]->placed / 4;
                const auto hint = static_cast<std::size_t>(rec.lane_hint);
                if (lanes[hint]->placed <= lanes[best]->placed + slack) best = hint;
            }
            rec.lane = static_cast<int>(best);
            ++lanes[best]->placed;
        }
        rec.queued = true;
        Lane& lane = *lanes[static_cast<std::size_t>(rec.lane)];
        lane.ready.push_back(rec.id);
        lane.cv.notify_one();
    }

    // Instances end outside the runtime lock (a worker instance talks to its
    // worker process, whose calls take the runtime lock for imports).
    std::vector<std::unique_ptr<Placed>> retired;
    void retire(PagletRec& rec) {
        if (rec.instance) retired.push_back(std::move(rec.instance));
        rec.loaded.reset();
    }

    // The module admission check (mu held); empty if the module may run.
    std::string refusal(const std::string& module, TrustClass trust) const {
        if (!admission) return {};
        auto ok = admission(module, trust);
        return ok ? std::string() : ok.error();
    }

    // Removes a paglet record; its module loses a user.
    void erase_paglet(const PagletId& id) {
        auto it = paglets.find(id);
        if (it == paglets.end()) return;
        if (!it->second->module.empty()) modules->remove_user(it->second->module);
        paglets.erase(it);
    }

    // Releases the lane of a paglet whose instance is gone.
    void release_lane(PagletRec& rec) {
        if (rec.lane >= 0 && !rec.instance) {
            --lanes[static_cast<std::size_t>(rec.lane)]->placed;
            rec.lane = -1;
        }
    }

    void enqueue(PagletRec& rec, Envelope env, std::int32_t priority) {
        rec.mailbox.emplace(std::pair{-priority, rec.next_seq++}, std::move(env));
        make_ready(rec);
    }

    std::int32_t add_cap(PagletRec& rec, Cap cap) {
        const std::int32_t h = rec.next_handle++;
        rec.caps.emplace(h, std::move(cap));
        return h;
    }

    // Delivers the one reply of a request. Late replies are discarded.
    // `host`: where the requester is, if it is not here.
    void deliver_reply(const PagletId& requester, std::uint64_t correlation, std::int32_t status, Bytes payload,
                       std::vector<Cap> caps, const std::string& host = {},
                       std::vector<std::string> residency_marks = {}) {
        const bool here = requester.empty() ? (host.empty() || host == mobility.host_id) : paglets.contains(requester);
        if (!here) {
            const std::string where = requester.empty() ? host : remote_host(requester, host);
            if (!std::ranges::all_of(caps, [&](const Cap& c) { return portable_cap(c); })) {
                for (auto& c : caps) drop_cap(std::move(c), abi::gone);
                caps.clear();
                payload.clear();
                status = abi::unsupported;
            }
            RemoteMessage m;
            m.kind = RemoteMessage::Kind::reply;
            m.target = requester;
            m.host = where;
            m.name = "reply";
            m.payload = std::move(payload);
            m.priority = abi::max_priority;
            m.correlation = correlation;
            m.status = status;
            m.residency_marks = std::move(residency_marks);
            m.caps = portable_caps(std::move(caps));
            if (where.empty() && !requester.empty() && mobility.unresolved) {
                mobility.unresolved(std::move(m));
            } else if (!where.empty() && mobility.deliver) {
                send_remote(std::move(m));
            } else {
                for (auto& c : m.caps) drop_cap(std::move(c), abi::gone);
            }
            return;
        }
        if (requester.empty()) {
            if (auto it = external.find(correlation); it != external.end()) {
                it->second.set_value(Reply{status, std::move(payload)});
                external.erase(it);
            }
            for (auto& c : caps) drop_cap(std::move(c), abi::gone);
            return;
        }
        PagletRec* rec = find(requester);
        if (rec == nullptr || rec->pending_requests.erase(correlation) == 0) {
            for (auto& c : caps) drop_cap(std::move(c), abi::gone);
            return;
        }
        // Provenance is host metadata, not a document supplied by a guest.
        rec->marks.insert(residency_marks.begin(), residency_marks.end());
        rec->jobs.erase(correlation);
        Envelope env;
        env.type = Envelope::Type::message;
        env.delivered.kind = abi::MessageKind::reply;
        env.delivered.name = "reply";
        env.delivered.payload = std::move(payload);
        env.delivered.priority = abi::max_priority;
        env.delivered.correlation = correlation;
        env.delivered.status = status;
        env.caps = std::move(caps);
        env.residency_marks = std::move(residency_marks);
        enqueue(*rec, std::move(env), abi::max_priority);
    }

    // Reports are host notifications, not ordinary remotely supplied reserved
    // messages. They carry no authority and never cause another failure report.
    std::int32_t deliver_undelivered(RemoteMessage m) {
        abi::Undelivered report;
        msgpack::Reader reader(m.payload);
        unsigned fields = 0;
        const bool valid = abi::detail::read_map(reader, [&](std::string_view field) {
            unsigned bit = field == "name" ? 1 : field == "handle" ? 2 : field == "status" ? 4 : 0;
            if (!bit) return reader.skip();
            if (fields & bit) return false;
            fields |= bit;
            if (bit == 1) return msgpack::read_value(reader, report.name);
            if (bit == 2) return msgpack::read_value(reader, report.handle);
            return msgpack::read_value(reader, report.status);
        }) && reader.at_end() && fields == 7;
        // The original name/handle may themselves have caused a local send
        // validation failure. They are data, not capability authority.
        if (m.target.empty() || m.name != abi::undelivered_message || !valid || report.status >= 0 ||
            !m.caps.empty() || m.sender || m.badge || m.failure || !m.residency_marks.empty()) return abi::invalid_argument;
        if (auto* rec = find(m.target)) {
            if (rec->native) return abi::denied;
            Envelope env;
            env.delivered.kind = abi::MessageKind::message;
            env.delivered.name = std::string(abi::undelivered_message);
            env.delivered.payload = std::move(m.payload);
            env.delivered.priority = abi::max_priority;
            enqueue(*rec, std::move(env), abi::max_priority);
            return abi::ok;
        }
        if (m.hops < 0 || m.hops >= max_forwards) return abi::not_found;
        const auto where = remote_host(m.target, m.host);
        if (!where.empty() && where != mobility.host_id) {
            m.host = where;
            ++m.hops;
            send_remote(std::move(m));
        } else if (mobility.unresolved) {
            mobility.unresolved(std::move(m));
        }
        return abi::ok;
    }

    // Releases a capability that leaves every table: an unused reply
    // capability answers its request with `status`.
    void drop_cap(Cap cap, std::int32_t status) {
        if (cap.kind == Cap::Kind::reply) {
            deliver_reply(cap.target, cap.correlation, status, {}, {}, cap.host);
        }
    }

    // Removes capabilities for a transfer; validates all before taking any.
    std::expected<std::vector<Cap>, std::int32_t> take_caps(PagletRec& rec, const std::vector<std::int32_t>& handles) {
        std::set<std::int32_t> seen;
        for (auto h : handles) {
            auto it = rec.caps.find(h);
            if (it == rec.caps.end()) return std::unexpected(abi::bad_handle);
            if (!it->second.transferable) return std::unexpected(abi::denied);
            if (!seen.insert(h).second) return std::unexpected(abi::invalid_argument);
        }
        std::vector<Cap> taken;
        for (auto h : handles) {
            if (h == abi::self_handle) {
                taken.push_back(rec.caps.at(h));  // the own endpoint is copied, never given away
            } else {
                taken.push_back(std::move(rec.caps.at(h)));
                rec.caps.erase(h);
            }
        }
        return taken;
    }

    std::int32_t check_endpoint(Cap& cap, std::string_view name) const {
        if (cap.kind != Cap::Kind::endpoint) return abi::bad_handle;
        if (auto u = usable(cap); u != abi::ok) return u;
        if (cap.uses_left && *cap.uses_left <= 0) return abi::quota;
        if (!op_allowed(cap.ops, name)) return abi::denied;
        return abi::ok;
    }

    static std::int32_t validate(const abi::OutMessage& m) {
        if (!abi::valid_message_name(m.name) || abi::reserved_message_name(m.name)) return abi::invalid_argument;
        if (m.priority < 0 || m.priority > abi::max_priority) return abi::invalid_argument;
        return abi::ok;
    }

    // A new paglet record; nullptr if its module is not in the store (a
    // module a paglet uses cannot be collected).
    // Whether `owner` has as many paglets here as one owner may have.
    bool owner_full(const std::string& owner) const {
        if (config.owner_paglet_limit == 0) return false;
        std::size_t n = 0;
        for (const auto& [id, p] : paglets) {
            if (p->trust != TrustClass::system && p->owner == owner && ++n >= config.owner_paglet_limit) return true;
        }
        return false;
    }

    PagletRec* new_paglet(const PagletId& id, std::string module, TrustClass trust, std::string owner) {
        if (!module.empty() && !modules->add_user(module)) return nullptr;
        auto rec = std::make_unique<PagletRec>();
        rec->id = id;
        rec->module = std::move(module);
        rec->trust = trust;
        rec->owner = std::move(owner);
        Cap self;
        self.kind = Cap::Kind::endpoint;
        self.target = id;
        self.ops = {"*"};
        self.transferable = true;
        self.id = new_cap_id();
        rec->caps.emplace(abi::self_handle, std::move(self));
        PagletRec* ref = rec.get();
        paglets.emplace(id, std::move(rec));
        return ref;
    }

    Cap endpoint_to(const PagletId& id) const {
        Cap c;
        c.kind = Cap::Kind::endpoint;
        c.target = id;
        c.ops = {"*"};
        c.transferable = true;
        c.id = new_cap_id();
        return c;
    }

    // The endpoints to system services every new paglet receives (plan of
    // the security design, section 7.5); not transferable.
    void install_services(PagletRec& rec) {
        for (const auto& [name, id] : system_paglets) {
            PagletRec* sys = find(id);
            if (sys == nullptr || !sys->native) continue;
            auto ops = sys->native->default_ops();
            if (ops.empty()) continue;
            Cap c = endpoint_to(id);
            c.ops = std::move(ops);
            c.transferable = false;
            rec.services.emplace_back(name, add_cap(rec, std::move(c)));
        }
    }

    // Copies of capabilities lent to a system paglet (validated, uses
    // counted).
    std::expected<std::vector<Cap>, std::int32_t> lend_caps(PagletRec& rec, const std::vector<std::int32_t>& handles,
                                                            const PagletRec& target) {
        if (handles.empty()) return std::vector<Cap>{};
        if (!target.native) return std::unexpected(abi::invalid_argument);
        std::set<std::int32_t> seen;
        for (auto h : handles) {
            auto it = rec.caps.find(h);
            if (it == rec.caps.end()) return std::unexpected(abi::bad_handle);
            const Cap& c = it->second;
            if (c.kind != Cap::Kind::endpoint && c.kind != Cap::Kind::resource) return std::unexpected(abi::bad_handle);
            if (auto u = usable(c); u != abi::ok) return std::unexpected(u);
            if (c.uses_left && *c.uses_left <= 0) return std::unexpected(abi::quota);
            if (!seen.insert(h).second) return std::unexpected(abi::invalid_argument);
        }
        std::vector<Cap> lent;
        for (auto h : handles) {
            Cap& c = rec.caps.at(h);
            if (c.kind == Cap::Kind::resource && c.uses_left) --*c.uses_left;
            lent.push_back(c);
        }
        return lent;
    }

    // Tells every system paglet that a paglet ended (in order with its messages).
    void notify_ended(const PagletId& ended) {
        for (const auto& [name, id] : system_paglets) {
            PagletRec* sys = find(id);
            if (sys == nullptr || sys->id == ended) continue;
            Envelope env;
            env.type = Envelope::Type::ended;
            env.ended_id = ended;
            enqueue(*sys, std::move(env), control_priority);
        }
    }

    void enqueue_start(PagletRec& rec, abi::EventKind kind, Bytes args, std::vector<Cap> caps,
                       std::optional<std::string> original) {
        Envelope env;
        env.type = Envelope::Type::start;
        env.event = kind;
        env.args = std::move(args);
        env.caps = std::move(caps);
        env.original = std::move(original);
        enqueue(rec, std::move(env), control_priority);
    }

    void notify_arrival(PagletRec& rec, bool succeeded) {
        auto receipt = std::exchange(rec.arrival_receipt, std::nullopt);
        if (receipt && mobility.arrived)
            mobility.arrived(receipt->first, receipt->second, succeeded, rec.arrival_activation_ns);
    }

    // Ends a paglet (mu held, the paglet is not running): answers queued
    // requests and held reply capabilities, removes it and its stored state.
    void end_paglet(PagletRec& rec, bool failed, std::string reason) {
        notify_arrival(rec, false);
        const std::int32_t status = failed ? abi::failed : abi::gone;
        cancel_jobs(rec, abi::gone, false);
        for (auto& [key, env] : rec.mailbox) {
            if (env.reply_cap) drop_cap(std::move(*env.reply_cap), status);
            for (auto& c : env.caps) drop_cap(std::move(c), status);
        }
        rec.mailbox.clear();
        for (auto& [h, cap] : rec.caps) drop_cap(std::move(cap), status);
        rec.caps.clear();
        for (auto& clone : rec.clones) {
            for (auto& cap : clone.caps) drop_cap(std::move(cap), status);
            erase_paglet(clone.id);
        }
        rec.clones.clear();
        if (store) store->remove_paglet(rec.id);
        remove_directories(rec.id);
        retire(rec);
        release_lane(rec);
        notify_ended(rec.id);
        endings[rec.id] = Ending{rec.id, failed, std::move(reason)};
        if (failed) log(3, rec.id, "paglet failed: " + endings[rec.id].reason);
        erase_paglet(rec.id);  // rec is destroyed here
    }

    bool persist(PagletRec& rec, const wasm::Snapshot& snap) {
        if (!store) return false;
        if (!rec.restart) {
            // Not to come back: nothing stored (an older record goes).
            if (rec.image_on_disk) store->remove_paglet(rec.id);
            rec.image_on_disk = false;
            return true;
        }
        PagletRecord r;
        r.id = rec.id;
        r.module = rec.module_hash();
        r.trust = std::string(to_string(rec.trust));
        r.owner = rec.owner;
        r.started = rec.started;
        r.next_handle = rec.next_handle;
        r.next_correlation = rec.next_correlation;
        r.next_timer = rec.next_timer;
        r.wake_at_ms = rec.wake_at_ms;
        r.caps.assign(rec.caps.begin(), rec.caps.end());
        auto outstanding = rec.pending_requests;
        for (const auto& [key, env] : rec.mailbox)
            if (env.type == Envelope::Type::message && env.delivered.kind == abi::MessageKind::reply)
                outstanding.insert(env.delivered.correlation);
        r.pending_requests.assign(outstanding.begin(), outstanding.end());
        for (const auto& [correlation, job] : rec.jobs) r.pending_jobs.push_back(correlation);
        r.checkpoint_ms = rec.checkpoint_interval ? rec.checkpoint_interval->count() : -1;
        r.services = rec.services;
        r.marks.assign(rec.marks.begin(), rec.marks.end());
        r.restart = rec.restart;
        r.notify_recovered = rec.notify_recovered;
        r.image_ms = wall_ms();
        if (auto ok = store->save_paglet(r, snap); !ok) {
            log(3, rec.id, "persisting the paglet failed: " + ok.error());
            return false;
        } else {
            rec.image_on_disk = true;
            rec.image_ms = r.image_ms;
        }
        return true;
    }

    // -- guest calls (mu not held; rec.running) -----------------------------

    // Runs a guest call under the handler time budget.
    template <class F>
    std::expected<std::int32_t, std::string> guest_call(PagletRec& rec, F&& call) {
        {
            std::lock_guard lock(mu);
            if (rec.kill_reason) return std::unexpected(*rec.kill_reason);
            rec.budget_exceeded = false;
            rec.call_deadline = SteadyClock::now() + config.handler_budget;
            in_call.insert(&rec);
        }
        auto result = call();
        {
            std::lock_guard lock(mu);
            in_call.erase(&rec);
            if (rec.kill_reason) return std::unexpected(*rec.kill_reason);
            if (rec.budget_exceeded) {
                return std::unexpected("handler exceeded its time budget of " +
                                       std::to_string(config.handler_budget.count()) + " ms");
            }
        }
        return result;
    }

    // Delivers a lifecycle event. Error results are logged; they do not stop
    // the transition (ABI v1, section 6).
    std::expected<std::int32_t, std::string> call_event(PagletRec& rec, abi::EventKind kind, const Bytes& doc) {
        auto r = guest_call(rec, [&] {
            const std::array<std::uint32_t, 1> lead{static_cast<std::uint32_t>(kind)};
            return rec.instance->call("paglets_on_event", lead, doc);
        });
        if (r && *r < 0) {
            std::lock_guard lock(mu);
            log(2, rec.id,
                "event " + std::to_string(static_cast<int>(kind)) + " handler returned " +
                    std::string(abi::error_name(*r)));
        }
        return r;
    }

    std::expected<void, std::string> activate(PagletRec& rec, bool deliver_activated);
    bool expired(Envelope& env) {
        if (!env.expires_at || SteadyClock::now() < *env.expires_at) return false;
        std::lock_guard lock(mu);
        if (env.reply_cap) drop_cap(std::move(*env.reply_cap), abi::timeout);
        env.reply_cap.reset();
        for (auto& cap : env.caps) drop_cap(std::move(cap), abi::gone);
        env.caps.clear();
        return true;
    }

    void process_native(PagletRec& rec, Envelope env);
    std::expected<wasm::Snapshot, std::string> snapshot(PagletRec& rec) { return rec.instance->capture(); }
    void process(PagletRec& rec, Envelope env, bool& remove);
    void finish_call(PagletRec& rec, bool& remove, std::unique_lock<std::mutex>& lock);
    void lane_loop(std::size_t index);
    void clock_loop();
    void fire(Deadline d);
    void recover();

    // Garbage collection of the module store (clock thread, without mu).
    void collect_modules() {
        auto r = modules->collect(config.module_gc_grace);
        if (r.removed.empty()) return;
        std::lock_guard lock(mu);
        log(1, "",
            "module store: removed " + std::to_string(r.removed.size()) + " unused modules (" +
                std::to_string((r.bytes + 1023) / 1024) + " KB)");
    }
};

// ---------------------------------------------------------------------------
// Host imports of one paglet

namespace {

class Imports final : public wasm::HostImports {
public:
    Imports(Runtime::Impl& rt, PagletRec& rec) : rt_(rt), rec_(rec) {}

    void log(std::int32_t level, std::string_view text) override {
        std::lock_guard lock(rt_.mu);
        rt_.log(level, rec_.id, std::string(text));
    }

    Document self_info() override {
        std::lock_guard lock(rt_.mu);
        abi::SelfInfo info{
            rec_.id,      rec_.module_hash(), rec_.owner,   std::string(to_string(rec_.trust)), rt_.identity_host(),
            abi::version, abi::minor_version, rec_.services};
        info.pinned_until = rec_.pinned_until();
        info.restart = rec_.restart;
        info.notify_recovered = rec_.notify_recovered;
        return abi::encode(info);
    }

    // Asynchronous sends and replies (ABI v1.1) return at once; a failure is
    // reported to the paglet as `paglets.undelivered`. Worker processes send
    // them as one-way frames, so both paths report the same way.
    std::int32_t send(std::int32_t endpoint, std::span<const std::uint8_t> doc) override {
        abi::OutMessage m;
        if (!abi::decode(doc, m)) return abi::malformed;
        const bool async = m.async;
        std::string name = m.name;
        std::vector<std::int32_t> caps = m.caps;
        const std::int32_t r = send_message(endpoint, std::move(m));
        if (r == abi::ok || !async) return r;
        undelivered(std::move(name), endpoint, r, caps, false);
        return abi::ok;
    }

    std::int32_t send_message(std::int32_t endpoint, abi::OutMessage m) {
        if (auto v = Runtime::Impl::validate(m); v != abi::ok) return v;
        std::lock_guard lock(rt_.mu);
        PagletRec* target = nullptr;
        std::string remote;
        auto cap = checked_endpoint(endpoint, m.name, target, remote);
        if (!cap) return cap.error();
        const auto badge = (*cap)->badge;
        if (target == nullptr) {
            // A paglet on another host: only endpoints travel with the message.
            if (!m.lend.empty()) return abi::invalid_argument;
            if (!rt_.portable_handles(rec_, m.caps)) return abi::unsupported;
            auto target_id = (*cap)->target;
            auto caps = take_endpoint_caps(endpoint, m.caps);
            if (!caps) return caps.error();
            RemoteMessage out;
            out.kind = RemoteMessage::Kind::message;
            if (m.async) out.failure = RemoteMessage::FailureRoute{rec_.id, rt_.identity_host(), endpoint};
            out.target = std::move(target_id);
            out.host = remote;
            out.name = std::move(m.name);
            out.payload = std::move(m.payload);
            out.priority = m.priority;
            out.sender = rec_.sender(rt_.identity_host());
            out.badge = badge;
            out.caps = rt_.portable_caps(std::move(*caps));
            rt_.send_remote(std::move(out));
            return abi::ok;
        }
        auto lent = rt_.lend_caps(rec_, m.lend, *target);
        if (!lent) return lent.error();
        auto caps = take_endpoint_caps(endpoint, m.caps);
        if (!caps) return caps.error();
        Envelope env;
        env.lent = std::move(*lent);
        if (m.async) env.failure = RemoteMessage::FailureRoute{rec_.id, rt_.identity_host(), endpoint};
        env.delivered.kind = abi::MessageKind::message;
        env.delivered.name = std::move(m.name);
        env.delivered.payload = std::move(m.payload);
        env.delivered.priority = m.priority;
        env.delivered.sender = rec_.sender(rt_.identity_host());
        env.delivered.badge = badge;
        env.caps = std::move(*caps);
        if (target->lane < 0) target->lane_hint = rec_.lane;
        rt_.enqueue(*target, std::move(env), m.priority);
        return abi::ok;
    }

    std::int64_t request(std::int32_t endpoint, std::span<const std::uint8_t> doc) override {
        abi::OutMessage m;
        if (!abi::decode(doc, m)) return abi::malformed;
        if (auto v = Runtime::Impl::validate(m); v != abi::ok) return v;
        if (m.timeout_ms <= 0 || m.async) return abi::invalid_argument;  // a request needs its correlation
        std::lock_guard lock(rt_.mu);
        PagletRec* target = nullptr;
        std::string remote;
        auto cap = checked_endpoint(endpoint, m.name, target, remote);
        if (!cap) return cap.error();
        const auto badge = (*cap)->badge;
        if (target == nullptr) {
            if (!m.lend.empty()) return abi::invalid_argument;
            if (!rt_.portable_handles(rec_, m.caps)) return abi::unsupported;
            auto target_id = (*cap)->target;
            auto caps = take_endpoint_caps(endpoint, m.caps);
            if (!caps) return caps.error();
            const std::uint64_t correlation = rec_.next_correlation++;
            rec_.pending_requests.insert(correlation);
            RemoteMessage out;
            out.kind = RemoteMessage::Kind::request;
            out.target = std::move(target_id);
            out.host = remote;
            out.name = std::move(m.name);
            out.payload = std::move(m.payload);
            out.priority = m.priority;
            out.sender = rec_.sender(rt_.identity_host());
            out.badge = badge;
            out.caps = rt_.portable_caps(std::move(*caps));
            out.requester = rec_.id;
            out.requester_host = rt_.mobility.host_id;
            out.correlation = correlation;
            out.timeout_ms = bounded_delay(m.timeout_ms).count();
            out.deadline_ms = wall_deadline(m.timeout_ms);
            rt_.send_remote(std::move(out));
            rt_.schedule(SteadyClock::now() + bounded_delay(m.timeout_ms),
                         Deadline{Deadline::Type::request_timeout, rec_.id, correlation});
            return static_cast<std::int64_t>(correlation);
        }
        auto lent = rt_.lend_caps(rec_, m.lend, *target);
        if (!lent) return lent.error();
        auto caps = take_endpoint_caps(endpoint, m.caps);
        if (!caps) return caps.error();
        const std::uint64_t correlation = rec_.next_correlation++;
        rec_.pending_requests.insert(correlation);
        Cap reply;
        reply.kind = Cap::Kind::reply;
        reply.target = rec_.id;
        reply.correlation = correlation;
        reply.transferable = true;
        Envelope env;
        env.delivered.kind = abi::MessageKind::request;
        env.delivered.name = std::move(m.name);
        env.delivered.payload = std::move(m.payload);
        env.delivered.priority = m.priority;
        env.delivered.sender = rec_.sender(rt_.identity_host());
        env.delivered.badge = badge;
        env.caps = std::move(*caps);
        env.lent = std::move(*lent);
        env.reply_cap = std::move(reply);
        env.deadline_ms = wall_deadline(m.timeout_ms);
        env.expires_at = SteadyClock::now() + bounded_delay(m.timeout_ms);
        if (target->lane < 0) target->lane_hint = rec_.lane;
        rt_.enqueue(*target, std::move(env), m.priority);
        rt_.schedule(SteadyClock::now() + bounded_delay(m.timeout_ms),
                     Deadline{Deadline::Type::request_timeout, rec_.id, correlation});
        return static_cast<std::int64_t>(correlation);
    }

    std::int32_t reply(std::int32_t handle, std::span<const std::uint8_t> doc) override {
        abi::OutMessage m;
        if (!abi::decode(doc, m)) return abi::malformed;
        const bool async = m.async;
        std::string name = m.name;
        std::vector<std::int32_t> caps = m.caps;
        const std::int32_t r = reply_message(handle, std::move(m));
        if (r == abi::ok || !async) return r;
        undelivered(std::move(name), handle, r, caps, true);
        return abi::ok;
    }

    std::int32_t reply_message(std::int32_t handle, abi::OutMessage m) {
        if (auto v = Runtime::Impl::validate(m); v != abi::ok) return v;
        std::lock_guard lock(rt_.mu);
        auto it = rec_.caps.find(handle);
        if (it == rec_.caps.end() || it->second.kind != Cap::Kind::reply) return abi::bad_handle;
        if (!m.lend.empty() || std::ranges::find(m.caps, handle) != m.caps.end()) return abi::invalid_argument;
        const Cap& r = it->second;
        const bool here =
            r.target.empty() ? (r.host.empty() || r.host == rt_.mobility.host_id) : rt_.paglets.contains(r.target);
        if (!here && !rt_.portable_handles(rec_, m.caps)) return abi::unsupported;
        auto caps = rt_.take_caps(rec_, m.caps);
        if (!caps) return caps.error();
        Cap reply = std::move(it->second);
        rec_.caps.erase(it);
        rt_.deliver_reply(reply.target, reply.correlation, abi::ok, std::move(m.payload), std::move(*caps), reply.host);
        return abi::ok;
    }

    std::int32_t cap_derive(std::int32_t handle, std::span<const std::uint8_t> doc) override {
        abi::DeriveSpec spec;
        if (!abi::decode(doc, spec)) return abi::malformed;
        std::lock_guard lock(rt_.mu);
        auto it = rec_.caps.find(handle);
        if (it == rec_.caps.end()) return abi::bad_handle;
        const Cap& src = it->second;
        if (src.kind != Cap::Kind::endpoint && src.kind != Cap::Kind::resource) return abi::bad_handle;
        if (auto u = rt_.usable(src); u != abi::ok) return u;
        if (rec_.caps.size() >= rt_.config.cap_limit) return abi::quota;
        Cap d = src;
        if (spec.ops) {
            for (const auto& op : *spec.ops) {
                const bool allowed =
                    op == "*" ? std::ranges::find(src.ops, std::string("*")) != src.ops.end() : op_allowed(src.ops, op);
                if (!allowed) return abi::denied;
            }
            d.ops = *spec.ops;
        }
        if (spec.expires_ms) {
            if (*spec.expires_ms <= 0) return abi::invalid_argument;
            const std::int64_t at = wall_ms() + *spec.expires_ms;
            if (src.expires && at > *src.expires) return abi::denied;
            d.expires = at;
        }
        if (spec.uses) {
            if (*spec.uses <= 0) return abi::invalid_argument;
            if (src.uses_left && *spec.uses > *src.uses_left) return abi::denied;
            d.uses_left = *spec.uses;
        }
        if (spec.transferable) {
            if (*spec.transferable && !src.transferable) return abi::denied;
            d.transferable = *spec.transferable;
        }
        if (spec.badge) {
            if (src.badge) return abi::denied;
            d.badge = *spec.badge;
        }
        if (spec.path) {
            if (src.kind != Cap::Kind::resource || src.resource_type != "dir") return abi::invalid_argument;
            auto rel = normalize_relative(*spec.path);
            if (!rel) return abi::invalid_argument;
            d.resource = src.resource.ends_with('/') ? src.resource + *rel : src.resource + "/" + *rel;
        }
        // The derived capability is a child in the revocation tree.
        d.lineage.push_back(src.id);
        d.id = new_cap_id();
        return rt_.add_cap(rec_, std::move(d));
    }

    std::int32_t cap_drop(std::int32_t handle) override {
        std::lock_guard lock(rt_.mu);
        if (handle == abi::self_handle) return abi::denied;
        auto it = rec_.caps.find(handle);
        if (it == rec_.caps.end()) return abi::bad_handle;
        Cap cap = std::move(it->second);
        rec_.caps.erase(it);
        rt_.drop_cap(std::move(cap), abi::gone);
        return abi::ok;
    }

    Document cap_inspect(std::int32_t handle) override {
        std::lock_guard lock(rt_.mu);
        auto it = rec_.caps.find(handle);
        if (it == rec_.caps.end()) return std::unexpected(abi::bad_handle);
        const Cap& c = it->second;
        abi::CapInfo info;
        switch (c.kind) {
            case Cap::Kind::endpoint:
                info.kind = "endpoint";
                info.target = "paglet:" + c.target;
                break;
            case Cap::Kind::reply:
                info.kind = "reply";
                info.target = "reply:" + std::to_string(c.correlation);
                break;
            case Cap::Kind::timer:
                info.kind = "timer";
                info.target = "timer:" + c.message.name;
                break;
            case Cap::Kind::resource: {
                info.kind = c.resource_type;
                PagletRec* provider = rt_.find(c.target);
                const std::string service =
                    provider != nullptr && provider->native ? std::string(provider->native->name()) : c.target;
                info.target = service + ":" + c.resource;
                break;
            }
        }
        if (c.kind == Cap::Kind::endpoint) {
            PagletRec* target = rt_.find(c.target);
            if (target != nullptr && target->native) info.target = "service:" + std::string(target->native->name());
        }
        info.revoked = rt_.revoked(c);
        info.ops = c.ops;
        info.transferable = c.transferable;
        info.badge = c.badge;
        info.expires = c.expires;
        info.uses_left = c.uses_left;
        return abi::encode(info);
    }

    Document cap_list() override {
        std::lock_guard lock(rt_.mu);
        std::vector<std::int32_t> handles;
        for (const auto& [h, cap] : rec_.caps) handles.push_back(h);
        return abi::encode(handles);
    }

    std::int32_t create_child(std::span<const std::uint8_t> doc) override {
        abi::ChildSpec spec;
        if (!abi::decode(doc, spec)) return abi::malformed;
        std::lock_guard lock(rt_.mu);
        if (rec_.in_end_hook) return abi::bad_state;
        std::string module = rec_.module;
        if (spec.module) {
            auto info = rt_.modules->info(*spec.module);
            if (!info) return abi::not_found;
            if (!check_paglet_module(*info, TrustClass::roaming)) return abi::denied;
            module = *spec.module;
        }
        if (auto why = rt_.refusal(module, TrustClass::roaming); !why.empty()) {
            rt_.log(2, rec_.id, "child refused: " + why);
            return abi::denied;
        }
        if (rec_.spawned_this_call >= rt_.config.spawn_limit_per_call || rec_.caps.size() >= rt_.config.cap_limit ||
            rt_.owner_full(rec_.owner)) {
            return abi::quota;
        }
        const PagletId id = new_paglet_id();
        PagletRec* created = rt_.new_paglet(id, std::move(module), TrustClass::roaming, rec_.owner);
        if (created != nullptr) created->marks = rec_.marks;  // what it was given may be marked content
        if (created == nullptr) return abi::not_found;  // collected since the check
        auto caps = rt_.take_caps(rec_, spec.caps);
        if (!caps) {
            rt_.erase_paglet(id);
            return caps.error();
        }
        ++rec_.spawned_this_call;
        rt_.record_spawn(rec_.id, id, "child", created->module);
        PagletRec& child = *created;
        child.lane_hint = rec_.lane;
        child.checkpoint_interval = rec_.checkpoint_interval;
        rt_.install_services(child);
        rt_.enqueue_start(child, abi::EventKind::created, std::move(spec.args), std::move(*caps), std::nullopt);
        return rt_.add_cap(rec_, rt_.endpoint_to(id));
    }

    std::int32_t lifecycle(std::int32_t op, std::span<const std::uint8_t> doc) override {
        std::lock_guard lock(rt_.mu);
        const auto operation = static_cast<abi::LifecycleOp>(op);
        if (rec_.in_end_hook && operation != abi::LifecycleOp::restart)
            return abi::bad_state;
        switch (static_cast<abi::LifecycleOp>(op)) {
            case abi::LifecycleOp::dispose:
                if (rec_.pending_end || rec_.dispatch_pending) return abi::bad_state;
                rec_.pending_end = abi::LifecycleOp::dispose;
                return abi::ok;
            case abi::LifecycleOp::deactivate: {
                abi::DeactivateArg arg;
                if (!doc.empty() && !abi::decode(doc, arg)) return abi::malformed;
                if (arg.wake_after_ms && *arg.wake_after_ms < 0) return abi::invalid_argument;
                if (rec_.pending_end || rec_.dispatch_pending) return abi::bad_state;
                rec_.pending_end = abi::LifecycleOp::deactivate;
                rec_.wake_after_ms = arg.wake_after_ms;
                return abi::ok;
            }
            case abi::LifecycleOp::clone: {
                abi::CloneArg arg;
                if (!doc.empty() && !abi::decode(doc, arg)) return abi::malformed;
                if (rec_.spawned_this_call >= rt_.config.spawn_limit_per_call ||
                    rec_.caps.size() >= rt_.config.cap_limit) {
                    return abi::quota;
                }
                if (arg.destination) {
                    // A clone made on another host: it exists here only as an
                    // image until the mesh delivered it.
                    if (!parse_transfer_ticket(*arg.destination)) return abi::invalid_argument;
                    if (!rt_.mobility.depart) return abi::unsupported;
                    if (rec_.trust != TrustClass::roaming) return abi::denied;
                    if (!rt_.portable_handles(rec_, arg.caps)) return abi::invalid_argument;
                    auto caps = rt_.take_caps(rec_, arg.caps);
                    if (!caps) return caps.error();
                    ++rec_.spawned_this_call;
                    const PagletId id = new_paglet_id();
                    rec_.clones.push_back(PendingClone{id, std::move(arg.args), std::move(*caps), *arg.destination});
                    return rt_.add_cap(rec_, rt_.endpoint_to(id));
                }
                const TrustClass trust = rec_.trust == TrustClass::system ? TrustClass::roaming : rec_.trust;
                if (auto why = rt_.refusal(rec_.module, trust); !why.empty()) {
                    rt_.log(2, rec_.id, "clone refused: " + why);
                    return abi::denied;
                }
                if (rt_.owner_full(rec_.owner)) return abi::quota;
                const PagletId id = new_paglet_id();
                // The module has a user (this paglet), so it is in the store.
                PagletRec* created = rt_.new_paglet(id, rec_.module, trust, rec_.owner);
                if (created == nullptr) return abi::internal;
                created->marks = rec_.marks;
                created->restart = rec_.restart;
                created->notify_recovered = rec_.notify_recovered;
                auto caps = rt_.take_caps(rec_, arg.caps);
                if (!caps) {
                    rt_.erase_paglet(id);
                    return caps.error();
                }
                ++rec_.spawned_this_call;
                rt_.record_spawn(rec_.id, id, "clone", created->module);
                PagletRec& clone = *created;
                clone.awaiting_image = true;
                clone.lane_hint = rec_.lane;
                clone.checkpoint_interval = rec_.checkpoint_interval;
                rec_.clones.push_back(PendingClone{id, std::move(arg.args), std::move(*caps)});
                return rt_.add_cap(rec_, rt_.endpoint_to(id));
            }
            case abi::LifecycleOp::dispatch: {
                abi::DispatchArg arg;
                if (!abi::decode(doc, arg) || !parse_transfer_ticket(arg.destination)) return abi::invalid_argument;
                if (!rt_.mobility.depart) return abi::unsupported;
                // System and resident paglets stay where they are.
                if (rec_.trust != TrustClass::roaming) return abi::denied;
                if (rec_.pending_end || rec_.dispatch_pending) return abi::bad_state;
                if (rec_.pinned_until() != 0) return abi::pinned;
                rec_.pending_end = abi::LifecycleOp::dispatch;
                rec_.dispatch_pending = true;
                rec_.dispatch_to = std::move(arg.destination);
                return abi::ok;
            }
            case abi::LifecycleOp::checkpoint:
                if (!rt_.store) return abi::unsupported;
                if (!rec_.restart || rec_.in_end_hook || rec_.pending_end) return abi::bad_state;
                // Scheduled for after this handler, not a synchronous barrier.
                rec_.checkpoint_due = true;
                rec_.checkpoint_requested = true;
                return abi::ok;
            case abi::LifecycleOp::restart: {
                abi::RestartArg arg;
                if (!abi::decode(doc, arg)) return abi::malformed;
                if (rec_.trust == TrustClass::system) return abi::denied;
                rec_.restart = arg.restart;
                rec_.notify_recovered = arg.notify_recovered;
                rec_.checkpoint_due = true;  // stored (or removed) after this handler
                return abi::ok;
            }
        }
        return abi::invalid_argument;
    }

    std::int32_t timer_set(std::int64_t delay_ms, std::span<const std::uint8_t> doc) override {
        abi::OutMessage m;
        if (!abi::decode(doc, m)) return abi::malformed;
        if (auto v = Runtime::Impl::validate(m); v != abi::ok) return v;
        if (delay_ms < 0) return abi::invalid_argument;
        std::lock_guard lock(rt_.mu);
        const auto timers =
            std::ranges::count_if(rec_.caps, [](const auto& e) { return e.second.kind == Cap::Kind::timer; });
        if (static_cast<std::size_t>(timers) >= rt_.config.timer_limit || rec_.caps.size() >= rt_.config.cap_limit) {
            return abi::quota;
        }
        Cap t;
        t.kind = Cap::Kind::timer;
        t.target = rec_.id;
        t.transferable = false;
        t.timer_id = rec_.next_timer++;
        const auto delay = bounded_delay(delay_ms);
        t.fire_at = wall_deadline(delay.count());
        t.message.name = std::move(m.name);
        t.message.payload = std::move(m.payload);
        t.message.priority = m.priority;
        rt_.schedule(SteadyClock::now() + delay,
                     Deadline{Deadline::Type::timer, rec_.id, t.timer_id});
        return rt_.add_cap(rec_, std::move(t));
    }

    // -- compute jobs (ABI v1.3) --

    // --8<-- [start:job_submit]
    std::int64_t job_submit(std::span<const std::uint8_t> doc) override {
        if (doc.size() > abi::max_document_size) return abi::too_large;
        abi::JobSpec spec;
        if (!abi::decode(doc, spec)) return abi::malformed;
        if (spec.function.empty() || spec.function.size() > abi::max_name_length) return abi::invalid_argument;
        if (spec.cores < 1 || spec.max_ms < 0 || spec.memory_mb < 0) return abi::invalid_argument;
        if (spec.on_departure != "cancel" && spec.on_departure != "finish") return abi::invalid_argument;
        std::lock_guard lock(rt_.mu);
        if (rec_.in_end_hook) return abi::bad_state;
        if (rt_.stopping || !rt_.job_pool) return abi::unsupported;
        const std::string module = spec.module.value_or(rec_.module_hash());
        if (module.size() != 64) return abi::invalid_argument;
        // The mesh policy decides, and may cap the wall time.
        if (rt_.job_hooks.admit) {
            auto cap_ms = rt_.job_hooks.admit(rec_.sender(rt_.identity_host()), spec);
            if (!cap_ms) return cap_ms.error();
            if (*cap_ms > 0 && (spec.max_ms == 0 || spec.max_ms > *cap_ms)) spec.max_ms = *cap_ms;
        }
        std::optional<Cap> artifact;
        if (spec.input_artifact) {
            auto it = rec_.caps.find(*spec.input_artifact);
            if (it == rec_.caps.end() || it->second.kind != Cap::Kind::resource || it->second.resource_type != "artifact") {
                return abi::bad_handle;
            }
            if (auto u = rt_.usable(it->second); u != abi::ok) return u;
            if (!rt_.job_artifacts.load) return abi::unsupported;
            artifact = it->second;
        }
        const bool remote = !spec.hosts.empty();
        if (remote && !rt_.job_hooks.submit_remote) return abi::unsupported;
        const std::uint64_t correlation = rec_.next_correlation++;
        rec_.pending_requests.insert(correlation);
        auto& pending = rec_.jobs[correlation];
        pending.remote = remote;
        pending.finish = spec.on_departure == "finish";
        pending.status.state = "queued";
        // If no result comes, the paglet hears `timeout` a minute after the
        // job's limit. The deadline also carries the correlation along when
        // the paglet moves, so a result can follow it.
        const std::int64_t limit = std::min(spec.max_ms > 0 ? spec.max_ms : rt_.config.job_default_time.count(),
                                          rt_.config.job_max_time.count());
        rt_.schedule(SteadyClock::now() + std::chrono::milliseconds(limit + 60'000),
                     Deadline{Deadline::Type::request_timeout, rec_.id, correlation});
        IncomingJob job{rec_.id, {}, correlation, rec_.owner, module, spec.function, std::move(spec.input),
                        spec.cores, spec.max_ms, spec.memory_mb, {rec_.marks.begin(), rec_.marks.end()}, artifact};
        if (remote) {
            // The mesh picks the host (without the runtime's lock).
            spec.input = std::move(job.input);
            spec.input_artifact.reset();  // a handle of this paglet; the capability travels instead
            RemoteJob rj{rec_.id, rt_.mobility.host_id, correlation, rec_.sender(rt_.identity_host()), module,
                         std::move(spec), artifact ? std::optional<Cap>(rt_.portable(*artifact)) : std::nullopt,
                         std::move(job.marks)};
            rt_.defer([submit = rt_.job_hooks.submit_remote, rj = std::move(rj)]() mutable { submit(std::move(rj)); });
            return static_cast<std::int64_t>(correlation);
        }
        if (artifact) {
            rt_.start_job_from_artifact(std::move(job));
            return static_cast<std::int64_t>(correlation);
        }
        auto id = rt_.start_job(std::move(job));
        if (!id) {
            rec_.pending_requests.erase(correlation);
            rec_.jobs.erase(correlation);
            return id.error();
        }
        pending.id = *id;
        return static_cast<std::int64_t>(correlation);
    }
    // --8<-- [end:job_submit]

    std::int32_t job_cancel(std::int64_t correlation) override {
        std::lock_guard lock(rt_.mu);
        auto it = rec_.jobs.find(static_cast<std::uint64_t>(correlation));
        if (it == rec_.jobs.end()) return abi::not_found;
        if (!it->second.remote) {
            if (it->second.id != 0) {
                rt_.job_pool->cancel(it->second.id, abi::cancelled);
            } else {
                rt_.deliver_reply(rec_.id, static_cast<std::uint64_t>(correlation), abi::cancelled, {}, {});
            }
        } else if (rt_.job_hooks.cancel_remote) {
            auto cancel = rt_.job_hooks.cancel_remote;
            rt_.defer([cancel, paglet = rec_.id, correlation] { cancel(paglet, static_cast<std::uint64_t>(correlation)); });
        }
        return abi::ok;
    }

    Document job_status(std::int64_t correlation) override {
        std::lock_guard lock(rt_.mu);
        auto it = rec_.jobs.find(static_cast<std::uint64_t>(correlation));
        if (it == rec_.jobs.end()) return std::unexpected(abi::not_found);
        abi::JobStatus st = it->second.status;
        if (!it->second.remote) {
            st.host = rt_.mobility.host_id.empty() ? rt_.config.host_name : rt_.mobility.host_id;
            if (auto info = rt_.job_pool->info(it->second.id)) {
                st.state = info->state;
                st.done = info->done;
                st.total = info->total;
                st.elapsed_ms = info->elapsed_ms;
                st.aot = info->aot;
            } else if (it->second.id != 0) {
                st.state = "done";  // the result is on its way
            }
        }
        return abi::encode(st);
    }

private:
    // Transfer validation must finish before consuming a use. If the endpoint
    // itself travels, update the moved capability, never its erased map entry.
    std::expected<std::vector<Cap>, std::int32_t> take_endpoint_caps(
        std::int32_t endpoint, const std::vector<std::int32_t>& handles) {
        auto caps = rt_.take_caps(rec_, handles);
        if (!caps) return std::unexpected(caps.error());
        Cap* used = nullptr;
        if (auto it = rec_.caps.find(endpoint); it != rec_.caps.end()) {
            used = &it->second;
        } else {
            const auto at = std::ranges::find(handles, endpoint);
            // checked_endpoint succeeded, so removal means it is in this batch.
            used = &(*caps)[static_cast<std::size_t>(at - handles.begin())];
        }
        if (used->uses_left) --*used->uses_left;
        return caps;
    }

    // Reports a failed asynchronous send or reply to the paglet itself. The
    // guest considers the message sent, so what it gave up is released here:
    // the capabilities it transferred and, for a reply, the reply capability
    // (the requester is answered `gone`).
    void undelivered(std::string name, std::int32_t handle, std::int32_t status, const std::vector<std::int32_t>& caps,
                     bool reply) {
        std::lock_guard lock(rt_.mu);
        auto release = [&](std::int32_t h) {
            if (h == abi::self_handle) return;
            auto it = rec_.caps.find(h);
            if (it == rec_.caps.end()) return;
            Cap cap = std::move(it->second);
            rec_.caps.erase(it);
            rt_.drop_cap(std::move(cap), abi::gone);
        };
        for (auto h : caps) release(h);
        if (reply) {
            if (auto it = rec_.caps.find(handle); it != rec_.caps.end() && it->second.kind == Cap::Kind::reply) {
                release(handle);
            }
        }
        Envelope env;
        env.delivered.kind = abi::MessageKind::message;
        env.delivered.name = std::string(abi::undelivered_message);
        env.delivered.payload = abi::encode(abi::Undelivered{std::move(name), handle, status});
        env.delivered.priority = abi::max_priority;
        rt_.enqueue(rec_, std::move(env), abi::max_priority);
    }

    // Looks up an endpoint capability and its target (mu held): a paglet
    // here, or the host of a paglet elsewhere (`target` null, `remote` set).
    std::expected<Cap*, std::int32_t> checked_endpoint(std::int32_t handle, std::string_view name, PagletRec*& target,
                                                       std::string& remote) {
        auto it = rec_.caps.find(handle);
        if (it == rec_.caps.end()) return std::unexpected(abi::bad_handle);
        if (auto c = rt_.check_endpoint(it->second, name); c != abi::ok) return std::unexpected(c);
        target = rt_.find(it->second.target);
        if (target == nullptr) {
            remote = rt_.mobility.deliver ? rt_.remote_host(it->second.target, it->second.host) : std::string();
            if (remote.empty()) return std::unexpected(abi::not_found);
            return &it->second;
        }
        if (target->mailbox.size() >= rt_.config.mailbox_limit) return std::unexpected(abi::quota);
        return &it->second;
    }

    Runtime::Impl& rt_;
    PagletRec& rec_;
};

}  // namespace

// ---------------------------------------------------------------------------
// Native system paglets

namespace {

class NativeContext final : public SystemContext {
public:
    NativeContext(Runtime& runtime, Runtime::Impl& rt, PagletId id) : runtime_(runtime), rt_(rt), id_(std::move(id)) {}

    Runtime& runtime() override { return runtime_; }
    const PagletId& id() const override { return id_; }
    std::string_view host_name() const override { return rt_.config.host_name; }

    Cap resource(std::string type, std::string resource, std::vector<std::string> rights,
                 std::optional<std::string> grant, std::optional<std::int64_t> expires) override {
        Cap c;
        c.kind = Cap::Kind::resource;
        c.target = id_;
        c.resource_type = std::move(type);
        c.resource = std::move(resource);
        c.ops = std::move(rights);
        c.transferable = true;
        c.id = new_cap_id();
        c.grant = std::move(grant);
        c.expires = expires;
        return c;
    }

    Cap endpoint(const PagletId& target, std::vector<std::string> ops) override {
        Cap c = rt_.endpoint_to(target);
        c.ops = std::move(ops);
        return c;
    }

    std::int32_t check(const Cap& cap, std::string_view type, std::string_view right) const override {
        std::lock_guard lock(rt_.mu);
        if (auto u = rt_.usable(cap); u != abi::ok) return u;
        if (cap.kind != Cap::Kind::resource || cap.target != id_ || cap.resource_type != type) return abi::bad_handle;
        if (!right.empty() && !op_allowed(cap.ops, right)) return abi::denied;
        return abi::ok;
    }

    std::expected<void, std::int32_t> send(const PagletId& to, std::string_view name, Bytes payload,
                                           std::vector<Cap> caps, std::int32_t priority,
                                           std::optional<std::string> badge) override {
        if (!abi::valid_message_name(name) || abi::reserved_message_name(name) || priority < 0 || priority > abi::max_priority) {
            return std::unexpected(abi::invalid_argument);
        }
        std::lock_guard lock(rt_.mu);
        PagletRec* target = rt_.find(to);
        PagletRec* self = rt_.find(id_);
        if (target == nullptr || self == nullptr) return std::unexpected(abi::not_found);
        if (target->mailbox.size() >= rt_.config.mailbox_limit) return std::unexpected(abi::quota);
        Envelope env;
        env.delivered.kind = abi::MessageKind::message;
        env.delivered.name = std::string(name);
        env.delivered.payload = std::move(payload);
        env.delivered.priority = priority;
        env.delivered.sender = self->sender(rt_.identity_host());
        env.delivered.badge = std::move(badge);
        env.caps = std::move(caps);
        rt_.enqueue(*target, std::move(env), priority);
        return {};
    }

    std::expected<void, std::int32_t> reply(Cap reply, std::int32_t status, Bytes payload,
                                            std::vector<Cap> caps) override {
        if (reply.kind != Cap::Kind::reply) return std::unexpected(abi::bad_handle);
        std::lock_guard lock(rt_.mu);
        rt_.deliver_reply(reply.target, reply.correlation, status, std::move(payload), std::move(caps));
        return {};
    }

    void log(abi::LogLevel level, std::string_view text) override {
        std::lock_guard lock(rt_.mu);
        rt_.log(static_cast<std::int32_t>(level), id_, std::string(text));
    }

private:
    Runtime& runtime_;
    Runtime::Impl& rt_;
    PagletId id_;
};

class NativeCall final : public ServiceCall {
public:
    NativeCall(Runtime::Impl& rt, Envelope& env) : rt_(rt), env_(env) {}

    abi::MessageKind kind() const override { return env_.delivered.kind; }
    const std::string& name() const override { return env_.delivered.name; }
    std::span<const std::uint8_t> payload() const override { return env_.delivered.payload; }
    const std::optional<abi::SenderRecord>& sender() const override { return env_.delivered.sender; }
    const std::optional<std::string>& badge() const override { return env_.delivered.badge; }
    const std::vector<Cap>& lent() const override { return env_.lent; }
    std::vector<Cap>& transferred() override { return env_.caps; }

    std::expected<void, std::int32_t> reply(std::int32_t status, Bytes payload, std::vector<Cap> caps) override {
        if (!env_.reply_cap) return std::unexpected(abi::bad_state);
        Cap cap = std::move(*env_.reply_cap);
        env_.reply_cap.reset();
        replied_ = true;
        std::lock_guard lock(rt_.mu);
        rt_.deliver_reply(cap.target, cap.correlation, status, std::move(payload), std::move(caps));
        return {};
    }
    bool replied() const override { return replied_; }

    std::optional<Cap> defer() override {
        if (!env_.reply_cap) return std::nullopt;
        std::optional<Cap> cap = std::move(env_.reply_cap);
        env_.reply_cap.reset();
        return cap;
    }

private:
    Runtime::Impl& rt_;
    Envelope& env_;
    bool replied_ = false;
};

}  // namespace

void Runtime::Impl::process_native(PagletRec& rec, Envelope env) {
    if (env.type == Envelope::Type::ended) {
        try {
            rec.native->paglet_ended(*rec.context, env.ended_id);
        } catch (const std::exception& e) {
            std::lock_guard lock(mu);
            log(3, rec.id, std::string("paglet_ended failed: ") + e.what());
        }
    } else if (env.type == Envelope::Type::message) {
        NativeCall call(*this, env);
        try {
            rec.native->handle(*rec.context, call);
        } catch (const std::exception& e) {
            std::lock_guard lock(mu);
            log(3, rec.id, "handling " + env.delivered.name + " failed: " + e.what());
        }
        std::lock_guard lock(mu);
        // A request the service neither answered nor deferred: `internal`.
        if (env.reply_cap) {
            log(3, rec.id, "request " + env.delivered.name + " was not answered");
            deliver_reply(env.reply_cap->target, env.reply_cap->correlation, abi::internal, {}, {});
            env.reply_cap.reset();
        }
        // Transferred capabilities the service did not take are released.
        for (auto& c : env.caps) drop_cap(std::move(c), abi::gone);
        env.caps.clear();
    }
    std::lock_guard lock(mu);
    ++rec.handled;
}

// ---------------------------------------------------------------------------
// Delivery

std::expected<void, std::string> Runtime::Impl::activate(PagletRec& rec, bool deliver_activated) {
    const auto activation_started = SteadyClock::now();
    std::optional<wasm::Snapshot> image;
    Executor* executor = nullptr;
    {
        std::lock_guard lock(mu);
        if (rec.image) {
            // With workers, the in-memory image stays as the fallback in case
            // the worker process ends; in-process it is no longer needed.
            if (uses_workers) {
                image = *rec.image;
            } else {
                image = std::move(rec.image);
                rec.image.reset();
            }
        }
        executor = lanes[static_cast<std::size_t>(rec.lane)]->executor.get();
    }
    if (!image && rec.image_on_disk && store) {
        auto loaded = store->load_image(rec.id);
        if (!loaded) return std::unexpected(loaded.error());
        image = std::move(*loaded);
    }
    if (!image && rec.started) {
        return std::unexpected(std::string("the paglet's state was lost with its worker process (no image)"));
    }
    auto module = modules->acquire(rec.module);
    if (!module) return std::unexpected(module.error());
    rec.imports = std::make_unique<Imports>(*this, rec);
    auto placed = executor->place(rec.id, *module, config.limits, image ? &*image : nullptr, *rec.imports);
    if (!placed) return std::unexpected(placed.error());
    {
        std::lock_guard lock(mu);  // info() and the watchdog read it under the lock
        rec.instance = std::move(*placed);
        rec.loaded = std::move(*module);
        if (rec.arrival_receipt)
            rec.arrival_activation_ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                SteadyClock::now() - activation_started).count());
    }
    // A paglet without a stored image is checkpointed after its first handler.
    rec.last_checkpoint = SteadyClock::now();
    rec.checkpoint_due = !rec.image_on_disk;
    if (deliver_activated) {
        auto r = call_event(rec, abi::EventKind::activated, {});
        if (!r) return std::unexpected(r.error());
    }
    return {};
}

void Runtime::Impl::process(PagletRec& rec, Envelope env, bool& remove) {
    if (expired(env)) return;
    if (env.type == Envelope::Type::wake) {
        std::lock_guard lock(mu);
        if (env.wake_at_ms != rec.wake_at_ms) return;
        rec.wake_at_ms = 0;
    }
    if (rec.native) {
        process_native(rec, std::move(env));
        return;
    }
    if (env.type == Envelope::Type::dispose && !rec.instance && !rec.image && !rec.image_on_disk) {
        std::lock_guard lock(mu);
        end_paglet(rec, false, "disposed");
        remove = true;
        return;
    }
    if (rec.instance && rec.instance->lost()) {
        // Its worker process ended: continue from the last image.
        std::lock_guard lock(mu);
        log(2, rec.id, "instance lost with its worker process; resuming from the last image");
        retire(rec);
    }
    if (env.type == Envelope::Type::deactivate && !rec.instance) return;  // already inactive

    if (!rec.instance) {
        const bool deliver_activated = rec.started && env.type != Envelope::Type::start;
        if (auto ok = activate(rec, deliver_activated); !ok) {
            std::lock_guard lock(mu);
            if (env.reply_cap) drop_cap(std::move(*env.reply_cap), abi::failed);
            for (auto& c : env.caps) drop_cap(std::move(c), abi::failed);
            end_paglet(rec, true, ok.error());
            remove = true;
            return;
        }
    }

    if (env.type == Envelope::Type::message) {
        std::lock_guard lock(mu);
        if (rec.wake_at_ms != 0) {
            rec.wake_at_ms = 0;
            rec.checkpoint_due = true;
        }
    }
    if (env.type == Envelope::Type::wake) {
        // activate() resets checkpoint_due. Persist consumption of the wake
        // after activation so a later restart cannot replay the old alarm.
        std::lock_guard lock(mu);
        rec.checkpoint_due = true;
    }

    if (expired(env)) return;
    std::expected<std::int32_t, std::string> result = abi::handled;
    std::optional<std::pair<std::int32_t, std::uint64_t>> reply_slot;  // handle, correlation

    switch (env.type) {
        case Envelope::Type::start: {
            rec.dormant = false;
            if (env.arrived) {
                result = call_event(rec, abi::EventKind::arrived, abi::encode(*env.arrived));
                rec.started = true;
                break;
            }
            abi::StartEvent e;
            {
                std::lock_guard lock(mu);
                for (auto& c : env.caps) e.caps.push_back(add_cap(rec, std::move(c)));
            }
            e.args = std::move(env.args);
            e.original = std::move(env.original);
            result = call_event(rec, env.event, abi::encode(e));
            rec.started = true;
            break;
        }
        case Envelope::Type::message: {
            abi::Delivered& d = env.delivered;
            {
                std::lock_guard lock(mu);
                for (auto& c : env.caps) d.caps.push_back(add_cap(rec, std::move(c)));
                if (env.reply_cap) {
                    const std::uint64_t correlation = env.reply_cap->correlation;
                    d.reply = add_cap(rec, std::move(*env.reply_cap));
                    reply_slot = std::pair{d.reply, correlation};
                }
            }
            const Bytes doc = abi::encode(d);
            result = guest_call(
                rec, [&] { return rec.instance->call("paglets_on_message", std::span<const std::uint32_t>{}, doc); });
            break;
        }
        case Envelope::Type::wake:
        case Envelope::Type::ended: break;
        case Envelope::Type::deactivate: {
            std::lock_guard lock(mu);
            if (!rec.pending_end) rec.pending_end = abi::LifecycleOp::deactivate;
            break;
        }
        case Envelope::Type::dispose: {
            std::lock_guard lock(mu);
            rec.pending_end = abi::LifecycleOp::dispose;
            break;
        }
        case Envelope::Type::move: {
            std::lock_guard lock(mu);
            if (const std::int64_t until = rec.pinned_until(); until != 0) {
                rec.dispatch_pending = false;
                move_failed(rec, env.destination, "pinned until " + std::to_string(until), {});
            } else if (!rec.pending_end) {
                rec.pending_end = abi::LifecycleOp::dispatch;
                rec.dispatch_to = std::move(env.destination);
            }
            break;
        }
    }

    std::lock_guard lock(mu);
    ++rec.handled;
    if (!result) {
        if (env.type == Envelope::Type::start) notify_arrival(rec, false);
        retire(rec);
        end_paglet(rec, true, result.error());
        remove = true;
        return;
    }
    // Requests the handler did not handle are answered by the host.
    if (env.type == Envelope::Type::start) notify_arrival(rec, true);
    if (reply_slot && (*result == abi::not_handled || *result < 0)) {
        auto it = rec.caps.find(reply_slot->first);
        if (it != rec.caps.end() && it->second.kind == Cap::Kind::reply &&
            it->second.correlation == reply_slot->second) {
            Cap cap = std::move(it->second);
            rec.caps.erase(it);
            deliver_reply(cap.target, cap.correlation, *result == abi::not_handled ? abi::unknown_message : *result, {},
                          {});
        }
    }
    if (*result == abi::not_handled && !reply_slot && env.type == Envelope::Type::message &&
        (env.delivered.kind == abi::MessageKind::message || env.delivered.kind == abi::MessageKind::timer)) {
        const auto kind = env.delivered.kind == abi::MessageKind::timer ? "timer " : "message ";
        log(static_cast<std::int32_t>(abi::LogLevel::debug), rec.id,
            std::string("handler returned not_handled for ") + kind + env.delivered.name);
    }
    if (*result < 0 && !reply_slot && env.type == Envelope::Type::message) {
        log(2, rec.id, "handler returned " + std::string(abi::error_name(*result)));
    }
}

// Applies the lifecycle operations of the call that just ended (mu held
// on entry and exit; released around guest calls).
void Runtime::Impl::finish_call(PagletRec& rec, bool& remove, std::unique_lock<std::mutex>& lock) {
    rec.spawned_this_call = 0;
    const bool checkpoint_requested = std::exchange(rec.checkpoint_requested, false);

    std::vector<Departure> departing;  // handed to the mesh after the lock is released
    auto publish = [&] {
        for (auto& d : departing) outgoing.push_back(std::move(d));
        departing.clear();
    };
    auto fail = [&](std::string reason) {
        // Clones already completed before the end hook are independent
        // paglets. The original's own departure is staged only after all
        // unlocked work has been checked for termination.
        publish();
        end_paglet(rec, true, std::move(reason));
        remove = true;
    };
    auto killed = [&] {
        if (!rec.kill_reason) return false;
        fail(*rec.kill_reason);
        return true;
    };
    auto end_hook = [&](abi::EventKind kind, const Bytes& doc) {
        rec.in_end_hook = true;
        lock.unlock();
        auto r = call_event(rec, kind, doc);
        lock.lock();
        rec.in_end_hook = false;
        return r;
    };
    auto clones = std::move(rec.clones);
    rec.clones.clear();
    if (!clones.empty() && rec.instance) {
        lock.unlock();
        const auto capture_started = SteadyClock::now();
        auto snap = snapshot(rec);
        const auto capture_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyClock::now() - capture_started).count();
        lock.lock();
        if (rec.kill_reason) {
            rec.clones = std::move(clones);  // end_paglet cleans unpublished clones and their capabilities
            (void)killed();
            return;
        }
        for (auto& pc : clones) {
            if (pc.destination) {
                if (!snap) {
                    move_failed(rec, *pc.destination, snap.error(), pc.id);
                    for (auto& c : pc.caps) drop_cap(std::move(c), abi::failed);
                    continue;
                }
                // The clone's state: transferable endpoints and the service
                // endpoints under the original's handles.
                TravelState st = state_of(rec);
                st.id = pc.id;
                std::erase_if(st.caps, [&](const auto& e) {
                    const bool service =
                        std::ranges::any_of(rec.services, [&](const auto& sv) { return sv.second == e.first; });
                    return e.first == abi::self_handle ||
                           (!service && (e.second.kind != Cap::Kind::endpoint || !e.second.transferable));
                });
                st.pending.clear();
                st.pending_captured_ms = 0;
                st.wake_at_ms = 0;
                Departure d;
                d.capture_started = capture_started;
                d.capture_ns = static_cast<std::uint64_t>(capture_ns);
                d.move = next_move++;
                d.state = std::move(st);
                d.image = *snap;
                d.destination = *pc.destination;
                d.clone = true;
                d.clone_args = std::move(pc.args);
                d.clone_caps = portable_caps(std::move(pc.caps));
                d.original = rec.id;
                departures[d.move] = Moving{rec.id, pc.id, *pc.destination};
                departing.push_back(std::move(d));
                continue;
            }
            PagletRec* clone = find(pc.id);
            if (clone == nullptr) continue;
            if (!snap) {
                log(3, rec.id, "clone failed: " + snap.error());
                for (auto& c : pc.caps) drop_cap(std::move(c), abi::failed);
                erase_paglet(pc.id);
                continue;
            }
            // The clone keeps the handle numbers of transferable endpoints,
            // so handles stored in its memory stay meaningful.
            for (const auto& [h, cap] : rec.caps) {
                if (h != abi::self_handle && cap.kind == Cap::Kind::endpoint && cap.transferable) {
                    clone->caps.emplace(h, cap);
                }
            }
            // Service endpoints keep their handles as well (fresh IDs: the
            // clone's own entries).
            for (const auto& [name, h] : rec.services) {
                auto it = rec.caps.find(h);
                if (it == rec.caps.end()) continue;
                Cap c = it->second;
                c.id = new_cap_id();
                clone->caps.emplace(h, std::move(c));
                clone->services.emplace_back(name, h);
            }
            clone->next_handle = rec.next_handle;
            clone->next_correlation = rec.next_correlation;
            clone->image = *snap;
            clone->awaiting_image = false;
            enqueue_start(*clone, abi::EventKind::cloned, std::move(pc.args), std::move(pc.caps), rec.id);
        }
    }

    const auto end = rec.pending_end;
    rec.pending_end.reset();
    if (end == abi::LifecycleOp::dispose) {
        if (rec.instance) {
            auto r = end_hook(abi::EventKind::disposing, {});
            if (killed()) return;
            if (!r) {
                fail("disposing: " + r.error());
                return;
            }
        }
        retire(rec);
        end_paglet(rec, false, "disposed");
        remove = true;
    } else if (end == abi::LifecycleOp::dispatch && rec.instance) {
        const std::string destination = rec.dispatch_to.value_or("");
        rec.dispatch_to.reset();
        auto r = end_hook(abi::EventKind::dispatching, abi::encode(abi::DispatchingEvent{destination}));
        if (killed()) return;
        if (!r) {
            fail("dispatching: " + r.error());
            return;
        }
        lock.unlock();
        const auto capture_started = SteadyClock::now();
        auto snap = snapshot(rec);
        const auto capture_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyClock::now() - capture_started).count();
        lock.lock();
        if (killed()) return;
        if (!snap) {
            // The paglet stays; it hears why.
            rec.dispatch_pending = false;
            move_failed(rec, destination, "no image: " + snap.error(), {});
        } else if (const std::int64_t until = rec.pinned_until(); until != 0) {
            // Pinned while it prepared to leave: the pin wins.
            rec.dispatch_pending = false;
            move_failed(rec, destination, "pinned until " + std::to_string(until), {});
        } else {
            auto captured = capture_ready(rec);
            if (!captured) {
                rec.dispatch_pending = false;
                move_failed(rec, destination, captured.error(), {});
            } else {
                Departure d;
                d.capture_started = capture_started;
                d.capture_ns = static_cast<std::uint64_t>(capture_ns);
                d.move = next_move++;
                d.state = state_of(rec);
                d.state.ready = std::move(captured->messages);
                d.image = *snap;
                d.destination = destination;
                departures[d.move] = Moving{rec.id, {}, destination, std::move(captured->sequences)};
                // Until the mesh reports the outcome the paglet waits here,
                // inactive, holding the messages that arrive.
                rec.moving = true;
                rec.image = std::move(*snap);
                retire(rec);
                rec.imports.reset();
                departing.push_back(std::move(d));
            }
        }
    } else if (end == abi::LifecycleOp::deactivate && rec.instance) {
        const auto wake = rec.wake_after_ms;
        rec.wake_after_ms.reset();
        auto r = end_hook(abi::EventKind::deactivating, {});
        if (killed()) return;
        if (!r) {
            fail("deactivating: " + r.error());
            return;
        }
        lock.unlock();
        auto snap = snapshot(rec);
        lock.lock();
        if (killed()) return;
        if (!snap) {
            fail(snap.error());
            return;
        }
        const auto now = wall_ms();
        rec.wake_at_ms = wake ? now + std::min(*wake, std::numeric_limits<std::int64_t>::max() - now) : 0;
        if (store) {
            if (!persist(rec, *snap) && rec.restart) {
                fail("deactivating: the image could not be stored");
                return;
            }
        } else {
            rec.image = std::move(*snap);
        }
        if (store && !rec.image_on_disk) rec.image = std::move(*snap);
        retire(rec);
        rec.imports.reset();
        schedule_wake(rec);
    } else if (store && rec.instance && (rec.restart || rec.image_on_disk) &&
               (rec.checkpoint_due || SteadyClock::now() - rec.last_checkpoint >=
                                          rec.checkpoint_interval.value_or(config.checkpoint_interval))) {
        lock.unlock();
        auto snap = snapshot(rec);
        lock.lock();
        if (killed()) return;
        if (snap && persist(rec, *snap)) {
            rec.last_checkpoint = SteadyClock::now();
            rec.checkpoint_due = false;
        } else if (checkpoint_requested) {
            fail(snap ? "checkpoint: the image could not be stored" : "checkpoint: " + snap.error());
            return;
        }
    }
    // The mesh gets them once the lane released the paglet (lane_loop).
    publish();
}

void Runtime::Impl::lane_loop(std::size_t index) {
    wasm::init_thread();
    Lane& lane = *lanes[index];
    std::unique_lock lock(mu);
    while (true) {
        lane.cv.wait(lock, [&] { return stopping || !lane.ready.empty(); });
        if (stopping) break;
        const PagletId id = std::move(lane.ready.front());
        lane.ready.pop_front();
        PagletRec* rec = find(id);
        if (rec == nullptr) continue;
        rec->queued = false;
        if (rec->mailbox.empty() || rec->running || rec->awaiting_image) {
            release_lane(*rec);
            continue;
        }
        auto node = rec->mailbox.extract(rec->mailbox.begin());
        rec->running = true;
        ++running_count;
        lock.unlock();

        bool remove = false;
        process(*rec, std::move(node.mapped()), remove);
        lock.lock();
        if (!remove && rec->kill_reason) {
            retire(*rec);
            end_paglet(*rec, true, *rec->kill_reason);
            remove = true;
        }
        if (!remove) {
            finish_call(*rec, remove, lock);
        }
        if (!remove && rec->kill_reason) {
            end_paglet(*rec, true, *rec->kill_reason);
            remove = true;
        }
        if (!remove) {
            rec->running = false;
            release_lane(*rec);
            make_ready(*rec);
        }
        --running_count;
        if (!outgoing.empty() && mobility.depart) {
            auto leaving = std::move(outgoing);
            outgoing.clear();
            auto depart = mobility.depart;
            lock.unlock();
            for (auto& d : leaving) depart(std::move(d));
            lock.lock();
        }
        if (!retired.empty()) {
            auto dead = std::move(retired);
            retired.clear();
            // The lane's worker keeps compiled modules that paglets placed
            // on the lane use or that the host's cache keeps.
            std::set<std::string> keep;
            for (const auto& [pid, p] : paglets) {
                if (p->loaded && p->lane == static_cast<int>(index)) keep.insert(p->module);
            }
            lock.unlock();
            dead.clear();
            for (auto& hash : modules->cached()) keep.insert(std::move(hash));
            lane.executor->retain_modules(keep);
            lock.lock();
        }
        idle_cv.notify_all();
    }
    lock.unlock();
    wasm::deinit_thread();
}

// Fires deadlines and enforces handler time budgets. Guest calls do not
// notify this thread (that would cost a wake-up per call); while calls are in
// progress it looks at them every few milliseconds instead.
void Runtime::Impl::clock_loop() {
    using namespace std::chrono_literals;
    std::unique_lock lock(mu);
    while (!stopping) {
        const auto now = SteadyClock::now();
        for (PagletRec* rec : in_call) {
            if (!rec->budget_exceeded && now >= rec->call_deadline && rec->instance) {
                rec->budget_exceeded = true;
                rec->instance->terminate();
            }
        }
        if (config.module_gc_interval.count() > 0 && now >= next_module_gc) {
            next_module_gc = now + config.module_gc_interval;
            lock.unlock();
            collect_modules();
            lock.lock();
            continue;
        }
        auto wake = now + (in_call.empty() ? 100ms : 5ms);
        if (config.module_gc_interval.count() > 0) wake = std::min(wake, next_module_gc);
        if (!deadlines.empty() && deadlines.begin()->first <= now) {
            Deadline d = std::move(deadlines.begin()->second);
            deadlines.erase(deadlines.begin());
            fire(std::move(d));
            continue;
        }
        if (!deadlines.empty()) wake = std::min(wake, deadlines.begin()->first);
        clock_cv.wait_until(lock, wake);
    }
}

void Runtime::Impl::fire(Deadline d) {
    switch (d.type) {
        case Deadline::Type::request_timeout: {
            if (PagletRec* rec = find(d.paglet)) {
                if (auto job = rec->jobs.find(d.id); job != rec->jobs.end()) {
                    if (!job->second.remote) {
                        if (job_pool && job->second.id) job_pool->cancel(job->second.id, abi::timeout);
                    } else if (job_hooks.cancel_remote) {
                        defer([cancel = job_hooks.cancel_remote, paglet = rec->id, correlation = d.id] {
                            cancel(paglet, correlation);
                        });
                    }
                }
            }
            // Requests of a paglet that left time out where it went.
            if (d.paglet.empty() || paglets.contains(d.paglet)) deliver_reply(d.paglet, d.id, abi::timeout, {}, {});
            break;
        }
        case Deadline::Type::wake: {
            if (PagletRec* rec = find(d.paglet); rec && rec->wake_at_ms == static_cast<std::int64_t>(d.id)) {
                if (rec->moving || rec->arriving || rec->dispatch_pending) {
                    schedule(SteadyClock::now() + std::chrono::milliseconds(10), d);
                    break;
                }
                if (wall_ms() < rec->wake_at_ms) { schedule_wake(*rec); break; }
                Envelope env;
                env.type = Envelope::Type::wake;
                env.wake_at_ms = rec->wake_at_ms;
                enqueue(*rec, std::move(env), 0);
            }
            break;
        }
        case Deadline::Type::timer: {
            PagletRec* rec = find(d.paglet);
            if (rec == nullptr) break;
            auto it = std::ranges::find_if(rec->caps, [&](const auto& e) {
                return e.second.kind == Cap::Kind::timer && e.second.timer_id == d.id;
            });
            if (it == rec->caps.end()) break;  // cancelled
            if (rec->moving || rec->arriving || rec->dispatch_pending) {
                schedule(SteadyClock::now() + std::chrono::milliseconds(10), d);
                break;
            }
            Cap t = std::move(it->second);
            rec->caps.erase(it);
            Envelope env;
            env.delivered.kind = abi::MessageKind::timer;
            env.delivered.name = std::move(t.message.name);
            env.delivered.payload = std::move(t.message.payload);
            env.delivered.priority = t.message.priority;
            enqueue(*rec, std::move(env), t.message.priority);
            break;
        }
    }
}

void Runtime::Impl::recover() {
    const Warn warn = [this](const std::string& text) { log(2, "", text); };
    for (auto& id : store->load_revoked(warn)) revoked_ids.insert(std::move(id));
    for (auto& r : store->load_paglets(warn)) {
        if (!r.restart) {  // stored before its policy changed; it does not come back
            store->remove_paglet(r.id);
            continue;
        }
        TrustClass trust = TrustClass::roaming;
        if (r.trust == "resident") trust = TrustClass::resident;
        if (r.trust == "system") trust = TrustClass::system;
        PagletRec* created = r.module.empty() ? nullptr : new_paglet(r.id, r.module, trust, r.owner);
        if (created == nullptr) {
            log(3, r.id, "recovery: module " + r.module + " missing; paglet dropped");
            continue;
        }
        PagletRec& rec = *created;
        rec.started = r.started;
        rec.next_handle = r.next_handle;
        rec.next_correlation = r.next_correlation;
        rec.next_timer = r.next_timer;
        if (r.checkpoint_ms >= 0) rec.checkpoint_interval = std::chrono::milliseconds(r.checkpoint_ms);
        rec.services = r.services;
        rec.marks.insert(r.marks.begin(), r.marks.end());
        rec.restart = r.restart;
        rec.notify_recovered = r.notify_recovered;
        rec.image_ms = r.image_ms;
        rec.wake_at_ms = r.wake_at_ms;
        schedule_wake(rec);
        rec.caps.clear();
        for (auto& [h, cap] : r.caps) {
            if (cap.kind == Cap::Kind::timer) {
                const auto delay = delay_until(cap.fire_at);
                schedule(SteadyClock::now() + delay,
                         Deadline{Deadline::Type::timer, rec.id, cap.timer_id});
            }
            rec.caps.emplace(h, std::move(cap));
        }
        rec.image_on_disk = true;
        log(1, rec.id, "recovered from its last image");
        if (rec.notify_recovered) {
            // Its first message: when the image was stored and when the
            // host came back.
            Envelope env;
            env.delivered.kind = abi::MessageKind::message;
            env.delivered.name = std::string(abi::recovered_message);
            env.delivered.payload = abi::encode(abi::Recovered{r.image_ms, wall_ms()});
            env.delivered.priority = abi::max_priority;
            enqueue(rec, std::move(env), abi::max_priority);
        }
        // Compute jobs ended with the host: `gone`, so the paglet submits
        // them again. Requests in flight at the crash are answered `timeout`.
        for (auto correlation : r.pending_jobs) {
            rec.pending_requests.insert(correlation);
            deliver_reply(rec.id, correlation, abi::gone, {}, {});
        }
        for (auto correlation : r.pending_requests) {
            if (std::ranges::find(r.pending_jobs, correlation) != r.pending_jobs.end()) continue;
            rec.pending_requests.insert(correlation);
            deliver_reply(rec.id, correlation, abi::timeout, {}, {});
        }
    }
}

// ---------------------------------------------------------------------------
// Compute jobs (ABI v1.3)

// Calls that must not run under the runtime's lock (hooks into the mesh,
// artifact stores), in order, on a thread of their own.
void Runtime::Impl::run_deferred() {
    std::unique_lock lock(mu);
    for (;;) {
        deferred_cv.wait(lock, [&] { return deferred_stop || !deferred.empty(); });
        if (deferred.empty()) return;
        auto f = std::move(deferred.front());
        deferred.pop_front();
        deferred_running = true;
        lock.unlock();
        f();
        lock.lock();
        deferred_running = false;
        deferred_cv.notify_all();
    }
}

// mu held.
std::expected<std::uint64_t, std::int32_t> Runtime::Impl::start_job(IncomingJob job) {
    if (!job_pool || stopping) return std::unexpected(abi::unsupported);
    if (!modules->info(job.module)) return std::unexpected(abi::not_found);
    if (!modules->add_user(job.module)) return std::unexpected(abi::not_found);
    std::uint32_t pages = 0;
    if (job.memory_mb > 0) {
        const auto mb = std::min<std::int64_t>(job.memory_mb, config.job_max_memory_mb);
        pages = static_cast<std::uint32_t>(mb * 1024 * 1024 / wasm::page_size);
    }
    JobRequest r{std::move(job.paglet), std::move(job.requester_host), job.correlation, std::move(job.owner),
                 job.module, std::move(job.function), std::move(job.input), job.cores, job.max_ms, pages,
                 std::move(job.marks)};
    auto id = job_pool->submit(std::move(r));
    if (!id) modules->remove_user(job.module);
    return id;
}

// mu held: the job's input comes from its artifact first (loaded on the
// deferred thread); a failure answers the paglet, wherever it is.
void Runtime::Impl::start_job_from_artifact(IncomingJob job) {
    if (!job_artifacts.load) {
        deliver_reply(job.paglet, job.correlation, abi::unsupported, {}, {}, job.requester_host);
        return;
    }
    const std::uint64_t max = std::uint64_t{config.job_max_memory_mb} * 1024 * 1024;
    Cap artifact = std::move(*job.input_artifact);
    job.input_artifact.reset();
    defer([this, load = job_artifacts.load, artifact = std::move(artifact), max, job = std::move(job)]() mutable {
        load(std::move(artifact), max,
             [this, job = std::move(job)](std::expected<JobArtifactInput, std::int32_t> input) mutable {
                 std::lock_guard lock(mu);
                 PagletRec* rec = job.requester_host.empty() ? find(job.paglet) : nullptr;
                 // A local paglet that cancelled meanwhile, or ended, waits no more.
                 if (job.requester_host.empty() && (rec == nullptr || !rec->jobs.contains(job.correlation))) return;
                 const auto correlation = job.correlation;
                 const PagletId paglet = job.paglet;
                 const std::string host = job.requester_host;
                 if (!input) {
                     deliver_reply(paglet, correlation, input.error(), {}, {}, host);
                     return;
                 }
                 for (auto& mark : input->marks) {
                     if (std::ranges::find(job.marks, mark) == job.marks.end()) job.marks.push_back(std::move(mark));
                 }
                 job.input = std::move(input->data);
                 auto id = start_job(std::move(job));
                 if (!id) {
                     deliver_reply(paglet, correlation, id.error(), {}, {}, host);
                 } else if (rec != nullptr) {
                     rec->jobs[correlation].id = *id;
                 }
             });
    });
}

// From the job pool, without the runtime's lock: the result goes to the
// paglet as the reply of its job, wherever it is now. An output too large
// for a message becomes an artifact.
void Runtime::Impl::job_done(JobOutcome o) {
    std::int32_t status = o.status;
    Bytes payload;
    std::vector<Cap> caps;
    std::string host;
    {
        std::lock_guard lock(mu);
        modules->remove_user(o.module);
        // A host that stops keeps the paglet's pending job: after a restart
        // the paglet hears `gone` and submits it again.
        if (stopping) return;
        host = mobility.host_id.empty() ? config.host_name : mobility.host_id;
    }
    if (status == abi::ok) {
        abi::JobResult r;
        r.marks = o.marks;
        r.host = host;
        r.elapsed_ms = o.elapsed_ms;
        r.queued_ms = o.queued_ms;
        r.aot = o.aot;
        if (o.output.size() > abi::max_document_size - 4096) {
            auto cap = job_artifacts.store ? job_artifacts.store(o.output, o.marks)
                                           : std::expected<Cap, std::int32_t>(std::unexpected(abi::too_large));
            if (cap) {
                r.artifact = true;
                caps.push_back(std::move(*cap));
            } else {
                status = cap.error();
            }
        } else {
            r.output = std::move(o.output);
        }
        if (status == abi::ok) payload = abi::encode(r);
    }
    if (status != abi::ok) payload.assign(o.error.begin(), o.error.end());
    std::lock_guard lock(mu);
    if (stopping) return;
    deliver_reply(o.paglet, o.correlation, status, std::move(payload), std::move(caps), o.requester_host,
                  std::move(o.marks));
}

// mu held: stops the paglet's jobs; their outcomes (`status`) arrive later.
// A departing paglet keeps the jobs that are to finish.
void Runtime::Impl::cancel_jobs(PagletRec& rec, std::int32_t status, bool departing) {
    std::vector<std::uint64_t> loading;  // local jobs still waiting for their input
    for (auto& [correlation, job] : rec.jobs) {
        if (departing && job.finish) continue;
        if (!job.remote) {
            if (job.id == 0) {
                loading.push_back(correlation);
            } else if (job_pool) {
                job_pool->cancel(job.id, status);
            }
        } else if (job_hooks.cancel_remote) {
            defer([cancel = job_hooks.cancel_remote, paglet = rec.id, correlation] { cancel(paglet, correlation); });
        }
    }
    for (auto correlation : loading) deliver_reply(rec.id, correlation, status, {}, {});
}

// ---------------------------------------------------------------------------
// Public API

Runtime::Runtime(Config config) : impl_(std::make_unique<Impl>(std::move(config))) {
    wasm::ensure_runtime();
    const unsigned n = std::max(1u, impl_->config.threads);
    for (unsigned i = 0; i < n; ++i) {
        auto lane = std::make_unique<Impl::Lane>();
        if (!impl_->config.worker_executable.empty()) {
            auto worker = make_worker_executor(impl_->config.worker_executable, impl_->config.sandbox_workers,
                                               [this](const std::string& text) {
                                                   std::lock_guard lock(impl_->mu);
                                                   impl_->log(2, "", text);
                                               });
            if (!worker) throw std::runtime_error(worker.error());
            if (i == 0) {
                if (auto ready = (*worker)->start(); !ready) throw std::runtime_error(ready.error());
            }
            lane->executor = std::move(*worker);
            impl_->uses_workers = true;
        }
        if (!lane->executor) lane->executor = make_in_process_executor();
        impl_->lanes.push_back(std::move(lane));
    }
    impl_->log(1, "", impl_->uses_workers ? "execution mode: worker processes" : "execution mode: in-process");
    // Scratch space starts empty: with a state directory under it, else in a
    // temporary directory of this runtime.
    std::error_code ec;
    if (!impl_->config.state_dir.empty()) {
        impl_->storage_root = impl_->config.state_dir / "storage";
        impl_->work_root = impl_->config.state_dir / "work";
        std::filesystem::remove_all(impl_->work_root, ec);
    } else {
        std::array<std::uint8_t, 8> suffix{};
        wasm::fill_random(suffix);
        impl_->work_root = std::filesystem::temp_directory_path(ec) /
                           ("paglets-work-" + to_hex(std::span<const std::uint8_t>(suffix)));
        impl_->own_work_root = true;
    }
    {
        ModuleStoreConfig mc;
        if (!impl_->config.state_dir.empty()) mc.dir = impl_->config.state_dir / "modules";
        mc.cache_bytes = impl_->config.module_cache_bytes;
        impl_->modules = std::make_unique<ModuleStore>(std::move(mc), [impl = impl_.get()](const std::string& text) {
            std::lock_guard lock(impl->mu);
            impl->log(2, "", text);
        });
    }
    impl_->next_module_gc = SteadyClock::now() + impl_->config.module_gc_interval;
    if (!impl_->config.state_dir.empty()) {
        impl_->store.emplace(impl_->config.state_dir);
        std::lock_guard lock(impl_->mu);
        impl_->recover();
    }
    {
        // Messages queued during recovery (replies `timeout`, `recovered`).
        std::lock_guard lock(impl_->mu);
        for (auto& [id, rec] : impl_->paglets) impl_->make_ready(*rec);
    }
    {
        // Compute jobs: a pool of their own, away from the lanes.
        const Config& c = impl_->config;
        JobPool::Settings js;
        js.slots = c.job_slots;
        js.worker = c.worker_executable;
        js.sandbox = c.sandbox_workers;
        js.default_max_ms = c.job_default_time.count();
        js.cap_max_ms = c.job_max_time.count();
        js.default_pages = static_cast<std::uint32_t>(std::uint64_t{c.job_memory_mb} * 1024 * 1024 / wasm::page_size);
        js.max_pages = static_cast<std::uint32_t>(std::uint64_t{c.job_max_memory_mb} * 1024 * 1024 / wasm::page_size);
        js.owner_limit = c.job_owner_limit;
        js.aot_compiler = c.aot_compiler;
        js.aot_cache = c.aot_cache;
        if (js.aot_cache.empty() && !c.state_dir.empty()) js.aot_cache = c.state_dir / "aot";
        js.hw_bound_check = PAGLETS_WAMR_HW_BOUND_CHECK != 0;
        js.host_name = c.host_name;
        Impl* impl = impl_.get();
        js.acquire = [impl](const std::string& hash) { return impl->modules->acquire(hash); };
        impl_->job_pool = std::make_unique<JobPool>(
            std::move(js), [impl](JobOutcome o) { impl->job_done(std::move(o)); },
            [impl](std::int32_t level, const std::string& text) {
                std::lock_guard lock(impl->mu);
                impl->log(level, "", text);
            });
        impl_->deferred_thread = std::thread([impl] { impl->run_deferred(); });
    }
    for (unsigned i = 0; i < n; ++i) {
        impl_->lanes[i]->thread = std::thread([this, i] { impl_->lane_loop(i); });
    }
    impl_->clock = std::thread([this] { impl_->clock_loop(); });
}

Runtime::~Runtime() {
    shutdown();
}

void Runtime::shutdown(bool save_state) {
    {
        std::lock_guard lock(impl_->mu);
        if (impl_->stopping) return;
        impl_->stopping = true;
    }
    for (auto& lane : impl_->lanes) lane->cv.notify_all();
    impl_->clock_cv.notify_all();
    for (auto& lane : impl_->lanes) lane->thread.join();
    if (impl_->clock.joinable()) impl_->clock.join();
    // Running jobs end; their paglets keep them as pending (`gone` after a
    // restart). Deferred calls still run, then their thread ends.
    if (impl_->job_pool) impl_->job_pool->shutdown();
    {
        std::lock_guard lock(impl_->mu);
        impl_->deferred_stop = true;
        impl_->deferred_cv.notify_all();
    }
    if (impl_->deferred_thread.joinable()) impl_->deferred_thread.join();
    // System paglets end the work they do on threads of their own.
    std::vector<std::shared_ptr<SystemPaglet>> natives;
    {
        std::lock_guard lock(impl_->mu);
        for (auto& [id, rec] : impl_->paglets) {
            if (rec->native) natives.push_back(rec->native);
        }
    }
    for (auto& n : natives) n->stop();

    std::vector<PagletRec*> recs;
    {
        std::lock_guard lock(impl_->mu);
        for (auto& [c, p] : impl_->external) p.set_value(Reply{abi::gone, {}});
        impl_->external.clear();
        for (auto& [id, rec] : impl_->paglets) recs.push_back(rec.get());
    }
    // No handler runs any more: a graceful stop stores the latest state of
    // every active paglet (after a crash, the last checkpoint counts).
    std::vector<std::unique_ptr<Placed>> instances;
    for (PagletRec* rec : recs) {
        if (save_state && impl_->store && rec->instance && rec->started) {
            if (auto snap = rec->instance->capture()) {
                std::lock_guard lock(impl_->mu);
                impl_->persist(*rec, *snap);
            }
        }
        std::lock_guard lock(impl_->mu);
        if (rec->instance) instances.push_back(std::move(rec->instance));
    }
    {
        // Retired instances too, but not under the runtime's lock: ending a
        // worker instance takes the worker's lock, which lanes hold while
        // they call into the runtime.
        std::lock_guard lock(impl_->mu);
        for (auto& r : impl_->retired) instances.push_back(std::move(r));
        impl_->retired.clear();
    }
    instances.clear();  // before the records and executors they refer to
    impl_->modules->flush();
    std::lock_guard lock(impl_->mu);
    if (impl_->own_work_root) {
        std::error_code ec;
        std::filesystem::remove_all(impl_->work_root, ec);
    }
}

const Config& Runtime::config() const {
    return impl_->config;
}

std::expected<std::string, std::string> Runtime::add_module(Bytes bytes) {
    return impl_->modules->add(std::move(bytes));
}

ModuleStore& Runtime::modules() {
    return *impl_->modules;
}

std::vector<std::string> Runtime::worker_modules() const {
    std::set<std::string> out;
    for (const auto& lane : impl_->lanes) {
        for (auto& hash : lane->executor->loaded_modules()) out.insert(std::move(hash));
    }
    return {out.begin(), out.end()};
}

std::expected<std::string, std::string> Runtime::add_module_file(const std::filesystem::path& path) {
    auto bytes = wasm::read_file(path.string());
    if (!bytes) return std::unexpected(bytes.error());
    return add_module(std::move(*bytes));
}

std::expected<PagletId, std::string> Runtime::create(std::string_view module, CreateOptions options) {
    std::lock_guard lock(impl_->mu);
    auto info = impl_->modules->info(module);
    if (!info) return std::unexpected("unknown module " + std::string(module));
    if (auto ok = check_paglet_module(*info, options.trust); !ok) return std::unexpected(ok.error());
    if (auto why = impl_->refusal(std::string(module), options.trust); !why.empty()) return std::unexpected(why);
    PagletId id = new_paglet_id();
    if (options.id) {
        const bool hex =
            options.id->size() == 32 && options.id->find_first_not_of("0123456789abcdef") == std::string::npos;
        if (!hex) return std::unexpected("a paglet ID has 32 lowercase hex digits");
        if (impl_->paglets.contains(*options.id) || impl_->endings.contains(*options.id)) {
            return std::unexpected("paglet " + *options.id + " exists or existed");
        }
        id = *options.id;
    }
    if (options.trust != TrustClass::system && impl_->owner_full(options.owner)) {
        return std::unexpected("quota: the owner has as many paglets here as one owner may have");
    }
    PagletRec* created = impl_->new_paglet(id, std::string(module), options.trust, std::move(options.owner));
    if (created == nullptr) return std::unexpected("unknown module " + std::string(module));
    PagletRec& rec = *created;
    rec.checkpoint_interval = options.checkpoint_interval;
    impl_->install_services(rec);
    impl_->enqueue_start(rec, abi::EventKind::created, std::move(options.args), {}, std::nullopt);
    return id;
}

std::expected<void, std::int32_t> Runtime::send(const PagletId& to, std::string_view name, Bytes payload,
                                                std::int32_t priority, std::string_view sender_owner) {
    abi::OutMessage m{std::string(name), std::move(payload), {}, priority, abi::default_timeout_ms};
    if (auto v = Impl::validate(m); v != abi::ok) return std::unexpected(v);
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(to);
    if (rec == nullptr) return std::unexpected(abi::not_found);
    if (rec->mailbox.size() >= impl_->config.mailbox_limit) return std::unexpected(abi::quota);
    Envelope env;
    env.delivered.kind = abi::MessageKind::message;
    env.delivered.name = std::move(m.name);
    env.delivered.payload = std::move(m.payload);
    env.delivered.priority = priority;
    env.delivered.sender = impl_->host_sender(sender_owner);
    impl_->enqueue(*rec, std::move(env), priority);
    return {};
}

std::future<Reply> Runtime::request(const PagletId& to, std::string_view name, Bytes payload,
                                    std::chrono::milliseconds timeout, std::string_view sender_owner) {
    std::promise<Reply> promise;
    auto future = promise.get_future();
    abi::OutMessage m{std::string(name), std::move(payload), {}, abi::default_priority, timeout.count()};
    if (auto v = Impl::validate(m); v != abi::ok) {
        promise.set_value(Reply{v, {}});
        return future;
    }
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(to);
    if (rec == nullptr || impl_->stopping) {
        promise.set_value(Reply{abi::not_found, {}});
        return future;
    }
    if (rec->mailbox.size() >= impl_->config.mailbox_limit) {
        promise.set_value(Reply{abi::quota, {}});
        return future;
    }
    const std::uint64_t correlation = impl_->next_external++;
    impl_->external.emplace(correlation, std::move(promise));
    Cap reply;
    reply.kind = Cap::Kind::reply;
    reply.correlation = correlation;  // target empty: the host
    reply.transferable = true;
    Envelope env;
    env.delivered.kind = abi::MessageKind::request;
    env.delivered.name = std::move(m.name);
    env.delivered.payload = std::move(m.payload);
    env.delivered.priority = abi::default_priority;
    env.delivered.sender = impl_->host_sender(sender_owner);
    env.reply_cap = std::move(reply);
    env.deadline_ms = wall_deadline(timeout.count());
    env.expires_at = SteadyClock::now() + bounded_delay(timeout.count());
    impl_->enqueue(*rec, std::move(env), abi::default_priority);
    impl_->schedule(SteadyClock::now() + bounded_delay(timeout.count()), Deadline{Deadline::Type::request_timeout, "", correlation});
    return future;
}

Reply Runtime::call(const PagletId& to, std::string_view name, Bytes payload, std::chrono::milliseconds timeout,
                    std::string_view sender_owner) {
    return request(to, name, std::move(payload), timeout, sender_owner).get();
}

std::expected<void, std::int32_t> Runtime::deactivate(const PagletId& id) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    if (rec == nullptr) return std::unexpected(abi::not_found);
    if (rec->native) return std::unexpected(abi::denied);
    if (rec->moving || rec->arriving || rec->dispatch_pending || rec->in_end_hook)
        return std::unexpected(abi::bad_state);
    Envelope env;
    env.type = Envelope::Type::deactivate;
    impl_->enqueue(*rec, std::move(env), control_priority);
    return {};
}

std::expected<void, std::int32_t> Runtime::dispose(const PagletId& id) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    if (rec == nullptr) return std::unexpected(abi::not_found);
    if (rec->native) return std::unexpected(abi::denied);
    if (rec->moving || rec->arriving || rec->dispatch_pending || rec->in_end_hook)
        return std::unexpected(abi::bad_state);
    Envelope env;
    env.type = Envelope::Type::dispose;
    impl_->enqueue(*rec, std::move(env), control_priority);
    return {};
}

std::expected<void, std::int32_t> Runtime::terminate(const PagletId& id, std::string reason) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    if (rec == nullptr) return std::unexpected(abi::not_found);
    if (rec->native) return std::unexpected(abi::denied);
    if (rec->moving || rec->arriving) return std::unexpected(abi::bad_state);  // the mesh decides first
    if (rec->running) {
        // The lane ends it when the call returns.
        rec->kill_reason = std::move(reason);
        if (rec->instance) rec->instance->terminate();
        return {};
    }
    impl_->retire(*rec);
    impl_->end_paglet(*rec, true, std::move(reason));
    impl_->idle_cv.notify_all();
    return {};
}

void Runtime::set_module_admission(ModuleAdmission admission) {
    std::lock_guard lock(impl_->mu);
    impl_->admission = std::move(admission);
}

// ---------------------------------------------------------------------------
// Compute jobs

std::vector<JobInfo> Runtime::jobs() const {
    return impl_->job_pool ? impl_->job_pool->list() : std::vector<JobInfo>{};
}

bool Runtime::cancel_job(std::uint64_t id) {
    return impl_->job_pool && impl_->job_pool->cancel(id, abi::cancelled);
}

void Runtime::set_job_slots(unsigned slots) {
    if (impl_->job_pool) impl_->job_pool->set_slots(slots);
}

bool Runtime::reserve_compute_slots(std::int64_t cores) {
    return reserve_compute_slots_for(cores, std::chrono::steady_clock::now());
}

bool Runtime::reserve_compute_slots_for(std::int64_t cores, std::chrono::steady_clock::time_point queued_at) {
    return impl_->job_pool && impl_->job_pool->reserve_slots(cores, queued_at);
}

void Runtime::set_compute_waiter(std::int64_t cores, std::chrono::steady_clock::time_point queued_at) {
    if (impl_->job_pool) impl_->job_pool->set_waiter(cores, queued_at);
}

void Runtime::release_compute_slots(std::int64_t cores) {
    if (impl_->job_pool) impl_->job_pool->release_slots(cores);
}

unsigned Runtime::job_slots() const {
    return impl_->job_pool ? impl_->job_pool->slots() : 0;
}

std::int64_t Runtime::job_cores_running() const {
    return impl_->job_pool ? impl_->job_pool->cores_running() : 0;
}

std::size_t Runtime::jobs_queued() const {
    return impl_->job_pool ? impl_->job_pool->queued() : 0;
}

bool Runtime::job_aot() const {
    return impl_->job_pool && impl_->job_pool->aot_available();
}

bool Runtime::compile_job_module(const std::string& module, std::chrono::milliseconds timeout) {
    if (!impl_->job_pool) return false;
    auto m = impl_->modules->acquire(module);
    return m && impl_->job_pool->compile_now(*m, timeout);
}

bool Runtime::wait_jobs_idle(std::chrono::milliseconds timeout) {
    if (!impl_->job_pool) return true;
    const auto until = SteadyClock::now() + timeout;
    if (!impl_->job_pool->wait_idle(timeout)) return false;
    // The last outcome may still be on its way to its paglet.
    std::unique_lock lock(impl_->mu);
    while ((!impl_->deferred.empty() || impl_->deferred_running) && SteadyClock::now() < until) {
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        lock.lock();
    }
    return impl_->deferred.empty() && !impl_->deferred_running;
}

// Calls already queued with the old hooks run first (the node clears its
// hooks before it goes).
void Runtime::set_job_hooks(JobHooks hooks) {
    std::unique_lock lock(impl_->mu);
    impl_->job_hooks = std::move(hooks);
    if (impl_->deferred_thread.get_id() == std::this_thread::get_id()) return;
    impl_->deferred_cv.wait(lock, [&] {
        return impl_->deferred_stop || (impl_->deferred.empty() && !impl_->deferred_running);
    });
}

void Runtime::set_job_artifacts(JobArtifacts artifacts) {
    std::lock_guard lock(impl_->mu);
    impl_->job_artifacts = std::move(artifacts);
}

std::expected<std::uint64_t, std::int32_t> Runtime::submit_job(IncomingJob job) {
    std::lock_guard lock(impl_->mu);
    if (impl_->stopping || !impl_->job_pool) return std::unexpected(abi::unsupported);
    if (job.input_artifact) {
        impl_->start_job_from_artifact(std::move(job));
        return 0;  // started once the input is loaded
    }
    return impl_->start_job(std::move(job));
}

bool Runtime::cancel_job_of(const PagletId& paglet, std::uint64_t correlation, const std::string& requester_host) {
    if (!impl_->job_pool) return false;
    for (const auto& j : impl_->job_pool->list()) {
        if (j.paglet == paglet && j.correlation == correlation && j.requester_host == requester_host) {
            return impl_->job_pool->cancel(j.id, abi::cancelled);
        }
    }
    return false;
}

void Runtime::note_job(const PagletId& paglet, std::uint64_t correlation, abi::JobStatus status) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(paglet);
    if (rec == nullptr) return;
    if (auto it = rec->jobs.find(correlation); it != rec->jobs.end()) it->second.status = std::move(status);
}

void Runtime::fail_job(const PagletId& paglet, std::uint64_t correlation, std::int32_t status) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(paglet);
    if (rec == nullptr || !rec->jobs.contains(correlation)) return;
    impl_->deliver_reply(paglet, correlation, status, {}, {});
}

std::vector<std::pair<PagletId, std::uint64_t>> Runtime::remote_jobs() const {
    std::lock_guard lock(impl_->mu);
    std::vector<std::pair<PagletId, std::uint64_t>> out;
    for (const auto& [id, rec] : impl_->paglets) {
        for (const auto& [correlation, job] : rec->jobs) {
            if (job.remote) out.emplace_back(id, correlation);
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// Movement between hosts

namespace {

std::string describe_cap(std::int32_t handle, const Cap& c) {
    std::string what;
    switch (c.kind) {
        case Cap::Kind::endpoint: what = "endpoint " + c.target; break;
        case Cap::Kind::reply: what = "reply"; break;
        case Cap::Kind::timer: what = "timer"; break;
        case Cap::Kind::resource: what = c.resource_type + " " + c.resource; break;
    }
    return "handle " + std::to_string(handle) + ": " + what;
}

}  // namespace

void Runtime::set_mobility(MobilityHooks hooks) {
    std::lock_guard lock(impl_->mu);
    impl_->mobility = std::move(hooks);
}

std::int32_t Runtime::deliver_remote(RemoteMessage m) {
    Impl& rt = *impl_;
    std::lock_guard lock(rt.mu);
    if (m.hops < 0 || (m.failure && (m.kind != RemoteMessage::Kind::message ||
        m.failure->paglet.empty() || m.failure->host.empty() || m.failure->handle <= 0))) return abi::invalid_argument;
    // Forwards a message for a paglet that left (a bounded number of times).
    auto forward = [&](RemoteMessage& fwd) -> bool {
        const std::string where = rt.remote_host(fwd.target, {});
        if (where.empty() || where == rt.mobility.host_id || fwd.hops >= max_forwards) return false;
        fwd.host = where;
        ++fwd.hops;
        rt.send_remote(std::move(fwd));
        return true;
    };
    if (m.kind == RemoteMessage::Kind::undelivered) return rt.deliver_undelivered(std::move(m));
    // Requests receive replies; asynchronous one-way sends receive diagnostics.
    auto refuse = [&](RemoteMessage& req, std::int32_t status) {
        if (req.kind == RemoteMessage::Kind::request) {
            rt.deliver_reply(req.requester, req.correlation, status, {}, {}, req.requester_host);
        } else if (req.kind == RemoteMessage::Kind::message) {
            const auto name = abi::valid_message_name(req.name) ? req.name : "<invalid name>";
            rt.log(static_cast<std::int32_t>(abi::LogLevel::warning), req.target,
                   "remote one-way message refused: " + name + " (" + std::string(abi::error_name(status)) + ")");
            if (auto report = undelivered_report(req, status)) rt.deliver_undelivered(std::move(*report));
        }
        return status;
    };
    if (m.kind == RemoteMessage::Kind::reply) {
        if (!m.target.empty() && !rt.paglets.contains(m.target)) {
            if (!forward(m)) {
                if (rt.mobility.unresolved && m.hops < max_forwards) {
                    rt.mobility.unresolved(std::move(m));
                } else {
                    for (auto& c : m.caps) rt.drop_cap(std::move(c), abi::gone);
                }
            }
            return abi::ok;
        }
        rt.deliver_reply(m.target, m.correlation, m.status, std::move(m.payload), std::move(m.caps), {},
                         std::move(m.residency_marks));
        return abi::ok;
    }
    if (m.kind == RemoteMessage::Kind::request &&
        (m.deadline_ms < 0 || m.timeout_ms < 0)) return refuse(m, abi::invalid_argument);
    if (m.kind == RemoteMessage::Kind::request &&
        ((m.deadline_ms != 0 && wall_ms() >= m.deadline_ms) ||
         (m.expires_at && SteadyClock::now() >= *m.expires_at))) return refuse(m, abi::timeout);
    if (!abi::valid_message_name(m.name) || abi::reserved_message_name(m.name)) {
        return refuse(m, abi::invalid_argument);
    }
    PagletRec* rec = rt.find(m.target);
    if (rec == nullptr) {
        if (forward(m)) return abi::ok;
        // The mesh's location records may know where it went.
        if (rt.mobility.unresolved && m.hops < max_forwards && rt.remote_host(m.target, {}).empty()) {
            rt.mobility.unresolved(std::move(m));
            return abi::ok;
        }
        return refuse(m, abi::not_found);
    }
    if (rec->native) return refuse(m, abi::denied);  // system paglets serve their own host
    if (rec->mailbox.size() >= rt.config.mailbox_limit) return refuse(m, abi::quota);
    Envelope env;
    env.delivered.kind = m.kind == RemoteMessage::Kind::request ? abi::MessageKind::request : abi::MessageKind::message;
    env.delivered.name = std::move(m.name);
    env.delivered.payload = std::move(m.payload);
    env.delivered.priority = std::clamp(m.priority, 0, abi::max_priority);
    env.delivered.sender = std::move(m.sender);
    env.delivered.badge = std::move(m.badge);
    env.failure = std::move(m.failure);
    for (auto& c : m.caps) {
        // Endpoints to paglets and artifacts that name their host; a host's
        // other system paglets are its own.
        if (c.kind == Cap::Kind::endpoint && !c.target.starts_with("system.")) {
            if (c.host == rt.mobility.host_id) c.host.clear();
            env.caps.push_back(std::move(c));
        } else if (mesh_artifact(c) && rt.paglets.contains(c.target)) {
            env.caps.push_back(std::move(c));
        }
    }
    if (m.kind == RemoteMessage::Kind::request) {
        env.deadline_ms = m.deadline_ms;
        if (m.deadline_ms != 0 || m.timeout_ms > 0) {
            auto remaining = m.deadline_ms != 0 ? m.deadline_ms - wall_ms() : m.timeout_ms;
            if (m.timeout_ms > 0) remaining = std::min(remaining, m.timeout_ms);
            env.expires_at = SteadyClock::now() + bounded_delay(remaining);
            if (m.expires_at) env.expires_at = std::min(*env.expires_at, *m.expires_at);
            if (env.deadline_ms == 0) env.deadline_ms = wall_deadline(remaining);
        }
        Cap reply;
        reply.kind = Cap::Kind::reply;
        reply.target = std::move(m.requester);
        reply.correlation = m.correlation;
        reply.transferable = true;
        reply.host = std::move(m.requester_host);
        env.reply_cap = std::move(reply);
    }
    const std::int32_t priority = env.delivered.priority;
    rt.enqueue(*rec, std::move(env), priority);
    return abi::ok;
}

void Runtime::finish_departure(std::uint64_t move, std::optional<std::string> host, std::string reason) {
    Impl& rt = *impl_;
    std::lock_guard lock(rt.mu);
    auto node = rt.departures.extract(move);
    if (node.empty()) return;
    const Impl::Moving& mv = node.mapped();
    const auto ttl = SteadyClock::now() + rt.config.tombstone_ttl;
    if (!mv.clone.empty()) {
        PagletRec* original = rt.find(mv.paglet);
        if (host) {
            // Endpoints to the clone name its host.
            for (auto& [id, rec] : rt.paglets) {
                for (auto& [h, c] : rec->caps) {
                    if (c.kind == Cap::Kind::endpoint && c.target == mv.clone) c.host = *host;
                }
            }
            rt.tombstones[mv.clone] = Impl::Tombstone{*host, ttl};
        } else if (original != nullptr) {
            rt.move_failed(*original, mv.destination, reason, mv.clone);
        }
        return;
    }
    PagletRec* rec = rt.find(mv.paglet);
    if (rec == nullptr || !rec->moving) return;
    if (!host) {
        rec->moving = false;
        rec->dispatch_pending = false;
        rt.log(2, rec->id, "dispatch to " + mv.destination + " failed: " + reason);
        rt.move_failed(*rec, mv.destination, std::move(reason), {});
        rt.make_ready(*rec);
        return;
    }
    // The paglet is on `host` now: messages that waited here follow it.
    rt.tombstones[rec->id] = Impl::Tombstone{*host, ttl};
    // Its jobs stop, unless they are to finish: then their results follow
    // it like any reply. Either way the reply reaches it there.
    rt.cancel_jobs(*rec, abi::cancelled, true);
    // Failures may target this very paglet. Deliver them after removing the
    // source record so they use its tombstone, rather than being inserted
    // into the mailbox that is currently being drained and then cleared.
    std::vector<std::pair<Cap, std::int32_t>> failed_caps;
    std::vector<RemoteMessage> failed_sends;
    auto defer_drop = [&](Cap cap, std::int32_t status) { failed_caps.emplace_back(std::move(cap), status); };
    for (auto& [key, env] : rec->mailbox) {
        if (mv.captured_ready.contains(key.second)) {
            // Portable authority is already staged with the image. Release only
            // nonportable capabilities represented by an unsupported response.
            for (auto& cap : env.caps) if (!rt.portable_cap(cap)) defer_drop(std::move(cap), abi::gone);
            continue;
        }
        if (env.type != Envelope::Type::message) {
            if (env.reply_cap) defer_drop(std::move(*env.reply_cap), abi::gone);
            continue;
        }
        if (env.expires_at && SteadyClock::now() >= *env.expires_at) {
            if (env.reply_cap) defer_drop(std::move(*env.reply_cap), abi::timeout);
            for (auto& cap : env.caps) defer_drop(std::move(cap), abi::gone);
            continue;
        }
        if (!env.lent.empty() || !std::ranges::all_of(env.caps, [&](const Cap& c) { return rt.portable_cap(c); })) {
            // A locally accepted request cannot silently lose its authority
            // merely because the receiver moved before handling it.
            if (env.reply_cap) defer_drop(std::move(*env.reply_cap), env.lent.empty() ? abi::unsupported : abi::invalid_argument);
            for (auto& cap : env.caps) defer_drop(std::move(cap), abi::gone);
            if (env.failure) {
                RemoteMessage failed;
                failed.name = env.delivered.name;
                failed.failure = std::move(env.failure);
                if (auto report = undelivered_report(failed, env.lent.empty() ? abi::unsupported : abi::invalid_argument))
                    failed_sends.push_back(std::move(*report));
            }
            if (env.delivered.kind != abi::MessageKind::reply) continue;
            env.caps.clear();
            env.delivered.payload.clear();
            env.delivered.status = abi::unsupported;
        }
        RemoteMessage m;
        m.deadline_ms = env.deadline_ms;
        m.expires_at = env.expires_at;
        if (env.expires_at) m.timeout_ms = std::max<std::int64_t>(1,
            std::chrono::ceil<std::chrono::milliseconds>(*env.expires_at - SteadyClock::now()).count());
        m.target = rec->id;
        m.host = *host;
        m.name = std::move(env.delivered.name);
        m.payload = std::move(env.delivered.payload);
        m.priority = env.delivered.priority;
        m.sender = std::move(env.delivered.sender);
        m.badge = std::move(env.delivered.badge);
        m.caps = rt.portable_caps(std::move(env.caps));
        m.residency_marks = std::move(env.residency_marks);
        m.failure = std::move(env.failure);
        switch (env.delivered.kind) {
            case abi::MessageKind::reply:
                m.kind = RemoteMessage::Kind::reply;
                m.correlation = env.delivered.correlation;
                m.status = env.delivered.status;
                break;
            case abi::MessageKind::request: {
                m.kind = RemoteMessage::Kind::request;
                Cap reply = rt.portable(std::move(*env.reply_cap));
                m.requester = std::move(reply.target);
                m.requester_host = std::move(reply.host);
                m.correlation = reply.correlation;
                break;
            }
            default:
                m.kind = m.name == abi::undelivered_message ? RemoteMessage::Kind::undelivered : RemoteMessage::Kind::message;
                break;
        }
        rt.send_remote(std::move(m));
    }
    rec->mailbox.clear();
    if (rt.store) rt.store->remove_paglet(rec->id);
    rt.remove_directories(rec->id);
    rt.notify_ended(rec->id);
    rt.release_lane(*rec);
    rt.log(1, rec->id, "moved to host " + host->substr(0, 16));
    if (rt.mobility.moved) rt.mobility.moved(rec->id, host, {});
    rt.erase_paglet(rec->id);
    for (auto& [cap, status] : failed_caps) rt.drop_cap(std::move(cap), status);
    for (auto& report : failed_sends) rt.deliver_undelivered(std::move(report));
    rt.idle_cv.notify_all();
}

std::expected<std::vector<std::string>, std::string> Runtime::prepare_arrival(Arrival a, const RecreateCap& recreate) {
    Impl& rt = *impl_;
    std::lock_guard lock(rt.mu);
    const TravelState& st = a.state;
    if (st.pending_captured_ms < 0) return std::unexpected("invalid pending request capture time");
    std::set<std::uint64_t> completed;
    std::size_t ready_bytes = 0;
    for (const auto& message : st.ready) {
        const auto& d = message.delivered;
        const auto size = encode_ready(message).size() + 5;
        if (size > rt.config.move_ready_bytes - ready_bytes) return std::unexpected("arrival ready notifications exceed the move byte budget");
        ready_bytes += size;
        if (a.clone || d.sender || d.badge || d.reply != 0 || !d.caps.empty() || d.priority < 0 || d.priority > abi::max_priority)
            return std::unexpected("invalid arrival ready notification authority");
        if (d.kind == abi::MessageKind::reply) {
            if (d.name != "reply" || d.correlation == 0 || d.correlation >= st.next_correlation ||
                !completed.insert(d.correlation).second || std::ranges::any_of(st.pending, [&](const auto& p) { return p.first == d.correlation; }) ||
                !std::ranges::all_of(message.caps, [&](const Cap& c) { return rt.portable_cap(c); }))
                return std::unexpected("invalid arrival ready reply");
        } else if (d.kind == abi::MessageKind::timer) {
            if (!abi::valid_message_name(d.name) || abi::reserved_message_name(d.name) || d.correlation || d.status || !message.caps.empty() || !message.residency_marks.empty())
                return std::unexpected("invalid arrival ready timer");
        } else if (d.kind == abi::MessageKind::message) {
            if (!Impl::host_report(d.name) || d.correlation || d.status || d.priority != abi::max_priority || !message.caps.empty() || !message.residency_marks.empty())
                return std::unexpected("invalid arrival ready host report");
        } else return std::unexpected("invalid arrival ready notification kind");
    }
    const bool hex = st.id.size() == 32 && st.id.find_first_not_of("0123456789abcdef") == std::string::npos;
    if (!hex) return std::unexpected(std::string("invalid paglet ID"));
    if (rt.paglets.contains(st.id) || rt.endings.contains(st.id)) {
        return std::unexpected("paglet " + st.id + " exists here");
    }
    auto info = rt.modules->info(st.module);
    if (!info) return std::unexpected("module " + st.module + " is not here");
    if (to_hex(a.image.module_hash) != st.module) return std::unexpected(std::string("the image is of another module"));
    // An arriving paglet is always roaming (security design, section 2).
    if (auto ok = check_paglet_module(*info, TrustClass::roaming); !ok) return std::unexpected(ok.error());
    if (auto why = rt.refusal(st.module, TrustClass::roaming); !why.empty()) return std::unexpected(why);
    if (rt.owner_full(st.owner)) return std::unexpected(std::string("quota: its owner has as many paglets here as one owner may have"));
    PagletRec* created = rt.new_paglet(st.id, st.module, TrustClass::roaming, st.owner);
    if (created == nullptr) return std::unexpected("module " + st.module + " is not here");
    PagletRec& rec = *created;
    rt.tombstones.erase(st.id);  // it is back
    std::vector<std::string> lost;
    std::set<std::int32_t> service_handles;
    for (const auto& [name, h] : st.services) service_handles.insert(h);
    for (const auto& [h, c] : st.caps) {
        if (h == abi::self_handle) continue;  // new_paglet made the own endpoint
        std::optional<Cap> here;
        switch (c.kind) {
            case Cap::Kind::endpoint:
                if (c.target.starts_with("system.")) {
                    PagletRec* sys = rt.find(c.target);
                    if (sys == nullptr || !sys->native) break;
                    if (service_handles.contains(h)) {
                        // Default service endpoints: what this host's service offers.
                        auto ops = sys->native->default_ops();
                        if (ops.empty()) break;
                        Cap e = rt.endpoint_to(c.target);
                        e.ops = std::move(ops);
                        e.transferable = false;
                        here = std::move(e);
                    } else if (recreate) {
                        here = recreate(c);
                    }
                } else {
                    here = c;
                    if (here->host == rt.mobility.host_id || here->target == st.id) here->host.clear();
                }
                break;
            case Cap::Kind::reply:
                here = c;
                if (here->host == rt.mobility.host_id) here->host.clear();
                break;
            case Cap::Kind::timer: {
                here = c;
                here->target = st.id;
                const auto delay = delay_until(c.fire_at);
                rt.schedule(SteadyClock::now() + delay,
                            Deadline{Deadline::Type::timer, st.id, c.timer_id});
                break;
            }
            case Cap::Kind::resource:
                if (recreate) here = recreate(c);
                break;
        }
        if (here) {
            rec.caps.insert_or_assign(h, std::move(*here));
        } else {
            lost.push_back(describe_cap(h, c));
        }
    }
    for (const auto& [name, h] : st.services) {
        if (rec.caps.contains(h)) rec.services.emplace_back(name, h);
    }
    rec.next_handle = std::max(st.next_handle, 2);
    rec.next_correlation = st.next_correlation;
    rec.next_timer = st.next_timer;
    if (st.checkpoint_ms >= 0) rec.checkpoint_interval = std::chrono::milliseconds(st.checkpoint_ms);
    rec.marks.insert(st.marks.begin(), st.marks.end());
    rec.restart = st.restart;
    rec.notify_recovered = st.notify_recovered;
    rec.wake_at_ms = st.wake_at_ms;
    // Wall time crosses hosts, as for timers. Once prepared, the remaining
    // budget runs on this host's monotonic clock, including the commit wait.
    const auto timeout_now = SteadyClock::now();
    const auto arrival_ms = wall_ms();
    const auto elapsed_ms = st.pending_captured_ms > 0 && arrival_ms > st.pending_captured_ms
        ? arrival_ms - st.pending_captured_ms : std::int64_t{0};
    for (const auto& [correlation, ms] : st.pending) {
        const auto remaining_ms = ms > elapsed_ms ? ms - elapsed_ms : std::int64_t{0};
        rec.pending_requests.insert(correlation);
        rt.schedule(timeout_now + bounded_delay(remaining_ms),
                    Deadline{Deadline::Type::request_timeout, st.id, correlation});
    }
    rec.image = std::move(a.image);
    rec.started = true;
    rec.checkpoint_due = true;
    rec.arriving = true;
    for (auto& message : a.state.ready) {
        Envelope env;
        env.delivered = std::move(message.delivered);
        env.caps = std::move(message.caps);
        env.residency_marks = std::move(message.residency_marks);
        for (auto& cap : env.caps) if (cap.host == rt.mobility.host_id) cap.host.clear();
        rec.marks.insert(env.residency_marks.begin(), env.residency_marks.end());
        const auto priority = env.delivered.priority;
        rt.enqueue(rec, std::move(env), priority);
    }
    if (a.request != 0) rec.arrival_receipt = std::pair{a.from, a.request};
    Envelope start;
    start.type = Envelope::Type::start;
    if (a.clone) {
        start.event = abi::EventKind::cloned;
        start.args = std::move(a.clone_args);
        for (auto& c : a.clone_caps) {
            if (c.host == rt.mobility.host_id) c.host.clear();
            start.caps.push_back(std::move(c));
        }
        start.original = std::move(a.original);
    } else {
        start.event = abi::EventKind::arrived;
        start.arrived = abi::ArrivedEvent{std::move(a.from), lost};
    }
    rec.arrival_start = std::move(start);
    return lost;
}

std::expected<void, std::string> Runtime::commit_arrival(const PagletId& id, bool activate) {
    Impl& rt = *impl_;
    std::lock_guard lock(rt.mu);
    PagletRec* rec = rt.find(id);
    if (rec == nullptr || !rec->arriving) return std::unexpected("no arrival of " + id);
    rec->arriving = false;
    rec->dormant = !activate;
    // Stored at once: after a crash the paglet is here, not lost.
    if (rt.store && rec->image && !rt.persist(*rec, *rec->image) && rec->restart) {
        rt.end_paglet(*rec, true, "committing arrival: the image could not be stored");
        return std::unexpected(std::string("the arrival image could not be stored"));
    }
    Envelope start = std::move(*rec->arrival_start);
    rec->arrival_start.reset();
    rt.enqueue(*rec, std::move(start), control_priority);
    rt.schedule_wake(*rec);
    rt.make_ready(*rec);
    return {};
}

void Runtime::abort_arrival(const PagletId& id) {
    Impl& rt = *impl_;
    std::lock_guard lock(rt.mu);
    PagletRec* rec = rt.find(id);
    if (rec == nullptr || !rec->arriving) return;
    // The paglet stays where it was: nothing here answers for it.
    rec->mailbox.clear();
    rt.release_lane(*rec);
    rt.erase_paglet(id);
    rt.idle_cv.notify_all();
}

std::expected<void, std::int32_t> Runtime::dispatch(const PagletId& id, std::string destination) {
    Impl& rt = *impl_;
    std::lock_guard lock(rt.mu);
    PagletRec* rec = rt.find(id);
    if (rec == nullptr) return std::unexpected(abi::not_found);
    if (rec->native || rec->trust != TrustClass::roaming) return std::unexpected(abi::denied);
    if (!rt.mobility.depart) return std::unexpected(abi::unsupported);
    if (!parse_transfer_ticket(destination)) return std::unexpected(abi::invalid_argument);
    if (rec->moving || rec->arriving || rec->dispatch_pending || rec->in_end_hook || rec->pending_end)
        return std::unexpected(abi::bad_state);
    if (rec->pinned_until() != 0) return std::unexpected(abi::pinned);
    Envelope env;
    env.type = Envelope::Type::move;
    env.destination = std::move(destination);
    rec->dispatch_pending = true;
    rt.enqueue(*rec, std::move(env), control_priority);
    return {};
}

std::expected<void, std::int32_t> Runtime::pin(const PagletId& id, std::string pin, std::int64_t until) {
    Impl& rt = *impl_;
    std::lock_guard lock(rt.mu);
    PagletRec* rec = rt.find(id);
    if (rec == nullptr || rec->arriving) return std::unexpected(abi::not_found);
    if (rec->native) return std::unexpected(abi::denied);
    // About to leave or leaving: the outcome decides where it is.
    if (rec->moving || rec->dispatch_pending) return std::unexpected(abi::bad_state);
    const std::int64_t now = wall_ms();
    if (until <= now) return std::unexpected(abi::invalid_argument);
    std::erase_if(rec->pins, [now](const auto& p) { return p.second <= now; });  // expired
    if (!rec->pins.contains(pin) && rt.config.pin_limit > 0 && rec->pins.size() >= rt.config.pin_limit) {
        return std::unexpected(abi::quota);
    }
    rec->pins[std::move(pin)] = until;
    return {};
}

bool Runtime::unpin(const PagletId& id, const std::string& pin) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    return rec != nullptr && rec->pins.erase(pin) > 0;
}

void Runtime::mark(const PagletId& id, std::string mark) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    if (rec == nullptr || rec->native || rec->marks.contains(mark)) return;
    rec->marks.insert(std::move(mark));
    rec->checkpoint_due = true;  // stored with the next checkpoint
}

std::vector<std::string> Runtime::marks(const PagletId& id) const {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    if (rec == nullptr) return {};
    return {rec->marks.begin(), rec->marks.end()};
}

std::int64_t Runtime::pinned_until(const PagletId& id) const {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    return rec == nullptr ? 0 : rec->pinned_until();
}

std::vector<Spawn> Runtime::take_spawns() {
    std::lock_guard lock(impl_->mu);
    std::vector<Spawn> out(impl_->spawns.begin(), impl_->spawns.end());
    impl_->spawns.clear();
    return out;
}

std::optional<std::string> Runtime::location(const PagletId& id) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->tombstones.find(id);
    if (it == impl_->tombstones.end() || SteadyClock::now() >= it->second.expires) return std::nullopt;
    return it->second.host;
}

std::optional<PagletInfo> Runtime::info(const PagletId& id) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->paglets.find(id);
    // A prepared arrival is not here until the move commits.
    if (it == impl_->paglets.end() || it->second->arriving) return std::nullopt;
    const PagletRec& r = *it->second;
    return PagletInfo{r.id,
                      r.module_hash(),
                      r.trust,
                      r.owner,
                      r.instance || r.native ? PagletState::active : PagletState::inactive,
                      r.mailbox.size(),
                      r.caps.size(),
                      r.handled,
                      r.lane,
                      r.restart,
                      r.image_ms};
}

std::vector<PagletInfo> Runtime::list() const {
    std::vector<PagletId> ids;
    {
        std::lock_guard lock(impl_->mu);
        for (const auto& [id, rec] : impl_->paglets) ids.push_back(id);
    }
    std::vector<PagletInfo> out;
    for (const auto& id : ids) {
        if (auto i = info(id)) out.push_back(std::move(*i));
    }
    return out;
}

std::optional<Ending> Runtime::ending(const PagletId& id) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->endings.find(id);
    if (it == impl_->endings.end()) return std::nullopt;
    return it->second;
}

namespace {

std::expected<std::filesystem::path, std::int32_t> ensure_directory(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) return std::unexpected(abi::internal);
    return path;
}

}  // namespace

std::expected<std::filesystem::path, std::int32_t> Runtime::storage_dir(const PagletId& id) {
    std::lock_guard lock(impl_->mu);
    if (impl_->find(id) == nullptr) return std::unexpected(abi::not_found);
    if (impl_->storage_root.empty()) return std::unexpected(abi::unsupported);
    return ensure_directory(impl_->storage_root / id);
}

std::expected<std::filesystem::path, std::int32_t> Runtime::scratch_dir(const PagletId& id) {
    std::lock_guard lock(impl_->mu);
    if (impl_->find(id) == nullptr) return std::unexpected(abi::not_found);
    return ensure_directory(impl_->work_root / id);
}

std::expected<PagletId, std::string> Runtime::add_system_paglet(std::shared_ptr<SystemPaglet> paglet) {
    if (!paglet) return std::unexpected(std::string("no system paglet"));
    const std::string name(paglet->name());
    if (name.empty() || !abi::valid_message_name(name)) return std::unexpected("invalid service name " + name);
    const PagletId id = system_paglet_id(name);
    SystemContext* context = nullptr;
    {
        std::lock_guard lock(impl_->mu);
        if (impl_->system_paglets.contains(name) || impl_->find(id) != nullptr) {
            return std::unexpected("system paglet " + name + " exists");
        }
        PagletRec& rec = *impl_->new_paglet(id, {}, TrustClass::system, "host");
        rec.native = paglet;
        rec.context = std::make_unique<NativeContext>(*this, *impl_, id);
        rec.started = true;
        impl_->system_paglets.emplace(name, id);
        context = rec.context.get();  // system paglets are never removed
    }
    // Outside the lock: start() may use the runtime. Nobody holds an
    // endpoint to the new paglet yet, so no message reaches it before.
    paglet->start(*context);
    return id;
}

std::shared_ptr<SystemPaglet> Runtime::system_paglet_object(std::string_view name) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->system_paglets.find(name);
    if (it == impl_->system_paglets.end()) return nullptr;
    auto rec = impl_->paglets.find(it->second);
    return rec == impl_->paglets.end() ? nullptr : rec->second->native;
}

std::vector<std::string> Runtime::system_paglet_names() const {
    std::lock_guard lock(impl_->mu);
    std::vector<std::string> names;
    for (const auto& [name, id] : impl_->system_paglets) names.push_back(name);
    return names;
}

std::optional<PagletId> Runtime::system_paglet(std::string_view name) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->system_paglets.find(name);
    if (it == impl_->system_paglets.end()) return std::nullopt;
    return it->second;
}

std::vector<std::pair<std::int32_t, Cap>> Runtime::capabilities(const PagletId& id) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->paglets.find(id);
    if (it == impl_->paglets.end()) return {};
    return {it->second->caps.begin(), it->second->caps.end()};
}

std::expected<std::int32_t, std::int32_t> Runtime::add_capability(const PagletId& id, Cap cap) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    if (rec == nullptr) return std::unexpected(abi::not_found);
    if (cap.kind == Cap::Kind::reply || cap.kind == Cap::Kind::timer) return std::unexpected(abi::invalid_argument);
    if (rec->caps.size() >= impl_->config.cap_limit) return std::unexpected(abi::quota);
    if (cap.id.empty()) cap.id = new_cap_id();
    return impl_->add_cap(*rec, std::move(cap));
}

void Runtime::revoke_capability(const std::string& id) {
    std::lock_guard lock(impl_->mu);
    if (!impl_->revoked_ids.insert(id).second || !impl_->store) return;
    std::vector<std::string> ids(impl_->revoked_ids.begin(), impl_->revoked_ids.end());
    if (auto ok = impl_->store->save_revoked(ids); !ok) impl_->log(3, "", "storing revocations failed: " + ok.error());
}

void Runtime::set_revoked_grants(std::set<std::string, std::less<>> grants) {
    std::lock_guard lock(impl_->mu);
    impl_->revoked_grants = std::move(grants);
}

std::vector<int> Runtime::worker_processes() const {
    std::lock_guard lock(impl_->mu);
    std::vector<int> pids;
    for (const auto& lane : impl_->lanes) {
        if (auto pid = lane->executor->process_id()) pids.push_back(*pid);
    }
    return pids;
}

bool Runtime::wait_idle(std::chrono::milliseconds timeout) {
    std::unique_lock lock(impl_->mu);
    return impl_->idle_cv.wait_for(lock, timeout, [&] {
        if (impl_->running_count > 0) return false;
        for (const auto& lane : impl_->lanes) {
            if (!lane->ready.empty()) return false;
        }
        return std::ranges::all_of(impl_->paglets,
                                   [](const auto& e) { return e.second->mailbox.empty() || e.second->awaiting_image; });
    });
}

}  // namespace paglets::runtime
