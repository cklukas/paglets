// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/runtime/runtime.hpp>

#include "runtime_exec.hpp"
#include "runtime_store.hpp"

#include <paglets/abi.hpp>
#include <paglets/runtime/system.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wasm/snapshot.hpp>

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <thread>

namespace paglets::runtime {

namespace {

using SteadyClock = std::chrono::steady_clock;

std::int64_t wall_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
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
        if (info.find_export(name) == nullptr) {
            return std::unexpected("paglet module lacks the required export " + std::string(name));
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
    enum class Type { start, message, wake, deactivate, dispose, ended };
    Type type = Type::message;
    abi::EventKind event = abi::EventKind::created;  // start
    Bytes args;                                      // start
    std::optional<std::string> original;             // start (clones)
    abi::Delivered delivered;                        // message; caps are installed on delivery
    std::vector<Cap> caps;                           // transferred capabilities
    std::optional<Cap> reply_cap;                    // requests
    std::vector<Cap> lent;                           // system paglets: capabilities lent for this message
    PagletId ended_id;                               // ended
};

struct PendingClone {
    PagletId id;
    Bytes args;
    std::vector<Cap> caps;
};

struct PagletRec {
    PagletId id;
    std::shared_ptr<wasm::Module> module;  // null for native system paglets
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
    std::uint64_t next_correlation = 1;
    std::uint64_t next_timer = 1;

    // Lifecycle operations requested during the current handler call.
    std::optional<abi::LifecycleOp> pending_end;
    std::optional<std::int64_t> wake_after_ms;
    std::vector<PendingClone> clones;
    std::size_t spawned_this_call = 0;

    bool budget_exceeded = false;
    SteadyClock::time_point call_deadline{};  // of the guest call in progress
    std::uint64_t handled = 0;
    SteadyClock::time_point last_checkpoint{};
    std::optional<std::chrono::milliseconds> checkpoint_interval;  // per paglet; default from the config
    bool checkpoint_due = false;                                   // no stored image yet
    std::unique_ptr<wasm::HostImports> imports;
    std::vector<std::pair<std::string, std::int32_t>> services;  // default service endpoints

    std::string module_hash() const { return module ? module->hash_hex() : std::string(); }
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

    std::map<std::string, std::shared_ptr<wasm::Module>> modules;  // by hex hash
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

    abi::SenderRecord host_sender() const { return {"host", "local", "system", "", config.host_name}; }

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

    void make_ready(PagletRec& rec) {
        if (rec.running || rec.queued || rec.awaiting_image || rec.mailbox.empty()) return;
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
    void deliver_reply(const PagletId& requester, std::uint64_t correlation, std::int32_t status, Bytes payload,
                       std::vector<Cap> caps) {
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
        Envelope env;
        env.type = Envelope::Type::message;
        env.delivered.kind = abi::MessageKind::reply;
        env.delivered.name = "reply";
        env.delivered.payload = std::move(payload);
        env.delivered.priority = abi::max_priority;
        env.delivered.correlation = correlation;
        env.delivered.status = status;
        env.caps = std::move(caps);
        enqueue(*rec, std::move(env), abi::max_priority);
    }

    // Releases a capability that leaves every table: an unused reply
    // capability answers its request with `status`.
    void drop_cap(Cap cap, std::int32_t status) {
        if (cap.kind == Cap::Kind::reply) {
            deliver_reply(cap.target, cap.correlation, status, {}, {});
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

    PagletRec& new_paglet(const PagletId& id, std::shared_ptr<wasm::Module> module, TrustClass trust,
                          std::string owner) {
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
        PagletRec& ref = *rec;
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

    // Ends a paglet (mu held, the paglet is not running): answers queued
    // requests and held reply capabilities, removes it and its stored state.
    void end_paglet(PagletRec& rec, bool failed, std::string reason) {
        const std::int32_t status = failed ? abi::failed : abi::gone;
        for (auto& [key, env] : rec.mailbox) {
            if (env.reply_cap) drop_cap(std::move(*env.reply_cap), status);
            for (auto& c : env.caps) drop_cap(std::move(c), status);
        }
        rec.mailbox.clear();
        for (auto& [h, cap] : rec.caps) drop_cap(std::move(cap), status);
        rec.caps.clear();
        for (auto& clone : rec.clones) {
            for (auto& cap : clone.caps) drop_cap(std::move(cap), status);
            paglets.erase(clone.id);
        }
        rec.clones.clear();
        if (store) store->remove_paglet(rec.id);
        remove_directories(rec.id);
        retire(rec);
        release_lane(rec);
        notify_ended(rec.id);
        endings[rec.id] = Ending{rec.id, failed, std::move(reason)};
        if (failed) log(3, rec.id, "paglet failed: " + endings[rec.id].reason);
        paglets.erase(rec.id);  // rec is destroyed here
    }

    void persist(PagletRec& rec, const wasm::Snapshot& snap) {
        if (!store) return;
        PagletRecord r;
        r.id = rec.id;
        r.module = rec.module_hash();
        r.trust = std::string(to_string(rec.trust));
        r.owner = rec.owner;
        r.started = rec.started;
        r.next_handle = rec.next_handle;
        r.next_correlation = rec.next_correlation;
        r.next_timer = rec.next_timer;
        r.caps.assign(rec.caps.begin(), rec.caps.end());
        r.pending_requests.assign(rec.pending_requests.begin(), rec.pending_requests.end());
        r.checkpoint_ms = rec.checkpoint_interval ? rec.checkpoint_interval->count() : -1;
        r.services = rec.services;
        if (auto ok = store->save_paglet(r, snap); !ok) {
            log(3, rec.id, "persisting the paglet failed: " + ok.error());
        } else {
            rec.image_on_disk = true;
        }
    }

    // -- guest calls (mu not held; rec.running) -----------------------------

    // Runs a guest call under the handler time budget.
    template <class F>
    std::expected<std::int32_t, std::string> guest_call(PagletRec& rec, F&& call) {
        {
            std::lock_guard lock(mu);
            rec.budget_exceeded = false;
            rec.call_deadline = SteadyClock::now() + config.handler_budget;
            in_call.insert(&rec);
        }
        auto result = call();
        {
            std::lock_guard lock(mu);
            in_call.erase(&rec);
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
    void process_native(PagletRec& rec, Envelope env);
    std::expected<wasm::Snapshot, std::string> snapshot(PagletRec& rec) { return rec.instance->capture(); }
    void process(PagletRec& rec, Envelope env, bool& remove);
    void finish_call(PagletRec& rec, bool& remove, std::unique_lock<std::mutex>& lock);
    void lane_loop(std::size_t index);
    void clock_loop();
    void fire(Deadline d);
    void recover();
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
            rec_.id,      rec_.module_hash(), rec_.owner,   std::string(to_string(rec_.trust)), rt_.config.host_name,
            abi::version, abi::minor_version, rec_.services};
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
        auto cap = checked_endpoint(endpoint, m.name, target);
        if (!cap) return cap.error();
        auto lent = rt_.lend_caps(rec_, m.lend, *target);
        if (!lent) return lent.error();
        auto caps = rt_.take_caps(rec_, m.caps);
        if (!caps) return caps.error();
        if ((*cap)->uses_left) --*(*cap)->uses_left;
        Envelope env;
        env.lent = std::move(*lent);
        env.delivered.kind = abi::MessageKind::message;
        env.delivered.name = std::move(m.name);
        env.delivered.payload = std::move(m.payload);
        env.delivered.priority = m.priority;
        env.delivered.sender = rec_.sender(rt_.config.host_name);
        env.delivered.badge = (*cap)->badge;
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
        auto cap = checked_endpoint(endpoint, m.name, target);
        if (!cap) return cap.error();
        auto lent = rt_.lend_caps(rec_, m.lend, *target);
        if (!lent) return lent.error();
        auto caps = rt_.take_caps(rec_, m.caps);
        if (!caps) return caps.error();
        if ((*cap)->uses_left) --*(*cap)->uses_left;
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
        env.delivered.sender = rec_.sender(rt_.config.host_name);
        env.delivered.badge = (*cap)->badge;
        env.caps = std::move(*caps);
        env.lent = std::move(*lent);
        env.reply_cap = std::move(reply);
        if (target->lane < 0) target->lane_hint = rec_.lane;
        rt_.enqueue(*target, std::move(env), m.priority);
        rt_.schedule(SteadyClock::now() + std::chrono::milliseconds(m.timeout_ms),
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
        std::lock_guard lock(rt_.mu);
        auto it = rec_.caps.find(handle);
        if (it == rec_.caps.end() || it->second.kind != Cap::Kind::reply) return abi::bad_handle;
        if (!m.lend.empty()) return abi::invalid_argument;
        auto caps = rt_.take_caps(rec_, m.caps);
        if (!caps) return caps.error();
        Cap reply = std::move(it->second);
        rec_.caps.erase(it);
        rt_.deliver_reply(reply.target, reply.correlation, abi::ok, std::move(m.payload), std::move(*caps));
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
        std::shared_ptr<wasm::Module> module = rec_.module;
        if (spec.module) {
            auto it = rt_.modules.find(*spec.module);
            if (it == rt_.modules.end()) return abi::not_found;
            module = it->second;
            if (!check_paglet_module(module->info(), TrustClass::roaming)) return abi::denied;
        }
        if (rec_.spawned_this_call >= rt_.config.spawn_limit_per_call || rec_.caps.size() >= rt_.config.cap_limit) {
            return abi::quota;
        }
        auto caps = rt_.take_caps(rec_, spec.caps);
        if (!caps) return caps.error();
        ++rec_.spawned_this_call;
        const PagletId id = new_paglet_id();
        PagletRec& child = rt_.new_paglet(id, std::move(module), TrustClass::roaming, rec_.owner);
        child.lane_hint = rec_.lane;
        child.checkpoint_interval = rec_.checkpoint_interval;
        rt_.install_services(child);
        rt_.enqueue_start(child, abi::EventKind::created, std::move(spec.args), std::move(*caps), std::nullopt);
        return rt_.add_cap(rec_, rt_.endpoint_to(id));
    }

    std::int32_t lifecycle(std::int32_t op, std::span<const std::uint8_t> doc) override {
        std::lock_guard lock(rt_.mu);
        switch (static_cast<abi::LifecycleOp>(op)) {
            case abi::LifecycleOp::dispose:
                if (rec_.pending_end) return abi::bad_state;
                rec_.pending_end = abi::LifecycleOp::dispose;
                return abi::ok;
            case abi::LifecycleOp::deactivate: {
                abi::DeactivateArg arg;
                if (!doc.empty() && !abi::decode(doc, arg)) return abi::malformed;
                if (arg.wake_after_ms && *arg.wake_after_ms < 0) return abi::invalid_argument;
                if (rec_.pending_end) return abi::bad_state;
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
                auto caps = rt_.take_caps(rec_, arg.caps);
                if (!caps) return caps.error();
                ++rec_.spawned_this_call;
                const PagletId id = new_paglet_id();
                PagletRec& clone = rt_.new_paglet(
                    id, rec_.module, rec_.trust == TrustClass::system ? TrustClass::roaming : rec_.trust, rec_.owner);
                clone.awaiting_image = true;
                clone.lane_hint = rec_.lane;
                clone.checkpoint_interval = rec_.checkpoint_interval;
                rec_.clones.push_back(PendingClone{id, std::move(arg.args), std::move(*caps)});
                return rt_.add_cap(rec_, rt_.endpoint_to(id));
            }
            case abi::LifecycleOp::dispatch: return abi::unsupported;
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
        t.fire_at = wall_ms() + delay_ms;
        t.message.name = std::move(m.name);
        t.message.payload = std::move(m.payload);
        t.message.priority = m.priority;
        rt_.schedule(SteadyClock::now() + std::chrono::milliseconds(delay_ms),
                     Deadline{Deadline::Type::timer, rec_.id, t.timer_id});
        return rt_.add_cap(rec_, std::move(t));
    }

private:
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

    // Looks up an endpoint capability and its target (mu held).
    std::expected<Cap*, std::int32_t> checked_endpoint(std::int32_t handle, std::string_view name, PagletRec*& target) {
        auto it = rec_.caps.find(handle);
        if (it == rec_.caps.end()) return std::unexpected(abi::bad_handle);
        if (auto c = rt_.check_endpoint(it->second, name); c != abi::ok) return std::unexpected(c);
        target = rt_.find(it->second.target);
        if (target == nullptr) return std::unexpected(abi::not_found);
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
        if (!abi::valid_message_name(name) || priority < 0 || priority > abi::max_priority) {
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
        env.delivered.sender = self->sender(rt_.config.host_name);
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
    rec.imports = std::make_unique<Imports>(*this, rec);
    auto placed = executor->place(rec.id, rec.module, config.limits, image ? &*image : nullptr, *rec.imports);
    if (!placed) return std::unexpected(placed.error());
    {
        std::lock_guard lock(mu);  // info() and the watchdog read it under the lock
        rec.instance = std::move(*placed);
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

    std::expected<std::int32_t, std::string> result = abi::handled;
    std::optional<std::pair<std::int32_t, std::uint64_t>> reply_slot;  // handle, correlation

    switch (env.type) {
        case Envelope::Type::start: {
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
    }

    std::lock_guard lock(mu);
    ++rec.handled;
    if (!result) {
        retire(rec);
        end_paglet(rec, true, result.error());
        remove = true;
        return;
    }
    // Requests the handler did not handle are answered by the host.
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
    if (*result < 0 && !reply_slot && env.type == Envelope::Type::message) {
        log(2, rec.id, "handler returned " + std::string(abi::error_name(*result)));
    }
}

// Applies the lifecycle operations of the call that just ended (mu held
// on entry and exit; released around guest calls).
void Runtime::Impl::finish_call(PagletRec& rec, bool& remove, std::unique_lock<std::mutex>& lock) {
    rec.spawned_this_call = 0;

    auto clones = std::move(rec.clones);
    rec.clones.clear();
    if (!clones.empty() && rec.instance) {
        lock.unlock();
        auto snap = snapshot(rec);
        lock.lock();
        for (auto& pc : clones) {
            PagletRec* clone = find(pc.id);
            if (clone == nullptr) continue;
            if (!snap) {
                log(3, rec.id, "clone failed: " + snap.error());
                for (auto& c : pc.caps) drop_cap(std::move(c), abi::failed);
                paglets.erase(pc.id);
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
            lock.unlock();
            auto r = call_event(rec, abi::EventKind::disposing, {});
            lock.lock();
            if (!r) log(2, rec.id, "disposing handler failed: " + r.error());
        }
        retire(rec);
        end_paglet(rec, false, "disposed");
        remove = true;
    } else if (end == abi::LifecycleOp::deactivate && rec.instance) {
        const auto wake = rec.wake_after_ms;
        rec.wake_after_ms.reset();
        lock.unlock();
        auto r = call_event(rec, abi::EventKind::deactivating, {});
        std::expected<wasm::Snapshot, std::string> snap = std::unexpected(std::string("deactivating failed"));
        if (r) snap = snapshot(rec);
        lock.lock();
        if (!snap) {
            retire(rec);
            end_paglet(rec, true, r ? snap.error() : r.error());
            remove = true;
            return;
        }
        if (store) {
            persist(rec, *snap);
        } else {
            rec.image = std::move(*snap);
        }
        if (store && !rec.image_on_disk) rec.image = std::move(*snap);
        retire(rec);
        rec.imports.reset();
        if (wake) {
            schedule(SteadyClock::now() + std::chrono::milliseconds(*wake), Deadline{Deadline::Type::wake, rec.id, 0});
        }
    } else if (store && rec.instance &&
               (rec.checkpoint_due || SteadyClock::now() - rec.last_checkpoint >=
                                          rec.checkpoint_interval.value_or(config.checkpoint_interval))) {
        lock.unlock();
        auto snap = snapshot(rec);
        lock.lock();
        if (snap) {
            persist(rec, *snap);
            rec.last_checkpoint = SteadyClock::now();
            rec.checkpoint_due = false;
        }
    }
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
        if (!remove) {
            finish_call(*rec, remove, lock);
        }
        if (!remove) {
            rec->running = false;
            release_lane(*rec);
            make_ready(*rec);
        }
        --running_count;
        if (!retired.empty()) {
            auto dead = std::move(retired);
            retired.clear();
            lock.unlock();
            dead.clear();
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
        auto wake = now + (in_call.empty() ? 100ms : 5ms);
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
        case Deadline::Type::request_timeout: deliver_reply(d.paglet, d.id, abi::timeout, {}, {}); break;
        case Deadline::Type::wake: {
            if (PagletRec* rec = find(d.paglet)) {
                Envelope env;
                env.type = Envelope::Type::wake;
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
    for (auto& module : store->load_modules(warn)) {
        modules.emplace(module->hash_hex(), module);
    }
    for (auto& r : store->load_paglets(warn)) {
        auto mod = modules.find(r.module);
        if (mod == modules.end()) {
            log(3, r.id, "recovery: module " + r.module + " missing; paglet dropped");
            continue;
        }
        TrustClass trust = TrustClass::roaming;
        if (r.trust == "resident") trust = TrustClass::resident;
        if (r.trust == "system") trust = TrustClass::system;
        PagletRec& rec = new_paglet(r.id, mod->second, trust, r.owner);
        rec.started = r.started;
        rec.next_handle = r.next_handle;
        rec.next_correlation = r.next_correlation;
        rec.next_timer = r.next_timer;
        if (r.checkpoint_ms >= 0) rec.checkpoint_interval = std::chrono::milliseconds(r.checkpoint_ms);
        rec.services = r.services;
        rec.caps.clear();
        for (auto& [h, cap] : r.caps) {
            if (cap.kind == Cap::Kind::timer) {
                const auto delay = std::max<std::int64_t>(0, cap.fire_at - wall_ms());
                schedule(SteadyClock::now() + std::chrono::milliseconds(delay),
                         Deadline{Deadline::Type::timer, rec.id, cap.timer_id});
            }
            rec.caps.emplace(h, std::move(cap));
        }
        rec.image_on_disk = true;
        // Requests in flight at the crash are answered `timeout`.
        for (auto correlation : r.pending_requests) {
            rec.pending_requests.insert(correlation);
            deliver_reply(rec.id, correlation, abi::timeout, {}, {});
        }
        log(1, rec.id, "recovered from its last image");
    }
}

// ---------------------------------------------------------------------------
// Public API

Runtime::Runtime(Config config) : impl_(std::make_unique<Impl>(std::move(config))) {
    wasm::ensure_runtime();
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
    if (!impl_->config.state_dir.empty()) {
        impl_->store.emplace(impl_->config.state_dir);
        std::lock_guard lock(impl_->mu);
        impl_->recover();
    }
    const unsigned n = std::max(1u, impl_->config.threads);
    for (unsigned i = 0; i < n; ++i) {
        auto lane = std::make_unique<Impl::Lane>();
        if (!impl_->config.worker_executable.empty()) {
            auto worker = make_worker_executor(impl_->config.worker_executable, impl_->config.sandbox_workers,
                                               [this](const std::string& text) {
                                                   std::lock_guard lock(impl_->mu);
                                                   impl_->log(2, "", text);
                                               });
            if (worker) {
                lane->executor = std::move(*worker);
                impl_->uses_workers = true;
            } else if (i == 0) {
                impl_->log(2, "", worker.error() + "; paglets run in the host process");
            }
        }
        if (!lane->executor) lane->executor = make_in_process_executor();
        impl_->lanes.push_back(std::move(lane));
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
    instances.clear();  // before the records and executors they refer to
    std::lock_guard lock(impl_->mu);
    impl_->retired.clear();
    if (impl_->own_work_root) {
        std::error_code ec;
        std::filesystem::remove_all(impl_->work_root, ec);
    }
}

const Config& Runtime::config() const {
    return impl_->config;
}

std::expected<std::string, std::string> Runtime::add_module(Bytes bytes) {
    auto info = wasm::parse_module(bytes);
    if (!info) return std::unexpected("invalid module: " + info.error());
    if (auto ok = check_paglet_module(*info, TrustClass::system); !ok) return std::unexpected(ok.error());
    const std::string hash = to_hex(sha256(bytes));
    {
        std::lock_guard lock(impl_->mu);
        if (impl_->modules.contains(hash)) return hash;
    }
    Bytes copy;
    if (impl_->store) copy = bytes;
    auto module = wasm::Module::load(std::move(bytes), wasm::ImportPolicy::system());
    if (!module) return std::unexpected(module.error());
    std::lock_guard lock(impl_->mu);
    if (impl_->store) {
        if (auto ok = impl_->store->save_module(hash, copy); !ok) return std::unexpected(ok.error());
    }
    impl_->modules.emplace(hash, std::move(*module));
    return hash;
}

std::expected<std::string, std::string> Runtime::add_module_file(const std::filesystem::path& path) {
    auto bytes = wasm::read_file(path.string());
    if (!bytes) return std::unexpected(bytes.error());
    return add_module(std::move(*bytes));
}

std::expected<PagletId, std::string> Runtime::create(std::string_view module, CreateOptions options) {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->modules.find(std::string(module));
    if (it == impl_->modules.end()) return std::unexpected("unknown module " + std::string(module));
    if (auto ok = check_paglet_module(it->second->info(), options.trust); !ok) return std::unexpected(ok.error());
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
    PagletRec& rec = impl_->new_paglet(id, it->second, options.trust, std::move(options.owner));
    rec.checkpoint_interval = options.checkpoint_interval;
    impl_->install_services(rec);
    impl_->enqueue_start(rec, abi::EventKind::created, std::move(options.args), {}, std::nullopt);
    return id;
}

std::expected<void, std::int32_t> Runtime::send(const PagletId& to, std::string_view name, Bytes payload,
                                                std::int32_t priority) {
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
    env.delivered.sender = impl_->host_sender();
    impl_->enqueue(*rec, std::move(env), priority);
    return {};
}

std::future<Reply> Runtime::request(const PagletId& to, std::string_view name, Bytes payload,
                                    std::chrono::milliseconds timeout) {
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
    env.delivered.sender = impl_->host_sender();
    env.reply_cap = std::move(reply);
    impl_->enqueue(*rec, std::move(env), abi::default_priority);
    impl_->schedule(SteadyClock::now() + timeout, Deadline{Deadline::Type::request_timeout, "", correlation});
    return future;
}

Reply Runtime::call(const PagletId& to, std::string_view name, Bytes payload, std::chrono::milliseconds timeout) {
    return request(to, name, std::move(payload), timeout).get();
}

std::expected<void, std::int32_t> Runtime::deactivate(const PagletId& id) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    if (rec == nullptr) return std::unexpected(abi::not_found);
    if (rec->native) return std::unexpected(abi::denied);
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
    Envelope env;
    env.type = Envelope::Type::dispose;
    impl_->enqueue(*rec, std::move(env), control_priority);
    return {};
}

std::optional<PagletInfo> Runtime::info(const PagletId& id) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->paglets.find(id);
    if (it == impl_->paglets.end()) return std::nullopt;
    const PagletRec& r = *it->second;
    return PagletInfo{r.id,
                      r.module_hash(),
                      r.trust,
                      r.owner,
                      r.instance || r.native ? PagletState::active : PagletState::inactive,
                      r.mailbox.size(),
                      r.caps.size(),
                      r.handled,
                      r.lane};
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
        PagletRec& rec = impl_->new_paglet(id, nullptr, TrustClass::system, "host");
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
