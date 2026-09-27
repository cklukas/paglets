// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/runtime/runtime.hpp>

#include "runtime_store.hpp"

#include <paglets/abi.hpp>
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
    enum class Type { start, message, wake, deactivate, dispose };
    Type type = Type::message;
    abi::EventKind event = abi::EventKind::created;  // start
    Bytes args;                                      // start
    std::optional<std::string> original;             // start (clones)
    abi::Delivered delivered;                        // message; caps are installed on delivery
    std::vector<Cap> caps;                           // transferred capabilities
    std::optional<Cap> reply_cap;                    // requests
};

struct PendingClone {
    PagletId id;
    Bytes args;
    std::vector<Cap> caps;
};

struct PagletRec {
    PagletId id;
    std::shared_ptr<wasm::Module> module;
    TrustClass trust = TrustClass::roaming;
    std::string owner;

    std::unique_ptr<wasm::Instance> instance;
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
    bool checkpoint_due = false;  // no stored image yet
    std::unique_ptr<wasm::HostImports> imports;

    abi::SenderRecord sender(const std::string& host) const {
        return {id, owner, std::string(to_string(trust)), module->hash_hex(), host};
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
    std::condition_variable work_cv;
    std::condition_variable idle_cv;
    std::condition_variable clock_cv;
    bool stopping = false;

    std::map<std::string, std::shared_ptr<wasm::Module>> modules;  // by hex hash
    std::map<PagletId, std::unique_ptr<PagletRec>> paglets;
    std::map<PagletId, Ending> endings;
    std::deque<PagletId> ready;
    std::size_t running_count = 0;

    std::multimap<SteadyClock::time_point, Deadline> deadlines;
    std::set<PagletRec*> in_call;  // guest calls in progress, checked by the clock thread

    std::map<std::uint64_t, std::promise<Reply>> external;  // host requests by correlation
    std::uint64_t next_external = 1;

    std::optional<Store> store;
    std::vector<std::thread> workers;
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

    void schedule(SteadyClock::time_point at, Deadline d) {
        const bool earliest = deadlines.empty() || at < deadlines.begin()->first;
        deadlines.emplace(at, std::move(d));
        if (earliest) clock_cv.notify_all();
    }

    void make_ready(PagletRec& rec) {
        if (rec.running || rec.queued || rec.awaiting_image || rec.mailbox.empty()) return;
        rec.queued = true;
        ready.push_back(rec.id);
        work_cv.notify_one();
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
        if (cap.expires && wall_ms() >= *cap.expires) return abi::expired;
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
        return c;
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
        endings[rec.id] = Ending{rec.id, failed, std::move(reason)};
        if (failed) log(3, rec.id, "paglet failed: " + endings[rec.id].reason);
        paglets.erase(rec.id);  // rec is destroyed here
    }

    void persist(PagletRec& rec, const wasm::Snapshot& snap) {
        if (!store) return;
        PagletRecord r;
        r.id = rec.id;
        r.module = rec.module->hash_hex();
        r.trust = std::string(to_string(rec.trust));
        r.owner = rec.owner;
        r.started = rec.started;
        r.next_handle = rec.next_handle;
        r.next_correlation = rec.next_correlation;
        r.next_timer = rec.next_timer;
        r.caps.assign(rec.caps.begin(), rec.caps.end());
        r.pending_requests.assign(rec.pending_requests.begin(), rec.pending_requests.end());
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

    std::expected<std::int32_t, std::string> call_event(PagletRec& rec, abi::EventKind kind, const Bytes& doc) {
        return guest_call(rec, [&] {
            const std::array<std::uint32_t, 1> lead{static_cast<std::uint32_t>(kind)};
            return rec.instance->call_with_data("paglets_on_event", lead, doc);
        });
    }

    std::expected<void, std::string> activate(PagletRec& rec, bool deliver_activated);
    std::expected<wasm::Snapshot, std::string> snapshot(PagletRec& rec) { return wasm::capture(*rec.instance); }
    void process(PagletRec& rec, Envelope env, bool& remove);
    void finish_call(PagletRec& rec, bool& remove, std::unique_lock<std::mutex>& lock);
    void worker_loop();
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
            rec_.id,     rec_.module->hash_hex(), rec_.owner, std::string(to_string(rec_.trust)), rt_.config.host_name,
            abi::version};
        return abi::encode(info);
    }

    std::int32_t send(std::int32_t endpoint, std::span<const std::uint8_t> doc) override {
        abi::OutMessage m;
        if (!abi::decode(doc, m)) return abi::malformed;
        if (auto v = Runtime::Impl::validate(m); v != abi::ok) return v;
        std::lock_guard lock(rt_.mu);
        PagletRec* target = nullptr;
        auto cap = checked_endpoint(endpoint, m.name, target);
        if (!cap) return cap.error();
        auto caps = rt_.take_caps(rec_, m.caps);
        if (!caps) return caps.error();
        if ((*cap)->uses_left) --*(*cap)->uses_left;
        Envelope env;
        env.delivered.kind = abi::MessageKind::message;
        env.delivered.name = std::move(m.name);
        env.delivered.payload = std::move(m.payload);
        env.delivered.priority = m.priority;
        env.delivered.sender = rec_.sender(rt_.config.host_name);
        env.delivered.badge = (*cap)->badge;
        env.caps = std::move(*caps);
        rt_.enqueue(*target, std::move(env), m.priority);
        return abi::ok;
    }

    std::int64_t request(std::int32_t endpoint, std::span<const std::uint8_t> doc) override {
        abi::OutMessage m;
        if (!abi::decode(doc, m)) return abi::malformed;
        if (auto v = Runtime::Impl::validate(m); v != abi::ok) return v;
        if (m.timeout_ms <= 0) return abi::invalid_argument;
        std::lock_guard lock(rt_.mu);
        PagletRec* target = nullptr;
        auto cap = checked_endpoint(endpoint, m.name, target);
        if (!cap) return cap.error();
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
        env.reply_cap = std::move(reply);
        rt_.enqueue(*target, std::move(env), m.priority);
        rt_.schedule(SteadyClock::now() + std::chrono::milliseconds(m.timeout_ms),
                     Deadline{Deadline::Type::request_timeout, rec_.id, correlation});
        return static_cast<std::int64_t>(correlation);
    }

    std::int32_t reply(std::int32_t handle, std::span<const std::uint8_t> doc) override {
        abi::OutMessage m;
        if (!abi::decode(doc, m)) return abi::malformed;
        std::lock_guard lock(rt_.mu);
        auto it = rec_.caps.find(handle);
        if (it == rec_.caps.end() || it->second.kind != Cap::Kind::reply) return abi::bad_handle;
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
        if (it == rec_.caps.end() || it->second.kind != Cap::Kind::endpoint) return abi::bad_handle;
        if (rec_.caps.size() >= rt_.config.cap_limit) return abi::quota;
        const Cap& src = it->second;
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
        }
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
// Delivery

std::expected<void, std::string> Runtime::Impl::activate(PagletRec& rec, bool deliver_activated) {
    std::optional<wasm::Snapshot> image;
    {
        std::lock_guard lock(mu);
        if (rec.image) {
            image = std::move(rec.image);
            rec.image.reset();
        }
    }
    if (!image && rec.image_on_disk && store) {
        auto loaded = store->load_image(rec.id);
        if (!loaded) return std::unexpected(loaded.error());
        image = std::move(*loaded);
    }
    if (image) {
        auto inst = wasm::restore(rec.module, *image, config.limits);
        if (!inst) return std::unexpected(inst.error());
        rec.instance = std::move(*inst);
    } else {
        auto inst = wasm::Instance::create(rec.module, config.limits);
        if (!inst) return std::unexpected(inst.error());
        rec.instance = std::move(*inst);
    }
    rec.imports = std::make_unique<Imports>(*this, rec);
    rec.instance->set_imports(rec.imports.get());
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
    if (env.type == Envelope::Type::dispose && !rec.instance && !rec.image && !rec.image_on_disk) {
        std::lock_guard lock(mu);
        end_paglet(rec, false, "disposed");
        remove = true;
        return;
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
            result = guest_call(rec, [&] {
                return rec.instance->call_with_data("paglets_on_message", std::span<const std::uint32_t>{}, doc);
            });
            break;
        }
        case Envelope::Type::wake: break;
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
        rec.instance.reset();
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
    if (*result < 0 && !reply_slot) {
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
        rec.instance.reset();
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
            rec.instance.reset();
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
        rec.instance.reset();
        rec.imports.reset();
        if (wake) {
            schedule(SteadyClock::now() + std::chrono::milliseconds(*wake), Deadline{Deadline::Type::wake, rec.id, 0});
        }
    } else if (store && rec.instance &&
               (rec.checkpoint_due || SteadyClock::now() - rec.last_checkpoint >= config.checkpoint_interval)) {
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

void Runtime::Impl::worker_loop() {
    wasm::init_thread();
    std::unique_lock lock(mu);
    while (true) {
        work_cv.wait(lock, [&] { return stopping || !ready.empty(); });
        if (stopping) break;
        const PagletId id = std::move(ready.front());
        ready.pop_front();
        PagletRec* rec = find(id);
        if (rec == nullptr) continue;
        rec->queued = false;
        if (rec->mailbox.empty() || rec->running || rec->awaiting_image) continue;
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
            make_ready(*rec);
        }
        --running_count;
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
    if (!impl_->config.state_dir.empty()) {
        impl_->store.emplace(impl_->config.state_dir);
        std::lock_guard lock(impl_->mu);
        impl_->recover();
    }
    const unsigned n = std::max(1u, impl_->config.threads);
    for (unsigned i = 0; i < n; ++i) impl_->workers.emplace_back([this] { impl_->worker_loop(); });
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
    impl_->work_cv.notify_all();
    impl_->clock_cv.notify_all();
    for (auto& t : impl_->workers) t.join();
    if (impl_->clock.joinable()) impl_->clock.join();
    std::lock_guard lock(impl_->mu);
    for (auto& [c, p] : impl_->external) p.set_value(Reply{abi::gone, {}});
    impl_->external.clear();
    // No handler runs any more: a graceful stop stores the latest state of
    // every active paglet (after a crash, the last checkpoint counts).
    for (auto& [id, rec] : impl_->paglets) {
        if (save_state && impl_->store && rec->instance && rec->started) {
            if (auto snap = wasm::capture(*rec->instance)) impl_->persist(*rec, *snap);
        }
        rec->instance.reset();  // instances hold pointers into their records
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
    const PagletId id = new_paglet_id();
    PagletRec& rec = impl_->new_paglet(id, it->second, options.trust, std::move(options.owner));
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
    Envelope env;
    env.type = Envelope::Type::deactivate;
    impl_->enqueue(*rec, std::move(env), control_priority);
    return {};
}

std::expected<void, std::int32_t> Runtime::dispose(const PagletId& id) {
    std::lock_guard lock(impl_->mu);
    PagletRec* rec = impl_->find(id);
    if (rec == nullptr) return std::unexpected(abi::not_found);
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
                      r.module->hash_hex(),
                      r.trust,
                      r.owner,
                      r.instance ? PagletState::active : PagletState::inactive,
                      r.mailbox.size(),
                      r.caps.size(),
                      r.handled};
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

bool Runtime::wait_idle(std::chrono::milliseconds timeout) {
    std::unique_lock lock(impl_->mu);
    return impl_->idle_cv.wait_for(lock, timeout, [&] {
        if (impl_->running_count > 0 || !impl_->ready.empty()) return false;
        return std::ranges::all_of(impl_->paglets,
                                   [](const auto& e) { return e.second->mailbox.empty() || e.second->awaiting_image; });
    });
}

}  // namespace paglets::runtime
