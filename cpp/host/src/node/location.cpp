// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Where paglets are, and pins that keep them there (planning/cpp-location.md):
// location records on responsible hosts chosen by consistent hashing, a
// majority update on every move commit, move counters, liveness and
// rebalancing, location caches, mesh-wide "who holds P" queries, message
// forwarding by location, pin leases and the `locator` system paglet.

#include "node_impl.hpp"

#include <paglets/runtime/mobility.hpp>
#include <paglets/sha256.hpp>

#include <future>

namespace paglets::node {

namespace {

namespace loc = services::locator;

constexpr std::size_t replicas = 3;        // responsible hosts per paglet
constexpr std::size_t virtual_nodes = 16;  // ring points per host
constexpr int max_hops = 8;                // redirects a lookup follows
constexpr int max_busy = 40;               // waits for a paglet in the middle of a move
constexpr int max_bumps = 3;               // counter conflicts while recording a move
constexpr auto cache_ttl = std::chrono::seconds(10);
constexpr auto busy_wait = std::chrono::milliseconds(100);
constexpr std::int64_t default_pin_ms = 3'600'000;  // without a policy maximum
constexpr std::size_t max_results = 1000;

std::size_t majority(std::size_t n) {
    return n / 2 + 1;
}

std::uint64_t ring_point(std::span<const std::uint8_t> data) {
    const auto d = paglets::sha256(data);
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | d[static_cast<std::size_t>(i)];
    return v;
}

std::uint64_t host_point(const mesh::PublicKey& key, std::size_t i) {
    std::vector<std::uint8_t> data{'p', 'a', 'g', 'l', 'e', 't', 's', ' ', 'r', 'i', 'n', 'g', ' ', 'v', '1', 0};
    data.insert(data.end(), key.begin(), key.end());
    data.push_back(static_cast<std::uint8_t>(i));
    return ring_point(data);
}

std::uint64_t paglet_point(const runtime::PagletId& paglet) {
    std::vector<std::uint8_t> data{'p', 'a', 'g', 'l', 'e', 't', 's', ' ', 'r', 'i', 'n', 'g', ' ', 'v', '1', 0};
    data.insert(data.end(), paglet.begin(), paglet.end());
    return ring_point(data);
}

mesh::Map record_frame(std::string_view type, const runtime::PagletId& paglet, const Node::Impl::LocRecord& r) {
    return mesh::Map{{"t", mesh::Value(type)},        {"p", mesh::Value(paglet)},
                     {"h", mesh::Value::bin(r.host)}, {"c", mesh::Value(static_cast<std::int64_t>(r.moves))},
                     {"tm", mesh::Value(r.moved)},    {"o", mesh::Value(r.owner)}};
}

std::optional<Node::Impl::LocRecord> parse_record(const mesh::Fields& f) {
    auto h = f.fixed<32>("h");
    auto c = f.integer("c");
    auto tm = f.integer("tm");
    if (!h || !c || *c < 0 || !tm) return std::nullopt;
    Node::Impl::LocRecord r;
    r.host = *h;
    r.moves = static_cast<std::uint64_t>(*c);
    r.moved = *tm;
    r.owner = f.str("o").value_or("");
    return r;
}

// Pin capabilities name the paglet, the pin and the host that holds the
// pin: `<paglet>/<pin>@<host key ID>`.
std::string pin_resource(const runtime::PagletId& paglet, const std::string& pin, const mesh::PublicKey& host) {
    return paglet + "/" + pin + "@" + mesh::key_id(host);
}

struct PinRef {
    runtime::PagletId paglet;
    std::string pin;
    mesh::PublicKey host{};
};

std::optional<PinRef> parse_pin_resource(std::string_view r) {
    const auto slash = r.find('/');
    const auto at = r.rfind('@');
    if (slash == std::string_view::npos || at == std::string_view::npos || at < slash) return std::nullopt;
    auto host = mesh::parse_key_id(r.substr(at + 1));
    if (!host) return std::nullopt;
    return PinRef{std::string(r.substr(0, slash)), std::string(r.substr(slash + 1, at - slash - 1)), *host};
}

bool allows(const std::vector<std::string>& ops, std::string_view right) {
    return std::ranges::any_of(ops, [&](const std::string& o) { return o == right || o == "*"; });
}

}  // namespace

// -- the locator system paglet ---------------------------------------------------------

class LocatorService final : public services::ContractPaglet<loc::Contract, LocatorService> {
public:
    explicit LocatorService(Node::Impl& node) : node_(node) {}

    std::vector<std::string> default_ops() const override { return {"locate", "locate_and_pin", "release"}; }

    void start(runtime::SystemContext& ctx) override {
        std::lock_guard lock(node_.mu);
        node_.locator_ctx = &ctx;
    }

    services::Result<loc::Location> locate(const loc::LocateRequest&, services::Operation& op) {
        auto target = lent_paglet(op, "locate");
        if (!target) return std::unexpected(target.error());
        auto reply = op.call.defer();
        if (!reply) return std::unexpected(abi::invalid_argument);
        runtime::SystemContext* ctx = &op.ctx;
        std::lock_guard lock(node_.mu);
        Node::Impl::Lookup l;
        l.kind = Node::Impl::LookupKind::locate;
        l.paglet = *target;
        l.done = [this, ctx, reply = *reply, paglet = *target](const Node::Impl::LookupResult& r) {
            if (r.status != abi::ok) {
                (void)ctx->reply(reply, r.status);
                return;
            }
            (void)ctx->reply(reply, abi::ok, wire::to_msgpack(location(paglet, r.where)));
        };
        node_.start_lookup(std::move(l));
        return loc::Location{};  // answered later
    }

    services::Result<loc::Pin> locate_and_pin(const loc::PinRequest& q, services::Operation& op) {
        const auto& caller = op.sender();
        if (!caller || caller->id == "host") return std::unexpected(abi::denied);
        auto target = lent_paglet(op, "pin");
        if (!target) return std::unexpected(target.error());
        std::lock_guard lock(node_.mu);
        // The mesh policy decides who may pin, and for how long.
        const mesh::Principal principal = Node::Impl::principal_of(*caller);
        const mesh::Item item{"locator", {"pin"}, {}, {}};
        const mesh::Evaluation e = mesh::evaluate(node_.ledger.state(), principal, node_.key.public_key(), item);
        if (e.decision != mesh::Decision::allow) return std::unexpected(abi::denied);
        std::int64_t duration = std::clamp(q.duration_ms, min_duration_ms, max_duration_ms);
        duration = std::min(duration, e.max_duration_ms.value_or(default_pin_ms));
        auto reply = op.call.defer();
        if (!reply) return std::unexpected(abi::invalid_argument);
        runtime::SystemContext* ctx = &op.ctx;
        Node::Impl::Lookup l;
        l.kind = Node::Impl::LookupKind::pin;
        l.paglet = *target;
        l.pin = new_cap_id();
        l.until = mesh::unix_ms() + duration;
        l.holder = caller->id;
        l.reason = q.reason;
        l.done = [this, ctx, reply = *reply, paglet = *target](const Node::Impl::LookupResult& r) {
            if (r.status != abi::ok) {
                (void)ctx->reply(reply, r.status);
                return;
            }
            loc::Pin pin{location(paglet, r.where), r.pin, r.until};
            runtime::Cap cap =
                ctx->resource("pin", pin_resource(paglet, r.pin, r.where.host), {"release"}, std::nullopt, r.until);
            cap.transferable = true;
            (void)ctx->reply(reply, abi::ok, wire::to_msgpack(pin), {std::move(cap)});
        };
        node_.start_lookup(std::move(l));
        return loc::Pin{};
    }

    services::Result<loc::ReleaseReply> release(const loc::ReleaseRequest&, services::Operation& op) {
        const runtime::Cap* cap = nullptr;
        for (const auto& c : op.lent()) {
            if (op.ctx.check(c, "pin", "release") == abi::ok) {
                cap = &c;
                break;
            }
        }
        if (cap == nullptr) return std::unexpected(abi::denied);
        auto ref = parse_pin_resource(cap->resource);
        if (!ref) return std::unexpected(abi::invalid_argument);
        // Copies of the capability end with the pin.
        op.ctx.runtime().revoke_capability(cap->id);
        std::lock_guard lock(node_.mu);
        if (ref->host == node_.key.public_key()) {
            return loc::ReleaseReply{node_.release_here(ref->paglet, ref->pin)};
        }
        auto reply = op.call.defer();
        if (!reply) return std::unexpected(abi::invalid_argument);
        const std::uint64_t r = node_.next_request++;
        node_.releases.emplace(r, Node::Impl::Release{*reply, Node::Impl::Clock::now() + node_.timing.lookup_timeout});
        node_.send_frame(ref->host, mesh::Map{{"t", mesh::Value("pin-release")},
                                              {"r", mesh::Value(static_cast<std::int64_t>(r))},
                                              {"p", mesh::Value(ref->paglet)},
                                              {"pin", mesh::Value(ref->pin)}});
        return loc::ReleaseReply{};
    }

private:
    // The paglet of the first endpoint the caller lent, if it has `right`.
    static std::expected<runtime::PagletId, std::int32_t> lent_paglet(services::Operation& op, std::string_view right) {
        for (const auto& c : op.lent()) {
            if (c.kind != runtime::Cap::Kind::endpoint || c.target.starts_with("system.")) continue;
            if (!allows(c.ops, right)) return std::unexpected(abi::denied);
            return c.target;
        }
        return std::unexpected(abi::invalid_argument);
    }

    loc::Location location(const runtime::PagletId& paglet, const Node::Impl::LocRecord& r) const {
        // Called with the node's lock held.
        const auto& hosts = node_.ledger.state().hosts;
        auto h = hosts.find(r.host);
        return loc::Location{paglet, mesh::key_id(r.host), h == hosts.end() ? std::string() : h->second.name, r.moved,
                             static_cast<std::int64_t>(r.moves)};
    }

    Node::Impl& node_;
};

std::shared_ptr<runtime::SystemPaglet> make_locator(Node::Impl& impl) {
    return std::make_shared<LocatorService>(impl);
}

// -- liveness and responsibility ----------------------------------------------------------

void Node::Impl::heard(const mesh::PublicKey& from) {
    last_heard[from] = Clock::now();
}

void Node::Impl::update_view() {
    const auto now = Clock::now();
    const mesh::LedgerState& st = ledger.state();
    std::vector<mesh::PublicKey> live;
    for (const auto& [k, h] : st.hosts) {
        if (k == key.public_key()) {
            live.push_back(k);
            continue;
        }
        auto [it, first] = last_heard.try_emplace(k, now);  // newly seen: up until it stays silent
        if (now - it->second <= timing.host_timeout) live.push_back(k);
    }
    if (std::ranges::find(live, key.public_key()) == live.end()) live.push_back(key.public_key());
    std::ranges::sort(live);
    if (live == view) return;
    const std::vector<mesh::PublicKey> before = std::move(view);
    view = std::move(live);
    ring.clear();
    for (const auto& k : view) {
        for (std::size_t i = 0; i < virtual_nodes; ++i) ring.emplace_back(host_point(k, i), k);
    }
    std::ranges::sort(ring);
    if (before.empty()) return;
    // Rebalancing: records reach the hosts that are responsible now.
    for (const auto& [paglet, h] : held) {
        if (auto rec = held_record(paglet)) publish(paglet, *rec, 0, responsible_for(paglet));
    }
    for (const auto& [paglet, rec] : records) {
        const auto old = responsible_in(before, paglet);
        std::vector<mesh::PublicKey> added;
        for (const auto& k : responsible_for(paglet)) {
            if (std::ranges::find(old, k) == old.end() && k != key.public_key()) added.push_back(k);
        }
        if (!added.empty()) publish(paglet, rec, 0, added);
    }
}

std::vector<mesh::PublicKey> Node::Impl::responsible_in(const std::vector<mesh::PublicKey>& hosts,
                                                        const runtime::PagletId& paglet) const {
    if (hosts.size() <= replicas) return hosts;
    std::vector<std::pair<std::uint64_t, mesh::PublicKey>> points;
    for (const auto& k : hosts) {
        for (std::size_t i = 0; i < virtual_nodes; ++i) points.emplace_back(host_point(k, i), k);
    }
    std::ranges::sort(points);
    std::vector<mesh::PublicKey> out;
    const std::uint64_t p = paglet_point(paglet);
    auto it = std::ranges::lower_bound(points, std::pair{p, mesh::PublicKey{}});
    for (std::size_t n = 0; n < points.size() && out.size() < replicas; ++n, ++it) {
        if (it == points.end()) it = points.begin();
        if (std::ranges::find(out, it->second) == out.end()) out.push_back(it->second);
    }
    return out;
}

std::vector<mesh::PublicKey> Node::Impl::responsible_for(const runtime::PagletId& paglet) {
    update_view();
    if (view.size() <= replicas) return view;
    std::vector<mesh::PublicKey> out;
    const std::uint64_t p = paglet_point(paglet);
    auto it = std::ranges::lower_bound(ring, std::pair{p, mesh::PublicKey{}});
    for (std::size_t n = 0; n < ring.size() && out.size() < replicas; ++n, ++it) {
        if (it == ring.end()) it = ring.begin();
        if (std::ranges::find(out, it->second) == out.end()) out.push_back(it->second);
    }
    return out;
}

// -- records --------------------------------------------------------------------------------

std::optional<Node::Impl::LocRecord> Node::Impl::held_record(const runtime::PagletId& paglet) {
    auto info = runtime.info(paglet);
    if (!info) return std::nullopt;
    auto& h = held[paglet];
    if (h.moved == 0) h.moved = mesh::unix_ms();
    return LocRecord{key.public_key(), h.moves, h.moved, info->owner, mesh::unix_ms()};
}

void Node::Impl::note_created(const runtime::PagletId& paglet) {
    held[paglet] = Held{0, mesh::unix_ms()};
    if (auto rec = held_record(paglet)) publish(paglet, *rec, 0, responsible_for(paglet));
}

// Sends a record to hosts (this host applies it itself); with a request,
// they acknowledge.
void Node::Impl::publish(const runtime::PagletId& paglet, const LocRecord& rec, std::uint64_t request,
                         const std::vector<mesh::PublicKey>& to) {
    for (const auto& k : to) {
        if (k == key.public_key()) {
            apply_record(paglet, rec);
            if (request != 0) {
                if (auto it = outgoing.find(request); it != outgoing.end()) {
                    const LocRecord& stored = records.at(paglet);
                    if (stored.host == rec.host && stored.moves == rec.moves) it->second.acks.insert(k);
                }
            }
            continue;
        }
        mesh::Map frame = record_frame("loc-set", paglet, rec);
        if (request != 0) frame.emplace_back("r", mesh::Value(static_cast<std::int64_t>(request)));
        send_frame(k, std::move(frame));
    }
}

// Keeps the newer record: a higher move counter wins; the same host
// refreshes. Returns true if it changed.
bool Node::Impl::apply_record(const runtime::PagletId& paglet, const LocRecord& rec) {
    auto [it, added] = records.try_emplace(paglet, rec);
    LocRecord& cur = it->second;
    const std::int64_t now = mesh::unix_ms();
    if (added || rec.moves > cur.moves) {
        cur = rec;
        cur.refreshed = now;
        records_dirty = true;
        return true;
    }
    if (rec.host == cur.host) cur.refreshed = now;
    return false;
}

// -- the majority update of a move ----------------------------------------------------------

void Node::Impl::record_move(std::uint64_t request) {
    Outgoing& o = outgoing.at(request);
    o.recording = true;
    o.acks.clear();
    o.responsible = responsible_for(o.departure.state.id);
    o.deadline = Clock::now() + move_timeout;
    const LocRecord rec{o.target, o.moves, mesh::unix_ms(), o.departure.state.owner, 0};
    publish(o.departure.state.id, rec, request, o.responsible);
    check_recorded(request);
}

void Node::Impl::check_recorded(std::uint64_t request) {
    auto it = outgoing.find(request);
    if (it == outgoing.end() || !it->second.recording) return;
    if (it->second.acks.size() >= majority(it->second.responsible.size())) commit_move(request);
}

void Node::Impl::abort_recording(std::uint64_t request, const std::string& why) {
    auto it = outgoing.find(request);
    if (it == outgoing.end()) return;
    Outgoing o = std::move(it->second);
    outgoing.erase(it);
    decisions[request] = Decision{false, o.target, Clock::now() + std::chrono::hours(1)};
    send_frame(o.target,
               mesh::Map{{"t", mesh::Value("move-abort")}, {"r", mesh::Value(static_cast<std::int64_t>(request))}});
    // Records that already name the destination are overwritten with a
    // higher counter: the paglet stays here.
    if (!o.departure.clone) {
        held[o.departure.state.id].moves = o.moves + 1;
        if (auto rec = held_record(o.departure.state.id)) {
            publish(o.departure.state.id, *rec, 0, responsible_for(o.departure.state.id));
        }
    }
    fail_move(o, why);
}

// -- lookups ------------------------------------------------------------------------------------

std::uint64_t Node::Impl::start_lookup(Lookup l) {
    const std::uint64_t id = next_request++;
    lookups.emplace(id, std::move(l));
    lookup_records(id);
    return id;
}

void Node::Impl::lookup_records(std::uint64_t id) {
    Lookup& l = lookups.at(id);
    l.phase = Lookup::Phase::records;
    l.best.reset();
    l.answered.clear();
    if (runtime.info(l.paglet)) {
        if (auto rec = held_record(l.paglet)) return lookup_found(id, *rec);
    }
    l.asked = responsible_for(l.paglet);
    l.need = majority(l.asked.size());
    for (const auto& k : l.asked) {
        if (k == key.public_key()) {
            l.answered.insert(k);
            if (auto r = records.find(l.paglet); r != records.end()) l.best = r->second;
            continue;
        }
        send_frame(k, mesh::Map{{"t", mesh::Value("loc-get")},
                                {"r", mesh::Value(static_cast<std::int64_t>(id))},
                                {"p", mesh::Value(l.paglet)}});
    }
    l.deadline = Clock::now() + timing.lookup_timeout;
    if (l.answered.size() >= l.need) {
        if (l.best) return lookup_found(id, *l.best);
        if (l.asked.size() == 1) l.deadline = Clock::now();  // nobody else to ask: the mesh at once
    }
}

// Where the paglet is (or was last seen): locate ends; pins and forced
// releases go there.
void Node::Impl::lookup_found(std::uint64_t id, const LocRecord& rec) {
    Lookup& l = lookups.at(id);
    loc_cache[l.paglet] = {rec, Clock::now()};
    if (l.kind == LookupKind::locate) return finish_lookup(id, LookupResult{abi::ok, {}, rec});
    l.best = rec;
    l.target = rec.host;
    lookup_send_target(id);
}

void Node::Impl::lookup_send_target(std::uint64_t id) {
    Lookup& l = lookups.at(id);
    l.phase = Lookup::Phase::target;
    l.deadline = Clock::now() + timing.lookup_timeout;
    mesh::Map frame;
    if (l.kind == LookupKind::pin) {
        if (l.target == key.public_key()) {
            frame = pin_here(l.paglet, l.pin, l.until, l.holder, l.reason);
        } else {
            send_frame(l.target, mesh::Map{{"t", mesh::Value("pin")},
                                           {"r", mesh::Value(static_cast<std::int64_t>(id))},
                                           {"p", mesh::Value(l.paglet)},
                                           {"pin", mesh::Value(l.pin)},
                                           {"until", mesh::Value(l.until)},
                                           {"holder", mesh::Value(l.holder)},
                                           {"reason", mesh::Value(l.reason)}});
            return;
        }
    } else {
        if (l.target == key.public_key()) {
            frame = force_here(l.paglet, l.pin);
        } else {
            send_frame(l.target, mesh::Map{{"t", mesh::Value("pin-force")},
                                           {"r", mesh::Value(static_cast<std::int64_t>(id))},
                                           {"p", mesh::Value(l.paglet)},
                                           {"pin", mesh::Value(l.pin)}});
            return;
        }
    }
    const mesh::Value answer(std::move(frame));  // sorted, as Fields needs
    lookup_answer(id, key.public_key(), mesh::Fields{*answer.as_map()});
}

// The answer of the host a pin or forced release went to.
void Node::Impl::lookup_answer(std::uint64_t id, const mesh::PublicKey& from, const mesh::Fields& f) {
    auto it = lookups.find(id);
    if (it == lookups.end() || it->second.phase != Lookup::Phase::target || it->second.target != from) return;
    Lookup& l = it->second;
    const std::string type = f.str("t").value_or("");
    if (type == "pin-ok" || type == "pin-forced") {
        LookupResult r;
        r.where = parse_record(f).value_or(l.best.value_or(LocRecord{}));
        r.where.host = from;
        r.pin = l.pin;
        r.until = f.integer("until").value_or(0);
        r.released = f.integer("n").value_or(0);
        loc_cache[l.paglet] = {r.where, Clock::now()};
        return finish_lookup(id, std::move(r));
    }
    if (type == "pin-moved") {
        if (++l.hops > max_hops) return finish_lookup(id, LookupResult{abi::timeout, "the paglet keeps moving"});
        if (auto to = f.fixed<32>("h")) {
            l.target = *to;
            return lookup_send_target(id);
        }
        return lookup_records(id);
    }
    if (type == "pin-busy") {
        if (++l.hops > max_busy) return finish_lookup(id, LookupResult{abi::timeout, "the paglet keeps moving"});
        l.phase = Lookup::Phase::wait;
        l.deadline = Clock::now() + busy_wait;
        return;
    }
    finish_lookup(id, LookupResult{static_cast<std::int32_t>(f.integer("s").value_or(abi::failed)),
                                   f.str("e").value_or("refused")});
}

void Node::Impl::finish_lookup(std::uint64_t id, LookupResult result) {
    auto node = lookups.extract(id);
    if (node.empty()) return;
    if (node.mapped().done) node.mapped().done(result);
}

void Node::Impl::check_lookups() {
    const auto now = Clock::now();
    std::vector<std::uint64_t> due;
    for (const auto& [id, l] : lookups) {
        if (now >= l.deadline) due.push_back(id);
    }
    for (auto id : due) {
        auto it = lookups.find(id);
        if (it == lookups.end()) continue;
        Lookup& l = it->second;
        switch (l.phase) {
            case Lookup::Phase::records:
                if (l.best) {
                    lookup_found(id, *l.best);  // fewer answers than a majority: the best there is
                } else {
                    // Nobody responsible knows: every live host is asked.
                    l.phase = Lookup::Phase::who;
                    l.deadline = now + timing.lookup_timeout;
                    for (const auto& k : view) {
                        if (k == key.public_key()) continue;
                        send_frame(k, mesh::Map{{"t", mesh::Value("loc-who")},
                                                {"r", mesh::Value(static_cast<std::int64_t>(id))},
                                                {"p", mesh::Value(l.paglet)}});
                    }
                }
                break;
            case Lookup::Phase::who:
                finish_lookup(id, LookupResult{abi::not_found, "no host holds paglet " + l.paglet});
                break;
            case Lookup::Phase::target:
                // The host did not answer: the records may know better now.
                if (++l.timeouts > 1) {
                    finish_lookup(id, LookupResult{abi::timeout,
                                                   "host " + mesh::key_id(l.target).substr(0, 16) + " did not answer"});
                } else {
                    lookup_records(id);
                }
                break;
            case Lookup::Phase::wait: lookup_records(id); break;
        }
    }
    for (auto it = releases.begin(); it != releases.end();) {
        if (now < it->second.deadline) {
            ++it;
            continue;
        }
        if (locator_ctx != nullptr) (void)locator_ctx->reply(it->second.reply, abi::timeout);
        it = releases.erase(it);
    }
}

// -- pins -----------------------------------------------------------------------------------------

mesh::Map Node::Impl::pin_here(const runtime::PagletId& paglet, const std::string& pin, std::int64_t until,
                               const std::string& holder, const std::string& reason) {
    until = std::min(until, mesh::unix_ms() + max_duration_ms);
    auto r = runtime.pin(paglet, pin, until);
    if (r) {
        pins[pin] = PinLease{paglet, until, holder, reason};
        save();
        auto rec = held_record(paglet).value_or(LocRecord{key.public_key()});
        mesh::Map answer = record_frame("pin-ok", paglet, rec);
        answer.emplace_back("until", mesh::Value(until));
        return answer;
    }
    switch (r.error()) {
        case abi::bad_state: return mesh::Map{{"t", mesh::Value("pin-busy")}};
        case abi::not_found: {
            if (auto where = runtime.location(paglet)) {
                if (auto to = mesh::parse_key_id(*where)) {
                    return mesh::Map{{"t", mesh::Value("pin-moved")}, {"h", mesh::Value::bin(*to)}};
                }
            }
            return mesh::Map{{"t", mesh::Value("pin-moved")}};
        }
        default:
            return mesh::Map{{"t", mesh::Value("pin-no")},
                             {"s", mesh::Value(static_cast<std::int64_t>(r.error()))},
                             {"e", mesh::Value(std::string(abi::error_name(r.error())))}};
    }
}

mesh::Map Node::Impl::force_here(const runtime::PagletId& paglet, const std::string& pin) {
    if (!runtime.info(paglet)) {
        if (auto where = runtime.location(paglet)) {
            if (auto to = mesh::parse_key_id(*where)) {
                return mesh::Map{{"t", mesh::Value("pin-moved")}, {"h", mesh::Value::bin(*to)}};
            }
        }
        return mesh::Map{{"t", mesh::Value("pin-moved")}};
    }
    std::int64_t n = 0;
    std::vector<std::string> ended;
    for (const auto& [id, lease] : pins) {
        if (lease.paglet == paglet && (pin.empty() || id == pin)) ended.push_back(id);
    }
    for (const auto& id : ended) {
        if (release_here(paglet, id)) ++n;
    }
    if (!ended.empty() && locator_ctx != nullptr)
        locator_ctx->log(abi::LogLevel::info, "pins of " + paglet.substr(0, 8) + " released by an admin");
    return mesh::Map{{"t", mesh::Value("pin-forced")}, {"n", mesh::Value(n)}};
}

bool Node::Impl::release_here(const runtime::PagletId& paglet, const std::string& pin) {
    auto it = pins.find(pin);
    if (it == pins.end() || it->second.paglet != paglet) return false;
    pins.erase(it);
    runtime.unpin(paglet, pin);
    save();
    return true;
}

void Node::Impl::expire_pins() {
    const std::int64_t now = mesh::unix_ms();
    bool changed = false;
    for (auto it = pins.begin(); it != pins.end();) {
        if (it->second.until > now && runtime.info(it->second.paglet)) {
            ++it;
            continue;
        }
        runtime.unpin(it->second.paglet, it->first);
        it = pins.erase(it);
        changed = true;
    }
    if (changed) save();
}

// -- messages for paglets that are elsewhere -------------------------------------------------------

void Node::Impl::send_remote(runtime::RemoteMessage m) {
    auto to = mesh::parse_key_id(m.host);
    if (!to) return;
    if (*to == key.public_key()) {
        runtime.deliver_remote(std::move(m));
        return;
    }
    send_frame(*to, mesh::Map{{"t", mesh::Value("deliver")}, {"m", mesh::Value(runtime::encode_remote(m))}});
}

void Node::Impl::process_unresolved() {
    std::deque<runtime::RemoteMessage> queue;
    {
        std::lock_guard lock(unresolved_mu);
        queue.swap(unresolved);
    }
    for (auto& m : queue) {
        auto deliver = [this](runtime::RemoteMessage& msg, const mesh::PublicKey& host) {
            msg.host = mesh::key_id(host);
            ++msg.hops;
            send_remote(std::move(msg));
        };
        if (auto c = loc_cache.find(m.target); c != loc_cache.end() && Clock::now() - c->second.second < cache_ttl &&
                                               c->second.first.host != key.public_key()) {
            deliver(m, c->second.first.host);
            continue;
        }
        Lookup l;
        l.kind = LookupKind::locate;
        l.paglet = m.target;
        l.done = [this, m = std::move(m), deliver](const LookupResult& r) mutable {
            if (r.status == abi::ok) return deliver(m, r.where.host);
            if (m.kind != runtime::RemoteMessage::Kind::request) return;
            // Nobody has it: the requester hears so.
            runtime::RemoteMessage answer;
            answer.kind = runtime::RemoteMessage::Kind::reply;
            answer.target = m.requester;
            answer.host = m.requester_host;
            answer.correlation = m.correlation;
            answer.status = abi::not_found;
            send_remote(std::move(answer));
        };
        start_lookup(std::move(l));
    }
}

// -- frames -----------------------------------------------------------------------------------------

bool Node::Impl::on_location_frame(std::string_view type, const mesh::PublicKey& from, const mesh::Fields& f) {
    if (!type.starts_with("loc-") && !type.starts_with("pin") && type != "hb") return false;
    if (!ledger.state().is_host(from)) return true;  // enrolled hosts only
    auto r = f.integer("r");
    auto p = f.str("p");
    if (type == "hb") return true;
    if (type == "loc-set") {
        auto rec = parse_record(f);
        if (!p || !rec) return true;
        apply_record(*p, *rec);
        if (r) {
            const LocRecord& stored = records.at(*p);
            mesh::Map ack = record_frame("loc-ack", *p, stored);
            ack.emplace_back("r", mesh::Value(*r));
            send_frame(from, std::move(ack));
        }
    } else if (type == "loc-ack") {
        auto rec = parse_record(f);
        if (!r || !rec) return true;
        auto it = outgoing.find(static_cast<std::uint64_t>(*r));
        if (it == outgoing.end() || !it->second.recording) return true;
        Outgoing& o = it->second;
        if (rec->host == o.target && rec->moves == o.moves) {
            o.acks.insert(from);
        } else if (rec->moves >= o.moves) {
            // A newer record exists (counters lost somewhere): record above it.
            if (++o.bumps > max_bumps) {
                abort_recording(it->first, "the location records disagree");
                return true;
            }
            o.moves = rec->moves + 1;
            record_move(it->first);
            return true;
        }
        check_recorded(it->first);
    } else if (type == "loc-get") {
        if (!r || !p) return true;
        mesh::Map answer;
        if (auto it = records.find(*p); it != records.end()) {
            answer = record_frame("loc-rec", *p, it->second);
        } else {
            answer = mesh::Map{{"t", mesh::Value("loc-rec")}, {"p", mesh::Value(*p)}};
        }
        answer.emplace_back("r", mesh::Value(*r));
        send_frame(from, std::move(answer));
    } else if (type == "loc-rec" || type == "loc-here") {
        if (!r) return true;
        auto it = lookups.find(static_cast<std::uint64_t>(*r));
        if (it == lookups.end()) return true;
        Lookup& l = it->second;
        auto rec = parse_record(f);
        if (type == "loc-here") {
            if (l.phase == Lookup::Phase::who && rec) lookup_found(it->first, *rec);
            return true;
        }
        if (l.phase != Lookup::Phase::records || std::ranges::find(l.asked, from) == l.asked.end()) return true;
        l.answered.insert(from);
        if (rec && (!l.best || rec->moves > l.best->moves)) l.best = rec;
        if (l.answered.size() >= l.need && l.best) {
            lookup_found(it->first, *l.best);
        } else if (l.answered.size() == l.asked.size()) {
            l.deadline = Clock::now();  // nobody responsible knows: next tick asks the mesh
        }
    } else if (type == "loc-who") {
        if (!r || !p || !runtime.info(*p)) return true;
        if (auto rec = held_record(*p)) {
            mesh::Map answer = record_frame("loc-here", *p, *rec);
            answer.emplace_back("r", mesh::Value(*r));
            send_frame(from, std::move(answer));
        }
    } else if (type == "pin" || type == "pin-force") {
        if (!r || !p) return true;
        mesh::Map answer = type == "pin" ? pin_here(*p, f.str("pin").value_or(""), f.integer("until").value_or(0),
                                                    f.str("holder").value_or(""), f.str("reason").value_or(""))
                                         : force_here(*p, f.str("pin").value_or(""));
        answer.emplace_back("r", mesh::Value(*r));
        send_frame(from, std::move(answer));
    } else if (type == "pin-release") {
        if (!r || !p) return true;
        const bool released = release_here(*p, f.str("pin").value_or(""));
        send_frame(
            from, mesh::Map{{"t", mesh::Value("pin-released")}, {"r", mesh::Value(*r)}, {"ok", mesh::Value(released)}});
    } else if (type == "pin-released") {
        if (!r) return true;
        auto it = releases.find(static_cast<std::uint64_t>(*r));
        if (it == releases.end()) return true;
        const mesh::Value* ok = f.get("ok");
        const bool released = ok != nullptr && ok->as_bool() != nullptr && *ok->as_bool();
        if (locator_ctx != nullptr) {
            (void)locator_ctx->reply(it->second.reply, abi::ok, wire::to_msgpack(loc::ReleaseReply{released}));
        }
        releases.erase(it);
    } else {
        // pin-ok, pin-moved, pin-busy, pin-no, pin-forced: answers to lookups.
        if (r) lookup_answer(static_cast<std::uint64_t>(*r), from, f);
    }
    return true;
}

// -- periodic work ----------------------------------------------------------------------------------

void Node::Impl::location_tick() {
    const auto now = Clock::now();
    // A host that did not run for a while (suspended) has not heard from
    // anybody: that says nothing about the others.
    if (last_tick != Clock::time_point{} && now - last_tick > timing.host_timeout) {
        for (auto& [k, t] : last_heard) t = now;
    }
    last_tick = now;
    if (now >= next_heartbeat) {
        next_heartbeat = now + timing.heartbeat;
        for (const auto& [k, h] : ledger.state().hosts) {
            if (k != key.public_key()) send_frame(k, mesh::Map{{"t", mesh::Value("hb")}});
        }
    }
    update_view();
    process_unresolved();
    check_lookups();
    expire_pins();
    if (now >= next_refresh) {
        next_refresh = now + timing.refresh;
        // Holders re-send the records of their paglets; records nobody
        // refreshed end.
        std::set<runtime::PagletId> here;
        for (const auto& p : runtime.list()) {
            if (p.module.empty()) continue;
            here.insert(p.id);
            if (auto rec = held_record(p.id)) publish(p.id, *rec, 0, responsible_for(p.id));
        }
        std::erase_if(held, [&](const auto& e) { return !here.contains(e.first) && !runtime.info(e.first); });
        const std::int64_t oldest = mesh::unix_ms() - timing.record_ttl.count();
        const auto before = records.size();
        std::erase_if(records, [&](const auto& e) { return e.second.refreshed < oldest; });
        if (records.size() != before) records_dirty = true;
        std::erase_if(loc_cache, [&](const auto& e) { return now - e.second.second >= cache_ttl; });
    }
    if (records_dirty) save_locations();
}

void Node::Impl::load_locations() {
    if (state_dir.empty()) return;
    auto bytes = wasm::read_file((state_dir / "locations").string());
    if (!bytes) return;
    auto v = mesh::decode(*bytes);
    if (!v || v->as_array() == nullptr) return;
    for (const auto& e : *v->as_array()) {
        const mesh::Map* m = e.as_map();
        if (m == nullptr) continue;
        const mesh::Fields f{*m};
        auto p = f.str("p");
        auto rec = parse_record(f);
        if (!p || !rec) continue;
        rec->refreshed = f.integer("rf").value_or(mesh::unix_ms());
        records[*p] = *rec;
    }
}

void Node::Impl::save_locations() {
    records_dirty = false;
    if (state_dir.empty()) return;
    mesh::Array a;
    for (const auto& [p, rec] : records) {
        mesh::Map m = record_frame("r", p, rec);
        m.emplace_back("rf", mesh::Value(rec.refreshed));
        a.emplace_back(std::move(m));
    }
    std::error_code ec;
    fs::create_directories(state_dir, ec);
    const fs::path temp = state_dir / "locations.tmp";
    if (wasm::write_file(temp.string(), mesh::encode(mesh::Value(std::move(a))))) {
        fs::rename(temp, state_dir / "locations", ec);
    }
}

// -- Node API ---------------------------------------------------------------------------------------

void Node::set_location_timing(LocationTiming timing) {
    std::lock_guard lock(impl_->mu);
    impl_->timing = timing;
    impl_->next_heartbeat = {};
    impl_->next_refresh = Impl::Clock::now() + timing.refresh;
}

std::vector<mesh::PublicKey> Node::live_hosts() const {
    std::lock_guard lock(impl_->mu);
    impl_->update_view();
    return impl_->view;
}

std::vector<mesh::PublicKey> Node::responsible(const runtime::PagletId& paglet) const {
    std::lock_guard lock(impl_->mu);
    return impl_->responsible_for(paglet);
}

std::optional<Node::LocationRecord> Node::location_record(const runtime::PagletId& paglet) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->records.find(paglet);
    if (it == impl_->records.end()) return std::nullopt;
    return LocationRecord{it->second.host, it->second.moves, it->second.moved};
}

std::uint64_t Node::locate(const runtime::PagletId& paglet, std::chrono::milliseconds pin_for, std::string reason) {
    std::lock_guard lock(impl_->mu);
    Impl::Lookup l;
    l.paglet = paglet;
    if (pin_for.count() > 0) {
        l.kind = Impl::LookupKind::pin;
        l.pin = new_cap_id();
        l.until = mesh::unix_ms() + pin_for.count();
        l.holder = "host";
        l.reason = std::move(reason);
    }
    const std::uint64_t id = impl_->next_request;  // the lookup's ID
    impl_->located_results[id] = Located{};
    l.done = [impl = impl_.get(), id](const Impl::LookupResult& r) {
        Located& out = impl->located_results[id];
        if (r.status != abi::ok) {
            out.state = Located::State::failed;
            out.error = r.error.empty() ? std::string(abi::error_name(r.status)) : r.error;
            return;
        }
        out.state = Located::State::found;
        out.host = r.where.host;
        out.moves = r.where.moves;
        out.moved_ms = r.where.moved;
        out.owner = r.where.owner;
        out.pin = r.pin;
        out.pinned_until = r.until;
    };
    impl_->start_lookup(std::move(l));
    while (impl_->located_results.size() > max_results) impl_->located_results.erase(impl_->located_results.begin());
    return id;
}

std::optional<Node::Located> Node::located(std::uint64_t lookup) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->located_results.find(lookup);
    if (it == impl_->located_results.end()) return std::nullopt;
    return it->second;
}

std::vector<Node::PinInfo> Node::pins() const {
    std::lock_guard lock(impl_->mu);
    std::vector<PinInfo> out;
    const std::int64_t now = mesh::unix_ms();
    for (const auto& [id, lease] : impl_->pins) {
        if (lease.until > now) out.push_back(PinInfo{id, lease.paglet, lease.until, lease.holder, lease.reason});
    }
    return out;
}

std::size_t Node::release_pins(const runtime::PagletId& paglet, const std::string& pin) {
    std::lock_guard lock(impl_->mu);
    const mesh::Value answer(impl_->force_here(paglet, pin));
    return static_cast<std::size_t>(mesh::Fields{*answer.as_map()}.integer("n").value_or(0));
}

}  // namespace paglets::node
