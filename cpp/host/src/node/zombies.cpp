// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `zombies` system paglet (docs/zombie-protocol.md): every host's
// registry of zombie groups. At most one replica per group and host;
// tombstones and sink redirects spread to every host by gossip (each host
// sends what it knows every few seconds; a merge keeps an ending and the
// highest sink number, so all hosts agree once they talk); protocol traces
// of the replicas here, collected from every host for live checks.

#include "node_impl.hpp"

#include <paglets/protocols/zombie/trace.hpp>
#include <paglets/services/zombies.hpp>

#include <future>

namespace paglets::node {

namespace {

namespace zb = services::zombies;

constexpr std::size_t max_traces_per_group = 4000;
constexpr std::size_t max_trace_batch = 256;
constexpr std::size_t max_sync_bytes = 1024 * 1024;
constexpr std::size_t max_result_bytes = 4u * 1024 * 1024;
constexpr auto deliver_wait = std::chrono::seconds(15);
constexpr std::int64_t tomb_ttl_ms = 30LL * 24 * 3600 * 1000;
constexpr auto traces_wait = std::chrono::seconds(5);

std::int64_t now_ms() {
    return mesh::unix_ms();
}

bool valid_group(std::string_view g) {
    return g.size() == 32 && std::ranges::all_of(g, [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}

// A replica as hosts tell each other ("zb-members").
struct Member {
    std::string group;
    std::uint32_t replica = 0;
    std::string paglet;
    std::string host;
};

struct Members {
    std::vector<Member> members;
};

// What a host keeps of a group (stored in zombies.state).
struct Stored {
    zb::GroupState state;
    std::string owner;
    std::string module;
    std::string paglet;
    std::int64_t joined_ms = 0;
    std::vector<zb::TraceRecord> traces;
    std::vector<Member> members;
};

struct StoredAll {
    std::vector<Stored> groups;
    std::vector<std::string> result_groups;  // with results: group, sink, result
    std::vector<std::int64_t> result_sinks;
    std::vector<zb::StoredResult> results;
};

}  // namespace

// -- registry ---------------------------------------------------------------------------------

void Node::Impl::zombie_notify(const std::string& group) {
    auto it = zombies.groups.find(group);
    if (it == zombies.groups.end() || it->second.paglet.empty() || zombies.ctx == nullptr) return;
    (void)zombies.ctx->send(it->second.paglet, zb::update_message, wire::to_msgpack(it->second.state));
}

bool Node::Impl::zombie_merge(const zb::GroupState& s) {
    if (!valid_group(s.group)) return false;
    auto& g = zombies.groups[s.group];
    if (g.state.group.empty()) {
        g.state.group = s.group;
        g.state.sink_spec = s.sink_spec;
    }
    bool changed = false;
    if (g.state.ending == zb::Ending::none && s.ending != zb::Ending::none) {
        g.state.ending = s.ending;
        g.state.reason = s.reason;
        g.state.ended_ms = s.ended_ms;
        changed = true;
    }
    if (s.sink > g.state.sink) {
        g.state.sink = s.sink;
        g.state.sink_spec = s.sink_spec;
        changed = true;
    }
    if (changed) {
        zombies.dirty = true;
        zombie_notify(s.group);
    }
    return changed;
}

void Node::Impl::zombie_end(const std::string& group, zb::Ending ending, std::string reason) {
    auto& g = zombies.groups[group];
    if (g.state.group.empty()) g.state.group = group;
    if (g.state.ending != zb::Ending::none) return;
    g.state.ending = ending;
    g.state.reason = std::move(reason);
    g.state.ended_ms = now_ms();
    zombies.dirty = true;
    zombies.next_gossip = {};  // spread at once
    zombie_notify(group);
}

void Node::Impl::zombies_tick() {
    const auto now = Clock::now();
    if (now < zombies.next_gossip) return;
    zombies.next_gossip = now + zombies.gossip_every;
    const std::int64_t oldest = now_ms() - tomb_ttl_ms;
    std::erase_if(zombies.groups, [&](const auto& e) {
        return e.second.state.ending != zb::Ending::none && e.second.state.ended_ms < oldest;
    });
    // Only what other hosts must learn: endings and redirects.
    mesh::Array states;
    for (const auto& [id, g] : zombies.groups) {
        if (g.state.ending != zb::Ending::none || g.state.sink > 1) states.emplace_back(wire::to_msgpack(g.state));
    }
    if (!states.empty()) {
        const mesh::Map frame{{"t", mesh::Value("zb-sync")}, {"s", mesh::Value(states)}};
        for (const auto& k : view) {
            if (k != key.public_key()) send_frame(k, frame);
        }
    }
    // The replicas of running groups this host knows of.
    Members known;
    for (const auto& [id, g] : zombies.groups) {
        if (g.state.ending != zb::Ending::none) continue;
        for (const auto& [n, m] : g.members) known.members.push_back(Member{id, n, m.first, m.second});
    }
    if (!known.members.empty()) {
        const mesh::Map frame{{"t", mesh::Value("zb-members")}, {"m", mesh::Value(wire::to_msgpack(known))}};
        for (const auto& k : view) {
            if (k != key.public_key()) send_frame(k, frame);
        }
    }
    if (zombies.dirty) save_zombies();
    for (auto it = zombies.deliveries.begin(); it != zombies.deliveries.end();) {
        if (now >= it->second.deadline) {
            if (zombies.ctx != nullptr) (void)zombies.ctx->reply(std::move(it->second.reply), abi::timeout);
            it = zombies.deliveries.erase(it);
        } else {
            ++it;
        }
    }
    // Trace collections that ran out of time end with what they have.
    for (auto it = zombies.collections.begin(); it != zombies.collections.end();) {
        if (now >= it->second.deadline) {
            it->second.done(std::move(it->second.records));
            it = zombies.collections.erase(it);
        } else {
            ++it;
        }
    }
}

bool Node::Impl::on_zombies_frame(std::string_view type, const mesh::PublicKey& from, const mesh::Fields& f) {
    if (type == "zb-sync") {
        const mesh::Array* states = f.array("s");
        if (states == nullptr || !ledger.state().is_host(from)) return true;
        std::size_t bytes = 0;
        for (const auto& v : *states) {
            const auto* b = v.as_bin();
            if (b == nullptr || (bytes += b->size()) > max_sync_bytes) break;
            zb::GroupState s;
            if (wire::from_msgpack(*b, s)) zombie_merge(s);
        }
        return true;
    }
    if (type == "zb-members") {
        auto data = f.bin("m");
        Members known;
        if (!data || data->size() > max_sync_bytes || !ledger.state().is_host(from) || !wire::from_msgpack(*data, known))
            return true;
        const std::string own = mesh::key_id(key.public_key());
        for (const auto& m : known.members) {
            if (!valid_group(m.group) || m.host == own) continue;  // this host knows its own replicas
            auto& g = zombies.groups[m.group];
            if (g.state.group.empty()) g.state.group = m.group;
            auto& slot = g.members[m.replica];
            if (slot != std::pair{m.paglet, m.host}) {
                slot = {m.paglet, m.host};
                zombies.dirty = true;
            }
        }
        return true;
    }
    if (type == "zb-deliver") {
        auto request = f.integer("r");
        auto group = f.str("g");
        auto sink = f.integer("s");
        auto name = f.str("n");
        auto data = f.bin("d");
        if (!request || !group || !sink || !name || !data || !ledger.state().is_host(from)) return true;
        const bool accepted = zombie_accept(*group, *sink, mesh::key_id(from), *name, *data);
        send_frame(from, mesh::Map{{"t", mesh::Value("zb-accepted")}, {"r", mesh::Value(*request)},
                                   {"a", mesh::Value(accepted)}});
        return true;
    }
    if (type == "zb-accepted") {
        auto request = f.integer("r");
        const mesh::Value* a = f.get("a");
        if (!request || a == nullptr || a->as_bool() == nullptr) return true;
        auto it = zombies.deliveries.find(static_cast<std::uint64_t>(*request));
        if (it == zombies.deliveries.end() || mesh::key_id(from) != it->second.host) return true;
        if (zombies.ctx != nullptr) {
            (void)zombies.ctx->reply(std::move(it->second.reply), abi::ok,
                                     wire::to_msgpack(zb::DeliverReply{*a->as_bool(), it->second.sink, it->second.host}));
        }
        zombies.deliveries.erase(it);
        return true;
    }
    if (type == "zb-traces?") {
        auto request = f.integer("r");
        auto group = f.str("g");
        if (!request || !group || !ledger.state().is_host(from)) return true;
        zb::Traces t;
        if (auto it = zombies.groups.find(*group); it != zombies.groups.end()) {
            t.records.assign(it->second.traces.begin(), it->second.traces.end());
        }
        send_frame(from, mesh::Map{{"t", mesh::Value("zb-traces")},
                                   {"r", mesh::Value(*request)},
                                   {"d", mesh::Value(wire::to_msgpack(t))}});
        return true;
    }
    if (type == "zb-traces") {
        auto request = f.integer("r");
        auto data = f.bin("d");
        if (!request || !data) return true;
        auto it = zombies.collections.find(static_cast<std::uint64_t>(*request));
        if (it == zombies.collections.end() || !it->second.waiting.erase(from)) return true;
        zb::Traces t;
        if (wire::from_msgpack(*data, t)) {
            const std::string host = mesh::key_id(from);
            for (auto& r : t.records) {
                if (r.host == host) it->second.records.push_back(std::move(r));  // a host speaks for itself
            }
        }
        if (it->second.waiting.empty()) {
            it->second.done(std::move(it->second.records));
            zombies.collections.erase(it);
        }
        return true;
    }
    return false;
}

void Node::Impl::collect_zombie_traces(const std::string& group,
                                       std::function<void(std::vector<zb::TraceRecord>)> done) {
    const std::uint64_t request = next_request++;
    auto& c = zombies.collections[request];
    c.deadline = Clock::now() + traces_wait;
    c.done = std::move(done);
    if (auto it = zombies.groups.find(group); it != zombies.groups.end()) {
        c.records.assign(it->second.traces.begin(), it->second.traces.end());
    }
    for (const auto& k : view) {
        if (k == key.public_key()) continue;
        c.waiting.insert(k);
        send_frame(k, mesh::Map{{"t", mesh::Value("zb-traces?")},
                                {"r", mesh::Value(static_cast<std::int64_t>(request))},
                                {"g", mesh::Value(group)}});
    }
    if (c.waiting.empty()) {
        auto records = std::move(c.records);
        auto finish = std::move(c.done);
        zombies.collections.erase(request);
        finish(std::move(records));
    }
}

bool Node::Impl::zombie_accept(const std::string& group, std::int64_t sink, const std::string& from, std::string name,
                               std::vector<std::uint8_t> data) {
    if (!valid_group(group) || data.size() > max_result_bytes) return false;
    auto [it, fresh] = zombies.results.try_emplace({group, sink});
    if (!fresh) return false;  // the first result counts
    it->second = zb::StoredResult{true, sink, now_ms(), from, std::move(name), std::move(data)};
    save_zombies();  // durable before the answer
    return true;
}

// `host:NAME`: a host name in the ledger, or (the start of) a key ID.
std::optional<mesh::PublicKey> Node::Impl::zombie_sink_host(const std::string& spec) const {
    if (!spec.starts_with("host:")) return std::nullopt;
    const std::string want = spec.substr(5);
    if (want.empty()) return std::nullopt;
    for (const auto& [k, h] : ledger.state().hosts) {
        const std::string id = mesh::key_id(k);
        if (h.name == want || id == want || (want.size() >= 8 && id.starts_with(want))) return k;
    }
    return std::nullopt;
}

void Node::Impl::load_zombies() {
    if (state_dir.empty()) return;
    auto bytes = wasm::read_file((state_dir / "zombies.state").string());
    if (!bytes) return;
    StoredAll all;
    if (!wire::from_msgpack(*bytes, all)) return;
    for (auto& s : all.groups) {
        auto& g = zombies.groups[s.state.group];
        g.state = std::move(s.state);
        g.owner = std::move(s.owner);
        g.module = std::move(s.module);
        g.paglet = std::move(s.paglet);
        g.joined_ms = s.joined_ms;
        g.traces.assign(s.traces.begin(), s.traces.end());
        for (const auto& m : s.members) g.members[m.replica] = {m.paglet, m.host};
    }
    for (std::size_t i = 0; i < all.results.size() && i < all.result_groups.size() && i < all.result_sinks.size(); ++i) {
        zombies.results[{all.result_groups[i], all.result_sinks[i]}] = std::move(all.results[i]);
    }
}

void Node::Impl::save_zombies() {
    zombies.dirty = false;
    if (state_dir.empty()) return;
    StoredAll all;
    for (const auto& [id, g] : zombies.groups) {
        Stored st{g.state, g.owner, g.module, g.paglet, g.joined_ms, {g.traces.begin(), g.traces.end()}, {}};
        for (const auto& [n, m] : g.members) st.members.push_back(Member{id, n, m.first, m.second});
        all.groups.push_back(std::move(st));
    }
    for (const auto& [k, r] : zombies.results) {
        all.result_groups.push_back(k.first);
        all.result_sinks.push_back(k.second);
        all.results.push_back(r);
    }
    const auto bytes = wire::to_msgpack(all);
    const auto file = state_dir / "zombies.state";
    const auto tmp = state_dir / "zombies.state.tmp";
    if (wasm::write_file(tmp.string(), bytes)) {
        std::error_code ec;
        fs::rename(tmp, file, ec);
    }
}

// -- the system paglet ------------------------------------------------------------------------

class ZombiesService final : public services::ContractPaglet<zb::Contract, ZombiesService> {
public:
    explicit ZombiesService(Node::Impl& node) : node_(node) {}
    std::vector<std::string> default_ops() const override { return operations(); }

    void start(runtime::SystemContext& ctx) override {
        std::lock_guard lock(node_.mu);
        node_.zombies.ctx = &ctx;
    }

    void paglet_ended(runtime::SystemContext&, const runtime::PagletId& id) override {
        std::lock_guard lock(node_.mu);
        for (auto& [group, g] : node_.zombies.groups) {
            if (g.paglet == id) {
                g.paglet.clear();
                node_.zombies.dirty = true;
            }
        }
    }

    services::Result<zb::JoinReply> join(const zb::JoinRequest& q, services::Operation& op) {
        const auto* caller = replica(op);
        if (caller == nullptr) return std::unexpected(abi::denied);
        if (!valid_group(q.group)) return std::unexpected(abi::invalid_argument);
        std::lock_guard lock(node_.mu);
        auto& g = node_.zombies.groups[q.group];
        if (g.state.group.empty()) {
            g.state.group = q.group;
            g.state.sink_spec = q.sink_spec;
        }
        zb::JoinReply out;
        out.state = g.state;
        if (g.owner.empty()) g.owner = caller->owner;
        if (g.owner != caller->owner) {
            out.reason = "the group belongs to another owner";
            return out;
        }
        if (g.state.ending != zb::Ending::none) {
            out.reason = "the group ended";
            return out;
        }
        if (!g.paglet.empty() && g.paglet != caller->id && node_.runtime.info(g.paglet)) {
            out.reason = "another replica of the group runs on this host";
            return out;
        }
        g.paglet = caller->id;
        g.module = caller->module;
        g.members[q.replica] = {caller->id, mesh::key_id(node_.key.public_key())};
        node_.zombies.next_gossip = {};  // the others learn it soon
        g.joined_ms = now_ms();
        node_.zombies.dirty = true;
        out.accepted = true;
        out.state = g.state;
        return out;
    }

    services::Result<zb::Done> trace(const zb::TraceRequest& q, services::Operation& op) {
        const auto* caller = replica(op);
        if (caller == nullptr || q.records.size() > max_trace_batch) return std::unexpected(abi::denied);
        std::lock_guard lock(node_.mu);
        auto it = node_.zombies.groups.find(q.group);
        if (it == node_.zombies.groups.end() || it->second.paglet != caller->id) return std::unexpected(abi::denied);
        const std::string host = mesh::key_id(node_.key.public_key());
        for (auto r : q.records) {
            r.host = host;
            r.paglet = caller->id;
            it->second.traces.push_back(std::move(r));
        }
        while (it->second.traces.size() > max_traces_per_group) it->second.traces.pop_front();
        node_.zombies.dirty = true;
        return zb::Done{};
    }

    services::Result<zb::GroupState> ended(const zb::EndedRequest& q, services::Operation& op) {
        const auto* caller = replica(op);
        if (caller == nullptr) return std::unexpected(abi::denied);
        std::lock_guard lock(node_.mu);
        auto it = node_.zombies.groups.find(q.group);
        if (it == node_.zombies.groups.end() || it->second.paglet != caller->id) return std::unexpected(abi::denied);
        node_.zombie_end(q.group, zb::Ending::delivered, "the result was delivered");
        node_.save_zombies();  // durable before the replica ends
        return it->second.state;
    }

    services::Result<zb::GroupState> state(const zb::StateRequest& q, services::Operation&) {
        std::lock_guard lock(node_.mu);
        auto it = node_.zombies.groups.find(q.group);
        if (it == node_.zombies.groups.end()) return std::unexpected(abi::not_found);
        return it->second.state;
    }

    services::Result<zb::GroupState> cancel(const zb::CancelRequest& q, services::Operation& op) {
        std::lock_guard lock(node_.mu);
        auto it = node_.zombies.groups.find(q.group);
        if (it == node_.zombies.groups.end()) return std::unexpected(abi::not_found);
        if (!owns(op, it->second)) return std::unexpected(abi::denied);
        node_.zombie_end(q.group, zb::Ending::cancelled, q.reason.empty() ? "cancelled by its owner" : q.reason);
        node_.save_zombies();
        return it->second.state;
    }

    services::Result<zb::GroupState> redirect(const zb::RedirectRequest& q, services::Operation& op) {
        std::lock_guard lock(node_.mu);
        auto it = node_.zombies.groups.find(q.group);
        if (it == node_.zombies.groups.end()) return std::unexpected(abi::not_found);
        if (!owns(op, it->second)) return std::unexpected(abi::denied);
        zb::GroupState s = it->second.state;
        s.sink += 1;
        s.sink_spec = q.sink_spec;
        node_.zombie_merge(s);
        node_.zombies.next_gossip = {};
        node_.save_zombies();
        return it->second.state;
    }

    services::Result<zb::Groups> list(const zb::ListRequest&, services::Operation& op) {
        std::lock_guard lock(node_.mu);
        zb::Groups out;
        for (const auto& [id, g] : node_.zombies.groups) {
            if (owns(op, g)) out.groups.push_back(node_.zombie_info(g));
        }
        return out;
    }

    services::Result<zb::Traces> traces(const zb::TracesRequest& q, services::Operation& op) {
        std::lock_guard lock(node_.mu);
        auto it = node_.zombies.groups.find(q.group);
        if (it == node_.zombies.groups.end()) return std::unexpected(abi::not_found);
        if (!owns(op, it->second)) return std::unexpected(abi::denied);
        return zb::Traces{{it->second.traces.begin(), it->second.traces.end()}};
    }

    services::Result<zb::DeliverReply> deliver(const zb::DeliverRequest& q, services::Operation& op) {
        const auto* caller = replica(op);
        if (caller == nullptr) return std::unexpected(abi::denied);
        if (q.result.size() > max_result_bytes) return std::unexpected(abi::quota);
        std::lock_guard lock(node_.mu);
        auto it = node_.zombies.groups.find(q.group);
        if (it == node_.zombies.groups.end() || it->second.paglet != caller->id) return std::unexpected(abi::denied);
        const zb::GroupState& s = it->second.state;
        auto host = node_.zombie_sink_host(s.sink_spec);
        if (!host) return std::unexpected(abi::not_found);
        const std::string here = mesh::key_id(node_.key.public_key());
        if (*host == node_.key.public_key()) {
            const bool accepted = node_.zombie_accept(q.group, s.sink, here, q.name, q.result);
            return zb::DeliverReply{accepted, s.sink, here};
        }
        auto reply = op.call.defer();
        if (!reply) return std::unexpected(abi::internal);
        const std::uint64_t request = node_.next_request++;
        node_.zombies.deliveries[request] =
            Node::Impl::ZombieState::Delivery{std::move(*reply), Node::Impl::Clock::now() + deliver_wait, s.sink,
                                              mesh::key_id(*host)};
        node_.send_frame(*host, mesh::Map{{"t", mesh::Value("zb-deliver")},
                                          {"r", mesh::Value(static_cast<std::int64_t>(request))},
                                          {"g", mesh::Value(q.group)},
                                          {"s", mesh::Value(s.sink)},
                                          {"n", mesh::Value(q.name)},
                                          {"d", mesh::Value(q.result)}});
        return zb::DeliverReply{};  // answered later
    }

    services::Result<zb::Peers> peers(const zb::PeersRequest& q, services::Operation& op) {
        const auto* caller = replica(op);
        if (caller == nullptr) return std::unexpected(abi::denied);
        std::lock_guard lock(node_.mu);
        auto it = node_.zombies.groups.find(q.group);
        if (it == node_.zombies.groups.end() || it->second.paglet != caller->id) return std::unexpected(abi::denied);
        const std::string here = mesh::key_id(node_.key.public_key());
        zb::Peers out;
        for (const auto& [n, m] : it->second.members) {
            if (m.first == caller->id) continue;
            runtime::Cap c = op.ctx.endpoint(m.first, {"*"});
            if (m.second != here) c.host = m.second;  // where it is
            op.give.push_back(std::move(c));
            out.replicas.push_back(n);
        }
        return out;
    }

    services::Result<zb::StoredResult> result(const zb::ResultRequest& q, services::Operation& op) {
        std::lock_guard lock(node_.mu);
        auto it = node_.zombies.groups.find(q.group);
        if (it != node_.zombies.groups.end() && !owns(op, it->second)) return std::unexpected(abi::denied);
        return node_.zombie_result(q.group);
    }

private:
    // Replicas are paglets (not the host, not system paglets).
    static const abi::SenderRecord* replica(services::Operation& op) {
        const auto& caller = op.sender();
        if (!caller || caller->id == "host" || caller->id.starts_with("system.")) return nullptr;
        return &*caller;
    }
    static bool owns(services::Operation& op, const Node::Impl::ZombieGroup& g) {
        const auto& caller = op.sender();
        return caller && (caller->id == "host" || caller->owner == g.owner || g.owner.empty());
    }

    Node::Impl& node_;
};

zb::GroupInfo Node::Impl::zombie_info(const ZombieGroup& g) const {
    return zb::GroupInfo{g.state, g.owner, g.paglet, g.module, g.joined_ms, static_cast<std::int64_t>(g.traces.size())};
}

std::shared_ptr<runtime::SystemPaglet> make_zombies(Node::Impl& impl) {
    return std::make_shared<ZombiesService>(impl);
}

// -- Node API (CLI sessions) ------------------------------------------------------------------

void Node::set_zombie_gossip(std::chrono::milliseconds every) {
    std::lock_guard lock(impl_->mu);
    impl_->zombies.gossip_every = every;
    impl_->zombies.next_gossip = {};
}

std::vector<zb::GroupInfo> Node::zombie_groups(const std::string& owner) const {
    std::lock_guard lock(impl_->mu);
    std::vector<zb::GroupInfo> out;
    for (const auto& [id, g] : impl_->zombies.groups) {
        if (owner.empty() || g.owner == owner) out.push_back(impl_->zombie_info(g));
    }
    return out;
}

std::expected<zb::GroupState, std::string> Node::zombie_cancel(const std::string& group, const std::string& owner,
                                                               const std::string& reason) {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->zombies.groups.find(group);
    if (it == impl_->zombies.groups.end()) return std::unexpected("no group " + group + " on this host");
    if (!owner.empty() && it->second.owner != owner) return std::unexpected(std::string("not the group's owner"));
    impl_->zombie_end(group, zb::Ending::cancelled, reason.empty() ? "cancelled by its owner" : reason);
    impl_->save_zombies();
    return it->second.state;
}

std::expected<zb::GroupState, std::string> Node::zombie_redirect(const std::string& group, const std::string& owner,
                                                                 const std::string& sink_spec) {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->zombies.groups.find(group);
    if (it == impl_->zombies.groups.end()) return std::unexpected("no group " + group + " on this host");
    if (!owner.empty() && it->second.owner != owner) return std::unexpected(std::string("not the group's owner"));
    zb::GroupState s = it->second.state;
    s.sink += 1;
    s.sink_spec = sink_spec;
    impl_->zombie_merge(s);
    impl_->zombies.next_gossip = {};
    impl_->save_zombies();
    return it->second.state;
}

zb::StoredResult Node::Impl::zombie_result(const std::string& group) const {
    zb::StoredResult out;
    for (const auto& [k, r] : zombies.results) {
        if (k.first == group && (!out.found || r.sink > out.sink)) out = r;
    }
    return out;
}

zb::StoredResult Node::zombie_result(const std::string& group) const {
    std::lock_guard lock(impl_->mu);
    return impl_->zombie_result(group);
}

std::vector<zb::TraceRecord> Node::zombie_traces(const std::string& group) {
    auto promise = std::make_shared<std::promise<std::vector<zb::TraceRecord>>>();
    auto future = promise->get_future();
    {
        std::lock_guard lock(impl_->mu);
        impl_->collect_zombie_traces(group, [promise](std::vector<zb::TraceRecord> r) { promise->set_value(std::move(r)); });
    }
    // The collection ends with what arrived by its deadline (zombies_tick).
    if (future.wait_for(traces_wait + std::chrono::seconds(5)) != std::future_status::ready) return {};
    auto records = future.get();
    std::ranges::sort(records, [](const auto& a, const auto& b) {
        return std::tie(a.time_ms, a.paglet, a.incarnation, a.seq) < std::tie(b.time_ms, b.paglet, b.incarnation, b.seq);
    });
    return records;
}

}  // namespace paglets::node
