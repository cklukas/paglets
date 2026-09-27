// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/node/node.hpp>

#include <paglets/services/contract.hpp>
#include <paglets/services/grants.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/reflect.hpp>

#include <algorithm>
#include <mutex>
#include <set>

namespace paglets::node {

namespace {

namespace fs = std::filesystem;
namespace g = services::grants;
using services::Operation;
using services::Result;

constexpr std::int64_t min_duration_ms = 1000;
constexpr std::int64_t max_duration_ms = 30LL * 24 * 3600 * 1000;

std::string hex(const mesh::RecordId& id) {
    return mesh::record_id_hex(id);
}

std::optional<mesh::RecordId> parse_hex_id(std::string_view text) {
    return mesh::parse_key_id(text);  // record IDs have the same form as key IDs
}

std::string new_cap_id() {
    std::array<std::uint8_t, 16> bytes{};
    wasm::fill_random(bytes);
    return to_hex(std::span<const std::uint8_t>(bytes));
}

mesh::Item to_mesh(const g::Item& i) {
    return mesh::Item{i.service, i.ops, i.root, i.path};
}

g::Item to_contract(const mesh::Item& i) {
    return g::Item{i.service, i.ops, i.root, i.path};
}

// Relative paths of files items: the rules of dir capabilities.
bool valid_path(std::string_view path) {
    if (path.empty()) return true;
    std::size_t start = 0;
    while (start <= path.size()) {
        const std::size_t end = std::min(path.find('/', start), path.size());
        const std::string_view seg = path.substr(start, end - start);
        if (seg.empty() || seg == "." || seg == ".." || seg.find_first_of("\\:") != std::string_view::npos) {
            return false;
        }
        start = end + 1;
    }
    return true;
}

}  // namespace

class GrantsService;

struct Node::Impl {
    Impl(runtime::Runtime& rt, std::shared_ptr<services::SystemServices> svc, mesh::Ledger l, mesh::SigningKey k,
         fs::path dir)
        : runtime(rt), services(std::move(svc)), ledger(std::move(l)), key(std::move(k)), state_dir(std::move(dir)) {}

    runtime::Runtime& runtime;
    std::shared_ptr<services::SystemServices> services;
    mutable std::mutex mu;
    mesh::Ledger ledger;
    mesh::SigningKey key;
    std::unique_ptr<mesh::Replica> replica;
    fs::path state_dir;
    bool dirty = false;
    std::set<std::string> materialized;  // grants delivered on this host
    std::set<std::string> answered;      // denials delivered on this host
    runtime::SystemContext* grants_ctx = nullptr;

    // -- mu held -----------------------------------------------------------

    std::expected<mesh::Record, std::string> issue(std::string type, mesh::Map data) {
        auto r = ledger.draft(std::move(type), std::move(data), false);
        if (!r) return r;
        r->sign(key);
        if (auto added = replica->submit(*r); !added) return std::unexpected(added.error());
        return r;
    }

    static mesh::Principal principal_of(const abi::SenderRecord& s) {
        return mesh::Principal{s.id, s.owner, s.module, s.trust};
    }

    std::expected<runtime::Cap, std::string> materialize(const mesh::RecordId& id, const mesh::Grant& grant) {
        runtime::Cap cap;
        if (grant.item.service == "files") {
            auto dir = services->directory_capability(grant.item.root, grant.item.path, grant.item.ops, hex(id),
                                                      grant.expires);
            if (!dir) return std::unexpected(dir.error());
            cap = std::move(*dir);
        } else {
            if (!runtime.system_paglet(grant.item.service)) {
                return std::unexpected("no service " + grant.item.service + " on this host");
            }
            cap.kind = runtime::Cap::Kind::endpoint;
            cap.target = runtime::system_paglet_id(grant.item.service);
            cap.ops = grant.item.ops;
            cap.transferable = false;
            cap.grant = hex(id);
            cap.expires = grant.expires;
        }
        cap.id = new_cap_id();
        return cap;
    }

    void load() {
        if (state_dir.empty()) return;
        auto bytes = wasm::read_file((state_dir / "node.state").string());
        if (!bytes) return;
        auto v = mesh::decode(*bytes);
        if (!v || v->as_map() == nullptr) return;
        const mesh::Fields f{*v->as_map()};
        if (auto m = f.strings("materialized")) materialized.insert(m->begin(), m->end());
        if (auto a = f.strings("answered")) answered.insert(a->begin(), a->end());
    }

    void save() const {
        if (state_dir.empty()) return;
        auto list = [](const std::set<std::string>& s) {
            mesh::Array a;
            for (const auto& e : s) a.emplace_back(e);
            return mesh::Value(std::move(a));
        };
        const auto bytes =
            mesh::encode(mesh::Value(mesh::Map{{"materialized", list(materialized)}, {"answered", list(answered)}}));
        std::error_code ec;
        fs::create_directories(state_dir, ec);
        const fs::path temp = state_dir / "node.state.tmp";
        if (wasm::write_file(temp.string(), bytes)) fs::rename(temp, state_dir / "node.state", ec);
    }

    void sync_if_dirty() {
        if (!dirty) return;
        dirty = false;
        sync_locked();
    }

    void sync_locked() {
        const mesh::LedgerState& st = ledger.state();
        std::set<std::string, std::less<>> ended;
        for (const auto& id : st.ended_grants) ended.insert(hex(id));
        runtime.set_revoked_grants(std::move(ended));
        if (grants_ctx == nullptr) return;
        bool changed = false;
        const std::int64_t now = mesh::unix_ms();
        // Approvals: admin grants for paglets on this host.
        for (const auto& [id, grant] : st.grants) {
            const std::string h = hex(id);
            if (!grant.by_admin || materialized.contains(h) || grant.expires <= now) continue;
            if (!runtime.info(grant.principal.paglet) || !mesh::selects(st, grant.hosts, key.public_key())) continue;
            auto cap = materialize(id, grant);
            if (!cap) {
                grants_ctx->log(abi::LogLevel::warning, "grant " + h.substr(0, 16) + ": " + cap.error());
                continue;
            }
            g::Answer a{g::Status::granted,
                        to_contract(grant.item),
                        h,
                        grant.request ? hex(*grant.request) : std::string(),
                        std::string(),
                        grant.expires,
                        "approved"};
            if (grants_ctx->send(grant.principal.paglet, "grants.granted", wire::to_msgpack(a), {std::move(*cap)})) {
                materialized.insert(h);
                changed = true;
            }
        }
        // Denials of requests of paglets on this host.
        for (const mesh::Record* r : ledger.records()) {
            if (r->type() != "request-deny") continue;
            const mesh::Value* request = r->fields().get("request");
            const mesh::Bytes* bin = request == nullptr ? nullptr : request->as_bin();
            if (bin == nullptr || bin->size() != 32) continue;
            mesh::RecordId rid{};
            std::copy(bin->begin(), bin->end(), rid.begin());
            const std::string h = hex(rid);
            if (answered.contains(h)) continue;
            const mesh::Record* req = ledger.find(rid);
            if (req == nullptr || req->type() != "grant-request") continue;
            auto parsed = mesh::parse_grant_request(*req);
            if (!parsed || !runtime.info(parsed->principal.paglet)) continue;
            g::Answer a{g::Status::denied,
                        parsed->items.empty() ? g::Item{} : to_contract(parsed->items.front()),
                        std::string(),
                        h,
                        std::string(),
                        0,
                        r->fields().str("reason").value_or("denied by an admin")};
            if (grants_ctx->send(parsed->principal.paglet, "grants.denied", wire::to_msgpack(a))) {
                answered.insert(h);
                changed = true;
            }
        }
        if (changed) save();
    }
};

// -- the grants system paglet ---------------------------------------------------

class GrantsService final : public services::ContractPaglet<g::Contract, GrantsService> {
public:
    explicit GrantsService(Node::Impl& node) : node_(node) {}

    std::vector<std::string> default_ops() const override { return operations(); }

    void start(runtime::SystemContext& ctx) override {
        std::lock_guard lock(node_.mu);
        node_.grants_ctx = &ctx;
    }

    Result<g::Answer> request(const g::Request& q, Operation& op) {
        const auto& caller = op.sender();
        if (!caller || caller->id == "host") return std::unexpected(abi::denied);
        const mesh::Item item = to_mesh(q.item);
        if (auto valid = check_item(item); valid != abi::ok) return std::unexpected(valid);
        const std::int64_t duration = std::clamp(q.duration_ms, min_duration_ms, max_duration_ms);

        std::lock_guard lock(node_.mu);
        const mesh::PublicKey host = node_.key.public_key();
        const mesh::Principal principal = Node::Impl::principal_of(*caller);
        const mesh::Evaluation e = mesh::evaluate(node_.ledger.state(), principal, host, item);
        g::Answer answer;
        answer.item = q.item;
        if (e.rule) answer.rule = hex(*e.rule);
        switch (e.decision) {
            case mesh::Decision::allow: {
                const std::int64_t expires = mesh::unix_ms() + std::min(duration, e.max_duration_ms.value_or(duration));
                auto grant = node_.issue("grant", mesh::data::grant(principal, item, mesh::HostSelector{{host}, {}},
                                                                    expires, std::nullopt, e.rule, host));
                if (!grant) return fail(op, grant.error());
                (void)node_.issue("audit", mesh::data::audit(host, principal, item, mesh::Decision::allow, e.rule,
                                                             grant->id(), std::nullopt));
                auto parsed = mesh::parse_grant(*grant);
                if (!parsed) return fail(op, "a grant this host made does not parse");
                auto cap = node_.materialize(grant->id(), *parsed);
                if (!cap) return fail(op, cap.error());
                node_.materialized.insert(hex(grant->id()));
                node_.save();
                op.give.push_back(std::move(*cap));
                answer.status = g::Status::granted;
                answer.grant = hex(grant->id());
                answer.expires_ms = expires;
                answer.reason = "allowed by rule";
                break;
            }
            case mesh::Decision::ask: {
                auto request = node_.issue("grant-request",
                                           mesh::data::grant_request(principal, host, {item}, duration, q.reason));
                if (!request) return fail(op, request.error());
                (void)node_.issue("audit", mesh::data::audit(host, principal, item, mesh::Decision::ask, e.rule,
                                                             std::nullopt, request->id()));
                answer.status = g::Status::pending;
                answer.request = hex(request->id());
                answer.reason = "waiting for an admin";
                break;
            }
            case mesh::Decision::deny:
                (void)node_.issue("audit", mesh::data::audit(host, principal, item, mesh::Decision::deny, e.rule,
                                                             std::nullopt, std::nullopt));
                answer.status = g::Status::denied;
                answer.reason = e.rule ? "denied by rule" : "no rule allows it";
                break;
        }
        node_.sync_if_dirty();
        return answer;
    }

    Result<g::Answer> status(const g::StatusRequest& q, Operation& op) {
        const auto& caller = op.sender();
        auto id = parse_hex_id(q.request);
        if (!caller || !id) return std::unexpected(abi::invalid_argument);
        std::lock_guard lock(node_.mu);
        const mesh::Record* r = node_.ledger.find(*id);
        if (r == nullptr || r->type() != "grant-request") return std::unexpected(abi::not_found);
        auto request = mesh::parse_grant_request(*r);
        if (!request || request->principal.paglet != caller->id) return std::unexpected(abi::denied);
        const mesh::LedgerState& st = node_.ledger.state();
        g::Answer a;
        a.request = q.request;
        a.item = request->items.empty() ? g::Item{} : to_contract(request->items.front());
        if (std::ranges::any_of(st.grant_requests, [&](const auto& p) { return p.id == *id; })) {
            a.status = g::Status::pending;
            return a;
        }
        for (const auto& [gid, grant] : st.grants) {
            if (grant.request == *id) {
                a.status = g::Status::granted;
                a.grant = hex(gid);
                a.expires_ms = grant.expires;
                return a;
            }
        }
        a.status = g::Status::denied;
        return a;
    }

    Result<g::ReleaseReply> release(const g::ReleaseRequest& q, Operation& op) {
        const auto& caller = op.sender();
        auto id = parse_hex_id(q.grant);
        if (!caller || !id) return std::unexpected(abi::invalid_argument);
        std::lock_guard lock(node_.mu);
        const auto& grants = node_.ledger.state().grants;
        auto it = grants.find(*id);
        if (it == grants.end()) return g::ReleaseReply{false};
        if (it->second.principal.paglet != caller->id) return std::unexpected(abi::denied);
        if (auto r = node_.issue("grant-release", mesh::data::grant_release(*id)); !r) return fail(op, r.error());
        node_.sync_if_dirty();
        return g::ReleaseReply{true};
    }

    Result<g::ListReply> list(const g::ListRequest&, Operation& op) {
        const auto& caller = op.sender();
        if (!caller) return std::unexpected(abi::denied);
        std::lock_guard lock(node_.mu);
        g::ListReply reply;
        const std::int64_t now = mesh::unix_ms();
        for (const auto& [id, grant] : node_.ledger.state().grants) {
            if (grant.principal.paglet == caller->id && grant.expires > now) {
                reply.grants.push_back(g::GrantInfo{hex(id), to_contract(grant.item), grant.expires});
            }
        }
        return reply;
    }

private:
    std::int32_t check_item(const mesh::Item& item) const {
        if (item.service.empty() || item.ops.empty()) return abi::invalid_argument;
        for (const auto& o : item.ops) {
            if (!abi::valid_message_name(o)) return abi::invalid_argument;
        }
        if (item.service == "files") {
            if (item.root.empty() || !valid_path(item.path)) return abi::invalid_argument;
            if (!node_.services->directory_capability(item.root, item.path, item.ops)) return abi::not_found;
            return abi::ok;
        }
        if (!item.root.empty() || !item.path.empty()) return abi::invalid_argument;
        auto service = node_.runtime.system_paglet_object(item.service);
        if (!service) return abi::not_found;
        const auto offered = service->operations();
        for (const auto& o : item.ops) {
            if (std::ranges::find(offered, o) == offered.end()) return abi::invalid_argument;
        }
        return abi::ok;
    }

    static std::unexpected<std::int32_t> fail(Operation& op, const std::string& why) {
        op.ctx.log(abi::LogLevel::error, "grants: " + why);
        return std::unexpected(std::int32_t{abi::internal});
    }

    Node::Impl& node_;
};

// -- Node ---------------------------------------------------------------------------

Node::Node(runtime::Runtime& runtime, std::shared_ptr<services::SystemServices> services, mesh::Ledger ledger,
           mesh::SigningKey host_key, mesh::GossipTransport& transport, std::filesystem::path state_dir)
    : impl_(std::make_unique<Impl>(runtime, std::move(services), std::move(ledger), std::move(host_key),
                                   std::move(state_dir))) {
    impl_->replica = std::make_unique<mesh::Replica>(impl_->ledger, impl_->key.public_key(), transport);
    impl_->replica->on_change([impl = impl_.get()] { impl->dirty = true; });
    impl_->load();
}

Node::~Node() = default;

std::expected<void, std::string> Node::start() {
    auto grants = std::make_shared<GrantsService>(*impl_);
    if (auto id = impl_->runtime.add_system_paglet(grants); !id) return std::unexpected(id.error());
    // Services with ambient authority: only the operations the rules allow.
    impl_->services->set_policy([impl = impl_.get()](const abi::SenderRecord& caller, std::string_view service,
                                                     const std::vector<std::string>& offered) {
        auto object = impl->runtime.system_paglet_object(service);
        if (!object || !object->ambient()) return offered;
        std::lock_guard lock(impl->mu);
        const mesh::Principal principal = Impl::principal_of(caller);
        std::vector<std::string> allowed;
        for (const auto& op : offered) {
            const mesh::Item item{std::string(service), {op}, {}, {}};
            const auto e = mesh::evaluate(impl->ledger.state(), principal, impl->key.public_key(), item);
            if (e.decision == mesh::Decision::allow) allowed.push_back(op);
        }
        return allowed;
    });
    sync();
    return {};
}

const mesh::PublicKey& Node::host() const {
    return impl_->key.public_key();
}

const mesh::RecordId& Node::mesh() const {
    return impl_->ledger.mesh();
}

void Node::add_seed(const mesh::PublicKey& peer) {
    std::lock_guard lock(impl_->mu);
    impl_->replica->add_seed(peer);
}

void Node::receive(const mesh::PublicKey& from, std::span<const std::uint8_t> frame) {
    std::lock_guard lock(impl_->mu);
    impl_->replica->receive(from, frame);
    impl_->sync_if_dirty();
}

void Node::tick() {
    std::lock_guard lock(impl_->mu);
    impl_->replica->tick();
}

std::expected<mesh::AddResult, std::string> Node::submit(const mesh::Record& record) {
    std::lock_guard lock(impl_->mu);
    auto r = impl_->replica->submit(record);
    impl_->sync_if_dirty();
    return r;
}

mesh::LedgerState Node::state() const {
    std::lock_guard lock(impl_->mu);
    return impl_->ledger.state();
}

paglets::Digest Node::ledger_digest() const {
    std::lock_guard lock(impl_->mu);
    return impl_->ledger.digest();
}

std::expected<mesh::Record, std::string> Node::draft(std::string type, mesh::Map data, bool with_epoch) const {
    std::lock_guard lock(impl_->mu);
    return impl_->ledger.draft(std::move(type), std::move(data), with_epoch);
}

void Node::sync() {
    std::lock_guard lock(impl_->mu);
    impl_->dirty = false;
    impl_->sync_locked();
}

}  // namespace paglets::node
