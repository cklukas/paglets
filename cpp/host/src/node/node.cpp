// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "node_impl.hpp"

namespace paglets::node {

class GrantsService;

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
    : impl_(std::make_unique<Impl>(runtime, std::move(services), std::move(ledger), std::move(host_key), transport,
                                   std::move(state_dir))) {
    impl_->replica = std::make_unique<mesh::Replica>(impl_->ledger, impl_->key.public_key(), transport);
    impl_->replica->on_change([impl = impl_.get()] { impl->dirty = true; });
    impl_->load();
    impl_->load_locations();
    impl_->load_registry();
    // Pins survive restarts: the paglets they hold stay here.
    for (const auto& [id, lease] : impl_->pins) (void)impl_->runtime.pin(lease.paglet, id, lease.until);
}

Node::~Node() {
    impl_->runtime.set_module_admission({});
    impl_->runtime.set_mobility({});
}

std::expected<void, std::string> Node::start() {
    auto grants = std::make_shared<GrantsService>(*impl_);
    if (auto id = impl_->runtime.add_system_paglet(grants); !id) return std::unexpected(id.error());
    if (auto id = impl_->runtime.add_system_paglet(make_locator(*impl_)); !id) return std::unexpected(id.error());
    for (auto& service : make_compute_services(*impl_)) {
        if (auto id = impl_->runtime.add_system_paglet(service); !id) return std::unexpected(id.error());
    }
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
    impl_->runtime.set_module_admission([impl = impl_.get()](const std::string& module, runtime::TrustClass trust) {
        return impl->admit(module, trust);
    });
    impl_->install_mobility();
    return {};
}

const mesh::PublicKey& Node::host() const {
    return impl_->key.public_key();
}

const mesh::RecordId& Node::mesh() const {
    return impl_->ledger.mesh();
}

const mesh::SigningKey& Node::host_key() const {
    return impl_->key;
}

std::vector<mesh::PublicKey> Node::peers() const {
    std::lock_guard lock(impl_->mu);
    return impl_->replica->peers();
}

void Node::add_seed(const mesh::PublicKey& peer) {
    std::lock_guard lock(impl_->mu);
    impl_->replica->add_seed(peer);
}

void Node::receive(const mesh::PublicKey& from, std::span<const std::uint8_t> frame) {
    std::lock_guard lock(impl_->mu);
    impl_->heard(from);
    if (!impl_->handle_node_frame(from, frame)) impl_->replica->receive(from, frame);
    impl_->process_unresolved();
    impl_->sync_if_dirty();
}

void Node::tick() {
    std::lock_guard lock(impl_->mu);
    impl_->replica->tick();
    impl_->check_fetch_deadlines();
    impl_->take_spawns();
    impl_->check_move_deadlines();
    impl_->location_tick();
    impl_->registry_tick();
    impl_->compute_tick();
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

std::expected<runtime::PagletId, std::string> Node::create(std::string_view module, const mesh::Passport& passport,
                                                           runtime::Bytes args) {
    std::lock_guard lock(impl_->mu);
    return impl_->create_locked(module, passport, std::move(args));
}

std::optional<mesh::Passport> Node::passport(const runtime::PagletId& paglet) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->passports.find(paglet);
    if (it == impl_->passports.end()) return std::nullopt;
    return it->second;
}

void Node::add_module_source(std::shared_ptr<runtime::ModuleSource> source) {
    std::lock_guard lock(impl_->mu);
    impl_->sources.push_back(std::move(source));
}

void Node::set_fetch_timeout(std::chrono::milliseconds timeout) {
    std::lock_guard lock(impl_->mu);
    impl_->fetch_timeout = timeout;
}

void Node::fetch_module(const Digest& module, std::optional<mesh::PublicKey> from) {
    std::lock_guard lock(impl_->mu);
    impl_->need_module(module, from, [impl = impl_.get(), module](const std::expected<std::string, std::string>& r) {
        if (!r) impl->warn("fetching module " + to_hex(module).substr(0, 16) + " failed: " + r.error());
    });
}

bool Node::fetching(const Digest& module) const {
    std::lock_guard lock(impl_->mu);
    return impl_->fetches.contains(module);
}

std::expected<std::uint64_t, std::string> Node::launch(const mesh::PublicKey& host, const mesh::Passport& passport,
                                                       runtime::Bytes args) {
    std::lock_guard lock(impl_->mu);
    const mesh::LedgerState& st = impl_->ledger.state();
    const bool here = host == impl_->key.public_key();
    if (!here && !st.is_host(host)) return std::unexpected(std::string("the target is not an enrolled host"));
    if (!passport.links().empty()) return std::unexpected(std::string("a root paglet needs a root passport"));
    const std::uint64_t id = impl_->next_request++;
    impl_->launches.emplace(id, std::pair{host, LaunchStatus{}});
    if (here) {
        impl_->start_launch(std::nullopt, passport.encode(), std::move(args),
                            [impl = impl_.get(), id](const std::expected<runtime::PagletId, std::string>& result) {
                                LaunchStatus& status = impl->launches.at(id).second;
                                if (result) {
                                    status.state = LaunchStatus::State::running;
                                    status.paglet = *result;
                                } else {
                                    status.state = LaunchStatus::State::failed;
                                    status.error = result.error();
                                }
                            });
        return id;
    }
    impl_->send_frame(host, mesh::Map{{"t", mesh::Value(frame_launch)},
                                      {"r", mesh::Value(static_cast<std::int64_t>(id))},
                                      {"p", mesh::Value(passport.encode())},
                                      {"a", mesh::Value(std::move(args))}});
    return id;
}

std::optional<Node::LaunchStatus> Node::launch_status(std::uint64_t launch) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->launches.find(launch);
    if (it == impl_->launches.end()) return std::nullopt;
    return it->second.second;
}

}  // namespace paglets::node
