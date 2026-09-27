// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `directory` system paglet: lookups of system services (with the
// operations the policy allows) and of names paglets published for
// themselves. Published names are kept in the service state file and
// removed when their paglet ends.

#include "services.hpp"

#include <paglets/wire/reflect.hpp>

#include <algorithm>

namespace paglets::services::impl {

namespace {

// Stored form of the published names.
struct StoredName {
    std::string name;
    std::string paglet;
    std::string owner;
    std::vector<std::string> ops;
    bool is_public = false;
};

struct StoredNames {
    std::vector<StoredName> names;
};

}  // namespace

Directory::Directory(fs::path state_file, ServicePolicy policy)
    : state_file_(std::move(state_file)), policy_(std::move(policy)) {}

void Directory::start(runtime::SystemContext& ctx) {
    std::lock_guard lock(mu_);
    runtime_ = &ctx.runtime();
    if (state_file_.empty()) return;
    std::error_code ec;
    if (!fs::exists(state_file_, ec)) return;
    auto bytes = read_all(state_file_);
    StoredNames stored;
    if (!bytes || !wire::from_msgpack(*bytes, stored)) {
        ctx.log(abi::LogLevel::warning, "directory: cannot read " + state_file_.string());
        return;
    }
    for (auto& n : stored.names) {
        // Names of paglets that ended while the host was down are dropped.
        if (!runtime_->info(n.paglet)) continue;
        names_[n.name] = Published{std::move(n.paglet), std::move(n.owner), std::move(n.ops), n.is_public};
    }
}

void Directory::save() {
    if (state_file_.empty()) return;
    StoredNames stored;
    for (const auto& [name, p] : names_)
        stored.names.push_back(StoredName{name, p.paglet, p.owner, p.ops, p.is_public});
    std::error_code ec;
    fs::create_directories(state_file_.parent_path(), ec);
    (void)write_atomically(state_file_, wire::to_msgpack(stored));
}

void Directory::set_policy(ServicePolicy policy) {
    std::lock_guard lock(mu_);
    policy_ = std::move(policy);
}

void Directory::paglet_ended(runtime::SystemContext&, const runtime::PagletId& id) {
    std::lock_guard lock(mu_);
    const auto removed = std::erase_if(names_, [&](const auto& e) { return e.second.paglet == id; });
    if (removed > 0) save();
}

bool Directory::visible(const Published& p, const abi::SenderRecord& caller) const {
    return p.is_public || p.owner == caller.owner;
}

Result<directory::LookupReply> Directory::lookup(const directory::LookupRequest& q, Operation& op) {
    auto caller = caller_of(op.call);
    if (!caller) return std::unexpected(abi::denied);
    // System services first: their names cannot be published.
    if (auto service = op.ctx.runtime().system_paglet_object(q.name)) {
        const auto offered = service->operations();
        ServicePolicy policy;
        {
            std::lock_guard lock(mu_);
            policy = policy_;
        }
        std::vector<std::string> allowed = policy ? policy(*caller, q.name, offered) : offered;
        std::erase_if(allowed, [&](const std::string& o) { return std::ranges::find(offered, o) == offered.end(); });
        if (allowed.empty()) return std::unexpected(abi::denied);
        runtime::Cap endpoint = op.ctx.endpoint(runtime::system_paglet_id(q.name), allowed);
        endpoint.transferable = false;  // the policy applies to this paglet
        op.give.push_back(std::move(endpoint));
        return directory::LookupReply{directory::EntryKind::service, std::move(allowed)};
    }
    std::lock_guard lock(mu_);
    auto it = names_.find(q.name);
    if (it == names_.end() || !visible(it->second, *caller)) return std::unexpected(abi::not_found);
    std::vector<std::string> ops = it->second.ops.empty() ? std::vector<std::string>{"*"} : it->second.ops;
    op.give.push_back(op.ctx.endpoint(it->second.paglet, ops));
    return directory::LookupReply{directory::EntryKind::paglet, std::move(ops)};
}

Result<directory::PublishReply> Directory::publish(const directory::PublishRequest& q, Operation& op) {
    auto caller = caller_of(op.call);
    if (!caller) return std::unexpected(abi::denied);
    if (!valid_name(q.name, 64) || op.ctx.runtime().system_paglet(q.name))
        return std::unexpected(abi::invalid_argument);
    for (const auto& o : q.ops) {
        if (!abi::valid_message_name(o)) return std::unexpected(abi::invalid_argument);
    }
    std::lock_guard lock(mu_);
    auto it = names_.find(q.name);
    if (it != names_.end() && it->second.paglet != caller->id) return std::unexpected(abi::bad_state);
    names_[q.name] = Published{caller->id, caller->owner, q.ops, q.is_public};
    save();
    return directory::PublishReply{q.name};
}

Result<directory::UnpublishReply> Directory::unpublish(const directory::UnpublishRequest& q, Operation& op) {
    auto caller = caller_of(op.call);
    if (!caller) return std::unexpected(abi::denied);
    std::lock_guard lock(mu_);
    auto it = names_.find(q.name);
    if (it == names_.end() || it->second.paglet != caller->id) return directory::UnpublishReply{false};
    names_.erase(it);
    save();
    return directory::UnpublishReply{true};
}

Result<directory::ListReply> Directory::list(const directory::ListRequest& q, Operation& op) {
    auto caller = caller_of(op.call);
    if (!caller) return std::unexpected(abi::denied);
    directory::ListReply reply;
    for (const auto& name : op.ctx.runtime().system_paglet_names()) {
        if (name.starts_with(q.prefix)) reply.entries.push_back({name, directory::EntryKind::service, ""});
    }
    std::lock_guard lock(mu_);
    for (const auto& [name, p] : names_) {
        if (name.starts_with(q.prefix) && visible(p, *caller)) {
            reply.entries.push_back({name, directory::EntryKind::paglet, p.owner});
        }
    }
    std::ranges::sort(reply.entries, {}, &directory::Listing::name);
    return reply;
}

}  // namespace paglets::services::impl
