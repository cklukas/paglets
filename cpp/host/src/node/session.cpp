// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// CLI sessions of admins and owners (planning/cpp-networking.md, section 8).

#include "node_impl.hpp"

#include <future>

namespace paglets::node {

namespace {

constexpr auto session_wait = std::chrono::seconds(20);
constexpr std::int64_t max_session_pin_ms = 24LL * 3600 * 1000;

runtime::Bytes answer(mesh::Map m) {
    m.emplace_back("ok", mesh::Value(true));
    return mesh::encode(mesh::Value(std::move(m)));
}

runtime::Bytes refusal(std::string_view why) {
    return mesh::encode(mesh::Value(mesh::Map{{"ok", mesh::Value(false)}, {"e", mesh::Value(why)}}));
}

}  // namespace

// Runs a lookup (location.cpp) and waits for its end; called without locks.
static Node::Impl::LookupResult run_lookup(Node::Impl& impl, Node::Impl::Lookup l) {
    auto promise = std::make_shared<std::promise<Node::Impl::LookupResult>>();
    auto future = promise->get_future();
    l.done = [promise](const Node::Impl::LookupResult& r) { promise->set_value(r); };
    {
        std::lock_guard lock(impl.mu);
        impl.start_lookup(std::move(l));
    }
    if (future.wait_for(session_wait) != std::future_status::ready) {
        return Node::Impl::LookupResult{abi::timeout, "no answer in time"};
    }
    return future.get();
}

runtime::Bytes Node::answer_session(const mesh::PublicKey& peer, std::string_view role, runtime::Bytes request) {
    auto v = mesh::decode(request);
    if (!v || v->as_map() == nullptr) return refusal("malformed request");
    const mesh::Fields f{*v->as_map()};
    auto type = f.str("t");
    if (!type) return refusal("malformed request");
    const bool admin = role == "admin";
    const std::string peer_id = mesh::key_id(peer);

    // May this session act on the paglet? Its owner and admins may.
    auto may_act = [&](const runtime::PagletId& paglet) -> std::expected<void, std::string> {
        auto info = impl_->runtime.info(paglet);
        if (!info) return std::unexpected("no paglet " + paglet + " here");
        if (!admin && info->owner != peer_id) return std::unexpected(std::string("not the paglet's owner"));
        return {};
    };

    if (*type == "status") {
        mesh::Array paglets;
        for (const auto& p : impl_->runtime.list()) {
            if (p.module.empty()) continue;  // native system paglets
            paglets.emplace_back(mesh::Array{mesh::Value(p.id), mesh::Value(p.module), mesh::Value(p.owner),
                                             mesh::Value(runtime::to_string(p.state))});
        }
        std::lock_guard lock(impl_->mu);
        const mesh::LedgerState& st = impl_->ledger.state();
        auto self = st.hosts.find(impl_->key.public_key());
        return answer(mesh::Map{{"key", mesh::Value::bin(impl_->key.public_key())},
                                {"name", mesh::Value(self == st.hosts.end() ? std::string() : self->second.name)},
                                {"mesh", mesh::Value(st.mesh_name)},
                                {"digest", mesh::Value::bin(impl_->ledger.digest())},
                                {"records", mesh::Value(static_cast<std::int64_t>(st.records))},
                                {"paglets", mesh::Value(std::move(paglets))}});
    }

    if (*type == "push") {
        // Records are signed; the ledger decides what they mean.
        const mesh::Array* records = f.array("records");
        if (records == nullptr) return refusal("malformed request");
        std::int64_t added = 0;
        mesh::Array errors;
        for (const auto& r : *records) {
            const mesh::Bytes* bytes = r.as_bin();
            if (bytes == nullptr) return refusal("malformed request");
            auto record = mesh::Record::decode(*bytes);
            if (!record) {
                errors.emplace_back(record.error());
                continue;
            }
            auto result = submit(*record);
            if (!result) {
                errors.emplace_back(result.error());
            } else if (*result != mesh::AddResult::known) {
                ++added;
            }
        }
        return answer(mesh::Map{{"added", mesh::Value(added)}, {"errors", mesh::Value(std::move(errors))}});
    }

    if (*type == "launch") {
        // An owner starts one of its paglets here from its passport.
        auto passport_bytes = f.bin("passport");
        if (!passport_bytes) return refusal("malformed request");
        auto passport = mesh::Passport::decode(*passport_bytes);
        if (!passport) return refusal("passport: " + passport.error());
        if (passport->root().owner != peer) return refusal("the passport belongs to another owner");
        if (auto module = f.bin("module")) {
            if (auto added = impl_->runtime.modules().add_verified(passport->module(), std::move(*module)); !added) {
                return refusal(added.error());
            }
        }
        const std::string hash = to_hex(passport->module());
        if (!impl_->runtime.modules().contains(hash)) return refusal("module " + hash.substr(0, 16) + " is not here");
        auto id = create(hash, *passport, f.bin("args").value_or(runtime::Bytes{}));
        if (!id) return refusal(id.error());
        return answer(mesh::Map{{"paglet", mesh::Value(*id)}});
    }

    if (*type == "call") {
        auto paglet = f.str("paglet");
        auto name = f.str("name");
        if (!paglet || !name) return refusal("malformed request");
        if (auto ok = may_act(*paglet); !ok) return refusal(ok.error());
        const auto timeout = std::chrono::milliseconds(f.integer("timeout_ms").value_or(30000));
        const runtime::Reply r =
            impl_->runtime.call(*paglet, *name, f.bin("payload").value_or(runtime::Bytes{}), timeout);
        return answer(mesh::Map{{"status", mesh::Value(static_cast<std::int64_t>(r.status))},
                                {"payload", mesh::Value(r.payload)}});
    }

    if (*type == "dispatch") {
        auto paglet = f.str("paglet");
        auto destination = f.str("destination");
        if (!paglet || !destination) return refusal("malformed request");
        if (auto ok = may_act(*paglet); !ok) return refusal(ok.error());
        if (auto ok = dispatch(*paglet, *destination); !ok) return refusal(ok.error());
        return answer({});
    }

    // Location and pins (planning/cpp-location.md): admins, and owners for
    // their own paglets.
    auto locate = [&](const runtime::PagletId& paglet) {
        Impl::Lookup l;
        l.paglet = paglet;
        return run_lookup(*impl_, std::move(l));
    };
    auto location = [&](const Impl::LookupResult& r) {
        std::lock_guard lock(impl_->mu);
        const auto& hosts = impl_->ledger.state().hosts;
        auto h = hosts.find(r.where.host);
        return mesh::Map{{"host", mesh::Value::bin(r.where.host)},
                         {"host_name", mesh::Value(h == hosts.end() ? std::string() : h->second.name)},
                         {"moves", mesh::Value(static_cast<std::int64_t>(r.where.moves))},
                         {"moved", mesh::Value(r.where.moved)}};
    };

    if (*type == "locate" || *type == "pin") {
        auto paglet = f.str("paglet");
        if (!paglet) return refusal("malformed request");
        auto found = locate(*paglet);
        if (found.status != abi::ok) return refusal(found.error.empty() ? "not found" : found.error);
        if (!admin && found.where.owner != peer_id) return refusal("not the paglet's owner");
        if (*type == "locate") return answer(location(found));
        Impl::Lookup l;
        l.kind = Impl::LookupKind::pin;
        l.paglet = *paglet;
        l.pin = new_cap_id();
        l.until = mesh::unix_ms() + std::clamp<std::int64_t>(f.integer("duration_ms").value_or(600'000),
                                                             min_duration_ms, max_session_pin_ms);
        l.holder = std::string(role) + ":" + peer_id;
        l.reason = f.str("reason").value_or("");
        auto pinned = run_lookup(*impl_, std::move(l));
        if (pinned.status != abi::ok) return refusal(pinned.error.empty() ? "not pinned" : pinned.error);
        mesh::Map m = location(pinned);
        m.emplace_back("pin", mesh::Value(pinned.pin));
        m.emplace_back("until", mesh::Value(pinned.until));
        return answer(std::move(m));
    }

    if (*type == "pins") {
        mesh::Array list;
        for (const auto& p : pins()) {
            auto info = impl_->runtime.info(p.paglet);
            if (!admin && (!info || info->owner != peer_id)) continue;
            list.emplace_back(mesh::Array{mesh::Value(p.pin), mesh::Value(p.paglet), mesh::Value(p.until),
                                          mesh::Value(p.holder), mesh::Value(p.reason)});
        }
        return answer(mesh::Map{{"pins", mesh::Value(std::move(list))}});
    }

    if (*type == "unpin") {
        // Admins end pins wherever the paglet is.
        auto paglet = f.str("paglet");
        if (!paglet) return refusal("malformed request");
        if (!admin) return refusal("only admins end pins");
        Impl::Lookup l;
        l.kind = Impl::LookupKind::force;
        l.paglet = *paglet;
        l.pin = f.str("pin").value_or("");
        auto ended = run_lookup(*impl_, std::move(l));
        if (ended.status != abi::ok) return refusal(ended.error.empty() ? "not found" : ended.error);
        mesh::Map m = location(ended);
        m.emplace_back("released", mesh::Value(ended.released));
        return answer(std::move(m));
    }

    return refusal("unknown request " + *type);
}

}  // namespace paglets::node
