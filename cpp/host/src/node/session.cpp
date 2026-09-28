// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// CLI sessions of admins and owners (planning/cpp-networking.md, section 8).

#include "node_impl.hpp"

namespace paglets::node {

namespace {

runtime::Bytes answer(mesh::Map m) {
    m.emplace_back("ok", mesh::Value(true));
    return mesh::encode(mesh::Value(std::move(m)));
}

runtime::Bytes refusal(std::string_view why) {
    return mesh::encode(mesh::Value(mesh::Map{{"ok", mesh::Value(false)}, {"e", mesh::Value(why)}}));
}

}  // namespace

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

    return refusal("unknown request " + *type);
}

}  // namespace paglets::node
