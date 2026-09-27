// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/mesh/ledger.hpp>

#include <paglets/wasm/engine.hpp>

#include <algorithm>
#include <fstream>

namespace paglets::mesh {

namespace {

constexpr std::string_view merge_domain = "paglets admin epoch merge v1";
constexpr std::string_view record_suffix = ".rec";

std::optional<PublicKey> as_key(const Value& v) {
    const Bytes* b = v.as_bin();
    if (b == nullptr || b->size() != 32) return std::nullopt;
    PublicKey k{};
    std::copy(b->begin(), b->end(), k.begin());
    return k;
}

std::optional<std::vector<PublicKey>> key_list(const Fields& f, std::string_view name) {
    const Array* a = f.array(name);
    if (a == nullptr) return std::nullopt;
    std::vector<PublicKey> out;
    for (const auto& item : *a) {
        auto k = as_key(item);
        if (!k) return std::nullopt;
        out.push_back(*k);
    }
    return out;
}

std::optional<std::set<RecordId>> id_set(const Array& a) {
    std::set<RecordId> out;
    for (const auto& item : a) {
        auto k = as_key(item);  // IDs are 32 bytes as well
        if (!k) return std::nullopt;
        out.insert(*k);
    }
    return out;
}

std::optional<Quorum> parse_quorum(const Map& m) {
    const Fields f{m};
    auto admin_set = f.integer("admin_set");
    auto normal = f.integer("default");
    if (!admin_set || !normal || *admin_set < 1 || *normal < 1) return std::nullopt;
    return Quorum{*admin_set, *normal};
}

Value quorum_value(const Quorum& q) {
    return Value(Map{{"admin_set", Value(q.admin_set)}, {"default", Value(q.normal)}});
}

struct GenesisInfo {
    std::string name;
    std::set<PublicKey> admins;
    Quorum quorum;
};

std::expected<GenesisInfo, std::string> parse_genesis(const Record& r) {
    if (r.type() != "genesis" || r.mesh() || r.epoch()) return std::unexpected(std::string("not a genesis record"));
    const Fields f = r.fields();
    auto name = f.str("name");
    auto admins = key_list(f, "admins");
    const Map* quorum = f.submap("quorum");
    if (!name || !admins || admins->empty() || quorum == nullptr) {
        return std::unexpected(std::string("genesis needs a name, admins and a quorum"));
    }
    auto q = parse_quorum(*quorum);
    GenesisInfo g{*name, std::set<PublicKey>(admins->begin(), admins->end()), {}};
    if (!q || g.admins.size() != admins->size() || q->admin_set > static_cast<std::int64_t>(g.admins.size()) ||
        q->normal > static_cast<std::int64_t>(g.admins.size())) {
        return std::unexpected(std::string("genesis quorum or admin list invalid"));
    }
    g.quorum = *q;
    for (const auto& admin : g.admins) {
        if (!r.signed_by(admin)) return std::unexpected("genesis lacks the signature of admin " + key_id(admin));
    }
    return g;
}

struct AdminChange {
    std::set<PublicKey> add;
    std::map<PublicKey, std::set<RecordId>> remove;  // key -> records of that key that stay valid
    std::optional<Quorum> quorum;
};

std::optional<AdminChange> parse_admin_change(const Record& r) {
    const Fields f = r.fields();
    AdminChange c;
    if (f.get("add") != nullptr) {
        auto add = key_list(f, "add");
        if (!add) return std::nullopt;
        c.add.insert(add->begin(), add->end());
    }
    if (const Value* remove = f.get("remove")) {
        const Array* a = remove->as_array();
        if (a == nullptr) return std::nullopt;
        for (const auto& item : *a) {
            const Map* m = item.as_map();
            if (m == nullptr) return std::nullopt;
            const Fields rf{*m};
            const Value* key = rf.get("key");
            auto k = key == nullptr ? std::nullopt : as_key(*key);
            const Array* keep = rf.array("keep");
            if (!k) return std::nullopt;
            std::set<RecordId> kept;
            if (keep != nullptr) {
                auto ids = id_set(*keep);
                if (!ids) return std::nullopt;
                kept = std::move(*ids);
            }
            c.remove[*k].insert(kept.begin(), kept.end());
        }
    }
    if (const Map* q = f.submap("quorum")) {
        c.quorum = parse_quorum(*q);
        if (!c.quorum) return std::nullopt;
    } else if (f.get("quorum") != nullptr) {
        return std::nullopt;
    }
    if (c.add.empty() && c.remove.empty() && !c.quorum) return std::nullopt;
    return c;
}

// One admin epoch: the admin set in force between two admin-set changes.
struct Epoch {
    RecordId id{};
    std::set<PublicKey> admins;
    Quorum quorum;
    // Admins removed on entering this epoch, with the records of theirs
    // that stay valid.
    std::map<PublicKey, std::set<RecordId>> removed;
};

std::int64_t count_signers(const Record& r, const std::set<PublicKey>& allowed,
                           const std::set<PublicKey>& excluded = {}) {
    std::int64_t n = 0;
    for (const auto& s : r.signatures()) {
        if (allowed.contains(s.key) && !excluded.contains(s.key)) ++n;
    }
    return n;
}

bool quorum_fits(const Quorum& q, std::size_t admins) {
    return admins > 0 && q.admin_set <= static_cast<std::int64_t>(admins) &&
           q.normal <= static_cast<std::int64_t>(admins);
}

struct Deriver {
    const RecordId& mesh;
    const std::vector<const Record*>& records;  // in ledger order
    LedgerState st;
    std::vector<Epoch> chain;
    std::map<RecordId, std::size_t> epoch_index;

    void ignore(const Record& r, std::string reason) {
        st.ignored.push_back(IgnoredRecord{r.id(), r.type(), std::move(reason)});
    }

    // Resolves the chain of admin epochs from genesis. At each epoch, the
    // admin-set records naming it and signed by its admin quorum are its
    // successors. One successor becomes the next epoch. Several successors
    // conflict (they were made concurrently, or by an admin another one
    // removed): all their removals apply ("removals win"), and additions and
    // quorum changes apply only from successors that keep their quorum
    // without the signatures of admins the others remove. The merged epoch's
    // ID is the hash of the successors' IDs, so every host computes it; the
    // successors' own IDs stay names of the merged epoch, so records made in
    // one of them before the conflict was known keep their epoch.
    void resolve_admin_chain(const Record& genesis, const GenesisInfo& g) {
        chain.push_back(Epoch{genesis.id(), g.admins, g.quorum, {}});
        epoch_index[genesis.id()] = 0;
        std::map<RecordId, std::vector<const Record*>> successors;
        std::size_t admin_records = 0;
        for (const Record* r : records) {
            if (r->type() != "admin-set") continue;
            ++admin_records;
            if (!r->epoch()) {
                ignore(*r, "admin change without an epoch");
                continue;
            }
            successors[*r->epoch()].push_back(r);
        }
        std::vector<RecordId> aliases{genesis.id()};
        for (std::size_t step = 0; step <= admin_records; ++step) {
            const Epoch current = chain.back();
            std::vector<const Record*> named;
            for (const auto& alias : aliases) {
                auto it = successors.find(alias);
                if (it == successors.end()) continue;
                named.insert(named.end(), it->second.begin(), it->second.end());
                successors.erase(it);
            }
            std::vector<std::pair<const Record*, AdminChange>> candidates;
            for (const Record* r : named) {
                auto change = parse_admin_change(*r);
                if (!change) {
                    ignore(*r, "malformed admin change");
                    continue;
                }
                if (count_signers(*r, current.admins) < current.quorum.admin_set) {
                    ignore(*r, "admin change without the admin quorum of its epoch");
                    continue;
                }
                std::set<PublicKey> after = current.admins;
                after.insert(change->add.begin(), change->add.end());
                for (const auto& [key, keep] : change->remove) after.erase(key);
                if (!quorum_fits(change->quorum.value_or(current.quorum), after.size())) {
                    ignore(*r, "admin change would leave fewer admins than its quorum");
                    continue;
                }
                candidates.emplace_back(r, std::move(*change));
            }
            if (candidates.empty()) break;

            Epoch next;
            aliases.clear();
            if (candidates.size() == 1) {
                const auto& [r, change] = candidates.front();
                next.id = r->id();
                next.admins = current.admins;
                next.admins.insert(change.add.begin(), change.add.end());
                for (const auto& [key, keep] : change.remove) {
                    if (current.admins.contains(key)) next.removed[key] = keep;
                    next.admins.erase(key);
                }
                next.quorum = change.quorum.value_or(current.quorum);
            } else {
                std::set<PublicKey> removed_all;
                for (const auto& [r, change] : candidates) {
                    for (const auto& [key, keep] : change.remove) {
                        if (!current.admins.contains(key)) continue;
                        removed_all.insert(key);
                        next.removed[key].insert(keep.begin(), keep.end());
                    }
                }
                next.admins = current.admins;
                for (const auto& key : removed_all) next.admins.erase(key);
                next.quorum = current.quorum;
                Sha256 h;
                h.update(std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(merge_domain.data()),
                                                       merge_domain.size()));
                std::vector<RecordId> ids;
                for (const auto& [r, change] : candidates) ids.push_back(r->id());
                std::ranges::sort(ids);
                for (const auto& id : ids) h.update(id);
                next.id = h.finish();
                for (const auto& [r, change] : candidates) {
                    std::set<PublicKey> removed_by_others;
                    for (const auto& [other, other_change] : candidates) {
                        if (other == r) continue;
                        for (const auto& [key, keep] : other_change.remove) removed_by_others.insert(key);
                    }
                    if (count_signers(*r, current.admins, removed_by_others) < current.quorum.admin_set) {
                        ignore(*r, "conflicting admin change; only its removals apply");
                        continue;
                    }
                    for (const auto& key : change.add) {
                        if (!removed_all.contains(key)) next.admins.insert(key);
                    }
                    if (change.quorum) {
                        next.quorum.admin_set = std::max(next.quorum.admin_set, change.quorum->admin_set);
                        next.quorum.normal = std::max(next.quorum.normal, change.quorum->normal);
                    }
                }
                // Removals can leave fewer admins than a quorum; the
                // remaining admins must still be able to act.
                const auto n = static_cast<std::int64_t>(next.admins.size());
                if (n > 0) {
                    next.quorum.admin_set = std::min(next.quorum.admin_set, n);
                    next.quorum.normal = std::min(next.quorum.normal, n);
                }
                aliases = std::move(ids);
            }
            aliases.push_back(next.id);
            for (const auto& alias : aliases) epoch_index[alias] = chain.size();
            chain.push_back(std::move(next));
        }
        // Admin changes that never became part of the chain.
        for (const auto& [epoch, rs] : successors) {
            for (const Record* r : rs) ignore(*r, "admin change for an epoch that is not current");
        }
    }

    // Admin signatures that count for `r`: the signer was an admin in the
    // record's epoch and, if a later epoch removed the signer, the removal
    // kept this record. That rule makes records a removed admin signs later
    // (with an old epoch and any clock) ineffective.
    std::expected<void, std::string> check_admin_record(const Record& r) const {
        if (!r.epoch()) return std::unexpected(std::string("admin record without an epoch"));
        auto it = epoch_index.find(*r.epoch());
        if (it == epoch_index.end()) return std::unexpected(std::string("epoch not on the admin chain"));
        const std::size_t index = it->second;
        const Epoch& epoch = chain[index];
        std::int64_t valid = 0;
        for (const auto& s : r.signatures()) {
            if (!epoch.admins.contains(s.key)) continue;
            bool kept = true;
            for (std::size_t j = index + 1; j < chain.size() && kept; ++j) {
                auto removed = chain[j].removed.find(s.key);
                if (removed != chain[j].removed.end() && !removed->second.contains(r.id())) kept = false;
            }
            if (kept) ++valid;
        }
        if (valid < epoch.quorum.normal) return std::unexpected(std::string("not signed by an admin quorum"));
        return {};
    }

    // Grant requests, grants, releases and audit entries: records of hosts
    // (and owners), valid only from enrolled keys; host-derived grants only
    // within an allow rule.
    void derive_policy(const std::set<RecordId>& admin_valid, std::set<RecordId>& resolved) {
        std::map<RecordId, GrantRequest> open;
        std::set<RecordId> released;
        for (const Record* r : records) {
            const std::string& type = r->type();
            if (type == "grant-request") {
                auto g = parse_grant_request(*r);
                if (!g) {
                    ignore(*r, "malformed grant request");
                } else if (!r->signed_by(g->host)) {
                    ignore(*r, "grant request not signed by its requester");
                } else if (g->by_owner ? !st.is_owner(g->host) : !st.is_host(g->host)) {
                    ignore(*r, "grant request from a key that is not an enrolled host or the owner");
                } else {
                    open[r->id()] = std::move(*g);
                }
            } else if (type == "grant-release") {
                const Value* grant = r->fields().get("grant");
                const Bytes* id = grant == nullptr ? nullptr : grant->as_bin();
                const bool by_host =
                    std::ranges::any_of(r->signatures(), [&](const Record::Signed& s) { return st.is_host(s.key); });
                if (id == nullptr || id->size() != 32) {
                    ignore(*r, "malformed grant release");
                } else if (!by_host) {
                    ignore(*r, "grant release not signed by an enrolled host");
                } else {
                    RecordId g{};
                    std::copy(id->begin(), id->end(), g.begin());
                    released.insert(g);
                }
            } else if (type == "audit") {
                auto a = parse_audit(*r);
                if (!a) {
                    ignore(*r, "malformed audit record");
                } else if (!r->signed_by(a->host) || !st.is_host(a->host)) {
                    ignore(*r, "audit record not signed by an enrolled host");
                } else {
                    st.audit.push_back(std::move(*a));
                }
            }
        }
        for (const Record* r : records) {
            if (r->type() != "grant") continue;
            auto g = parse_grant(*r);
            if (!g) {
                ignore(*r, "malformed grant");
                continue;
            }
            if (admin_valid.contains(r->id())) {
                g->by_admin = true;
            } else if (auto why = check_derived_grant(*r, *g); !why.empty()) {
                ignore(*r, why);
                continue;
            }
            if (g->request) resolved.insert(*g->request);
            if (st.revoked_records.contains(r->id()) || released.contains(r->id())) {
                st.ended_grants.insert(r->id());
                continue;
            }
            st.grants.emplace(r->id(), std::move(*g));
        }
        for (auto& [id, request] : open) {
            if (!resolved.contains(id) && !st.revoked_records.contains(id)) {
                st.grant_requests.push_back(std::move(request));
            }
        }
    }

    // Empty if `g` is a valid grant a host derived from an allow rule;
    // otherwise why not.
    std::string check_derived_grant(const Record& r, const Grant& g) const {
        if (g.issuer == PublicKey{} || !r.signed_by(g.issuer)) return "grant not signed by an admin quorum or its host";
        if (!st.is_host(g.issuer)) return "grant issued by a key that is not an enrolled host";
        if (!g.rule) return "host grant without a rule";
        auto it = std::ranges::find(st.rules, *g.rule, &Rule::id);
        if (it == st.rules.end()) return "grant from a rule that is not valid";
        if (it->decision != Decision::allow) return "grant from a rule that does not allow";
        if (!rule_covers(st, *it, g.principal, g.issuer, g.item)) return "grant outside its rule";
        if (it->max_duration_ms && g.expires > r.time() + *it->max_duration_ms) {
            return "grant longer than its rule allows";
        }
        return {};
    }

    void run() {
        st.mesh = mesh;
        st.records = records.size();
        const Record* genesis = nullptr;
        for (const Record* r : records) {
            st.clock = std::max(st.clock, r->clock());
            if (r->id() == mesh) genesis = r;
        }
        if (genesis == nullptr) return;
        auto g = parse_genesis(*genesis);
        if (!g) {
            ignore(*genesis, g.error());
            return;
        }
        st.mesh_name = g->name;
        resolve_admin_chain(*genesis, *g);
        st.epoch = chain.back().id;
        st.admins = chain.back().admins;
        st.quorum = chain.back().quorum;

        // Revocations first: they win over enrollments regardless of order.
        std::set<RecordId> admin_valid;
        for (const Record* r : records) {
            const std::string& type = r->type();
            if (type == "genesis" || type == "admin-set" || type.ends_with("-request") || type == "audit" ||
                type == "grant-release") {
                continue;  // not admin records
            }
            auto ok = check_admin_record(*r);
            if (type == "grant") {
                if (ok) admin_valid.insert(r->id());  // otherwise possibly derived by a host (below)
                continue;
            }
            if (!ok) {
                ignore(*r, ok.error());
                continue;
            }
            admin_valid.insert(r->id());
            if (r->type() != "revoke") continue;
            const Fields f = r->fields();
            const Value* key = f.get("key");
            const Value* record = f.get("record");
            auto k = key == nullptr ? std::nullopt : as_key(*key);
            auto id = record == nullptr ? std::nullopt : as_key(*record);
            if ((key != nullptr && !k) || (record != nullptr && !id) || (!k && !id)) {
                ignore(*r, "malformed revocation");
                continue;
            }
            if (k) st.revoked_keys.insert(*k);
            if (id) st.revoked_records.insert(*id);
        }

        std::map<RecordId, PendingRequest> open;
        std::set<RecordId> resolved;
        std::size_t order = 0;
        for (const Record* r : records) {
            ++order;
            const std::string& type = r->type();
            if (type == "genesis" || type == "admin-set" || type == "revoke" || type == "grant" ||
                type == "grant-request" || type == "grant-release" || type == "audit") {
                continue;  // policy records follow below
            }
            const Fields f = r->fields();
            const Value* key_value = f.get("key");
            const auto key = key_value == nullptr ? std::nullopt : as_key(*key_value);
            if (type == "host-enroll-request" || type == "owner-enroll-request") {
                auto name = f.str("name");
                auto labels = f.get("labels") == nullptr
                                  ? std::optional<std::vector<std::string>>(std::vector<std::string>{})
                                  : f.strings("labels");
                if (!key || !name || !labels) {
                    ignore(*r, "malformed enrollment request");
                } else if (!r->signed_by(*key)) {
                    ignore(*r, "enrollment request not signed by its key");
                } else if (st.revoked_keys.contains(*key)) {
                    ignore(*r, "key revoked");
                } else {
                    const auto kind = type == "host-enroll-request" ? RequestKind::host : RequestKind::owner;
                    open[r->id()] = PendingRequest{r->id(), kind, *key, *name, std::move(*labels), r->time()};
                }
                continue;
            }
            if (!admin_valid.contains(r->id())) continue;  // already listed as ignored
            if (st.revoked_records.contains(r->id())) {
                ignore(*r, "revoked");
                continue;
            }
            if (type == "host-enroll" || type == "owner-enroll") {
                const bool host = type == "host-enroll";
                auto name = f.str("name");
                const std::string_view list_name = host ? "labels" : "groups";
                auto list = f.get(list_name) == nullptr
                                ? std::optional<std::vector<std::string>>(std::vector<std::string>{})
                                : f.strings(list_name);
                const Value* request = f.get("request");
                auto request_id = request == nullptr ? std::nullopt : as_key(*request);
                if (!key || !name || !list || (request != nullptr && !request_id)) {
                    ignore(*r, "malformed enrollment");
                    continue;
                }
                if (st.revoked_keys.contains(*key)) {
                    ignore(*r, "key revoked");
                    continue;
                }
                if (host) {
                    st.hosts[*key] = HostEntry{*key, *name, std::move(*list), r->id()};
                } else {
                    st.owners[*key] = OwnerEntry{*key, *name, std::move(*list), r->id()};
                }
                if (request_id) resolved.insert(*request_id);
            } else if (type == "host-remove" || type == "owner-remove") {
                if (!key) {
                    ignore(*r, "malformed removal");
                    continue;
                }
                if (type == "host-remove") {
                    st.hosts.erase(*key);
                } else {
                    st.owners.erase(*key);
                }
            } else if (type == "policy-rule") {
                auto rule = parse_rule(*r);
                if (!rule) {
                    ignore(*r, "malformed policy rule");
                    continue;
                }
                rule->order = order;
                st.rules.push_back(std::move(*rule));
            } else if (type == "request-deny") {
                const Value* request = f.get("request");
                auto request_id = request == nullptr ? std::nullopt : as_key(*request);
                if (!request_id) {
                    ignore(*r, "malformed denial");
                    continue;
                }
                resolved.insert(*request_id);
            } else {
                ignore(*r, "record type without effect in this version");
            }
        }
        for (const auto& key : st.revoked_keys) {
            st.hosts.erase(key);
            st.owners.erase(key);
        }
        derive_policy(admin_valid, resolved);
        for (auto& [id, request] : open) {
            if (resolved.contains(id) || st.revoked_keys.contains(request.key)) continue;
            if (request.kind == RequestKind::host ? st.is_host(request.key) : st.is_owner(request.key)) continue;
            st.pending.push_back(std::move(request));
        }
        std::ranges::sort(
            st.pending, [](const auto& a, const auto& b) { return a.time != b.time ? a.time < b.time : a.id < b.id; });
        std::ranges::sort(st.ignored, [](const auto& a, const auto& b) { return a.id < b.id; });
    }
};

std::expected<void, std::string> write_atomically(const std::filesystem::path& path,
                                                  std::span<const std::uint8_t> data) {
    const std::filesystem::path temp = path.string() + ".tmp";
    if (auto w = wasm::write_file(temp.string(), data); !w) return w;
    std::error_code ec;
    std::filesystem::rename(temp, path, ec);
    if (ec) return std::unexpected("cannot write " + path.string() + ": " + ec.message());
    return {};
}

}  // namespace

// ---------------------------------------------------------------------------

std::int64_t unix_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

Value LedgerState::to_value() const {
    auto keys = [](const auto& set) {
        Array a;
        for (const auto& k : set) a.push_back(Value::bin(k));
        return Value(std::move(a));
    };
    auto strings = [](const std::vector<std::string>& list) {
        Array a;
        for (const auto& s : list) a.emplace_back(s);
        return Value(std::move(a));
    };
    Array host_list;
    for (const auto& [key, h] : hosts) {
        host_list.emplace_back(Map{{"key", Value::bin(key)},
                                   {"name", Value(h.name)},
                                   {"labels", strings(h.labels)},
                                   {"record", Value::bin(h.record)}});
    }
    Array owner_list;
    for (const auto& [key, o] : owners) {
        owner_list.emplace_back(Map{{"key", Value::bin(key)},
                                    {"name", Value(o.name)},
                                    {"groups", strings(o.groups)},
                                    {"record", Value::bin(o.record)}});
    }
    Array pending_list;
    for (const auto& p : pending) pending_list.push_back(Value::bin(p.id));
    auto ids = [](const auto& list) {
        Array a;
        for (const auto& e : list) a.push_back(Value::bin(e.id));
        return Value(std::move(a));
    };
    auto grant_ids = [this] {
        Array a;
        for (const auto& [id, g] : grants) a.push_back(Value::bin(id));
        return Value(std::move(a));
    };
    Array ignored_list;
    for (const auto& i : ignored) ignored_list.push_back(Value::bin(i.id));
    return Value(Map{{"mesh", Value::bin(mesh)},
                     {"name", Value(mesh_name)},
                     {"epoch", Value::bin(epoch)},
                     {"admins", keys(admins)},
                     {"quorum", quorum_value(quorum)},
                     {"hosts", Value(std::move(host_list))},
                     {"owners", Value(std::move(owner_list))},
                     {"pending", Value(std::move(pending_list))},
                     {"revoked_keys", keys(revoked_keys)},
                     {"revoked_records", keys(revoked_records)},
                     {"ignored", Value(std::move(ignored_list))},
                     {"rules", ids(rules)},
                     {"grants", grant_ids()},
                     {"ended_grants", keys(ended_grants)},
                     {"grant_requests", ids(grant_requests)},
                     {"audit", ids(audit)},
                     {"clock", Value(clock)},
                     {"records", Value(static_cast<std::int64_t>(records))}});
}

Digest LedgerState::digest() const {
    return sha256(encode(to_value()));
}

std::expected<Record, std::string> make_genesis(std::string_view name, const std::vector<const SigningKey*>& admins,
                                                Quorum quorum, std::int64_t time_ms) {
    Array keys;
    for (const SigningKey* k : admins) keys.push_back(Value::bin(k->public_key()));
    auto r = Record::make(Record::Draft{
        "genesis", std::nullopt, 0, std::nullopt, time_ms,
        Map{{"name", Value(name)}, {"admins", Value(std::move(keys))}, {"quorum", quorum_value(quorum)}}});
    if (!r) return r;
    for (const SigningKey* k : admins) r->sign(*k);
    if (auto g = parse_genesis(*r); !g) return std::unexpected(g.error());
    return r;
}

LedgerState derive_state(const RecordId& mesh, const std::vector<const Record*>& records) {
    Deriver d{mesh, records, {}, {}, {}};
    d.run();
    return std::move(d.st);
}

// ---------------------------------------------------------------------------

struct Ledger::Impl {
    RecordId mesh{};
    std::filesystem::path directory;
    std::map<RecordId, Record> records;
    std::int64_t max_clock = 0;
    mutable std::optional<LedgerState> cache;

    std::expected<void, std::string> persist(const Record& r) const {
        if (directory.empty()) return {};
        return write_atomically(directory / (record_id_hex(r.id()) + std::string(record_suffix)), r.encode());
    }

    std::expected<AddResult, std::string> add(const Record& r) {
        if (r.type() == "genesis") {
            if (r.id() != mesh) return std::unexpected(std::string("a second genesis record"));
        } else if (r.mesh() != mesh) {
            return std::unexpected("record " + record_id_hex(r.id()) + " belongs to another mesh");
        }
        auto it = records.find(r.id());
        if (it != records.end()) {
            Record merged = it->second;
            if (merged.merge(r) == 0) return AddResult::known;
            if (auto p = persist(merged); !p) return std::unexpected(p.error());
            it->second = std::move(merged);
            cache.reset();
            return AddResult::merged;
        }
        if (auto p = persist(r); !p) return std::unexpected(p.error());
        records.emplace(r.id(), r);
        max_clock = std::max(max_clock, r.clock());
        cache.reset();
        return AddResult::added;
    }
};

Ledger::Ledger(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Ledger::Ledger(Ledger&&) noexcept = default;
Ledger& Ledger::operator=(Ledger&&) noexcept = default;
Ledger::~Ledger() = default;

std::expected<Ledger, std::string> Ledger::create(const Record& genesis, const std::filesystem::path& directory) {
    if (auto g = parse_genesis(genesis); !g) return std::unexpected(g.error());
    auto impl = std::make_unique<Impl>();
    impl->mesh = genesis.id();
    impl->directory = directory;
    if (!directory.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(directory, ec);
        if (ec) return std::unexpected("cannot create " + directory.string() + ": " + ec.message());
    }
    if (auto added = impl->add(genesis); !added) return std::unexpected(added.error());
    return Ledger(std::move(impl));
}

std::expected<Ledger, std::string> Ledger::open(const std::filesystem::path& directory, std::optional<RecordId> mesh) {
    std::error_code ec;
    std::vector<Record> loaded;
    for (const auto& entry : std::filesystem::directory_iterator(directory, ec)) {
        const auto path = entry.path();
        if (path.extension() != record_suffix) continue;
        auto bytes = wasm::read_file(path.string());
        if (!bytes) return std::unexpected(bytes.error());
        auto r = Record::decode(*bytes);
        if (!r) return std::unexpected(path.string() + ": " + r.error());
        if (path.stem().string() != record_id_hex(r->id())) {
            return std::unexpected(path.string() + ": file name does not match the record ID");
        }
        loaded.push_back(std::move(*r));
    }
    if (ec) return std::unexpected("cannot read " + directory.string() + ": " + ec.message());
    const Record* genesis = nullptr;
    for (const auto& r : loaded) {
        if (r.type() == "genesis") {
            if (genesis != nullptr) return std::unexpected(directory.string() + ": two genesis records");
            genesis = &r;
        }
    }
    if (genesis == nullptr) return std::unexpected(directory.string() + ": no genesis record");
    if (mesh && genesis->id() != *mesh) {
        return std::unexpected(directory.string() + ": the ledger belongs to another mesh");
    }
    if (auto g = parse_genesis(*genesis); !g) return std::unexpected(g.error());
    auto impl = std::make_unique<Impl>();
    impl->mesh = genesis->id();
    for (const auto& r : loaded) {
        if (auto added = impl->add(r); !added) return std::unexpected(added.error());
    }
    impl->directory = directory;  // records are already on disk
    return Ledger(std::move(impl));
}

const RecordId& Ledger::mesh() const {
    return impl_->mesh;
}

std::expected<AddResult, std::string> Ledger::add(const Record& record) {
    return impl_->add(record);
}

std::expected<AddResult, std::string> Ledger::add_encoded(std::span<const std::uint8_t> bytes) {
    auto r = Record::decode(bytes);
    if (!r) return std::unexpected(r.error());
    return impl_->add(*r);
}

const Record* Ledger::find(const RecordId& id) const {
    auto it = impl_->records.find(id);
    return it == impl_->records.end() ? nullptr : &it->second;
}

std::size_t Ledger::size() const {
    return impl_->records.size();
}

std::vector<const Record*> Ledger::records() const {
    std::vector<const Record*> out;
    out.reserve(impl_->records.size());
    for (const auto& [id, r] : impl_->records) out.push_back(&r);
    std::ranges::sort(out, [](const Record* a, const Record* b) { return a->before(*b); });
    return out;
}

Digest Ledger::digest() const {
    Sha256 h;
    for (const auto& [id, r] : impl_->records) {
        h.update(id);
        h.update(r.signature_digest());
    }
    return h.finish();
}

const LedgerState& Ledger::state() const {
    if (!impl_->cache) impl_->cache = derive_state(impl_->mesh, records());
    return *impl_->cache;
}

std::expected<Record, std::string> Ledger::draft(std::string type, Map data, bool with_epoch) const {
    Record::Draft d;
    d.type = std::move(type);
    d.mesh = impl_->mesh;
    d.clock = impl_->max_clock + 1;
    if (with_epoch) d.epoch = state().epoch;
    d.time = unix_ms();
    d.data = std::move(data);
    return Record::make(std::move(d));
}

// ---------------------------------------------------------------------------

namespace data {

namespace {

Value string_list(const std::vector<std::string>& list) {
    Array a;
    for (const auto& s : list) a.emplace_back(s);
    return Value(std::move(a));
}

}  // namespace

Map admin_set(const Ledger& ledger, const std::vector<PublicKey>& add, const std::vector<PublicKey>& remove,
              std::optional<Quorum> quorum) {
    Map m;
    if (!add.empty()) {
        Array a;
        for (const auto& k : add) a.push_back(Value::bin(k));
        m.emplace_back("add", Value(std::move(a)));
    }
    if (!remove.empty()) {
        Array a;
        for (const auto& k : remove) {
            Array keep;
            for (const Record* r : ledger.records()) {
                if (r->signed_by(k)) keep.push_back(Value::bin(r->id()));
            }
            a.emplace_back(Map{{"key", Value::bin(k)}, {"keep", Value(std::move(keep))}});
        }
        m.emplace_back("remove", Value(std::move(a)));
    }
    if (quorum) m.emplace_back("quorum", quorum_value(*quorum));
    return m;
}

Map host_enroll_request(const PublicKey& key, std::string_view name, const std::vector<std::string>& labels) {
    return Map{{"key", Value::bin(key)}, {"name", Value(name)}, {"labels", string_list(labels)}};
}

Map host_enroll(const PublicKey& key, std::string_view name, const std::vector<std::string>& labels,
                std::optional<RecordId> request) {
    Map m{{"key", Value::bin(key)}, {"name", Value(name)}, {"labels", string_list(labels)}};
    if (request) m.emplace_back("request", Value::bin(*request));
    return m;
}

Map owner_enroll_request(const PublicKey& key, std::string_view name) {
    return Map{{"key", Value::bin(key)}, {"name", Value(name)}};
}

Map owner_enroll(const PublicKey& key, std::string_view name, const std::vector<std::string>& groups,
                 std::optional<RecordId> request) {
    Map m{{"key", Value::bin(key)}, {"name", Value(name)}, {"groups", string_list(groups)}};
    if (request) m.emplace_back("request", Value::bin(*request));
    return m;
}

Map key_removal(const PublicKey& key) {
    return Map{{"key", Value::bin(key)}};
}

Map request_deny(const RecordId& request, std::string_view reason) {
    return Map{{"request", Value::bin(request)}, {"reason", Value(reason)}};
}

Map revoke_key(const PublicKey& key, std::string_view reason) {
    return Map{{"key", Value::bin(key)}, {"reason", Value(reason)}};
}

Map revoke_record(const RecordId& record, std::string_view reason) {
    return Map{{"record", Value::bin(record)}, {"reason", Value(reason)}};
}

}  // namespace data

}  // namespace paglets::mesh
