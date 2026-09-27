// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/glob.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/policy.hpp>

#include <algorithm>

namespace paglets::mesh {

namespace {

std::optional<PublicKey> key_of(const Value& v) {
    const Bytes* b = v.as_bin();
    if (b == nullptr || b->size() != 32) return std::nullopt;
    PublicKey k{};
    std::copy(b->begin(), b->end(), k.begin());
    return k;
}

std::optional<std::vector<PublicKey>> keys_of(const Value& v) {
    const Array* a = v.as_array();
    if (a == nullptr) return std::nullopt;
    std::vector<PublicKey> out;
    for (const auto& item : *a) {
        auto k = key_of(item);
        if (!k) return std::nullopt;
        out.push_back(*k);
    }
    return out;
}

std::optional<std::vector<std::string>> strings_of(const Value& v) {
    const Array* a = v.as_array();
    if (a == nullptr) return std::nullopt;
    std::vector<std::string> out;
    for (const auto& item : *a) {
        if (item.as_str() == nullptr) return std::nullopt;
        out.push_back(*item.as_str());
    }
    return out;
}

Value strings_value(const std::vector<std::string>& list) {
    Array a;
    for (const auto& s : list) a.emplace_back(s);
    return Value(std::move(a));
}

Value keys_value(const std::vector<PublicKey>& keys) {
    Array a;
    for (const auto& k : keys) a.push_back(Value::bin(k));
    return Value(std::move(a));
}

// Optional member helpers: absent is fine, a wrong type is not.
template <class T, class F>
bool optional_member(const Fields& f, std::string_view key, std::optional<T>& out, F&& parse) {
    const Value* v = f.get(key);
    if (v == nullptr) return true;
    out = parse(*v);
    return out.has_value();
}

std::optional<RecordId> id_member(const Fields& f, std::string_view key, bool& ok) {
    const Value* v = f.get(key);
    if (v == nullptr) return std::nullopt;
    auto id = key_of(*v);
    if (!id) ok = false;
    return id;
}

bool contains(const std::vector<std::string>& list, std::string_view s) {
    return std::ranges::find(list, s) != list.end();
}

int decision_rank(Decision d) {
    switch (d) {
        case Decision::deny: return 2;
        case Decision::ask: return 1;
        case Decision::allow: return 0;
    }
    return 2;
}

// Is `a` a better (more specific) match than `b`?
bool better(const Rule& a, const Rule& b) {
    if (a.priority != b.priority) return a.priority > b.priority;
    if (a.match.members() != b.match.members()) return a.match.members() > b.match.members();
    if (a.scope.has_value() != b.scope.has_value()) return a.scope.has_value();
    if (decision_rank(a.decision) != decision_rank(b.decision)) {
        return decision_rank(a.decision) > decision_rank(b.decision);
    }
    return a.order < b.order;
}

}  // namespace

int Match::members() const {
    return (owners ? 1 : 0) + (groups ? 1 : 0) + (modules ? 1 : 0) + (trust ? 1 : 0) + (hosts ? 1 : 0);
}

std::string_view to_string(Decision d) {
    switch (d) {
        case Decision::allow: return "allow";
        case Decision::ask: return "ask";
        case Decision::deny: return "deny";
    }
    return "deny";
}

std::optional<Decision> parse_decision(std::string_view text) {
    if (text == "allow") return Decision::allow;
    if (text == "ask") return Decision::ask;
    if (text == "deny") return Decision::deny;
    return std::nullopt;
}

// -- values -------------------------------------------------------------------

Value to_value(const Item& item) {
    Map m{{"service", Value(item.service)}, {"ops", strings_value(item.ops)}};
    if (!item.root.empty()) m.emplace_back("root", Value(item.root));
    if (!item.path.empty()) m.emplace_back("path", Value(item.path));
    return Value(std::move(m));
}

std::optional<Item> parse_item(const Value& value) {
    const Map* m = value.as_map();
    if (m == nullptr) return std::nullopt;
    const Fields f{*m};
    auto service = f.str("service");
    const Value* ops_value = f.get("ops");
    auto ops = ops_value == nullptr ? std::nullopt : strings_of(*ops_value);
    if (!service || service->empty() || !ops || ops->empty()) return std::nullopt;
    Item item{*service, std::move(*ops), f.str("root").value_or(""), f.str("path").value_or("")};
    if ((f.get("root") != nullptr && !f.str("root")) || (f.get("path") != nullptr && !f.str("path"))) {
        return std::nullopt;
    }
    if (item.root.empty() && !item.path.empty()) return std::nullopt;
    return item;
}

Value to_value(const HostSelector& hosts) {
    Map m;
    if (!hosts.keys.empty()) m.emplace_back("keys", keys_value(hosts.keys));
    if (!hosts.labels.empty()) m.emplace_back("labels", strings_value(hosts.labels));
    return Value(std::move(m));
}

std::optional<HostSelector> parse_hosts(const Value& value) {
    const Map* m = value.as_map();
    if (m == nullptr) return std::nullopt;
    const Fields f{*m};
    HostSelector h;
    if (const Value* k = f.get("keys")) {
        auto keys = keys_of(*k);
        if (!keys) return std::nullopt;
        h.keys = std::move(*keys);
    }
    if (const Value* l = f.get("labels")) {
        auto labels = strings_of(*l);
        if (!labels) return std::nullopt;
        h.labels = std::move(*labels);
    }
    return h;
}

Value to_value(const Principal& p) {
    return Value(Map{{"paglet", Value(p.paglet)},
                     {"owner", Value(p.owner)},
                     {"module", Value(p.module)},
                     {"trust", Value(p.trust)}});
}

std::optional<Principal> parse_principal(const Fields& f) {
    auto paglet = f.str("paglet");
    auto owner = f.str("owner");
    auto module = f.str("module");
    auto trust = f.str("trust");
    if (!paglet || paglet->empty() || !owner || !module || !trust) return std::nullopt;
    return Principal{*paglet, *owner, *module, *trust};
}

namespace {

std::optional<Match> parse_match(const Value& value) {
    const Map* m = value.as_map();
    if (m == nullptr) return std::nullopt;
    const Fields f{*m};
    Match match;
    if (!optional_member(f, "owners", match.owners, keys_of) ||
        !optional_member(f, "groups", match.groups, strings_of) ||
        !optional_member(f, "modules", match.modules, strings_of) ||
        !optional_member(f, "trust", match.trust, strings_of) ||
        !optional_member(f, "hosts", match.hosts, parse_hosts)) {
        return std::nullopt;
    }
    return match;
}

Value match_value(const Match& match) {
    Map m;
    if (match.owners) m.emplace_back("owners", keys_value(*match.owners));
    if (match.groups) m.emplace_back("groups", strings_value(*match.groups));
    if (match.modules) m.emplace_back("modules", strings_value(*match.modules));
    if (match.trust) m.emplace_back("trust", strings_value(*match.trust));
    if (match.hosts) m.emplace_back("hosts", to_value(*match.hosts));
    return Value(std::move(m));
}

std::optional<Scope> parse_scope(const Value& value) {
    const Map* m = value.as_map();
    if (m == nullptr) return std::nullopt;
    const Fields f{*m};
    Scope scope;
    if (!optional_member(f, "roots", scope.roots, strings_of) ||
        !optional_member(f, "paths", scope.paths, strings_of)) {
        return std::nullopt;
    }
    return scope;
}

Value scope_value(const Scope& scope) {
    Map m;
    if (scope.roots) m.emplace_back("roots", strings_value(*scope.roots));
    if (scope.paths) m.emplace_back("paths", strings_value(*scope.paths));
    return Value(std::move(m));
}

std::optional<std::vector<Item>> items_of(const Value& value) {
    const Array* a = value.as_array();
    if (a == nullptr || a->empty()) return std::nullopt;
    std::vector<Item> items;
    for (const auto& v : *a) {
        auto item = parse_item(v);
        if (!item) return std::nullopt;
        items.push_back(std::move(*item));
    }
    return items;
}

Value items_value(const std::vector<Item>& items) {
    Array a;
    for (const auto& item : items) a.push_back(to_value(item));
    return Value(std::move(a));
}

}  // namespace

std::optional<Rule> parse_rule(const Record& r) {
    const Fields f = r.fields();
    Rule rule;
    rule.id = r.id();
    auto name = f.str("name");
    auto decision = f.str("decision");
    auto service = f.str("service");
    const Value* ops = f.get("ops");
    const Value* match = f.get("match");
    if (!name || !decision || !service || service->empty() || ops == nullptr || match == nullptr) return std::nullopt;
    auto d = parse_decision(*decision);
    auto op_list = strings_of(*ops);
    auto m = parse_match(*match);
    if (!d || !op_list || op_list->empty() || !m) return std::nullopt;
    rule.name = *name;
    rule.decision = *d;
    rule.service = *service;
    rule.ops = std::move(*op_list);
    rule.match = std::move(*m);
    if (!optional_member(f, "scope", rule.scope, parse_scope)) return std::nullopt;
    if (f.get("max_duration_ms") != nullptr) {
        rule.max_duration_ms = f.integer("max_duration_ms");
        if (!rule.max_duration_ms || *rule.max_duration_ms <= 0) return std::nullopt;
    }
    if (f.get("priority") != nullptr) {
        auto p = f.integer("priority");
        if (!p) return std::nullopt;
        rule.priority = *p;
    }
    return rule;
}

std::optional<GrantRequest> parse_grant_request(const Record& r) {
    const Fields f = r.fields();
    auto principal = parse_principal(f);
    const Value* host = f.get("host");
    const Value* items = f.get("items");
    auto duration = f.integer("duration_ms");
    if (!principal || host == nullptr || items == nullptr || !duration || *duration <= 0) return std::nullopt;
    auto key = key_of(*host);
    auto list = items_of(*items);
    if (!key || !list) return std::nullopt;
    GrantRequest g;
    g.id = r.id();
    g.principal = std::move(*principal);
    g.host = *key;
    g.by_owner = parse_key_id(g.principal.owner) == *key;
    g.items = std::move(*list);
    g.duration_ms = *duration;
    g.reason = f.str("reason").value_or("");
    g.time = r.time();
    return g;
}

std::optional<Grant> parse_grant(const Record& r) {
    const Fields f = r.fields();
    auto principal = parse_principal(f);
    const Value* item = f.get("item");
    const Value* hosts = f.get("hosts");
    auto expires = f.integer("expires");
    if (!principal || item == nullptr || hosts == nullptr || !expires) return std::nullopt;
    auto parsed_item = parse_item(*item);
    auto parsed_hosts = parse_hosts(*hosts);
    if (!parsed_item || !parsed_hosts) return std::nullopt;
    Grant g;
    g.id = r.id();
    g.principal = std::move(*principal);
    g.item = std::move(*parsed_item);
    g.hosts = std::move(*parsed_hosts);
    g.expires = *expires;
    bool ok = true;
    g.request = id_member(f, "request", ok);
    g.rule = id_member(f, "rule", ok);
    if (auto issuer = id_member(f, "issuer", ok)) g.issuer = *issuer;
    if (!ok) return std::nullopt;
    return g;
}

std::optional<AuditEntry> parse_audit(const Record& r) {
    const Fields f = r.fields();
    auto principal = parse_principal(f);
    const Value* host = f.get("host");
    const Value* item = f.get("item");
    auto decision = f.str("decision");
    if (!principal || host == nullptr || item == nullptr || !decision) return std::nullopt;
    auto key = key_of(*host);
    auto parsed_item = parse_item(*item);
    auto d = parse_decision(*decision);
    if (!key || !parsed_item || !d) return std::nullopt;
    AuditEntry a;
    a.id = r.id();
    a.host = *key;
    a.principal = std::move(*principal);
    a.item = std::move(*parsed_item);
    a.decision = *d;
    bool ok = true;
    a.rule = id_member(f, "rule", ok);
    a.grant = id_member(f, "grant", ok);
    a.request = id_member(f, "request", ok);
    if (!ok) return std::nullopt;
    a.time = r.time();
    return a;
}

// -- evaluation ---------------------------------------------------------------

bool selects(const LedgerState& state, const HostSelector& hosts, const PublicKey& host) {
    if (hosts.all()) return true;
    if (std::ranges::find(hosts.keys, host) != hosts.keys.end()) return true;
    auto it = state.hosts.find(host);
    if (it == state.hosts.end()) return false;
    return std::ranges::any_of(hosts.labels, [&](const std::string& l) { return contains(it->second.labels, l); });
}

bool rule_covers(const LedgerState& state, const Rule& rule, const Principal& principal, const PublicKey& host,
                 const Item& item) {
    if (rule.service != "*" && rule.service != item.service) return false;
    if (item.ops.empty()) return false;
    if (!contains(rule.ops, "*") &&
        !std::ranges::all_of(item.ops, [&](const std::string& op) { return contains(rule.ops, op); })) {
        return false;
    }
    const auto owner = parse_key_id(principal.owner);
    const Match& m = rule.match;
    if (m.owners && (!owner || std::ranges::find(*m.owners, *owner) == m.owners->end())) return false;
    if (m.groups) {
        auto it = owner ? state.owners.find(*owner) : state.owners.end();
        if (it == state.owners.end()) return false;
        if (!std::ranges::any_of(*m.groups, [&](const std::string& g) { return contains(it->second.groups, g); })) {
            return false;
        }
    }
    if (m.modules && !contains(*m.modules, principal.module)) return false;
    if (m.trust && !contains(*m.trust, principal.trust)) return false;
    if (m.hosts && !selects(state, *m.hosts, host)) return false;
    if (rule.scope) {
        if (item.root.empty()) return false;
        if (rule.scope->roots && !contains(*rule.scope->roots, item.root)) return false;
        if (rule.scope->paths && !std::ranges::any_of(*rule.scope->paths, [&](const std::string& pattern) {
                return glob::match(pattern, item.path);
            })) {
            return false;
        }
    }
    return true;
}

Evaluation evaluate(const LedgerState& state, const Principal& principal, const PublicKey& host, const Item& item) {
    const Rule* best = nullptr;
    for (const auto& rule : state.rules) {
        if (!rule_covers(state, rule, principal, host, item)) continue;
        if (best == nullptr || better(rule, *best)) best = &rule;
    }
    if (best == nullptr) return Evaluation{};
    return Evaluation{best->decision, best->id, best->max_duration_ms};
}

std::optional<std::vector<Item>> parse_manifest(const Value& manifest) {
    if (manifest.is_nil()) return std::vector<Item>{};
    return items_of(manifest);
}

Value manifest_value(const std::vector<Item>& items) {
    return items_value(items);
}

std::vector<Evaluation> preflight(const LedgerState& state, const Principal& principal,
                                  const std::vector<Item>& manifest, const PublicKey& host) {
    std::vector<Evaluation> out;
    for (const auto& item : manifest) out.push_back(evaluate(state, principal, host, item));
    return out;
}

// -- record data --------------------------------------------------------------

namespace data {

Map policy_rule(const RuleSpec& spec) {
    Map m{{"name", Value(spec.name)},
          {"decision", Value(to_string(spec.decision))},
          {"service", Value(spec.service)},
          {"ops", strings_value(spec.ops)},
          {"match", match_value(spec.match)}};
    if (spec.scope) m.emplace_back("scope", scope_value(*spec.scope));
    if (spec.max_duration_ms) m.emplace_back("max_duration_ms", Value(*spec.max_duration_ms));
    if (spec.priority != 0) m.emplace_back("priority", Value(spec.priority));
    return m;
}

Map grant_request(const Principal& principal, const PublicKey& host, const std::vector<Item>& items,
                  std::int64_t duration_ms, std::string_view reason) {
    Map m = *to_value(principal).as_map();
    m.emplace_back("host", Value::bin(host));
    m.emplace_back("items", items_value(items));
    m.emplace_back("duration_ms", Value(duration_ms));
    m.emplace_back("reason", Value(reason));
    return m;
}

Map grant(const Principal& principal, const Item& item, const HostSelector& hosts, std::int64_t expires,
          std::optional<RecordId> request, std::optional<RecordId> rule, std::optional<PublicKey> issuer) {
    Map m = *to_value(principal).as_map();
    m.emplace_back("item", to_value(item));
    m.emplace_back("hosts", to_value(hosts));
    m.emplace_back("expires", Value(expires));
    if (request) m.emplace_back("request", Value::bin(*request));
    if (rule) m.emplace_back("rule", Value::bin(*rule));
    if (issuer) m.emplace_back("issuer", Value::bin(*issuer));
    return m;
}

Map grant_release(const RecordId& grant) {
    return Map{{"grant", Value::bin(grant)}};
}

Map audit(const PublicKey& host, const Principal& principal, const Item& item, Decision decision,
          std::optional<RecordId> rule, std::optional<RecordId> grant, std::optional<RecordId> request) {
    Map m = *to_value(principal).as_map();
    m.emplace_back("host", Value::bin(host));
    m.emplace_back("item", to_value(item));
    m.emplace_back("decision", Value(to_string(decision)));
    if (rule) m.emplace_back("rule", Value::bin(*rule));
    if (grant) m.emplace_back("grant", Value::bin(*grant));
    if (request) m.emplace_back("request", Value::bin(*request));
    return m;
}

}  // namespace data

}  // namespace paglets::mesh
