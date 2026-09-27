// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Policy rules, grants and manifests in the ledger (planning/cpp-policy.md).
//
// An item is one piece of access: a service, operations (for `files`:
// rights) and, for `files`, a named root and a path below it. Rules decide
// allow, ask or deny for items of matching principals on matching hosts;
// grants record access that was allowed or approved; audit records keep
// every decision.

#pragma once

#include <paglets/mesh/crypto.hpp>
#include <paglets/mesh/record.hpp>
#include <paglets/mesh/value.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace paglets::mesh {

struct LedgerState;

struct Item {
    std::string service;
    std::vector<std::string> ops;
    std::string root;  // files only
    std::string path;  // files only, relative to the root ("" for the root)
    bool operator==(const Item&) const = default;
};

// Hosts a rule or grant applies to; empty: all hosts.
struct HostSelector {
    std::vector<PublicKey> keys;
    std::vector<std::string> labels;
    bool all() const { return keys.empty() && labels.empty(); }
};

struct Match {
    std::optional<std::vector<PublicKey>> owners;
    std::optional<std::vector<std::string>> groups;
    std::optional<std::vector<std::string>> modules;  // hex SHA-256
    std::optional<std::vector<PublicKey>> signers;    // modules with a valid signature of one of them
    std::optional<std::vector<std::string>> trust;
    std::optional<HostSelector> hosts;
    int members() const;
};

struct Scope {
    std::optional<std::vector<std::string>> roots;
    std::optional<std::vector<std::string>> paths;  // patterns (glob.hpp)
};

enum class Decision { allow, ask, deny };
std::string_view to_string(Decision d);
std::optional<Decision> parse_decision(std::string_view text);

struct Rule {
    RecordId id{};
    std::string name;
    Decision decision = Decision::deny;
    std::string service;  // or "*"
    std::vector<std::string> ops;
    Match match;
    std::optional<Scope> scope;
    std::optional<std::int64_t> max_duration_ms;
    std::int64_t priority = 0;
    std::size_t order = 0;  // position in the ledger
};

// Who asks: the paglet and what the host knows about it.
struct Principal {
    std::string paglet;
    std::string owner;   // key ID (hex) of the owner
    std::string module;  // hex SHA-256
    std::string trust;
    bool operator==(const Principal&) const = default;
};

struct GrantRequest {
    RecordId id{};
    Principal principal;
    PublicKey host{};  // the host that asked (or the owner's key for early approval)
    bool by_owner = false;
    std::vector<Item> items;
    std::int64_t duration_ms = 0;
    std::string reason;
    std::int64_t time = 0;
};

struct Grant {
    RecordId id{};
    Principal principal;
    Item item;
    HostSelector hosts;
    std::int64_t expires = 0;  // Unix milliseconds
    std::optional<RecordId> request;
    std::optional<RecordId> rule;
    bool by_admin = false;
    PublicKey issuer{};  // the host that derived it (not for admin grants)
};

struct AuditEntry {
    RecordId id{};
    PublicKey host{};
    Principal principal;
    Item item;
    Decision decision = Decision::deny;
    std::optional<RecordId> rule;
    std::optional<RecordId> grant;
    std::optional<RecordId> request;
    std::int64_t time = 0;
};

struct Evaluation {
    Decision decision = Decision::deny;
    std::optional<RecordId> rule;  // absent: no rule matched (default deny)
    std::optional<std::int64_t> max_duration_ms;
};

// -- values -------------------------------------------------------------------

Value to_value(const Item& item);
std::optional<Item> parse_item(const Value& value);
Value to_value(const HostSelector& hosts);
std::optional<HostSelector> parse_hosts(const Value& value);
Value to_value(const Principal& p);
std::optional<Principal> parse_principal(const Fields& f);

// Parses the data of the record types (nullopt: malformed).
std::optional<Rule> parse_rule(const Record& r);
std::optional<GrantRequest> parse_grant_request(const Record& r);
std::optional<Grant> parse_grant(const Record& r);
std::optional<AuditEntry> parse_audit(const Record& r);

// -- evaluation ---------------------------------------------------------------

// True if `hosts` selects the enrolled host `host`.
bool selects(const LedgerState& state, const HostSelector& hosts, const PublicKey& host);
// True if the rule applies to the principal on the host and covers the item.
bool rule_covers(const LedgerState& state, const Rule& rule, const Principal& principal, const PublicKey& host,
                 const Item& item);
// The decision for an item (section 2 of planning/cpp-policy.md).
Evaluation evaluate(const LedgerState& state, const Principal& principal, const PublicKey& host, const Item& item);

// Manifests: the items a passport declares (a list of item values).
std::optional<std::vector<Item>> parse_manifest(const Value& manifest);
Value manifest_value(const std::vector<Item>& items);
// Evaluates every item of a manifest for a target host.
std::vector<Evaluation> preflight(const LedgerState& state, const Principal& principal,
                                  const std::vector<Item>& manifest, const PublicKey& host);

// -- record data --------------------------------------------------------------

namespace data {
struct RuleSpec {
    std::string name;
    Decision decision = Decision::deny;
    std::string service;
    std::vector<std::string> ops;
    Match match;
    std::optional<Scope> scope;
    std::optional<std::int64_t> max_duration_ms;
    std::int64_t priority = 0;
};
Map policy_rule(const RuleSpec& spec);
Map grant_request(const Principal& principal, const PublicKey& host, const std::vector<Item>& items,
                  std::int64_t duration_ms, std::string_view reason);
// An admin approval (with the request) or a grant derived from a rule.
Map grant(const Principal& principal, const Item& item, const HostSelector& hosts, std::int64_t expires,
          std::optional<RecordId> request, std::optional<RecordId> rule, std::optional<PublicKey> issuer);
Map grant_release(const RecordId& grant);
Map audit(const PublicKey& host, const Principal& principal, const Item& item, Decision decision,
          std::optional<RecordId> rule, std::optional<RecordId> grant, std::optional<RecordId> request);
}  // namespace data

}  // namespace paglets::mesh
