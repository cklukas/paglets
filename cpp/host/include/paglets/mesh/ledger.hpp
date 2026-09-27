// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The mesh security ledger (planning/cpp-security-and-communication.md,
// section 3.2; design details in planning/cpp-ledger.md).
//
// A Ledger is one host's copy: a grow-only set of signed records (record.hpp)
// anchored at the genesis record, whose ID is the mesh ID. Adding a record
// only checks its form and signatures; what a record means is decided when
// the state is derived, from the whole set, deterministically. Hosts holding
// the same records therefore derive the same state, whatever order the
// records arrived in.
//
// Record types (their data: planning/cpp-ledger.md, planning/cpp-policy.md),
// and who signs them:
//
// - genesis: every initial admin;
// - admin-set: the admin-set quorum of its epoch;
// - host-enroll, host-remove, owner-enroll, owner-remove, request-deny,
//   revoke, policy-rule: the normal admin quorum;
// - host-enroll-request, owner-enroll-request: the key they name;
// - grant-request: the requesting host, or the owner (early approval);
// - grant: the normal admin quorum, or a host under an allow rule;
// - grant-release, audit: an enrolled host.
//
// Records of other types are kept and replicated but have no effect (types
// of newer hosts).

#pragma once

#include <paglets/mesh/policy.hpp>
#include <paglets/mesh/record.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace paglets::mesh {

struct Quorum {
    std::int64_t admin_set = 1;  // signatures needed to change the admin set
    std::int64_t normal = 1;     // signatures needed for every other admin record
    bool operator==(const Quorum&) const = default;
};

struct HostEntry {
    PublicKey key{};
    std::string name;
    std::vector<std::string> labels;
    RecordId record{};  // the enrollment record
};

struct OwnerEntry {
    PublicKey key{};
    std::string name;
    std::vector<std::string> groups;
    RecordId record{};
};

enum class RequestKind { host, owner };

struct PendingRequest {
    RecordId id{};
    RequestKind kind = RequestKind::host;
    PublicKey key{};
    std::string name;
    std::vector<std::string> labels;
    std::int64_t time = 0;
};

// A record that has no effect, and why (for administrators and audits).
struct IgnoredRecord {
    RecordId id{};
    std::string type;
    std::string reason;
};

// The state all hosts derive from the same records.
struct LedgerState {
    RecordId mesh{};
    std::string mesh_name;
    RecordId epoch{};  // current admin epoch: an admin-set record, genesis, or a merge
    std::set<PublicKey> admins;
    Quorum quorum;
    std::map<PublicKey, HostEntry> hosts;
    std::map<PublicKey, OwnerEntry> owners;
    std::vector<PendingRequest> pending;
    std::set<PublicKey> revoked_keys;
    std::set<RecordId> revoked_records;
    std::vector<IgnoredRecord> ignored;

    // Policy (planning/cpp-policy.md).
    std::vector<Rule> rules;                   // in ledger order
    std::map<RecordId, Grant> grants;          // valid; expiry is checked when used
    std::set<RecordId> ended_grants;           // revoked or released
    std::vector<GrantRequest> grant_requests;  // pending
    std::vector<AuditEntry> audit;             // in ledger order

    std::int64_t clock = 0;  // largest clock of any record
    std::size_t records = 0;

    bool is_admin(const PublicKey& key) const { return admins.contains(key); }
    bool is_host(const PublicKey& key) const { return hosts.contains(key); }
    bool is_owner(const PublicKey& key) const { return owners.contains(key); }

    // Canonical form and its hash, to compare the state of two hosts.
    Value to_value() const;
    Digest digest() const;
};

enum class AddResult { added, merged, known };

// Creates the genesis record, signed by every initial admin.
std::expected<Record, std::string> make_genesis(std::string_view name, const std::vector<const SigningKey*>& admins,
                                                Quorum quorum, std::int64_t time_ms);

// Milliseconds since the Unix epoch.
std::int64_t unix_ms();

class Ledger {
public:
    // An in-memory ledger for `genesis`, or one kept in `directory` (one
    // file per record, written before add() returns).
    static std::expected<Ledger, std::string> create(const Record& genesis,
                                                     const std::filesystem::path& directory = {});
    // Opens an existing ledger directory; `mesh` (the trust anchor a host
    // is installed with) must match its genesis.
    static std::expected<Ledger, std::string> open(const std::filesystem::path& directory,
                                                   std::optional<RecordId> mesh = std::nullopt);

    Ledger(Ledger&&) noexcept;
    Ledger& operator=(Ledger&&) noexcept;
    ~Ledger();

    const RecordId& mesh() const;

    // Adds a record or new signatures of a known one. Fails for records of
    // another mesh and a second genesis.
    std::expected<AddResult, std::string> add(const Record& record);
    // Decodes and adds a transferred record.
    std::expected<AddResult, std::string> add_encoded(std::span<const std::uint8_t> bytes);

    const Record* find(const RecordId& id) const;
    std::size_t size() const;
    // Every record in ledger order (clock, then ID).
    std::vector<const Record*> records() const;
    // Hash over all record IDs and signature sets: equal digests mean equal
    // ledgers.
    Digest digest() const;

    // The derived state (cached until the next change).
    const LedgerState& state() const;

    // A new record in the current admin epoch with the next clock value, to
    // be signed and added. Request records need no epoch (`with_epoch`
    // false).
    std::expected<Record, std::string> draft(std::string type, Map data, bool with_epoch = true) const;

private:
    struct Impl;
    explicit Ledger(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

// Data of the record types above.
namespace data {
// `remove` gets keep lists of every record the removed admin signed in
// `ledger`: those stay valid, anything the admin signs later does not.
Map admin_set(const Ledger& ledger, const std::vector<PublicKey>& add, const std::vector<PublicKey>& remove,
              std::optional<Quorum> quorum = std::nullopt);
Map host_enroll_request(const PublicKey& key, std::string_view name, const std::vector<std::string>& labels);
Map host_enroll(const PublicKey& key, std::string_view name, const std::vector<std::string>& labels,
                std::optional<RecordId> request = std::nullopt);
Map owner_enroll_request(const PublicKey& key, std::string_view name);
Map owner_enroll(const PublicKey& key, std::string_view name, const std::vector<std::string>& groups,
                 std::optional<RecordId> request = std::nullopt);
Map key_removal(const PublicKey& key);
Map request_deny(const RecordId& request, std::string_view reason);
Map revoke_key(const PublicKey& key, std::string_view reason);
Map revoke_record(const RecordId& record, std::string_view reason);
}  // namespace data

// Derives the state from a set of records (the ledger's own derivation;
// exposed for tests and tools).
LedgerState derive_state(const RecordId& mesh, const std::vector<const Record*>& records);

}  // namespace paglets::mesh
