// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Signed ledger records (planning/cpp-security-and-communication.md,
// section 3.2).
//
// A record is a canonical body (value.hpp) plus a set of Ed25519
// signatures. The record ID is the SHA-256 of the body bytes; signatures
// cover the ID with a domain prefix, so co-signers can add signatures
// without changing the ID, and replicas merge the signature sets of the same
// record.
//
// Body members:
//
// | Key | Type | Meaning |
// |---|---|---|
// | `type` | str | Record type (genesis, admin-set, host-enroll, ...) |
// | `mesh` | bin(32) | Mesh ID (the genesis record ID); absent in genesis |
// | `clock` | int | Lamport clock: larger than every record the author knew |
// | `epoch` | bin(32) | Admin epoch the signers acted in (admin records) |
// | `time` | int | Wall-clock time of creation, Unix milliseconds (informational) |
// | `data` | map | Type-specific content |

#pragma once

#include <paglets/mesh/crypto.hpp>
#include <paglets/mesh/value.hpp>
#include <paglets/sha256.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace paglets::mesh {

using RecordId = Digest;

// Largest Lamport clock a record may carry, so clocks never overflow.
inline constexpr std::int64_t max_clock = std::int64_t{1} << 53;

std::string record_id_hex(const RecordId& id);

class Record {
public:
    struct Draft {
        std::string type;
        std::optional<RecordId> mesh;
        std::int64_t clock = 0;
        std::optional<RecordId> epoch;
        std::int64_t time = 0;
        Map data;
    };
    struct Signed {
        PublicKey key{};
        Signature signature{};
    };

    // Builds an unsigned record from a draft; fails if the draft does not
    // form a valid body (empty type, clock out of range).
    static std::expected<Record, std::string> make(Draft draft);
    // Decodes a record as transferred between hosts and stored on disk;
    // verifies the canonical form and every signature.
    static std::expected<Record, std::string> decode(std::span<const std::uint8_t> bytes);
    Bytes encode() const;

    const RecordId& id() const { return id_; }
    const Bytes& body() const { return body_; }
    const std::string& type() const { return type_; }
    const std::optional<RecordId>& mesh() const { return mesh_; }
    std::int64_t clock() const { return clock_; }
    const std::optional<RecordId>& epoch() const { return epoch_; }
    std::int64_t time() const { return time_; }
    const Map& data() const { return *body_value_.find("data")->as_map(); }
    Fields fields() const { return Fields{data()}; }

    // Signatures, sorted by key, one per key.
    const std::vector<Signed>& signatures() const { return signatures_; }
    bool signed_by(const PublicKey& key) const;
    void sign(const SigningKey& key);
    // Adds the signatures of `other` (the same record); returns how many
    // were new.
    std::size_t merge(const Record& other);
    // Hash of the signature set, to compare replicas cheaply.
    Digest signature_digest() const;

    // Order of records in the ledger: by clock, then by ID.
    bool before(const Record& other) const;

private:
    Record() = default;
    std::expected<void, std::string> parse_body();
    bool add_signature(const Signed& s);

    Bytes body_;
    RecordId id_{};
    Value body_value_;
    std::string type_;
    std::optional<RecordId> mesh_;
    std::int64_t clock_ = 0;
    std::optional<RecordId> epoch_;
    std::int64_t time_ = 0;
    std::vector<Signed> signatures_;
};

// The message an Ed25519 signature of a record covers.
Bytes record_signing_message(const RecordId& id);

}  // namespace paglets::mesh
