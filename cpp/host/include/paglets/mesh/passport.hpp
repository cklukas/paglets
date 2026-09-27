// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Paglet passports (planning/cpp-security-and-communication.md, section 3.5).
//
// A root paglet's passport is signed by its owner: mesh, owner key, module
// hash, paglet ID, capability manifest, issue and expiry time. A child or
// clone extends its creator's passport with a link signed by the host that
// created it: the parent and child IDs, the child's module hash, the kind
// (`child` or `clone`), and the hash of the previous element, so links
// cannot be moved to another chain. Every element is a canonical value
// (value.hpp).
//
// Verification needs the ledger state: the owner must be enrolled, every
// linking host must be enrolled, and no key may be revoked.

#pragma once

#include <paglets/mesh/ledger.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace paglets::mesh {

struct PassportRoot {
    RecordId mesh{};
    PublicKey owner{};
    Digest module{};
    std::string paglet;
    Value manifest;            // capability manifest (WP9); nil if none is declared
    std::int64_t issued = 0;   // Unix milliseconds
    std::int64_t expires = 0;  // Unix milliseconds
};

struct PassportLink {
    std::string kind;  // "child" or "clone"
    std::string parent;
    std::string paglet;
    Digest module{};
    PublicKey host{};
    std::int64_t issued = 0;
};

class Passport {
public:
    // A root passport for a paglet the owner is about to create.
    static std::expected<Passport, std::string> issue(const SigningKey& owner, const RecordId& mesh,
                                                      const Digest& module, std::string paglet, Value manifest,
                                                      std::int64_t issued, std::int64_t expires);
    static std::expected<Passport, std::string> decode(std::span<const std::uint8_t> bytes);
    Bytes encode() const;

    // The passport of a child or clone created on the host with key `host`.
    std::expected<Passport, std::string> extend(const SigningKey& host, std::string kind, std::string paglet,
                                                const Digest& module, std::int64_t issued) const;

    const PassportRoot& root() const { return root_; }
    const std::vector<PassportLink>& links() const { return links_; }
    // The paglet and module the passport is for (the last link's, or the root's).
    const std::string& paglet() const;
    const Digest& module() const;

private:
    struct Element {
        Bytes body;
        Signature signature{};
    };
    Passport() = default;

    PassportRoot root_;
    std::vector<PassportLink> links_;
    Element root_element_;
    std::vector<Element> link_elements_;
};

// Checks a passport for a paglet with `paglet` ID and `module` hash against
// the ledger state at time `now` (Unix milliseconds).
std::expected<void, std::string> verify_passport(const Passport& passport, const LedgerState& state,
                                                 std::string_view paglet, const Digest& module, std::int64_t now);

}  // namespace paglets::mesh
