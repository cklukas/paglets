// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// A host in a mesh (planning/cpp-policy.md): its ledger copy with gossip,
// its host key, and the glue between the ledger and its runtime:
//
// - the `grants` system paglet (evaluates the policy, records grants,
//   requests and audit entries, materializes capabilities);
// - the directory's policy for services with ambient authority;
// - after every ledger change: revoked and released grants reach the
//   runtime's revocation tree, approvals and denials for local paglets are
//   delivered to them.
//
// Thread-safe. Lock order: node, then runtime.

#pragma once

#include <paglets/mesh/gossip.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/services/system_services.hpp>

#include <expected>
#include <filesystem>
#include <memory>
#include <span>
#include <string>

namespace paglets::node {

class Node {
public:
    // `state_dir` keeps which grants and denials this host delivered
    // (empty: in memory).
    Node(runtime::Runtime& runtime, std::shared_ptr<services::SystemServices> services, mesh::Ledger ledger,
         mesh::SigningKey host_key, mesh::GossipTransport& transport, std::filesystem::path state_dir = {});
    ~Node();
    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;

    // Registers the `grants` system paglet and the directory policy; call
    // before paglets are created.
    std::expected<void, std::string> start();

    const mesh::PublicKey& host() const;
    const mesh::RecordId& mesh() const;

    // Gossip.
    void add_seed(const mesh::PublicKey& peer);
    void receive(const mesh::PublicKey& from, std::span<const std::uint8_t> frame);
    void tick();
    // Adds a record made on this host (for example by an admin CLI) and
    // gossips it.
    std::expected<mesh::AddResult, std::string> submit(const mesh::Record& record);

    // A copy of the derived state, and the ledger digest.
    mesh::LedgerState state() const;
    paglets::Digest ledger_digest() const;
    // A new record in the ledger's current epoch (for admin tools on this
    // host; sign it and submit it).
    std::expected<mesh::Record, std::string> draft(std::string type, mesh::Map data, bool with_epoch = true) const;

    // Applies the ledger to the runtime (called after every change; also
    // after creating a paglet that may have grants approved in advance).
    void sync();

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace paglets::node
