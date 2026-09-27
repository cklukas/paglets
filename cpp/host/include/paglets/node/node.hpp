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
//   delivered to them;
// - module trust (planning/cpp-modules.md): the runtime's admission check,
//   and paglets whose module loses trust end;
// - code mobility: modules this host lacks are fetched from its module
//   sources and from other hosts (verified against their hash), and root
//   paglets can be launched on other hosts from their passports.
//
// Frames between nodes share the gossip transport; node frames are
// canonical values like gossip frames (planning/cpp-modules.md, section 5).
//
// Thread-safe. Lock order: node, then runtime.

#pragma once

#include <paglets/mesh/gossip.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/passport.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/services/system_services.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
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

    // Gossip and node frames.
    void add_seed(const mesh::PublicKey& peer);
    void receive(const mesh::PublicKey& from, std::span<const std::uint8_t> frame);
    // Anti-entropy, and module requests that timed out move on.
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

    // Creates a root paglet from its passport (planning/cpp-ledger.md,
    // section 6): the passport must verify against the ledger for this
    // module; the paglet gets the passport's ID and owner and is roaming.
    // Grants approved in advance are delivered right away. The passport is
    // kept with the paglet (passport()).
    std::expected<runtime::PagletId, std::string> create(std::string_view module, const mesh::Passport& passport,
                                                         runtime::Bytes args = {});
    std::optional<mesh::Passport> passport(const runtime::PagletId& paglet) const;

    // -- code mobility (planning/cpp-modules.md, section 5) --

    // Sources asked for a missing module before other hosts (for example a
    // directory of modules); in the order added.
    void add_module_source(std::shared_ptr<runtime::ModuleSource> source);
    // How long a host may take to answer a module request (default 10 s);
    // tick() then asks the next host.
    void set_fetch_timeout(std::chrono::milliseconds timeout);
    // Gets a module this host lacks: from its sources, then from `from`, then
    // from the other hosts of the mesh, each verified against the hash.
    // Completes during receive() and tick(); fetching() is true meanwhile.
    void fetch_module(const Digest& module, std::optional<mesh::PublicKey> from = std::nullopt);
    bool fetching(const Digest& module) const;

    struct LaunchStatus {
        enum class State { pending, running, failed };
        State state = State::pending;
        runtime::PagletId paglet;  // running
        std::string error;         // failed
    };
    // Starts a root paglet from its passport on `host` (an enrolled host, or
    // this one). The target verifies the passport and the module's trust,
    // fetches the module if it lacks it (from this host first) and creates
    // the paglet; the answer arrives through receive().
    std::expected<std::uint64_t, std::string> launch(const mesh::PublicKey& host, const mesh::Passport& passport,
                                                     runtime::Bytes args = {});
    std::optional<LaunchStatus> launch_status(std::uint64_t launch) const;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace paglets::node
