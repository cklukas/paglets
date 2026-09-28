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
    // The host key (for the channels of the network transport).
    const mesh::SigningKey& host_key() const;
    // The hosts this node talks to: enrolled hosts and seeds.
    std::vector<mesh::PublicKey> peers() const;

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

    // -- movement (planning/cpp-networking.md, section 6) --

    // Asks a paglet here to move; the destination is a transfer ticket:
    // `<host name | key ID | label:L | any>[?retries=N&arrival=active|inactive]`.
    std::expected<void, std::string> dispatch(const runtime::PagletId& paglet, std::string destination);
    // How long the other side of a move may take for each step (default 30 s).
    void set_move_timeout(std::chrono::milliseconds timeout);
    // Memory pages kept for later moves (default 256 MB).
    void set_page_cache_bytes(std::size_t bytes);
    struct MoveStats {
        std::uint64_t moves_out = 0;       // paglets and clones that left
        std::uint64_t moves_in = 0;        // that arrived
        std::uint64_t moves_failed = 0;    // that stayed
        std::uint64_t pages_sent = 0;      // memory pages sent
        std::uint64_t pages_received = 0;  // received
        std::uint64_t pages_reused = 0;    // found here and not transferred
        std::uint64_t bytes_sent = 0;      // compressed page bytes sent
    };
    MoveStats move_stats() const;

    // -- location and pins (planning/cpp-location.md) --

    struct LocationTiming {
        std::chrono::milliseconds heartbeat{2000};                    // to every enrolled host
        std::chrono::milliseconds host_timeout{10000};                // not heard from for this long: down
        std::chrono::milliseconds refresh{std::chrono::minutes(5)};   // holders re-send their records
        std::chrono::milliseconds record_ttl{std::chrono::hours(1)};  // records not refreshed end
        std::chrono::milliseconds lookup_timeout{3000};               // per step of a lookup
    };
    void set_location_timing(LocationTiming timing);
    // Enrolled hosts this host believes are up (itself included), sorted.
    std::vector<mesh::PublicKey> live_hosts() const;
    // The hosts that keep a paglet's location record, in this host's view.
    std::vector<mesh::PublicKey> responsible(const runtime::PagletId& paglet) const;
    struct LocationRecord {
        mesh::PublicKey host{};
        std::uint64_t moves = 0;
        std::int64_t moved_ms = 0;
    };
    // The record this host keeps for a paglet (as a responsible host).
    std::optional<LocationRecord> location_record(const runtime::PagletId& paglet) const;

    struct Located {
        enum class State { pending, found, failed };
        State state = State::pending;
        mesh::PublicKey host{};
        std::uint64_t moves = 0;
        std::int64_t moved_ms = 0;
        std::string owner;
        std::string pin;  // with a pin: its ID and end
        std::int64_t pinned_until = 0;
        std::string error;
    };
    // Finds a paglet anywhere in the mesh (and with `pin_for`, pins it where
    // it is); the result arrives through receive() and tick().
    std::uint64_t locate(const runtime::PagletId& paglet,
                         std::chrono::milliseconds pin_for = std::chrono::milliseconds(0), std::string reason = {});
    std::optional<Located> located(std::uint64_t lookup) const;
    struct PinInfo {
        std::string pin;
        runtime::PagletId paglet;
        std::int64_t until = 0;
        std::string holder;
        std::string reason;
    };
    // Pins of paglets on this host.
    std::vector<PinInfo> pins() const;
    // Ends pins of a paglet on this host (an admin's force-release): the
    // one named, or all. Returns how many ended.
    std::size_t release_pins(const runtime::PagletId& paglet, const std::string& pin = {});

    // -- CLI sessions (planning/cpp-networking.md, section 8) --

    // Answers a request of an admin's or owner's session (`role`: "admin" or
    // "owner"; the transport proved the key). Requests and answers are
    // canonical maps: status, push (ledger records), launch (a paglet from
    // an owner's passport, with its module), call, dispatch.
    runtime::Bytes answer_session(const mesh::PublicKey& peer, std::string_view role, runtime::Bytes request);

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace paglets::node
