// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Replication of the ledger between hosts by gossip (planning/cpp-ledger.md).
//
// - Eager push: a record or signature that is new to a replica is sent on to
//   every peer except the one it came from. A replica that already has it
//   answers nothing, so pushes stop after one round.
// - Anti-entropy: tick() sends the ledger digest to the next peer. A peer
//   with a different digest answers with its inventory (record IDs and
//   signature digests); both sides then send each other what the other
//   lacks. This repairs everything pushes missed, for example across a
//   partition.
//
// Frames are canonical values (value.hpp); records inside are verified by
// Ledger::add, so a peer cannot inject anything that is not validly signed.
// Authenticating and encrypting the channel is the transport's job (WP12).

#pragma once

#include <paglets/mesh/ledger.hpp>

#include <cstddef>
#include <functional>
#include <set>
#include <span>
#include <string>
#include <vector>

namespace paglets::mesh {

// Sends frames to other hosts, addressed by host key.
class GossipTransport {
public:
    virtual ~GossipTransport() = default;
    virtual void send(const PublicKey& to, Bytes frame) = 0;
};

struct GossipStats {
    std::size_t frames_sent = 0;
    std::size_t frames_received = 0;
    std::size_t records_sent = 0;
    std::size_t records_added = 0;  // new records or new signatures
    std::size_t records_rejected = 0;
};

class Replica {
public:
    // Largest frame a replica accepts, and records per push frame.
    static constexpr std::size_t max_frame = 16u * 1024 * 1024;
    static constexpr std::size_t max_records_per_frame = 512;

    Replica(Ledger& ledger, const PublicKey& self, GossipTransport& transport);

    // Peers are the enrolled hosts of the ledger plus these seeds (hosts
    // this one knows before it is enrolled, or that are not enrolled yet).
    void add_seed(const PublicKey& peer);
    std::vector<PublicKey> peers() const;

    // Adds a local record (created or signed on this host) and pushes it.
    std::expected<AddResult, std::string> submit(const Record& record);
    // Handles a frame from `from`.
    void receive(const PublicKey& from, std::span<const std::uint8_t> frame);
    // One anti-entropy round with the next peer.
    void tick();

    const Ledger& ledger() const { return ledger_; }
    const GossipStats& stats() const { return stats_; }
    // Called after records or signatures were added (from any peer).
    void on_change(std::function<void()> callback) { on_change_ = std::move(callback); }

private:
    void send(const PublicKey& to, const Value& frame);
    void push(const PublicKey& to, const std::vector<const Record*>& records);
    void push_to_all(const std::vector<const Record*>& records, const PublicKey* except);
    void handle_digest(const PublicKey& from, const Fields& f);
    void handle_inventory(const PublicKey& from, const Fields& f);
    void handle_want(const PublicKey& from, const Fields& f);
    void handle_push(const PublicKey& from, const Fields& f);

    Ledger& ledger_;
    PublicKey self_;
    GossipTransport& transport_;
    std::set<PublicKey> seeds_;
    std::size_t next_peer_ = 0;
    GossipStats stats_;
    std::function<void()> on_change_;
};

}  // namespace paglets::mesh
