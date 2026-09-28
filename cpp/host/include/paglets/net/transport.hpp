// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Channels over HTTPS (planning/cpp-networking.md, section 3).
//
// Every host runs an HTTPS server. A host sends frames to a peer through a
// channel it opened to the peer's server:
//
//   POST /paglets/v1/open         Noise message 1   -> {s: session, m: message 2}
//   POST /paglets/v1/finish/<s>   Noise message 3   -> 204
//   POST /paglets/v1/frames/<s>   Noise messages    -> Noise messages
//
// Frame bodies are sequences of [u32 big-endian length][Noise message]. The
// response to a frames request carries the frames the server answers (CLI
// sessions); frames between hosts are one-way, each host sending through
// its own channel. Every peer has an ordered queue and a sender thread;
// frames that cannot be delivered are dropped (protocols above retry).
//
// TLS keeps proxies and firewalls working; it does not authenticate hosts
// (the channels do), so a host uses a self-signed certificate unless one is
// configured, and certificates of peers are only checked against a CA file
// when one is given.

#pragma once

#include <paglets/mesh/gossip.hpp>
#include <paglets/net/channel.hpp>

#include <chrono>
#include <cstddef>
#include <expected>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace paglets::net {

struct TransportConfig {
    bool listen = true;  // CLI sessions only connect
    std::string listen_host = "127.0.0.1";
    int listen_port = 0;  // 0: any free port
    // PEM files of the server's certificate and key; empty: a self-signed
    // certificate made at start.
    std::filesystem::path tls_cert;
    std::filesystem::path tls_key;
    // Checks peers' certificates against this CA file; empty: not checked.
    std::filesystem::path tls_ca;
    // The address this host announces to the peers it opens channels to
    // (default: url()); peers use it to reach the host.
    std::string advertise_url;
    std::chrono::milliseconds timeout{15000};
    std::size_t max_frame = channel_max_frame;
    std::size_t max_batch_bytes = 8u * 1024 * 1024;  // frames per request
    std::chrono::milliseconds session_idle{600000};  // unused server sessions end
    std::function<void(const std::string&)> log;     // default: none
    std::int64_t protocol = mesh_protocol;           // announced in handshakes (other values: tests)
};

// A frame from a peer; returns the frames to answer with (CLI sessions).
using FrameHandler = std::function<std::vector<Bytes>(const Identity& from, Bytes frame)>;

// A self-signed certificate and key (PEM) for a host's HTTPS server.
struct Certificate {
    std::string cert_pem;
    std::string key_pem;
};
std::expected<Certificate, std::string> self_signed_certificate(std::string_view common_name);

struct TransportStats {
    std::uint64_t frames_sent = 0;
    std::uint64_t frames_received = 0;
    std::uint64_t frames_dropped = 0;  // not deliverable (no address, refused, transport errors)
    std::uint64_t channels_opened = 0;
    std::uint64_t channels_accepted = 0;
    std::uint64_t handshakes_refused = 0;
};

class Transport final : public mesh::GossipTransport {
public:
    // `accept` decides which identities may open channels to this host;
    // `handler` receives their frames (from its server threads).
    Transport(const mesh::SigningKey& identity, mesh::RecordId mesh, TransportConfig config, AcceptPeer accept,
              FrameHandler handler);
    ~Transport() override;
    Transport(const Transport&) = delete;
    Transport& operator=(const Transport&) = delete;

    // Starts the HTTPS server (if configured to listen).
    std::expected<void, std::string> start();
    void stop();
    int port() const;
    std::string url() const;  // https://<listen host>:<port>

    // Where peers are reached: https://host:port (or http:// for tests).
    void set_address(const mesh::PublicKey& peer, std::string url) override;
    std::optional<std::string> address(const mesh::PublicKey& peer) const override;
    // The address this host announces (advertise_url, or url()).
    std::string own_address() const override;
    // Opens a channel to a host whose key is not known yet (a bootstrap
    // contact, planning/cpp-mesh.md): the server must pass `accept` as a
    // host. Returns its key; its address is then known.
    std::expected<mesh::PublicKey, std::string> probe(const std::string& url);

    // Queues a frame for a peer (a host); returns at once.
    void send(const mesh::PublicKey& to, Bytes frame) override;
    // Waits until every queued frame was sent or dropped.
    bool flush(std::chrono::milliseconds timeout = std::chrono::milliseconds(30000));

    TransportStats stats() const;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

// A client-side channel for request and answer (CLI sessions, tools): opens
// a channel to `url` as `identity` in `role`; `accept` checks the server's
// identity.
class ClientSession {
public:
    static std::expected<ClientSession, std::string> open(const std::string& url, const mesh::SigningKey& identity,
                                                          PeerRole role, const mesh::RecordId& mesh, AcceptPeer accept,
                                                          TransportConfig config = {});
    ClientSession(ClientSession&&) noexcept;
    ClientSession& operator=(ClientSession&&) noexcept;
    ~ClientSession();

    // Sends frames; returns the frames the server answered with.
    std::expected<std::vector<Bytes>, std::string> exchange(const std::vector<Bytes>& frames);
    const Identity& server() const;

    struct Impl;

private:
    explicit ClientSession(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace paglets::net
