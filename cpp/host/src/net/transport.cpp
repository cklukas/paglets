// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>

#include <paglets/mesh/value.hpp>
#include <paglets/net/transport.hpp>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/x509.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <map>
#include <mutex>
#include <thread>

namespace paglets::net {

namespace {

using Clock = std::chrono::steady_clock;

constexpr const char* path_open = "/paglets/v1/open";
constexpr const char* path_finish = "/paglets/v1/finish/";
constexpr const char* path_frames = "/paglets/v1/frames/";
constexpr const char* content_type = "application/octet-stream";

// -- bodies: [u32 big-endian length][message]... -----------------------------------

Bytes pack(const std::vector<Bytes>& messages) {
    Bytes out;
    for (const auto& m : messages) {
        const auto n = static_cast<std::uint32_t>(m.size());
        out.push_back(static_cast<std::uint8_t>(n >> 24));
        out.push_back(static_cast<std::uint8_t>(n >> 16));
        out.push_back(static_cast<std::uint8_t>(n >> 8));
        out.push_back(static_cast<std::uint8_t>(n));
        out.insert(out.end(), m.begin(), m.end());
    }
    return out;
}

std::optional<std::vector<Bytes>> unpack(std::string_view body) {
    std::vector<Bytes> out;
    std::size_t at = 0;
    while (at < body.size()) {
        if (body.size() - at < 4) return std::nullopt;
        const auto* p = reinterpret_cast<const std::uint8_t*>(body.data() + at);
        const std::size_t n = (std::size_t{p[0]} << 24) | (std::size_t{p[1]} << 16) | (std::size_t{p[2]} << 8) | p[3];
        at += 4;
        if (n > noise_max_message || body.size() - at < n) return std::nullopt;
        out.emplace_back(p + 4, p + 4 + n);
        at += n;
    }
    return out;
}

std::string as_string(std::span<const std::uint8_t> b) {
    return std::string(reinterpret_cast<const char*>(b.data()), b.size());
}

std::span<const std::uint8_t> as_bytes(std::string_view s) {
    return {reinterpret_cast<const std::uint8_t*>(s.data()), s.size()};
}

std::string random_session_id() {
    std::array<std::uint8_t, 16> b{};
    mesh::random_bytes(b);
    return to_hex(std::span<const std::uint8_t>(b));
}

bool valid_session_id(std::string_view s) {
    return s.size() == 32 && s.find_first_not_of("0123456789abcdef") == std::string_view::npos;
}

// -- client side ---------------------------------------------------------------------

struct Failure {
    bool retry_safe = false;  // the server did not act on the request
    std::string text;
};

struct Connection {
    std::string url;
    std::unique_ptr<httplib::Client> http;
    std::string session;
    std::optional<Channel> channel;
    Identity server;
};

std::unique_ptr<httplib::Client> make_client(const std::string& url, const TransportConfig& config) {
    auto http = std::make_unique<httplib::Client>(url);
    const auto secs = std::chrono::duration_cast<std::chrono::seconds>(config.timeout).count();
    http->set_connection_timeout(static_cast<time_t>(secs));
    http->set_read_timeout(static_cast<time_t>(secs));
    http->set_write_timeout(static_cast<time_t>(secs));
    http->set_keep_alive(true);
    if (!config.tls_ca.empty()) {
        http->set_ca_cert_path(config.tls_ca.string());
        http->enable_server_certificate_verification(true);
    } else {
        http->enable_server_certificate_verification(false);
    }
    return http;
}

std::string describe(const httplib::Result& r) {
    if (!r) return "HTTP error: " + httplib::to_string(r.error());
    return "HTTP " + std::to_string(r->status) + (r->body.empty() ? "" : ": " + r->body.substr(0, 200));
}

std::expected<Connection, std::string> connect(const std::string& url, const mesh::SigningKey& identity, PeerRole role,
                                               const mesh::RecordId& mesh_id, const AcceptPeer& accept,
                                               const TransportConfig& config, const std::string& my_url = {}) {
    Connection c;
    c.url = url;
    c.http = make_client(url, config);
    ChannelHandshake hs(Handshake::Role::initiator, identity, role, mesh_id, accept, config.max_frame, my_url);
    auto m1 = hs.next_message();
    if (!m1) return std::unexpected(m1.error());
    auto r1 = c.http->Post(path_open, as_string(*m1), content_type);
    if (!r1 || r1->status != 200) return std::unexpected("opening a channel to " + url + ": " + describe(r1));
    auto v = mesh::decode(as_bytes(r1->body));
    if (!v || v->as_map() == nullptr) return std::unexpected(std::string("malformed channel answer"));
    const mesh::Fields f{*v->as_map()};
    auto session = f.str("s");
    auto m2 = f.bin("m");
    if (!session || !valid_session_id(*session) || !m2) return std::unexpected(std::string("malformed channel answer"));
    if (auto ok = hs.receive(*m2); !ok) return std::unexpected("channel to " + url + ": " + ok.error());
    auto m3 = hs.next_message();
    if (!m3) return std::unexpected(m3.error());
    auto r3 = c.http->Post(std::string(path_finish) + *session, as_string(*m3), content_type);
    if (!r3 || r3->status != 204) return std::unexpected("channel to " + url + " refused: " + describe(r3));
    auto channel = hs.finish();
    if (!channel) return std::unexpected(channel.error());
    c.session = *session;
    c.server = channel->peer();
    c.channel.emplace(std::move(*channel));
    return c;
}

std::expected<std::vector<Bytes>, Failure> exchange(Connection& c, const std::vector<Bytes>& frames) {
    std::vector<Bytes> messages;
    for (const auto& f : frames) {
        for (auto& m : c.channel->seal(f)) messages.push_back(std::move(m));
    }
    auto r = c.http->Post(std::string(path_frames) + c.session, as_string(pack(messages)), content_type);
    // A connection that could not be made carried nothing.
    if (!r) return std::unexpected(Failure{r.error() == httplib::Error::Connection, describe(r)});
    // An unknown session (for example after the server restarted): nothing
    // was processed.
    if (r->status == 404) return std::unexpected(Failure{true, "channel session unknown"});
    if (r->status != 200) return std::unexpected(Failure{false, describe(r)});
    auto answers = unpack(r->body);
    if (!answers) return std::unexpected(Failure{false, "malformed answer"});
    std::vector<Bytes> out;
    for (const auto& m : *answers) {
        auto frame = c.channel->open(m);
        if (!frame) return std::unexpected(Failure{false, frame.error()});
        if (*frame) out.push_back(std::move(**frame));
    }
    return out;
}

}  // namespace

// -- self-signed certificates -----------------------------------------------------------

std::expected<Certificate, std::string> self_signed_certificate(std::string_view common_name) {
    EVP_PKEY* key = EVP_EC_gen("P-256");
    if (key == nullptr) return std::unexpected(std::string("cannot generate a TLS key"));
    X509* cert = X509_new();
    auto cleanup = [&] {
        X509_free(cert);
        EVP_PKEY_free(key);
    };
    std::array<unsigned char, 16> serial{};
    RAND_bytes(serial.data(), static_cast<int>(serial.size()));
    serial[0] &= 0x7f;  // positive
    BIGNUM* bn = BN_bin2bn(serial.data(), static_cast<int>(serial.size()), nullptr);
    BN_to_ASN1_INTEGER(bn, X509_get_serialNumber(cert));
    BN_free(bn);
    X509_set_version(cert, 2);
    X509_gmtime_adj(X509_getm_notBefore(cert), -3600);
    X509_gmtime_adj(X509_getm_notAfter(cert), 10L * 365 * 24 * 3600);
    X509_set_pubkey(cert, key);
    X509_NAME* name = X509_get_subject_name(cert);
    const std::string cn(common_name);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_UTF8, reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1,
                               0);
    X509_set_issuer_name(cert, name);
    if (X509_sign(cert, key, EVP_sha256()) == 0) {
        cleanup();
        return std::unexpected(std::string("cannot sign the TLS certificate"));
    }
    auto pem = [](auto write) {
        BIO* bio = BIO_new(BIO_s_mem());
        write(bio);
        char* data = nullptr;
        const long n = BIO_get_mem_data(bio, &data);
        std::string out(data, static_cast<std::size_t>(n));
        BIO_free(bio);
        return out;
    };
    Certificate c;
    c.cert_pem = pem([&](BIO* b) { PEM_write_bio_X509(b, cert); });
    c.key_pem = pem([&](BIO* b) { PEM_write_bio_PrivateKey(b, key, nullptr, nullptr, 0, nullptr, nullptr); });
    cleanup();
    if (c.cert_pem.empty() || c.key_pem.empty()) return std::unexpected(std::string("cannot encode the certificate"));
    return c;
}

// -- Transport ---------------------------------------------------------------------------

struct Transport::Impl {
    Impl(const mesh::SigningKey& id, mesh::RecordId m, TransportConfig c, AcceptPeer a, FrameHandler h)
        : identity(id), mesh_id(m), config(std::move(c)), accept(std::move(a)), handler(std::move(h)) {}

    const mesh::SigningKey& identity;
    mesh::RecordId mesh_id;
    TransportConfig config;
    AcceptPeer accept;
    FrameHandler handler;

    std::unique_ptr<httplib::SSLServer> server;
    std::thread server_thread;
    int port = 0;
    Certificate certificate;

    struct ServerSession {
        std::mutex mu;
        std::optional<ChannelHandshake> handshake;
        std::optional<Channel> channel;
        Clock::time_point used = Clock::now();
    };
    std::mutex sessions_mu;
    std::map<std::string, std::shared_ptr<ServerSession>> sessions;

    struct Peer {
        std::deque<Bytes> queue;
        std::optional<Connection> connection;
        bool busy = false;
        std::thread thread;
        std::condition_variable cv;
    };
    mutable std::mutex mu;
    std::condition_variable idle_cv;
    bool stopping = false;
    std::map<mesh::PublicKey, std::string> addresses;
    std::map<mesh::PublicKey, std::unique_ptr<Peer>> peers;

    std::atomic<std::uint64_t> sent{0}, received{0}, dropped{0}, opened{0}, accepted{0}, refused{0};

    void log(const std::string& text) const {
        if (config.log) config.log(text);
    }

    std::string own_url() const {
        if (!config.advertise_url.empty()) return config.advertise_url;
        if (port <= 0) return {};
        return "https://" + config.listen_host + ":" + std::to_string(port);
    }

    // -- server ---------------------------------------------------------------

    void prune() {
        const auto now = Clock::now();
        std::lock_guard lock(sessions_mu);
        for (auto it = sessions.begin(); it != sessions.end();) {
            const auto limit = it->second->channel ? config.session_idle : config.timeout;
            std::unique_lock session_lock(it->second->mu, std::try_to_lock);
            if (session_lock.owns_lock() && now - it->second->used > limit) {
                session_lock.unlock();
                it = sessions.erase(it);
            } else {
                ++it;
            }
        }
    }

    std::shared_ptr<ServerSession> find(const std::string& id) {
        std::lock_guard lock(sessions_mu);
        auto it = sessions.find(id);
        return it == sessions.end() ? nullptr : it->second;
    }

    void drop(const std::string& id) {
        std::lock_guard lock(sessions_mu);
        sessions.erase(id);
    }

    void on_open(const httplib::Request& req, httplib::Response& res) {
        prune();
        auto session = std::make_shared<ServerSession>();
        AcceptPeer check = [this](const Identity& peer) -> std::expected<void, std::string> {
            auto ok = accept ? accept(peer) : std::expected<void, std::string>();
            if (!ok) {
                ++refused;
                log("channel refused for " + std::string(to_string(peer.role)) + " " +
                    mesh::key_id(peer.key).substr(0, 16) + ": " + ok.error());
            }
            return ok;
        };
        session->handshake.emplace(Handshake::Role::responder, identity, PeerRole::host, mesh_id, check,
                                   config.max_frame);
        auto ok = session->handshake->receive(as_bytes(req.body));
        auto m2 =
            ok ? session->handshake->next_message() : std::expected<Bytes, std::string>(std::unexpected(ok.error()));
        if (!m2) {
            res.status = 400;
            res.set_content(m2.error(), "text/plain");
            return;
        }
        const std::string id = random_session_id();
        {
            std::lock_guard lock(sessions_mu);
            sessions.emplace(id, session);
        }
        const Bytes body = mesh::encode(mesh::Value(mesh::Map{{"s", mesh::Value(id)}, {"m", mesh::Value(*m2)}}));
        res.set_content(as_string(body), content_type);
    }

    void on_finish(const httplib::Request& req, httplib::Response& res) {
        const std::string id = req.path_params.at("session");
        auto session = find(id);
        if (!session) {
            res.status = 404;
            return;
        }
        std::lock_guard lock(session->mu);
        if (!session->handshake) {
            res.status = 409;
            return;
        }
        auto ok = session->handshake->receive(as_bytes(req.body));
        auto channel =
            ok ? session->handshake->finish() : std::expected<Channel, std::string>(std::unexpected(ok.error()));
        if (!channel) {
            drop(id);
            res.status = 403;
            res.set_content(channel.error(), "text/plain");
            return;
        }
        session->handshake.reset();
        // A host announces where it can be reached; only for itself, since
        // the channel proved its key.
        const Identity& peer = channel->peer();
        if (peer.role == PeerRole::host && !peer.url.empty() &&
            (peer.url.starts_with("https://") || peer.url.starts_with("http://"))) {
            std::lock_guard address_lock(mu);
            addresses[peer.key] = peer.url;
        }
        session->channel.emplace(std::move(*channel));
        session->used = Clock::now();
        ++accepted;
        res.status = 204;
    }

    void on_frames(const httplib::Request& req, httplib::Response& res) {
        const std::string id = req.path_params.at("session");
        auto session = find(id);
        if (!session) {
            res.status = 404;
            return;
        }
        std::lock_guard lock(session->mu);
        if (!session->channel) {
            res.status = 409;
            return;
        }
        session->used = Clock::now();
        auto messages = unpack(req.body);
        if (!messages) {
            drop(id);
            res.status = 400;
            return;
        }
        Channel& channel = *session->channel;
        std::vector<Bytes> answers;
        for (const auto& m : *messages) {
            auto frame = channel.open(m);
            if (!frame) {
                log("channel from " + mesh::key_id(channel.peer().key).substr(0, 16) + " broken: " + frame.error());
                drop(id);
                res.status = 409;
                return;
            }
            if (!*frame) continue;
            ++received;
            if (handler) {
                for (auto& a : handler(channel.peer(), std::move(**frame))) answers.push_back(std::move(a));
            }
        }
        std::vector<Bytes> sealed;
        for (const auto& a : answers) {
            for (auto& m : channel.seal(a)) sealed.push_back(std::move(m));
        }
        res.set_content(as_string(pack(sealed)), content_type);
    }

    // -- client -----------------------------------------------------------------

    void worker(mesh::PublicKey key, Peer& peer) {
        std::unique_lock lock(mu);
        while (true) {
            peer.cv.wait(lock, [&] { return stopping || !peer.queue.empty(); });
            if (stopping) break;
            std::vector<Bytes> batch;
            std::size_t bytes = 0;
            while (!peer.queue.empty() &&
                   (batch.empty() || bytes + peer.queue.front().size() <= config.max_batch_bytes)) {
                bytes += peer.queue.front().size();
                batch.push_back(std::move(peer.queue.front()));
                peer.queue.pop_front();
            }
            peer.busy = true;
            auto address = addresses.find(key);
            const std::optional<std::string> url =
                address == addresses.end() ? std::nullopt : std::optional<std::string>(address->second);
            lock.unlock();
            deliver(key, peer, url, batch);
            lock.lock();
            peer.busy = false;
            idle_cv.notify_all();
        }
    }

    void deliver(const mesh::PublicKey& key, Peer& peer, const std::optional<std::string>& url,
                 const std::vector<Bytes>& batch) {
        auto give_up = [&](const std::string& why) {
            dropped += batch.size();
            peer.connection.reset();
            log("frames to " + mesh::key_id(key).substr(0, 16) + " dropped: " + why);
        };
        if (!url) return give_up("no address");
        for (int attempt = 0; attempt < 2; ++attempt) {
            if (peer.connection && peer.connection->url != *url) peer.connection.reset();  // the peer moved
            if (!peer.connection) {
                AcceptPeer expected = [&key](const Identity& other) -> std::expected<void, std::string> {
                    if (other.key != key || other.role != PeerRole::host) {
                        return std::unexpected(std::string("the server is not the expected host"));
                    }
                    return {};
                };
                auto c = connect(*url, identity, PeerRole::host, mesh_id, expected, config, own_url());
                if (!c) return give_up(c.error());
                ++opened;
                peer.connection.emplace(std::move(*c));
            }
            auto answers = exchange(*peer.connection, batch);
            if (answers) {
                sent += batch.size();
                if (handler) {
                    for (auto& a : *answers) (void)handler(peer.connection->server, std::move(a));
                }
                return;
            }
            peer.connection.reset();
            if (!answers.error().retry_safe) return give_up(answers.error().text);
        }
        give_up("channel session unknown twice");
    }
};

Transport::Transport(const mesh::SigningKey& identity, mesh::RecordId mesh, TransportConfig config, AcceptPeer accept,
                     FrameHandler handler)
    : impl_(std::make_unique<Impl>(identity, mesh, std::move(config), std::move(accept), std::move(handler))) {}

Transport::~Transport() {
    stop();
}

std::expected<void, std::string> Transport::start() {
    Impl& t = *impl_;
    if (!t.config.listen) return {};
    if (!t.config.tls_cert.empty() || !t.config.tls_key.empty()) {
        std::ifstream cert(t.config.tls_cert, std::ios::binary);
        std::ifstream key(t.config.tls_key, std::ios::binary);
        if (!cert || !key) return std::unexpected(std::string("cannot read the TLS certificate or key"));
        t.certificate.cert_pem.assign(std::istreambuf_iterator<char>(cert), {});
        t.certificate.key_pem.assign(std::istreambuf_iterator<char>(key), {});
    } else {
        auto c = self_signed_certificate("paglets host " + t.identity.id().substr(0, 16));
        if (!c) return std::unexpected(c.error());
        t.certificate = std::move(*c);
    }
    httplib::SSLServer::PemMemory pem{t.certificate.cert_pem.data(),
                                      t.certificate.cert_pem.size(),
                                      t.certificate.key_pem.data(),
                                      t.certificate.key_pem.size(),
                                      nullptr,
                                      0,
                                      nullptr};
    t.server = std::make_unique<httplib::SSLServer>(pem);
    if (!t.server->is_valid()) return std::unexpected(std::string("invalid TLS certificate or key"));
    const auto secs = std::chrono::duration_cast<std::chrono::seconds>(t.config.timeout).count();
    t.server->set_read_timeout(static_cast<time_t>(secs));
    t.server->set_write_timeout(static_cast<time_t>(secs));
    t.server->set_payload_max_length(t.config.max_frame + t.config.max_frame / 1024 + t.config.max_batch_bytes +
                                     (1u << 20));
    t.server->Post(path_open, [&t](const httplib::Request& q, httplib::Response& s) { t.on_open(q, s); });
    t.server->Post(std::string(path_finish) + ":session",
                   [&t](const httplib::Request& q, httplib::Response& s) { t.on_finish(q, s); });
    t.server->Post(std::string(path_frames) + ":session",
                   [&t](const httplib::Request& q, httplib::Response& s) { t.on_frames(q, s); });
    if (t.config.listen_port == 0) {
        t.port = t.server->bind_to_any_port(t.config.listen_host);
    } else {
        t.port = t.server->bind_to_port(t.config.listen_host, t.config.listen_port) ? t.config.listen_port : -1;
    }
    if (t.port <= 0) return std::unexpected("cannot listen on " + t.config.listen_host);
    t.server_thread = std::thread([&t] { t.server->listen_after_bind(); });
    return {};
}

void Transport::stop() {
    Impl& t = *impl_;
    std::vector<std::thread> threads;
    {
        std::lock_guard lock(t.mu);
        if (t.stopping) return;
        t.stopping = true;
        for (auto& [key, peer] : t.peers) {
            peer->cv.notify_all();
            if (peer->thread.joinable()) threads.push_back(std::move(peer->thread));
        }
    }
    for (auto& th : threads) th.join();
    if (t.server) t.server->stop();
    if (t.server_thread.joinable()) t.server_thread.join();
    t.idle_cv.notify_all();
}

int Transport::port() const {
    return impl_->port;
}

std::string Transport::url() const {
    return "https://" + impl_->config.listen_host + ":" + std::to_string(impl_->port);
}

void Transport::set_address(const mesh::PublicKey& peer, std::string url) {
    std::lock_guard lock(impl_->mu);
    impl_->addresses[peer] = std::move(url);
}

std::optional<std::string> Transport::address(const mesh::PublicKey& peer) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->addresses.find(peer);
    if (it == impl_->addresses.end()) return std::nullopt;
    return it->second;
}

void Transport::send(const mesh::PublicKey& to, Bytes frame) {
    Impl& t = *impl_;
    std::lock_guard lock(t.mu);
    if (t.stopping) {
        ++t.dropped;
        return;
    }
    auto& slot = t.peers[to];
    if (!slot) {
        slot = std::make_unique<Impl::Peer>();
        Impl::Peer& peer = *slot;
        peer.thread = std::thread([&t, to, &peer] { t.worker(to, peer); });
    }
    slot->queue.push_back(std::move(frame));
    slot->cv.notify_one();
}

bool Transport::flush(std::chrono::milliseconds timeout) {
    Impl& t = *impl_;
    std::unique_lock lock(t.mu);
    return t.idle_cv.wait_for(lock, timeout, [&t] {
        return std::ranges::all_of(t.peers, [](const auto& e) { return e.second->queue.empty() && !e.second->busy; });
    });
}

TransportStats Transport::stats() const {
    const Impl& t = *impl_;
    return TransportStats{t.sent, t.received, t.dropped, t.opened, t.accepted, t.refused};
}

// -- ClientSession ----------------------------------------------------------------------

struct ClientSession::Impl {
    Connection connection;
};

ClientSession::ClientSession(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
ClientSession::ClientSession(ClientSession&&) noexcept = default;
ClientSession& ClientSession::operator=(ClientSession&&) noexcept = default;
ClientSession::~ClientSession() = default;

std::expected<ClientSession, std::string> ClientSession::open(const std::string& url, const mesh::SigningKey& identity,
                                                              PeerRole role, const mesh::RecordId& mesh,
                                                              AcceptPeer accept, TransportConfig config) {
    auto c = connect(url, identity, role, mesh, accept, config);
    if (!c) return std::unexpected(c.error());
    auto impl = std::make_unique<Impl>();
    impl->connection = std::move(*c);
    return ClientSession(std::move(impl));
}

std::expected<std::vector<Bytes>, std::string> ClientSession::exchange(const std::vector<Bytes>& frames) {
    auto r = net::exchange(impl_->connection, frames);
    if (!r) return std::unexpected(r.error().text);
    return std::move(*r);
}

const Identity& ClientSession::server() const {
    return impl_->connection.server;
}

}  // namespace paglets::net
