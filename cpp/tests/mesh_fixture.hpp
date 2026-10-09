// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Hosts with nodes for tests of code mobility and movement: `Mesh` joins
// them with an in-memory network (frames can be inspected, changed or
// dropped), `NetHost` runs a node over the HTTPS transport.

#pragma once

#include "runtime_fixture.hpp"
#include "test.hpp"

#include <paglets/mesh/gossip.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/passport.hpp>
#include <paglets/net/transport.hpp>
#include <paglets/node/node.hpp>
#include <paglets/services/system_services.hpp>

#include <array>
#include <deque>
#include <random>
#include <filesystem>
#include <functional>
#include <map>
#include <tuple>

namespace paglets::test {

using namespace paglets::mesh;
namespace ps = paglets::services;
using Launch = paglets::node::Node::LaunchStatus;

inline std::string frame_type(const Bytes& frame) {
    auto v = decode(frame);
    if (!v || v->as_map() == nullptr) return {};
    return Fields{*v->as_map()}.str("t").value_or("");
}

// Hosts connected by an in-memory network. `filter` sees every frame before
// delivery and may change or drop it.
struct Mesh {
    struct Port final : GossipTransport {
        Mesh* net = nullptr;
        PublicKey self{};
        // Runtime threads send too (deliveries and departures).
        void send(const PublicKey& to, Bytes frame) override {
            std::lock_guard lock(net->queue_mu);
            net->queue.emplace_back(self, to, std::move(frame));
        }
    };

    struct Host {
        std::string name;
        PublicKey key{};
        Port port;
        std::unique_ptr<Fixture> f;
        std::shared_ptr<ps::SystemServices> services;
        std::unique_ptr<paglets::node::Node> node;
        bool down = false;  // shut down: no frames in or out, no ticks
        std::filesystem::path state;
        ps::ServicesConfig services_config;
        std::array<std::uint8_t, 32> seed{};  // of the host key, for restarts
    };

    SigningKey admin = SigningKey::generate();
    SigningKey owner = SigningKey::generate();
    std::optional<Record> genesis;
    std::mutex queue_mu;
    std::deque<std::tuple<PublicKey, PublicKey, Bytes>> queue;
    std::vector<std::unique_ptr<Host>> hosts;
    std::function<bool(const PublicKey& from, const PublicKey& to, Bytes& frame)> filter;
    std::map<std::string, int> sent;  // frames by type

    Mesh() {
        auto g = make_genesis("mobility", {&admin}, {}, 1);
        REQUIRE_OK(g);
        genesis = *g;
    }

    ~Mesh() {
        // Runtimes first: their threads call into the nodes.
        for (auto& h : hosts) h->f->runtime->shutdown();
        for (auto& h : hosts) h->node.reset();
    }

    // `state`: a directory for the runtime and the node (empty: in memory).
    Host& add_host(const std::string& name, ps::ServicesConfig services_config = {}, std::filesystem::path state = {}) {
        auto h = std::make_unique<Host>();
        h->name = name;
        h->state = std::move(state);
        h->services_config = std::move(services_config);
        std::random_device random;
        for (auto& b : h->seed) b = static_cast<std::uint8_t>(random());
        auto key = SigningKey::from_seed(h->seed);
        REQUIRE_OK(key);
        h->key = key->public_key();
        h->port.net = this;
        h->port.self = h->key;
        boot(*h, std::move(*key));
        for (auto& other : hosts) {
            h->node->add_seed(other->key);
            other->node->add_seed(h->key);
        }
        hosts.push_back(std::move(h));
        return *hosts.back();
    }

    void boot(Host& h, SigningKey key) {
        rt::Config config;
        if (!h.state.empty()) config.state_dir = h.state / "runtime";
        h.f = std::make_unique<Fixture>(std::move(config));
        auto installed = ps::install_system_services(*h.f->runtime, h.services_config);
        REQUIRE_OK(installed);
        h.services = *installed;
        auto ledger = Ledger::create(*genesis);
        REQUIRE_OK(ledger);
        h.node = std::make_unique<paglets::node::Node>(*h.f->runtime, h.services, std::move(*ledger), std::move(key),
                                                       h.port, h.state.empty() ? h.state : h.state / "node");
        REQUIRE_OK(h.node->start());
    }

    // Stops a host (its state stays in its directory) and starts it again;
    // it learns the ledger from its peers.
    void restart(Host& h) {
        h.f->runtime->shutdown();
        h.node.reset();
        h.services.reset();
        h.f.reset();
        auto key = SigningKey::from_seed(h.seed);
        REQUIRE_OK(key);
        boot(h, std::move(*key));
        for (auto& other : hosts) {
            if (other.get() != &h) h.node->add_seed(other->key);
        }
    }

    Host* find(const PublicKey& key) {
        for (auto& h : hosts) {
            if (h->key == key) return h.get();
        }
        return nullptr;
    }

    // An admin record made on the first host.
    void admin_record(const std::string& type, Map data) {
        auto r = hosts.front()->node->draft(type, std::move(data));
        REQUIRE_OK(r);
        r->sign(admin);
        REQUIRE_OK(hosts.front()->node->submit(*r));
    }

    void enroll_all() {
        for (auto& h : hosts) admin_record("host-enroll", data::host_enroll(h->key, h->name, {}));
        admin_record("owner-enroll", data::owner_enroll(owner.public_key(), "olga", {}));
        REQUIRE(settle([] { return false; }, 40) || converged());
    }

    bool converged() {
        for (auto& h : hosts) {
            if (h->node->ledger_digest() != hosts.front()->node->ledger_digest()) return false;
        }
        return true;
    }

    void deliver() {
        while (true) {
            std::tuple<PublicKey, PublicKey, Bytes> next;
            {
                std::lock_guard lock(queue_mu);
                if (queue.empty()) return;
                next = std::move(queue.front());
                queue.pop_front();
            }
            auto& [from, to, frame] = next;
            ++sent[frame_type(frame)];
            if (filter && !filter(from, to, frame)) continue;
            if (Host* src = find(from); src != nullptr && src->down) continue;
            if (Host* h = find(to); h != nullptr && !h->down) h->node->receive(from, frame);
        }
    }

    // Delivers and ticks until `done` holds (or rounds run out).
    bool settle(const std::function<bool()>& done, int rounds = 200) {
        for (int i = 0; i < rounds; ++i) {
            deliver();
            if (done()) return true;
            for (auto& h : hosts) {
                if (!h->down) h->node->tick();
            }
            std::this_thread::sleep_for(2ms);
        }
        deliver();
        return done();
    }

    Passport passport(const std::string& module, std::string paglet) {
        const std::int64_t now = unix_ms();
        auto p = Passport::issue(owner, genesis->id(), *parse_key_id(module), std::move(paglet), Value(), now - 1000,
                                 now + 600'000);
        REQUIRE_OK(p);
        return *p;
    }

    Launch launch(Host& from, Host& to, const Passport& p, Bytes args = {}) {
        auto id = from.node->launch(to.key, p, std::move(args));
        REQUIRE_OK(id);
        REQUIRE(settle([&] { return from.node->launch_status(*id)->state != Launch::State::pending; }));
        return *from.node->launch_status(*id);
    }
};

inline std::string paglet_id(char c) {
    return std::string(32, c);
}

// A host with a node and the HTTPS transport: frames travel over real
// channels on the loopback interface.
struct NetHost {
    ForwardingTransport forward;
    std::unique_ptr<Fixture> f;
    std::shared_ptr<ps::SystemServices> services;
    std::unique_ptr<paglets::node::Node> node;
    std::unique_ptr<paglets::net::Transport> transport;

    explicit NetHost(const Record& genesis, ps::ServicesConfig services_config = {},
                     paglets::net::TransportConfig transport_config = {}) {
        f = std::make_unique<Fixture>();
        auto installed = ps::install_system_services(*f->runtime, services_config);
        REQUIRE_OK(installed);
        services = *installed;
        auto ledger = Ledger::create(genesis);
        REQUIRE_OK(ledger);
        node = std::make_unique<paglets::node::Node>(*f->runtime, services, std::move(*ledger), SigningKey::generate(),
                                                     forward);
        REQUIRE_OK(node->start());
        namespace net = paglets::net;
        transport = std::make_unique<net::Transport>(
            node->host_key(), genesis.id(), std::move(transport_config),
            [this](const net::Identity& peer) -> std::expected<void, std::string> {
                const auto peers = node->peers();
                if (peer.role != net::PeerRole::host || std::ranges::find(peers, peer.key) == peers.end()) {
                    return std::unexpected(std::string("not a host of this mesh"));
                }
                return {};
            },
            [this](const net::Identity& from, Bytes frame) -> std::vector<Bytes> {
                if (from.role == net::PeerRole::host) node->receive(from.key, frame);
                return {};
            });
        REQUIRE_OK(transport->start());
        forward.set_target(transport.get());
    }

    ~NetHost() {
        f->runtime->shutdown();  // its threads call into the node
        forward.set_target(nullptr);
        transport->stop();
        transport.reset();
        node.reset();
        f.reset();
    }
};

}  // namespace paglets::test
