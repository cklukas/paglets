// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-host serve: a host of a mesh on the network
// (planning/cpp-networking.md, section 8). It opens its ledger and host key,
// runs the runtime with the system paglets and the node, and serves channels
// over HTTPS: gossip, code mobility and moves with other hosts, CLI sessions
// of admins and owners.

#include "serve_cli.hpp"

#if PAGLETS_HAVE_REFLECTION

#include <paglets/gateway/gateway.hpp>
#include <paglets/mesh/crypto.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/net/beacon.hpp>
#include <paglets/net/transport.hpp>
#include <paglets/node/node.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/services/system_services.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace paglets::cli {

namespace {

namespace fs = std::filesystem;
namespace rt = paglets::runtime;
namespace mesh = paglets::mesh;
namespace net = paglets::net;

std::atomic<bool> stop_requested{false};

extern "C" void on_signal(int) {
    stop_requested = true;
}

int usage() {
    std::cerr << "usage: paglets-host serve --key HOST-KEY --ledger DIR --state DIR [--listen HOST:PORT]\n"
                 "                          [--advertise URL] [--peer KEY-ID=URL]... [--root NAME=DIR]...\n"
                 "                          [--module-source DIR]... [--tls-cert FILE --tls-key FILE] [--tls-ca FILE]\n"
                 "                          [--threads N] [--in-process] [--no-sandbox] [--stop-file FILE]\n"
                 "                          [--join URL]... [--no-beacon] [--beacon-port N] [--no-listen]\n"
                 "                          [--slots N] [--web [--web-internal URL-PREFIX]... [--web-no-internet]\n"
                 "                          [--web-proxy URL] [--web-search URL] [--web-ca FILE]]\n"
                 "                          [--ai ollama|test [--ai-url URL] [--ai-model NAME]...]\n"
                 "Runs a host of the mesh whose ledger is in DIR (the host key must be enrolled, or the host\n"
                 "starts with its peers as seeds and waits for its enrollment). Hosts find each other through\n"
                 "any enrolled host they can reach (--join), gossip and multicast beacons on the local network.\n"
                 "--no-listen: no inbound port (behind NAT); other hosts relay for this one.\n"
                 "--web: offer the `web` system paglet (internal destinations only with --web-internal; the\n"
                 "proxy defaults to HTTPS_PROXY); --ai: offer the `ai` system paglet through Ollama (or `test`).\n"
                 "Stops on SIGINT/SIGTERM, or when the stop file appears.\n";
    return 2;
}

int fail(const std::string& message) {
    std::cerr << "paglets-host: " << message << "\n";
    return 1;
}

struct Options {
    std::optional<std::string> key, ledger, state, advertise, tls_cert, tls_key, tls_ca, stop_file;
    std::string listen = "127.0.0.1:0";
    std::vector<std::pair<std::string, std::string>> peers;
    std::map<std::string, std::string> roots;
    std::vector<std::string> sources;
    std::vector<std::string> joins;
    unsigned threads = 2;
    bool in_process = false;
    bool no_sandbox = false;
    bool beacon = true;
    int beacon_port = 0;
    bool inbound = true;
    std::int64_t slots = 0;
    bool web = false;
    bool web_internet = true;
    std::vector<std::string> web_internal;
    std::optional<std::string> web_proxy, web_search, web_ca, ai, ai_url;
    std::vector<std::string> ai_models;
};

std::optional<Options> parse(int argc, char** argv) {
    Options o;
    for (int i = 2; i < argc; ++i) {
        const std::string_view a = argv[i];
        auto next = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc) return std::nullopt;
            return std::string(argv[++i]);
        };
        auto set = [&](std::optional<std::string>& field) {
            field = next();
            return field.has_value();
        };
        auto pair = [&](std::string& k, std::string& v) {
            auto x = next();
            if (!x) return false;
            const auto eq = x->find('=');
            if (eq == std::string::npos || eq == 0 || eq + 1 == x->size()) return false;
            k = x->substr(0, eq);
            v = x->substr(eq + 1);
            return true;
        };
        bool ok = true;
        if (a == "--key") {
            ok = set(o.key);
        } else if (a == "--ledger") {
            ok = set(o.ledger);
        } else if (a == "--state") {
            ok = set(o.state);
        } else if (a == "--listen") {
            auto v = next();
            ok = v.has_value();
            if (ok) o.listen = *v;
        } else if (a == "--advertise") {
            ok = set(o.advertise);
        } else if (a == "--peer") {
            std::string k, v;
            ok = pair(k, v);
            if (ok) o.peers.emplace_back(k, v);
        } else if (a == "--root") {
            std::string k, v;
            ok = pair(k, v);
            if (ok) o.roots[k] = v;
        } else if (a == "--module-source") {
            auto v = next();
            ok = v.has_value();
            if (ok) o.sources.push_back(*v);
        } else if (a == "--tls-cert") {
            ok = set(o.tls_cert);
        } else if (a == "--tls-key") {
            ok = set(o.tls_key);
        } else if (a == "--tls-ca") {
            ok = set(o.tls_ca);
        } else if (a == "--stop-file") {
            ok = set(o.stop_file);
        } else if (a == "--threads") {
            auto v = next();
            ok = v.has_value();
            if (ok) {
                try {
                    o.threads = static_cast<unsigned>(std::stoul(*v));
                } catch (const std::exception&) {
                    ok = false;
                }
            }
        } else if (a == "--join") {
            auto v = next();
            ok = v.has_value();
            if (ok) o.joins.push_back(*v);
        } else if (a == "--slots") {
            auto v = next();
            ok = v.has_value();
            if (ok) {
                try {
                    o.slots = std::stoll(*v);
                } catch (const std::exception&) {
                    ok = false;
                }
                ok = ok && o.slots > 0;
            }
        } else if (a == "--web") {
            o.web = true;
        } else if (a == "--web-no-internet") {
            o.web_internet = false;
        } else if (a == "--web-internal") {
            auto v = next();
            ok = v.has_value();
            if (ok) o.web_internal.push_back(*v);
        } else if (a == "--web-proxy") {
            ok = set(o.web_proxy);
        } else if (a == "--web-search") {
            ok = set(o.web_search);
        } else if (a == "--web-ca") {
            ok = set(o.web_ca);
        } else if (a == "--ai") {
            ok = set(o.ai);
        } else if (a == "--ai-url") {
            ok = set(o.ai_url);
        } else if (a == "--ai-model") {
            auto v = next();
            ok = v.has_value();
            if (ok) o.ai_models.push_back(*v);
        } else if (a == "--no-listen") {
            o.inbound = false;
        } else if (a == "--no-beacon") {
            o.beacon = false;
        } else if (a == "--beacon-port") {
            auto v = next();
            ok = v.has_value();
            if (ok) {
                try {
                    o.beacon_port = std::stoi(*v);
                } catch (const std::exception&) {
                    ok = false;
                }
                ok = ok && o.beacon_port > 0 && o.beacon_port < 65536;
            }
        } else if (a == "--in-process") {
            o.in_process = true;
        } else if (a == "--no-sandbox") {
            o.no_sandbox = true;
        } else {
            ok = false;
        }
        if (!ok) return std::nullopt;
    }
    if (!o.key || !o.ledger || !o.state) return std::nullopt;
    return o;
}

}  // namespace

int serve_command(int argc, char** argv, const fs::path& self) {
    auto parsed = parse(argc, argv);
    if (!parsed) return usage();
    const Options& o = *parsed;

    auto key = mesh::load_key(*o.key, std::nullopt);
    if (!key) return fail(key.error());
    auto ledger = mesh::Ledger::open(*o.ledger);
    if (!ledger) return fail(ledger.error());
    const mesh::RecordId mesh_id = ledger->mesh();
    const std::string key_id = key->id();

    const auto colon = o.listen.rfind(':');
    if (colon == std::string::npos) return fail("--listen is HOST:PORT");
    net::TransportConfig tc;
    tc.listen_host = o.listen.substr(0, colon);
    try {
        tc.listen_port = std::stoi(o.listen.substr(colon + 1));
    } catch (const std::exception&) {
        return fail("--listen is HOST:PORT");
    }
    if (o.tls_cert) tc.tls_cert = *o.tls_cert;
    if (o.tls_key) tc.tls_key = *o.tls_key;
    if (o.tls_ca) tc.tls_ca = *o.tls_ca;
    if (o.advertise) tc.advertise_url = *o.advertise;
    tc.listen = o.inbound;
    tc.log = [](const std::string& text) { std::cerr << "[net] " << text << "\n"; };

    rt::Config rc;
    rc.host_name = key_id.substr(0, 16);
    rc.threads = std::max(1u, o.threads);
    rc.state_dir = fs::path(*o.state) / "runtime";
    rc.sandbox_workers = !o.no_sandbox;
    if (!o.in_process && !self.empty()) {
        auto worker = self.parent_path() / "paglets-worker";
#ifdef _WIN32
        worker += ".exe";
#endif
        std::error_code ec;
        if (fs::exists(worker, ec)) rc.worker_executable = worker;
    }
    rc.log = [](const rt::LogRecord& r) {
        std::cerr << "[" << (r.paglet.empty() ? std::string("host") : r.paglet.substr(0, 8)) << "] " << r.text << "\n";
    };
    rt::Runtime runtime(std::move(rc));

    services::ServicesConfig sc;
    for (const auto& [name, dir] : o.roots) sc.roots[name] = dir;
    sc.state_dir = fs::path(*o.state) / "services";
    auto services = services::install_system_services(runtime, std::move(sc));
    if (!services) return fail(services.error());

    mesh::ForwardingTransport forward;
    node::Node node(runtime, *services, std::move(*ledger), std::move(*key), forward, fs::path(*o.state) / "node");
    if (auto ok = node.start(); !ok) return fail(ok.error());
    if (o.slots > 0) node.set_compute_slots(o.slots);
    for (const auto& dir : o.sources) node.add_module_source(std::make_shared<rt::DirectorySource>(dir));

    // Gateway system paglets (planning/cpp-web-ai.md): offered to the mesh,
    // used within the mesh policy.
    std::shared_ptr<gateway::Gateway> gateways;
    if (o.web || o.ai) {
        gateway::GatewayConfig gc;
        gc.web.enabled = o.web;
        gc.web.internet = o.web_internet;
        gc.web.internal = o.web_internal;
        if (o.web_proxy) {
            gc.web.proxy = *o.web_proxy;
        } else {
            // The host's proxy settings.
            for (const char* name : {"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"}) {
                if (const char* v = std::getenv(name); v != nullptr && *v != '\0') {
                    gc.web.proxy = v;
                    break;
                }
            }
        }
        if (o.web_search) gc.web.search_url = *o.web_search;
        if (o.web_ca) gc.web.ca_file = *o.web_ca;
        if (o.ai) {
            gc.ai.enabled = true;
            gc.ai.backend = *o.ai;
            if (o.ai_url) gc.ai.url = *o.ai_url;
            gc.ai.models = o.ai_models;
        }
        gc.offers = [&node](const std::string& service, std::optional<services::mesh_info::Offer> offer) {
            if (offer) {
                node.set_offer(std::move(*offer));
            } else {
                node.withdraw_offer(service);
            }
        };
        gc.authorize = [&node](const abi::SenderRecord& caller, std::string_view service, std::string_view op,
                               std::string_view root, std::string_view path) {
            return node.allows(caller, mesh::Item{std::string(service), {std::string(op)}, std::string(root),
                                                  std::string(path)});
        };
        auto installed = gateway::install_gateway(runtime, *services, std::move(gc));
        if (!installed) return fail(installed.error());
        gateways = *installed;
    }

    net::Transport transport(
        node.host_key(), mesh_id, tc,
        [&node](const net::Identity& peer) -> std::expected<void, std::string> {
            switch (peer.role) {
                case net::PeerRole::host: {
                    const auto peers = node.peers();
                    if (std::ranges::find(peers, peer.key) != peers.end()) return {};
                    return std::unexpected(std::string("not a host of this mesh"));
                }
                case net::PeerRole::admin:
                    if (node.state().is_admin(peer.key)) return {};
                    return std::unexpected(std::string("not an admin of this mesh"));
                case net::PeerRole::owner:
                    if (node.state().is_owner(peer.key)) return {};
                    return std::unexpected(std::string("not an enrolled owner"));
            }
            return std::unexpected(std::string("unknown role"));
        },
        [&node](const net::Identity& from, net::Bytes frame) -> std::vector<net::Bytes> {
            if (from.role == net::PeerRole::host) {
                node.receive(from.key, frame);
                return {};
            }
            return {node.answer_session(from.key, net::to_string(from.role), std::move(frame))};
        });
    for (const auto& [id, url] : o.peers) {
        auto peer = mesh::parse_key_id(id);
        if (!peer) return fail("--peer takes a key ID (64 hex digits): " + id);
        node.add_seed(*peer);
        transport.set_address(*peer, url);
    }
    if (auto ok = transport.start(); !ok) return fail(ok.error());
    forward.set_target(&transport);

    std::cout << "host " << key_id << "\n"
              << "mesh " << mesh::record_id_hex(mesh_id) << "\n"
              << "url  " << (o.inbound ? transport.url() : std::string("(none: reached through relays)")) << "\n"
              << std::flush;

    // Discovery (planning/cpp-mesh.md): multicast beacons on the local
    // network, and bootstrap contacts tried until they answer.
    std::unique_ptr<net::Beacon> beacon;
    if (o.beacon) {
        net::BeaconConfig bc;
        if (o.beacon_port > 0) bc.port = o.beacon_port;
        beacon = std::make_unique<net::Beacon>(
            bc, [&node] { return node.beacon(); },
            [&node](std::vector<std::uint8_t> datagram, const std::string& ip) { node.receive_beacon(datagram, ip); });
        if (auto ok = beacon->start(); !ok) {
            std::cerr << "[mesh] no multicast beacons: " << ok.error() << "\n";
            beacon.reset();
        }
    }
    std::thread joiner([&] {
        std::vector<std::string> pending = o.joins;
        while (!pending.empty() && !stop_requested) {
            for (auto it = pending.begin(); it != pending.end();) {
                auto peer = transport.probe(*it);
                if (peer) {
                    std::cerr << "[mesh] joined through " << *it << " (host " << mesh::key_id(*peer).substr(0, 16)
                              << ")\n";
                    node.joined(*peer);
                    it = pending.erase(it);
                } else {
                    std::cerr << "[mesh] " << *it << ": " << peer.error() << "\n";
                    ++it;
                }
            }
            for (int i = 0; i < 20 && !pending.empty() && !stop_requested; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    });

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    using namespace std::chrono_literals;
    auto next_tick = std::chrono::steady_clock::now();
    while (!stop_requested) {
        if (o.stop_file) {
            std::error_code ec;
            if (fs::exists(*o.stop_file, ec)) break;
        }
        if (std::chrono::steady_clock::now() >= next_tick) {
            node.tick();
            next_tick = std::chrono::steady_clock::now() + 500ms;
        }
        std::this_thread::sleep_for(50ms);
    }
    std::cerr << "[host] stopping\n";
    stop_requested = true;
    joiner.join();
    if (beacon) beacon->stop();
    runtime.shutdown();  // its threads call into the node
    forward.set_target(nullptr);
    transport.stop();
    return 0;
}

}  // namespace paglets::cli

#else

#include <iostream>

namespace paglets::cli {

int serve_command(int, char**, const std::filesystem::path&) {
    std::cerr << "paglets-host: this build has no mesh host (no reflection)\n";
    return 1;
}

}  // namespace paglets::cli

#endif
