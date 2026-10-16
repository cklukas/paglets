// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-web: a local web UI for a mesh (WP35, docs/web-ui.md).

#include "app.hpp"

#include <paglets/mesh/crypto.hpp>
#include <paglets/sha256.hpp>

#include <atomic>
#include <csignal>
#include <iostream>
#include <thread>

namespace {

using namespace paglets;

std::atomic<bool> g_stop{false};

extern "C" void on_signal(int) {
    g_stop.store(true);
}

int usage() {
    std::cerr
        << "usage: paglets-web --connect URL --key KEY --ledger DIR [--host-key KEY-ID]\n"
           "                   [--passphrase-file FILE] [--port N] [--port-range A-B] [--no-port-search]\n"
           "                   [--listen ADDR] [--allow-remote] [--no-browser] [--module-dir DIR]\n"
           "                   [--refresh-ms N] [--program NAME]\n"
           "A web UI for the mesh of the ledger copy DIR, served on this computer: the hosts with their\n"
           "properties and paglets; commands to paglets that describe their messages; launching\n"
           "modules (an owner KEY), dispatching, pinning and ending paglets. It talks to the host at URL\n"
           "(and to the other hosts it lists) as `paglets-host remote` does, and shows the matching\n"
           "commands next to every view and action.\n"
           "--port N listens on exactly that port; without it, 8470 is tried, then the rest of\n"
           "--port-range (default 8470-8499), then any free port (--no-port-search: only the first).\n"
           "--listen defaults to 127.0.0.1; ::1 and localhost are also allowed. Other addresses need\n"
           "--allow-remote (anyone who reaches the port and has the printed link acts with KEY).\n"
           "--module-dir offers its .wasm files for\n"
           "launching and pushing. --program names paglets-host in the commands shown.\n";
    return 2;
}

int fail(const std::string& message) {
    std::cerr << "paglets-web: " << message << "\n";
    return 1;
}

}  // namespace

int main(int argc, char** argv) {
    webapp::Options o;
    web::PortChoice ports;
    std::string listen = "127.0.0.1";
    bool allow_remote = false;
    bool browser = true;
    std::optional<std::string> connect, key, ledger;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc) return std::nullopt;
            return std::string(argv[++i]);
        };
        std::optional<std::string> v;
        if (a == "--help" || a == "-h") return usage();
        if (a == "--version") {
            std::cout << "paglets-web " << PAGLETS_VERSION << "\n";
            return 0;
        }
        if (a == "--no-port-search") {
            ports.search = false;
        } else if (a == "--allow-remote") {
            allow_remote = true;
        } else if (a == "--no-browser") {
            browser = false;
        } else if (!(v = next())) {
            return usage();
        } else if (a == "--connect") {
            connect = v;
        } else if (a == "--key") {
            key = v;
        } else if (a == "--ledger") {
            ledger = v;
        } else if (a == "--host-key") {
            o.host_key = v;
        } else if (a == "--passphrase-file") {
            o.passphrase_file = v;
        } else if (a == "--module-dir") {
            o.module_dir = *v;
        } else if (a == "--listen") {
            listen = *v;
        } else if (a == "--program") {
            o.program = *v;
        } else if (a == "--port") {
            try {
                const int p = std::stoi(*v);
                if (p < 0 || p > 65535) return fail("--port is 0 to 65535");
                ports.port = static_cast<std::uint16_t>(p);
            } catch (const std::exception&) {
                return fail("--port takes a number");
            }
        } else if (a == "--port-range") {
            auto range = web::parse_port_range(*v);
            if (!range) return fail("--port-range takes A-B");
            ports.first = range->first;
            ports.last = range->second;
        } else if (a == "--refresh-ms") {
            try {
                o.refresh = std::chrono::milliseconds(std::max(500, std::stoi(*v)));
            } catch (const std::exception&) {
                return fail("--refresh-ms takes a number");
            }
        } else {
            return usage();
        }
    }
    if (!connect || !key || !ledger) return usage();
    if (listen != "127.0.0.1" && listen != "::1" && listen != "localhost" && !allow_remote) {
        return fail("listening on " + listen + " needs --allow-remote");
    }
    o.connect = *connect;
    o.key_file = *key;
    o.ledger_dir = *ledger;
    if (o.host_key && !mesh::parse_key_id(*o.host_key)) return fail("--host-key takes a key ID (64 hex digits)");
    if (o.module_dir && !std::filesystem::is_directory(*o.module_dir)) {
        return fail(o.module_dir->string() + " is not a directory");
    }

    try {
        auto passphrases = client::Passphrases::make(o.passphrase_file);
        if (!passphrases) return fail(passphrases.error());
        auto identity = client::Identity::open(o.key_file, o.ledger_dir, *passphrases);
        if (!identity) return fail(identity.error());

        web::ServerConfig config;
        config.address = listen;
        config.ports = web::port_candidates(ports);
        config.limits.max_body = 32 * 1024 * 1024;  // module uploads, in base64
        auto server = web::Server::bind(config);
        if (!server) return fail(server.error());
        const std::uint16_t port = (*server)->port();

        std::array<std::uint8_t, 24> secret{};
        mesh::random_bytes(secret);
        const web::Guard guard(to_hex(std::span<const std::uint8_t>(secret)), port, allow_remote);

        webapp::App app(o, std::move(*identity));
        app.refresh();  // the first page shows the mesh at once (or why not)

        const std::string host = listen == "::1" ? "[::1]" : (listen == "0.0.0.0" || listen == "::") ? "127.0.0.1" : listen;
        const std::string url = "http://" + host + ":" + std::to_string(port) + "/?t=" + guard.token();
        std::cout << "paglets-web: " << url << "\n" << std::flush;
        if (browser && !web::open_browser(url)) std::cerr << "paglets-web: open the URL above in a browser\n";

        std::signal(SIGINT, on_signal);
        std::signal(SIGTERM, on_signal);
        std::thread poller([&] { app.run_poller(); });
        std::thread watcher([&] {
            while (!g_stop.load()) std::this_thread::sleep_for(std::chrono::milliseconds(200));
            app.stop();
            (*server)->stop();
        });
        (*server)->serve([&](const web::Request& r) { return webapp::route(app, guard, r); });
        g_stop.store(true);
        watcher.join();
        poller.join();
    } catch (const std::exception& e) {
        return fail(e.what());
    }
    return 0;
}
