// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Mesh client sessions (WP31): keys, the session channel and the typed
// requests of the session protocol.

#include <paglets/client/client.hpp>

#include <paglets/mesh/passport.hpp>

#include <fstream>
#include <iostream>
#include <span>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <termios.h>
#include <unistd.h>
#endif

namespace paglets::client {

using mesh::Array;
using mesh::Fields;
using mesh::Map;
using mesh::Value;

// -- keys and passphrases ------------------------------------------------------

std::optional<std::string> read_hidden(const std::string& prompt) {
    std::cerr << prompt << std::flush;
    std::string line;
#ifdef _WIN32
    HANDLE in = GetStdHandle(STD_INPUT_HANDLE);
    DWORD mode = 0;
    const bool console = GetConsoleMode(in, &mode) != 0;
    if (console) SetConsoleMode(in, mode & ~static_cast<DWORD>(ENABLE_ECHO_INPUT));
    const bool got = static_cast<bool>(std::getline(std::cin, line));
    if (console) SetConsoleMode(in, mode);
#else
    termios old{};
    const bool tty = ::isatty(STDIN_FILENO) != 0 && ::tcgetattr(STDIN_FILENO, &old) == 0;
    if (tty) {
        termios quiet = old;
        quiet.c_lflag &= ~static_cast<tcflag_t>(ECHO);
        ::tcsetattr(STDIN_FILENO, TCSAFLUSH, &quiet);
    }
    const bool got = static_cast<bool>(std::getline(std::cin, line));
    if (tty) ::tcsetattr(STDIN_FILENO, TCSAFLUSH, &old);
#endif
    std::cerr << "\n";
    if (!got) return std::nullopt;
    if (!line.empty() && line.back() == '\r') line.pop_back();
    return line;
}

std::expected<Passphrases, std::string> Passphrases::make(const std::optional<std::string>& file) {
    Passphrases p;
    if (file) {
        std::ifstream in(*file);
        std::string line;
        if (!in || !std::getline(in, line)) return std::unexpected("cannot read " + *file);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        p.from_file = line;
    }
    return p;
}

std::optional<std::string> Passphrases::get(const std::string& prompt) const {
    if (from_file) return from_file;
    return read_hidden(prompt);
}

std::expected<mesh::SigningKey, std::string> open_key(const std::string& path, const Passphrases& passphrases) {
    auto info = mesh::read_key_info(path);
    if (!info) return std::unexpected(info.error());
    std::optional<std::string> passphrase;
    if (info->encrypted) {
        passphrase = passphrases.get("passphrase for " + std::string(mesh::to_string(info->role)) + " key '" +
                                     info->name + "' (" + path + "): ");
        if (!passphrase) return std::unexpected(std::string("no passphrase given"));
    }
    return mesh::load_key(path, passphrase);
}

std::expected<Identity, std::string> Identity::open(const std::string& key_file, const std::string& ledger_dir,
                                                    const Passphrases& passphrases) {
    auto ledger = mesh::Ledger::open(ledger_dir);
    if (!ledger) return std::unexpected(ledger.error());
    auto info = mesh::read_key_info(key_file);
    if (!info) return std::unexpected(info.error());
    net::PeerRole role = net::PeerRole::owner;
    if (info->role == mesh::KeyRole::admin) {
        role = net::PeerRole::admin;
    } else if (info->role != mesh::KeyRole::owner) {
        return std::unexpected(std::string("sessions use an admin or owner key"));
    }
    auto key = open_key(key_file, passphrases);
    if (!key) return std::unexpected(key.error());
    return Identity{std::move(*ledger), std::move(*key), role, info->name};
}

// -- answers -------------------------------------------------------------------

std::string HostRow::state() const {
    // online: both ways; one-way: it reaches this host, this host's frames
    // to it fail (send_error says why).
    if (self) return "this host";
    if (online && send_error.empty()) return "online";
    if (online) return "one-way";
    return "offline";
}

namespace {

bool flag(const Fields& f, std::string_view key) {
    const Value* v = f.get(key);
    return v != nullptr && v->as_bool() != nullptr && *v->as_bool();
}

std::optional<mesh::PublicKey> key_of(const Value& v) {
    const Bytes* b = v.as_bin();
    if (b == nullptr || b->size() != 32) return std::nullopt;
    mesh::PublicKey k{};
    std::copy(b->begin(), b->end(), k.begin());
    return k;
}

std::string str_at(const Array& a, std::size_t i) {
    if (i >= a.size() || a[i].as_str() == nullptr) return {};
    return *a[i].as_str();
}

}  // namespace

// -- sessions ------------------------------------------------------------------

struct Session::Impl {
    net::ClientSession channel;
};

Session::Session(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Session::Session(Session&&) noexcept = default;
Session& Session::operator=(Session&&) noexcept = default;
Session::~Session() = default;

std::expected<Session, std::string> Session::open(const std::string& url, const Identity& identity,
                                                  std::optional<mesh::PublicKey> host_key,
                                                  std::chrono::seconds read_timeout) {
    const mesh::LedgerState state = identity.ledger.state();
    auto accept = [host_key, state](const net::Identity& server) -> std::expected<void, std::string> {
        if (server.role != net::PeerRole::host) return std::unexpected(std::string("the server is not a host"));
        if (host_key ? server.key != *host_key : !state.is_host(server.key)) {
            return std::unexpected("host " + mesh::key_id(server.key) + " is not enrolled in this ledger copy");
        }
        return {};
    };
    // Hosts answer calls when the paglet replied (up to 30 s) and moves when
    // they ended (up to 120 s): wait longer than that.
    net::TransportConfig config;
    config.read_timeout = read_timeout;
    auto channel = net::ClientSession::open(url, identity.key, identity.role, identity.ledger.mesh(), accept, config);
    if (!channel) return std::unexpected(channel.error());
    return Session(std::make_unique<Impl>(Impl{std::move(*channel)}));
}

const mesh::PublicKey& Session::host() const {
    return impl_->channel.server().key;
}

std::expected<Map, std::string> Session::ask(Map request) {
    auto answers = impl_->channel.exchange({mesh::encode(Value(std::move(request)))});
    if (!answers) return std::unexpected(answers.error());
    if (answers->size() != 1) return std::unexpected(std::string("no answer"));
    auto v = mesh::decode((*answers)[0]);
    if (!v || v->as_map() == nullptr) return std::unexpected(std::string("malformed answer"));
    const Fields f{*v->as_map()};
    if (!flag(f, "ok")) return std::unexpected(f.str("e").value_or("refused"));
    return *v->as_map();
}

std::expected<Status, std::string> Session::status() {
    auto a = ask(Map{{"t", Value("status")}});
    if (!a) return std::unexpected(a.error());
    const Fields f{*a};
    Status s;
    if (auto k = f.fixed<32>("key")) s.key = *k;
    s.name = f.str("name").value_or("");
    s.mesh = f.str("mesh").value_or("");
    s.records = f.integer("records").value_or(0);
    if (const Array* paglets = f.array("paglets")) {
        for (const auto& p : *paglets) {
            const Array* e = p.as_array();
            if (e == nullptr || e->size() < 4) continue;
            s.paglets.push_back(PagletRow{str_at(*e, 0), str_at(*e, 1), str_at(*e, 2), str_at(*e, 3), str_at(*e, 4)});
        }
    }
    if (auto system = f.strings("system")) {
        s.system = std::move(*system);
        s.extended = true;
    }
    s.uptime_s = f.integer("uptime_s").value_or(0);
    s.version = f.str("version").value_or("");
    s.os = f.str("os").value_or("");
    s.os_version = f.str("os_version").value_or("");
    s.architecture = f.str("architecture").value_or("");
    s.cpu_model = f.str("cpu_model").value_or("");
    s.cpus = f.integer("cpus").value_or(0);
    s.memory_total = f.integer("memory_total").value_or(0);
    return s;
}

std::expected<std::vector<HostRow>, std::string> Session::hosts() {
    auto a = ask(Map{{"t", Value("hosts")}});
    if (!a) return std::unexpected(a.error());
    std::vector<HostRow> out;
    if (const Array* hosts = Fields{*a}.array("hosts")) {
        for (const auto& h : *hosts) {
            if (h.as_map() == nullptr) continue;
            const Fields f{*h.as_map()};
            HostRow r;
            if (auto k = f.fixed<32>("key")) r.key = *k;
            r.name = f.str("name").value_or("");
            r.labels = f.strings("labels").value_or(std::vector<std::string>{});
            r.url = f.str("url").value_or("");
            r.self = flag(f, "self");
            r.online = flag(f, "online");
            r.send_error = f.str("send_error").value_or("");
            r.compatible = flag(f, "compatible");
            r.seen_ms = f.integer("seen").value_or(0);
            r.protocol = f.integer("proto").value_or(0);
            r.abi = f.str("abi").value_or("");
            r.via = f.str("via").value_or("");
            if (const Array* relays = f.array("relays")) {
                for (const auto& k : *relays) {
                    if (auto key = key_of(k)) r.relays.push_back(*key);
                }
            }
            out.push_back(std::move(r));
        }
    }
    return out;
}

std::expected<std::vector<Bytes>, std::string> Session::landscape() {
    auto a = ask(Map{{"t", Value("landscape")}});
    if (!a) return std::unexpected(a.error());
    std::vector<Bytes> out;
    if (const Array* hosts = Fields{*a}.array("hosts")) {
        for (const auto& h : *hosts) {
            if (const Bytes* b = h.as_bin()) out.push_back(*b);
        }
    }
    return out;
}

std::expected<Bytes, std::string> Session::slots() {
    auto a = ask(Map{{"t", Value("slots")}});
    if (!a) return std::unexpected(a.error());
    auto bytes = Fields{*a}.bin("status");
    if (!bytes) return std::unexpected(std::string("malformed answer"));
    return *bytes;
}

std::expected<std::vector<ModuleRow>, std::string> Session::modules() {
    auto a = ask(Map{{"t", Value("modules")}});
    if (!a) return std::unexpected(a.error());
    std::vector<ModuleRow> out;
    if (const Array* modules = Fields{*a}.array("modules")) {
        for (const auto& m : *modules) {
            if (m.as_map() == nullptr) continue;
            const Fields e{*m.as_map()};
            ModuleRow r;
            r.hash = e.str("hash").value_or("");
            r.size = e.integer("size").value_or(0);
            r.users = e.integer("users").value_or(0);
            r.pinned = flag(e, "pinned");
            r.names = e.strings("names").value_or(std::vector<std::string>{});
            r.interface = e.str("interface").value_or("");
            out.push_back(std::move(r));
        }
    }
    return out;
}

std::expected<std::string, std::string> Session::push_module(Bytes module) {
    auto a = ask(Map{{"t", Value("push-module")}, {"module", Value(std::move(module))}});
    if (!a) return std::unexpected(a.error());
    return Fields{*a}.str("hash").value_or("");
}

std::string new_paglet_id() {
    std::array<std::uint8_t, 16> b{};
    mesh::random_bytes(b);
    return paglets::to_hex(std::span<const std::uint8_t>(b));
}

std::expected<std::string, std::string> Session::launch(const Identity& identity, const paglets::Digest& module,
                                                        Bytes module_bytes, Bytes args, std::string id,
                                                        std::int64_t hours) {
    if (id.empty()) id = new_paglet_id();
    const std::int64_t now = mesh::unix_ms();
    auto passport = mesh::Passport::issue(identity.key, identity.ledger.mesh(), module, id, Value(), now - 60'000,
                                          now + hours * 3'600'000);
    if (!passport) return std::unexpected(passport.error());
    Map request{{"t", Value("launch")}, {"passport", Value(passport->encode())}, {"args", Value(std::move(args))}};
    if (!module_bytes.empty()) request.emplace_back("module", Value(std::move(module_bytes)));
    auto a = ask(std::move(request));
    if (!a) return std::unexpected(a.error());
    return Fields{*a}.str("paglet").value_or("");
}

std::expected<CallResult, std::string> Session::call(const std::string& paglet, const std::string& name,
                                                     Bytes payload, std::int64_t timeout_ms) {
    auto a = ask(Map{{"t", Value("call")},
                     {"paglet", Value(paglet)},
                     {"name", Value(name)},
                     {"payload", Value(std::move(payload))},
                     {"timeout_ms", Value(timeout_ms)}});
    if (!a) return std::unexpected(a.error());
    const Fields f{*a};
    return CallResult{static_cast<std::int32_t>(f.integer("status").value_or(0)), f.bin("payload").value_or(Bytes{})};
}

std::expected<void, std::string> Session::send(const std::string& paglet, const std::string& name, Bytes payload,
                                               std::int64_t priority) {
    auto a = ask(Map{{"t", Value("send")},
                     {"paglet", Value(paglet)},
                     {"name", Value(name)},
                     {"payload", Value(std::move(payload))},
                     {"priority", Value(priority)}});
    if (!a) return std::unexpected(a.error());
    return {};
}

std::expected<std::string, std::string> Session::describe_paglet(const std::string& paglet) {
    auto a = ask(Map{{"t", Value("describe")}, {"paglet", Value(paglet)}});
    if (!a) return std::unexpected(a.error());
    return Fields{*a}.str("interface").value_or("");
}

std::expected<std::string, std::string> Session::describe_module(const std::string& module_hash) {
    auto a = ask(Map{{"t", Value("describe")}, {"module", Value(module_hash)}});
    if (!a) return std::unexpected(a.error());
    return Fields{*a}.str("interface").value_or("");
}

std::expected<void, std::string> Session::dispose(const std::string& paglet) {
    auto a = ask(Map{{"t", Value("dispose")}, {"paglet", Value(paglet)}});
    if (!a) return std::unexpected(a.error());
    return {};
}

std::expected<void, std::string> Session::terminate(const std::string& paglet, const std::string& reason) {
    auto a = ask(Map{{"t", Value("terminate")}, {"paglet", Value(paglet)}, {"reason", Value(reason)}});
    if (!a) return std::unexpected(a.error());
    return {};
}

std::expected<Map, std::string> Session::dispatch(const std::string& paglet, const std::string& destination,
                                                  std::int64_t wait_ms) {
    return ask(Map{{"t", Value("dispatch")},
                   {"paglet", Value(paglet)},
                   {"destination", Value(destination)},
                   {"wait_ms", Value(wait_ms)}});
}

std::expected<Location, std::string> Session::locate(const std::string& paglet) {
    auto a = ask(Map{{"t", Value("locate")}, {"paglet", Value(paglet)}});
    if (!a) return std::unexpected(a.error());
    const Fields f{*a};
    Location l;
    if (auto k = f.fixed<32>("host")) l.host = *k;
    l.host_name = f.str("host_name").value_or("");
    l.moves = f.integer("moves").value_or(0);
    return l;
}

std::expected<Map, std::string> Session::pin(const std::string& paglet, std::int64_t minutes,
                                             const std::string& reason) {
    return ask(Map{{"t", Value("pin")},
                   {"paglet", Value(paglet)},
                   {"duration_ms", Value(minutes * 60'000)},
                   {"reason", Value(reason)}});
}

// -- shell commands ------------------------------------------------------------

std::string shell_quote(const std::string& arg) {
    if (arg.empty()) return "''";
    bool plain = true;
    for (char c : arg) {
        const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                        c == '-' || c == '_' || c == '.' || c == '/' || c == ':' || c == '=' || c == '@' ||
                        c == ',' || c == '+' || c == '%';
        if (!ok) {
            plain = false;
            break;
        }
    }
    if (plain) return arg;
    std::string out = "'";
    for (char c : arg) {
        if (c == '\'') {
            out += "'\\''";
        } else {
            out += c;
        }
    }
    return out + "'";
}

std::string remote_command(const ShellContext& context, const std::string& action,
                           const std::vector<std::string>& args) {
    std::string out = context.program + " remote " + action;
    out += " --connect " + shell_quote(context.connect);
    out += " --key " + shell_quote(context.key_file);
    out += " --ledger " + shell_quote(context.ledger_dir);
    if (context.host_key) out += " --host-key " + shell_quote(*context.host_key);
    if (context.passphrase_file) out += " --passphrase-file " + shell_quote(*context.passphrase_file);
    for (const auto& a : args) out += " " + shell_quote(a);
    return out;
}

}  // namespace paglets::client
