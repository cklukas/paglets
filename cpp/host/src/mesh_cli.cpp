// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Key and ledger commands of paglets-host (WP8). Until hosts talk to each
// other (WP12), they work on a local ledger directory.

#include "mesh_cli.hpp"

#include <paglets/abi.hpp>
#include <paglets/mesh/crypto.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/passport.hpp>
#include <paglets/net/transport.hpp>
#include <paglets/wire/json_msgpack.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wasm/engine.hpp>

#include <algorithm>
#include <fstream>
#include <chrono>
#include <format>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <termios.h>
#include <unistd.h>
#endif

namespace paglets::cli {

namespace {

using namespace paglets::mesh;
namespace fs = std::filesystem;

int usage() {
    std::cerr
        << "usage: paglets-host keys init --role admin|owner|host|signer --name NAME --out FILE\n"
           "                             [--passphrase-file FILE] [--kdf moderate|interactive]\n"
           "       paglets-host keys show FILE\n"
           "       paglets-host mesh create --name NAME --ledger DIR --admin KEY... [--admin-quorum N] [--quorum N]\n"
           "       paglets-host ledger show --ledger DIR [--ignored]\n"
           "       paglets-host ledger request --ledger DIR --key KEY [--label L]...\n"
           "       paglets-host ledger approve --ledger DIR --admin KEY... REQUEST [--label L | --group G]...\n"
           "       paglets-host ledger deny --ledger DIR --admin KEY... REQUEST [--reason TEXT]\n"
           "       paglets-host ledger enroll --ledger DIR --admin KEY... host|owner KEY-ID NAME\n"
           "                                  [--label L | --group G]...\n"
           "       paglets-host ledger remove --ledger DIR --admin KEY... host|owner KEY-ID\n"
           "       paglets-host ledger revoke --ledger DIR --admin KEY... KEY-ID|RECORD-ID [--reason TEXT]\n"
           "       paglets-host ledger revoke --ledger DIR --admin KEY... --module MODULE [--reason TEXT]\n"
           "       paglets-host ledger admins --ledger DIR --admin KEY... [--add KEY-ID]... [--remove KEY-ID]...\n"
           "                                  [--admin-quorum N] [--quorum N]\n"
           "       paglets-host ledger sign --ledger DIR --admin KEY... RECORD-ID\n"
           "       paglets-host ledger rule --ledger DIR --admin KEY... --name NAME --decision allow|ask|deny\n"
           "                                --service SERVICE --op OP... [--owner KEY-ID]... [--group G]...\n"
           "                                [--module HASH]... [--signer KEY-ID]... [--trust T]...\n"
           "                                [--host-label L]... [--root R]... [--path PATTERN]...\n"
           "                                [--max-duration MS] [--priority N]\n"
           "       paglets-host ledger audit --ledger DIR\n"
           "       paglets-host ledger sign-module --ledger DIR --key SIGNER-KEY --name NAME [--version V] MODULE\n"
           "       paglets-host ledger trust --ledger DIR --admin KEY... --name NAME --class C...\n"
           "                                 [--signer KEY-ID]... [--module MODULE]...\n"
           "       paglets-host ledger module-policy --ledger DIR --admin KEY... any|trusted\n"
           "       paglets-host remote status --connect URL --key KEY --ledger DIR [--host-key KEY-ID]\n"
           "       paglets-host remote push --connect URL --key KEY --ledger DIR\n"
           "       paglets-host remote launch --connect URL --key OWNER-KEY --ledger DIR MODULE.wasm\n"
           "                                  [--args JSON] [--id PAGLET-ID] [--hours N]\n"
           "       paglets-host remote call --connect URL --key KEY --ledger DIR PAGLET NAME [JSON] [--expect TEXT]\n"
           "       paglets-host remote dispatch --connect URL --key KEY --ledger DIR PAGLET DESTINATION\n"
           "       paglets-host remote hosts --connect URL --key KEY --ledger DIR\n"
           "       paglets-host remote locate --connect URL --key KEY --ledger DIR PAGLET\n"
           "       paglets-host remote pin --connect URL --key KEY --ledger DIR PAGLET [--minutes N] [--reason TEXT]\n"
           "       paglets-host remote pins --connect URL --key KEY --ledger DIR\n"
           "       paglets-host remote unpin --connect URL --key ADMIN-KEY --ledger DIR PAGLET [--pin ID]\n"
           "MODULE is a .wasm file or a module hash. Trust classes (--class) are roaming, resident and\n"
           "system; 'module-policy trusted' makes roaming modules need trust as well.\n"
           "remote: a session with a host over an end-to-end channel, as the admin or owner of KEY; the\n"
           "host must be enrolled in the ledger copy (or be --host-key). push sends the records of the\n"
           "ledger copy, launch starts a paglet of the owner on the host (its passport is signed here),\n"
           "call sends a request to a paglet of the owner, dispatch moves it (a transfer ticket).\n"
           "hosts lists the enrolled hosts as the host knows them (address, online, versions);\n"
           "locate finds a paglet anywhere in the mesh, pin keeps it where it is (default 10 minutes),\n"
           "pins lists the pins on the host, unpin ends pins wherever the paglet is (admins).\n"
           "Requests are enrollment or grant requests; approving a grant request grants each item\n"
           "on every host (--host-only: on the requesting host).\n"
           "KEY is a key file. Passphrases are read from the terminal, or from the first line of\n"
           "--passphrase-file (used for every key of the command). IDs may be abbreviated to a unique\n"
           "prefix of at least 8 hex digits where the ledger knows them.\n";
    return 2;
}

int fail(const std::string& message) {
    std::cerr << "paglets-host: " << message << "\n";
    return 1;
}

struct Options {
    std::vector<std::string> positional;
    std::vector<std::string> admins;
    std::vector<std::string> labels;
    std::vector<std::string> groups;
    std::vector<std::string> add;
    std::vector<std::string> remove;
    std::vector<std::string> ops, owners, modules, trust, host_labels, roots, paths, classes, signers;
    std::optional<std::string> role, name, out, ledger, key, passphrase_file, reason, kdf, decision, service, version;
    std::optional<std::string> connect, host_key, args, id, expect, pin;
    std::optional<std::int64_t> hours, minutes;
    std::optional<std::int64_t> admin_quorum, quorum, max_duration, priority;
    bool ignored = false;
    bool host_only = false;
};

std::optional<Options> parse(int first, int argc, char** argv) {
    Options o;
    for (int i = first; i < argc; ++i) {
        const std::string_view a = argv[i];
        auto next = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc) return std::nullopt;
            return std::string(argv[++i]);
        };
        auto set = [&](std::optional<std::string>& field) {
            field = next();
            return field.has_value();
        };
        auto append = [&](std::vector<std::string>& list) {
            auto v = next();
            if (v) list.push_back(*v);
            return v.has_value();
        };
        auto number = [&](std::optional<std::int64_t>& field) {
            auto v = next();
            if (!v) return false;
            try {
                field = std::stoll(*v);
            } catch (const std::exception&) {
                return false;
            }
            return *field > 0;
        };
        bool ok = true;
        if (a == "--role") {
            ok = set(o.role);
        } else if (a == "--name") {
            ok = set(o.name);
        } else if (a == "--out") {
            ok = set(o.out);
        } else if (a == "--ledger") {
            ok = set(o.ledger);
        } else if (a == "--key") {
            ok = set(o.key);
        } else if (a == "--passphrase-file") {
            ok = set(o.passphrase_file);
        } else if (a == "--reason") {
            ok = set(o.reason);
        } else if (a == "--kdf") {
            ok = set(o.kdf);
        } else if (a == "--admin") {
            ok = append(o.admins);
        } else if (a == "--label") {
            ok = append(o.labels);
        } else if (a == "--group") {
            ok = append(o.groups);
        } else if (a == "--add") {
            ok = append(o.add);
        } else if (a == "--remove") {
            ok = append(o.remove);
        } else if (a == "--admin-quorum") {
            ok = number(o.admin_quorum);
        } else if (a == "--quorum") {
            ok = number(o.quorum);
        } else if (a == "--ignored") {
            o.ignored = true;
        } else if (a == "--host-only") {
            o.host_only = true;
        } else if (a == "--decision") {
            ok = set(o.decision);
        } else if (a == "--service") {
            ok = set(o.service);
        } else if (a == "--op") {
            ok = append(o.ops);
        } else if (a == "--owner") {
            ok = append(o.owners);
        } else if (a == "--module") {
            ok = append(o.modules);
        } else if (a == "--trust") {
            ok = append(o.trust);
        } else if (a == "--class") {
            ok = append(o.classes);
        } else if (a == "--signer") {
            ok = append(o.signers);
        } else if (a == "--version") {
            ok = set(o.version);
        } else if (a == "--connect") {
            ok = set(o.connect);
        } else if (a == "--host-key") {
            ok = set(o.host_key);
        } else if (a == "--args") {
            ok = set(o.args);
        } else if (a == "--id") {
            ok = set(o.id);
        } else if (a == "--expect") {
            ok = set(o.expect);
        } else if (a == "--pin") {
            ok = set(o.pin);
        } else if (a == "--minutes") {
            ok = number(o.minutes);
        } else if (a == "--hours") {
            ok = number(o.hours);
        } else if (a == "--host-label") {
            ok = append(o.host_labels);
        } else if (a == "--root") {
            ok = append(o.roots);
        } else if (a == "--path") {
            ok = append(o.paths);
        } else if (a == "--max-duration") {
            ok = number(o.max_duration);
        } else if (a == "--priority") {
            auto v = next();
            try {
                ok = v.has_value();
                if (ok) o.priority = std::stoll(*v);
            } catch (const std::exception&) {
                ok = false;
            }
        } else if (a.starts_with("--")) {
            ok = false;
        } else {
            o.positional.emplace_back(a);
        }
        if (!ok) return std::nullopt;
    }
    return o;
}

// -- passphrases --------------------------------------------------------------

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

struct Passphrases {
    std::optional<std::string> from_file;

    static std::expected<Passphrases, std::string> make(const Options& o) {
        Passphrases p;
        if (o.passphrase_file) {
            std::ifstream in(*o.passphrase_file);
            std::string line;
            if (!in || !std::getline(in, line)) return std::unexpected("cannot read " + *o.passphrase_file);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            p.from_file = line;
        }
        return p;
    }

    std::optional<std::string> get(const std::string& prompt) const {
        if (from_file) return from_file;
        return read_hidden(prompt);
    }
};

std::expected<SigningKey, std::string> open_key(const std::string& path, const Passphrases& passphrases) {
    auto info = read_key_info(path);
    if (!info) return std::unexpected(info.error());
    std::optional<std::string> passphrase;
    if (info->encrypted) {
        passphrase = passphrases.get("passphrase for " + std::string(to_string(info->role)) + " key '" + info->name +
                                     "' (" + path + "): ");
        if (!passphrase) return std::unexpected(std::string("no passphrase given"));
    }
    return load_key(path, passphrase);
}

// -- IDs -----------------------------------------------------------------------

std::optional<Digest> parse_hex32(std::string_view text) {
    return parse_key_id(text);
}

// Resolves a full ID, or a unique prefix among `known`.
std::expected<Digest, std::string> resolve(std::string_view text, const std::vector<Digest>& known,
                                           std::string_view what) {
    if (auto full = parse_hex32(text)) return *full;
    if (text.size() < 8) return std::unexpected(std::string(what) + " IDs need at least 8 hex digits");
    std::optional<Digest> match;
    for (const auto& id : known) {
        if (to_hex(id).starts_with(text)) {
            if (match && *match != id)
                return std::unexpected("ambiguous " + std::string(what) + " ID " + std::string(text));
            match = id;
        }
    }
    if (!match) return std::unexpected("unknown " + std::string(what) + " " + std::string(text));
    return *match;
}

std::string short_id(const Digest& id) {
    return to_hex(id).substr(0, 16);
}

std::string describe(const Item& item) {
    std::string out = item.service + " " + (item.ops.empty() ? std::string("-") : "");
    for (std::size_t i = 0; i < item.ops.size(); ++i) out += (i == 0 ? "" : ",") + item.ops[i];
    if (!item.root.empty()) out += " " + item.root + (item.path.empty() ? "" : "/" + item.path);
    return out;
}

std::string joined(const std::vector<std::string>& list) {
    std::string out;
    for (const auto& s : list) out += (out.empty() ? "" : ",") + s;
    return out.empty() ? "-" : out;
}

// -- keys ----------------------------------------------------------------------

// A module given as a .wasm file (hashed) or as its hash.
std::expected<Digest, std::string> module_hash(const std::string& text) {
    if (auto h = parse_hex32(text)) return *h;
    std::error_code ec;
    if (!fs::is_regular_file(text, ec)) return std::unexpected("not a module file or hash: " + text);
    auto bytes = paglets::wasm::read_file(text);
    if (!bytes) return std::unexpected(bytes.error());
    return paglets::sha256(*bytes);
}

std::expected<std::vector<PublicKey>, std::string> key_ids(const std::vector<std::string>& texts) {
    std::vector<PublicKey> out;
    for (const auto& text : texts) {
        auto key = parse_key_id(text);
        if (!key) return std::unexpected("a key is given by its key ID (64 hex digits): " + text);
        out.push_back(*key);
    }
    return out;
}

int keys_init(const Options& o) {
    if (!o.role || !o.name || !o.out || !o.positional.empty()) return usage();
    auto role = parse_key_role(*o.role);
    if (!role) return fail("unknown role " + *o.role);
    if (o.kdf && *o.kdf != "moderate" && *o.kdf != "interactive") return fail("unknown --kdf " + *o.kdf);
    auto passphrases = Passphrases::make(o);
    if (!passphrases) return fail(passphrases.error());
    std::optional<std::string> passphrase;
    if (*role != KeyRole::host || o.passphrase_file) {
        if (passphrases->from_file) {
            passphrase = passphrases->from_file;
        } else {
            passphrase = read_hidden("new passphrase: ");
            auto again = read_hidden("repeat passphrase: ");
            if (!passphrase || !again || *passphrase != *again) return fail("the passphrases differ");
        }
        if (!passphrase || passphrase->empty()) return fail("an empty passphrase is not allowed");
    }
    const SigningKey key = SigningKey::generate();
    if (auto saved = save_key(*o.out, key, *role, *o.name, passphrase, o.kdf && *o.kdf == "interactive"); !saved) {
        return fail(saved.error());
    }
    std::cout << key.id() << "\n";
    return 0;
}

int keys_show(const Options& o) {
    if (o.positional.size() != 1) return usage();
    auto info = read_key_info(o.positional[0]);
    if (!info) return fail(info.error());
    std::cout << "key:       " << key_id(info->public_key) << "\n"
              << "role:      " << to_string(info->role) << "\n"
              << "name:      " << info->name << "\n"
              << "encrypted: " << (info->encrypted ? "yes" : "no") << "\n";
    return 0;
}

// -- ledger --------------------------------------------------------------------

int mesh_create(const Options& o) {
    if (!o.name || !o.ledger || o.admins.empty() || !o.positional.empty()) return usage();
    std::error_code ec;
    if (fs::exists(*o.ledger, ec) && !fs::is_empty(*o.ledger, ec)) return fail(*o.ledger + " is not empty");
    auto passphrases = Passphrases::make(o);
    if (!passphrases) return fail(passphrases.error());
    std::vector<SigningKey> keys;
    for (const auto& path : o.admins) {
        auto info = read_key_info(path);
        if (!info) return fail(info.error());
        if (info->role != KeyRole::admin) return fail(path + " is not an admin key");
        auto key = open_key(path, *passphrases);
        if (!key) return fail(key.error());
        keys.push_back(std::move(*key));
    }
    std::vector<const SigningKey*> signers;
    for (const auto& k : keys) signers.push_back(&k);
    const Quorum quorum{o.admin_quorum.value_or(1), o.quorum.value_or(1)};
    auto genesis = make_genesis(*o.name, signers, quorum, unix_ms());
    if (!genesis) return fail(genesis.error());
    auto ledger = Ledger::create(*genesis, *o.ledger);
    if (!ledger) return fail(ledger.error());
    std::cout << record_id_hex(ledger->mesh()) << "\n";
    return 0;
}

std::expected<Ledger, std::string> open_ledger(const Options& o) {
    if (!o.ledger) return std::unexpected(std::string("--ledger is required"));
    return Ledger::open(*o.ledger);
}

int ledger_show(const Options& o) {
    auto ledger = open_ledger(o);
    if (!ledger) return fail(ledger.error());
    const LedgerState& st = ledger->state();
    std::cout << "mesh:    " << st.mesh_name << " " << record_id_hex(st.mesh) << "\n"
              << "records: " << st.records << " (clock " << st.clock << ")\n"
              << "epoch:   " << short_id(st.epoch) << "\n"
              << "quorum:  " << st.quorum.admin_set << " for admin changes, " << st.quorum.normal << " otherwise\n"
              << "state:   " << short_id(st.digest()) << "\n";
    std::cout << "admins:\n";
    for (const auto& k : st.admins) std::cout << "  " << key_id(k) << "\n";
    std::cout << "hosts:\n";
    for (const auto& [k, h] : st.hosts)
        std::cout << "  " << key_id(k) << "  " << h.name << "  " << joined(h.labels) << "\n";
    std::cout << "owners:\n";
    for (const auto& [k, w] : st.owners)
        std::cout << "  " << key_id(k) << "  " << w.name << "  " << joined(w.groups) << "\n";
    std::cout << "pending requests:\n";
    for (const auto& p : st.pending) {
        std::cout << "  " << short_id(p.id) << "  " << (p.kind == RequestKind::host ? "host " : "owner") << "  "
                  << p.name << "  " << key_id(p.key) << "\n";
    }
    std::cout << "rules:\n";
    for (const auto& r : st.rules) {
        std::cout << "  " << short_id(r.id) << "  " << to_string(r.decision) << "  " << r.service << "  "
                  << joined(r.ops) << "  " << r.name << "\n";
    }
    std::cout << "pending grant requests:\n";
    for (const auto& g : st.grant_requests) {
        for (const auto& item : g.items) {
            std::cout << "  " << short_id(g.id) << "  paglet " << g.principal.paglet.substr(0, 8) << "  "
                      << describe(item) << "  " << g.reason << "\n";
        }
    }
    std::cout << "grants:\n";
    for (const auto& [id, g] : st.grants) {
        std::cout << "  " << short_id(id) << "  paglet " << g.principal.paglet.substr(0, 8) << "  " << describe(g.item)
                  << "  " << (g.by_admin ? "approved" : "by rule") << "\n";
    }
    std::cout << "module trust (roaming modules: " << to_string(st.roaming_modules) << "):\n";
    for (const auto& t : st.module_trust) {
        std::cout << "  " << short_id(t.id) << "  " << joined(t.classes) << "  " << t.name;
        for (const auto& k : t.signers) std::cout << "  signer " << key_id(k).substr(0, 16);
        for (const auto& m : t.modules) std::cout << "  module " << paglets::to_hex(m).substr(0, 16);
        std::cout << "\n";
    }
    std::cout << "module signatures:\n";
    for (const auto& m : st.module_signatures) {
        std::cout << "  " << paglets::to_hex(m.module).substr(0, 16) << "  " << m.name
                  << (m.version.empty() ? "" : " " + m.version) << "  signer " << key_id(m.signer).substr(0, 16)
                  << "\n";
    }
    if (!st.revoked_keys.empty() || !st.revoked_records.empty() || !st.revoked_modules.empty()) {
        std::cout << "revoked:\n";
        for (const auto& k : st.revoked_keys) std::cout << "  key    " << key_id(k) << "\n";
        for (const auto& r : st.revoked_records) std::cout << "  record " << record_id_hex(r) << "\n";
        for (const auto& m : st.revoked_modules) std::cout << "  module " << paglets::to_hex(m) << "\n";
    }
    if (o.ignored) {
        std::cout << "records without effect:\n";
        for (const auto& i : st.ignored)
            std::cout << "  " << short_id(i.id) << "  " << i.type << ": " << i.reason << "\n";
    } else if (!st.ignored.empty()) {
        std::cout << "records without effect: " << st.ignored.size() << " (--ignored lists them)\n";
    }
    return 0;
}

// Opens the admin keys of a command and signs, adds and reports a record.
struct AdminSession {
    Ledger ledger;
    std::vector<SigningKey> keys;

    static std::expected<AdminSession, std::string> open(const Options& o) {
        if (o.admins.empty()) return std::unexpected(std::string("--admin is required"));
        auto ledger = open_ledger(o);
        if (!ledger) return std::unexpected(ledger.error());
        auto passphrases = Passphrases::make(o);
        if (!passphrases) return std::unexpected(passphrases.error());
        AdminSession s{std::move(*ledger), {}};
        for (const auto& path : o.admins) {
            auto key = open_key(path, *passphrases);
            if (!key) return std::unexpected(key.error());
            if (!s.ledger.state().is_admin(key->public_key())) {
                return std::unexpected(path + " is not an admin of this mesh");
            }
            s.keys.push_back(std::move(*key));
        }
        return s;
    }

    int issue(const std::string& type, Map data) {
        auto r = ledger.draft(type, std::move(data));
        if (!r) return fail(r.error());
        for (const auto& k : keys) r->sign(k);
        if (auto added = ledger.add(*r); !added) return fail(added.error());
        report(*r);
        return 0;
    }

    void report(const Record& r) const {
        std::cout << record_id_hex(r.id()) << "\n";
        for (const auto& i : ledger.state().ignored) {
            if (i.id == r.id()) std::cerr << "paglets-host: note: the record has no effect yet: " << i.reason << "\n";
        }
    }

    std::vector<Digest> request_ids() const {
        std::vector<Digest> ids;
        for (const auto& p : ledger.state().pending) ids.push_back(p.id);
        for (const auto& g : ledger.state().grant_requests) ids.push_back(g.id);
        return ids;
    }
    std::vector<Digest> record_ids() const {
        std::vector<Digest> ids;
        for (const Record* r : ledger.records()) ids.push_back(r->id());
        return ids;
    }
    std::vector<PublicKey> known_keys() const {
        const LedgerState& st = ledger.state();
        std::vector<PublicKey> keys_out(st.admins.begin(), st.admins.end());
        for (const auto& [k, h] : st.hosts) keys_out.push_back(k);
        for (const auto& [k, w] : st.owners) keys_out.push_back(k);
        for (const auto& p : st.pending) keys_out.push_back(p.key);
        return keys_out;
    }
};

int ledger_request(const Options& o) {
    if (!o.key || !o.positional.empty()) return usage();
    auto ledger = open_ledger(o);
    if (!ledger) return fail(ledger.error());
    auto passphrases = Passphrases::make(o);
    if (!passphrases) return fail(passphrases.error());
    auto info = read_key_info(*o.key);
    if (!info) return fail(info.error());
    if (info->role == KeyRole::admin) return fail("admins are added with 'ledger admins', not by request");
    if (info->role == KeyRole::signer) return fail("signer keys are not enrolled; admins trust them ('ledger trust')");
    auto key = open_key(*o.key, *passphrases);
    if (!key) return fail(key.error());
    const bool host = info->role == KeyRole::host;
    auto r = ledger->draft(host ? "host-enroll-request" : "owner-enroll-request",
                           host ? data::host_enroll_request(key->public_key(), info->name, o.labels)
                                : data::owner_enroll_request(key->public_key(), info->name),
                           false);
    if (!r) return fail(r.error());
    r->sign(*key);
    if (auto added = ledger->add(*r); !added) return fail(added.error());
    std::cout << record_id_hex(r->id()) << "\n";
    return 0;
}

int ledger_approve(const Options& o) {
    if (o.positional.size() != 1) return usage();
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    auto id = resolve(o.positional[0], s->request_ids(), "request");
    if (!id) return fail(id.error());
    const LedgerState& st = s->ledger.state();
    if (auto g = std::ranges::find(st.grant_requests, *id, &GrantRequest::id); g != st.grant_requests.end()) {
        // A grant for each item, valid for the requested duration.
        const GrantRequest request = *g;
        HostSelector hosts;
        if (o.host_only && !request.by_owner) hosts.keys.push_back(request.host);
        const std::int64_t expires = unix_ms() + request.duration_ms;
        for (const auto& item : request.items) {
            if (int rc = s->issue("grant", data::grant(request.principal, item, hosts, expires, request.id,
                                                       std::nullopt, std::nullopt));
                rc != 0) {
                return rc;
            }
        }
        return 0;
    }
    auto it = std::ranges::find(st.pending, *id, &PendingRequest::id);
    if (it == st.pending.end()) return fail("no pending request " + o.positional[0]);
    const PendingRequest request = *it;
    if (request.kind == RequestKind::host) {
        return s->issue("host-enroll", data::host_enroll(request.key, request.name,
                                                         o.labels.empty() ? request.labels : o.labels, request.id));
    }
    return s->issue("owner-enroll", data::owner_enroll(request.key, request.name, o.groups, request.id));
}

int ledger_deny(const Options& o) {
    if (o.positional.size() != 1) return usage();
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    auto id = resolve(o.positional[0], s->request_ids(), "request");
    if (!id) return fail(id.error());
    return s->issue("request-deny", data::request_deny(*id, o.reason.value_or("")));
}

int ledger_enroll(const Options& o) {
    if (o.positional.size() != 3 || (o.positional[0] != "host" && o.positional[0] != "owner")) return usage();
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    auto key = parse_key_id(o.positional[1]);
    if (!key) return fail("a key ID has 64 hex digits");
    if (o.positional[0] == "host") return s->issue("host-enroll", data::host_enroll(*key, o.positional[2], o.labels));
    return s->issue("owner-enroll", data::owner_enroll(*key, o.positional[2], o.groups));
}

int ledger_remove(const Options& o) {
    if (o.positional.size() != 2 || (o.positional[0] != "host" && o.positional[0] != "owner")) return usage();
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    auto key = resolve(o.positional[1], s->known_keys(), "key");
    if (!key) return fail(key.error());
    return s->issue(o.positional[0] + "-remove", data::key_removal(*key));
}

int ledger_revoke(const Options& o) {
    if (o.modules.size() == 1 && o.positional.empty()) {
        auto module = module_hash(o.modules[0]);
        if (!module) return fail(module.error());
        auto s = AdminSession::open(o);
        if (!s) return fail(s.error());
        return s->issue("revoke", data::revoke_module(*module, o.reason.value_or("")));
    }
    if (o.positional.size() != 1 || !o.modules.empty()) return usage();
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    const std::string& text = o.positional[0];
    auto key = resolve(text, s->known_keys(), "key");
    auto record = resolve(text, s->record_ids(), "record");
    const std::string reason = o.reason.value_or("");
    if (key && record && *key != *record) return fail("ambiguous ID " + text + ": a key and a record match");
    if (key) {
        if (s->ledger.state().is_admin(*key)) return fail("admins are removed with 'ledger admins --remove'");
        if (s->ledger.find(*key) != nullptr) return s->issue("revoke", data::revoke_record(*key, reason));
        return s->issue("revoke", data::revoke_key(*key, reason));
    }
    if (record) return s->issue("revoke", data::revoke_record(*record, reason));
    return fail(key.error());
}

int ledger_admins(const Options& o) {
    if (!o.positional.empty() || (o.add.empty() && o.remove.empty() && !o.admin_quorum && !o.quorum)) return usage();
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    std::vector<PublicKey> add;
    std::vector<PublicKey> remove;
    for (const auto& text : o.add) {
        auto key = parse_key_id(text);
        if (!key) return fail("a key ID has 64 hex digits: " + text);
        add.push_back(*key);
    }
    const LedgerState& st = s->ledger.state();
    const std::vector<PublicKey> admins(st.admins.begin(), st.admins.end());
    for (const auto& text : o.remove) {
        auto key = resolve(text, admins, "admin");
        if (!key) return fail(key.error());
        remove.push_back(*key);
    }
    std::optional<Quorum> quorum;
    if (o.admin_quorum || o.quorum) {
        quorum = Quorum{o.admin_quorum.value_or(st.quorum.admin_set), o.quorum.value_or(st.quorum.normal)};
    }
    return s->issue("admin-set", data::admin_set(s->ledger, add, remove, quorum));
}

int ledger_rule(const Options& o) {
    if (!o.positional.empty() || !o.name || !o.decision || !o.service || o.ops.empty()) return usage();
    auto decision = parse_decision(*o.decision);
    if (!decision) return fail("the decision is allow, ask or deny");
    data::RuleSpec spec;
    spec.name = *o.name;
    spec.decision = *decision;
    spec.service = *o.service;
    spec.ops = o.ops;
    if (!o.owners.empty()) {
        std::vector<PublicKey> owners;
        for (const auto& text : o.owners) {
            auto key = parse_key_id(text);
            if (!key) return fail("an owner is given by its key ID (64 hex digits): " + text);
            owners.push_back(*key);
        }
        spec.match.owners = std::move(owners);
    }
    if (!o.groups.empty()) spec.match.groups = o.groups;
    if (!o.modules.empty()) spec.match.modules = o.modules;
    if (!o.signers.empty()) {
        auto signers = key_ids(o.signers);
        if (!signers) return fail(signers.error());
        spec.match.signers = std::move(*signers);
    }
    if (!o.trust.empty()) spec.match.trust = o.trust;
    if (!o.host_labels.empty()) spec.match.hosts = HostSelector{{}, o.host_labels};
    if (!o.roots.empty() || !o.paths.empty()) {
        Scope scope;
        if (!o.roots.empty()) scope.roots = o.roots;
        if (!o.paths.empty()) scope.paths = o.paths;
        spec.scope = std::move(scope);
    }
    spec.max_duration_ms = o.max_duration;
    spec.priority = o.priority.value_or(0);
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    return s->issue("policy-rule", data::policy_rule(spec));
}

int ledger_sign_module(const Options& o) {
    if (o.positional.size() != 1 || !o.key || !o.name) return usage();
    auto module = module_hash(o.positional[0]);
    if (!module) return fail(module.error());
    auto ledger = open_ledger(o);
    if (!ledger) return fail(ledger.error());
    auto info = read_key_info(*o.key);
    if (!info) return fail(info.error());
    if (info->role == KeyRole::host) return fail("host keys do not sign modules");
    auto passphrases = Passphrases::make(o);
    if (!passphrases) return fail(passphrases.error());
    auto key = open_key(*o.key, *passphrases);
    if (!key) return fail(key.error());
    auto r = ledger->draft("module-sign",
                           data::module_sign(*module, key->public_key(), *o.name, o.version.value_or("")), false);
    if (!r) return fail(r.error());
    r->sign(*key);
    if (auto added = ledger->add(*r); !added) return fail(added.error());
    std::cout << record_id_hex(r->id()) << "\n";
    return 0;
}

int ledger_trust(const Options& o) {
    if (!o.positional.empty() || !o.name || o.classes.empty() || (o.signers.empty() && o.modules.empty())) {
        return usage();
    }
    for (const auto& c : o.classes) {
        if (c != "roaming" && c != "resident" && c != "system") return fail("unknown trust class " + c);
    }
    auto signers = key_ids(o.signers);
    if (!signers) return fail(signers.error());
    std::vector<Digest> modules;
    for (const auto& text : o.modules) {
        auto module = module_hash(text);
        if (!module) return fail(module.error());
        modules.push_back(*module);
    }
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    return s->issue("module-trust", data::module_trust(*o.name, *signers, modules, o.classes));
}

int ledger_module_policy(const Options& o) {
    if (o.positional.size() != 1) return usage();
    auto roaming = parse_roaming_modules(o.positional[0]);
    if (!roaming) return fail("the module policy is 'any' or 'trusted'");
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    return s->issue("module-policy", data::module_policy(*roaming));
}

int ledger_audit(const Options& o) {
    auto ledger = open_ledger(o);
    if (!ledger) return fail(ledger.error());
    const LedgerState& st = ledger->state();
    for (const auto& e : st.audit) {
        auto host = st.hosts.find(e.host);
        std::cout << e.time << "  " << (host == st.hosts.end() ? key_id(e.host).substr(0, 8) : host->second.name)
                  << "  paglet " << e.principal.paglet.substr(0, 8) << "  owner " << e.principal.owner.substr(0, 8)
                  << "  " << to_string(e.decision) << "  " << describe(e.item);
        if (e.rule) std::cout << "  rule " << short_id(*e.rule);
        if (e.grant) std::cout << "  grant " << short_id(*e.grant);
        if (e.request) std::cout << "  request " << short_id(*e.request);
        std::cout << "\n";
    }
    return 0;
}

int ledger_sign(const Options& o) {
    if (o.positional.size() != 1) return usage();
    auto s = AdminSession::open(o);
    if (!s) return fail(s.error());
    auto id = resolve(o.positional[0], s->record_ids(), "record");
    if (!id) return fail(id.error());
    Record r = *s->ledger.find(*id);
    for (const auto& k : s->keys) r.sign(k);
    auto added = s->ledger.add(r);
    if (!added) return fail(added.error());
    if (*added == AddResult::known) return fail("the record already carries these signatures");
    s->report(r);
    return 0;
}

// -- remote: CLI sessions with a host ----------------------------------------

struct Remote {
    Ledger ledger;
    SigningKey key;
    net::ClientSession session;

    static std::expected<Remote, std::string> open(const Options& o) {
        if (!o.connect || !o.key) return std::unexpected(std::string("--connect and --key are required"));
        auto ledger = open_ledger(o);
        if (!ledger) return std::unexpected(ledger.error());
        auto info = read_key_info(*o.key);
        if (!info) return std::unexpected(info.error());
        net::PeerRole role = net::PeerRole::owner;
        if (info->role == KeyRole::admin) {
            role = net::PeerRole::admin;
        } else if (info->role != KeyRole::owner) {
            return std::unexpected(std::string("sessions use an admin or owner key"));
        }
        auto passphrases = Passphrases::make(o);
        if (!passphrases) return std::unexpected(passphrases.error());
        auto key = open_key(*o.key, *passphrases);
        if (!key) return std::unexpected(key.error());
        std::optional<PublicKey> expected;
        if (o.host_key) {
            expected = parse_key_id(*o.host_key);
            if (!expected) return std::unexpected(std::string("--host-key takes a key ID (64 hex digits)"));
        }
        const LedgerState state = ledger->state();
        auto accept = [expected, state](const net::Identity& server) -> std::expected<void, std::string> {
            if (server.role != net::PeerRole::host) return std::unexpected(std::string("the server is not a host"));
            if (expected ? server.key != *expected : !state.is_host(server.key)) {
                return std::unexpected("host " + key_id(server.key) + " is not enrolled in this ledger copy");
            }
            return {};
        };
        auto session = net::ClientSession::open(*o.connect, *key, role, ledger->mesh(), accept);
        if (!session) return std::unexpected(session.error());
        return Remote{std::move(*ledger), std::move(*key), std::move(*session)};
    }

    // One request, one answer; failures of the host are errors.
    std::expected<Map, std::string> ask(Map request) {
        auto answers = session.exchange({encode(Value(std::move(request)))});
        if (!answers) return std::unexpected(answers.error());
        if (answers->size() != 1) return std::unexpected(std::string("no answer"));
        auto v = decode((*answers)[0]);
        if (!v || v->as_map() == nullptr) return std::unexpected(std::string("malformed answer"));
        const Fields f{*v->as_map()};
        const Value* ok = f.get("ok");
        if (ok == nullptr || ok->as_bool() == nullptr || !*ok->as_bool()) {
            return std::unexpected(f.str("e").value_or("refused"));
        }
        return *v->as_map();
    }
};

std::expected<Bytes, std::string> json_body(const std::optional<std::string>& json) {
    if (!json) return Bytes{};
    auto parsed = paglets::wire::Json::parse(*json);
    if (!parsed) return std::unexpected("not valid JSON: " + *json);
    return paglets::wire::json_to_msgpack(*parsed);
}

int remote_status(const Options& o) {
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    auto a = r->ask(Map{{"t", Value("status")}});
    if (!a) return fail(a.error());
    const Fields f{*a};
    std::cout << "host:    " << f.str("name").value_or("") << " " << key_id(*f.fixed<32>("key")) << "\n"
              << "mesh:    " << f.str("mesh").value_or("") << "\n"
              << "ledger:  " << f.integer("records").value_or(0) << " records, state "
              << short_id(r->ledger.state().digest()) << " here\n"
              << "paglets:\n";
    if (const Array* paglets = f.array("paglets")) {
        for (const auto& p : *paglets) {
            const Array* e = p.as_array();
            if (e == nullptr || e->size() != 4) continue;
            std::cout << "  " << *(*e)[0].as_str() << "  " << *(*e)[3].as_str() << "  module "
                      << (*e)[1].as_str()->substr(0, 16) << "  owner " << (*e)[2].as_str()->substr(0, 16) << "\n";
        }
    }
    return 0;
}

int remote_push(const Options& o) {
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    Array records;
    for (const Record* rec : r->ledger.records()) records.emplace_back(rec->encode());
    auto a = r->ask(Map{{"t", Value("push")}, {"records", Value(std::move(records))}});
    if (!a) return fail(a.error());
    const Fields f{*a};
    std::cout << f.integer("added").value_or(0) << " records new to the host\n";
    if (const Array* errors = f.array("errors")) {
        for (const auto& e : *errors) std::cerr << "paglets-host: " << *e.as_str() << "\n";
    }
    return 0;
}

int remote_launch(const Options& o) {
    if (o.positional.size() != 1) return usage();
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    auto module = paglets::wasm::read_file(o.positional[0]);
    if (!module) return fail(module.error());
    auto args = json_body(o.args);
    if (!args) return fail(args.error());
    std::string id = o.id.value_or("");
    if (id.empty()) {
        std::array<std::uint8_t, 16> b{};
        random_bytes(b);
        id = paglets::to_hex(std::span<const std::uint8_t>(b));
    }
    const std::int64_t now = unix_ms();
    auto passport = Passport::issue(r->key, r->ledger.mesh(), paglets::sha256(*module), id, Value(), now - 60'000,
                                    now + o.hours.value_or(24) * 3'600'000);
    if (!passport) return fail(passport.error());
    auto a = r->ask(Map{{"t", Value("launch")},
                        {"passport", Value(passport->encode())},
                        {"module", Value(std::move(*module))},
                        {"args", Value(std::move(*args))}});
    if (!a) return fail(a.error());
    std::cout << Fields{*a}.str("paglet").value_or("") << "\n";
    return 0;
}

int remote_call(const Options& o) {
    if (o.positional.size() < 2 || o.positional.size() > 3) return usage();
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    auto body = json_body(o.positional.size() == 3 ? std::optional<std::string>(o.positional[2]) : std::nullopt);
    if (!body) return fail(body.error());
    auto a = r->ask(Map{{"t", Value("call")},
                        {"paglet", Value(o.positional[0])},
                        {"name", Value(o.positional[1])},
                        {"payload", Value(std::move(*body))}});
    if (!a) return fail(a.error());
    const Fields f{*a};
    const auto status = static_cast<std::int32_t>(f.integer("status").value_or(0));
    std::string shown;
    if (status != 0) {
        shown = "error: " + std::string(paglets::abi::error_name(status));
    } else {
        const Bytes payload = f.bin("payload").value_or(Bytes{});
        auto json = payload.empty() ? std::nullopt : paglets::wire::msgpack_to_json(payload);
        shown = payload.empty() ? "ok" : json ? json->dump() : "(" + std::to_string(payload.size()) + " bytes)";
    }
    std::cout << shown << "\n";
    if (o.expect && shown.find(*o.expect) == std::string::npos) return fail("expected " + *o.expect);
    return status == 0 ? 0 : 1;
}

int remote_dispatch(const Options& o) {
    if (o.positional.size() != 2) return usage();
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    auto a = r->ask(
        Map{{"t", Value("dispatch")}, {"paglet", Value(o.positional[0])}, {"destination", Value(o.positional[1])}});
    if (!a) return fail(a.error());
    return 0;
}

// Unix milliseconds as UTC time.
std::string format_time(std::int64_t ms) {
    const std::chrono::sys_seconds t{std::chrono::seconds(ms / 1000)};
    return std::format("{:%Y-%m-%d %H:%M:%S} UTC", t);
}

// Where a paglet is, from the answer of locate, pin and unpin.
void print_location(const Fields& f) {
    std::cout << "host:  " << f.str("host_name").value_or("") << " " << key_id(*f.fixed<32>("host")) << "\n"
              << "moves: " << f.integer("moves").value_or(0) << "\n";
}

int remote_hosts(const Options& o) {
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    auto a = r->ask(Map{{"t", Value("hosts")}});
    if (!a) return fail(a.error());
    if (const Array* hosts = Fields{*a}.array("hosts")) {
        for (const auto& h : *hosts) {
            if (h.as_map() == nullptr) continue;
            const Fields f{*h.as_map()};
            auto flag = [&](std::string_view k) {
                const Value* v = f.get(k);
                return v != nullptr && v->as_bool() != nullptr && *v->as_bool();
            };
            std::cout << f.str("name").value_or("") << "  " << key_id(*f.fixed<32>("key")).substr(0, 16) << "  "
                      << (flag("self")     ? "this host"
                          : flag("online") ? "online"
                                           : "offline")
                      << "  " << (f.str("url").value_or("").empty() ? "(no address)" : f.str("url").value_or(""));
            if (f.integer("proto").value_or(0) != 0) {
                std::cout << "  protocol " << f.integer("proto").value_or(0) << " abi " << f.str("abi").value_or("");
            }
            if (const Array* relays = f.array("relays"); relays != nullptr && !relays->empty()) {
                std::cout << "  relays";
                for (const auto& k : *relays) {
                    if (const Bytes* b = k.as_bin(); b != nullptr && b->size() == 32) {
                        PublicKey key{};
                        std::copy(b->begin(), b->end(), key.begin());
                        std::cout << " " << key_id(key).substr(0, 8);
                    }
                }
            }
            if (!flag("compatible")) std::cout << "  INCOMPATIBLE";
            if (!f.str("via").value_or("").empty() && !flag("self")) std::cout << "  via " << f.str("via").value_or("");
            std::cout << "\n";
        }
    }
    return 0;
}

int remote_locate(const Options& o) {
    if (o.positional.size() != 1) return usage();
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    auto a = r->ask(Map{{"t", Value("locate")}, {"paglet", Value(o.positional[0])}});
    if (!a) return fail(a.error());
    print_location(Fields{*a});
    return 0;
}

int remote_pin(const Options& o) {
    if (o.positional.size() != 1) return usage();
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    auto a = r->ask(Map{{"t", Value("pin")},
                        {"paglet", Value(o.positional[0])},
                        {"duration_ms", Value(o.minutes.value_or(10) * 60'000)},
                        {"reason", Value(o.reason.value_or(""))}});
    if (!a) return fail(a.error());
    const Fields f{*a};
    print_location(f);
    std::cout << "pin:   " << f.str("pin").value_or("") << " until " << format_time(f.integer("until").value_or(0))
              << "\n";
    return 0;
}

int remote_pins(const Options& o) {
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    auto a = r->ask(Map{{"t", Value("pins")}});
    if (!a) return fail(a.error());
    if (const Array* pins = Fields{*a}.array("pins")) {
        for (const auto& p : *pins) {
            const Array* e = p.as_array();
            if (e == nullptr || e->size() != 5) continue;
            std::cout << *(*e)[0].as_str() << "  paglet " << *(*e)[1].as_str() << "  until "
                      << format_time(*(*e)[2].as_int()) << "  by " << *(*e)[3].as_str();
            if (!(*e)[4].as_str()->empty()) std::cout << "  (" << *(*e)[4].as_str() << ")";
            std::cout << "\n";
        }
    }
    return 0;
}

int remote_unpin(const Options& o) {
    if (o.positional.size() != 1) return usage();
    auto r = Remote::open(o);
    if (!r) return fail(r.error());
    Map request{{"t", Value("unpin")}, {"paglet", Value(o.positional[0])}};
    if (o.pin) request.emplace_back("pin", Value(*o.pin));
    auto a = r->ask(std::move(request));
    if (!a) return fail(a.error());
    const Fields f{*a};
    print_location(f);
    std::cout << f.integer("released").value_or(0) << " pins ended\n";
    return 0;
}

}  // namespace

bool is_mesh_command(std::string_view command) {
    return command == "keys" || command == "mesh" || command == "ledger" || command == "remote";
}

int mesh_command(int argc, char** argv) {
    if (argc < 3) return usage();
    const std::string_view group = argv[1];
    const std::string_view action = argv[2];
    auto options = parse(3, argc, argv);
    if (!options) return usage();
    const Options& o = *options;
    try {
        if (group == "keys" && action == "init") return keys_init(o);
        if (group == "keys" && action == "show") return keys_show(o);
        if (group == "mesh" && action == "create") return mesh_create(o);
        if (group == "ledger") {
            if (action == "show") return ledger_show(o);
            if (action == "request") return ledger_request(o);
            if (action == "approve") return ledger_approve(o);
            if (action == "deny") return ledger_deny(o);
            if (action == "enroll") return ledger_enroll(o);
            if (action == "remove") return ledger_remove(o);
            if (action == "revoke") return ledger_revoke(o);
            if (action == "admins") return ledger_admins(o);
            if (action == "sign") return ledger_sign(o);
            if (action == "rule") return ledger_rule(o);
            if (action == "audit") return ledger_audit(o);
            if (action == "sign-module") return ledger_sign_module(o);
            if (action == "trust") return ledger_trust(o);
            if (action == "module-policy") return ledger_module_policy(o);
        }
        if (group == "remote") {
            if (action == "status") return remote_status(o);
            if (action == "push") return remote_push(o);
            if (action == "launch") return remote_launch(o);
            if (action == "call") return remote_call(o);
            if (action == "dispatch") return remote_dispatch(o);
            if (action == "hosts") return remote_hosts(o);
            if (action == "locate") return remote_locate(o);
            if (action == "pin") return remote_pin(o);
            if (action == "pins") return remote_pins(o);
            if (action == "unpin") return remote_unpin(o);
        }
    } catch (const std::exception& e) {
        return fail(e.what());
    }
    return usage();
}

}  // namespace paglets::cli
