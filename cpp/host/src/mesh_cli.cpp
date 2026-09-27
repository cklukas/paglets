// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Key and ledger commands of paglets-host (WP8). Until hosts talk to each
// other (WP12), they work on a local ledger directory.

#include "mesh_cli.hpp"

#include <paglets/mesh/crypto.hpp>
#include <paglets/mesh/ledger.hpp>

#include <algorithm>
#include <fstream>
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
        << "usage: paglets-host keys init --role admin|owner|host --name NAME --out FILE\n"
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
           "       paglets-host ledger admins --ledger DIR --admin KEY... [--add KEY-ID]... [--remove KEY-ID]...\n"
           "                                  [--admin-quorum N] [--quorum N]\n"
           "       paglets-host ledger sign --ledger DIR --admin KEY... RECORD-ID\n"
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
    std::optional<std::string> role, name, out, ledger, key, passphrase_file, reason, kdf;
    std::optional<std::int64_t> admin_quorum, quorum;
    bool ignored = false;
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

std::string joined(const std::vector<std::string>& list) {
    std::string out;
    for (const auto& s : list) out += (out.empty() ? "" : ",") + s;
    return out.empty() ? "-" : out;
}

// -- keys ----------------------------------------------------------------------

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
    if (!st.revoked_keys.empty() || !st.revoked_records.empty()) {
        std::cout << "revoked:\n";
        for (const auto& k : st.revoked_keys) std::cout << "  key    " << key_id(k) << "\n";
        for (const auto& r : st.revoked_records) std::cout << "  record " << record_id_hex(r) << "\n";
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
    if (o.positional.size() != 1) return usage();
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

}  // namespace

bool is_mesh_command(std::string_view command) {
    return command == "keys" || command == "mesh" || command == "ledger";
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
        }
    } catch (const std::exception& e) {
        return fail(e.what());
    }
    return usage();
}

}  // namespace paglets::cli
