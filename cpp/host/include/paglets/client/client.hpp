// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Mesh client sessions (WP31, planning/cpp-web-ui.md): what `paglets-host
// remote` and `paglets-web` share. An Identity is a key file opened once (an
// admin or owner key) together with a ledger copy; a Session is one
// end-to-end channel with one host, carrying requests and answers of the
// session protocol (host/src/node/session.cpp). Sessions are not
// thread-safe; use one per thread or guard it.

#pragma once

#include <paglets/mesh/crypto.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/value.hpp>
#include <paglets/net/channel.hpp>
#include <paglets/net/transport.hpp>
#include <paglets/sha256.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace paglets::client {

using mesh::Bytes;

// -- keys and passphrases ------------------------------------------------------

// Reads a line from the terminal without echo.
std::optional<std::string> read_hidden(const std::string& prompt);

// Passphrases from the first line of a file, or else from the terminal.
struct Passphrases {
    std::optional<std::string> from_file;

    static std::expected<Passphrases, std::string> make(const std::optional<std::string>& file);
    std::optional<std::string> get(const std::string& prompt) const;
};

// Opens a key file, asking for its passphrase when it is encrypted.
std::expected<mesh::SigningKey, std::string> open_key(const std::string& path, const Passphrases& passphrases);

// A client identity: the key (admin or owner) and the ledger copy that says
// which hosts are enrolled.
struct Identity {
    mesh::Ledger ledger;
    mesh::SigningKey key;
    net::PeerRole role = net::PeerRole::owner;
    std::string name;  // of the key file

    static std::expected<Identity, std::string> open(const std::string& key_file, const std::string& ledger_dir,
                                                     const Passphrases& passphrases);
};

// -- answers -------------------------------------------------------------------

struct PagletRow {
    std::string id;
    std::string module;  // hex hash; empty for native system paglets
    std::string owner;   // key ID
    std::string state;
    std::string trust;  // roaming, resident, system; empty from older hosts
};

struct Status {
    mesh::PublicKey key{};
    std::string name;
    std::string mesh;
    std::int64_t records = 0;
    std::vector<PagletRow> paglets;
    // Newer hosts (WP32); empty or zero from older ones.
    std::vector<std::string> system;  // native system paglets
    std::int64_t uptime_s = 0;
    std::string version;
    std::string os;
    std::string os_version;
    std::string architecture;
    std::string cpu_model;
    std::int64_t cpus = 0;
    std::int64_t memory_total = 0;
    bool extended = false;  // the host sent the fields above
};

struct HostRow {
    mesh::PublicKey key{};
    std::string name;
    std::vector<std::string> labels;
    std::string url;
    bool self = false;
    bool online = false;
    std::string send_error;
    bool compatible = true;
    std::int64_t seen_ms = 0;
    std::int64_t protocol = 0;
    std::string abi;
    std::string via;
    std::vector<mesh::PublicKey> relays;

    // "this host", "online", "one-way" or "offline".
    std::string state() const;
};

struct ModuleRow {
    std::string hash;
    std::int64_t size = 0;
    std::int64_t users = 0;
    bool pinned = false;
    std::vector<std::string> names;  // "name version" from module signatures
    std::string interface;           // JSON text; empty when the module has none
};

struct CallResult {
    std::int32_t status = 0;  // abi status; 0 is ok
    Bytes payload;
};

struct Location {
    mesh::PublicKey host{};
    std::string host_name;
    std::int64_t moves = 0;
};

// -- sessions ------------------------------------------------------------------

class Session {
public:
    // Opens a channel to the host at `url`. The host must be enrolled in the
    // identity's ledger copy, or be `host_key` when given.
    static std::expected<Session, std::string> open(const std::string& url, const Identity& identity,
                                                    std::optional<mesh::PublicKey> host_key = std::nullopt,
                                                    std::chrono::seconds read_timeout = std::chrono::seconds(150));

    // One request, one answer; refusals of the host are errors.
    std::expected<mesh::Map, std::string> ask(mesh::Map request);

    const mesh::PublicKey& host() const;

    std::expected<Status, std::string> status();
    std::expected<std::vector<HostRow>, std::string> hosts();
    // mesh-info snapshots and the compute-slots status (reflection-encoded).
    std::expected<std::vector<Bytes>, std::string> landscape();
    std::expected<Bytes, std::string> slots();
    std::expected<std::vector<ModuleRow>, std::string> modules();
    std::expected<std::string, std::string> push_module(Bytes module);
    // Starts a paglet of the identity (an owner) from `module`; returns its ID.
    // `module_bytes` may be empty when the host already has the module.
    std::expected<std::string, std::string> launch(const Identity& identity, const paglets::Digest& module,
                                                   Bytes module_bytes, Bytes args, std::string id = {},
                                                   std::int64_t hours = 24);
    std::expected<CallResult, std::string> call(const std::string& paglet, const std::string& name, Bytes payload,
                                                std::int64_t timeout_ms = 30000);
    std::expected<void, std::string> send(const std::string& paglet, const std::string& name, Bytes payload,
                                          std::int64_t priority = 3);
    // The interface description (JSON text) of a paglet's module or of a
    // module; empty when there is none.
    std::expected<std::string, std::string> describe_paglet(const std::string& paglet);
    std::expected<std::string, std::string> describe_module(const std::string& module_hash);
    std::expected<void, std::string> dispose(const std::string& paglet);
    std::expected<void, std::string> terminate(const std::string& paglet, const std::string& reason = {});
    // Moves a paglet and waits up to `wait_ms` for the outcome.
    std::expected<mesh::Map, std::string> dispatch(const std::string& paglet, const std::string& destination,
                                                   std::int64_t wait_ms = 120'000);
    std::expected<Location, std::string> locate(const std::string& paglet);
    std::expected<mesh::Map, std::string> pin(const std::string& paglet, std::int64_t minutes,
                                              const std::string& reason);

    Session(Session&&) noexcept;
    Session& operator=(Session&&) noexcept;
    ~Session();

private:
    struct Impl;
    explicit Session(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

// A new random paglet ID (32 hex digits).
std::string new_paglet_id();

// -- shell commands ------------------------------------------------------------

// Quotes one argument for a POSIX shell (unchanged when it needs no quotes).
std::string shell_quote(const std::string& arg);

// The connection part of `paglets-host remote` commands.
struct ShellContext {
    std::string program = "paglets-host";
    std::string connect;
    std::string key_file;
    std::string ledger_dir;
    std::optional<std::string> host_key;
    std::optional<std::string> passphrase_file;
};

// `paglets-host remote <action> --connect … --key … --ledger … <args…>`.
std::string remote_command(const ShellContext& context, const std::string& action,
                           const std::vector<std::string>& args = {});

}  // namespace paglets::client
