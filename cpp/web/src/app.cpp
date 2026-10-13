// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-web (WP35): the mesh model, the JSON API and its actions.

#include "app.hpp"

#include <paglets/abi.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/interface.hpp>
#include <paglets/wire/json_msgpack.hpp>

#include <algorithm>
#include <future>
#include <set>

namespace paglets::webapp {

using wire::Json;
using client::Bytes;
namespace wi = wire::interface;

namespace {

Json str(std::string s) {
    return Json(std::move(s));
}
Json num(std::int64_t v) {
    return Json::integer(v);
}
Json flag(bool b) {
    return Json(b);
}
Json strings(const std::vector<std::string>& list) {
    Json::Array a;
    for (const auto& s : list) a.emplace_back(s);
    return Json(std::move(a));
}

struct Obj {
    Json::Object m;
    Obj& add(std::string k, Json v) {
        m.emplace_back(std::move(k), std::move(v));
        return *this;
    }
    Json done() { return Json(std::move(m)); }
};

std::string member(const Json& j, std::string_view k) {
    const Json* v = j.find(k);
    return v != nullptr && v->is_string() ? v->as_string() : std::string();
}

std::optional<std::int64_t> integer(const Json& j, std::string_view k) {
    const Json* v = j.find(k);
    std::int64_t n = 0;
    if (v == nullptr || !v->number_as(n)) return std::nullopt;
    return n;
}

web::Response ok(Json body) {
    return web::Response::json(200, body.dump());
}

web::Response failure(int status, const std::string& message, std::string shell = {}) {
    Obj o;
    o.add("error", str(message));
    if (!shell.empty()) o.add("shell", str(std::move(shell)));
    return web::Response::json(status, o.done().dump());
}

// A JSON value as the message payload (absent or null: empty).
Bytes payload_of(const Json* v) {
    if (v == nullptr || v->is_null()) return {};
    return wire::json_to_msgpack(*v);
}

Json reply_json(const Bytes& payload) {
    if (payload.empty()) return Json();
    if (auto j = wire::msgpack_to_json(payload)) return std::move(*j);
    return str("(" + std::to_string(payload.size()) + " bytes)");
}

// A parsed interface description, or null.
Json interface_json(const std::string& text) {
    if (text.empty()) return Json();
    if (auto d = wi::parse(text)) return std::move(*d);
    return Json();
}

constexpr std::chrono::seconds poll_timeout{15};

// Standard base64 (uploads from the browser); nullopt for anything else.
std::optional<Bytes> base64_decode(std::string_view text) {
    auto value = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    while (!text.empty() && text.back() == '=') text.remove_suffix(1);
    Bytes out;
    out.reserve(text.size() * 3 / 4);
    std::uint32_t acc = 0;
    int bits = 0;
    for (char c : text) {
        const int v = value(c);
        if (v < 0) return std::nullopt;
        acc = (acc << 6) | static_cast<std::uint32_t>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<std::uint8_t>((acc >> bits) & 0xff));
        }
    }
    return out;
}

}  // namespace

// A host the poller keeps a session with.
struct App::HostLink {
    std::mutex mu;
    std::string url;
    std::optional<mesh::PublicKey> key;  // expected; none: the ledger copy decides
    std::optional<client::Session> session;
};

App::App(Options options, client::Identity identity) : options_(std::move(options)), identity_(std::move(identity)) {
    Obj o;
    o.add("version", num(0)).add("loading", flag(true)).add("hosts", Json(Json::Array{}));
    model_ = o.done();
}

App::~App() {
    stop();
}

void App::stop() {
    stopping_.store(true);
    wait_cv.notify_all();
}

void App::run_poller() {
    while (!stopping_.load()) {
        refresh();
        std::unique_lock lock(wait_mu);
        wait_cv.wait_for(lock, options_.refresh, [&] { return stopping_.load(); });
    }
}

std::string App::model() const {
    std::lock_guard lock(mu);
    return model_.dump();
}

client::ShellContext App::shell(const std::string& host_key) const {
    client::ShellContext c;
    c.program = options_.program;
    c.key_file = options_.key_file;
    c.ledger_dir = options_.ledger_dir;
    c.passphrase_file = options_.passphrase_file;
    std::lock_guard lock(mu);
    auto url = urls_.find(host_key);
    if (host_key.empty() || host_key == entry_key_ || url == urls_.end() || url->second.empty()) {
        c.connect = options_.connect;
        c.host_key = options_.host_key;
    } else {
        c.connect = url->second;
    }
    return c;
}

std::optional<std::pair<std::string, mesh::PublicKey>> App::host_address(const std::string& host_key) const {
    auto key = mesh::parse_key_id(host_key);
    if (!key) return std::nullopt;
    std::lock_guard lock(mu);
    if (host_key == entry_key_) return std::pair{options_.connect, *key};
    auto url = urls_.find(host_key);
    if (url == urls_.end() || url->second.empty()) return std::nullopt;
    return std::pair{url->second, *key};
}

std::expected<client::Session, std::string> App::open_session(const std::string& host_key) {
    bool entry = host_key.empty();
    {
        std::lock_guard lock(mu);
        entry = entry || host_key == entry_key_;
    }
    if (entry) {
        std::optional<mesh::PublicKey> expected;
        if (options_.host_key) expected = mesh::parse_key_id(*options_.host_key);
        return client::Session::open(options_.connect, identity_, expected);
    }
    auto address = host_address(host_key);
    if (!address) return std::unexpected("no address of host " + host_key.substr(0, 16) + " is known here");
    return client::Session::open(address->first, identity_, address->second);
}

// -- the model -----------------------------------------------------------------

void App::refresh() {
    // The entry host: its registry, mesh-info's landscape.
    std::shared_ptr<HostLink> entry;
    {
        std::lock_guard lock(links_mu);
        auto& l = links_[""];
        if (!l) {
            l = std::make_shared<HostLink>();
            l->url = options_.connect;
            if (options_.host_key) l->key = mesh::parse_key_id(*options_.host_key);
        }
        entry = l;
    }
    std::vector<client::HostRow> hosts;
    std::vector<Bytes> landscape;
    std::optional<client::Status> entry_status;
    std::string entry_error;
    {
        std::lock_guard lock(entry->mu);
        for (int attempt = 0; attempt < 2; ++attempt) {
            if (!entry->session) {
                auto s = client::Session::open(entry->url, identity_, entry->key, poll_timeout);
                if (!s) {
                    entry_error = s.error();
                    break;
                }
                entry->session.emplace(std::move(*s));
            }
            auto h = entry->session->hosts();
            auto l = h ? entry->session->landscape() : std::unexpected(h.error());
            auto st = l ? entry->session->status() : std::unexpected(l.error());
            if (h && l && st) {
                hosts = std::move(*h);
                landscape = std::move(*l);
                entry_status = std::move(*st);
                entry_error.clear();
                break;
            }
            entry_error = !h ? h.error() : !l ? l.error() : st.error();
            entry->session.reset();
        }
    }
    const std::string entry_key = entry_status ? mesh::key_id(entry_status->key) : std::string();

    // Every other host with an address: its status, in parallel.
    std::map<std::string, std::future<std::expected<client::Status, std::string>>> statuses;
    for (const auto& h : hosts) {
        const std::string id = mesh::key_id(h.key);
        if (id == entry_key || h.url.empty() || !h.compatible) continue;
        std::shared_ptr<HostLink> link;
        {
            std::lock_guard lock(links_mu);
            auto& l = links_[id];
            if (!l) l = std::make_shared<HostLink>();
            link = l;
        }
        statuses[id] = std::async(std::launch::async, [this, link, url = h.url, key = h.key]()
                                                          -> std::expected<client::Status, std::string> {
            std::lock_guard lock(link->mu);
            if (link->url != url) link->session.reset();
            link->url = url;
            link->key = key;
            std::string why;
            for (int attempt = 0; attempt < 2; ++attempt) {
                if (!link->session) {
                    auto s = client::Session::open(url, identity_, key, poll_timeout);
                    if (!s) return std::unexpected(s.error());
                    link->session.emplace(std::move(*s));
                }
                auto st = link->session->status();
                if (st) return st;
                why = st.error();
                link->session.reset();
            }
            return std::unexpected(why);
        });
    }

    // Names from the ledger copy: owners and signed modules.
    const mesh::LedgerState ledger = identity_.ledger.state();
    std::map<std::string, std::string> owner_names;
    for (const auto& [k, o] : ledger.owners) owner_names[mesh::key_id(k)] = o.name;
    for (const auto& k : ledger.admins) owner_names.emplace(mesh::key_id(k), "admin");
    std::map<std::string, std::string> module_names;
    for (const auto& s : ledger.module_signatures) {
        module_names[to_hex(s.module)] = s.name + (s.version.empty() ? "" : " " + s.version);
    }
    const std::string me = identity_.key.id();

    std::map<std::string, Json> snapshots;
    for (const auto& b : landscape) {
        auto j = wire::msgpack_to_json(b);
        if (!j) continue;
        const std::string host = member(*j, "host");
        if (!host.empty()) snapshots[host] = std::move(*j);
    }

    auto status_json = [&](const client::Status& st) {
        Json::Array rows;
        std::int64_t system = static_cast<std::int64_t>(st.system.size());
        std::int64_t user = 0;
        for (const auto& p : st.paglets) {
            (p.trust == "system" ? system : user) += 1;
            auto name = module_names.find(p.module);
            auto owner = owner_names.find(p.owner);
            rows.push_back(Obj()
                               .add("id", str(p.id))
                               .add("module", str(p.module))
                               .add("module_name", str(name == module_names.end() ? std::string() : name->second))
                               .add("owner", str(p.owner))
                               .add("owner_name", str(owner == owner_names.end() ? std::string() : owner->second))
                               .add("mine", flag(p.owner == me))
                               .add("state", str(p.state))
                               .add("trust", str(p.trust.empty() ? "roaming" : p.trust))
                               .done());
        }
        return Obj()
            .add("reachable", flag(true))
            .add("version", str(st.version))
            .add("uptime_s", num(st.uptime_s))
            .add("os", str(st.os))
            .add("os_version", str(st.os_version))
            .add("architecture", str(st.architecture))
            .add("cpu_model", str(st.cpu_model))
            .add("cpus", num(st.cpus))
            .add("memory_total", num(st.memory_total))
            .add("extended", flag(st.extended))
            .add("system", strings(st.system))
            .add("paglets", Json(std::move(rows)))
            .add("counts", Obj().add("system", num(system)).add("user", num(user)).done())
            .done();
    };

    Json::Array host_list;
    std::int64_t total_system = 0, total_user = 0, online = 0;
    std::map<std::string, std::string> urls;
    for (const auto& h : hosts) {
        const std::string id = mesh::key_id(h.key);
        const bool is_entry = id == entry_key;
        urls[id] = is_entry ? options_.connect : h.url;
        Json status;
        std::string status_error;
        if (is_entry && entry_status) {
            status = status_json(*entry_status);
        } else if (auto it = statuses.find(id); it != statuses.end()) {
            auto st = it->second.get();
            if (st) {
                status = status_json(*st);
            } else {
                status_error = st.error();
            }
        } else {
            status_error = h.url.empty() ? "no address known (reached through relays)" : "incompatible version";
        }
        auto snap = snapshots.find(id);
        std::int64_t sys = 0, usr = 0;
        if (const Json* counts = status.find("counts")) {
            sys = integer(*counts, "system").value_or(0);
            usr = integer(*counts, "user").value_or(0);
        } else if (snap != snapshots.end()) {
            usr = integer(snap->second, "paglets").value_or(0);  // modules' paglets, from mesh-info
        }
        total_system += sys;
        total_user += usr;
        if (h.self || (h.online && h.send_error.empty())) ++online;
        Json::Array relays;
        for (const auto& k : h.relays) relays.emplace_back(mesh::key_id(k));
        const client::ShellContext sc = [&] {
            client::ShellContext c;
            c.program = options_.program;
            c.key_file = options_.key_file;
            c.ledger_dir = options_.ledger_dir;
            c.passphrase_file = options_.passphrase_file;
            c.connect = is_entry || h.url.empty() ? options_.connect : h.url;
            if (is_entry) c.host_key = options_.host_key;
            return c;
        }();
        host_list.push_back(Obj()
                                .add("key", str(id))
                                .add("name", str(h.name))
                                .add("labels", strings(h.labels))
                                .add("url", str(h.url))
                                .add("entry", flag(is_entry))
                                .add("state", str(is_entry ? "online" : h.state()))
                                .add("online", flag(h.online || h.self))
                                .add("send_error", str(h.send_error))
                                .add("compatible", flag(h.compatible))
                                .add("seen_ms", num(h.seen_ms))
                                .add("protocol", num(h.protocol))
                                .add("abi", str(h.abi))
                                .add("via", str(h.via))
                                .add("relays", Json(std::move(relays)))
                                .add("snapshot", snap == snapshots.end() ? Json() : snap->second)
                                .add("status", std::move(status))
                                .add("status_error", str(status_error))
                                .add("counts", Obj().add("system", num(sys)).add("user", num(usr)).done())
                                .add("shell", Obj()
                                                  .add("status", str(client::remote_command(sc, "status")))
                                                  .add("modules", str(client::remote_command(sc, "modules")))
                                                  .add("slots", str(client::remote_command(sc, "slots")))
                                                  .add("pins", str(client::remote_command(sc, "pins")))
                                                  .done())
                                .done());
    }
    std::ranges::sort(host_list, [](const Json& a, const Json& b) {
        const bool ea = a.find("entry")->as_bool(), eb = b.find("entry")->as_bool();
        if (ea != eb) return ea;
        return member(a, "name") < member(b, "name");
    });

    client::ShellContext entry_shell;
    entry_shell.program = options_.program;
    entry_shell.connect = options_.connect;
    entry_shell.key_file = options_.key_file;
    entry_shell.ledger_dir = options_.ledger_dir;
    entry_shell.host_key = options_.host_key;
    entry_shell.passphrase_file = options_.passphrase_file;

    std::lock_guard lock(mu);
    ++version_;
    urls_ = std::move(urls);
    if (!entry_key.empty()) entry_key_ = entry_key;
    Obj o;
    o.add("version", num(static_cast<std::int64_t>(version_)))
        .add("updated_ms", num(mesh::unix_ms()))
        .add("refresh_ms", num(options_.refresh.count()))
        .add("error", str(entry_error))
        .add("me", Obj()
                       .add("key", str(me))
                       .add("name", str(identity_.name))
                       .add("role", str(std::string(net::to_string(identity_.role))))
                       .add("key_file", str(options_.key_file))
                       .done())
        .add("mesh", Obj()
                         .add("name", str(ledger.mesh_name))
                         .add("id", str(mesh::record_id_hex(identity_.ledger.mesh())))
                         .add("records", num(entry_status ? entry_status->records : 0))
                         .add("entry", str(options_.connect))
                         .add("entry_key", str(entry_key_))
                         .add("entry_name", str(entry_status ? entry_status->name : std::string()))
                         .done())
        .add("totals", Obj()
                           .add("hosts", num(static_cast<std::int64_t>(hosts.size())))
                           .add("online", num(online))
                           .add("system", num(total_system))
                           .add("user", num(total_user))
                           .done())
        .add("hosts", Json(std::move(host_list)))
        .add("shell", Obj()
                          .add("hosts", str(client::remote_command(entry_shell, "hosts")))
                          .add("landscape", str(client::remote_command(entry_shell, "landscape")))
                          .add("status", str(client::remote_command(entry_shell, "status")))
                          .done());
    model_ = o.done();
}

// -- routes --------------------------------------------------------------------

web::Response App::mesh_route() const {
    return web::Response::json(200, model());
}

web::Response App::host_route(const web::Request& request) {
    const std::string key = request.param("key").value_or("");
    Json host;
    {
        std::lock_guard lock(mu);
        if (const Json* hosts = model_.find("hosts")) {
            for (const auto& h : hosts->as_array()) {
                if (member(h, "key") == key) host = h;
            }
        }
    }
    if (host.is_null()) return failure(404, "no host " + key + " in the mesh");
    Obj o;
    o.add("host", std::move(host));
    // Its modules and pins, fresh.
    auto session = open_session(key);
    if (session) {
        Json::Array modules;
        if (auto list = session->modules()) {
            for (const auto& m : *list) {
                modules.push_back(Obj()
                                      .add("hash", str(m.hash))
                                      .add("size", num(m.size))
                                      .add("users", num(m.users))
                                      .add("pinned", flag(m.pinned))
                                      .add("names", strings(m.names))
                                      .add("interface", interface_json(m.interface))
                                      .done());
            }
        }
        o.add("modules", Json(std::move(modules)));
        Json::Array pins;
        if (auto answer = session->ask(mesh::Map{{"t", mesh::Value("pins")}})) {
            if (const mesh::Array* list = mesh::Fields{*answer}.array("pins")) {
                for (const auto& p : *list) {
                    const mesh::Array* e = p.as_array();
                    if (e == nullptr || e->size() != 5) continue;
                    pins.push_back(Obj()
                                       .add("pin", str(*(*e)[0].as_str()))
                                       .add("paglet", str(*(*e)[1].as_str()))
                                       .add("until_ms", num(*(*e)[2].as_int()))
                                       .add("holder", str(*(*e)[3].as_str()))
                                       .add("reason", str(*(*e)[4].as_str()))
                                       .done());
                }
            }
        }
        o.add("pins", Json(std::move(pins)));
    } else {
        o.add("error", str(session.error()));
    }
    return ok(o.done());
}

web::Response App::paglet_route(const web::Request& request) {
    const std::string key = request.param("host").value_or("");
    const std::string id = request.param("id").value_or("");
    auto session = open_session(key);
    if (!session) return failure(502, session.error());
    auto text = session->describe_paglet(id);
    if (!text) return failure(404, text.error());
    const client::ShellContext sc = shell(key);
    Obj o;
    o.add("host", str(key))
        .add("id", str(id))
        .add("interface", interface_json(*text))
        .add("shell", Obj()
                          .add("describe", str(client::remote_command(sc, "describe", {id})))
                          .add("locate", str(client::remote_command(sc, "locate", {id})))
                          .add("dispose", str(client::remote_command(sc, "dispose", {id})))
                          .add("terminate", str(client::remote_command(sc, "terminate", {id})))
                          .add("pin", str(client::remote_command(sc, "pin", {id, "--minutes", "10"})))
                          .done());
    return ok(o.done());
}

web::Response App::modules_route() {
    // By hash: the hosts that have it, and the local module directory.
    struct Entry {
        std::string hash;
        std::int64_t size = 0;
        std::set<std::string> names;
        std::string interface;
        Json::Array hosts;
        std::string file;
    };
    std::map<std::string, Entry> by_hash;
    std::vector<std::pair<std::string, std::string>> targets;  // key, name
    {
        std::lock_guard lock(mu);
        if (const Json* hosts = model_.find("hosts")) {
            for (const auto& h : hosts->as_array()) {
                const Json* status = h.find("status");
                if (status != nullptr && !status->is_null()) targets.emplace_back(member(h, "key"), member(h, "name"));
            }
        }
    }
    Json::Array errors;
    std::vector<std::future<std::pair<std::string, std::expected<std::vector<client::ModuleRow>, std::string>>>> jobs;
    for (const auto& [key, name] : targets) {
        jobs.push_back(std::async(std::launch::async, [this, key, name] {
            auto session = open_session(key);
            if (!session) {
                return std::pair{key, std::expected<std::vector<client::ModuleRow>, std::string>(
                                          std::unexpected(session.error()))};
            }
            return std::pair{key, session->modules()};
        }));
    }
    for (std::size_t i = 0; i < jobs.size(); ++i) {
        auto [key, list] = jobs[i].get();
        if (!list) {
            errors.push_back(Obj().add("host", str(key)).add("error", str(list.error())).done());
            continue;
        }
        for (const auto& m : *list) {
            Entry& e = by_hash[m.hash];
            e.hash = m.hash;
            e.size = m.size;
            for (const auto& n : m.names) e.names.insert(n);
            if (e.interface.empty()) e.interface = m.interface;
            e.hosts.push_back(Obj()
                                  .add("key", str(key))
                                  .add("name", str(targets[i].second))
                                  .add("users", num(m.users))
                                  .add("pinned", flag(m.pinned))
                                  .done());
        }
    }
    if (options_.module_dir) {
        std::error_code ec;
        for (const auto& f : std::filesystem::directory_iterator(*options_.module_dir, ec)) {
            if (f.path().extension() != ".wasm" || !f.is_regular_file()) continue;
            auto bytes = wasm::read_file(f.path());
            if (!bytes) continue;
            const std::string hash = to_hex(sha256(*bytes));
            Entry& e = by_hash[hash];
            e.hash = hash;
            e.size = static_cast<std::int64_t>(bytes->size());
            e.file = f.path().filename().string();
            if (e.interface.empty()) {
                if (auto d = wi::of_module(*bytes); d && *d) e.interface = (*d)->dump();
            }
        }
    }
    const mesh::LedgerState ledger = identity_.ledger.state();
    for (const auto& s : ledger.module_signatures) {
        auto it = by_hash.find(to_hex(s.module));
        if (it != by_hash.end()) it->second.names.insert(s.name + (s.version.empty() ? "" : " " + s.version));
    }
    Json::Array modules;
    for (auto& [hash, e] : by_hash) {
        Json d = interface_json(e.interface);
        std::string title = e.file;
        if (!e.names.empty()) title = *e.names.begin();
        if (title.empty() && !d.is_null()) title = member(d, "name");
        const client::ShellContext sc = shell("");
        modules.push_back(Obj()
                              .add("hash", str(hash))
                              .add("title", str(title))
                              .add("size", num(e.size))
                              .add("names", strings(std::vector<std::string>(e.names.begin(), e.names.end())))
                              .add("file", str(e.file))
                              .add("hosts", Json(std::move(e.hosts)))
                              .add("interface", std::move(d))
                              .add("shell", Obj()
                                                .add("describe", str(client::remote_command(sc, "describe", {"--module", hash})))
                                                .done())
                              .done());
    }
    Obj o;
    o.add("modules", Json(std::move(modules)))
        .add("errors", Json(std::move(errors)))
        .add("module_dir", str(options_.module_dir ? options_.module_dir->string() : std::string()))
        .add("shell", Obj().add("modules", str(client::remote_command(shell(""), "modules"))).done());
    return ok(o.done());
}

web::Response App::zombies_route() {
    // Every reachable host's view of the zombie groups, by group.
    std::vector<std::pair<std::string, std::string>> targets;  // key, name
    {
        std::lock_guard lock(mu);
        if (const Json* hosts = model_.find("hosts")) {
            for (const auto& h : hosts->as_array()) {
                const Json* status = h.find("status");
                if (status != nullptr && !status->is_null()) targets.emplace_back(member(h, "key"), member(h, "name"));
            }
        }
    }
    std::vector<std::future<std::expected<mesh::Map, std::string>>> jobs;
    for (const auto& [key, name] : targets) {
        jobs.push_back(std::async(std::launch::async, [this, key = key]() -> std::expected<mesh::Map, std::string> {
            auto session = open_session(key);
            if (!session) return std::unexpected(session.error());
            return session->ask(mesh::Map{{"t", mesh::Value("zombies")}, {"op", mesh::Value("list")}});
        }));
    }
    struct Group {
        Json state;
        std::string owner, module;
        Json::Array replicas;
    };
    std::map<std::string, Group> groups;
    Json::Array errors;
    for (std::size_t i = 0; i < jobs.size(); ++i) {
        auto answer = jobs[i].get();
        if (!answer) {
            errors.push_back(Obj().add("host", str(targets[i].first)).add("error", str(answer.error())).done());
            continue;
        }
        const mesh::Array* list = mesh::Fields{*answer}.array("groups");
        if (list == nullptr) continue;
        for (const auto& g : *list) {
            const Bytes* b = g.as_bin();
            if (b == nullptr) continue;
            auto info = wire::msgpack_to_json(*b);
            if (!info || info->find("state") == nullptr) continue;
            const Json& state = *info->find("state");
            Group& e = groups[member(state, "group")];
            // The newest view of the state: an ending, or a higher sink number.
            const bool ended = member(state, "ending") != "none";
            if (e.state.is_null() || ended || integer(state, "sink").value_or(0) > integer(e.state, "sink").value_or(0)) {
                e.state = state;
            }
            e.owner = member(*info, "owner");
            if (e.module.empty()) e.module = member(*info, "module");
            e.replicas.push_back(Obj()
                                     .add("host", str(targets[i].first))
                                     .add("host_name", str(targets[i].second))
                                     .add("paglet", str(member(*info, "paglet")))
                                     .add("joined_ms", num(integer(*info, "joined_ms").value_or(0)))
                                     .add("traces", num(integer(*info, "traces").value_or(0)))
                                     .done());
        }
    }
    const client::ShellContext sc = shell("");
    Json::Array list;
    for (auto& [id, g] : groups) {
        list.push_back(Obj()
                           .add("group", str(id))
                           .add("state", std::move(g.state))
                           .add("owner", str(g.owner))
                           .add("module", str(g.module))
                           .add("replicas", Json(std::move(g.replicas)))
                           .add("shell", Obj()
                                             .add("traces", str(client::remote_command(sc, "zombies", {"traces", id})))
                                             .add("check", str(client::remote_command(sc, "zombies", {"check", id})))
                                             .add("result", str(client::remote_command(sc, "zombies", {"result", id})))
                                             .add("cancel", str(client::remote_command(sc, "zombies", {"cancel", id})))
                                             .done())
                           .done());
    }
    return ok(Obj()
                  .add("groups", Json(std::move(list)))
                  .add("errors", Json(std::move(errors)))
                  .add("shell", Obj().add("list", str(client::remote_command(sc, "zombies", {"list"}))).done())
                  .done());
}

web::Response App::action(const std::string& name, const Json& body) {
    static const std::set<std::string, std::less<>> actions{
        "call",     "send", "launch", "push-module",   "dispose",         "terminate",
        "dispatch", "pin",  "locate", "zombie-cancel", "zombie-redirect", "zombie-check"};
    if (!actions.contains(name)) return failure(404, "unknown action " + name);
    const std::string host = member(body, "host");
    const std::string paglet = member(body, "paglet");
    const client::ShellContext sc = shell(host);
    auto session_for = [&]() { return open_session(host); };

    if (name == "call" || name == "send") {
        const std::string message = member(body, "name");
        if (paglet.empty() || message.empty()) return failure(400, "paglet and name are required");
        const Json* payload = body.find("payload");
        std::vector<std::string> args{paglet, message};
        if (payload != nullptr && !payload->is_null()) args.push_back(payload->dump());
        const auto priority = integer(body, "priority");
        if (name == "send" && priority) {
            if (*priority < 0 || *priority > 7) return failure(400, "the priority is 0 to 7");
            args.push_back("--priority");
            args.push_back(std::to_string(*priority));
        }
        const std::string command = client::remote_command(sc, name, args);
        auto session = session_for();
        if (!session) return failure(502, session.error(), command);
        if (name == "send") {
            if (auto r = session->send(paglet, message, payload_of(payload), priority.value_or(3)); !r) {
                return failure(422, r.error(), command);
            }
            return ok(Obj().add("sent", flag(true)).add("shell", str(command)).done());
        }
        auto r = session->call(paglet, message, payload_of(payload),
                               std::clamp<std::int64_t>(integer(body, "timeout_ms").value_or(30000), 100, 120000));
        if (!r) return failure(422, r.error(), command);
        return ok(Obj()
                      .add("status", num(r->status))
                      .add("status_name", str(std::string(r->status == 0 ? "ok" : abi::error_name(r->status))))
                      .add("reply", reply_json(r->payload))
                      .add("shell", str(command))
                      .done());
    }

    if (name == "launch") {
        // A module the host has (by hash), or a file of --module-dir.
        const std::string file = member(body, "file");
        const std::string data = member(body, "data");  // an upload: base64, and its file name
        std::string module = member(body, "module");
        const Json* args = body.find("args");
        const std::string id = member(body, "id");
        const std::int64_t hours = std::clamp<std::int64_t>(integer(body, "hours").value_or(24), 1, 24 * 30);
        Bytes bytes;
        std::string shown = module;
        if (!data.empty()) {
            auto decoded = base64_decode(data);
            if (!decoded) return failure(400, "data is a module in base64");
            bytes = std::move(*decoded);
            module = to_hex(sha256(bytes));
            shown = member(body, "name").empty() ? "MODULE.wasm" : member(body, "name");
        } else if (!file.empty()) {
            if (!options_.module_dir) return failure(400, "paglets-web runs without --module-dir");
            const std::filesystem::path name_only = std::filesystem::path(file).filename();
            if (name_only.string() != file || name_only.extension() != ".wasm") return failure(400, "not a module file");
            const auto path = *options_.module_dir / name_only;
            auto read = wasm::read_file(path);
            if (!read) return failure(404, read.error());
            bytes = std::move(*read);
            module = to_hex(sha256(bytes));
            shown = path.string();
        }
        auto hash = mesh::parse_key_id(module);
        if (!hash) return failure(400, "module is a hash (64 hex digits) or file a module file");
        std::vector<std::string> cli_args{shown};
        if (args != nullptr && !args->is_null()) {
            cli_args.push_back("--args");
            cli_args.push_back(args->dump());
        }
        if (!id.empty()) {
            cli_args.push_back("--id");
            cli_args.push_back(id);
        }
        if (hours != 24) {
            cli_args.push_back("--hours");
            cli_args.push_back(std::to_string(hours));
        }
        const std::string command = client::remote_command(sc, "launch", cli_args);
        if (identity_.role != net::PeerRole::owner) {
            return failure(403, "paglets are launched by owners: start paglets-web with an owner key", command);
        }
        auto session = session_for();
        if (!session) return failure(502, session.error(), command);
        auto launched = session->launch(identity_, *hash, std::move(bytes), payload_of(args), id, hours);
        if (!launched) return failure(422, launched.error(), command);
        refresh();
        return ok(Obj().add("paglet", str(*launched)).add("host", str(host)).add("shell", str(command)).done());
    }

    if (name == "push-module") {
        // A file of --module-dir, or an upload (base64 `data` and its `name`).
        const std::string file = member(body, "file");
        const std::string data = member(body, "data");
        Bytes bytes;
        std::string shown;
        if (!data.empty()) {
            auto decoded = base64_decode(data);
            if (!decoded) return failure(400, "data is a module in base64");
            bytes = std::move(*decoded);
            shown = member(body, "name").empty() ? "MODULE.wasm" : member(body, "name");
        } else {
            if (!options_.module_dir) return failure(400, "paglets-web runs without --module-dir");
            const std::filesystem::path name_only = std::filesystem::path(file).filename();
            if (name_only.string() != file || name_only.extension() != ".wasm") return failure(400, "not a module file");
            const auto path = *options_.module_dir / name_only;
            auto read = wasm::read_file(path);
            if (!read) return failure(404, read.error());
            bytes = std::move(*read);
            shown = path.string();
        }
        const std::string command = client::remote_command(sc, "push-module", {shown});
        auto session = session_for();
        if (!session) return failure(502, session.error(), command);
        auto pushed = session->push_module(std::move(bytes));
        if (!pushed) return failure(422, pushed.error(), command);
        return ok(Obj().add("hash", str(*pushed)).add("shell", str(command)).done());
    }

    if (name.starts_with("zombie-")) {
        // Zombie groups (docs/zombie-protocol.md): cancel, redirect, check.
        const std::string group = member(body, "group");
        if (group.empty()) return failure(400, "group is required");
        const std::string op = name.substr(7);
        std::vector<std::string> args{op, group};
        mesh::Map request{{"t", mesh::Value("zombies")}, {"op", mesh::Value(op)}, {"g", mesh::Value(group)}};
        if (op == "redirect") {
            const std::string sink = member(body, "sink");
            if (sink.empty()) return failure(400, "sink is required");
            args.push_back(sink);
            request.emplace_back("sink", mesh::Value(sink));
        }
        if (op == "cancel") request.emplace_back("reason", mesh::Value(member(body, "reason")));
        const std::string command = client::remote_command(sc, "zombies", args);
        auto session = session_for();
        if (!session) return failure(502, session.error(), command);
        auto r = session->ask(std::move(request));
        if (!r) return failure(422, r.error(), command);
        const mesh::Fields f{*r};
        Obj o;
        if (op == "check") {
            Json::Array violations;
            if (const mesh::Array* list = f.array("violations")) {
                for (const auto& v : *list) {
                    if (const auto* text = v.as_str()) violations.emplace_back(*text);
                }
            }
            o.add("records", num(f.integer("records").value_or(0)))
                .add("hosts", num(f.integer("hosts").value_or(0)))
                .add("replicas", num(f.integer("replicas").value_or(0)))
                .add("takeovers", num(f.integer("takeovers").value_or(0)))
                .add("restarts", num(f.integer("restarts").value_or(0)))
                .add("accepted", num(f.integer("accepted").value_or(0)))
                .add("violations", Json(std::move(violations)));
        } else if (auto state = f.bin("state")) {
            o.add("state", reply_json(*state));
        }
        o.add("shell", str(command));
        return ok(o.done());
    }

    if (paglet.empty()) return failure(400, "paglet is required");

    if (name == "dispose" || name == "terminate") {
        const std::string reason = member(body, "reason");
        std::vector<std::string> args{paglet};
        if (name == "terminate" && !reason.empty()) {
            args.push_back("--reason");
            args.push_back(reason);
        }
        const std::string command = client::remote_command(sc, name, args);
        auto session = session_for();
        if (!session) return failure(502, session.error(), command);
        auto r = name == "dispose" ? session->dispose(paglet) : session->terminate(paglet, reason);
        if (!r) return failure(422, r.error(), command);
        refresh();
        return ok(Obj().add("done", flag(true)).add("shell", str(command)).done());
    }

    if (name == "dispatch") {
        const std::string destination = member(body, "destination");
        if (destination.empty()) return failure(400, "destination is required");
        const std::string command = client::remote_command(sc, "dispatch", {paglet, destination});
        auto session = session_for();
        if (!session) return failure(502, session.error(), command);
        auto r = session->dispatch(paglet, destination);
        if (!r) return failure(422, r.error(), command);
        const mesh::Fields f{*r};
        const mesh::Value* moved = f.get("moved");
        Obj o;
        o.add("pending", flag(f.get("pending") != nullptr))
            .add("moved", flag(moved != nullptr && moved->as_bool() != nullptr && *moved->as_bool()))
            .add("reason", str(f.str("reason").value_or("")))
            .add("host_name", str(f.str("host_name").value_or("")))
            .add("to", str(f.fixed<32>("host") ? mesh::key_id(*f.fixed<32>("host")) : std::string()))
            .add("shell", str(command));
        refresh();
        return ok(o.done());
    }

    if (name == "pin") {
        const std::int64_t minutes = std::clamp<std::int64_t>(integer(body, "minutes").value_or(10), 1, 24 * 60);
        const std::string reason = member(body, "reason");
        std::vector<std::string> args{paglet, "--minutes", std::to_string(minutes)};
        if (!reason.empty()) {
            args.push_back("--reason");
            args.push_back(reason);
        }
        const std::string command = client::remote_command(sc, "pin", args);
        auto session = session_for();
        if (!session) return failure(502, session.error(), command);
        auto r = session->pin(paglet, minutes, reason);
        if (!r) return failure(422, r.error(), command);
        const mesh::Fields f{*r};
        return ok(Obj()
                      .add("pin", str(f.str("pin").value_or("")))
                      .add("until_ms", num(f.integer("until").value_or(0)))
                      .add("host_name", str(f.str("host_name").value_or("")))
                      .add("shell", str(command))
                      .done());
    }

    if (name == "locate") {
        const std::string command = client::remote_command(sc, "locate", {paglet});
        auto session = session_for();
        if (!session) return failure(502, session.error(), command);
        auto r = session->locate(paglet);
        if (!r) return failure(422, r.error(), command);
        return ok(Obj()
                      .add("host", str(mesh::key_id(r->host)))
                      .add("host_name", str(r->host_name))
                      .add("moves", num(r->moves))
                      .add("shell", str(command))
                      .done());
    }

    return failure(404, "unknown action " + name);
}

web::Response App::api(const web::Request& request) {
    const std::string_view path = std::string_view(request.path).substr(std::string_view("/api/").size());
    if (request.method == "GET" || request.method == "HEAD") {
        if (path == "mesh") return mesh_route();
        if (path == "host") return host_route(request);
        if (path == "paglet") return paglet_route(request);
        if (path == "modules") return modules_route();
        if (path == "zombies") return zombies_route();
        return failure(404, "no route /api/" + std::string(path));
    }
    if (request.method == "POST") {
        auto body = Json::parse(request.body);
        if (!body || !body->is_object()) return failure(400, "the body is a JSON object");
        return action(std::string(path), *body);
    }
    return failure(405, "method not allowed");
}

web::Response route(App& app, const web::Guard& guard, const web::Request& request) {
    const bool api = request.path.starts_with("/api/");
    if (auto refused = guard.check(request, api)) return *refused;
    if (api) return app.api(request);
    if (request.method == "POST") return web::Response::text(405, "method not allowed\n");
    if (request.path == "/" || request.path == "/index.html") {
        if (request.param("t")) {
            if (auto r = guard.exchange(request)) return *r;
            return web::Response::text(403, "this link is not valid (anymore): use the one paglets-web printed\n");
        }
        web::Response r;
        r.content_type = "text/html; charset=utf-8";
        r.body = std::string(index_html);
        return r;
    }
    if (request.path == "/app.css") {
        web::Response r;
        r.content_type = "text/css; charset=utf-8";
        r.body = std::string(app_css);
        return r;
    }
    if (request.path == "/app.js") {
        web::Response r;
        r.content_type = "text/javascript; charset=utf-8";
        r.body = std::string(app_js);
        return r;
    }
    if (request.path == "/favicon.ico") return web::Response::text(404, "");
    return web::Response::text(404, "not found\n");
}

}  // namespace paglets::webapp
