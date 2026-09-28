// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/node/node.hpp>

#include <paglets/services/contract.hpp>
#include <paglets/services/grants.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/reflect.hpp>

#include <algorithm>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <set>

namespace paglets::node {

namespace {

namespace fs = std::filesystem;
namespace g = services::grants;
using services::Operation;
using services::Result;

constexpr std::int64_t min_duration_ms = 1000;
constexpr std::int64_t max_duration_ms = 30LL * 24 * 3600 * 1000;

// Code mobility frames (planning/cpp-modules.md, section 5).
constexpr std::string_view frame_module_want = "module-want";        // m, r
constexpr std::string_view frame_module_part = "module-part";        // m, r, o, n, d
constexpr std::string_view frame_module_missing = "module-missing";  // m, r, e
constexpr std::string_view frame_launch = "launch";                  // r, p, a
constexpr std::string_view frame_launch_result = "launch-result";    // r, id | e
constexpr std::size_t module_part_bytes = 1024 * 1024;
constexpr std::size_t max_module_bytes = 64u * 1024 * 1024;

std::string hex(const mesh::RecordId& id) {
    return mesh::record_id_hex(id);
}

std::optional<mesh::RecordId> parse_hex_id(std::string_view text) {
    return mesh::parse_key_id(text);  // record IDs have the same form as key IDs
}

std::string new_cap_id() {
    std::array<std::uint8_t, 16> bytes{};
    wasm::fill_random(bytes);
    return to_hex(std::span<const std::uint8_t>(bytes));
}

mesh::Item to_mesh(const g::Item& i) {
    return mesh::Item{i.service, i.ops, i.root, i.path};
}

g::Item to_contract(const mesh::Item& i) {
    return g::Item{i.service, i.ops, i.root, i.path};
}

// Relative paths of files items: the rules of dir capabilities.
bool valid_path(std::string_view path) {
    if (path.empty()) return true;
    std::size_t start = 0;
    while (start <= path.size()) {
        const std::size_t end = std::min(path.find('/', start), path.size());
        const std::string_view seg = path.substr(start, end - start);
        if (seg.empty() || seg == "." || seg == ".." || seg.find_first_of("\\:") != std::string_view::npos) {
            return false;
        }
        start = end + 1;
    }
    return true;
}

}  // namespace

class GrantsService;

struct Node::Impl {
    Impl(runtime::Runtime& rt, std::shared_ptr<services::SystemServices> svc, mesh::Ledger l, mesh::SigningKey k,
         mesh::GossipTransport& t, fs::path dir)
        : runtime(rt),
          services(std::move(svc)),
          ledger(std::move(l)),
          key(std::move(k)),
          transport(t),
          state_dir(std::move(dir)) {}

    runtime::Runtime& runtime;
    std::shared_ptr<services::SystemServices> services;
    mutable std::mutex mu;
    mesh::Ledger ledger;
    mesh::SigningKey key;
    mesh::GossipTransport& transport;
    std::unique_ptr<mesh::Replica> replica;
    fs::path state_dir;
    bool dirty = false;
    std::set<std::string> materialized;               // grants delivered on this host
    std::set<std::string> answered;                   // denials delivered on this host
    std::map<std::string, mesh::Passport> passports;  // of paglets created here
    runtime::SystemContext* grants_ctx = nullptr;
    // The ledger state as of the last sync, for the runtime's module
    // admission check (called under the runtime's lock, so it cannot take mu).
    std::mutex trust_mu;
    std::shared_ptr<const mesh::LedgerState> trust;

    // Code mobility.
    using Clock = std::chrono::steady_clock;
    using ModuleDone = std::function<void(const std::expected<std::string, std::string>&)>;
    struct Fetch {
        std::deque<mesh::PublicKey> candidates;  // hosts still to ask
        std::optional<mesh::PublicKey> peer;     // asked now
        std::uint64_t request = 0;
        runtime::Bytes data;
        std::int64_t total = -1;
        Clock::time_point deadline{};
        std::vector<std::string> failures;
        std::vector<ModuleDone> waiters;
    };
    std::vector<std::shared_ptr<runtime::ModuleSource>> sources;
    std::chrono::milliseconds fetch_timeout{10000};
    std::map<paglets::Digest, Fetch> fetches;
    std::uint64_t next_request = 1;
    std::map<std::uint64_t, std::pair<mesh::PublicKey, LaunchStatus>> launches;  // started here: target, status

    std::expected<void, std::string> admit(const std::string& module, runtime::TrustClass trust_class) {
        std::shared_ptr<const mesh::LedgerState> st;
        {
            std::lock_guard lock(trust_mu);
            st = trust;
        }
        if (!st) return {};
        auto digest = mesh::parse_key_id(module);
        if (!digest) return std::unexpected("invalid module hash " + module);
        auto v = mesh::check_module(*st, *digest, runtime::to_string(trust_class));
        if (!v.allowed) return std::unexpected(v.reason);
        return {};
    }

    // -- mu held -----------------------------------------------------------

    std::expected<mesh::Record, std::string> issue(std::string type, mesh::Map data) {
        auto r = ledger.draft(std::move(type), std::move(data), false);
        if (!r) return r;
        r->sign(key);
        if (auto added = replica->submit(*r); !added) return std::unexpected(added.error());
        return r;
    }

    static mesh::Principal principal_of(const abi::SenderRecord& s) {
        return mesh::Principal{s.id, s.owner, s.module, s.trust};
    }

    std::expected<runtime::Cap, std::string> materialize(const mesh::RecordId& id, const mesh::Grant& grant) {
        runtime::Cap cap;
        if (grant.item.service == "files") {
            auto dir = services->directory_capability(grant.item.root, grant.item.path, grant.item.ops, hex(id),
                                                      grant.expires);
            if (!dir) return std::unexpected(dir.error());
            cap = std::move(*dir);
        } else {
            if (!runtime.system_paglet(grant.item.service)) {
                return std::unexpected("no service " + grant.item.service + " on this host");
            }
            cap.kind = runtime::Cap::Kind::endpoint;
            cap.target = runtime::system_paglet_id(grant.item.service);
            cap.ops = grant.item.ops;
            cap.transferable = false;
            cap.grant = hex(id);
            cap.expires = grant.expires;
        }
        cap.id = new_cap_id();
        return cap;
    }

    // -- code mobility (mu held) -----------------------------------------------

    void warn(const std::string& text) const {
        if (grants_ctx != nullptr) grants_ctx->log(abi::LogLevel::warning, text);
    }

    void send_frame(const mesh::PublicKey& to, mesh::Map frame) {
        transport.send(to, mesh::encode(mesh::Value(std::move(frame))));
    }

    // Gets `module` and calls `done` with its hex hash, or why it could not.
    void need_module(const paglets::Digest& module, std::optional<mesh::PublicKey> from, ModuleDone done) {
        const std::string h = to_hex(module);
        if (runtime.modules().contains(h)) {
            done(h);
            return;
        }
        if (auto it = fetches.find(module); it != fetches.end()) {
            it->second.waiters.push_back(std::move(done));
            if (from && *from != key.public_key() && it->second.peer != from &&
                std::ranges::find(it->second.candidates, *from) == it->second.candidates.end()) {
                it->second.candidates.push_front(*from);
            }
            return;
        }
        Fetch f;
        for (const auto& source : sources) {
            auto bytes = source->fetch(module);
            if (!bytes) {
                f.failures.push_back(source->name() + ": " + bytes.error());
                continue;
            }
            auto added = runtime.modules().add_verified(module, std::move(*bytes));
            if (added) {
                done(*added);
                return;
            }
            f.failures.push_back(source->name() + ": " + added.error());
        }
        if (from && *from != key.public_key()) f.candidates.push_back(*from);
        for (const auto& peer : replica->peers()) {
            if (peer != key.public_key() && std::ranges::find(f.candidates, peer) == f.candidates.end()) {
                f.candidates.push_back(peer);
            }
        }
        f.waiters.push_back(std::move(done));
        fetches.emplace(module, std::move(f));
        ask_next(module);
    }

    void ask_next(const paglets::Digest& module) {
        Fetch& f = fetches.at(module);
        f.peer.reset();
        f.data.clear();
        f.total = -1;
        if (f.candidates.empty()) {
            std::string why = "module " + to_hex(module).substr(0, 16) + " not found";
            for (const auto& failure : f.failures) why += "; " + failure;
            finish_fetch(module, std::unexpected(why));
            return;
        }
        f.peer = f.candidates.front();
        f.candidates.pop_front();
        f.request = next_request++;
        f.deadline = Clock::now() + fetch_timeout;
        send_frame(*f.peer, mesh::Map{{"t", mesh::Value(frame_module_want)},
                                      {"m", mesh::Value::bin(module)},
                                      {"r", mesh::Value(static_cast<std::int64_t>(f.request))}});
    }

    void peer_failed(const paglets::Digest& module, const std::string& why) {
        Fetch& f = fetches.at(module);
        f.failures.push_back("host " + mesh::key_id(*f.peer).substr(0, 16) + ": " + why);
        ask_next(module);
    }

    void finish_fetch(const paglets::Digest& module, const std::expected<std::string, std::string>& result) {
        auto node = fetches.extract(module);
        for (auto& done : node.mapped().waiters) done(result);
    }

    // The fetch a frame from `from` answers, if any.
    Fetch* answered_fetch(const mesh::PublicKey& from, const mesh::Fields& f, paglets::Digest& module) {
        auto m = f.fixed<32>("m");
        auto r = f.integer("r");
        if (!m || !r) return nullptr;
        auto it = fetches.find(*m);
        if (it == fetches.end() || it->second.peer != from || it->second.request != static_cast<std::uint64_t>(*r)) {
            return nullptr;
        }
        module = *m;
        return &it->second;
    }

    void on_module_want(const mesh::PublicKey& from, const mesh::Fields& f) {
        auto m = f.fixed<32>("m");
        auto r = f.integer("r");
        if (!m || !r) return;
        auto answer = [&](std::string_view why) {
            send_frame(from, mesh::Map{{"t", mesh::Value(frame_module_missing)},
                                       {"m", mesh::Value::bin(*m)},
                                       {"r", mesh::Value(*r)},
                                       {"e", mesh::Value(why)}});
        };
        // Modules go to enrolled hosts only.
        if (!ledger.state().is_host(from)) return answer("not an enrolled host");
        auto bytes = runtime.modules().bytes(to_hex(*m));
        if (!bytes) return answer("module not here");
        const auto total = static_cast<std::int64_t>(bytes->size());
        std::size_t offset = 0;
        do {
            const std::size_t n = std::min(module_part_bytes, bytes->size() - offset);
            send_frame(from, mesh::Map{{"t", mesh::Value(frame_module_part)},
                                       {"m", mesh::Value::bin(*m)},
                                       {"r", mesh::Value(*r)},
                                       {"o", mesh::Value(static_cast<std::int64_t>(offset))},
                                       {"n", mesh::Value(total)},
                                       {"d", mesh::Value(runtime::Bytes(
                                                 bytes->begin() + static_cast<std::ptrdiff_t>(offset),
                                                 bytes->begin() + static_cast<std::ptrdiff_t>(offset + n)))}});
            offset += n;
        } while (offset < bytes->size());
    }

    void on_module_part(const mesh::PublicKey& from, const mesh::Fields& f) {
        paglets::Digest module{};
        Fetch* fetch = answered_fetch(from, f, module);
        if (fetch == nullptr) return;
        auto offset = f.integer("o");
        auto total = f.integer("n");
        auto data = f.bin("d");
        if (!offset || !total || !data || *total <= 0 || static_cast<std::uint64_t>(*total) > max_module_bytes ||
            (fetch->total >= 0 && fetch->total != *total) || *offset != static_cast<std::int64_t>(fetch->data.size()) ||
            fetch->data.size() + data->size() > static_cast<std::uint64_t>(*total)) {
            return peer_failed(module, "malformed module transfer");
        }
        fetch->total = *total;
        fetch->data.insert(fetch->data.end(), data->begin(), data->end());
        fetch->deadline = Clock::now() + fetch_timeout;
        if (static_cast<std::int64_t>(fetch->data.size()) < fetch->total) return;
        auto added = runtime.modules().add_verified(module, std::move(fetch->data));
        if (!added) return peer_failed(module, added.error());
        finish_fetch(module, *added);
    }

    void on_module_missing(const mesh::PublicKey& from, const mesh::Fields& f) {
        paglets::Digest module{};
        if (answered_fetch(from, f, module) == nullptr) return;
        peer_failed(module, f.str("e").value_or("module not there"));
    }

    void check_fetch_deadlines() {
        const auto now = Clock::now();
        std::vector<paglets::Digest> late;
        for (const auto& [module, f] : fetches) {
            if (f.peer && now >= f.deadline) late.push_back(module);
        }
        for (const auto& module : late) peer_failed(module, "no answer in time");
    }

    // Creates a root paglet from its passport (Node::create).
    std::expected<runtime::PagletId, std::string> create_locked(std::string_view module, const mesh::Passport& passport,
                                                                runtime::Bytes args) {
        auto hash = mesh::parse_key_id(module);  // module hashes have the form of key IDs
        if (!hash) return std::unexpected("module " + std::string(module) + " is not a hex SHA-256");
        if (!passport.links().empty()) return std::unexpected(std::string("a root paglet needs a root passport"));
        if (auto ok = mesh::verify_passport(passport, ledger.state(), passport.paglet(), *hash, mesh::unix_ms()); !ok) {
            return std::unexpected("passport: " + ok.error());
        }
        runtime::CreateOptions options;
        options.args = std::move(args);
        options.trust = runtime::TrustClass::roaming;
        options.owner = mesh::key_id(passport.root().owner);
        options.id = passport.paglet();
        auto id = runtime.create(module, std::move(options));
        if (!id) return id;
        passports.emplace(*id, passport);
        save_passport(passport);
        sync_locked();  // grants approved in advance
        return id;
    }

    using LaunchDone = std::function<void(const std::expected<runtime::PagletId, std::string>&)>;

    // A launch arriving from `origin` (or started on this host): checks
    // what can be checked before the module is here, gets the module, then
    // creates the paglet.
    void start_launch(const std::optional<mesh::PublicKey>& origin, runtime::Bytes passport_bytes, runtime::Bytes args,
                      LaunchDone done) {
        const mesh::LedgerState& st = ledger.state();
        if (origin && !st.is_host(*origin))
            return done(std::unexpected(std::string("launch from a host that is not enrolled")));
        auto passport = mesh::Passport::decode(passport_bytes);
        if (!passport) return done(std::unexpected("passport: " + passport.error()));
        if (!passport->links().empty())
            return done(std::unexpected(std::string("a root paglet needs a root passport")));
        const paglets::Digest module = passport->module();
        if (auto ok = mesh::verify_passport(*passport, st, passport->paglet(), module, mesh::unix_ms()); !ok) {
            return done(std::unexpected("passport: " + ok.error()));
        }
        if (auto v = mesh::check_module(st, module, "roaming"); !v.allowed) return done(std::unexpected(v.reason));
        need_module(module, origin,
                    [this, passport = std::move(*passport), args = std::move(args),
                     done = std::move(done)](const std::expected<std::string, std::string>& hash) mutable {
                        if (!hash) return done(std::unexpected(hash.error()));
                        done(create_locked(*hash, passport, std::move(args)));
                    });
    }

    void on_launch(const mesh::PublicKey& from, const mesh::Fields& f) {
        auto r = f.integer("r");
        auto p = f.bin("p");
        auto a = f.bin("a");
        if (!r || !p) return;
        const std::int64_t request = *r;
        start_launch(from, std::move(*p), a ? std::move(*a) : runtime::Bytes{},
                     [this, from, request](const std::expected<runtime::PagletId, std::string>& result) {
                         mesh::Map answer{{"t", mesh::Value(frame_launch_result)}, {"r", mesh::Value(request)}};
                         if (result) {
                             answer.emplace_back("id", mesh::Value(*result));
                         } else {
                             answer.emplace_back("e", mesh::Value(result.error()));
                         }
                         send_frame(from, std::move(answer));
                     });
    }

    void on_launch_result(const mesh::PublicKey& from, const mesh::Fields& f) {
        auto r = f.integer("r");
        if (!r) return;
        auto it = launches.find(static_cast<std::uint64_t>(*r));
        if (it == launches.end() || it->second.first != from ||
            it->second.second.state != LaunchStatus::State::pending) {
            return;
        }
        LaunchStatus& status = it->second.second;
        if (auto id = f.str("id")) {
            status.state = LaunchStatus::State::running;
            status.paglet = std::move(*id);
        } else {
            status.state = LaunchStatus::State::failed;
            status.error = f.str("e").value_or("launch failed");
        }
    }

    // Node frames; false for gossip frames.
    bool handle_node_frame(const mesh::PublicKey& from, std::span<const std::uint8_t> frame) {
        if (frame.size() > mesh::Replica::max_frame) return false;
        auto v = mesh::decode(frame);
        if (!v || v->as_map() == nullptr) return false;
        const mesh::Fields f{*v->as_map()};
        auto type = f.str("t");
        if (!type) return false;
        if (*type == frame_module_want) {
            on_module_want(from, f);
        } else if (*type == frame_module_part) {
            on_module_part(from, f);
        } else if (*type == frame_module_missing) {
            on_module_missing(from, f);
        } else if (*type == frame_launch) {
            on_launch(from, f);
        } else if (*type == frame_launch_result) {
            on_launch_result(from, f);
        } else {
            return false;
        }
        return true;
    }

    void load() {
        if (state_dir.empty()) return;
        auto bytes = wasm::read_file((state_dir / "node.state").string());
        if (!bytes) return;
        auto v = mesh::decode(*bytes);
        if (!v || v->as_map() == nullptr) return;
        const mesh::Fields f{*v->as_map()};
        if (auto m = f.strings("materialized")) materialized.insert(m->begin(), m->end());
        if (auto a = f.strings("answered")) answered.insert(a->begin(), a->end());
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator(state_dir / "passports", ec)) {
            auto p = wasm::read_file(entry.path().string());
            if (!p) continue;
            if (auto passport = mesh::Passport::decode(*p)) passports.emplace(passport->paglet(), std::move(*passport));
        }
    }

    void save_passport(const mesh::Passport& passport) const {
        if (state_dir.empty()) return;
        std::error_code ec;
        fs::create_directories(state_dir / "passports", ec);
        const fs::path file = state_dir / "passports" / (passport.paglet() + ".passport");
        const fs::path temp = file.string() + ".tmp";
        if (wasm::write_file(temp.string(), passport.encode())) fs::rename(temp, file, ec);
    }

    void save() const {
        if (state_dir.empty()) return;
        auto list = [](const std::set<std::string>& s) {
            mesh::Array a;
            for (const auto& e : s) a.emplace_back(e);
            return mesh::Value(std::move(a));
        };
        const auto bytes =
            mesh::encode(mesh::Value(mesh::Map{{"materialized", list(materialized)}, {"answered", list(answered)}}));
        std::error_code ec;
        fs::create_directories(state_dir, ec);
        const fs::path temp = state_dir / "node.state.tmp";
        if (wasm::write_file(temp.string(), bytes)) fs::rename(temp, state_dir / "node.state", ec);
    }

    void sync_if_dirty() {
        if (!dirty) return;
        dirty = false;
        sync_locked();
    }

    void sync_locked() {
        const mesh::LedgerState& st = ledger.state();
        std::set<std::string, std::less<>> ended;
        for (const auto& id : st.ended_grants) ended.insert(hex(id));
        runtime.set_revoked_grants(std::move(ended));
        {
            std::lock_guard lock(trust_mu);
            trust = std::make_shared<const mesh::LedgerState>(st);
        }
        // Paglets whose module lost the mesh's trust end (also paglets
        // recovered before this host saw the change).
        for (const auto& p : runtime.list()) {
            if (p.module.empty()) continue;  // native system paglets
            if (auto ok = admit(p.module, p.trust); !ok) {
                (void)runtime.terminate(p.id, "module no longer trusted: " + ok.error());
            }
        }
        if (grants_ctx == nullptr) return;
        bool changed = false;
        const std::int64_t now = mesh::unix_ms();
        // Approvals: admin grants for paglets on this host.
        for (const auto& [id, grant] : st.grants) {
            const std::string h = hex(id);
            if (!grant.by_admin || materialized.contains(h) || grant.expires <= now) continue;
            if (!runtime.info(grant.principal.paglet) || !mesh::selects(st, grant.hosts, key.public_key())) continue;
            auto cap = materialize(id, grant);
            if (!cap) {
                grants_ctx->log(abi::LogLevel::warning, "grant " + h.substr(0, 16) + ": " + cap.error());
                continue;
            }
            g::Answer a{g::Status::granted,
                        to_contract(grant.item),
                        h,
                        grant.request ? hex(*grant.request) : std::string(),
                        std::string(),
                        grant.expires,
                        "approved"};
            if (grants_ctx->send(grant.principal.paglet, "grants.granted", wire::to_msgpack(a), {std::move(*cap)})) {
                materialized.insert(h);
                changed = true;
            }
        }
        // Denials of requests of paglets on this host.
        for (const mesh::Record* r : ledger.records()) {
            if (r->type() != "request-deny") continue;
            const mesh::Value* request = r->fields().get("request");
            const mesh::Bytes* bin = request == nullptr ? nullptr : request->as_bin();
            if (bin == nullptr || bin->size() != 32) continue;
            mesh::RecordId rid{};
            std::copy(bin->begin(), bin->end(), rid.begin());
            const std::string h = hex(rid);
            if (answered.contains(h)) continue;
            const mesh::Record* req = ledger.find(rid);
            if (req == nullptr || req->type() != "grant-request") continue;
            auto parsed = mesh::parse_grant_request(*req);
            if (!parsed || !runtime.info(parsed->principal.paglet)) continue;
            g::Answer a{g::Status::denied,
                        parsed->items.empty() ? g::Item{} : to_contract(parsed->items.front()),
                        std::string(),
                        h,
                        std::string(),
                        0,
                        r->fields().str("reason").value_or("denied by an admin")};
            if (grants_ctx->send(parsed->principal.paglet, "grants.denied", wire::to_msgpack(a))) {
                answered.insert(h);
                changed = true;
            }
        }
        if (changed) save();
    }
};

// -- the grants system paglet ---------------------------------------------------

class GrantsService final : public services::ContractPaglet<g::Contract, GrantsService> {
public:
    explicit GrantsService(Node::Impl& node) : node_(node) {}

    std::vector<std::string> default_ops() const override { return operations(); }

    void start(runtime::SystemContext& ctx) override {
        std::lock_guard lock(node_.mu);
        node_.grants_ctx = &ctx;
    }

    Result<g::Answer> request(const g::Request& q, Operation& op) {
        const auto& caller = op.sender();
        if (!caller || caller->id == "host") return std::unexpected(abi::denied);
        const mesh::Item item = to_mesh(q.item);
        if (auto valid = check_item(item); valid != abi::ok) return std::unexpected(valid);
        const std::int64_t duration = std::clamp(q.duration_ms, min_duration_ms, max_duration_ms);

        std::lock_guard lock(node_.mu);
        const mesh::PublicKey host = node_.key.public_key();
        const mesh::Principal principal = Node::Impl::principal_of(*caller);
        const mesh::Evaluation e = mesh::evaluate(node_.ledger.state(), principal, host, item);
        g::Answer answer;
        answer.item = q.item;
        if (e.rule) answer.rule = hex(*e.rule);
        switch (e.decision) {
            case mesh::Decision::allow: {
                const std::int64_t expires = mesh::unix_ms() + std::min(duration, e.max_duration_ms.value_or(duration));
                auto grant = node_.issue("grant", mesh::data::grant(principal, item, mesh::HostSelector{{host}, {}},
                                                                    expires, std::nullopt, e.rule, host));
                if (!grant) return fail(op, grant.error());
                (void)node_.issue("audit", mesh::data::audit(host, principal, item, mesh::Decision::allow, e.rule,
                                                             grant->id(), std::nullopt));
                auto parsed = mesh::parse_grant(*grant);
                if (!parsed) return fail(op, "a grant this host made does not parse");
                auto cap = node_.materialize(grant->id(), *parsed);
                if (!cap) return fail(op, cap.error());
                node_.materialized.insert(hex(grant->id()));
                node_.save();
                op.give.push_back(std::move(*cap));
                answer.status = g::Status::granted;
                answer.grant = hex(grant->id());
                answer.expires_ms = expires;
                answer.reason = "allowed by rule";
                break;
            }
            case mesh::Decision::ask: {
                auto request = node_.issue("grant-request",
                                           mesh::data::grant_request(principal, host, {item}, duration, q.reason));
                if (!request) return fail(op, request.error());
                (void)node_.issue("audit", mesh::data::audit(host, principal, item, mesh::Decision::ask, e.rule,
                                                             std::nullopt, request->id()));
                answer.status = g::Status::pending;
                answer.request = hex(request->id());
                answer.reason = "waiting for an admin";
                break;
            }
            case mesh::Decision::deny:
                (void)node_.issue("audit", mesh::data::audit(host, principal, item, mesh::Decision::deny, e.rule,
                                                             std::nullopt, std::nullopt));
                answer.status = g::Status::denied;
                answer.reason = e.rule ? "denied by rule" : "no rule allows it";
                break;
        }
        node_.sync_if_dirty();
        return answer;
    }

    Result<g::Answer> status(const g::StatusRequest& q, Operation& op) {
        const auto& caller = op.sender();
        auto id = parse_hex_id(q.request);
        if (!caller || !id) return std::unexpected(abi::invalid_argument);
        std::lock_guard lock(node_.mu);
        const mesh::Record* r = node_.ledger.find(*id);
        if (r == nullptr || r->type() != "grant-request") return std::unexpected(abi::not_found);
        auto request = mesh::parse_grant_request(*r);
        if (!request || request->principal.paglet != caller->id) return std::unexpected(abi::denied);
        const mesh::LedgerState& st = node_.ledger.state();
        g::Answer a;
        a.request = q.request;
        a.item = request->items.empty() ? g::Item{} : to_contract(request->items.front());
        if (std::ranges::any_of(st.grant_requests, [&](const auto& p) { return p.id == *id; })) {
            a.status = g::Status::pending;
            return a;
        }
        for (const auto& [gid, grant] : st.grants) {
            if (grant.request == *id) {
                a.status = g::Status::granted;
                a.grant = hex(gid);
                a.expires_ms = grant.expires;
                return a;
            }
        }
        a.status = g::Status::denied;
        return a;
    }

    Result<g::ReleaseReply> release(const g::ReleaseRequest& q, Operation& op) {
        const auto& caller = op.sender();
        auto id = parse_hex_id(q.grant);
        if (!caller || !id) return std::unexpected(abi::invalid_argument);
        std::lock_guard lock(node_.mu);
        const auto& grants = node_.ledger.state().grants;
        auto it = grants.find(*id);
        if (it == grants.end()) return g::ReleaseReply{false};
        if (it->second.principal.paglet != caller->id) return std::unexpected(abi::denied);
        if (auto r = node_.issue("grant-release", mesh::data::grant_release(*id)); !r) return fail(op, r.error());
        node_.sync_if_dirty();
        return g::ReleaseReply{true};
    }

    Result<g::ListReply> list(const g::ListRequest&, Operation& op) {
        const auto& caller = op.sender();
        if (!caller) return std::unexpected(abi::denied);
        std::lock_guard lock(node_.mu);
        g::ListReply reply;
        const std::int64_t now = mesh::unix_ms();
        for (const auto& [id, grant] : node_.ledger.state().grants) {
            if (grant.principal.paglet == caller->id && grant.expires > now) {
                reply.grants.push_back(g::GrantInfo{hex(id), to_contract(grant.item), grant.expires});
            }
        }
        return reply;
    }

private:
    std::int32_t check_item(const mesh::Item& item) const {
        if (item.service.empty() || item.ops.empty()) return abi::invalid_argument;
        for (const auto& o : item.ops) {
            if (!abi::valid_message_name(o)) return abi::invalid_argument;
        }
        if (item.service == "files") {
            if (item.root.empty() || !valid_path(item.path)) return abi::invalid_argument;
            if (!node_.services->directory_capability(item.root, item.path, item.ops)) return abi::not_found;
            return abi::ok;
        }
        if (!item.root.empty() || !item.path.empty()) return abi::invalid_argument;
        auto service = node_.runtime.system_paglet_object(item.service);
        if (!service) return abi::not_found;
        const auto offered = service->operations();
        for (const auto& o : item.ops) {
            if (std::ranges::find(offered, o) == offered.end()) return abi::invalid_argument;
        }
        return abi::ok;
    }

    static std::unexpected<std::int32_t> fail(Operation& op, const std::string& why) {
        op.ctx.log(abi::LogLevel::error, "grants: " + why);
        return std::unexpected(std::int32_t{abi::internal});
    }

    Node::Impl& node_;
};

// -- Node ---------------------------------------------------------------------------

Node::Node(runtime::Runtime& runtime, std::shared_ptr<services::SystemServices> services, mesh::Ledger ledger,
           mesh::SigningKey host_key, mesh::GossipTransport& transport, std::filesystem::path state_dir)
    : impl_(std::make_unique<Impl>(runtime, std::move(services), std::move(ledger), std::move(host_key), transport,
                                   std::move(state_dir))) {
    impl_->replica = std::make_unique<mesh::Replica>(impl_->ledger, impl_->key.public_key(), transport);
    impl_->replica->on_change([impl = impl_.get()] { impl->dirty = true; });
    impl_->load();
}

Node::~Node() {
    impl_->runtime.set_module_admission({});
}

std::expected<void, std::string> Node::start() {
    auto grants = std::make_shared<GrantsService>(*impl_);
    if (auto id = impl_->runtime.add_system_paglet(grants); !id) return std::unexpected(id.error());
    // Services with ambient authority: only the operations the rules allow.
    impl_->services->set_policy([impl = impl_.get()](const abi::SenderRecord& caller, std::string_view service,
                                                     const std::vector<std::string>& offered) {
        auto object = impl->runtime.system_paglet_object(service);
        if (!object || !object->ambient()) return offered;
        std::lock_guard lock(impl->mu);
        const mesh::Principal principal = Impl::principal_of(caller);
        std::vector<std::string> allowed;
        for (const auto& op : offered) {
            const mesh::Item item{std::string(service), {op}, {}, {}};
            const auto e = mesh::evaluate(impl->ledger.state(), principal, impl->key.public_key(), item);
            if (e.decision == mesh::Decision::allow) allowed.push_back(op);
        }
        return allowed;
    });
    sync();
    impl_->runtime.set_module_admission([impl = impl_.get()](const std::string& module, runtime::TrustClass trust) {
        return impl->admit(module, trust);
    });
    return {};
}

const mesh::PublicKey& Node::host() const {
    return impl_->key.public_key();
}

const mesh::RecordId& Node::mesh() const {
    return impl_->ledger.mesh();
}

const mesh::SigningKey& Node::host_key() const {
    return impl_->key;
}

std::vector<mesh::PublicKey> Node::peers() const {
    std::lock_guard lock(impl_->mu);
    return impl_->replica->peers();
}

void Node::add_seed(const mesh::PublicKey& peer) {
    std::lock_guard lock(impl_->mu);
    impl_->replica->add_seed(peer);
}

void Node::receive(const mesh::PublicKey& from, std::span<const std::uint8_t> frame) {
    std::lock_guard lock(impl_->mu);
    if (!impl_->handle_node_frame(from, frame)) impl_->replica->receive(from, frame);
    impl_->sync_if_dirty();
}

void Node::tick() {
    std::lock_guard lock(impl_->mu);
    impl_->replica->tick();
    impl_->check_fetch_deadlines();
}

std::expected<mesh::AddResult, std::string> Node::submit(const mesh::Record& record) {
    std::lock_guard lock(impl_->mu);
    auto r = impl_->replica->submit(record);
    impl_->sync_if_dirty();
    return r;
}

mesh::LedgerState Node::state() const {
    std::lock_guard lock(impl_->mu);
    return impl_->ledger.state();
}

paglets::Digest Node::ledger_digest() const {
    std::lock_guard lock(impl_->mu);
    return impl_->ledger.digest();
}

std::expected<mesh::Record, std::string> Node::draft(std::string type, mesh::Map data, bool with_epoch) const {
    std::lock_guard lock(impl_->mu);
    return impl_->ledger.draft(std::move(type), std::move(data), with_epoch);
}

void Node::sync() {
    std::lock_guard lock(impl_->mu);
    impl_->dirty = false;
    impl_->sync_locked();
}

std::expected<runtime::PagletId, std::string> Node::create(std::string_view module, const mesh::Passport& passport,
                                                           runtime::Bytes args) {
    std::lock_guard lock(impl_->mu);
    return impl_->create_locked(module, passport, std::move(args));
}

std::optional<mesh::Passport> Node::passport(const runtime::PagletId& paglet) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->passports.find(paglet);
    if (it == impl_->passports.end()) return std::nullopt;
    return it->second;
}

void Node::add_module_source(std::shared_ptr<runtime::ModuleSource> source) {
    std::lock_guard lock(impl_->mu);
    impl_->sources.push_back(std::move(source));
}

void Node::set_fetch_timeout(std::chrono::milliseconds timeout) {
    std::lock_guard lock(impl_->mu);
    impl_->fetch_timeout = timeout;
}

void Node::fetch_module(const Digest& module, std::optional<mesh::PublicKey> from) {
    std::lock_guard lock(impl_->mu);
    impl_->need_module(module, from, [impl = impl_.get(), module](const std::expected<std::string, std::string>& r) {
        if (!r) impl->warn("fetching module " + to_hex(module).substr(0, 16) + " failed: " + r.error());
    });
}

bool Node::fetching(const Digest& module) const {
    std::lock_guard lock(impl_->mu);
    return impl_->fetches.contains(module);
}

std::expected<std::uint64_t, std::string> Node::launch(const mesh::PublicKey& host, const mesh::Passport& passport,
                                                       runtime::Bytes args) {
    std::lock_guard lock(impl_->mu);
    const mesh::LedgerState& st = impl_->ledger.state();
    const bool here = host == impl_->key.public_key();
    if (!here && !st.is_host(host)) return std::unexpected(std::string("the target is not an enrolled host"));
    if (!passport.links().empty()) return std::unexpected(std::string("a root paglet needs a root passport"));
    const std::uint64_t id = impl_->next_request++;
    impl_->launches.emplace(id, std::pair{host, LaunchStatus{}});
    if (here) {
        impl_->start_launch(std::nullopt, passport.encode(), std::move(args),
                            [impl = impl_.get(), id](const std::expected<runtime::PagletId, std::string>& result) {
                                LaunchStatus& status = impl->launches.at(id).second;
                                if (result) {
                                    status.state = LaunchStatus::State::running;
                                    status.paglet = *result;
                                } else {
                                    status.state = LaunchStatus::State::failed;
                                    status.error = result.error();
                                }
                            });
        return id;
    }
    impl_->send_frame(host, mesh::Map{{"t", mesh::Value(frame_launch)},
                                      {"r", mesh::Value(static_cast<std::int64_t>(id))},
                                      {"p", mesh::Value(passport.encode())},
                                      {"a", mesh::Value(std::move(args))}});
    return id;
}

std::optional<Node::LaunchStatus> Node::launch_status(std::uint64_t launch) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->launches.find(launch);
    if (it == impl_->launches.end()) return std::nullopt;
    return it->second.second;
}

}  // namespace paglets::node
